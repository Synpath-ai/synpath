"""ProphetX order entry: order bodies, records, typed refusals, and the
adapter against a scripted venue. Records carry the fields of ProphetX's
Trading API reference (docs.prophetx.co: `get_order`, `get_trades`,
`get_order_matched_detail`, the `orders` WebSocket event) with values in the
reference's examples; they need an account to record."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from synpath.prophetx import lines_of, markets_of
from synpath.trading.credentials import CredentialsMissing, ProphetXCredentials
from synpath.trading.errors import DuplicateClientOrderId, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected
from synpath.trading.prophetx import (
    ProphetXTrading, build_order, contracts_of, error_of, fill_of_row, ladder_price, order_of_row, status_of,
)
from synpath.trading.types import (
    Liquidity, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce,
)
from synpath.errors import BadRequest

from conftest import load

V3 = load("prophetx_markets_v3.json")
AUTH = load("prophetx_auth.json")
BALANCE = load("prophetx_balance.json")
LIONS = "90de769f4f4977a04446ed203e957351"
VIKINGS = "7d75b0c55f5578219f534413d0bd872b"
MARKET_ID = f"prophetx:18925-{LIONS}"
LADDER = [-10000, -500, -470, -200, -150, -136, -120, -112, -110, -105, -101, 100, 101, 105, 110, 120, 124, 150,
          200, 500, 10000]
ORDER_ROW = {
    "order_id": "partner_6b0d2e8c-0f5e-4bd4-9a51-2b0f6c1b7a10", "external_id": "c-1", "strike_id": LIONS,
    "sport_event_id": 18925, "market_id": 219, "outcome_id": 4, "price": -110, "strike": 0,
    "quantity": 11.0, "original_quantity": 11.0, "filled_quantity": 5.5, "open_quantity": 5.5, "profit": 10.0,
    "matching_status": "partially_matched", "status": "open", "winning_status": "tbd",
    "user_id": "550e8400-e29b-41d4-a716-446655440000", "created_at": "2026-08-10T14:00:00Z",
    "updated_at": "2026-08-10T14:00:01Z", "fee": 0.05,
}
TRADE_ROW = {"id": 77, "order_id": ORDER_ROW["order_id"], "price": 124, "quantity": 10.0, "profit": 12.4,
             "strike": 0, "strike_id": VIKINGS, "status": "open", "winning_status": "tbd",
             "matched_at": "2026-08-10T14:00:02Z"}


def request(**changes) -> OrderRequest:
    base = {"market_id": MARKET_ID, "side": Side.BUY, "quantity": Decimal("21"), "price": Decimal("0.53")}
    return OrderRequest(**{**base, **changes})


class TestOrders:
    def test_a_buy_is_side_one_on_the_ladder_on_the_cheaper_side(self):
        # 0.53 sits between -112 (0.5283) and -115: the dearest price at or below it.
        body = build_order(request(client_order_id="c-1"), strike_id=LIONS, ladder=LADDER)
        assert body == {"external_id": "c-1", "strike_id": LIONS, "price": -112, "quantity": 11.09}

    def test_the_stake_is_the_contracts_at_the_ladder_price_rounded_down(self):
        body = build_order(request(price=Decimal("0.52381")), strike_id=LIONS, ladder=LADDER)
        assert body["price"] == -110 and body["quantity"] == 11.0  # 21 * 110/210
        # A hair below -110's 11/21 is not -110: the limit is never exceeded.
        assert build_order(request(price=Decimal("0.5238095")), strike_id=LIONS, ladder=LADDER)["price"] == -105

    def test_a_sell_buys_side_two_at_one_minus_the_price(self):
        body = build_order(request(side=Side.SELL, price=Decimal("0.55")), strike_id=VIKINGS, ladder=LADDER)
        # NO at 0.45 at most: +124 is 0.4464.
        assert body["strike_id"] == VIKINGS and body["price"] == 124

    @pytest.mark.parametrize("changes, strategy", [
        ({"type": OrderType.MARKET}, "fillOrKill"),
        ({"time_in_force": TimeInForce.IOC}, "fillOrKill"),
        ({"post_only": True}, "ALO"),
        ({}, None),
    ])
    def test_time_in_force(self, changes, strategy):
        body = build_order(request(**changes), strike_id=LIONS, ladder=LADDER)
        assert body.get("order_strategy") == strategy

    @pytest.mark.parametrize("changes", [
        {"time_in_force": TimeInForce.FOK},
        {"time_in_force": TimeInForce.GTD, "expires_at": 1_800_000_000_000},
        {"reduce_only": True},
        {"post_only": True, "time_in_force": TimeInForce.IOC},
        {"client_order_id": "has space"},
        {"quantity": Decimal("1")},
        {"type": OrderType.STOP_MARKET, "stop_price": Decimal("0.5")},
    ])
    def test_refused_before_sending(self, changes):
        with pytest.raises(InvalidOrder):
            build_order(request(**changes), strike_id=LIONS, ladder=LADDER)

    def test_no_ladder_price_below_the_limit(self):
        with pytest.raises(InvalidOrder, match="ladder"):
            ladder_price(Decimal("0.005"), LADDER)

    def test_an_external_id_is_made_when_none_is_given(self):
        body = build_order(request(), strike_id=LIONS, ladder=LADDER)
        assert len(body["external_id"]) == 32


class TestRecords:
    def test_an_order_row_in_contracts_on_the_yes_leg(self):
        order = order_of_row(ORDER_ROW, market_id=MARKET_ID, is_yes=True)
        assert (order.id, order.client_order_id, order.status) == (ORDER_ROW["order_id"], "c-1", OrderStatus.OPEN)
        assert order.price == Decimal(11) / Decimal(21) and order.side == Side.BUY
        assert (order.quantity, order.filled, order.remaining) == (Decimal("21"), Decimal("10.5"), Decimal("10.5"))
        assert order.fee == Decimal("0.05") and order.created_at == 1786370400000

    def test_a_side_two_order_is_a_sell_at_one_minus_its_price(self):
        order = order_of_row({**ORDER_ROW, "strike_id": VIKINGS, "price": 124}, market_id=MARKET_ID, is_yes=False)
        assert order.side == Side.SELL and order.price == 1 - Decimal(100) / Decimal(224)

    @pytest.mark.parametrize("changes, status", [
        ({"status": "inactive"}, OrderStatus.PENDING),
        ({"status": "invalid"}, OrderStatus.REJECTED),
        ({"status": "canceled", "filled_quantity": 0}, OrderStatus.CANCELED),
        ({"status": "wiped"}, OrderStatus.CANCELED),
        ({"status": "open", "matching_status": "unmatched"}, OrderStatus.OPEN),
        ({"status": "open", "matching_status": "fully_matched", "quantity": 11.0, "filled_quantity": 11.0}, OrderStatus.CLOSED),
        # A partly filled order whose rest was cancelled is rewritten smaller.
        ({"status": "open", "matching_status": "fully_matched", "quantity": 5.5}, OrderStatus.CANCELED),
        ({"status": "closed"}, OrderStatus.CLOSED),
        ({"status": "closed", "filled_quantity": 0}, OrderStatus.CANCELED),
    ])
    def test_statuses(self, changes, status):
        assert status_of({**ORDER_ROW, **changes}) == status

    def test_a_partial_whose_rest_was_cancelled_keeps_its_fill(self):
        order = order_of_row({**ORDER_ROW, "matching_status": "fully_matched", "quantity": 5.5, "open_quantity": 0},
                             market_id=MARKET_ID, is_yes=True)
        assert order.status == OrderStatus.CANCELED and order.filled == Decimal("10.5") and order.remaining == 0

    def test_a_fill_is_its_stake_plus_its_profit(self):
        fill = fill_of_row(TRADE_ROW, market_id=MARKET_ID, is_yes=False)
        assert fill.quantity == Decimal("22.4") == contracts_of(10, 124)
        assert fill.side == Side.SELL and fill.price == 1 - Decimal(100) / Decimal(224) and fill.id == "77"

    def test_a_stream_fill(self):
        row = {**ORDER_ROW, "id": ORDER_ROW["order_id"], "update_type": "matching", "fill_price": -105,
               "filled_quantity": 2.1, "total_filled_quantity": 7.6, "matching_id": 991, "aggressive": True}
        fill = fill_of_row(row, market_id=MARKET_ID, is_yes=True)
        assert fill.id == "991" and fill.quantity == Decimal("4.1") and fill.liquidity == Liquidity.TAKER
        order = order_of_row(row, market_id=MARKET_ID, is_yes=True, stream=True)
        assert order.filled == contracts_of(7.6, -110) and order.last_fill_quantity == Decimal("4.1")

    @pytest.mark.parametrize("code, error", [
        ("order_external_id_existed", DuplicateClientOrderId),
        ("invalid_price", InvalidOrder),
        ("order_invalid_profit", InvalidOrder),
        ("market_is_only_for_prematch", MarketHalted),
        ("sport_event_is_not_booked", MarketHalted),
        ("order_unable_to_place", OrderRejected),
    ])
    def test_refusals_are_typed(self, code, error):
        exc = BadRequest(f"prophetx: {code}", body={"error": code, "message": "m"}, status=400)
        assert isinstance(error_of(exc), error)


class Venue:
    """Answers requests by `METHOD /path` (below `/partner`), and records them."""

    def __init__(self, **answers):
        self.answers = {
            "POST /auth/login": {"data": {**AUTH["data"], "access_token": "tok", "refresh_token": "ref"}},
            "GET /v4/mm/get_price_ladder": {"data": LADDER},
            **answers,
        }
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        key = f"{req.method} {req.url.path.removeprefix('/partner')}"
        answer = self.answers.get(key)
        if answer is None:
            return httpx.Response(404, json={"error": "data_not_found", "message": "not found"})
        if callable(answer):
            answer = answer(req)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == "/partner" + path]


class Catalog:
    """The read adapter's lines for event 18925, from the documented markets."""

    def __init__(self):
        self.asked: list[str] = []

    def lines(self, event_id):
        self.asked.append(event_id)
        return [{**ln, "event_id": event_id} for m in markets_of(V3) for ln in lines_of(m)] if event_id == "18925" else []

    def close(self):
        pass


CREDS = ProphetXCredentials(access_key="acc", secret_key="sec")


def adapter(venue: Venue) -> ProphetXTrading:
    return ProphetXTrading(CREDS, catalog=Catalog(), client=httpx.AsyncClient(transport=httpx.MockTransport(venue)))


def run(coro):
    return asyncio.run(coro)


def placed(req):
    body = json.loads(req.content)
    return {"data": {"success": True, "order": {
        "order_id": "partner_1", "external_id": body["external_id"], "strike_id": body["strike_id"],
        "price": body["price"], "quantity": body["quantity"], "filled_quantity": 0, "open_quantity": body["quantity"],
        "matching_status": "unmatched", "status": "inactive", "sport_event_id": 18925, "market_id": 219}}}


class TestAdapter:
    def test_an_order_logs_in_snaps_and_comes_back_pending(self):
        venue = Venue(**{"POST /v4/mm/submit_order": placed})

        async def go():
            async with adapter(venue) as px:
                return await px.create_order(request(client_order_id="c-1"))

        order = run(go())
        [login] = venue.sent("POST", "/auth/login")
        assert json.loads(login.content) == {"access_key": "acc", "secret_key": "sec"}
        [sent] = venue.sent("POST", "/v4/mm/submit_order")
        assert sent.headers["authorization"] == "Bearer tok"
        assert json.loads(sent.content) == {"external_id": "c-1", "strike_id": LIONS, "price": -112, "quantity": 11.09}
        assert (order.id, order.status, order.market_id, order.side) == ("partner_1", OrderStatus.PENDING, MARKET_ID, Side.BUY)

    def test_a_sell_buys_the_lines_other_strike(self):
        venue = Venue(**{"POST /v4/mm/submit_order": placed})

        async def go():
            async with adapter(venue) as px:
                return await px.create_order(request(side=Side.SELL, price=Decimal("0.55")))

        order = run(go())
        assert json.loads(venue.sent("POST", "/v4/mm/submit_order")[0].content)["strike_id"] == VIKINGS
        assert order.side == Side.SELL and order.market_id == MARKET_ID

    def test_a_refusal_is_typed_and_a_batch_keeps_it(self):
        venue = Venue(**{"POST /v4/mm/submit_order": httpx.Response(
            400, json={"error": "order_external_id_existed", "message": "external id existed"})})

        async def go():
            async with adapter(venue) as px:
                return await px.create_orders([request(client_order_id="dup"), request(quantity=Decimal("1"))])

        dup, small = run(go())
        assert isinstance(dup, DuplicateClientOrderId) and isinstance(small, InvalidOrder)

    def test_a_cancel_is_final_though_the_read_lags(self):
        order = {**ORDER_ROW, "matching_status": "unmatched", "filled_quantity": 0}
        venue = Venue(**{
            "POST /v4/mm/cancel_order": {"data": {"success": True}},
            f"GET /v4/mm/get_order/{ORDER_ROW['order_id']}": {"data": order},
        })

        async def go():
            async with adapter(venue) as px:
                return await px.cancel_order(ORDER_ROW["order_id"])

        cancelled = run(go())
        [sent] = venue.sent("POST", "/v4/mm/cancel_order")
        assert json.loads(sent.content) == {"external_id": "c-1", "order_id": ORDER_ROW["order_id"]}
        assert cancelled.status == OrderStatus.CANCELED and cancelled.remaining == 0

    def test_a_cancel_waits_out_placing_and_reads_a_filled_order_back(self):
        answers = iter([httpx.Response(409, json={"error": "order_is_placing", "message": "placing"}),
                        httpx.Response(400, json={"error": "order_already_filled", "message": "filled"})])
        filled = {**ORDER_ROW, "matching_status": "fully_matched", "filled_quantity": 11.0, "open_quantity": 0}
        venue = Venue(**{
            "POST /v4/mm/cancel_order": lambda req: next(answers),
            f"GET /v4/mm/get_order/{ORDER_ROW['order_id']}": {"data": filled},
        })

        async def go():
            async with adapter(venue) as px:
                return await px.cancel_order(ORDER_ROW["order_id"])

        order = run(go())
        assert len(venue.sent("POST", "/v4/mm/cancel_order")) == 2 and order.status == OrderStatus.CLOSED

    def test_an_unknown_order(self):
        async def go():
            async with adapter(Venue()) as px:
                return await px.fetch_order("partner_x")

        with pytest.raises(OrderNotFound):
            run(go())

    def test_cancel_all_on_the_account_or_both_sides_of_a_line(self):
        venue = Venue(**{
            "POST /v4/mm/cancel_all_orders": {"data": {"success": True}},
            "POST /v4/mm/cancel_orders_by_market": lambda req: (
                {"data": {"success": True}} if json.loads(req.content)["strike_id"] == LIONS
                else httpx.Response(404, json={"error": "data_not_found", "message": "none"})),
        })

        async def go():
            async with adapter(venue) as px:
                return await px.cancel_all_orders(), await px.cancel_all_orders(market_id=MARKET_ID)

        assert run(go()) == (None, None)
        bodies = [json.loads(r.content) for r in venue.sent("POST", "/v4/mm/cancel_orders_by_market")]
        assert bodies == [{"event_id": 18925, "market_id": 219, "strike_id": LIONS},
                          {"event_id": 18925, "market_id": 219, "strike_id": VIKINGS}]

    def test_nothing_to_cancel_is_zero(self):
        venue = Venue(**{"POST /v4/mm/cancel_all_orders": httpx.Response(
            404, json={"error": "no_cancellable_orders", "message": "none"})})

        async def go():
            async with adapter(venue) as px:
                return await px.cancel_all_orders()

        assert run(go()) == 0

    def test_open_orders_are_the_resting_ones_on_the_line(self):
        rows = [ORDER_ROW, {**ORDER_ROW, "order_id": "partner_2", "matching_status": "fully_matched"},
                {**ORDER_ROW, "order_id": "partner_3", "strike_id": VIKINGS, "price": 124}]
        venue = Venue(**{"GET /v4/mm/get_order_history": {"data": {"orders": rows, "next_cursor": None}}})

        async def go():
            async with adapter(venue) as px:
                return await px.fetch_open_orders(market_id=MARKET_ID)

        orders = run(go())
        assert [(o.id, o.side) for o in orders] == [(ORDER_ROW["order_id"], Side.BUY), ("partner_3", Side.SELL)]
        params = dict(venue.sent("GET", "/v4/mm/get_order_history")[0].url.params)
        assert params["status"] == "open" and params["event_id"] == "18925" and params["limit"] == "1000"

    def test_my_trades_find_their_line_through_the_strike(self):
        venue = Venue(**{
            "GET /v4/mm/get_trades": {"data": {"trades": [TRADE_ROW], "next_cursor": "n2"}},
            "GET /v4/mm/get_strikes": {"data": [{"strike_id": VIKINGS, "event_id": 18925, "market_id": 219,
                                                 "type": "moneyline"}]},
        })

        async def go():
            async with adapter(venue) as px:
                return await px.fetch_my_trades(market_id=MARKET_ID)

        page = run(go())
        assert [(f.market_id, f.side, f.quantity) for f in page] == [(MARKET_ID, Side.SELL, Decimal("22.4"))]
        assert page.next_cursor == "n2"

    def test_balance_and_positions(self):
        rows = [ORDER_ROW, {**ORDER_ROW, "order_id": "partner_3", "strike_id": VIKINGS, "price": 124,
                            "filled_quantity": 2.0}, {**ORDER_ROW, "order_id": "partner_4", "filled_quantity": 0}]
        venue = Venue(**{
            "GET /v4/mm/get_balance": BALANCE,
            "GET /v4/mm/get_order_history": {"data": {"orders": rows}},
        })

        async def go():
            async with adapter(venue) as px:
                return await px.fetch_balance(), await px.fetch_positions()

        balance, [position] = run(go())
        assert (balance.available, balance.locked, balance.total) == (Decimal("1000.0"), Decimal("250.0"), Decimal("1250.0"))
        assert balance.info["gec_balance"] == 500.0
        assert position.market_id == MARKET_ID and position.side == PositionSide.LONG
        assert position.inventory_yes == Decimal("10.5") and position.inventory_no == Decimal("4.48")
        assert position.contracts == Decimal("6.02") and position.margin == Decimal("7.5")


class TestWiring:
    def test_the_affiliate_key_alone_does_not_trade(self):
        with pytest.raises(CredentialsMissing, match="PROPHETX_ACCESS_KEY"):
            ProphetXTrading(ProphetXCredentials(affiliate_key="k"))

    def test_the_client_builds_the_adapter(self):
        from synpath import Client

        client = Client(credentials={"prophetx": CREDS})
        trading = client.trading("prophetx")
        assert isinstance(trading, ProphetXTrading) and trading.url == "https://cash.api.prophetx.co/partner"
        assert trading.catalog.session is trading.session

    def test_the_engine_streams_on_the_adapters_session(self):
        from synpath.engine.feeds import default_streams

        trading = ProphetXTrading(CREDS, catalog=Catalog())
        streams = default_streams({"prophetx": trading}, {"prophetx": CREDS})["prophetx"]
        assert streams.market.session is trading.session and streams.private.session is trading.session
        assert streams.private.resolver == trading.side

    def test_doctor_says_what_each_key_does(self, tmp_path, monkeypatch, capsys):
        from synpath.trading.__main__ import doctor

        for name in ("PROPHETX_ACCESS_KEY", "PROPHETX_SECRET_KEY", "PROPHETX_AFFILIATE_KEY", "PROPHETX_SANDBOX"):
            monkeypatch.delenv(name, raising=False)
        env = tmp_path / ".env"
        env.write_text("PROPHETX_AFFILIATE_KEY=aff-key-123456\n")
        assert doctor(str(env)) == 0
        out = capsys.readouterr().out
        assert "affiliate key loaded; set PROPHETX_ACCESS_KEY / PROPHETX_SECRET_KEY to trade" in out
        env.write_text("PROPHETX_ACCESS_KEY=acc-key-123456\nPROPHETX_SECRET_KEY=sec-key-123456\nPROPHETX_SANDBOX=1\n")
        assert doctor(str(env)) == 0
        out = capsys.readouterr().out
        assert "configured (sandbox)" in out and "Trading API key pair loaded" in out
        assert "key-123456" not in out
