"""Smarkets order entry: orders built from requests, Smarkets' records read on
the YES leg, and the adapter against a scripted venue.

Records in `tests/samples/smarkets_trading.json` are assembled from the
field examples of Smarkets' trading API reference (the OpenAPI document
behind docs.smarkets.com), since betting needs an approved API account: each
response is its schema with every field at its documented example. The login
example lost its NemID fields (`nemid_session_id`, `redirect_url`) and has
`verify` false; the error bodies are the documented `{"data", "error_type"}`
shape with documented codes. Tests change single fields where a scenario
needs consistent numbers. Every request goes to an `httpx.MockTransport`."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from synpath.trading.credentials import SmarketsCredentials
from synpath.trading.errors import (
    CredentialsMissing, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected, PermissionDenied,
)
from synpath.trading.smarkets import (
    PRICE_LADDER, SmarketsTrading, build_order, fill_of_activity, order_after_placing, order_of_row,
    positions_of_orders, price_bps, quantity_units, settlements_of_orders,
)
from synpath.trading.types import OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce

from conftest import load

S = load("smarkets_trading.json")
CONTRACT = "smarkets:128939-123456"
ORDER_ID = "1518455672861"


def request(**changes) -> OrderRequest:
    base = {"market_id": CONTRACT, "side": Side.BUY, "quantity": Decimal("10"), "price": Decimal("0.5")}
    return OrderRequest(**{**base, **changes})


def row(base: dict, **changes) -> dict:
    return {**base, **changes}


def resting(**changes) -> dict:
    """The documented order as a consistent unmatched one: 10 contracts, none matched."""
    return row(S["order"], **{
        "state": "created", "quantity": 100000, "quantity_user_currency": 100000, "quantity_filled": 0,
        "quantity_filled_user_currency": 0, "quantity_filled_pending": 0, "quantity_filled_pending_user_currency": 0,
        "quantity_unfilled": 100000, "quantity_unfilled_user_currency": 100000, "outcome": None, **changes,
    })


class Venue:
    """Answers requests by `METHOD /path`, and records them."""

    def __init__(self, **answers):
        self.answers = {"POST /v3/sessions/": httpx.Response(201, json=S["login"]), **answers}
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        answer = self.answers.get(f"{req.method} {req.url.path}")
        if answer is None:
            return httpx.Response(404, json={"data": None, "error_type": "NOT_FOUND"})
        if callable(answer):
            answer = answer(req)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


def adapter(venue: Venue, **kwargs) -> SmarketsTrading:
    creds = SmarketsCredentials(username="john.doe@company.com", password="verysecurepassword")
    return SmarketsTrading(
        creds, client=httpx.AsyncClient(transport=httpx.MockTransport(venue)),
        session_client=httpx.Client(transport=httpx.MockTransport(venue)), **kwargs,
    )


def run(coro):
    return asyncio.run(coro)


def error(code: str, status: int = 400) -> httpx.Response:
    return httpx.Response(status, json=S["errors"][code])


class TestOrders:
    def test_a_buy_backs_the_contract_at_its_price_in_basis_points(self):
        assert build_order(request(client_order_id="c-1")) == {
            "market_id": "128939", "contract_id": "123456", "side": "buy", "price": 5000, "quantity": 100000,
            "type": "good_til_halted", "label": "c-1",
        }

    def test_a_sell_lays_it(self):
        body = build_order(request(side=Side.SELL, price=Decimal("0.4")))
        assert body["side"] == "sell" and body["price"] == 4000

    def test_the_ladder_is_betfairs_odds_in_basis_points_with_the_ends(self):
        assert PRICE_LADDER[0] == 1 and PRICE_LADDER[-1] == 9999
        # Odds 2.0, 2.1, 2.2; and 32 and 6.4, whose half basis points round to even.
        assert {5000, 4762, 4545, 312, 1562}.issubset(PRICE_LADDER)
        # Every price in the recorded public quotes is on it.
        quotes = load("smarkets_quotes.json")
        seen = {level["price"] for contract in quotes.values() for side in ("bids", "offers")
                for level in contract.get(side) or []}
        assert seen and seen <= set(PRICE_LADDER)

    def test_prices_snap_to_the_ladder_on_the_cheaper_side(self):
        # 1/2.1 is the tick 4762 for either side, within half a basis point.
        assert price_bps(Side.BUY, Decimal("0.476190")) == price_bps(Side.SELL, Decimal("0.476190")) == 4762
        # 0.475 lies between 4717 (odds 2.12) and 4762 (odds 2.1): a buy pays no more, a sell receives no less.
        assert price_bps(Side.BUY, Decimal("0.475")) == 4717
        assert price_bps(Side.SELL, Decimal("0.475")) == 4762
        assert price_bps(Side.BUY, Decimal("0.0001")) == 1 and price_bps(Side.SELL, Decimal("0.9999")) == 9999

    def test_prices_outside_the_book_are_refused(self):
        for bad in (Decimal("0"), Decimal("1"), Decimal("1.2")):
            with pytest.raises(InvalidOrder):
                price_bps(Side.BUY, bad)
        with pytest.raises(InvalidOrder):
            price_bps(Side.BUY, Decimal("0.00004"))
        with pytest.raises(InvalidOrder):
            price_bps(Side.SELL, Decimal("0.99996"))

    def test_quantities_are_pots_in_ten_thousandths_rounded_down(self):
        assert quantity_units(Decimal("12.34567")) == 123456
        with pytest.raises(InvalidOrder):
            quantity_units(Decimal("0.00001"))
        with pytest.raises(InvalidOrder):
            quantity_units(Decimal("500000"))

    def test_order_types(self):
        assert build_order(request(params={"keep_in_play": True}))["type"] == "keep_in_play"
        assert build_order(request(params={"order_type": "good_til_cancelled"}))["type"] == "good_til_cancelled"
        ioc = build_order(request(time_in_force=TimeInForce.IOC, params={"minimum_quantity": "4"}))
        assert ioc["type"] == "immediate_or_cancel" and ioc["minimum_accepted_quantity"] == 40000
        market = build_order(request(type=OrderType.MARKET))
        assert market["type"] == "immediate_or_cancel" and "minimum_accepted_quantity" not in market
        fok = build_order(request(time_in_force=TimeInForce.FOK))
        assert fok["type"] == "immediate_or_cancel" and fok["minimum_accepted_quantity"] == 100000

    def test_a_label_in_params_wins(self):
        assert build_order(request(client_order_id="c-1", params={"label": "strategy_1"}))["label"] == "strategy_1"
        assert "label" not in build_order(request())

    def test_sell_yes_and_buy_no_are_the_same_lay(self):
        lay = build_order(request(side=Side.SELL))
        assert build_order(request(side=Side.SELL, outcome="yes", action="sell")) == lay
        assert build_order(request(side=Side.SELL, outcome="no", action="buy")) == lay

    @pytest.mark.parametrize("changes", [
        {"time_in_force": TimeInForce.GTD, "expires_at": 1}, {"time_in_force": TimeInForce.DAY},
        {"post_only": True}, {"type": OrderType.ICEBERG},
        {"type": OrderType.MARKET, "price": None}, {"market_id": "smarkets:128939"},
        {"params": {"order_type": "immediate_or_cancel"}},
    ])
    def test_what_smarkets_does_not_hold_is_refused(self, changes):
        from synpath.errors import BadRequest

        with pytest.raises(BadRequest):        # InvalidOrder is a BadRequest
            build_order(request(**changes))


class TestRecords:
    def test_the_documented_order(self):
        order = order_of_row(S["order"])
        assert order.id == ORDER_ID and order.market_id == CONTRACT and order.side == Side.BUY
        assert order.client_order_id == "strategy_1" and order.price == Decimal("0.5")
        assert order.quantity == Decimal("1") and order.created_at == 1344905805291
        # Filled 10000 and unfilled 2500 do not add up with the pending 2500, so
        # the pending part is taken as already inside `quantity_filled`.
        assert order.filled == Decimal("1") and order.average_price == Decimal("0.5")

    def test_an_unmatched_order_rests(self):
        order = order_of_row(resting())
        assert order.status == OrderStatus.OPEN and order.filled == 0 and order.remaining == Decimal("10")
        assert order.time_in_force == TimeInForce.GTC and order.average_price is None

    def test_pending_matches_count_as_filled_when_smarkets_counts_them_apart(self):
        order = order_of_row(resting(state="partial", quantity_filled_user_currency=20000,
                                     quantity_filled_pending_user_currency=30000,
                                     quantity_unfilled_user_currency=50000))
        assert order.status == OrderStatus.OPEN and order.filled == Decimal("5") and order.remaining == Decimal("5")

    def test_nothing_left_unmatched_is_cancelled_or_closed(self):
        cancelled = order_of_row(resting(quantity_unfilled_user_currency=0))
        assert cancelled.status == OrderStatus.CANCELED and cancelled.remaining == 0
        partly = order_of_row(resting(state="partial", quantity_filled_user_currency=40000,
                                      quantity_unfilled_user_currency=0))
        assert partly.status == OrderStatus.CANCELED and partly.filled == Decimal("4")
        filled = order_of_row(resting(state="filled", quantity_filled_user_currency=100000,
                                      quantity_unfilled_user_currency=0))
        assert filled.status == OrderStatus.CLOSED and filled.filled == filled.quantity

    def test_an_immediate_order_is_ioc(self):
        assert order_of_row(resting(type="immediate_or_cancel")).time_in_force == TimeInForce.IOC

    def test_a_lay_is_a_sell(self):
        assert order_of_row(resting(side="sell")).side == Side.SELL

    def test_a_placement(self):
        placed = order_after_placing(request(client_order_id="c-1"), row(S["create"], quantity=100000,
                                     total_executed_quantity=0, available_quantity=100000, executed_avg_price=None),
                                     account=None)  # type: ignore[arg-type]
        assert placed.id == ORDER_ID and placed.status == OrderStatus.OPEN and placed.remaining == Decimal("10")
        assert placed.client_order_id == "c-1" and placed.market_id == CONTRACT

    def test_a_placement_that_matched_in_full_is_closed(self):
        placed = order_after_placing(request(), S["create"], account=None)  # type: ignore[arg-type]
        assert placed.status == OrderStatus.CLOSED and placed.remaining == 0 and placed.average_price == Decimal("0.5")

    def test_an_ioc_placement_is_cancelled_after_what_matched(self):
        placed = order_after_placing(request(time_in_force=TimeInForce.IOC),
                                     row(S["create"], quantity=100000, total_executed_quantity=30000,
                                         available_quantity=70000), account=None)  # type: ignore[arg-type]
        assert placed.status == OrderStatus.CANCELED and placed.filled == Decimal("3") and placed.remaining == 0

    def test_an_execution_is_a_fill(self):
        fill = fill_of_activity(row(S["activity"]["account_activity"][0], source="order.execute"))
        assert fill.id == "11423-123242" and fill.order_id == ORDER_ID and fill.market_id == CONTRACT
        assert fill.price == Decimal("0.5") and fill.quantity == Decimal("1") and fill.side == Side.BUY
        assert fill.fee == Decimal("2.20") and fill.timestamp == 1485944634361

    def test_positions_net_backs_and_lays_per_contract(self):
        back = resting(state="filled", quantity_filled_user_currency=100000, quantity_unfilled_user_currency=0,
                       average_price_matched_precise="4000")
        lay = resting(id="2", side="sell", state="partial", quantity_filled_user_currency=40000,
                      quantity_unfilled_user_currency=60000, average_price_matched_precise="5000")
        settled = resting(id="3", state="settled", quantity_filled_user_currency=100000)
        [position] = positions_of_orders([back, lay, settled])
        assert position.market_id == CONTRACT and position.side == PositionSide.LONG
        assert position.contracts == Decimal("6") and position.inventory_yes == 10 and position.inventory_no == 4
        # 10 at 0.40 and 4 at 0.50 on the YES leg.
        assert position.entry_price == (Decimal("6") / Decimal("14")).quantize(Decimal("1e-10"))
        # Paid 4.00 backing and 2.00 laying; if the contract loses, the lays return 4.
        assert position.margin == Decimal("2")

    def test_settlements_read_the_outcome_on_the_yes_leg(self):
        won = resting(state="settled", outcome="winner", quantity_filled_user_currency=100000,
                      quantity_unfilled_user_currency=0, average_price_matched_precise="4000")
        lay = resting(id="2", side="sell", state="settled", outcome="winner", quantity_filled_user_currency=50000,
                      quantity_unfilled_user_currency=0, average_price_matched_precise="4000")
        back, laid = settlements_of_orders([won, lay, resting()])
        assert back.held == PositionSide.LONG and back.result == "yes" and back.won is True
        assert back.quantity == 10 and back.cost == Decimal("4") and back.payout == 10 and back.pnl == Decimal("6")
        assert laid.held == PositionSide.SHORT and laid.won is False and laid.cost == Decimal("3") and laid.pnl == -3
        void = settlements_of_orders([row(won, outcome="void")])[0]
        assert void.result == "void" and void.pnl == 0


class TestAdapter:
    def test_login_then_the_session_token_on_every_call(self):
        venue = Venue(**{"POST /v3/orders/": row(S["create"], quantity=100000, total_executed_quantity=0,
                                                 available_quantity=100000)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.create_order(request(client_order_id="c-1"))

        order = run(go())
        login, submit = venue.requests
        assert json.loads(login.content) == {"username": "john.doe@company.com", "password": "verysecurepassword"}
        assert submit.headers["Authorization"] == f"Session-Token {S['login']['token']}"
        assert json.loads(submit.content)["label"] == "c-1"
        assert order.status == OrderStatus.OPEN and order.client_order_id == "c-1"

    def test_an_expired_session_logs_in_again_once(self):
        answers = iter([error("AUTH_REQUIRED", 401), httpx.Response(200, json=S["account"])])
        venue = Venue(**{"GET /v3/accounts/": lambda req: next(answers)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.fetch_balance()

        balance = run(go())
        assert len(venue.sent("POST", "/v3/sessions/")) == 2
        assert (balance.total, balance.available, balance.locked) == (Decimal("150.00"), Decimal("100.00"), Decimal("50.00"))
        assert balance.currency == "GBP"

    def test_an_account_without_api_access_is_refused_with_a_reason(self):
        venue = Venue(**{"POST /v3/orders/": error("FORBIDDEN", 403)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.create_order(request())

        with pytest.raises(PermissionDenied, match="approved API access"):
            run(go())

    @pytest.mark.parametrize("code, kind", [
        ("ORDER_REJECTED_INSUFFICIENT_FUNDS", InsufficientFunds), ("ORDER_REJECTED_MARKET_HALTED", MarketHalted),
        ("ORDER_INVALID_INVALID_PRICE", InvalidOrder), ("ORDER_REJECTED_CROSSED_SELF", OrderRejected),
        ("ORDER_CANCELLED_INSUFFICIENT_LIQUIDITY", OrderRejected),
    ])
    def test_a_refused_order_is_typed(self, code, kind):
        venue = Venue(**{"POST /v3/orders/": error(code)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.create_order(request())

        with pytest.raises(kind) as caught:
            run(go())
        assert code in str(caught.value)

    def test_create_orders_returns_one_result_each(self):
        answers = iter([httpx.Response(200, json=S["create"]), error("ORDER_REJECTED_INSUFFICIENT_FUNDS")])
        venue = Venue(**{"POST /v3/orders/": lambda req: next(answers)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.create_orders([request(), request(), request(post_only=True)])

        placed, refused, invalid = run(go())
        assert placed.status == OrderStatus.CLOSED
        assert isinstance(refused, InsufficientFunds) and isinstance(invalid, InvalidOrder)
        assert len(venue.sent("POST", "/v3/orders/")) == 2

    def test_cancel_reads_the_order_back(self):
        listed = iter([{"orders": [resting(quantity_unfilled_user_currency=0)]}])
        venue = Venue(**{f"DELETE /v3/orders/{ORDER_ID}/": S["cancel"], "GET /v3/orders/": lambda req: next(listed)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.cancel_order(ORDER_ID)

        order = run(go())
        assert order.status == OrderStatus.CANCELED
        assert venue.sent("GET", "/v3/orders/")[0].url.params.get_list("id") == [ORDER_ID]

    def test_a_cancel_not_applied_yet_is_pending(self):
        venue = Venue(**{f"DELETE /v3/orders/{ORDER_ID}/": S["cancel"], "GET /v3/orders/": {"orders": [resting()]}})

        async def go():
            async with adapter(venue) as smk:
                return await smk.cancel_order(ORDER_ID)

        assert run(go()).status == OrderStatus.PENDING_CANCEL

    def test_a_refused_cancel_reads_back_and_an_unknown_one_is_not_found(self):
        filled = resting(state="filled", quantity_filled_user_currency=100000, quantity_unfilled_user_currency=0)
        venue = Venue(**{
            f"DELETE /v3/orders/{ORDER_ID}/": error("ORDER_CANCEL_REJECTED_TOO_LATE"),
            "GET /v3/orders/": {"orders": [filled]},
            "DELETE /v3/orders/9/": error("ORDER_CANCEL_REJECTED_NOT_FOUND", 404),
        })

        async def go():
            async with adapter(venue) as smk:
                too_late = await smk.cancel_order(ORDER_ID)
                with pytest.raises(OrderNotFound):
                    await smk.cancel_order("9")
                return too_late

        assert run(go()).status == OrderStatus.CLOSED

    def test_cancel_all_is_one_request_once_a_minute_then_order_by_order(self):
        venue = Venue(**{
            "DELETE /v3/orders/": S["cancel_all"],
            "GET /v3/orders/": lambda req: {"orders": [resting()] if req.url.params.get_list("state") else
                                            [resting(quantity_unfilled_user_currency=0)]},
            f"DELETE /v3/orders/{ORDER_ID}/": S["cancel"],
        })

        async def go():
            async with adapter(venue) as smk:
                return await smk.cancel_all_orders(market_id="smarkets:128939"), await smk.cancel_all_orders()

        assert run(go()) == (None, 1)
        [everything] = venue.sent("DELETE", "/v3/orders/")
        assert everything.url.params["market_id"] == "128939"
        assert len(venue.sent("DELETE", f"/v3/orders/{ORDER_ID}/")) == 1

    def test_cancel_all_on_a_contract_and_after_a_429_goes_order_by_order(self):
        venue = Venue(**{
            "DELETE /v3/orders/": error("RATE_LIMIT_EXCEEDED", 429),
            "GET /v3/orders/": lambda req: {"orders": [resting()] if req.url.params.get_list("state") else
                                            [resting(quantity_unfilled_user_currency=0)]},
            f"DELETE /v3/orders/{ORDER_ID}/": S["cancel"],
        })

        async def go():
            async with adapter(venue) as smk:
                return await smk.cancel_all_orders(), await smk.cancel_all_orders(market_id=CONTRACT)

        assert run(go()) == (1, 1)
        assert len(venue.sent("DELETE", "/v3/orders/")) == 1    # not retried
        on_contract = [r for r in venue.sent("GET", "/v3/orders/") if r.url.params.get("contract_id")]
        assert on_contract and on_contract[0].url.params["contract_id"] == "123456"

    def test_open_orders_page_by_id(self):
        pages = iter([{"orders": [resting()], "pagination": {"next_page": "?limit=100&pagination_last_id=1518455672861"}},
                      {"orders": [resting(id="1518455672862"), resting(id="3", quantity_unfilled_user_currency=0)],
                       "pagination": {"next_page": None}}])
        venue = Venue(**{"GET /v3/orders/": lambda req: next(pages)})

        async def go():
            async with adapter(venue) as smk:
                return await smk.fetch_open_orders(market_id=CONTRACT)

        orders = run(go())
        assert [o.id for o in orders] == [ORDER_ID, "1518455672862"]
        first, second = venue.sent("GET", "/v3/orders/")
        assert first.url.params.get_list("state") == ["created", "partial"]
        assert first.url.params["market_id"] == "128939" and first.url.params["contract_id"] == "123456"
        assert second.url.params["pagination_last_id"] == ORDER_ID

    def test_fetch_orders_by_status_with_a_cursor(self):
        venue = Venue(**{"GET /v3/orders/": {**S["orders"], "orders": [resting(state="filled",
                                             quantity_filled_user_currency=100000, quantity_unfilled_user_currency=0)]}})

        async def go():
            async with adapter(venue) as smk:
                return await smk.fetch_orders(status="closed", limit=5)

        page = run(go())
        assert [o.status for o in page] == [OrderStatus.CLOSED] and page.next_cursor == "810214202189041"
        sent = venue.sent("GET", "/v3/orders/")[0]
        assert sent.url.params.get_list("state") == ["filled", "settled"] and sent.url.params["limit"] == "5"

    def test_an_order_not_listed_is_read_by_id_or_not_found(self):
        venue = Venue(**{"GET /v3/orders/": {"orders": []}, f"GET /v3/orders/{ORDER_ID}/": S["order"],
                         "GET /v3/orders/9/": error("ORDER_NOT_FOUND", 404)})

        async def go():
            async with adapter(venue) as smk:
                found = await smk.fetch_order(ORDER_ID)
                with pytest.raises(OrderNotFound):
                    await smk.fetch_order("9")
                return found

        assert run(go()).id == ORDER_ID

    def test_my_trades_are_the_statements_executions(self):
        execution = row(S["activity"]["account_activity"][0], source="order.execute")
        other = row(execution, seq=1, contract_id="999")
        venue = Venue(**{
            "GET /v3/accounts/activity/": {**S["activity"], "account_activity": [execution, other],
                                           "pagination": {"next_page": "?limit=500&pagination_last_seq=1&pagination_last_subseq=123242"}},
            "GET /v3/accounts/": S["account"],
        })

        async def go():
            async with adapter(venue) as smk:
                return await smk.fetch_my_trades(market_id=CONTRACT)

        fills = run(go())
        assert [f.id for f in fills] == ["11423-123242"] and fills[0].fee_currency == "GBP"
        assert fills.next_cursor == "1:123242"
        sent = venue.sent("GET", "/v3/accounts/activity/")[0]
        assert sent.url.params.get_list("source") == ["order.execute"] and sent.url.params["market_id"] == "128939"

    def test_positions_and_settlements_read_the_orders(self):
        def orders(req):
            states = req.url.params.get_list("state")
            if states == ["settled"]:
                return {"orders": [resting(state="settled", outcome="loser", quantity_filled_user_currency=100000,
                                           quantity_unfilled_user_currency=0)]}
            return {"orders": [resting(state="filled", quantity_filled_user_currency=100000,
                                       quantity_unfilled_user_currency=0)]}

        venue = Venue(**{"GET /v3/orders/": orders})

        async def go():
            async with adapter(venue) as smk:
                return await smk.fetch_positions(market_id=CONTRACT), await smk.fetch_settlements()

        [position], settlements = run(go())
        assert position.contracts == Decimal("10") and position.side == PositionSide.LONG
        assert settlements[0].result == "no" and settlements[0].won is False and settlements[0].pnl == Decimal("-5")
        current = venue.sent("GET", "/v3/orders/")[0]
        assert current.url.params.get_list("state") == ["created", "partial", "filled"]

    def test_the_fee_is_the_accounts_commission_on_what_the_bet_wins(self):
        venue = Venue(**{"GET /v3/accounts/": {"account": {**S["account"]["account"], "commission_type": "0.03"}}})

        async def go():
            async with adapter(venue) as smk:
                return (await smk.fetch_fee_estimate(CONTRACT, Side.BUY, Decimal("0.4"), Decimal("100")),
                        await smk.fetch_fee_estimate(CONTRACT, Side.SELL, Decimal("0.4"), Decimal("100")))

        buy, sell = run(go())
        assert buy.taker_fee == Decimal("1.8") and sell.taker_fee == Decimal("1.2") and buy.maker_fee == buy.taker_fee
        assert buy.currency == "GBP" and len(venue.sent("GET", "/v3/accounts/")) == 1


class TestWiring:
    def test_credentials_are_needed(self):
        with pytest.raises(CredentialsMissing):
            SmarketsTrading(None)

    def test_the_client_the_engine_and_serve_build_it(self):
        import synpath
        from synpath.engine.__main__ import ADAPTERS
        from synpath.server.serve import VENUES

        creds = SmarketsCredentials(username="u", password="p")
        trading = synpath.Client({"smarkets": creds}).trading("smarkets")
        try:
            assert isinstance(trading, SmarketsTrading) and trading.has["edit_order"] is False
        finally:
            run(trading.close())
        assert ADAPTERS["smarkets"] == ("..trading.smarkets", "SmarketsTrading")
        assert "smarkets" in VENUES and synpath.SmarketsTrading is SmarketsTrading

    def test_the_engine_streams_share_the_adapters_session(self):
        from synpath.engine.feeds import default_streams

        creds = SmarketsCredentials(username="u", password="p")
        trading = SmarketsTrading(creds)
        try:
            streams = default_streams({"smarkets": trading}, {"smarkets": creds})["smarkets"]
            assert streams.private is None and streams.market.session is trading.session
        finally:
            run(trading.close())

    def test_doctor_names_the_login(self, tmp_path, monkeypatch, capsys):
        from synpath.trading.__main__ import doctor

        for name in ("SMARKETS_USERNAME", "SMARKETS_PASSWORD"):
            monkeypatch.delenv(name, raising=False)
        env = tmp_path / ".env"
        env.write_text("SMARKETS_USERNAME=jbloggs\nSMARKETS_PASSWORD=secret-pw-123\n")
        assert doctor(str(env)) == 0
        out = capsys.readouterr().out
        assert "user jbloggs" in out and "secret-pw-123" not in out
