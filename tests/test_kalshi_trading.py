"""Kalshi order entry, offline.

Samples under `tests/samples/kalshi_trading_*.json` are real demo-environment
responses, captured unedited. The adapter tests run against an `httpx`
mock transport so the request the adapter *sends* -- headers, path, body --
is asserted, not only what it makes of the answer.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from synpath.errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from synpath.trading import (
    Account, EditRequest, OrderRequest, OrderStatus, OrderType, Side, TimeInForce, TradingExchange,
)
from synpath.trading.credentials import KalshiCredentials
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from synpath.trading.kalshi import (
    KalshiSigner, KalshiTrading, _path_of, _status_of, balance_of, fill_of,
    map_error, order_from_response, order_of, position_of, settlement_of, translate_order,
)
from synpath.trading.limiter import BudgetLimiter
from synpath.trading.types import Liquidity, PositionSide

SAMPLES = Path(__file__).parent / "samples"
D = Decimal
pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def sample(name: str):
    return json.loads((SAMPLES / f"kalshi_trading_{name}.json").read_text())


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def pem(rsa_key) -> bytes:
    return rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    )


@pytest.fixture
def creds(pem) -> KalshiCredentials:
    return KalshiCredentials(key_id="key-123", private_key_pem=pem, env="demo")


# ---------------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------------

class TestSigner:
    def test_signature_verifies_over_timestamp_method_path(self, rsa_key, pem):
        signer = KalshiSigner("key-123", pem)
        sig = signer.sign(1700000000000, "get", "/trade-api/v2/portfolio/balance")
        import base64
        rsa_key.public_key().verify(
            base64.b64decode(sig), b"1700000000000GET/trade-api/v2/portfolio/balance",
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )

    def test_headers_carry_key_signature_and_timestamp(self, pem):
        signer = KalshiSigner("key-123", pem)
        headers = signer.headers("POST", "/trade-api/v2/portfolio/events/orders", timestamp_ms=42)
        assert headers["KALSHI-ACCESS-KEY"] == "key-123"
        assert headers["KALSHI-ACCESS-TIMESTAMP"] == "42"
        assert headers["KALSHI-ACCESS-SIGNATURE"]

    def test_signed_path_is_the_base_url_path(self):
        assert _path_of("https://external-api.demo.kalshi.co/trade-api/v2") == "/trade-api/v2"
        assert _path_of("https://external-api.kalshi.com/trade-api/v2/") == "/trade-api/v2"


# ---------------------------------------------------------------------------
# Request translation
# ---------------------------------------------------------------------------

def req(**kw) -> OrderRequest:
    base = dict(market_id="kalshi:T-1", side=Side.BUY, amount=D("2"), price=D("0.30"))
    base.update(kw)
    return OrderRequest(**base)


class TestTranslate:
    @pytest.mark.parametrize("market,side,price,book_side,yes_price", [
        ("kalshi:T-1", Side.BUY, "0.30", "bid", "0.3000"),
        ("kalshi:T-1", Side.SELL, "0.30", "ask", "0.3000"),
        ("T-1", Side.BUY, "0.70", "bid", "0.7000"),
        ("T-1", Side.SELL, "0.70", "ask", "0.7000"),
    ])
    def test_buy_is_a_yes_bid_and_sell_a_yes_ask_at_the_price_given(self, market, side, price, book_side, yes_price):
        body = translate_order(req(market_id=market, side=side, price=D(price)))
        assert body["ticker"] == "T-1"
        assert body["side"] == book_side
        assert body["price"] == yes_price
        assert body["count"] == "2.00"

    def test_defaults(self):
        body = translate_order(req())
        assert body["time_in_force"] == "good_till_canceled"
        assert body["self_trade_prevention_type"] == "taker_at_cross"
        assert body["post_only"] is False and body["reduce_only"] is False
        assert len(body["client_order_id"]) == 36
        assert "expiration_time" not in body and "subaccount" not in body

    def test_client_order_id_is_kept(self):
        assert translate_order(req(client_order_id="mine-1"))["client_order_id"] == "mine-1"

    @pytest.mark.parametrize("tif,venue", [
        (TimeInForce.GTC, "good_till_canceled"), (TimeInForce.IOC, "immediate_or_cancel"), (TimeInForce.FOK, "fill_or_kill"),
    ])
    def test_time_in_force(self, tif, venue):
        assert translate_order(req(time_in_force=tif))["time_in_force"] == venue

    def test_gtd_is_gtc_with_expiration_in_seconds(self):
        body = translate_order(req(time_in_force=TimeInForce.GTD, expires_at=1_800_000_000_500))
        assert body["time_in_force"] == "good_till_canceled"
        assert body["expiration_time"] == 1_800_000_000

    def test_day_is_refused_here(self):
        with pytest.raises(InvalidOrder, match="engine"):
            translate_order(req(time_in_force=TimeInForce.DAY))

    def test_market_order_is_ioc_at_the_protection_price(self):
        body = translate_order(req(type=OrderType.MARKET, price=D("0.35")))
        assert body["time_in_force"] == "immediate_or_cancel" and body["price"] == "0.3500"
        fok = translate_order(req(type=OrderType.MARKET, price=D("0.35"), time_in_force=TimeInForce.FOK))
        assert fok["time_in_force"] == "fill_or_kill"

    def test_market_order_without_price_is_refused(self):
        with pytest.raises(InvalidOrder, match="protection price"):
            translate_order(req(type=OrderType.MARKET, price=None))

    def test_engine_order_types_are_refused(self):
        with pytest.raises(InvalidOrder, match="execution engine"):
            translate_order(req(type=OrderType.STOP_LIMIT, stop_price=D("0.40")))

    def test_off_tick_price_is_refused(self):
        with pytest.raises(InvalidOrder, match="tick"):
            translate_order(req(price=D("0.305")))

    def test_amount_below_the_grid_is_refused(self):
        with pytest.raises(InvalidOrder):
            translate_order(req(amount=D("0.001")))

    def test_another_venues_market_is_refused(self):
        with pytest.raises(InvalidOrder, match="belongs to polymarket"):
            translate_order(req(market_id="polymarket:1"))

    def test_subaccount_and_passthrough_params(self):
        body = translate_order(req(
            account=Account(venue="kalshi", subaccount="3"),
            params={"order_group_id": "g-1", "self_trade_prevention_type": "maker", "cancel_order_on_pause": True},
        ))
        assert body["subaccount"] == 3
        assert body["order_group_id"] == "g-1"
        assert body["self_trade_prevention_type"] == "maker"
        assert body["cancel_order_on_pause"] is True


# ---------------------------------------------------------------------------
# Normalizers, on captured responses
# ---------------------------------------------------------------------------

class TestNormalizers:
    def test_resting_order(self):
        order = order_of(sample("get_orders_id")["order"])
        assert order.id == "01a0afc0-46e8-7596-b183-1ff448331615"
        assert order.market_id == "kalshi:KXGOLDH-26SEP1712-T4407.99"
        assert order.side == Side.BUY and order.type == OrderType.LIMIT
        assert order.status == OrderStatus.OPEN and not order.is_terminal
        assert order.price == D("0.0200") and order.amount == D("2.00")
        assert order.filled == 0 and order.remaining == D("2.00")
        assert order.time_in_force == TimeInForce.GTC and order.expires_at is None
        assert order.cost is None and order.fee is None
        stamp = datetime.fromisoformat(sample("get_orders_id")["order"]["created_time"].replace("Z", "+00:00"))
        assert order.created_at == int(stamp.timestamp() * 1000)  # ms, from the ISO stamp
        assert order.info["outcome_side"] == "yes"

    def test_order_list_page_shape(self):
        rows = sample("get_orders")["orders"]
        assert all(order_of(r).venue == "kalshi" for r in rows)

    def test_create_response_resting(self):
        raw = sample("post_events_orders")
        body = {"ticker": "T-1", "side": "bid", "price": "0.0200", "count": "2.00", "time_in_force": "good_till_canceled",
                "client_order_id": raw["client_order_id"]}
        order = order_from_response(raw, body=body)
        assert order.id == raw["order_id"] and order.client_order_id == raw["client_order_id"]
        assert order.status == OrderStatus.OPEN and order.amount == D("2.00") and order.remaining == D("2.00")
        assert order.price == D("0.0200") and order.side == Side.BUY
        assert order.created_at == raw["ts_ms"]

    def test_ioc_that_found_nothing_is_canceled(self):
        raw = {"client_order_id": "c", "fill_count": "0.00", "order_id": "o", "remaining_count": "0.00", "ts_ms": 1}
        order = order_from_response(raw, body={"ticker": "T-1", "side": "bid", "price": "0.0200", "time_in_force": "immediate_or_cancel"})
        assert order.status == OrderStatus.CANCELED and order.is_terminal
        assert order.time_in_force == TimeInForce.IOC

    def test_taker_fill_response_is_closed_with_average_and_fee(self):
        raw = {"average_fee_paid": "0.0014", "average_fill_price": "0.9800", "client_order_id": "c",
               "fill_count": "1.00", "order_id": "o", "remaining_count": "0.00", "ts_ms": 1789655011496}
        order = order_from_response(raw, body={"ticker": "T-1", "side": "bid", "price": "0.9800", "time_in_force": "immediate_or_cancel"})
        assert order.status == OrderStatus.CLOSED
        assert order.filled == D("1.00") and order.average_price == D("0.9800") and order.fee == D("0.0014")

    def test_status_words(self):
        assert _status_of({"status": "resting"}, filled=D(0), remaining=D(1)) == OrderStatus.OPEN
        assert _status_of({"status": "executed"}, filled=D(1), remaining=D(0)) == OrderStatus.CLOSED
        assert _status_of({"status": "canceled"}, filled=D(0), remaining=D(0)) == OrderStatus.CANCELED
        assert _status_of({}, filled=D(1), remaining=D(0)) == OrderStatus.CLOSED
        assert _status_of({}, filled=D(0), remaining=D(0)) == OrderStatus.CANCELED

    def test_fill(self):
        fill = fill_of(sample("get_fills")["fills"][0])
        assert fill.id == "072210b7-9701-9f67-dbeb-8607028b7a25"
        assert fill.market_id == "kalshi:KXGOLDH-26SEP1712-T4407.99"
        assert fill.side == Side.BUY and fill.price == D("0.9800") and fill.amount == D("1.00")
        assert fill.fee == D("0.001400") and fill.fee_currency == "USD"
        assert fill.liquidity == Liquidity.TAKER
        assert fill.timestamp == 1789655011496  # ms, from created_time

    def test_position_sign_names_the_side(self):
        row = sample("get_positions")["market_positions"][0]
        pos = position_of(row)
        assert pos.market_id.startswith("kalshi:") and pos.side == PositionSide.LONG
        assert pos.contracts == D("1.00") and pos.inventory_yes is None
        assert pos.entry_price == D("0.9800") and pos.margin == D("0.980000")
        assert pos.realized_pnl == D("0.000000")
        assert pos.timestamp is not None
        short = position_of({**row, "position_fp": "-3.00", "market_exposure_dollars": "0.60"})
        assert short.side == PositionSide.SHORT and short.contracts == D("3.00") and short.entry_price == D("0.2000")
        flat = position_of({**row, "position_fp": "0.00", "market_exposure_dollars": "0"})
        assert flat.side == PositionSide.FLAT and flat.entry_price is None

    def test_balance_is_available_cash_with_no_locked_guess(self):
        bal = balance_of(sample("get_balance"), account=Account(venue="kalshi"))
        assert bal.available == D("100.0000") and bal.total == D("100.0000")
        assert bal.locked is None and bal.buying_power is None
        assert bal.currency == "USD" and bal.timestamp == 1789655008000

    def test_settlement(self):
        row = {"ticker": "T-1", "market_result": "yes", "yes_count_fp": "3.00", "no_count_fp": "0.00",
               "yes_total_cost_dollars": "1.50", "no_total_cost_dollars": "0", "revenue": 300, "fee_cost": "0.02",
               "settled_time": "2026-09-17T16:00:00Z"}
        s = settlement_of(row)
        assert s.market_id == "kalshi:T-1" and s.held == PositionSide.LONG and s.result == "yes" and s.won is True
        assert s.amount == D("3.00") and s.cost == D("1.50") and s.payout == D("3.00")
        assert s.pnl == D("1.48") and s.timestamp == 1789660800000
        lost = settlement_of({**row, "market_result": "no"})
        assert lost.won is False


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

class TestMapError:
    def test_fok_rejection_carries_the_venue_code(self):
        body = {"error": {"code": "fill_or_kill_insufficient_resting_volume", "message": "fill or kill insufficient resting volume"}}
        err = map_error(BadRequest("rejected (409)", body=body, status=409))
        assert isinstance(err, OrderRejected)
        assert err.reason == "fill_or_kill_insufficient_resting_volume" and err.status == 409

    def test_not_found_is_an_order_on_an_order_path(self):
        body = {"error": {"code": "not_found", "message": "not found"}}
        err = map_error(MarketNotFound("x", body=body, status=404), path="/portfolio/events/orders/abc")
        assert isinstance(err, OrderNotFound)
        assert isinstance(map_error(MarketNotFound("x", body=body, status=404), path="/markets/abc"), MarketNotFound)

    def test_insufficient_balance(self):
        err = map_error(BadRequest("x", body={"error": {"code": "insufficient_balance", "message": "insufficient balance"}}, status=400))
        assert isinstance(err, InsufficientFunds)

    def test_auth_passes_through(self):
        err = map_error(AuthenticationError("x", status=401))
        assert isinstance(err, AuthenticationError)

    def test_error_code_property(self):
        assert ExchangeError("x", body={"error": {"code": "abc"}}).code == "abc"
        assert ExchangeError("x", body={"code": "def"}).code == "def"
        assert ExchangeError("x", body="plain").code is None


# ---------------------------------------------------------------------------
# Adapter, over a mock transport
# ---------------------------------------------------------------------------

class Venue:
    """A scripted venue: records every request, answers from a queue."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.answers: list[tuple[int, object]] = []

    def push(self, status: int, body: object = None):
        self.answers.append((status, body))

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, body = self.answers.pop(0) if self.answers else (200, {})
        if body is None:
            return httpx.Response(status)
        return httpx.Response(status, json=body)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


@pytest.fixture
def venue():
    return Venue()


@pytest.fixture
def adapter(creds, venue):
    return KalshiTrading(creds, client=venue.client(), limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000))


class TestAdapter:
    def test_capabilities_are_complete_and_honest(self):
        has = KalshiTrading.has
        assert set(has.values()) <= {True, False}
        assert has["create_order"] and has["edit_order"] and has["fetch_queue_position"] and has["rfq"]
        assert has["split_merge"] is False
        assert all(has[k] is False for k in ("watch_orders", "watch_my_trades", "watch_positions", "watch_balance"))
        # Read keys are answered too, as False: a caller can ask anything.
        assert has["fetch_markets"] is False

    def test_capability_typo_fails_at_class_creation(self):
        with pytest.raises(TypeError, match="unknown capability"):
            class Broken(TradingExchange):
                has = {"create_orderz": True}

    async def test_create_order_signs_and_sends_the_yes_leg(self, adapter, venue):
        venue.push(201, sample("post_events_orders"))
        order = await adapter.create_order(req(side=Side.SELL, price=D("0.98"), client_order_id="c-1", book="alpha"))
        sent = venue.requests[0]
        assert sent.method == "POST" and sent.url.path == "/trade-api/v2/portfolio/events/orders"
        assert sent.headers["KALSHI-ACCESS-KEY"] == "key-123" and sent.headers["KALSHI-ACCESS-SIGNATURE"]
        body = json.loads(sent.content)
        assert body["side"] == "ask" and body["price"] == "0.9800" and body["count"] == "2.00"
        assert order.side == Side.SELL and order.price == D("0.9800") and order.book == "alpha"
        assert order.market_id == "kalshi:T-1" and order.info["request"]["side"] == "ask"

    async def test_signature_covers_path_without_query(self, adapter, venue, rsa_key):
        import base64
        venue.push(200, sample("get_orders"))
        await adapter.fetch_orders(status="resting", market_id="T-1")
        sent = venue.requests[0]
        assert sent.url.params["status"] == "resting" and sent.url.params["ticker"] == "T-1"
        stamp = sent.headers["KALSHI-ACCESS-TIMESTAMP"]
        rsa_key.public_key().verify(
            base64.b64decode(sent.headers["KALSHI-ACCESS-SIGNATURE"]),
            f"{stamp}GET/trade-api/v2/portfolio/orders".encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256(),
        )

    async def test_none_params_are_dropped(self, adapter, venue):
        venue.push(200, sample("get_orders"))
        await adapter.fetch_orders()
        assert dict(venue.requests[0].url.params) == {}

    async def test_venue_rejection_is_typed(self, adapter, venue):
        venue.push(409, {"error": {"code": "fill_or_kill_insufficient_resting_volume", "message": "fill or kill insufficient resting volume"}})
        with pytest.raises(OrderRejected) as info:
            await adapter.create_order(req(time_in_force=TimeInForce.FOK))
        assert info.value.reason == "fill_or_kill_insufficient_resting_volume"

    async def test_fetch_order_retries_a_fresh_404_then_gives_up(self, adapter, venue):
        venue.push(404, {"error": {"code": "not_found", "message": "not found"}})
        venue.push(200, sample("get_orders_id"))
        order = await adapter.fetch_order("01a0afc0-46e8-7596-b183-1ff448331615")
        assert order.status == OrderStatus.OPEN and len(venue.requests) == 2
        for _ in range(3):
            venue.push(404, {"error": {"code": "not_found", "message": "not found"}})
        with pytest.raises(OrderNotFound):
            await adapter.fetch_order("nope")

    async def test_cancel_reads_back_and_marks_canceled_despite_lag(self, adapter, venue):
        venue.push(200, sample("delete_events_orders_id"))
        venue.push(200, sample("get_orders_id"))  # still says resting: the store lags the cancel
        order = await adapter.cancel_order("01a0afc0-46e8-7596-b183-1ff448331615", market_id="KXGOLDH-26SEP1712-T4407.99")
        assert venue.requests[0].method == "DELETE" and venue.requests[0].url.params["market_ticker"] == "KXGOLDH-26SEP1712-T4407.99"
        assert order.status == OrderStatus.CANCELED and order.remaining == D("1.00")  # 2 resting, reduced_by 1
        assert order.info["cancel"]["reduced_by"] == "1.00"

    async def test_cancel_all_account_wide_returns_none_on_204(self, adapter, venue):
        venue.push(204)
        assert await adapter.cancel_all_orders() is None
        assert venue.requests[0].method == "DELETE" and venue.requests[0].url.path.endswith("/portfolio/events/orders")

    async def test_cancel_all_on_a_market_is_a_batch_cancel(self, adapter, venue):
        venue.push(200, {"orders": [sample("get_orders_id")["order"]], "cursor": ""})  # one resting order
        venue.push(200, sample("delete_events_orders_batched"))
        venue.push(200, {"order": {**sample("get_orders_id")["order"], "status": "canceled"}})
        assert await adapter.cancel_all_orders(market_id="KXGOLDH-26SEP1712-T4407.99") == 1
        batch = json.loads(venue.requests[1].content)
        assert batch == {"orders": [{"order_id": "01a0afc0-46e8-7596-b183-1ff448331615", "market_ticker": "KXGOLDH-26SEP1712-T4407.99"}]}

    async def test_decrease_keeps_priority_and_amend_does_not(self, adapter, venue):
        current = order_of(sample("get_orders_id")["order"])  # buy 2 @ 0.02, none filled
        venue.push(200, sample("post_events_orders_id_decrease"))
        smaller = await adapter.edit_order(EditRequest(order_id=current.id, amount=D("1")), current=current)
        sent = json.loads(venue.requests[0].content)
        assert venue.requests[0].url.path.endswith("/decrease") and sent["reduce_to"] == "1.00"
        assert smaller.queue_priority_preserved is True and smaller.remaining == D("1.00") and smaller.price == D("0.0200")
        assert smaller.id == current.id

        venue.push(200, sample("post_events_orders_id_amend"))
        moved = await adapter.edit_order(EditRequest(order_id=current.id, price=D("0.03")), current=current)
        sent = json.loads(venue.requests[1].content)
        assert venue.requests[1].url.path.endswith("/amend")
        assert sent["price"] == "0.0300" and sent["count"] == "2.00" and sent["side"] == "bid"
        assert sent["client_order_id"] == current.client_order_id and len(sent["updated_client_order_id"]) == 36
        assert moved.queue_priority_preserved is False
        assert moved.price == D("0.0300") and moved.amount == D("2.00") and moved.remaining == D("2.00")
        assert moved.client_order_id == sample("post_events_orders_id_amend")["client_order_id"]

    async def test_growing_the_order_is_an_amend(self, adapter, venue):
        current = order_of(sample("get_orders_id")["order"])
        venue.push(200, sample("post_events_orders_id_amend"))
        bigger = await adapter.edit_order(EditRequest(order_id=current.id, amount=D("5")), current=current)
        assert venue.requests[0].url.path.endswith("/amend") and bigger.queue_priority_preserved is False
        assert bigger.amount == D("5.00")

    async def test_edit_time_in_force_is_refused(self, adapter):
        current = order_of(sample("get_orders_id")["order"])
        with pytest.raises(InvalidOrder, match="cancel and replace"):
            await adapter.edit_order(EditRequest(order_id=current.id, time_in_force=TimeInForce.IOC), current=current)

    async def test_create_orders_mixes_local_refusals_with_venue_answers(self, adapter, venue):
        venue.push(200, sample("post_events_orders_batched"))
        results = await adapter.create_orders([
            req(client_order_id="a"),
            req(price=D("0.305")),  # off tick, never sent
            req(side=Side.SELL, client_order_id="b"),
        ])
        sent = json.loads(venue.requests[0].content)["orders"]
        assert [o["client_order_id"] for o in sent] == ["a", "b"]
        assert isinstance(results[1], InvalidOrder)
        assert results[0].id == "01a0afc0-4eb8-7304-bf2b-7ce445444752" and results[2].id == "01a0afc0-4eb8-7bda-8572-a31aa5cee4c9"
        assert results[2].side == Side.SELL  # sent as a YES ask, reported as sell

    async def test_create_orders_per_order_errors(self, adapter, venue):
        venue.push(200, {"orders": [
            {"error": {"code": "insufficient_balance", "message": "insufficient balance"}},
            sample("post_events_orders"),
        ]})
        results = await adapter.create_orders([req(), req()])
        assert isinstance(results[0], OrderRejected) and results[0].reason == "insufficient_balance"
        assert results[1].status == OrderStatus.OPEN

    async def test_cancel_orders_per_order_errors(self, adapter, venue):
        venue.push(200, {"orders": [
            {"order_id": "gone", "error": {"code": "not_found", "message": "not found"}},
            {"order_id": "01a0afc0-46e8-7596-b183-1ff448331615", "reduced_by": "2.00", "ts_ms": 1},
        ]})
        venue.push(200, sample("get_orders_id"))
        results = await adapter.cancel_orders(["gone", "01a0afc0-46e8-7596-b183-1ff448331615"])
        assert isinstance(results[0], OrderRejected)
        assert results[1].status == OrderStatus.CANCELED and results[1].remaining == 0

    async def test_fetch_open_orders_walks_pages(self, adapter, venue):
        row = sample("get_orders")["orders"][0]
        venue.push(200, {"orders": [row], "cursor": "next"})
        venue.push(200, {"orders": [{**row, "order_id": "second"}], "cursor": ""})
        orders = await adapter.fetch_open_orders(market_id="T-1")
        assert [o.id for o in orders] == [row["order_id"], "second"]
        assert venue.requests[0].url.params["status"] == "resting"
        assert venue.requests[1].url.params["cursor"] == "next"

    async def test_positions_drop_flat_rows(self, adapter, venue):
        row = sample("get_positions")["market_positions"][0]
        venue.push(200, {"market_positions": [row, {**row, "ticker": "T-2", "position_fp": "0.00"}], "cursor": ""})
        positions = await adapter.fetch_positions()
        assert [p.market_id for p in positions] == ["kalshi:KXGOLDH-26SEP1712-T4407.99"]

    async def test_balance_for_a_subaccount(self, adapter, venue):
        venue.push(200, sample("get_balance"))
        bal = await adapter.fetch_balance(account=Account(venue="kalshi", name="desk-2", subaccount="2"))
        assert venue.requests[0].url.params["subaccount"] == "2"
        assert bal.account.name == "desk-2" and bal.available == D("100.0000")

    async def test_fetch_limits_reconfigures_the_budget(self, adapter, venue):
        venue.push(200, sample("get_account_limits"))
        venue.push(200, sample("get_account_endpoint_costs"))
        await adapter.fetch_limits()
        snap = adapter.limiter.snapshot()
        assert snap["read"]["rate"] == 200 and snap["write"]["rate"] == 100
        assert adapter.endpoint_costs[("GET", "/trade-api/v2/cfbenchmarks")] == 50

    def test_costs_follow_the_venue_table(self, adapter):
        assert adapter.cost_of("POST", "/trade-api/v2/portfolio/events/orders") == 10
        assert adapter.cost_of("DELETE", "/trade-api/v2/portfolio/events/orders/abc-123") == 2
        assert adapter.cost_of("DELETE", "/trade-api/v2/portfolio/events/orders/batched") == 2
        assert adapter.cost_of("GET", "/trade-api/v2/portfolio/orders/abc-123") == 2
        assert adapter.cost_of("GET", "/trade-api/v2/portfolio/orders") == 10
        assert adapter.cost_of("PUT", "/trade-api/v2/communications/rfqs/r/quotes/q/confirm") == 1
        adapter.endpoint_costs[("GET", "/trade-api/v2/cfbenchmarks/*endpoint")] = 50
        assert adapter.cost_of("GET", "/trade-api/v2/cfbenchmarks/a/b/c") == 50
        assert adapter.cost_of("GET", "/trade-api/v2/cfbenchmarks") == 10

    async def test_fetch_limits_adopts_the_live_cost_table(self, adapter, venue):
        venue.push(200, sample("get_account_limits"))
        venue.push(200, sample("get_account_endpoint_costs"))
        await adapter.fetch_limits()
        assert adapter.default_cost == 10
        assert adapter.cost_of("GET", "/trade-api/v2/cfbenchmarks/x") == 50
        assert adapter.cost_of("GET", "/trade-api/v2/portfolio/orders/abc") == 2

    async def test_fee_estimate_uses_the_series_schedule(self, adapter, venue):
        venue.push(200, sample("get_series_id"))
        fee = await adapter.fetch_fee_estimate("kalshi:KXSILVERH-26SEP1712-T65.349", Side.BUY, D("0.50"), D("10"))
        assert venue.requests[0].url.path.endswith("/series/KXSILVERH")
        assert fee.taker_fee == D("0.18"), "0.175 is charged as 0.18: rounded up to the cent"
        assert fee.maker_fee == D("0.0") and fee.info["schedule"]["fee_type"] == "quadratic"

    async def test_queue_position(self, adapter, venue):
        venue.push(200, sample("get_orders_id_queue_position"))
        assert await adapter.fetch_queue_position("x") == D("0.00")

    async def test_order_group_round_trip(self, adapter, venue):
        venue.push(200, sample("post_order_groups_create"))
        venue.push(200)
        venue.push(200, {**sample("get_order_groups_id"), "is_auto_cancel_enabled": True})
        group = await adapter.create_order_group(5)
        assert json.loads(venue.requests[0].content) == {"contracts_limit_fp": "5.00"}
        await adapter.trigger_order_group(group)
        assert venue.requests[1].method == "PUT" and venue.requests[1].url.path.endswith(f"/{group}/trigger")
        assert (await adapter.fetch_order_group(group))["is_auto_cancel_enabled"] is True

    async def test_budget_is_drawn_per_order_in_a_batch(self, creds, venue):
        limiter = BudgetLimiter(read_per_second=1000, write_per_second=10, burst_seconds=2, borrow_seconds=1, max_wait_s=0.5)
        adapter = KalshiTrading(creds, client=venue.client(), limiter=limiter)
        venue.push(200, {"orders": [sample("post_events_orders")] * 2})
        await adapter.create_orders([req(), req()])  # 20 tokens: the whole 2-second bucket
        from synpath.trading.errors import RateBudgetExceeded
        with pytest.raises(RateBudgetExceeded):
            await adapter.create_order(req())

    async def test_context_manager_closes(self, creds, venue):
        async with KalshiTrading(creds, client=venue.client()) as k:
            assert repr(k) == "<KalshiTrading kalshi>"
