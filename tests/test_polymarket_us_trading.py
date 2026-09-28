"""Polymarket US order entry, offline: the retail API and the exchange API.

No Polymarket US account can be opened from the UK, so nothing here was
captured live. Request and response shapes follow the venue's published
OpenAPI schemas (`api-reference/oapi-schemas/orders-schema.json`,
`portfolio-schema.json`, `account-schema.json` for the retail API;
`institutional/oapi-schemas/trading-schema.json`, `report-schema.json`,
`positions-schema.json`, `refdata-schema.json` for the exchange API), and
authentication follows the venue's SDK (`polymarket-us` 0.x `auth.py`) and
its Auth0 private-key JWT guide. Market rules come from a real gateway
market captured for the read API.
"""
from __future__ import annotations

import base64
import json
from decimal import Decimal

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa

from synpath.trading import EditRequest, OrderRequest, OrderStatus, OrderType, Side, TimeInForce
from synpath.trading.credentials import PolymarketUSCredentials, PolymarketUSExchangeCredentials, load_credentials
from synpath.trading.errors import (
    CredentialsMissing, InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected, PermissionDenied,
)
from synpath.trading.limiter import BudgetLimiter
from synpath.trading import polymarket_us as retail
from synpath.trading import polymarket_us_exchange as exchange
from synpath.trading.types import Liquidity, PositionSide

D = Decimal
pytestmark = pytest.mark.anyio
SLUG = "tec-mlb-nlchamp-2026-09-27-atl"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
def ed_key():
    return ed25519.Ed25519PrivateKey.generate()


@pytest.fixture(scope="module")
def ed_secret(ed_key) -> str:
    raw = ed_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
    public = ed_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return base64.b64encode(raw + public).decode()   # the 64-byte form the portal issues


@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def rules(**kw) -> retail.MarketRules:
    market = {"slug": SLUG, "orderPriceMinTickSize": 0.001, "minimumTradeQty": 1, "feeCoefficient": 0.0695}
    market.update(kw)
    return retail.MarketRules.from_market(market)


def req(**kw) -> OrderRequest:
    base = dict(market_id=f"polymarket_us:{SLUG}", side=Side.BUY, amount=D("10"), price=D("0.107"))
    base.update(kw)
    return OrderRequest(**base)


# ===========================================================================
# Retail API
# ===========================================================================

class TestRetailCredentials:
    def test_loaded_from_environment(self):
        loaded = load_credentials({"POLYMARKET_US_KEY_ID": "kid", "POLYMARKET_US_SECRET_KEY": "c2VjcmV0"}, dotenv="/nonexistent", redact_logs=False)
        assert loaded["polymarket_us"].key_id == "kid"
        assert "c2VjcmV0" not in repr(loaded["polymarket_us"])

    def test_secret_required(self):
        with pytest.raises(CredentialsMissing, match="SECRET_KEY"):
            load_credentials({"POLYMARKET_US_KEY_ID": "kid"}, dotenv="/nonexistent", redact_logs=False)


class TestRetailSigner:
    def test_signs_timestamp_method_path(self, ed_key, ed_secret):
        signer = retail.PolymarketUSSigner("key-1", ed_secret)
        headers = signer.headers("get", "/v1/portfolio/positions", timestamp_ms=1789650000000)
        assert headers["X-PM-Access-Key"] == "key-1" and headers["X-PM-Timestamp"] == "1789650000000"
        ed_key.public_key().verify(base64.b64decode(headers["X-PM-Signature"]), b"1789650000000GET/v1/portfolio/positions")

    def test_32_byte_seed_accepted_and_garbage_refused(self, ed_key):
        seed = ed_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
        retail.PolymarketUSSigner("k", base64.b64encode(seed).decode())
        with pytest.raises(InvalidOrder, match="Ed25519"):
            retail.PolymarketUSSigner("k", base64.b64encode(b"short").decode())


class TestRetailTranslate:
    @pytest.mark.parametrize("market,side,price,wire_price,outcome_side,action", [
        (f"polymarket_us:{SLUG}", Side.BUY, "0.107", "0.107", "OUTCOME_SIDE_YES", "ORDER_ACTION_BUY"),
        (f"polymarket_us:{SLUG}", Side.SELL, "0.107", "0.107", "OUTCOME_SIDE_YES", "ORDER_ACTION_SELL"),
        (SLUG, Side.BUY, "0.17", "0.17", "OUTCOME_SIDE_YES", "ORDER_ACTION_BUY"),
        (SLUG, Side.SELL, "0.17", "0.17", "OUTCOME_SIDE_YES", "ORDER_ACTION_SELL"),
    ])
    def test_every_order_is_a_yes_order_at_the_price_given(self, market, side, price, wire_price, outcome_side, action):
        body = retail.translate_order(req(market_id=market, side=side, price=D(price)), rules())
        assert body["marketSlug"] == SLUG and body["type"] == "ORDER_TYPE_LIMIT"
        assert body["price"] == {"value": wire_price, "currency": "USD"}
        assert body["outcomeSide"] == outcome_side and body["action"] == action
        assert body["quantity"] == 10 and isinstance(body["quantity"], int)
        assert body["manualOrderIndicator"] == "MANUAL_ORDER_INDICATOR_AUTOMATIC"
        assert "intent" not in body and "participateDontInitiate" not in body

    @pytest.mark.parametrize("tif,wire", [
        (TimeInForce.GTC, "TIME_IN_FORCE_GOOD_TILL_CANCEL"), (TimeInForce.IOC, "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"),
        (TimeInForce.FOK, "TIME_IN_FORCE_FILL_OR_KILL"),
    ])
    def test_time_in_force(self, tif, wire):
        assert retail.translate_order(req(time_in_force=tif), rules())["tif"] == wire

    def test_gtd_carries_rfc3339_good_till_time(self):
        body = retail.translate_order(req(time_in_force=TimeInForce.GTD, expires_at=1789653600123), rules())
        assert body["tif"] == "TIME_IN_FORCE_GOOD_TILL_DATE" and body["goodTillTime"] == "2026-09-17T14:00:00.123Z"

    def test_market_is_an_immediate_limit_and_post_only_maps(self):
        assert retail.translate_order(req(type=OrderType.MARKET), rules())["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
        assert retail.translate_order(req(post_only=True), rules())["participateDontInitiate"] is True

    def test_client_order_id_is_not_sent(self):
        body = retail.translate_order(req(client_order_id="mine"), rules())
        assert "mine" not in json.dumps(body)

    def test_fractional_contract_market(self):
        body = retail.translate_order(req(amount=D("0.5")), rules(minimumTradeQty=0.01))
        assert body["quantity"] == 0.5
        with pytest.raises(InvalidOrder, match="multiple"):
            retail.translate_order(req(amount=D("0.505")), rules(minimumTradeQty=0.01))

    @pytest.mark.parametrize("kw,match", [
        ({"amount": D("1.5")}, "whole contracts"),
        ({"price": D("0.1075")}, "tick"),
        ({"price": D("0.995")}, "outside"),
        ({"side": Side.SELL, "price": D("0.995")}, "outside"),
        ({"reduce_only": True}, "close_position"),
        ({"time_in_force": TimeInForce.DAY}, "session roll"),
        ({"type": OrderType.STOP_LIMIT, "stop_price": D("0.2")}, "execution engine"),
        ({"type": OrderType.MARKET, "price": None}, "protection price"),
        ({"market_id": "kalshi:T"}, "belongs to kalshi"),
        ({"post_only": True, "time_in_force": TimeInForce.IOC}, "gtc or gtd"),
    ])
    def test_refused_before_sending(self, kw, match):
        with pytest.raises(InvalidOrder, match=match):
            retail.translate_order(req(**kw), rules())


RETAIL_ORDER = {
    "id": "ord-1", "marketSlug": SLUG, "side": "ORDER_SIDE_SELL", "type": "ORDER_TYPE_LIMIT",
    "price": {"value": "0.17", "currency": "USD"}, "quantity": 10, "cumQuantity": 4, "leavesQuantity": 6,
    "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL", "intent": "ORDER_INTENT_BUY_SHORT", "state": "ORDER_STATE_PARTIALLY_FILLED",
    "avgPx": {"value": "0.16", "currency": "USD"}, "commissionNotionalTotalCollected": {"value": "0.05", "currency": "USD"},
    "createTime": "2026-09-17T12:00:00Z", "insertTime": "2026-09-17T12:00:01Z",
    "outcomeSide": "OUTCOME_SIDE_NO", "action": "ORDER_ACTION_BUY",
}


class TestRetailNormalizers:
    def test_a_no_order_reads_back_on_the_yes_leg(self):
        order = retail.order_of(RETAIL_ORDER)
        assert order.market_id == f"polymarket_us:{SLUG}" and order.side == Side.SELL
        assert order.price == D("0.17") and order.average_price == D("0.16")
        assert order.status == OrderStatus.OPEN and order.filled == D("4") and order.remaining == D("6")
        assert order.fee == D("0.05") and order.created_at == 1789646400000

    def test_intent_alone_is_enough(self):
        raw = {k: v for k, v in RETAIL_ORDER.items() if k not in ("outcomeSide", "action")} | {"intent": "ORDER_INTENT_SELL_LONG"}
        order = retail.order_of(raw)
        assert order.market_id == f"polymarket_us:{SLUG}" and order.side == Side.SELL and order.price == D("0.17")

    @pytest.mark.parametrize("state,status", [
        ("ORDER_STATE_PENDING_NEW", OrderStatus.PENDING), ("ORDER_STATE_NEW", OrderStatus.OPEN),
        ("ORDER_STATE_FILLED", OrderStatus.CLOSED), ("ORDER_STATE_REPLACED", OrderStatus.CANCELED),
        ("ORDER_STATE_PENDING_CANCEL", OrderStatus.PENDING_CANCEL), ("ORDER_STATE_EXPIRED", OrderStatus.EXPIRED),
        ("ORDER_STATE_REJECTED", OrderStatus.REJECTED),
    ])
    def test_states(self, state, status):
        assert retail.order_of({**RETAIL_ORDER, "state": state}).status == status

    def test_positions_net_long_and_short(self):
        long = retail.position_of(SLUG, {"netPositionDecimal": "12", "cost": {"value": "1.284"}, "realized": {"value": "0"}, "cashValue": {"value": "0.12"}})
        short = retail.position_of(SLUG, {"netPositionDecimal": "-5", "cost": {"value": "-0.5"}})
        assert long.side == PositionSide.LONG and long.contracts == D("12") and long.entry_price == D("0.107")
        assert long.unrealized_pnl == D("0.12") and long.inventory_yes is None
        assert short.side == PositionSide.SHORT and short.contracts == D("5") and short.market_id == f"polymarket_us:{SLUG}"

    def test_balance(self):
        bal = retail.balance_of({"balances": [{"currentBalance": 100.5, "currency": "USD", "buyingPower": 80.25, "openOrders": 20.25}]}, account=retail.Account(venue="polymarket_us"))
        assert bal.total == D("100.5") and bal.available == bal.buying_power == D("80.25") and bal.locked == D("20.25")

    def test_settlement_from_a_resolution(self):
        row = {"marketSlug": SLUG, "side": "POSITION_RESOLUTION_SIDE_LONG", "updateTime": "2026-10-24T03:00:00Z",
               "beforePosition": {"netPositionDecimal": "10", "cost": {"value": "1.07"}, "realized": {"value": "0"}},
               "afterPosition": {"netPositionDecimal": "0", "realized": {"value": "8.93"}}}
        s = retail.settlement_of(row)
        assert s.result == "yes" and s.won is True and s.amount == D("10") and s.pnl == D("8.93")

    def test_errors(self):
        from synpath.errors import BadRequest, MarketNotFound
        stopgap = retail.error_of(BadRequest("x", body={"message": "Global Rate Limit Exceeded"}, status=400))
        assert isinstance(stopgap, OrderRejected) and stopgap.reason == "latency_stopgap"
        assert isinstance(retail.error_of(BadRequest("x", body={"message": "insufficient buying power"})), InsufficientFunds)
        assert isinstance(retail.error_of(MarketNotFound("x", body={"message": "order not found"})), OrderNotFound)


class Venue:
    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.routes: dict[tuple[str, str], list[tuple[int, object]]] = {}

    def on(self, method: str, path: str, status: int = 200, body: object = None):
        self.routes.setdefault((method, path), []).append((status, body))

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queue = self.routes.get((request.method, request.url.path)) or []
        if not queue:
            return httpx.Response(599, json={"message": f"no route {request.method} {request.url.path}"})
        status, body = queue.pop(0) if len(queue) > 1 else queue[0]
        return httpx.Response(status, json=body) if body is not None else httpx.Response(status)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


@pytest.fixture
def us(ed_secret):
    venue = Venue()
    venue.on("GET", f"/v1/market/slug/{SLUG}", body={"market": {"slug": SLUG, "orderPriceMinTickSize": 0.001, "minimumTradeQty": 1, "feeCoefficient": 0.0695}})
    adapter = retail.PolymarketUSTrading(
        PolymarketUSCredentials(key_id="key-1", secret_key=ed_secret), client=venue.client(),
        limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000),
    )
    return adapter, venue


class TestRetailAdapter:
    def test_capabilities(self):
        has = retail.PolymarketUSTrading.has
        assert has["edit_order"] and has["fetch_settlements"] and has["cancel_all_orders"]
        assert has["fetch_my_trades"] is False and has["rfq"] is False and has["split_merge"] is False

    async def test_create_order_is_pending_and_signed(self, us, ed_key):
        adapter, venue = us
        venue.on("POST", "/v1/orders", body={"id": "ord-9"})
        order = await adapter.create_order(req(side=Side.SELL, price=D("0.17"), client_order_id="mine", book="b1"))
        sent = venue.sent("POST", "/v1/orders")[0]
        ed_key.public_key().verify(base64.b64decode(sent.headers["X-PM-Signature"]), f"{sent.headers['X-PM-Timestamp']}POST/v1/orders".encode())
        body = json.loads(sent.content)
        assert body["price"]["value"] == "0.17" and body["outcomeSide"] == "OUTCOME_SIDE_YES" and body["action"] == "ORDER_ACTION_SELL"
        assert order.id == "ord-9" and order.status == OrderStatus.PENDING and order.client_order_id == "mine"
        assert order.market_id == f"polymarket_us:{SLUG}" and order.price == D("0.17") and order.book == "b1"
        await adapter.create_order(req())
        assert len(venue.sent("GET", f"/v1/market/slug/{SLUG}")) == 1   # rules cached

    async def test_synchronous_execution_reads_the_order(self, us):
        adapter, venue = us
        venue.on("POST", "/v1/orders", body={"id": "ord-1", "executions": [{"type": "EXECUTION_TYPE_PARTIAL_FILL", "order": RETAIL_ORDER}]})
        order = await adapter.create_order(req(params={}))
        assert order.status == OrderStatus.OPEN and order.filled == D("4")

    async def test_create_orders_batches_by_twenty_and_skips_refusals(self, us):
        adapter, venue = us
        venue.on("POST", "/v1/orders/batched", body={"createdOrderIds": [f"o{i}" for i in range(20)]})
        venue.on("POST", "/v1/orders/batched", body={"createdOrderIds": ["o20"]})
        results = await adapter.create_orders([req() for _ in range(21)] + [req(price=D("0.9999"))])
        assert [len(json.loads(r.content)["orders"]) for r in venue.sent("POST", "/v1/orders/batched")] == [20, 1]
        assert results[20].id == "o20" and isinstance(results[21], InvalidOrder)

    async def test_cancel_sends_the_slug_and_reads_back(self, us):
        adapter, venue = us
        venue.on("POST", "/v1/order/ord-1/cancel", body={})
        venue.on("GET", "/v1/order/ord-1", body={"order": {**RETAIL_ORDER, "state": "ORDER_STATE_PENDING_CANCEL"}})
        order = await adapter.cancel_order("ord-1", market_id=SLUG)
        assert json.loads(venue.sent("POST", "/v1/order/ord-1/cancel")[0].content) == {"marketSlug": SLUG}
        assert order.status == OrderStatus.PENDING_CANCEL

    async def test_cancel_orders_only_sends_open_ones(self, us):
        adapter, venue = us
        venue.on("GET", "/v1/orders/open", body={"orders": [RETAIL_ORDER]})
        venue.on("POST", "/v1/orders/batched/cancel", body={"canceledOrderIds": ["ord-1"]})
        mine, gone = await adapter.cancel_orders(["ord-1", "ord-404"])
        assert mine.status == OrderStatus.PENDING_CANCEL and isinstance(gone, OrderNotFound)
        assert json.loads(venue.sent("POST", "/v1/orders/batched/cancel")[0].content) == {"orders": [{"orderId": "ord-1", "marketSlug": SLUG}]}

    async def test_cancel_all(self, us):
        adapter, venue = us
        venue.on("POST", "/v1/orders/open/cancel", body={"canceledOrderIds": ["a", "b"]})
        assert await adapter.cancel_all_orders(market_id=SLUG) == 2
        assert json.loads(venue.sent("POST", "/v1/orders/open/cancel")[0].content) == {"slugs": [SLUG]}

    async def test_edit_a_no_order_on_the_yes_leg(self, us):
        adapter, venue = us
        current = retail.order_of({**RETAIL_ORDER, "state": "ORDER_STATE_NEW", "cumQuantity": 0, "leavesQuantity": 10})
        venue.on("POST", "/v1/order/ord-1/modify", body={})
        venue.on("GET", "/v1/order/ord-1", body={"order": {**RETAIL_ORDER, "price": {"value": "0.15"}}})
        order = await adapter.edit_order(EditRequest(order_id="ord-1", price=D("0.15"), amount=D("12")), current=current)
        body = json.loads(venue.sent("POST", "/v1/order/ord-1/modify")[0].content)
        assert body == {"marketSlug": SLUG, "price": {"value": "0.15", "currency": "USD"}, "quantity": 12, "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL"}
        assert order.price == D("0.15") and order.queue_priority_preserved is None

    async def test_positions_page_until_eof(self, us):
        adapter, venue = us
        venue.on("GET", "/v1/portfolio/positions", body={"positions": {SLUG: {"netPositionDecimal": "3"}}, "nextCursor": "c2", "eof": False})
        venue.on("GET", "/v1/portfolio/positions", body={"positions": {"other": {"netPositionDecimal": "0"}}, "eof": True})
        positions = await adapter.fetch_positions()
        assert [p.market_id for p in positions] == [f"polymarket_us:{SLUG}"]
        assert venue.sent("GET", "/v1/portfolio/positions")[1].url.params["cursor"] == "c2"

    async def test_signature_excludes_the_query(self, us, ed_key):
        adapter, venue = us
        venue.on("GET", "/v1/orders/open", body={"orders": []})
        await adapter.fetch_open_orders(market_id=SLUG)
        sent = venue.sent("GET", "/v1/orders/open")[0]
        assert sent.url.params["slugs"] == SLUG
        ed_key.public_key().verify(base64.b64decode(sent.headers["X-PM-Signature"]), f"{sent.headers['X-PM-Timestamp']}GET/v1/orders/open".encode())

    async def test_settlements(self, us):
        adapter, venue = us
        venue.on("GET", "/v1/portfolio/activities", body={"activities": [{"type": "ACTIVITY_TYPE_POSITION_RESOLUTION", "positionResolution": {
            "marketSlug": SLUG, "side": "POSITION_RESOLUTION_SIDE_SHORT",
            "beforePosition": {"netPositionDecimal": "10", "realized": {"value": "0"}}, "afterPosition": {"realized": {"value": "-1.07"}}}}], "eof": True})
        (settlement,) = await adapter.fetch_settlements()
        assert settlement.result == "no" and settlement.won is False and settlement.pnl == D("-1.07")
        assert venue.sent("GET", "/v1/portfolio/activities")[0].url.params["types"] == "ACTIVITY_TYPE_POSITION_RESOLUTION"

    async def test_close_position_needs_a_reference_for_slippage(self, us):
        adapter, venue = us
        venue.on("POST", "/v1/order/close-position", body={"id": "close-1"})
        with pytest.raises(InvalidOrder, match="reference"):
            await adapter.close_position(SLUG, slippage_ticks=5)
        order = await adapter.close_position(SLUG, slippage_ticks=5, reference_price=D("0.5"))
        body = json.loads(venue.sent("POST", "/v1/order/close-position")[0].content)
        assert body["slippageTolerance"] == {"currentPrice": {"value": "0.5", "currency": "USD"}, "ticks": 5}
        assert order.id == "close-1" and order.status == OrderStatus.PENDING

    async def test_fee_estimate_has_a_maker_rebate(self, us):
        adapter, _ = us
        fee = await adapter.fetch_fee_estimate(SLUG, Side.BUY, D("0.5"), D("100"))
        assert fee.taker_fee == D(str(round(0.0695 * 100 * 0.25, 6))) and fee.maker_fee < 0


# ===========================================================================
# Exchange API
# ===========================================================================

INSTRUMENT = {
    "symbol": SLUG, "tickSize": 0.001, "priceScale": "1000", "fractionalQtyScale": "1", "minimumTradeQty": "1",
    "state": "INSTRUMENT_STATE_OPEN",
}


def scale() -> exchange.InstrumentScale:
    return exchange.InstrumentScale.from_instrument(INSTRUMENT)


class TestExchangeCredentials:
    def test_participant_id_is_required(self, tmp_path, rsa_key):
        pem = tmp_path / "k.pem"
        pem.write_bytes(rsa_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        env = {"POLYMARKET_US_CLIENT_ID": "cid", "POLYMARKET_US_PRIVATE_KEY_PATH": str(pem)}
        with pytest.raises(CredentialsMissing, match="PARTICIPANT_ID"):
            load_credentials(env, dotenv=tmp_path / "none", redact_logs=False)
        loaded = load_credentials({**env, "POLYMARKET_US_PARTICIPANT_ID": "firms/F/users/u"}, dotenv=tmp_path / "none", redact_logs=False)
        creds = loaded["polymarket_us_exchange"]
        assert creds.env == "preprod" and creds.participant_id == "firms/F/users/u" and "BEGIN" not in repr(creds)


class TestScale:
    def test_round_trip(self):
        s = scale()
        assert s.price_to_wire(D("0.107")) == "107" and s.price_from_wire("107") == D("0.107")
        assert s.qty_to_wire(D("10")) == "10" and s.tick == D("0.001") and s.min_quantity == 1

    def test_finer_than_scale_is_refused(self):
        with pytest.raises(InvalidOrder, match="price scale"):
            scale().price_to_wire(D("0.1075"))

    def test_fractional_quantities(self):
        s = exchange.InstrumentScale.from_instrument({**INSTRUMENT, "fractionalQtyScale": "100", "minimumTradeQty": "1"})
        assert s.min_quantity == D("0.01") and s.qty_to_wire(D("2.5")) == "250" and not s.precision.whole_contracts

    def test_a_tick_in_scaled_units(self):
        assert exchange.InstrumentScale.from_instrument({**INSTRUMENT, "tickSize": 1}).tick == D("0.001")


class TestExchangeTranslate:
    @pytest.mark.parametrize("side,price,wire_side,wire_price", [
        (Side.BUY, "0.107", "SIDE_BUY", "107"),
        (Side.SELL, "0.107", "SIDE_SELL", "107"),
        (Side.SELL, "0.17", "SIDE_SELL", "170"),
        (Side.BUY, "0.17", "SIDE_BUY", "170"),
    ])
    def test_yes_leg(self, side, price, wire_side, wire_price):
        body = exchange.translate_order(req(side=side, price=D(price)), scale(), account="acct-1")
        assert body["side"] == wire_side and body["price"] == wire_price and body["symbol"] == SLUG
        assert body["type"] == "ORDER_TYPE_LIMIT" and body["orderQty"] == "10" and body["account"] == "acct-1"
        assert len(body["clordId"]) == 36 and body["manualOrderIndicator"] == "MANUAL_ORDER_INDICATOR_AUTOMATED"

    def test_stop_limit_is_sent_on_the_yes_leg_as_given(self):
        body = exchange.translate_order(
            req(side=Side.BUY, type=OrderType.STOP_LIMIT, stop_price=D("0.6"), price=D("0.62")),
            scale(), account="a",
        )
        # buy stop at 0.60 (YES rises to 0.60), then a limit at 0.62
        assert body["type"] == "ORDER_TYPE_STOP_LIMIT" and body["side"] == "SIDE_BUY"
        assert body["stopPrice"] == "600" and body["price"] == "620"

    def test_stop_market_needs_no_price(self):
        body = exchange.translate_order(req(type=OrderType.STOP_MARKET, stop_price=D("0.1"), price=None, side=Side.SELL), scale(), account="a")
        assert body["type"] == "ORDER_TYPE_STOP" and "price" not in body and body["stopPrice"] == "100"

    def test_client_order_id_and_gtd(self):
        body = exchange.translate_order(req(client_order_id="clord-7", time_in_force=TimeInForce.GTD, expires_at=1789653600000), scale(), account="a")
        assert body["clordId"] == "clord-7" and body["timeInForce"] == "TIME_IN_FORCE_GOOD_TILL_TIME"
        assert body["goodTillTime"] == "2026-09-17T14:00:00.000Z"

    @pytest.mark.parametrize("kw,match", [
        ({"type": OrderType.TRAILING_STOP, "stop_price": D("0.1")}, "execution engine"),
        ({"type": OrderType.MARKET, "price": None}, "protection price"),
        ({"time_in_force": TimeInForce.DAY}, "roll"),
        ({"reduce_only": True}, "reduce-only"),
        ({"amount": D("0.5")}, "whole contracts"),
    ])
    def test_refused(self, kw, match):
        with pytest.raises(InvalidOrder, match=match):
            exchange.translate_order(req(**kw), scale(), account="a")


EXCHANGE_ORDER = {
    "id": "x-1", "type": "ORDER_TYPE_LIMIT", "side": "SIDE_SELL", "orderQty": "10", "symbol": SLUG, "clordId": "clord-7",
    "timeInForce": "TIME_IN_FORCE_GOOD_TILL_CANCEL", "account": "acct-1", "cumQty": "4", "avgPx": "168", "leavesQty": "6",
    "state": "ORDER_STATE_PARTIALLY_FILLED", "price": "170", "createTime": "2026-09-17T12:00:00Z",
}


class TestExchangeNormalizers:
    def test_order(self):
        order = exchange.order_of(EXCHANGE_ORDER, scale())
        assert order.market_id == f"polymarket_us:{SLUG}" and order.side == Side.SELL and order.price == D("0.17")
        assert order.filled == D("4") and order.remaining == D("6") and order.average_price == D("0.168")
        assert order.client_order_id == "clord-7" and order.status == OrderStatus.OPEN

    def test_stop_order(self):
        order = exchange.order_of({**EXCHANGE_ORDER, "type": "ORDER_TYPE_STOP", "stopPrice": "600", "price": "0"}, scale())
        assert order.type == OrderType.STOP_MARKET and order.stop_price == D("0.6") and order.price is None

    def test_fill_and_position(self):
        fill = exchange.fill_of({"id": "e1", "tradeId": "t1", "order": EXCHANGE_ORDER, "lastShares": "4", "lastPx": "168",
                                 "aggressor": False, "transactTime": "2026-09-17T12:00:02Z", "commissionNotionalCollected": "12"}, scale())
        assert fill.id == "t1" and fill.order_id == "x-1" and fill.price == D("0.168") and fill.amount == D("4")
        # Commission is notional units: price scale 1000 times quantity scale 1.
        assert fill.liquidity == Liquidity.MAKER and fill.fee == D("0.012") and fill.info["commissionNotionalCollected"] == "12"
        pos = exchange.position_of({"symbol": SLUG, "netPosition": "-3"}, scale())
        assert pos.side == PositionSide.SHORT and pos.contracts == D("3")

    def test_fee_uses_the_scales_the_order_carries(self):
        # The venue's worked example: 312 units at 97 on a 100/100 instrument
        # is 3.12 contracts at $0.97, and 100 notional units is one cent.
        order = {"id": "BT3DNMHB94XJ", "type": "ORDER_TYPE_LIMIT", "side": "SIDE_BUY", "orderQty": "312", "symbol": SLUG,
                 "timeInForce": "TIME_IN_FORCE_FILL_OR_KILL", "cumQty": "312", "avgPx": "97", "state": "ORDER_STATE_FILLED",
                 "commissionNotionalTotalCollected": "100", "priceScale": "100", "fractionalQuantityScale": "100"}
        fill = exchange.fill_of({"id": "BW9QY8RWN52Z", "order": order, "lastShares": "312", "lastPx": "97",
                                 "type": "EXECUTION_TYPE_FILL", "aggressor": True, "commissionNotionalCollected": "100"}, scale())
        assert fill.amount == D("3.12") and fill.price == D("0.97") and fill.fee == D("0.01")
        assert fill.liquidity == Liquidity.TAKER
        rebate = exchange.fill_of({"order": {**order, "fractionalQuantityScale": "0"}, "lastShares": "100", "lastPx": "97",
                                   "commissionNotionalCollected": "-20"}, scale())
        assert rebate.amount == D("100") and rebate.fee == D("-0.2")

    def test_errors(self):
        from synpath.errors import AuthenticationError, BadRequest
        assert isinstance(exchange.error_of(AuthenticationError("x", body={"code": 7, "message": "permission denied: missing required scope write:orders"}, status=403)), PermissionDenied)
        rejected = exchange.error_of(BadRequest("x", body={"code": 3, "message": "invalid order quantity"}, status=400))
        assert isinstance(rejected, OrderRejected) and rejected.reason == "invalid_argument"


@pytest.fixture
def pmx(rsa_key):
    venue = Venue()
    venue.on("POST", "/oauth/token", body={"access_token": "tok-1", "token_type": "Bearer", "expires_in": 180})
    venue.on("POST", "/v1/refdata/instruments", body={"instruments": [INSTRUMENT], "eof": True})
    pem = rsa_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    creds = PolymarketUSExchangeCredentials(client_id="cid", private_key_pem=pem, participant_id="firms/F/users/u", account="acct-1")
    adapter = exchange.PolymarketUSExchangeTrading(creds, client=venue.client(), limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000))
    for budget in ("search_orders_budget", "search_executions_budget", "refdata_budget"):
        setattr(adapter, budget, BudgetLimiter(read_per_second=1000, write_per_second=1000))
    return adapter, venue


class TestExchangeAdapter:
    def test_capabilities_and_native_types(self):
        cls = exchange.PolymarketUSExchangeTrading
        assert cls.has["fetch_orders"] and cls.has["fetch_my_trades"] and cls.has["fetch_settlements"] is False
        assert OrderType.STOP_LIMIT in cls.native_order_types and OrderType.TRAILING_STOP not in cls.native_order_types

    async def test_token_is_a_signed_client_assertion_and_cached(self, pmx, rsa_key):
        adapter, venue = pmx
        venue.on("GET", "/v1/whoami", body={"user": "firms/F/users/u"})
        await adapter.whoami()
        await adapter.whoami()
        (token_request,) = venue.sent("POST", "/oauth/token")
        assert token_request.url.host == "pmx-preprod.us.auth0.com"
        body = json.loads(token_request.content)
        assert body["grant_type"] == "client_credentials" and body["audience"] == "https://api.preprod.polymarketexchange.com"
        claims = jwt.decode(body["client_assertion"], rsa_key.public_key(), algorithms=["RS256"], audience="https://pmx-preprod.us.auth0.com/oauth/token")
        assert claims["iss"] == claims["sub"] == "cid" and claims["exp"] - claims["iat"] == 300 and claims["jti"]
        whoami = venue.sent("GET", "/v1/whoami")[0]
        assert whoami.headers["Authorization"] == "Bearer tok-1" and whoami.headers["x-participant-id"] == "firms/F/users/u"

    async def test_expired_token_is_refreshed_once(self, pmx):
        adapter, venue = pmx
        venue.on("GET", "/v1/whoami", 401, {"code": 16, "message": "token expired"})
        venue.on("GET", "/v1/whoami", 200, {"user": "u"})
        assert (await adapter.whoami())["user"] == "u"
        assert len(venue.sent("POST", "/oauth/token")) == 2

    async def test_create_order_is_pending(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/trading/orders", body={"orderId": "x-9"})
        order = await adapter.create_order(req(side=Side.SELL, price=D("0.17"), client_order_id="c1"))
        body = json.loads(venue.sent("POST", "/v1/trading/orders")[0].content)
        assert body["side"] == "SIDE_SELL" and body["price"] == "170" and body["clordId"] == "c1"
        assert order.id == "x-9" and order.status == OrderStatus.PENDING and order.market_id == f"polymarket_us:{SLUG}"
        assert order.side == Side.SELL and order.price == D("0.17")
        refdata = venue.sent("POST", "/v1/refdata/instruments")[0]
        assert "x-participant-id" not in refdata.headers and json.loads(refdata.content)["symbols"] == [SLUG]

    async def test_create_orders_batches_by_twenty(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/trading/orders/list", body={"responses": [{"orderId": f"o{i}"} for i in range(20)]})
        venue.on("POST", "/v1/trading/orders/list", body={"responses": [{"orderId": "o20"}]})
        results = await adapter.create_orders([req() for _ in range(21)] + [req(market_id="unknown-symbol")])
        assert [len(json.loads(r.content)["requests"]) for r in venue.sent("POST", "/v1/trading/orders/list")] == [20, 1]
        assert results[20].id == "o20" and isinstance(results[21], InvalidOrder)

    async def test_cancel_with_current_needs_no_lookup(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/trading/orders/cancel", body={})
        current = exchange.order_of(EXCHANGE_ORDER, scale())
        order = await adapter.cancel_order("x-1", current=current)
        assert json.loads(venue.sent("POST", "/v1/trading/orders/cancel")[0].content) == {"orderId": "x-1", "symbol": SLUG}
        assert order.status == OrderStatus.PENDING_CANCEL and not venue.sent("GET", "/v1/trading/orders/open")

    async def test_cancel_all_lists_then_cancels(self, pmx):
        adapter, venue = pmx
        venue.on("GET", "/v1/trading/orders/open", body={"orders": [EXCHANGE_ORDER, {**EXCHANGE_ORDER, "id": "x-2"}]})
        venue.on("POST", "/v1/trading/orders/cancel/list", body={"responses": [{}, {}]})
        assert await adapter.cancel_all_orders(market_id=SLUG) == 2
        listed = venue.sent("GET", "/v1/trading/orders/open")[0]
        assert listed.url.params["symbols"] == SLUG and listed.url.params["accounts"] == "acct-1"

    async def test_edit_is_a_pending_replace(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/trading/orders/replace", body={})
        current = exchange.order_of(EXCHANGE_ORDER, scale())
        order = await adapter.edit_order(EditRequest(order_id="x-1", amount=D("8")), current=current)
        body = json.loads(venue.sent("POST", "/v1/trading/orders/replace")[0].content)
        assert body["orderQty"] == "8" and body["price"] == "170" and body["symbol"] == SLUG and body["clordId"]
        assert order.status == OrderStatus.PENDING_REPLACE and order.queue_priority_preserved is None

    async def test_fetch_order_falls_back_to_search(self, pmx):
        adapter, venue = pmx
        venue.on("GET", "/v1/trading/orders/open", body={"orders": []})
        venue.on("POST", "/v1/report/orders/search", body={"order": [{**EXCHANGE_ORDER, "state": "ORDER_STATE_FILLED", "cumQty": "10", "leavesQty": "0"}]})
        order = await adapter.fetch_order("x-1")
        assert order.status == OrderStatus.CLOSED
        assert json.loads(venue.sent("POST", "/v1/report/orders/search")[0].content)["orderId"] == "x-1"

    async def test_search_budget_is_rationed(self, pmx):
        adapter, venue = pmx
        from synpath.trading.errors import RateBudgetExceeded
        adapter.search_orders_budget = BudgetLimiter(read_per_second=12 / 60, write_per_second=12 / 60, burst_seconds=5, max_wait_s=0)
        venue.on("POST", "/v1/report/orders/search", body={"order": []})
        await adapter.fetch_orders()
        with pytest.raises(RateBudgetExceeded):
            await adapter.fetch_orders()

    async def test_my_trades(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/report/executions/search", body={"executions": [
            {"id": "e1", "tradeId": "t1", "order": EXCHANGE_ORDER, "lastShares": "4", "lastPx": "168", "aggressor": True,
             "transactTime": "2026-09-17T12:00:02Z", "type": "EXECUTION_TYPE_PARTIAL_FILL"}], "eof": True})
        (fill,) = await adapter.fetch_my_trades(market_id=SLUG)
        body = json.loads(venue.sent("POST", "/v1/report/executions/search")[0].content)
        assert body["types"] == ["EXECUTION_TYPE_FILL", "EXECUTION_TYPE_PARTIAL_FILL"] and body["symbol"] == SLUG
        assert fill.liquidity == Liquidity.TAKER and fill.amount == D("4")

    async def test_balance(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/positions/balance", body={"balance": "1000.00", "buyingPower": "750.50", "openOrders": "249.50", "updateTime": "2026-09-17T12:00:00Z"})
        bal = await adapter.fetch_balance()
        assert bal.total == D("1000.00") and bal.available == D("750.50") and bal.locked == D("249.50")
        assert json.loads(venue.sent("POST", "/v1/positions/balance")[0].content) == {"name": "acct-1", "currency": "USD"}

    async def test_account_is_discovered_when_not_configured(self, pmx):
        adapter, venue = pmx
        adapter.trading_account = None
        venue.on("GET", "/v1/accounts", body={"accounts": ["firms/F/accounts/main"], "displayNames": ["Main"]})
        venue.on("POST", "/v1/positions/balance", body={"balance": "1"})
        await adapter.fetch_balance()
        assert json.loads(venue.sent("POST", "/v1/positions/balance")[0].content)["name"] == "firms/F/accounts/main"

    async def test_permission_denied(self, pmx):
        adapter, venue = pmx
        venue.on("POST", "/v1/trading/orders", 403, {"code": 7, "message": "permission denied: missing required scope write:orders"})
        with pytest.raises(PermissionDenied, match="write:orders"):
            await adapter.create_order(req())
