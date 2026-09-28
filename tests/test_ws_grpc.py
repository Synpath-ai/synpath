"""The Polymarket US exchange API's gRPC streams, offline.

Three layers:

* the normalizers, on messages as dicts in the protos' JSON mapping;
* stream behaviour (reconnects, resume tokens, deduplication, refusals)
  against scripted calls, which needs neither grpcio nor the protos;
* a real local gRPC server speaking the venue's own messages, which needs
  the proto bundle. Polymarket's protos are not in this repository; point
  `SYNPATH_POLYMARKET_US_PROTOS` at the downloaded bundle to run it.

Message shapes follow the published protos and documentation, including the
venue's worked example for fees.
"""
from __future__ import annotations

import asyncio
import base64
import os
import types
from decimal import Decimal

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from synpath.trading.credentials import PolymarketUSExchangeCredentials
from synpath.trading.polymarket_us_exchange import InstrumentScale, PolymarketUSExchangeTrading
from synpath.trading.types import Liquidity, OrderStatus, PositionSide, SettlementState, Side
from synpath.ws import grpc as wsgrpc
from synpath.ws import polymarket_us_exchange as pmx
from synpath.ws.base import (
    BookEvent, FillEvent, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, StreamStatusEvent, VenueEvent,
)

D = Decimal
pytestmark = pytest.mark.anyio
SYM = "aec-nfl-buf-kc-2026-01-26"
FRAC = "tec-nfl-sbw-2026-02-08-kc"
INSTRUMENTS = {
    SYM: {"symbol": SYM, "tickSize": 0.001, "priceScale": "1000", "fractionalQtyScale": "1", "minimumTradeQty": "1"},
    FRAC: {"symbol": FRAC, "tickSize": 0.01, "priceScale": "100", "fractionalQtyScale": "100", "minimumTradeQty": "1"},
}


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def order(**kw):
    base = {
        "id": "O1", "type": "ORDER_TYPE_LIMIT", "side": "SIDE_BUY", "orderQty": "10", "symbol": SYM, "clordId": "c1",
        "timeInForce": "TIME_IN_FORCE_GOOD_TILL_CANCEL", "account": "acct-1", "cumQty": "0", "avgPx": "0", "leavesQty": "10",
        "state": "ORDER_STATE_NEW", "price": "420", "priceScale": "1000", "fractionalQuantityScale": "1",
        "commissionNotionalTotalCollected": "0",
    }
    return {**base, **kw}


def execution(id="E1", kind="EXECUTION_TYPE_NEW", **kw):
    return {"id": id, "type": kind, "order": kw.pop("order", order()), "lastShares": "0", "lastPx": "0", "tradeId": "",
            "aggressor": False, "commissionNotionalCollected": "0", "transactTime": "2026-09-17T12:00:00.123456789Z",
            "orderRejectReason": "ORD_REJECT_REASON_EXCHANGE_OPTION", "unsolicitedCancelReason": "UNSOLICITED_CXL_REASON_UNDEFINED", **kw}


class FakeTokens:
    def __init__(self):
        self.issued = 0
        self.invalidated = 0

    async def token(self):
        self.issued += 1
        return f"tok-{self.issued}"

    def invalidate(self):
        self.invalidated += 1


class RpcError(Exception):
    def __init__(self, name: str, details: str = ""):
        super().__init__(f"{name}: {details}")
        self._name, self._details = name, details

    def code(self):
        return types.SimpleNamespace(name=self._name)

    def details(self):
        return self._details


class Script:
    """Each call plays one list of steps: messages, `READY`, an exception to
    raise, or "end" to finish the call. A call left without "end" stays open.
    An exception in place of the list refuses the call outright."""

    def __init__(self, *calls):
        self.calls = list(calls)
        self.requests: list[dict] = []
        self.metadata: list[dict] = []
        self.methods: list[str] = []

    async def __call__(self, method, request, metadata):
        self.methods.append(method)
        self.requests.append(request)
        self.metadata.append(dict(metadata))
        steps = self.calls.pop(0) if self.calls else []
        if isinstance(steps, BaseException):
            raise steps

        async def responses():
            for step in steps:
                if step == "end":
                    return
                if isinstance(step, BaseException):
                    raise step
                yield step
            await asyncio.Event().wait()

        return responses()


@pytest.fixture(scope="module")
def pem():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())


@pytest.fixture
async def trading(pem):
    creds = PolymarketUSExchangeCredentials(client_id="cid", private_key_pem=pem, participant_id="firms/F/users/u", account="acct-1")
    adapter = PolymarketUSExchangeTrading(creds)
    adapter.tokens = FakeTokens()
    adapter.refdata_calls = []

    async def load_instruments(symbols, *, strict=True):
        adapter.refdata_calls.append(list(symbols))
        for symbol in symbols:
            if symbol in INSTRUMENTS:
                adapter.remember_instrument(InstrumentScale.from_instrument(INSTRUMENTS[symbol]))
        return {s: adapter.cached_scale(s) for s in symbols if adapter.cached_scale(s)}

    adapter.load_instruments = load_instruments
    yield adapter
    await adapter.close()


FAST = {"backoff_initial": 0.001, "backoff_max": 0.01}


async def collect(stream, until, timeout: float = 3.0) -> list:
    events: list = []

    async def run():
        async for event in stream:
            events.append(event)
            if until(events):
                return

    await asyncio.wait_for(run(), timeout)
    return events


def of(events, kind):
    return [e for e in events if isinstance(e, kind)]


def states(events):
    return [e.state for e in of(events, StreamStatusEvent)]


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

class TestNormalizers:
    def test_new_execution_is_an_order_and_no_fill(self):
        events = pmx.execution_events(execution(), None, account=None)
        assert len(events) == 1 and isinstance(events[0], OrderEvent)
        o = events[0].order
        assert o.status == OrderStatus.OPEN and o.price == D("0.42") and o.amount == D("10") and events[0].native == "new"
        # Default enum values that mean nothing here are not carried along.
        assert o.info["execution"] == {"id": "E1", "type": "EXECUTION_TYPE_NEW", "transactTime": "2026-09-17T12:00:00.123456789Z"}

    def test_fill_on_a_fractional_instrument_uses_the_order_scales(self):
        filled = order(symbol=FRAC, orderQty="312", cumQty="312", leavesQty="0", avgPx="97", price="97", state="ORDER_STATE_FILLED",
                       priceScale="100", fractionalQuantityScale="100", commissionNotionalTotalCollected="100")
        events = pmx.execution_events(execution("E2", "EXECUTION_TYPE_FILL", order=filled, lastShares="312", lastPx="97",
                                                tradeId="T1", aggressor=True, commissionNotionalCollected="100"), None, account=None)
        o, f = events[0].order, events[1].fill
        assert o.status == OrderStatus.CLOSED and o.filled == D("3.12") and o.average_price == D("0.97")
        assert f.id == "T1" and f.amount == D("3.12") and f.price == D("0.97") and f.fee == D("0.01")
        assert f.liquidity == Liquidity.TAKER and f.settlement == SettlementState.CONFIRMED

    def test_reject_keeps_its_reason(self):
        events = pmx.execution_events(execution("E3", "EXECUTION_TYPE_REJECTED", order=order(state="ORDER_STATE_REJECTED"),
                                                text="price out of bounds", orderRejectReason="ORD_REJECT_REASON_PRICE_OUT_OF_BOUNDS"), None, account=None)
        o = events[0].order
        assert o.status == OrderStatus.REJECTED and events[0].native == "rejected"
        assert o.info["execution"]["orderRejectReason"] == "ORD_REJECT_REASON_PRICE_OUT_OF_BOUNDS" and o.info["execution"]["text"] == "price out of bounds"

    def test_an_order_without_scales_needs_reference_data(self):
        bare = order(priceScale="0", fractionalQuantityScale="0")
        with pytest.raises(LookupError, match="reference data"):
            pmx.execution_events(execution(order=bare), None, account=None)
        scale = InstrumentScale.from_instrument(INSTRUMENTS[SYM])
        assert pmx.execution_events(execution(order=bare), scale, account=None)[0].order.price == D("0.42")

    def test_trade_capture_settlement_and_the_reporting_side(self):
        buy = execution("EA", "EXECUTION_TYPE_FILL", order=order(side="SIDE_BUY"), lastShares="4", lastPx="420", tradeId="T9", aggressor=True)
        sell = execution("EP", "EXECUTION_TYPE_FILL", order=order(id="O2", side="SIDE_SELL", account=""), lastShares="4", lastPx="420", aggressor=False)
        trade = {"id": "T9", "aggressor": buy, "passive": sell, "state": "TRADE_STATE_NEW", "reportingCounterparty": "SIDE_SELL",
                 "tradeType": "TRADE_TYPE_REGULAR"}

        def fills(**kw):
            return pmx.trade_fills({**trade, **kw}, lambda s: None, account_for=lambda name: None)

        [mine] = fills()
        assert mine.order_id == "O2" and mine.side == Side.SELL and mine.id == "T9" and mine.settlement == SettlementState.MATCHED
        assert mine.info["role"] == "passive" and mine.info["trade"]["state"] == "TRADE_STATE_NEW"
        assert fills(state="TRADE_STATE_CLEARED")[0].settlement == SettlementState.CONFIRMED
        assert fills(state="TRADE_STATE_BUSTED")[0].settlement == SettlementState.FAILED
        assert fills(state="TRADE_STATE_REJECTED")[0].settlement == SettlementState.FAILED
        assert fills(state="TRADE_STATE_SOMETHING_NEW")[0].settlement == SettlementState.MATCHED
        # No reporting side: the executions that name an account are the firm's.
        [by_account] = fills(reportingCounterparty="SIDE_UNDEFINED")
        assert by_account.order_id == "O1"

    def test_instrument_states(self):
        def state(native):
            return pmx.market_status_of({"symbol": SYM, "state": native, "updateTime": "2026-09-17T12:00:00Z"})

        assert state("INSTRUMENT_STATE_OPEN").state == "open" and state("INSTRUMENT_STATE_HALTED").state == "paused"
        assert state("INSTRUMENT_STATE_EXPIRED").state == "closed" and state("INSTRUMENT_STATE_TERMINATED").state == "settled"
        assert state("INSTRUMENT_STATE_PENDING").state == "created" and state("INSTRUMENT_STATE_NEW_ONE").state == "updated"
        closed = pmx.market_status_of({"symbol": SYM})
        assert closed.state == "closed" and closed.native == "INSTRUMENT_STATE_CLOSED"

    def test_ledger_entry(self):
        event = pmx.ledger_event({"id": "L1", "account": "firms/F/accounts/a", "currency": "USD", "beforeBalance": "100.00",
                                  "afterBalance": "96.9636", "description": "fill", "updateTime": "2026-09-17T12:00:00Z",
                                  "entryType": "LEDGER_ENTRY_TYPE_BALANCE_ORDER_EXECUTION", "symbol": FRAC, "updateBusinessDate": "2026-09-17"})
        p = event.payload
        assert event.name == "balance_ledger" and p["change"] == D("-3.0364") and p["entry_type"] == "order_execution"
        assert p["symbol"] == FRAC and p["business_date"] == "2026-09-17"


# ---------------------------------------------------------------------------
# Streams against scripted calls
# ---------------------------------------------------------------------------

class TestOrderStream:
    async def test_snapshot_executions_dedupe_and_reconcile(self, trading):
        snapshot = {"snapshot": {"orders": [order()]}, "sessionId": "s1"}
        update = {"update": {"executions": [execution("E1"), execution("E2", "EXECUTION_TYPE_PARTIAL_FILL",
                  order=order(cumQty="4", leavesQty="6", avgPx="420", state="ORDER_STATE_PARTIALLY_FILLED"),
                  lastShares="4", lastPx="420", tradeId="T1", commissionNotionalCollected="7")]}, "sessionId": "s1"}
        reject = {"update": {"executions": [], "cancelReject": {"orderId": "O1", "clordId": "c1", "rejectReason": "CXL_REJ_REASON_TOO_LATE_TO_CANCEL",
                                                                 "text": "filled"}}, "sessionId": "s1"}
        script = Script(
            [wsgrpc.READY, {"heartbeat": {}, "sessionId": "s1"}, snapshot, update, RpcError("UNAVAILABLE", "restart")],
            [wsgrpc.READY, snapshot, update, reject],
        )
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, symbols=[SYM], **FAST)
        events = await collect(stream, lambda ev: any(isinstance(e, VenueEvent) for e in ev))
        await stream.close()

        assert script.methods[0] == "polymarket.v1.OrderEntryAPI/CreateOrderSubscription"
        assert script.requests[0] == {"symbols": [SYM], "accounts": [], "snapshotOnly": False}
        assert script.metadata[0] == {"authorization": "Bearer tok-1", "x-participant-id": "firms/F/users/u"}
        assert script.metadata[1]["authorization"] == "Bearer tok-2"
        connected = [e for e in of(events, StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, True]
        orders = of(events, OrderEvent)
        # Two snapshots; the executions only once, although sent twice.
        assert [e.native for e in orders] == ["snapshot", "new", "partial_fill", "snapshot"]
        [fill] = of(events, FillEvent)
        assert fill.fill.amount == D("4") and fill.fill.fee == D("0.007") and fill.fill.account == trading.account
        assert orders[2].order.status == OrderStatus.OPEN and orders[2].order.filled == D("4")
        [rejected] = of(events, VenueEvent)
        assert rejected.name == "cancel_rejected" and rejected.payload["order_id"] == "O1" and not rejected.payload["is_replace"]
        assert stream.session_id == "s1"

    async def test_watch_orders_reopens_with_the_new_filter(self, trading):
        script = Script([wsgrpc.READY], [wsgrpc.READY, {"heartbeat": {}, "sessionId": "s2"}])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, **FAST)
        stream.start()
        await asyncio.wait_for(stream.connected.wait(), 2)
        await stream.watch_orders([SYM], accounts=["acct-1"])
        events = await collect(stream, lambda ev: states(ev).count("connected") == 2)
        await stream.close()
        assert script.requests[1]["symbols"] == [SYM] and script.requests[1]["accounts"] == ["acct-1"]
        assert "disconnected" in states(events)

    async def test_an_unreadable_item_costs_only_itself(self, trading):
        broken = order(id="BAD", symbol="unknown-symbol", priceScale="0")
        script = Script([wsgrpc.READY, {"snapshot": {"orders": [broken, order()]}}])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: len(of(ev, OrderEvent)) == 1)
        await stream.close()
        assert of(events, OrderEvent)[0].order.id == "O1"
        errors = [e for e in of(events, StreamStatusEvent) if e.state == "error"]
        assert errors and "unknown-symbol" in errors[0].detail
        # The missing scale was asked of reference data once, and not found.
        assert trading.refdata_calls == [["unknown-symbol"]]

    async def test_an_expired_token_is_refreshed(self, trading):
        script = Script(RpcError("UNAUTHENTICATED", "token expired"), [wsgrpc.READY])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: "connected" in states(ev))
        await stream.close()
        assert states(events)[:2] == ["connect_failed", "connected"] and trading.tokens.invalidated == 1

    async def test_permission_denied_ends_the_stream(self, trading):
        script = Script([RpcError("PERMISSION_DENIED", "missing scope read:orders")])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, **FAST)
        events = [e async for e in stream]
        assert states(events) == ["connect_failed", "failed"] and "read:orders" in events[-1].detail
        assert len(script.requests) == 1
        await stream.close()

    async def test_too_many_streams_waits_longest(self, trading):
        script = Script(RpcError("RESOURCE_EXHAUSTED", "20 streams"), [wsgrpc.READY])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, backoff_initial=0.001, backoff_max=0.2)
        loop = asyncio.get_running_loop()
        started = loop.time()
        await collect(stream, lambda ev: "connected" in states(ev))
        await stream.close()
        assert loop.time() - started >= 0.2

    async def test_silence_past_the_idle_timeout_reconnects(self, trading):
        script = Script([wsgrpc.READY], [wsgrpc.READY])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, idle_timeout=0.05, **FAST)
        events = await collect(stream, lambda ev: states(ev).count("connected") == 2)
        await stream.close()
        assert any(e.state == "disconnected" and "no message" in e.detail for e in of(events, StreamStatusEvent))

    async def test_a_call_that_died_while_the_machine_slept_is_replaced(self, trading):
        # A suspended machine freezes the loop's timers while the call dies.
        script = Script([wsgrpc.READY], [wsgrpc.READY, {"heartbeat": {}, "sessionId": "s3"}])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, idle_timeout=4.0, **FAST)
        stream.start()
        await asyncio.wait_for(stream.connected.wait(), 2)
        stream._last_seen -= 600_000
        events = await collect(stream, lambda ev: states(ev).count("connected") == 2, timeout=5)
        await stream.close()
        [dropped] = [e for e in of(events, StreamStatusEvent) if e.state == "disconnected"]
        assert "wall clock" in dropped.detail and "60" in dropped.detail

    async def test_heartbeat_streams_default_to_a_two_minute_idle_timeout(self, trading):
        assert pmx.PolymarketUSExchangeOrderStream(trading, call=Script()).idle_timeout == 120.0
        assert pmx.PolymarketUSExchangeDropCopyStream(trading, call=Script()).idle_timeout is None

    async def test_a_bad_message_does_not_end_the_call(self, trading):
        script = Script([wsgrpc.READY, {"update": {"executions": "not a list of executions"}}, {"snapshot": {"orders": [order()]}}])
        stream = pmx.PolymarketUSExchangeOrderStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: bool(of(ev, OrderEvent)))
        await stream.close()
        assert "error" in states(events) and len(script.requests) == 1


class TestDropCopy:
    async def test_resume_token_checkpoints_and_no_reconcile(self, trading):
        token1, token2 = base64.b64encode(b"\x00\x01").decode(), base64.b64encode(b"\x00\x02").decode()
        fill = execution("E5", "EXECUTION_TYPE_FILL", order=order(account="firms/F/accounts/other", cumQty="10", leavesQty="0",
                         avgPx="420", state="ORDER_STATE_FILLED"), lastShares="10", lastPx="420", tradeId="T5")
        script = Script(
            [wsgrpc.READY, {"resumeToken": token1, "executions": [execution("E4")]}, {"resumeToken": token1, "executions": []},
             RpcError("UNAVAILABLE", "")],
            [wsgrpc.READY, {"resumeToken": token1, "executions": [execution("E4")]}, {"resumeToken": token2, "executions": [fill]}],
        )
        stream = pmx.PolymarketUSExchangeDropCopyStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: sum(1 for e in of(ev, VenueEvent) if e.name == "checkpoint") == 2)
        await stream.close()
        assert "resumeToken" not in script.requests[0] and "resumeTime" not in script.requests[0]
        assert script.requests[1]["resumeToken"] == token1 and script.metadata[0]["x-participant-id"]
        connected = [e for e in of(events, StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, False]
        assert [e.order.id for e in of(events, OrderEvent)] == ["O1", "O1"]
        assert [e.order.info["execution"]["id"] for e in of(events, OrderEvent)] == ["E4", "E5"]
        [f] = of(events, FillEvent)
        assert f.fill.account.name == "firms/F/accounts/other" and f.fill.amount == D("10")
        checkpoints = [e.payload["resume_token"] for e in of(events, VenueEvent)]
        assert checkpoints == [token1, token2] and stream.resume_token == token2
        # The checkpoint follows the events it covers.
        assert events.index(of(events, VenueEvent)[1]) > events.index(f)

    async def test_a_reconnect_before_any_token_replays_from_the_start(self, trading):
        script = Script([wsgrpc.READY, RpcError("UNAVAILABLE", "")], [wsgrpc.READY])
        stream = pmx.PolymarketUSExchangeDropCopyStream(trading, call=script, symbols=[SYM], **FAST)
        await collect(stream, lambda ev: states(ev).count("connected") == 2)
        await stream.close()
        assert "resumeTime" not in script.requests[0] and script.requests[1]["resumeTime"].startswith("20")
        assert script.requests[1]["symbols"] == [SYM] and script.requests[1]["firms"] == []

    async def test_a_refused_token_falls_back_to_time_and_then_stops(self, trading):
        script = Script(
            [wsgrpc.READY, {"resumeToken": "AAE=", "executions": [execution("E6", transactTime="2026-09-17T12:30:00Z")]}, RpcError("UNAVAILABLE", "")],
            RpcError("INVALID_ARGUMENT", "resume token expired"),
            RpcError("INVALID_ARGUMENT", "still bad"),
        )
        stream = pmx.PolymarketUSExchangeDropCopyStream(trading, call=script, **FAST)
        events = [e async for e in stream]
        await stream.close()
        assert script.requests[1]["resumeToken"] == "AAE="
        assert "resumeToken" not in script.requests[2] and script.requests[2]["resumeTime"] == "2026-09-17T12:30:00.000Z"
        assert any(e.state == "error" and "refused the resume token" in e.detail for e in of(events, StreamStatusEvent))
        assert states(events)[-1] == "failed"

    async def test_a_saved_token_resumes_after_a_restart(self, trading):
        script = Script([wsgrpc.READY])
        stream = pmx.PolymarketUSExchangeDropCopyStream(trading, call=script, resume_token=b"\x07\x08", **FAST)
        await collect(stream, lambda ev: "connected" in states(ev))
        await stream.close()
        assert script.requests[0]["resumeToken"] == base64.b64encode(b"\x07\x08").decode()

    async def test_trade_capture_emits_the_same_fill_as_it_clears_or_busts(self, trading):
        buy = execution("EA", "EXECUTION_TYPE_FILL", order=order(), lastShares="2", lastPx="420", tradeId="T7", aggressor=True)
        trade = {"id": "T7", "aggressor": buy, "state": "TRADE_STATE_NEW", "reportingCounterparty": "SIDE_BUY"}
        script = Script([wsgrpc.READY, {"resumeToken": "AQ==", "tradeCaptureReports": [trade, trade]},
                         {"resumeToken": "Ag==", "tradeCaptureReports": [{**trade, "state": "TRADE_STATE_CLEARED"}]},
                         {"resumeToken": "Aw==", "tradeCaptureReports": [{**trade, "state": "TRADE_STATE_BUSTED"}]}])
        stream = pmx.PolymarketUSExchangeTradeCaptureStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: len(of(ev, FillEvent)) == 3)
        await stream.close()
        fills = [e.fill for e in of(events, FillEvent)]
        assert [f.id for f in fills] == ["T7"] * 3
        assert [f.settlement for f in fills] == [SettlementState.MATCHED, SettlementState.CONFIRMED, SettlementState.FAILED]
        assert script.methods[0].endswith("CreateTradeCaptureReportSubscription")

    async def test_position_changes_read_scales_from_reference_data(self, trading):
        change = {"position": {"account": "acct-1", "symbol": FRAC, "netPosition": "-250", "updateTime": "2026-09-17T12:00:00Z"},
                  "changeTime": "2026-09-17T12:00:00Z"}
        script = Script([wsgrpc.READY, {"resumeToken": "AQ==", "positionChanges": [change, change]}])
        stream = pmx.PolymarketUSExchangePositionChangeStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: len(of(ev, PositionEvent)) == 2)
        await stream.close()
        p = of(events, PositionEvent)[0].position
        assert p.side == PositionSide.SHORT and p.contracts == D("2.5") and p.account == trading.account
        assert trading.refdata_calls == [[FRAC]]

    async def test_instrument_states_need_no_participant_and_teach_scales(self, trading):
        instrument = {"symbol": "new-market", "state": "INSTRUMENT_STATE_HALTED", "priceScale": "100", "fractionalQtyScale": "10",
                      "tickSize": 0.01, "minimumTradeQty": "10", "updateTime": "2026-09-17T12:00:00Z"}
        script = Script([wsgrpc.READY, {"resumeToken": "AQ==", "instruments": [instrument, instrument]}])
        stream = pmx.PolymarketUSExchangeInstrumentStream(trading, call=script, symbols=["new-market"], **FAST)
        events = await collect(stream, lambda ev: bool(of(ev, MarketStatusEvent)) and bool(of(ev, VenueEvent)))
        await stream.close()
        [status] = of(events, MarketStatusEvent)
        assert status.state == "paused" and status.native == "INSTRUMENT_STATE_HALTED"
        assert "x-participant-id" not in script.metadata[0] and "firms" not in script.requests[0]
        assert trading.cached_scale("new-market").qty_scale == 10


class TestPositionStream:
    async def test_a_position_gone_from_a_new_snapshot_is_flat(self, trading):
        long = {"account": "acct-1", "symbol": SYM, "netPosition": "5"}
        short = {"account": "acct-1", "symbol": FRAC, "netPosition": "-100"}
        script = Script(
            [wsgrpc.READY, {"snapshot": {"positions": [long, short]}}, {"update": {"positions": [{**short, "netPosition": "-200"}]}},
             RpcError("UNAVAILABLE", "")],
            [wsgrpc.READY, {"heartbeat": {}}, {"snapshot": {"positions": [long]}}],
        )
        stream = pmx.PolymarketUSExchangePositionStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: len(of(ev, PositionEvent)) == 5)
        await stream.close()
        positions = [(e.position.market_id, e.position.side, e.position.contracts) for e in of(events, PositionEvent)]
        q = "polymarket_us:"
        assert positions == [
            (q + SYM, PositionSide.LONG, D("5")), (q + FRAC, PositionSide.SHORT, D("1")), (q + FRAC, PositionSide.SHORT, D("2")),
            (q + SYM, PositionSide.LONG, D("5")), (q + FRAC, PositionSide.FLAT, D("0")),
        ]
        connected = [e for e in of(events, StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, False]


class TestMarketData:
    async def test_books_quotes_and_subscriptions(self, trading):
        update = {"update": {"symbol": SYM, "bids": [{"px": "410", "qty": "30"}, {"px": "400", "qty": "5"}],
                             "offers": [{"px": "430", "qty": "12"}], "transactTime": "2026-09-17T12:00:00Z",
                             "stats": {"lastTradePx": "420", "sharesTraded": "900", "openInterest": "4000"}, "bookHidden": False}}
        frac = {"update": {"symbol": FRAC, "bids": [{"px": "50", "qty": "250"}], "offers": [], "bookHidden": False}}
        script = Script([wsgrpc.READY, update], [wsgrpc.READY, frac])
        stream = pmx.PolymarketUSExchangeMarketDataStream(trading, call=script, depth=5, **FAST)
        assert stream.request() is None
        await stream.watch_order_book([SYM, "not-listed"])
        events = await collect(stream, lambda ev: bool(of(ev, QuoteEvent)))
        [book] = of(events, BookEvent)
        assert book.kind == "snapshot" and book.best_bid == D("0.41") and book.best_ask == D("0.43")
        assert [(l.price, l.size) for l in book.bids] == [(D("0.41"), D("30")), (D("0.4"), D("5"))]
        [quote] = of(events, QuoteEvent)
        assert quote.last == D("0.42") and quote.volume == D("900") and quote.bid_size == D("30")
        assert script.requests[0] == {"symbols": [SYM], "depth": 5, "unaggregated": False, "snapshotOnly": False}
        assert "x-participant-id" not in script.metadata[0]
        assert any("not-listed" in e.detail for e in of(events, StreamStatusEvent) if e.state == "error")
        no = stream.book(SYM, side="no")
        assert no.best_bid == D("0.57") and no.best_ask == D("0.59")

        await stream.watch_order_book([FRAC])
        events = await collect(stream, lambda ev: bool(of(ev, BookEvent)))
        await stream.close()
        assert script.requests[1]["symbols"] == [SYM, FRAC]
        assert of(events, BookEvent)[0].bids[0].size == D("2.5")
        assert stream.book(SYM).ready is False  # the old call ended; its book waits for the next update

    async def test_a_hidden_book_is_not_ready(self, trading):
        script = Script([wsgrpc.READY, {"update": {"symbol": SYM, "bids": [], "offers": [], "bookHidden": True}}])
        stream = pmx.PolymarketUSExchangeMarketDataStream(trading, call=script, **FAST)
        await stream.watch_order_book([SYM])
        events = await collect(stream, lambda ev: bool(of(ev, BookEvent)))
        await stream.close()
        assert of(events, BookEvent)[0].info == {"book_hidden": True} and stream.book(SYM).ready is False

    async def test_a_thousand_symbols_per_stream(self, trading):
        stream = pmx.PolymarketUSExchangeMarketDataStream(trading, call=Script(), **FAST)
        with pytest.raises(ValueError, match="1000"):
            await stream.watch_order_book([f"s{i}" for i in range(1001)])
        await stream.close()


class TestBalanceLedger:
    async def test_replays_from_the_last_entry_and_drops_duplicates(self, trading):
        trading.trading_account = "firms/F/accounts/a"
        entry = {"id": "L1", "account": "firms/F/accounts/a", "currency": "USD", "beforeBalance": "10", "afterBalance": "12.5",
                 "updateTime": "2026-09-17T12:00:00Z", "entryType": "LEDGER_ENTRY_TYPE_BALANCE_DEPOSIT"}
        later = {**entry, "id": "L2", "beforeBalance": "12.5", "afterBalance": "12.49", "updateTime": "2026-09-17T12:05:00Z",
                 "entryType": "LEDGER_ENTRY_TYPE_BALANCE_COMMISSION"}
        script = Script([wsgrpc.READY, {"entries": [entry]}, {"entries": []}, RpcError("UNAVAILABLE", "")],
                        [wsgrpc.READY, {"entries": [entry, later]}])
        stream = pmx.PolymarketUSExchangeBalanceLedgerStream(trading, call=script, **FAST)
        events = await collect(stream, lambda ev: len(of(ev, VenueEvent)) == 2)
        await stream.close()
        assert script.requests[0] == {"account": "firms/F/accounts/a", "currency": "", "entryTypes": []}
        assert script.requests[1]["resumeTime"] == "2026-09-17T12:00:00.000Z"
        assert [e.payload["change"] for e in of(events, VenueEvent)] == [D("2.5"), D("-0.01")]
        connected = [e for e in of(events, StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, False]


class TestProtos:
    async def test_protos_are_required_and_say_where_to_get_them(self, trading, monkeypatch):
        monkeypatch.delenv(wsgrpc.PROTO_ENV, raising=False)
        with pytest.raises(wsgrpc.ProtosMissing, match=wsgrpc.PROTO_ENV):
            pmx.PolymarketUSExchangeOrderStream(trading)

    async def test_a_directory_without_the_bundle(self, tmp_path):
        with pytest.raises(wsgrpc.ProtosMissing, match="does not contain"):
            wsgrpc.ProtoBundle(tmp_path, anchor=pmx.ANCHOR, files=pmx.PROTO_FILES, cache_dir=tmp_path / "cache")

    async def test_recent_ids_forget_the_oldest(self):
        ids = wsgrpc.RecentIds(capacity=2)
        assert not ids.seen("a") and not ids.seen("b") and ids.seen("a")
        assert not ids.seen("c") and not ids.seen("b") and len(ids) == 2


# ---------------------------------------------------------------------------
# A real gRPC server, with the venue's own messages
# ---------------------------------------------------------------------------

PROTOS = os.environ.get(wsgrpc.PROTO_ENV)
needs_protos = pytest.mark.skipif(not PROTOS, reason=f"set {wsgrpc.PROTO_ENV} to the downloaded Polymarket US proto bundle")


@pytest.fixture(scope="module")
def bundle(tmp_path_factory):
    pytest.importorskip("grpc")
    return pmx.load_protos(PROTOS, cache_dir=tmp_path_factory.mktemp("protos"))


async def serve(bundle, service: str, method: str, handler):
    import grpc

    rpc = bundle.method(f"{service}/{method}")
    generic = grpc.method_handlers_generic_handler(service, {method: grpc.unary_stream_rpc_method_handler(
        handler, request_deserializer=rpc.request.FromString, response_serializer=rpc.response.SerializeToString,
    )})
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((generic,))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}", rpc


@needs_protos
class TestRealServer:
    async def test_order_stream_over_grpc(self, trading, bundle):
        import grpc

        calls: list[dict] = []
        filled = order(orderQty="312", cumQty="312", leavesQty="0", avgPx="97", price="97", state="ORDER_STATE_FILLED",
                       priceScale="100", fractionalQuantityScale="100", symbol=FRAC)
        fill = execution("E9", "EXECUTION_TYPE_FILL", order=filled, lastShares="312", lastPx="97", tradeId="T9",
                         aggressor=True, commissionNotionalCollected="100", transactTime="2026-09-17T12:00:00Z")

        async def handler(request, context):
            calls.append({"metadata": dict(context.invocation_metadata()), "request": bundle.to_dict(request)})
            if len(calls) == 1:
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "token expired")
            yield bundle.from_dict(rpc.response, {"heartbeat": {}, "sessionId": "grpc-1"})
            yield bundle.from_dict(rpc.response, {"snapshot": {"orders": [order(state="ORDER_STATE_NEW")]}, "sessionId": "grpc-1"})
            yield bundle.from_dict(rpc.response, {"update": {"executions": [fill]}, "sessionId": "grpc-1"})
            if len(calls) == 2:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "restarting")
            await asyncio.Event().wait()

        server, target, rpc = await serve(bundle, "polymarket.v1.OrderEntryAPI", "CreateOrderSubscription", handler)
        stream = pmx.PolymarketUSExchangeOrderStream(trading, protos=bundle, target=target, insecure=True, symbols=[FRAC], **FAST)
        try:
            events = await collect(stream, lambda ev: len(of(ev, OrderEvent)) == 3, timeout=10)
        finally:
            await stream.close()
            await server.stop(None)

        assert states(events)[:2] == ["connect_failed", "connected"] and trading.tokens.invalidated == 1
        assert calls[1]["metadata"]["authorization"] == "Bearer tok-2" and calls[1]["metadata"]["x-participant-id"] == "firms/F/users/u"
        assert calls[1]["request"]["symbols"] == [FRAC] and calls[1]["request"]["snapshotOnly"] is False
        # The enum's zero value survives the wire: a NEW order is open, not pending.
        assert [e.native for e in of(events, OrderEvent)] == ["snapshot", "fill", "snapshot"]
        assert of(events, OrderEvent)[0].order.status == OrderStatus.OPEN
        [f] = of(events, FillEvent)
        assert f.fill.amount == D("3.12") and f.fill.price == D("0.97") and f.fill.fee == D("0.01")
        connected = [e for e in of(events, StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, True]

    async def test_drop_copy_resume_token_over_grpc(self, trading, bundle):
        import grpc

        tokens: list[bytes] = []

        async def handler(request, context):
            tokens.append(request.resume_token)
            yield bundle.from_dict(rpc.response, {"resumeToken": base64.b64encode(b"\xff\x00cursor").decode(), "executions": [execution("E1")]})
            if len(tokens) == 1:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "restarting")
            await asyncio.Event().wait()

        server, target, rpc = await serve(bundle, "polymarket.v1.DropCopyAPI", "CreateDropCopySubscription", handler)
        stream = pmx.PolymarketUSExchangeDropCopyStream(trading, protos=bundle, target=target, insecure=True, **FAST)
        try:
            events = await collect(stream, lambda ev: states(ev).count("connected") == 2 and len(of(ev, VenueEvent)) == 1, timeout=10)
        finally:
            await stream.close()
            await server.stop(None)
        assert tokens == [b"", b"\xff\x00cursor"]
        assert len(of(events, OrderEvent)) == 1  # the replayed execution is dropped

    async def test_a_zip_bundle_compiles_to_the_same_messages(self, bundle, tmp_path):
        import zipfile

        archive = tmp_path / "protos.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            for path in bundle.root.rglob("*.proto"):
                zf.write(path, f"x/api/{path.relative_to(bundle.root)}")
        again = wsgrpc.ProtoBundle(archive, anchor=pmx.ANCHOR, files=pmx.PROTO_FILES, cache_dir=tmp_path / "cache")
        assert again.method("polymarket.v1.FundingAPI/CreateBalanceLedgerSubscription").path == "/polymarket.v1.FundingAPI/CreateBalanceLedgerSubscription"
