"""Novig's streams: the public book polled without a key, the websocket's
verbs and channels with one, and the account's private channels. Polled
answers come from the recorded samples through an `httpx.MockTransport`;
websocket frames follow the venue's documented shapes, against a fake socket."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from synpath import AuthenticationError
from synpath.trading.types import OrderStatus, PositionSide, Side
from synpath.ws.base import (
    BookEvent, FillEvent, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, StreamStatusEvent, TradeEvent,
)
from synpath.ws.novig import NovigMarketStream, NovigUserStream, lifecycle_kind, trade_event

from conftest import load
from test_novig_trading import ORDER_ROW, credentials, verified

MARKET = load("novig_market.json")
BOOK = load("novig_book.json")
TRADES = load("novig_trades.json")
MID = MARKET["marketId"]
SID = f"novig:{MID}"
YES, NO = sorted(o["outcomeId"] for o in MARKET["outcomes"])


class Catalog:
    def outcome_ids(self, market_id):
        return YES, NO


class FakeSocket:
    def __init__(self):
        self.sent: list = []

    async def send(self, text):
        self.sent.append(json.loads(text))


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)
    await asyncio.sleep(0.01)
    for _ in range(5):
        await asyncio.sleep(0)


def of(events, kind):
    return [e for e in events if isinstance(e, kind)]


class TestRecords:
    def test_a_print_on_yes_is_a_taker_selling_it(self):
        row = next(r for r in TRADES["items"] if r["outcomeId"] == YES)
        event = trade_event(row, yes_outcome=YES, market=MID)
        assert event.taker_side == Side.SELL and event.price == Decimal(row["price"])
        assert event.quantity == Decimal(row["qty"]) / 100 and event.market_id == SID

    def test_a_print_on_no_is_a_taker_buying_yes(self):
        event = trade_event({**TRADES["items"][0], "outcomeId": NO, "price": "0.370"}, yes_outcome=YES, market=MID)
        assert event.taker_side == Side.BUY and event.price == Decimal("0.630")

    def test_lifecycle_deltas_in_both_documented_forms(self):
        assert lifecycle_kind({"kind": "GRADE", "status": "SETTLED"}) == ("GRADE", "SETTLED")
        assert lifecycle_kind("GOLIVE") == ("GOLIVE", None)


# ---------------------------------------------------------------------------
# Polled, without a key
# ---------------------------------------------------------------------------

class Public:
    """The public catalog's book, tape and market routes."""

    def __init__(self):
        self.book = BOOK
        self.trades = TRADES
        self.market = MARKET
        self.requests: list[httpx.Request] = []
        self.throttle = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.throttle:
            return httpx.Response(429, json={"code": "RATE_LIMIT_EXCEEDED", "message": "slow down"})
        path = request.url.path
        if path.endswith("/book"):
            return httpx.Response(200, json=self.book)
        if path.endswith("/trades"):
            return httpx.Response(200, json=self.trades)
        return httpx.Response(200, json=self.market)


def polled(api: Public) -> NovigMarketStream:
    client = httpx.AsyncClient(transport=httpx.MockTransport(api))
    return NovigMarketStream(catalog=Catalog(), client=client, limiter=None, interval=0.01)


class TestPolled:
    def test_without_a_key_it_polls(self):
        stream = NovigMarketStream(catalog=Catalog())
        assert stream.polling and stream.has["watch_order_book"] == "partial"
        assert NovigMarketStream.has["watch_order_book"] is True

    def test_a_book_then_its_changes(self):
        async def go():
            api = Public()
            stream = polled(api)
            await stream.watch_order_book([SID])
            await stream._open()
            first = stream.handle(await stream.poll())
            top = BOOK["orders"][YES][0]
            api.book = {**BOOK, "orders": {**BOOK["orders"], YES: [{**top, "qty": top["qty"] + 100}] + BOOK["orders"][YES][1:]}}
            second = stream.handle(await stream.poll())
            unchanged = stream.handle(await stream.poll())
            await stream.close()
            return first, second, unchanged, api

        first, second, unchanged, api = asyncio.run(go())
        (snapshot,) = of(first, BookEvent)
        assert snapshot.kind == "snapshot" and snapshot.best_bid == Decimal(BOOK["orders"][YES][0]["price"])
        assert snapshot.best_ask == 1 - Decimal(BOOK["orders"][NO][0]["price"])
        (delta,) = of(second, BookEvent)
        assert delta.kind == "delta" and [level.price for level in delta.bids] == [snapshot.best_bid]
        assert delta.bids[0].size == snapshot.bids[0].size + 1
        assert of(unchanged, BookEvent) == []
        assert api.requests[0].url.path == f"/v3/public/catalog/markets/{MID}/book"

    def test_new_prints_only(self):
        async def go():
            api = Public()
            stream = polled(api)
            await stream.watch_trades([SID])
            await stream._open()
            seeded = stream.handle(await stream.poll())
            new = {**TRADES["items"][0], "tradeId": "new-trade", "ts": TRADES["items"][0]["ts"] + 1}
            api.trades = {"items": [new, *TRADES["items"]]}
            later = stream.handle(await stream.poll())
            await stream.close()
            return seeded, later

        seeded, later = asyncio.run(go())
        assert of(seeded, TradeEvent) == []
        assert [e.id for e in of(later, TradeEvent)] == ["new-trade"]

    def test_status_changes(self):
        async def go():
            api = Public()
            stream = polled(api)
            stream.status_every = 1
            await stream.watch_market_status([SID])
            await stream._open()
            stream.handle(await stream.poll())
            api.market = {**MARKET, "status": "CLOSED"}
            events = stream.handle(await stream.poll())
            await stream.close()
            return events

        (event,) = of(asyncio.run(go()), MarketStatusEvent)
        assert (event.market_id, event.state, event.native) == (SID, "closed", "CLOSED")

    def test_a_throttled_read_keeps_the_last_answer(self):
        async def go():
            api = Public()
            stream = polled(api)
            await stream.watch_order_book([SID])
            await stream._open()
            stream.handle(await stream.poll())
            api.throttle = True
            message = await stream.poll()
            events = stream.handle(message)
            await stream.close()
            return message, events, stream

        message, events, stream = asyncio.run(go())
        assert message["throttled"] and events == [] and stream.book(SID).ready


# ---------------------------------------------------------------------------
# The websocket, with a key
# ---------------------------------------------------------------------------

def keyed(**kwargs) -> NovigMarketStream:
    stream = NovigMarketStream(credentials=credentials(), catalog=Catalog(), **kwargs)
    stream._ws = FakeSocket()
    return stream


def snapshot_message(book: dict | None = None, *, nonce: int = 1, lifecycle: str = "OPEN", form: str = "spec") -> str:
    orders = book or BOOK["orders"]
    if form == "docs":
        orders = {k: [{"order": o["orderId"], "price": o["price"], "qty": o["qty"]} for o in v] for k, v in orders.items()}
    return json.dumps({"ts": 1, "nonce": nonce, "subscribed": {"markets": {MID: "book"}, "events": {}, "private": []},
                       "snapshot": {MID: {"eventId": MARKET["eventId"], "book": {"seq": 100, "orders": orders},
                                          "lifecycle": {"seq": 3, "status": lifecycle}}}})


def delta(channel: str, seq: int, deltas: list) -> str:
    return json.dumps({"ts": 2, "delta": {MID: {"eventId": MARKET["eventId"], channel: {"seq": seq, "deltas": deltas}}}})


class TestWebsocket:
    def test_the_upgrade_is_signed(self):
        stream = keyed()
        assert not stream.polling and stream.url == "wss://api.paper.novig.com/v3/ws"
        headers = stream.headers()
        request = httpx.Request("GET", "https://api.paper.novig.com/v3/ws", headers=headers)
        assert verified(request) and headers["Novig-Key-Id"] == credentials().key_id

    def test_one_channel_per_market(self):
        async def go():
            stream = keyed()
            await stream.watch_ticker([SID])
            await stream.watch_order_book([SID])
            await stream.watch_trades([SID])
            return stream._ws.sent

        sent = asyncio.run(go())
        assert [s["subscribe"]["markets"] for s in sent] == [{MID: "bbo"}, {MID: "book"}]
        assert [s["nonce"] for s in sent] == [1, 2]

    def test_a_snapshot_then_per_order_changes(self):
        async def go():
            stream = keyed()
            await stream.watch_order_book([SID])
            stream._dispatch(snapshot_message())
            top = BOOK["orders"][YES][0]
            stream._dispatch(delta("book", 101, [
                {"kind": "update", "orderId": top["orderId"], "remaining": top["qty"] - 1000},
                {"kind": "add", "orderId": "new", "outcomeId": NO, "price": "0.300", "qty": 500},
            ]))
            return stream, drain(stream)

        stream, events = asyncio.run(go())
        snapshot, change = of(events, BookEvent)
        assert snapshot.kind == "snapshot" and snapshot.sequence == 100
        assert snapshot.best_bid == Decimal(BOOK["orders"][YES][0]["price"])
        assert change.kind == "delta" and change.sequence == 101
        bid = {level.price: level.size for level in change.bids}
        assert bid[snapshot.best_bid] == snapshot.bids[0].size - 10
        assert {level.price: level.size for level in change.asks} == {Decimal("0.700"): Decimal("5")}
        assert stream.book(SID).ready

    def test_the_documentations_field_names(self):
        async def go():
            stream = keyed()
            await stream.watch_order_book([SID])
            stream._dispatch(snapshot_message(form="docs"))
            first = BOOK["orders"][YES][0]
            stream._dispatch(delta("book", 101, [{"kind": "remove", "order": first["orderId"], "reason": "cancel"}]))
            return drain(stream)

        snapshot, change = of(asyncio.run(go()), BookEvent)
        assert snapshot.bids and change.kind == "delta"

    def test_fills_on_the_book_are_trades_when_both_are_watched(self):
        async def go():
            stream = keyed()
            await stream.watch_order_book([SID])
            await stream.watch_trades([SID])
            stream._dispatch(snapshot_message())
            top = BOOK["orders"][NO][0]
            stream._dispatch(delta("book", 101, [
                {"kind": "remove", "order": top["orderId"], "reason": "fill"},
                {"kind": "add", "order": top["orderId"], "outcome": NO, "price": top["price"], "qty": top["qty"] - 300},
            ]))
            return drain(stream), top

        events, top = asyncio.run(go())
        (trade,) = of(events, TradeEvent)
        assert trade.quantity == Decimal("3") and trade.taker_side == Side.BUY
        assert trade.price == 1 - Decimal(top["price"]) and trade.info["source"] == "book"

    def test_a_gap_asks_for_a_snapshot(self):
        async def go():
            stream = keyed()
            await stream.watch_order_book([SID])
            stream._dispatch(snapshot_message())
            stream._dispatch(delta("book", 105, [{"kind": "add", "orderId": "x", "outcomeId": YES, "price": "0.5", "qty": 1}]))
            await settle()
            return stream, drain(stream)

        stream, events = asyncio.run(go())
        assert any(isinstance(e, StreamStatusEvent) and e.state == "gap" for e in events)
        assert stream._ws.sent[-1]["snapshot"] == {"markets": {MID: "book"}}
        assert not stream.book(SID).ready

    def test_the_tape(self):
        async def go():
            stream = keyed()
            await stream.watch_trades([SID])
            stream._dispatch(json.dumps({"ts": 1, "nonce": 1, "snapshot": {MID: {"eventId": "e", "trades": {"seq": 9, "trades": []}}}}))
            stream._dispatch(delta("trades", 10, [TRADES["items"][0]]))
            stream._dispatch(delta("trades", 10, [TRADES["items"][0]]))  # a repeat is dropped
            return drain(stream)

        (trade,) = of(asyncio.run(go()), TradeEvent)
        assert trade.id == TRADES["items"][0]["tradeId"]

    def test_top_of_book_from_bbo(self):
        async def go():
            stream = keyed()
            await stream.watch_ticker([SID])
            stream._dispatch(json.dumps({"ts": 1, "nonce": 1, "snapshot": {MID: {"eventId": "e", "bbo": {
                "seq": 4, "bbo": {YES: {"price": "0.625", "qty": 1000}, NO: {"price": "0.370", "qty": 500}}}}}}))
            stream._dispatch(delta("bbo", 5, [{"outcomeId": NO}]))
            return drain(stream)

        first, second = of(asyncio.run(go()), QuoteEvent)
        assert (first.bid, first.ask, first.bid_size, first.ask_size) == (Decimal("0.625"), Decimal("0.630"), Decimal("10"), Decimal("5"))
        assert second.ask is None and second.bid == Decimal("0.625")

    def test_lifecycle(self):
        async def go():
            stream = keyed()
            await stream.watch_market_status([SID])
            stream._dispatch(json.dumps({"ts": 1, "snapshot": {MID: {"eventId": "e", "lifecycle": {"seq": 3, "status": "OPEN"}}}}))
            stream._dispatch(delta("lifecycle", 4, ["GOLIVE"]))
            stream._dispatch(delta("lifecycle", 5, [{"kind": "GRADE", "status": "SETTLED"}]))
            return drain(stream)

        opened, live, graded = of(asyncio.run(go()), MarketStatusEvent)
        assert opened.state == "open" and live.state == "updated" and live.info["voids_resting_orders"]
        assert (graded.state, graded.native) == ("settled", "GRADE")

    def test_a_refusal_is_reported(self):
        async def go():
            stream = keyed()
            stream._dispatch(json.dumps({"code": "SUBSCRIPTION_LIMIT_EXCEEDED", "message": "too many", "nonce": 3}))
            return drain(stream)

        (event,) = asyncio.run(go())
        assert event.state == "error" and "SUBSCRIPTION_LIMIT_EXCEEDED" in event.detail


# ---------------------------------------------------------------------------
# The account
# ---------------------------------------------------------------------------

class Trading:
    def __init__(self):
        self.asked: list[str] = []

    async def fetch_order(self, order_id):
        from synpath.trading.novig import order_of_row

        self.asked.append(order_id)
        return order_of_row({**ORDER_ROW, "orderId": order_id, "status": "OPEN", "remaining": 11000},
                            yes_outcome=YES)


def user(**kwargs) -> NovigUserStream:
    stream = NovigUserStream(credentials=credentials(), catalog=Catalog(), **kwargs)
    stream._ws = FakeSocket()
    return stream


OPEN = {k: v for k, v in ORDER_ROW.items() if k not in ("remaining", "status", "createdTs")}


class TestAccount:
    def test_it_needs_the_trading_key(self):
        with pytest.raises(AuthenticationError):
            NovigUserStream(credentials=credentials(key_id=None, private_key_pem=None))

    def test_subscribing(self):
        async def go():
            stream = user()
            await stream.watch_orders()
            await stream.watch_positions()
            return stream._ws.sent

        assert [s["subscribe"]["private"] for s in asyncio.run(go())] == [["orders"], ["orders", "positions"]]

    def test_orders_from_snapshot_to_fill(self):
        async def go():
            stream = user()
            await stream.watch_orders()
            stream._dispatch(json.dumps({"ts": 1, "nonce": 1, "subscribed": {"private": ["orders"]}, "snapshot": {},
                                         "orders": {"seq": 907, "open": [OPEN]}}))
            stream._dispatch(json.dumps({"ts": 2, "delta": {}, "orders": {"seq": 908, "deltas": [
                {"kind": "fill", "fillId": "f-1", "orderId": OPEN["orderId"], "outcomeId": NO, "price": "0.370",
                 "qty": 4000, "remaining": 7000, "taker": False}]}}))
            stream._dispatch(json.dumps({"ts": 3, "delta": {}, "orders": {"seq": 909, "deltas": [
                {"kind": "cancel", "orderId": OPEN["orderId"], "reason": "GO_LIVE"}]}}))
            await settle()
            return drain(stream)

        events = asyncio.run(go())
        orders, fills = of(events, OrderEvent), of(events, FillEvent)
        assert [o.native for o in orders] == ["snapshot", "fill", "cancel:GO_LIVE"]
        assert orders[0].order.side == Side.SELL and orders[0].order.remaining == Decimal("110")
        assert fills[0].fill.price == Decimal("0.630") and fills[0].fill.quantity == Decimal("40")
        assert (orders[1].order.filled, orders[1].order.remaining, orders[1].order.quantity) == (
            Decimal("40"), Decimal("70"), Decimal("110"))
        assert orders[2].order.status == OrderStatus.CANCELED and orders[2].order.info["reason"] == "GO_LIVE"

    def test_fills_before_open_and_a_full_fill(self):
        async def go():
            stream = user()
            stream._dispatch(json.dumps({"ts": 1, "delta": {}, "orders": {"seq": 1, "deltas": [
                {"kind": "open", **OPEN, "qty": 7000},
                {"kind": "fill", "fillId": "f-2", "orderId": OPEN["orderId"], "outcomeId": NO, "price": "0.375",
                 "qty": 7000, "remaining": 0, "taker": False}]}}))
            await settle()
            return drain(stream)

        orders = of(asyncio.run(go()), OrderEvent)
        assert orders[-1].order.status == OrderStatus.CLOSED and orders[-1].order.filled == Decimal("70")

    def test_fills_matched_on_entry_come_before_the_open(self):
        async def go():
            stream = user()
            stream._dispatch(json.dumps({"ts": 1, "delta": {}, "orders": {"seq": 1, "deltas": [
                {"kind": "fill", "fillId": "f-4", "orderId": OPEN["orderId"], "outcomeId": NO, "price": "0.370",
                 "qty": 4000, "remaining": 7000, "taker": True},
                {"kind": "open", **OPEN, "qty": 7000}]}}))
            await settle()
            return drain(stream)

        events = asyncio.run(go())
        assert [type(e).__name__ for e in events] == ["FillEvent", "OrderEvent", "OrderEvent"]
        rested = events[-1].order
        assert rested.status == OrderStatus.OPEN
        assert (rested.filled, rested.remaining, rested.quantity) == (Decimal("40"), Decimal("70"), Decimal("110"))

    def test_an_order_first_seen_in_a_fill_is_read(self):
        async def go():
            trading = Trading()
            stream = user(trading=trading)
            stream._dispatch(json.dumps({"ts": 1, "delta": {}, "orders": {"seq": 1, "deltas": [
                {"kind": "fill", "fillId": "f-3", "orderId": "placed-elsewhere", "outcomeId": NO, "price": "0.375",
                 "qty": 100, "remaining": 10900, "taker": True}]}}))
            await settle()
            return trading, drain(stream)

        trading, events = asyncio.run(go())
        assert trading.asked == ["placed-elsewhere"] and of(events, FillEvent)[0].fill.order_id == "placed-elsewhere"

    def test_a_heartbeat_ahead_is_a_gap(self):
        async def go():
            stream = user()
            stream._dispatch(json.dumps({"ts": 1, "snapshot": {}, "orders": {"seq": 907, "open": []}}))
            await settle()
            stream._dispatch(json.dumps({"heartbeat": {"orders": 911, "ts": 2}}))
            await settle()
            return stream, drain(stream)

        stream, events = asyncio.run(go())
        gap = [e for e in events if isinstance(e, StreamStatusEvent) and e.state == "gap"]
        assert gap and gap[0].reconcile_required and stream._ws.sent[-1]["snapshot"] == {"private": ["orders"]}

    def test_positions(self):
        async def go():
            stream = user()
            stream._dispatch(json.dumps({"ts": 1, "snapshot": {}, "positions": {"seq": 12, "positions": [
                {"marketId": MID, "outcomeId": YES, "qty": 11000, "cost": "68.75000"}]}}))
            stream._dispatch(json.dumps({"ts": 2, "delta": {}, "positions": {"seq": 13, "deltas": [
                {"marketId": MID, "outcomeId": YES, "qty": 0, "cost": "0.00000"}]}}))
            await settle()
            return drain(stream)

        held, closed = of(asyncio.run(go()), PositionEvent)
        assert held.position.side == PositionSide.LONG and held.position.contracts == Decimal("110")
        assert closed.position.side == PositionSide.FLAT and closed.position.contracts == 0
