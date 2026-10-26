"""Myriad's streams: Centrifugo's protocol, spoken against a fake socket.
Protocol and book frames are recorded from the live venue; the trade, order
and position bodies follow the venue's API reference."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from synpath import BadRequest, MyriadMarketStream, MyriadUserStream
from synpath.trading.types import OrderStatus, Side
from synpath.ws.base import BookEvent, FillEvent, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, TradeEvent
from synpath.ws.myriad import book_levels, status_event, trade_event

from conftest import load

FRAMES = load("myriad_ws.json")
PUSHES = [json.loads(f) for f in FRAMES if f.startswith('{"push"')]
DELTAS = [p for p in PUSHES if p["push"]["channel"] == "orderbook:56:3723"]
PRICES = next(p for p in PUSHES if p["push"]["channel"] == "prices:56:3723")
MARKET = "myriad:56-3723"
WALLET = "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
TRADE = {
    "marketId": 3723, "networkId": 56, "txHash": "0xt", "blockNumber": 1, "ts": 1719835200123,
    "taker": {"trader": "0xtaker", "outcome": 1, "side": "buy", "orderHash": "0xo1", "totalAmount": "2000000000000000000",
              "averagePrice": "600000000000000000", "averagePriceAfterFees": "610000000000000000",
              "totalFees": {"total": "10000000000000000"}},
    "makers": [{"trader": WALLET, "role": "maker", "outcome": 0, "side": "buy", "orderHash": "0xo2",
                "price": "400000000000000000", "priceAfterFees": "400000000000000000", "amount": "2000000000000000000",
                "fees": {"total": "0"}}],
}


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


class FakeSocket:
    def __init__(self):
        self.sent: list = []

    async def send(self, text):
        self.sent.append(json.loads(text))


class FakeCatalog:
    """Answers the REST snapshots a book is seeded from, one per outcome."""

    def __init__(self, books):
        self.books = books
        self.asked: list = []

    def _get(self, path, params=None):
        self.asked.append((path, params))
        return self.books[params["outcome"]]


SNAPSHOTS = {
    0: {"bids": [["320000000000000000", "35714285714285710000"], ["310000000000000000", "1000000000000000000"]],
        "asks": [["360000000000000000", "17857142857142854000"]]},
    1: {"bids": [["640000000000000000", "17857142857142854000"]],
        "asks": [["680000000000000000", "35714285714285710000"], ["690000000000000000", "1000000000000000000"]]},
}


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)
    await asyncio.sleep(0.01)
    for _ in range(5):
        await asyncio.sleep(0)


async def connected(stream):
    """A stream past `connect`: the anonymous command sent, the recorded reply read."""
    stream._ws = FakeSocket()
    await stream.on_connect()
    command = stream._ws.sent[-1]
    assert command["connect"] == {}
    stream._dispatch(FRAMES[0].replace('"id":1', f'"id":{command["id"]}'))
    await settle()
    return stream


def confirm_subscriptions(stream):
    for command in [c for c in stream._ws.sent if "subscribe" in c]:
        stream._dispatch(json.dumps({"id": command["id"], "subscribe": {}}))


class TestRecords:
    def test_book_levels(self):
        assert book_levels([["510000000000000000", "2000000000000000000"]]) == [(Decimal("0.51"), Decimal("2"))]

    def test_a_match_is_the_takers_print_on_yes(self):
        event = trade_event(TRADE)
        assert (event.price, event.quantity, event.taker_side) == (Decimal("0.4"), Decimal("2"), Side.SELL)
        assert event.market_id == MARKET and event.id == "0xt"

    def test_market_events(self):
        assert status_event({"type": "resolved", "networkId": 56, "marketId": 42}).state == "settled"
        assert status_event({"type": "created", "networkId": 56, "marketId": 42}).state == "created"

    def test_an_amm_market_or_a_slug_is_refused(self):
        stream = MyriadMarketStream()
        with pytest.raises(BadRequest, match="AMM"):
            asyncio.run(stream.watch_order_book(["myriad:56-amm-120473"]))
        with pytest.raises(BadRequest, match="slug"):
            asyncio.run(stream.watch_order_book(["myriad:patriots-vs-bears-who-wins"]))


class TestMarketStream:
    def test_anonymous_connect_then_one_subscribe_per_channel(self):
        async def go():
            stream = await connected(MyriadMarketStream(catalog=FakeCatalog(SNAPSHOTS)))
            await stream.watch_order_book([MARKET])
            await stream.watch_ticker([MARKET])
            await stream.watch_trades([MARKET])
            return [c["subscribe"]["channel"] for c in stream._ws.sent if "subscribe" in c]

        assert run(go()) == ["orderbook:56:3723", "prices:56:3723", "trades:56:3723"]

    def test_books_seed_from_rest_and_replay_early_deltas(self):
        async def go():
            catalog = FakeCatalog(SNAPSHOTS)
            stream = await connected(MyriadMarketStream(catalog=catalog))
            await stream.watch_order_book([MARKET])
            command = next(c for c in stream._ws.sent if "subscribe" in c)
            stream._dispatch(json.dumps({"id": command["id"], "subscribe": {}}))
            # A delta in flight before the snapshot lands is held, then replayed.
            stream._dispatch(json.dumps(DELTAS[0]))
            held = drain(stream)
            await settle()
            return stream, catalog, held, drain(stream)

        stream, catalog, held, events = run(go())
        assert not [e for e in held if isinstance(e, BookEvent)], "nothing is applied before the snapshot"
        assert [p["outcome"] for _, p in catalog.asked] == [0, 1]
        snapshots = [e for e in events if isinstance(e, BookEvent)]
        assert [(e.side, e.kind) for e in snapshots] == [("yes", "snapshot"), ("no", "snapshot")]
        assert snapshots[0].info["replayed"] == 1
        yes, no = stream.book(MARKET), stream.book(MARKET, "no")
        for change in DELTAS[0]["push"]["pub"]["data"]["changes"]:
            book = yes if change["outcome"] == 0 else no
            levels = book.bids if change["side"] == "bid" else book.asks
            price = (Decimal(change["price"]) / Decimal(10) ** 18).quantize(Decimal("0.000001"))
            if change["amount"] == "0":
                assert price not in levels
            else:
                assert levels[price] == Decimal(change["amount"]) / Decimal(10) ** 18

    def test_deltas_after_the_snapshot_change_both_books(self):
        async def go():
            stream = await connected(MyriadMarketStream(catalog=FakeCatalog(SNAPSHOTS)))
            await stream.watch_order_book([MARKET])
            confirm_subscriptions(stream)
            await settle()
            drain(stream)
            for delta in DELTAS[1:]:
                stream._dispatch(json.dumps(delta))
            return stream, drain(stream)

        stream, events = run(go())
        books = [e for e in events if isinstance(e, BookEvent)]
        assert books and all(e.kind == "delta" for e in books) and {e.side for e in books} == {"yes", "no"}
        yes, no = stream.book(MARKET), stream.book(MARKET, "no")
        assert yes.best_ask is not None and no.best_bid is not None
        assert yes.best_ask + no.best_bid == Decimal("1"), "the venue's two books stay each other's mirror"

    def test_a_reconnect_invalidates_and_reseeds(self):
        async def go():
            catalog = FakeCatalog(SNAPSHOTS)
            stream = await connected(MyriadMarketStream(catalog=catalog))
            await stream.watch_order_book([MARKET])
            confirm_subscriptions(stream)
            await settle()
            stream.on_disconnect()
            ready = stream.book(MARKET).ready
            await connected(stream)
            confirm_subscriptions(stream)
            await settle()
            return ready, stream, catalog, drain(stream)

        ready, stream, catalog, events = run(go())
        assert not ready and stream.book(MARKET).ready and len(catalog.asked) == 4
        assert any(getattr(e, "state", None) == "resynced" for e in events)

    def test_prices_are_quotes_per_outcome(self):
        async def go():
            stream = await connected(MyriadMarketStream())
            await stream.watch_ticker([MARKET])
            stream._dispatch(json.dumps(PRICES))
            return drain(stream)

        quotes = [e for e in run(go()) if isinstance(e, QuoteEvent)]
        data = PRICES["push"]["pub"]["data"]["outcomes"]
        assert [q.side for q in quotes] == ["yes", "no"]
        assert quotes[0].bid == Decimal(data[0]["bestBid"]) / Decimal(10) ** 18 and quotes[0].last is not None

    def test_trades_and_market_changes(self):
        async def go():
            stream = await connected(MyriadMarketStream())
            await stream.watch_trades([MARKET])
            await stream.watch_market_status([MARKET])
            stream._dispatch(json.dumps({"push": {"channel": "trades:56:3723", "pub": {"data": TRADE}}}))
            stream._dispatch(json.dumps({"push": {"channel": "markets:56", "pub": {
                "data": {"type": "resolved", "networkId": 56, "marketId": 3723}}}}))
            stream._dispatch(json.dumps({"push": {"channel": "markets:56", "pub": {
                "data": {"type": "resolved", "networkId": 56, "marketId": 1}}}}))
            return drain(stream)

        events = run(go())
        assert [type(e) for e in events if isinstance(e, (TradeEvent, MarketStatusEvent))] == [TradeEvent, MarketStatusEvent]

    def test_pings_are_answered(self):
        async def go():
            stream = await connected(MyriadMarketStream())
            stream._dispatch("{}")
            await settle()
            return stream._ws.sent[-1]

        assert run(go()) == {}


class TestUserStream:
    def test_account_channels_carry_a_token_and_fills_do_not(self):
        asked = []

        async def token(channel):
            asked.append(channel)
            return f"t:{channel}"

        async def go():
            stream = await connected(MyriadUserStream(address=WALLET, token=token))
            await stream.watch_orders()
            await stream.watch_my_trades()
            await stream.watch_positions()
            return [c["subscribe"] for c in stream._ws.sent if "subscribe" in c]

        wallet = WALLET.lower()
        assert run(go()) == [
            {"channel": f"orders:56:{wallet}", "token": f"t:orders:56:{wallet}"},
            {"channel": f"trades:56:*:{wallet}"},
            {"channel": f"positions:56:{wallet}", "token": f"t:positions:56:{wallet}"},
        ]
        assert asked == [f"orders:56:{wallet}", f"positions:56:{wallet}"]

    def test_orders_fills_and_positions(self):
        async def go():
            stream = await connected(MyriadUserStream(address=WALLET, token=lambda channel: "t"))
            wallet = WALLET.lower()
            stream._dispatch(json.dumps({"push": {"channel": f"orders:56:{wallet}", "pub": {"data": {
                "type": "FILLED", "orderHash": "0xo2", "networkId": 56, "marketId": 3723, "trader": WALLET,
                "side": "buy", "outcome": 0, "price": "400000000000000000", "amount": "2000000000000000000",
                "filledAmount": "2000000000000000000", "status": "filled", "timeInForce": "GTC"}}}}))
            stream._dispatch(json.dumps({"push": {"channel": f"trades:56:*:{wallet}", "pub": {"data": TRADE}}}))
            for outcome, balance in ((0, "3000000000000000000"), (1, "1000000000000000000")):
                stream._dispatch(json.dumps({"push": {"channel": f"positions:56:{wallet}", "pub": {"data": {
                    "networkId": 56, "marketId": 3723, "trader": WALLET, "outcome": outcome, "delta": "+1",
                    "balance": balance, "averageEntryPrice": "400000000000000000", "reason": "fill", "ts": 1}}}}))
            return drain(stream)

        events = run(go())
        order = next(e.order for e in events if isinstance(e, OrderEvent))
        assert order.status == OrderStatus.CLOSED and order.filled == Decimal("2") and order.market_id == MARKET
        fill = next(e.fill for e in events if isinstance(e, FillEvent))
        assert fill.side == Side.BUY and fill.price == Decimal("0.4") and fill.order_id == "0xo2"
        first, second = [e.position for e in events if isinstance(e, PositionEvent)]
        assert first.inventory_yes == Decimal("3") and first.inventory_no is None
        assert second.contracts == Decimal("2") and second.inventory_no == Decimal("1")

    def test_an_account_channel_without_a_token_source_is_reported(self):
        async def go():
            stream = await connected(MyriadUserStream(address=WALLET))
            await stream.watch_orders()
            return drain(stream), [c for c in stream._ws.sent if "subscribe" in c]

        events, sent = run(go())
        assert not sent and any(getattr(e, "state", None) == "error" for e in events)


def run(coro):
    return asyncio.run(coro)
