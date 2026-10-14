"""SX Bet's market stream: Centrifugo's protocol, spoken against a fake socket.
Book bodies are recorded from the live venue's REST snapshot (the channel
carries the same shape); protocol frames follow Centrifugo's JSON protocol."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from synpath import AuthenticationError, SxBetMarketStream
from synpath.trading.types import Side
from synpath.ws.base import BookEvent, MarketStatusEvent, QuoteEvent, StreamStatusEvent, TradeEvent
from synpath.ws.sx_bet import levels_of, status_event, trade_event

from conftest import load

BOOK = load("sx_bet_book.json")["data"]
HASH = BOOK["marketHash"]
MARKET = f"sx_bet:{HASH}"
TRADE = load("sx_bet_trades.json")["data"]["trades"][0]


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
    """Answers the REST snapshot a book is seeded from."""

    def __init__(self, payload):
        self.payload = payload
        self.asked = 0

    def _get(self, path, params=None):
        self.asked += 1
        return self.payload


def stream_with(**kwargs) -> SxBetMarketStream:
    return SxBetMarketStream(token="tok", catalog=FakeCatalog(kwargs.pop("snapshot", BOOK)), **kwargs)


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)
    await asyncio.sleep(0.01)
    for _ in range(5):
        await asyncio.sleep(0)


def push(channel, data):
    return json.dumps({"push": {"channel": channel, "pub": {"data": data, "tags": {"messageId": "m"}}}})


async def connected(stream):
    """A stream past `connect`: the token sent, the server's reply read."""
    stream._ws = FakeSocket()
    await stream.on_connect()
    connect = stream._ws.sent[-1]
    assert connect["connect"]["token"] == "tok"
    stream._dispatch(json.dumps({"id": connect["id"], "connect": {"client": "c", "ping": 25, "pong": True}}))
    await settle()
    return stream


def confirm_subscriptions(stream):
    for command in [c for c in stream._ws.sent if "subscribe" in c]:
        stream._dispatch(json.dumps({"id": command["id"], "subscribe": {}}))


class TestRecords:
    def test_levels_read_on_yes(self):
        level = {"percentageOdds": "40000000000000000000", "size": "2000000"}
        assert levels_of([level], outcome_one=True) == [(Decimal("0.4"), Decimal("5"))]
        assert levels_of([level], outcome_one=False) == [(Decimal("0.6"), Decimal("5"))]

    def test_a_tape_bet(self):
        event = trade_event(TRADE)
        assert event.market_id == f"sx_bet:{TRADE['marketHash'].lower()}"
        assert event.taker_side == (Side.BUY if TRADE["isBettingOutcomeOne"] else Side.SELL)
        assert event.quantity == Decimal(TRADE["totalReturn"]) / 1_000_000 and 0 < event.price < 1
        assert trade_event({**TRADE, "isParlay": True}) is None

    def test_a_settlement(self):
        event = status_event({"marketHash": HASH.upper().replace("0X", "0x"), "status": "INACTIVE",
                              "reportedDate": 1791900000, "outcome": 2})
        assert (event.market_id, event.state, event.result) == (MARKET, "settled", "no")


class TestProtocol:
    def test_every_channel_needs_a_key(self):
        with pytest.raises(AuthenticationError):
            SxBetMarketStream()

    def test_connect_then_subscribe_then_seed(self):
        async def go():
            stream = stream_with()
            await stream.watch_order_book([MARKET])
            assert stream._ws is None  # nothing sent before connecting
            await connected(stream)
            subscribe = [c for c in stream._ws.sent if "subscribe" in c]
            assert [c["subscribe"]["channel"] for c in subscribe] == [f"orderbook_v3:{HASH}"]
            confirm_subscriptions(stream)
            await settle()
            return stream, drain(stream)

        stream, events = asyncio.run(go())
        books = [e for e in events if isinstance(e, BookEvent)]
        assert books and books[-1].market_id == MARKET and books[-1].best_bid is not None
        assert stream.catalog.asked == 1
        assert any(isinstance(e, StreamStatusEvent) and e.state == "subscribed" for e in events)

    def test_pings_are_answered(self):
        async def go():
            stream = await connected(stream_with())
            before = len(stream._ws.sent)
            stream._dispatch("{}")
            await settle()
            return stream._ws.sent[before:]

        assert asyncio.run(go()) == [{}]

    def test_several_messages_in_one_frame(self):
        async def go():
            stream = await connected(stream_with())
            await stream.watch_ticker([MARKET])
            body = {**BOOK, "version": "9" * 30}
            stream._dispatch(push(f"orderbook_v3:{HASH}", body) + "\n" + "{}")
            await settle()
            return stream._ws.sent, drain(stream)

        sent, events = asyncio.run(go())
        assert {} in sent and any(isinstance(e, QuoteEvent) for e in events)

    def test_an_older_book_is_dropped(self):
        async def go():
            stream = await connected(stream_with())
            await stream.watch_order_book([MARKET])
            drain(stream)
            stream._dispatch(push(f"orderbook_v3:{HASH}", {**BOOK, "version": "2"}))
            stream._dispatch(push(f"orderbook_v3:{HASH}", {**BOOK, "version": "1", "outcomeOne": []}))
            return drain(stream)

        books = [e for e in asyncio.run(go()) if isinstance(e, BookEvent)]
        assert len(books) == 1

    def test_a_refused_subscription_is_reported(self):
        async def go():
            stream = await connected(stream_with())
            await stream.watch_trades()
            command = [c for c in stream._ws.sent if "subscribe" in c][-1]
            stream._dispatch(json.dumps({"id": command["id"], "error": {"code": 103, "message": "permission denied"}}))
            return stream, drain(stream)

        stream, events = asyncio.run(go())
        assert any(isinstance(e, StreamStatusEvent) and e.state == "error" and "103" in e.detail for e in events)
        assert "recent_trades_v3:global" not in stream._live

    def test_the_token_is_renewed_before_it_expires(self):
        async def go():
            stream = stream_with()
            stream._ws = FakeSocket()
            await stream.on_connect()
            connect = stream._ws.sent[-1]
            stream._dispatch(json.dumps({"id": connect["id"], "connect": {"expires": True, "ttl": 1}}))
            await asyncio.sleep(5.2)
            sent = stream._ws.sent
            stream.on_disconnect()
            return sent

        assert any("refresh" in c and c["refresh"]["token"] == "tok" for c in asyncio.run(go()))


class TestGlobalChannels:
    def test_trades_are_filtered_to_the_markets_asked_for(self):
        other = {**TRADE, "marketHash": "0x" + "1" * 64}
        mine = {**TRADE, "marketHash": HASH}

        async def go():
            stream = await connected(stream_with())
            await stream.watch_trades([MARKET])
            stream._dispatch(push("recent_trades_v3:global", {"trade": other}))
            stream._dispatch(push("recent_trades_v3:global", {"trade": mine}))
            return drain(stream)

        trades = [e for e in asyncio.run(go()) if isinstance(e, TradeEvent)]
        assert [t.market_id for t in trades] == [MARKET]

    def test_every_market_change_when_none_are_named(self):
        rows = [{"marketHash": HASH, "status": "INACTIVE"}, {"marketHash": "0x" + "2" * 64, "status": "ACTIVE"}]

        async def go():
            stream = await connected(stream_with())
            await stream.watch_market_status()
            stream._dispatch(push("markets:global", rows))
            return drain(stream)

        states = [(e.market_id, e.state) for e in asyncio.run(go()) if isinstance(e, MarketStatusEvent)]
        assert states == [(MARKET, "closed"), ("sx_bet:0x" + "2" * 64, "open")]

    def test_unwatching_unsubscribes(self):
        async def go():
            stream = await connected(stream_with())
            await stream.watch_order_book([MARKET])
            await stream.unwatch([MARKET])
            return stream._ws.sent

        assert any(c.get("unsubscribe", {}).get("channel") == f"orderbook_v3:{HASH}" for c in asyncio.run(go()))

    def test_a_disconnect_invalidates_books_and_resubscribes_on_reconnect(self):
        async def go():
            stream = await connected(stream_with())
            await stream.watch_order_book([MARKET])
            stream.on_disconnect()
            assert not stream.books[HASH].ready
            await connected(stream)
            return [c["subscribe"]["channel"] for c in stream._ws.sent if "subscribe" in c]

        assert asyncio.run(go()) == [f"orderbook_v3:{HASH}"]
