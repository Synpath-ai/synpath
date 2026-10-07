"""Limitless's streams. Market frames are recorded from the live venue; account
frames follow the venue's documented examples (they need an API token)."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from synpath import AuthenticationError, LimitlessMarketStream, LimitlessUserStream
from synpath.trading.limitless import fill_of_event, order_of_event
from synpath.trading.types import Liquidity, OrderStatus, OrderType, SettlementState, Side
from synpath.ws.base import BookEvent, FillEvent, MarketStatusEvent, OrderEvent, QuoteEvent, StreamStatusEvent
from synpath.ws.limitless import auth_headers

from conftest import load

WS = load("limitless_ws.json")
SLUG = WS["slug"]
MARKET = f"limitless:{SLUG}"
OPEN, JOINED, *_, BOOK = WS["frames"]
YES, NO = "111", "222"


def frame(name, data=None):
    return "42/markets," + json.dumps([name] if data is None else [name, data])


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


class FakeSocket:
    def __init__(self):
        self.sent: list[str] = []

    async def send(self, text):
        self.sent.append(text)


def run(stream, *frames, then=None):
    """Feed frames through a fake socket inside a loop, and return what was sent."""
    async def go():
        stream._ws = FakeSocket()
        for raw in frames:
            stream._dispatch(raw)
            await asyncio.sleep(0)
        if then is not None:
            await then
        for _ in range(3):
            await asyncio.sleep(0)
        return stream._ws.sent

    return asyncio.run(go())


ORDER = {
    "source": "OME", "type": "PLACEMENT", "eventId": 1234567, "orderId": "550e8400", "clientOrderId": "c-1",
    "userId": 42, "marketId": "17", "token": YES, "side": "BUY", "price": 0.53, "remainingSize": 100,
    "timestamp": "2026-04-20T10:15:30.000Z", "occurredAt": "2026-04-20T10:15:30.000Z",
}
MINED = {
    "source": "SETTLEMENT", "type": "MINED", "eventId": "settlement:1b3a:550e8400", "tradeEventId": "1b3a",
    "orderId": "550e8400", "clientOrderId": "c-1", "takerOrderId": "550e8400", "marketSlug": SLUG,
    "tokenId": NO, "side": "BUY", "price": "0.47", "amountContracts": "25", "amountCollateral": "11.75",
    "feeAmountContracts": "0.75", "txHash": "0xabc", "timestamp": "2026-04-20T10:15:40.000Z",
    "matchedAt": "2026-04-20T10:15:40.000Z",
}


class TestRecords:
    def test_a_resting_order_on_yes(self):
        order = order_of_event(ORDER, market_id=MARKET, outcome="yes")
        assert (order.id, order.client_order_id, order.side, order.price) == ("550e8400", "c-1", Side.BUY, Decimal("0.53"))
        assert order.status == OrderStatus.OPEN and order.remaining == Decimal("100") and order.type == OrderType.LIMIT

    def test_a_cancellation_and_an_immediate_order(self):
        assert order_of_event({**ORDER, "type": "CANCELLATION"}, market_id=MARKET, outcome="yes").status == OrderStatus.CANCELED
        killed = order_of_event({**ORDER, "type": "EXECUTION", "status": "KILLED", "remainingSize": "50000000"},
                                market_id=MARKET, outcome="no")
        assert killed.status == OrderStatus.CANCELED and killed.amount == Decimal("50") and killed.side == Side.SELL
        filled = order_of_event({**ORDER, "type": "EXECUTION", "status": "FILLED", "remainingSize": "0"}, market_id=MARKET, outcome="yes")
        assert filled.status == OrderStatus.CLOSED and filled.type == OrderType.MARKET

    def test_a_taker_fill_on_no_reads_on_yes(self):
        fill = fill_of_event(MINED, market_id=MARKET, outcome="no")
        assert fill.side == Side.SELL and fill.price == Decimal("0.53") and fill.amount == Decimal("25")
        assert fill.settlement == SettlementState.CONFIRMED and fill.liquidity == Liquidity.TAKER
        assert fill.fee == Decimal("0.75") and fill.fee_currency == "shares" and fill.id == "1b3a:550e8400"

    def test_a_fill_moves_from_matched_to_mined_under_one_id_and_makers_pay_nothing(self):
        matched = fill_of_event({**MINED, "type": "MATCHED"}, market_id=MARKET, outcome="no")
        failed = fill_of_event({**MINED, "type": "FAILED"}, market_id=MARKET, outcome="no")
        assert matched.id == failed.id and (matched.settlement, failed.settlement) == (SettlementState.MATCHED, SettlementState.FAILED)
        maker = fill_of_event({**MINED, "takerOrderId": "someone-else", "feeAmountContracts": "0.3"}, market_id=MARKET, outcome="no")
        assert maker.liquidity == Liquidity.MAKER and maker.fee == 0

    def test_an_order_event_is_not_a_fill(self):
        assert fill_of_event(ORDER, market_id=MARKET, outcome="yes") is None


class TestSocketIO:
    def test_open_joins_the_namespace_and_pings_are_answered(self):
        stream = LimitlessMarketStream()
        assert run(stream, OPEN, "2") == ["40/markets,", "3"]

    def test_joining_sends_the_whole_set_of_subscriptions(self):
        stream = LimitlessMarketStream()
        stream.books[SLUG] = stream.books.get(SLUG) or __import__("synpath").LocalBook()
        stream.tickers.add("another-market")
        sent = run(stream, JOINED)
        assert stream.joined
        name, data = json.loads(sent[0][len("42/markets,"):])
        assert name == "subscribe_market_prices" and data == {"marketSlugs": sorted([SLUG, "another-market"])}

    def test_nothing_is_sent_before_the_namespace_is_joined(self):
        stream = LimitlessMarketStream()
        assert run(stream, then=stream.watch_order_book([MARKET])) == []

    def test_a_refused_connection_is_reported(self):
        stream = LimitlessMarketStream()
        run(stream, '44/markets,{"message":"unauthorized"}')
        assert [e.state for e in drain(stream) if isinstance(e, StreamStatusEvent)] == ["error"]


class TestMarketStream:
    def book_events(self, *frames):
        stream = LimitlessMarketStream()
        run(stream, JOINED, then=stream.watch_order_book([MARKET]))
        run(stream, *frames)
        return stream, [e for e in drain(stream) if isinstance(e, BookEvent)]

    def test_every_book_frame_is_a_snapshot_in_shares(self):
        stream, [event] = self.book_events(BOOK)
        payload = json.loads(BOOK[len("42/markets,"):])[1]
        assert event.kind == "snapshot" and event.market_id == MARKET and event.sequence == payload["version"]
        top = max(payload["orderbook"]["bids"], key=lambda level: level["price"])
        assert stream.book(MARKET).best_bid == Decimal(str(top["price"]))
        assert event.bids[0].size == Decimal(top["size"]) / 1_000_000
        assert stream.book(MARKET, "no").best_ask == 1 - Decimal(str(top["price"]))

    def test_an_older_version_is_dropped(self):
        payload = json.loads(BOOK[len("42/markets,"):])[1]
        older = frame("orderbookUpdate", {**payload, "version": payload["version"] - 1})
        _, events = self.book_events(BOOK, older)
        assert len(events) == 1

    def test_the_ticker_is_the_top_of_book(self):
        stream = LimitlessMarketStream()
        run(stream, JOINED, then=stream.watch_ticker([MARKET]))
        run(stream, BOOK)
        [quote] = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        payload = json.loads(BOOK[len("42/markets,"):])[1]
        assert quote.bid == Decimal(str(max(level["price"] for level in payload["orderbook"]["bids"])))

    def test_a_resolution(self):
        stream = LimitlessMarketStream()
        run(stream, frame("marketResolved", {"slug": SLUG, "type": "CLOB", "winningOutcome": "YES", "winningIndex": 0,
                                             "resolutionDate": "2026-04-05T14:00:00.000Z"}))
        [event] = [e for e in drain(stream) if isinstance(e, MarketStatusEvent)]
        assert (event.market_id, event.state, event.result) == (MARKET, "settled", "yes")

    def test_a_reconnect_marks_books_not_ready(self):
        stream, _ = self.book_events(BOOK)
        stream.on_disconnect()
        assert not stream.book(MARKET).ready and not stream.joined


class TestUserStream:
    def test_a_token_is_required(self):
        with pytest.raises(AuthenticationError):
            LimitlessUserStream(token_id="", secret="")

    def test_the_handshake_is_signed(self):
        secret = base64.b64encode(b"secret-bytes").decode()
        now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        headers = auth_headers("token-1", secret, now=now)
        message = f"{headers['lmts-timestamp']}\nGET\n/socket.io/?EIO=4&transport=websocket\n"
        expected = base64.b64encode(hmac.new(b"secret-bytes", message.encode(), hashlib.sha256).digest()).decode()
        assert headers == {"lmts-api-key": "token-1", "lmts-timestamp": "2026-10-07T12:00:00.000Z", "lmts-signature": expected}

    def stream(self, catalog=None):
        stream = LimitlessUserStream(token_id="t", secret=base64.b64encode(b"s").decode(), catalog=catalog)
        stream.learn(MARKET, YES, NO)
        return stream

    def test_known_tokens_become_orders_and_fills(self):
        stream = self.stream()
        run(stream, frame("orderEvent", ORDER), frame("orderEvent", MINED))
        events = drain(stream)
        assert [type(e) for e in events] == [OrderEvent, FillEvent]
        assert events[0].order.market_id == MARKET and events[1].fill.side == Side.SELL

    def test_joining_subscribes_once_watching(self):
        stream = self.stream()
        assert run(stream, JOINED) == []
        stream.joined = False
        assert run(stream, then=stream.watch_orders()) == []
        assert run(stream, JOINED) == ['42/markets,["subscribe_order_events"]']

    def test_an_unknown_token_is_placed_from_the_catalog(self):
        class Catalog:
            walks = 0

            def token_index(self):
                Catalog.walks += 1
                return {"999": ("limitless:other-market", "no")}

        stream = LimitlessUserStream(token_id="t", secret=base64.b64encode(b"s").decode(), catalog=Catalog())
        run(stream, frame("orderEvent", {**ORDER, "token": "999"}), frame("orderEvent", {**ORDER, "token": "999", "orderId": "2"}))
        orders = [e.order for e in drain(stream) if isinstance(e, OrderEvent)]
        assert [o.id for o in orders] == ["550e8400", "2"] and all(o.market_id == "limitless:other-market" for o in orders)
        assert orders[0].side == Side.SELL and Catalog.walks == 1

    def test_a_settlement_frame_names_its_market(self):
        stream = LimitlessUserStream(token_id="t", secret=base64.b64encode(b"s").decode(), catalog=object())
        matched = {**MINED, "type": "MATCHED", "token": "NO", "tokenId": "333"}
        run(stream, frame("orderEvent", matched))
        [event] = drain(stream)
        assert event.fill.market_id == MARKET and event.fill.settlement == SettlementState.MATCHED
