"""Opinion's WebSocket streams, offline.

Message shapes are the venue's documented samples (developer guide, market
and user channels); books come from the REST samples recorded for the read
adapter. Connection handling runs against the scripted fake connection the
other stream tests use.
"""
from __future__ import annotations

import asyncio
import json
import threading
from decimal import Decimal

import pytest

from synpath.errors import AuthenticationError
from synpath.opinion import normalize_market, normalize_order_book
from synpath.trading.types import OrderStatus, OrderType, SettlementState, Side
from synpath.ws.base import BookEvent, FillEvent, OrderEvent, QuoteEvent, StreamStatusEvent, TradeEvent, VenueEvent
from synpath.ws.opinion import HEARTBEAT, OpinionMarketStream, OpinionUserStream, api_key_from

from conftest import load
from test_ws import Connector, FakeConnection, drain, of, wait_for

D = Decimal
pytestmark = pytest.mark.anyio
KEY = "test-key-0123"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeCatalog:
    """The read adapter, answering from recorded samples."""

    def __init__(self, markets, books, *, gate: threading.Event | None = None):
        self.markets = markets
        self.books = books
        self.gate = gate
        self.reads: list[tuple[str, str]] = []

    def fetch_market(self, native):
        return self.markets[native]

    def fetch_order_book(self, native, *, side="yes"):
        if self.gate is not None:
            self.gate.wait(5)
        self.reads.append((native, side))
        return self.books[(native, side)]


@pytest.fixture
def binary(opinion_market):
    return normalize_market(opinion_market)


@pytest.fixture
def option(opinion_child, opinion_categorical):
    return normalize_market(opinion_child, opinion_categorical)


@pytest.fixture
def catalog(binary, option):
    yes = load("opinion_book.json")["result"]
    no = load("opinion_book_no.json")["result"]
    books = {}
    for market in (binary, option):
        books[(market.venue_market_id, "yes")] = normalize_order_book(yes, market_id=market.id)
        books[(market.venue_market_id, "no")] = normalize_order_book(no, market_id=market.id, side="no")
    return FakeCatalog({"8453": binary, "5342": option}, books)


def depth(market, token, side, price, size):
    return {"marketId": int(market.venue_market_id), "tokenId": token, "outcomeSide": 1 if token == market.yes.venue_token_id else 2,
            "side": side, "price": price, "size": size, "msgType": "market.depth.diff"}


def market_stream(catalog, **kwargs):
    kwargs.setdefault("resync_interval", None)
    return OpinionMarketStream(api_key=KEY, catalog=catalog, **kwargs)


class TestKey:
    def test_the_key_is_required(self, monkeypatch, tmp_path):
        monkeypatch.delenv("OPINION_API_KEY", raising=False)
        monkeypatch.chdir(tmp_path)
        with pytest.raises(AuthenticationError, match="OPINION_API_KEY"):
            api_key_from(None)

    def test_the_key_comes_from_the_environment_or_dotenv(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("OPINION_API_KEY", "from-env")
        assert api_key_from(None) == "from-env"
        monkeypatch.delenv("OPINION_API_KEY")
        (tmp_path / ".env").write_text("OPINION_API_KEY=from-dotenv\n")
        assert api_key_from(None) == "from-dotenv"

    def test_the_key_is_in_the_url(self, catalog):
        assert market_stream(catalog).url == f"wss://ws.opinion.trade?apikey={KEY}"

    async def test_the_key_never_reaches_a_status_event(self, catalog):
        stream = market_stream(catalog, connect=Connector(OSError(f"cannot reach wss://ws.opinion.trade?apikey={KEY}")))
        stream.start()
        await wait_for(lambda: not stream._queue.empty())
        await stream.close()
        details = [e.detail for e in of(drain(stream), StreamStatusEvent)]
        assert details and all(KEY not in d for d in details)
        assert any("***" in d for d in details)


class TestBooks:
    async def test_subscribe_then_snapshot_both_sides(self, catalog, binary):
        conn = FakeConnection()
        stream = market_stream(catalog, connect=Connector(conn))
        await stream.watch_order_book([binary.id])
        assert catalog.reads == []                      # not connected: nothing to snapshot against
        stream.start()
        seen: list = []

        def snapshots() -> int:
            seen.extend(drain(stream))
            return len(of(seen, BookEvent))

        await wait_for(lambda: snapshots() >= 2)
        assert conn.sent[0] == {"action": "SUBSCRIBE", "channel": "market.depth.diff", "marketId": 8453}
        assert stream.book(binary.id).ready and stream.book(binary.id, "no").ready
        assert stream.book(binary.id).best_bid == D("0.66")
        assert stream.book(binary.id, "no").best_ask == D("0.34")
        await stream.close()

    async def test_changes_apply_as_the_level_size(self, catalog, binary):
        stream = market_stream(catalog)
        stream._ws = FakeConnection()
        await stream.watch_order_book([binary.id])
        drain(stream)
        yes = binary.yes.venue_token_id
        [event] = stream.handle(depth(binary, yes, "bids", "0.67", "40"))
        assert event.kind == "delta" and event.side == "yes"
        assert event.bids[0].price == D("0.67") and event.best_bid == D("0.67")
        stream.handle(depth(binary, yes, "bids", "0.67", "0"))
        assert stream.book(binary.id).best_bid == D("0.66")

    async def test_changes_during_the_snapshot_are_replayed(self, binary, catalog):
        gate = threading.Event()
        catalog.gate = gate
        stream = market_stream(catalog)
        stream._ws = FakeConnection()
        task = asyncio.get_running_loop().create_task(stream.watch_order_book([binary.id]))
        await wait_for(lambda: binary.yes.venue_token_id in stream._pending)
        stream.handle(depth(binary, binary.yes.venue_token_id, "asks", "0.70", "5"))
        gate.set()
        await task
        assert stream.book(binary.id).best_ask == D("0.70")
        snapshots = [e for e in of(drain(stream), BookEvent) if e.side == "yes"]
        assert snapshots[0].info["replayed"] == 1

    async def test_changes_before_any_snapshot_are_ignored(self, catalog, binary):
        stream = market_stream(catalog)
        await stream.watch_order_book([binary.id])
        assert stream.handle(depth(binary, binary.yes.venue_token_id, "bids", "0.5", "1")) == []

    async def test_a_reconnect_reads_the_books_again(self, catalog, binary):
        first, second = FakeConnection(), FakeConnection()
        stream = market_stream(catalog, connect=Connector(first, second), backoff_initial=0.001)
        await stream.watch_order_book([binary.id])
        stream.start()
        await wait_for(lambda: stream.book(binary.id).ready)
        first.feed(ConnectionError("dropped"))
        await wait_for(lambda: second.sent and stream.book(binary.id).ready and len(catalog.reads) >= 4)
        assert second.sent[0]["channel"] == "market.depth.diff"
        states = [e.state for e in of(drain(stream), StreamStatusEvent)]
        assert "resynced" in states
        await stream.close()

    async def test_the_timer_catches_a_book_that_drifted(self, catalog, binary):
        stream = market_stream(catalog)
        stream._ws = FakeConnection()
        await stream.watch_order_book([binary.id])
        drain(stream)
        stream.book(binary.id).bids[D("0.5")] = D("999")          # a change the feed never sent
        await stream._snapshot(stream.watched["8453"], check=True)
        events = drain(stream)
        assert [e.state for e in of(events, StreamStatusEvent)] == ["gap", "resynced"]
        assert D("0.5") not in stream.book(binary.id).bids
        await stream._snapshot(stream.watched["8453"], check=True)
        assert drain(stream) == []                                  # in agreement: silent

    async def test_unwatch_unsubscribes(self, catalog, binary):
        stream = market_stream(catalog)
        stream._ws = conn = FakeConnection()
        await stream.watch_order_book([binary.id])
        await stream.unwatch([binary.id])
        assert conn.sent[-1] == {"action": "UNSUBSCRIBE", "channel": "market.depth.diff", "marketId": 8453}
        assert stream.book(binary.id) is None


class TestTradesAndPrices:
    async def test_an_option_subscribes_by_topic_and_filters_its_siblings(self, catalog, option):
        stream = market_stream(catalog)
        stream._ws = conn = FakeConnection()
        await stream.watch_trades([option.id])
        assert conn.sent == [{"action": "SUBSCRIBE", "channel": "market.last.trade", "rootMarketId": 337}]
        sibling = {"marketId": 5343, "tokenId": "x", "side": "Buy", "outcomeSide": 1, "price": "0.4",
                   "shares": "1", "amount": "0.4", "msgType": "market.last.trade"}
        assert stream.handle(sibling) == []

    async def test_a_no_trade_is_reported_on_the_yes_leg(self, catalog, binary):
        stream = market_stream(catalog)
        await stream.watch_trades([binary.id])
        [trade] = stream.handle({"tokenId": binary.no.venue_token_id, "side": "Buy", "outcomeSide": 2, "price": "0.15",
                                 "shares": "10", "amount": "1.5", "marketId": 8453, "msgType": "market.last.trade"})
        assert isinstance(trade, TradeEvent)
        assert trade.price == D("0.85") and trade.taker_side == Side.SELL and trade.amount == D("10")

    async def test_a_split_is_not_a_trade(self, catalog, binary):
        stream = market_stream(catalog)
        await stream.watch_trades([binary.id])
        [event] = stream.handle({"side": "Split", "outcomeSide": 1, "price": "0.5", "shares": "10",
                                 "marketId": 8453, "msgType": "market.last.trade"})
        assert isinstance(event, VenueEvent) and event.name == "split"

    async def test_last_price_is_a_quote_on_its_side(self, catalog, binary):
        stream = market_stream(catalog)
        await stream.watch_ticker([binary.id])
        [quote] = stream.handle({"tokenId": binary.no.venue_token_id, "outcomeSide": 2, "price": "0.15",
                                 "marketId": 8453, "msgType": "market.last.price"})
        assert isinstance(quote, QuoteEvent) and quote.side == "no" and quote.last == D("0.15")

    async def test_venue_errors_are_reported_and_acks_are_not(self, catalog):
        stream = market_stream(catalog)
        assert stream.handle({"code": 400, "message": "invalid channel"}) == []
        assert stream.handle({"code": 0, "message": "ok"}) == []
        assert [e.state for e in of(drain(stream), StreamStatusEvent)] == ["error"]

    async def test_the_heartbeat_frame_is_sent(self, catalog):
        conn = FakeConnection()
        stream = market_stream(catalog, connect=Connector(conn), ping_interval=0.01)
        stream.start()
        await wait_for(lambda: HEARTBEAT in conn.sent or json.loads(HEARTBEAT) in conn.sent)
        await stream.close()


ORDER_UPDATE = {
    "orderUpdateType": "orderConfirm", "marketId": 2770, "rootMarketId": 122,
    "orderId": "a11ee07e-e22f-11f0-9714-0a58a9feac02", "side": 1, "outcomeSide": 1,
    "price": "0.150000000000000000", "shares": "66.66", "amount": "9.999000000000000000",
    "status": 1, "tradingMethod": 2, "quoteToken": "0x55d398326f99059fF775485246999027B3197955",
    "createdAt": 1766735464, "expiresAt": 0, "chainId": "56",
    "filledShares": "10.000000000000000000", "filledAmount": "1.500000000000000000",
    "msgType": "trade.order.update",
}

TRADE_RECORD = {
    "orderId": "3c7af25f-e21f-11f0-9714-0a58a9feac02", "tradeNo": "e1403840-e22f-11f0-83af-0a58a9feac02",
    "marketId": 2770, "rootMarketId": 122,
    "txHash": "0x272c8d9b8f90f50564173cf624c0ac5a371978b72bcd12604b26312a27e24195",
    "side": "Buy", "outcomeSide": 2, "price": "0.100000000000000000", "shares": "9.44444",
    "amount": "0.944444", "profit": "0.000000000000000000", "status": 2,
    "quoteToken": "0x55d398326f99059fF775485246999027B3197955", "quoteTokenUsdPrice": "1.000000000000000000",
    "usdAmount": "1000000.000000000000000000", "fee": "0.000000000000000000", "chainId": "56",
    "createdAt": 1766735571, "msgType": "trade.record.new",
}


class TestUser:
    async def test_orders_and_fills_subscribe_per_topic(self, catalog, option, binary):
        stream = OpinionUserStream(api_key=KEY, catalog=catalog)
        stream._ws = conn = FakeConnection()
        await stream.watch_orders([option.id, binary.id])
        assert conn.sent == [
            {"action": "SUBSCRIBE", "channel": "trade.order.update", "rootMarketId": 337},
            {"action": "SUBSCRIBE", "channel": "trade.record.new", "rootMarketId": 337},
            {"action": "SUBSCRIBE", "channel": "trade.order.update", "marketId": 8453},
            {"action": "SUBSCRIBE", "channel": "trade.record.new", "marketId": 8453},
        ]

    async def test_an_order_update_is_an_order_on_the_yes_leg(self, catalog):
        stream = OpinionUserStream(api_key=KEY, catalog=catalog)
        [event] = stream.handle(ORDER_UPDATE)
        assert isinstance(event, OrderEvent) and event.native == "orderConfirm"
        order = event.order
        assert order.market_id == "opinion:2770" and order.side == Side.BUY and order.price == D("0.15")
        assert order.type == OrderType.LIMIT and order.status == OrderStatus.OPEN
        assert (order.amount, order.filled, order.remaining) == (D("66.66"), D("10"), D("56.66"))
        assert order.created_at == 1766735464000 and order.expires_at is None

    async def test_a_no_order_is_a_sell_at_the_complement(self, catalog):
        stream = OpinionUserStream(api_key=KEY, catalog=catalog)
        [event] = stream.handle({**ORDER_UPDATE, "outcomeSide": 2, "status": 3})
        assert event.order.side == Side.SELL and event.order.price == D("0.85")
        assert event.order.status == OrderStatus.CANCELED and event.order.remaining == 0

    async def test_a_trade_record_is_a_confirmed_fill(self, catalog):
        stream = OpinionUserStream(api_key=KEY, catalog=catalog)
        [event] = stream.handle(TRADE_RECORD)
        fill = event.fill
        assert isinstance(event, FillEvent)
        assert fill.id == TRADE_RECORD["tradeNo"] and fill.order_id == TRADE_RECORD["orderId"]
        assert fill.side == Side.SELL and fill.price == D("0.9") and fill.amount == D("9.44444")
        assert fill.settlement == SettlementState.CONFIRMED and fill.fee == 0 and fill.fee_currency == "USDT"

    async def test_a_failed_trade_is_reported_failed(self, catalog):
        stream = OpinionUserStream(api_key=KEY, catalog=catalog)
        [event] = stream.handle({**TRADE_RECORD, "status": 6})
        assert event.fill.settlement == SettlementState.FAILED

    async def test_a_merge_is_not_a_fill(self, catalog):
        stream = OpinionUserStream(api_key=KEY, catalog=catalog)
        [event] = stream.handle({**TRADE_RECORD, "side": "Merge"})
        assert isinstance(event, VenueEvent) and event.name == "merge"

    async def test_a_reconnect_asks_for_reconciliation(self, catalog):
        first, second = FakeConnection(), FakeConnection()
        stream = OpinionUserStream(api_key=KEY, catalog=catalog, connect=Connector(first, second), backoff_initial=0.001)
        stream.start()
        await wait_for(lambda: stream._ws is first)
        first.feed(ConnectionError("dropped"))
        await wait_for(lambda: stream._ws is second)
        connected = [e for e in of(drain(stream), StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, True]
        await stream.close()
