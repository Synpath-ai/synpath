"""The streaming layer, offline.

Kalshi and Polymarket parsing is checked against real transcripts recorded
on 2026-09-17 (`tests/samples/ws/`): Kalshi's from the demo environment while
an order rested, was cancelled and another filled (account ids replaced by
zeros), Polymarket's from the public market channel. The Kalshi transcript
ends with a snapshot requested mid-stream, so the local book built from the
136 real deltas before it can be compared with the venue's own. Polymarket US shapes follow
its documentation and SDK.

Connection handling runs against a scripted fake connection and against a
real local WebSocket server that drops the client mid-stream.
"""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

from synpath.base import CAPABILITIES
from synpath.trading.credentials import KalshiCredentials, PolymarketUSCredentials
from synpath.trading.types import Liquidity, OrderStatus, PositionSide, SettlementState, Side
from synpath.ws.base import (
    BalanceEvent, BookEvent, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, Stream,
    StreamStatusEvent, TradeEvent, VenueEvent,
)
from synpath.ws.kalshi import KalshiStream
from synpath.ws.polymarket import PolymarketMarketStream, PolymarketUserStream
from synpath.ws.polymarket_us import PolymarketUSMarketStream, PolymarketUSPrivateStream

D = Decimal
pytestmark = pytest.mark.anyio
SAMPLES = Path(__file__).parent / "samples" / "ws"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def transcript(name: str) -> list[dict]:
    return [json.loads(line) for line in (SAMPLES / name).read_text().splitlines()]


def drain(stream: Stream) -> list:
    events = []
    while not stream._queue.empty():
        events.append(stream._queue.get_nowait())
    return events


def of(events: list, kind: type) -> list:
    return [e for e in events if isinstance(e, kind)]


class FakeConnection:
    """A WebSocket the test scripts: frames to deliver, frames sent, a close."""

    def __init__(self, incoming: list | None = None, *, close_after: bool = False):
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.sent: list = []
        self.closed = False
        for item in incoming or []:
            self.inbox.put_nowait(item)
        if close_after:
            self.inbox.put_nowait(ConnectionError("server closed"))

    async def recv(self):
        item = await self.inbox.get()
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, str) else json.dumps(item)

    async def send(self, frame):
        self.sent.append(json.loads(frame) if frame.startswith(("{", "[")) else frame)

    async def close(self):
        # As the real client does: closing unblocks a waiting `recv`.
        self.closed = True
        self.inbox.put_nowait(ConnectionError("closed"))

    def feed(self, item):
        self.inbox.put_nowait(item)


class Connector:
    def __init__(self, *connections):
        self.connections = list(connections)
        self.headers: list[dict] = []
        self.opened: list[FakeConnection] = []

    async def __call__(self, url, additional_headers=None):
        self.headers.append(dict(additional_headers or {}))
        if not self.connections:
            await asyncio.sleep(3600)
        item = self.connections.pop(0)
        if isinstance(item, Exception):
            raise item
        self.opened.append(item)
        return item


async def _collect_all(stream):
    return [event async for event in stream]


async def wait_for(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


# ---------------------------------------------------------------------------
# Local book
# ---------------------------------------------------------------------------

class TestLocalBook:
    def test_levels_best_and_removal(self):
        book = LocalBook()
        assert not book.ready and book.best_bid is None
        book.replace([(D("0.40"), D("10")), (D("0.42"), D("5")), (D("0.30"), D("0"))], [(D("0.45"), D("7"))])
        assert book.ready and book.best_bid == D("0.42") and book.best_ask == D("0.45")
        assert D("0.30") not in book.bids
        assert book.add("bid", D("0.42"), D("-5")) == 0 and book.best_bid == D("0.40")
        assert book.set("ask", D("0.44"), D("3")) == D("3") and book.best_ask == D("0.44")
        bids, asks = book.levels()
        assert [b.price for b in bids] == [D("0.40")] and [a.price for a in asks] == [D("0.44"), D("0.45")]

    def test_mirrored_view(self):
        book = LocalBook()
        book.replace([(D("0.40"), D("10"))], [(D("0.45"), D("7"))])
        no = book.mirrored()
        assert no.best_bid == D("0.55") and no.best_ask == D("0.60") and no.bids[D("0.55")] == D("7")


# ---------------------------------------------------------------------------
# Connection handling
# ---------------------------------------------------------------------------

class Echo(Stream):
    venue = "test"
    name = "echo"

    def __init__(self, **kwargs):
        super().__init__("wss://test", **kwargs)
        self.subscriptions = ["a"]

    async def on_connect(self):
        await self.send({"subscribe": self.subscriptions})

    def handle(self, message):
        if isinstance(message, dict) and "value" in message:
            return [VenueEvent(venue="test", name="value", payload=message)]
        if isinstance(message, dict) and "boom" in message:
            raise ValueError("cannot read this")
        return []


class TestStreamLifecycle:
    async def test_subscribes_on_connect_and_delivers_events(self):
        conn = FakeConnection([{"value": 1}, {"value": 2}])
        stream = Echo(connect=Connector(conn), backoff_initial=0.001)
        stream.start()
        await wait_for(lambda: stream.stats.messages == 2)
        events = drain(stream)
        assert conn.sent == [{"subscribe": ["a"]}]
        assert isinstance(events[0], StreamStatusEvent) and events[0].state == "connected"
        assert [e.payload["value"] for e in of(events, VenueEvent)] == [1, 2]
        await stream.close()

    async def test_reconnects_and_resubscribes(self):
        first = FakeConnection([{"value": 1}], close_after=True)
        second = FakeConnection([{"value": 2}])
        stream = Echo(connect=Connector(first, second), backoff_initial=0.001, backoff_max=0.002)
        stream.start()
        await wait_for(lambda: stream.stats.connects == 2 and stream.stats.messages == 2)
        events = drain(stream)
        states = [(e.state, e.detail) for e in of(events, StreamStatusEvent)]
        assert states[0] == ("connected", "") and states[1][0] == "disconnected" and states[2] == ("connected", "reconnected")
        assert second.sent == [{"subscribe": ["a"]}] and first.closed
        assert not of(events, StreamStatusEvent)[2].reconcile_required  # nothing private
        await stream.close()

    async def test_a_private_stream_asks_for_reconciliation_after_a_reconnect(self):
        class Private(Echo):
            private = True
        stream = Private(connect=Connector(FakeConnection([], close_after=True), FakeConnection([])), backoff_initial=0.001, backoff_max=0.002)
        stream.start()
        await wait_for(lambda: stream.stats.connects == 2)
        connected = [e for e in of(drain(stream), StreamStatusEvent) if e.state == "connected"]
        assert [e.reconcile_required for e in connected] == [False, True]
        await stream.close()

    async def test_a_silent_connection_is_replaced(self):
        stream = Echo(connect=Connector(FakeConnection([]), FakeConnection([{"value": 9}])), idle_timeout=0.05, backoff_initial=0.001, backoff_max=0.002)
        stream.start()
        await wait_for(lambda: stream.stats.messages == 1)
        disconnected = [e for e in of(drain(stream), StreamStatusEvent) if e.state == "disconnected"]
        assert "no message for 0.05s" in disconnected[0].detail
        await stream.close()

    async def test_a_connection_that_died_while_the_machine_slept_is_replaced(self):
        """A suspended laptop freezes the event loop's timers, so the idle
        timeout counts no time while the connection dies. The wall clock
        still moves, and the stream drops the connection when it wakes."""
        first, second = FakeConnection([]), FakeConnection([{"value": 9}])
        stream = Echo(connect=Connector(first, second), idle_timeout=4.0, backoff_initial=0.001, backoff_max=0.002)
        stream.start()
        await wait_for(lambda: stream.stats.connects == 1)
        stream._last_seen -= 600_000  # ten minutes of sleep
        await wait_for(lambda: stream.stats.messages == 1, timeout=4.0)
        disconnected = [e for e in of(drain(stream), StreamStatusEvent) if e.state == "disconnected"]
        assert "wall clock" in disconnected[0].detail and "60" in disconnected[0].detail
        assert first.closed
        await stream.close()

    async def test_failed_connects_back_off_then_succeed(self):
        stream = Echo(connect=Connector(OSError("refused"), OSError("refused"), FakeConnection([{"value": 1}])), backoff_initial=0.001, backoff_max=0.002)
        stream.start()
        await wait_for(lambda: stream.stats.messages == 1)
        states = [e.state for e in of(drain(stream), StreamStatusEvent)]
        assert states[:3] == ["connect_failed", "connect_failed", "connected"]
        await stream.close()

    async def test_an_unreadable_message_does_not_end_the_stream(self):
        conn = FakeConnection([{"boom": 1}, {"value": 2}, "not json"])
        stream = Echo(connect=Connector(conn))
        stream.start()
        await wait_for(lambda: stream.stats.messages == 3)
        events = drain(stream)
        assert [e.state for e in of(events, StreamStatusEvent)] == ["connected", "error"]
        assert of(events, VenueEvent)[0].payload == {"value": 2} and stream.stats.errors == 1
        await stream.close()

    async def test_iteration_ends_on_close(self):
        stream = Echo(connect=Connector(FakeConnection([{"value": 1}])))
        seen = []

        async def consume():
            async for event in stream:
                seen.append(event)

        task = asyncio.create_task(consume())
        await wait_for(lambda: len(seen) == 2)
        await stream.close()
        await asyncio.wait_for(task, 1)

    async def test_a_closed_stream_does_not_reconnect_when_iterated(self):
        connector = Connector(FakeConnection([{"value": 1}]), FakeConnection([{"value": 2}]))
        stream = Echo(connect=connector)
        stream.start()
        await wait_for(lambda: stream.stats.messages == 1)
        await stream.close()
        drained = await asyncio.wait_for(_collect_all(stream), 1)   # ends: the close marker is queued
        assert [e.payload["value"] for e in drained if isinstance(e, VenueEvent)] == [1]
        await asyncio.sleep(0.01)
        assert len(connector.opened) == 1 and stream._task is None

    async def test_a_real_server_dropping_the_client(self):
        websockets = pytest.importorskip("websockets")
        received: list = []
        connections = 0

        async def handler(ws):
            nonlocal connections
            connections += 1
            received.append(json.loads(await ws.recv()))
            await ws.send(json.dumps({"value": connections}))
            if connections == 1:
                await ws.close()
            else:
                await asyncio.sleep(5)

        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]

            class Local(Echo):
                def __init__(self, **kwargs):
                    Stream.__init__(self, f"ws://127.0.0.1:{port}", **kwargs)
                    self.subscriptions = ["a"]

            stream = Local(backoff_initial=0.001, backoff_max=0.002)
            values = []
            async for event in stream:
                if isinstance(event, VenueEvent):
                    values.append(event.payload["value"])
                if values == [1, 2]:
                    break
            await stream.close()
        assert received == [{"subscribe": ["a"]}, {"subscribe": ["a"]}]
        assert stream.stats.connects == 2 and stream.stats.disconnects >= 1


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def kalshi(rsa_key):
    pem = rsa_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return KalshiStream(KalshiCredentials(key_id="key-1", private_key_pem=pem, env="demo"))


def replay_kalshi(stream: KalshiStream, rows: list[dict], *, skip=lambda row: False) -> None:
    from synpath.ws.kalshi import Channel

    for row in rows:
        if row["dir"] == "out" and row["msg"].get("cmd") == "subscribe":
            params = row["msg"]["params"]
            channel = Channel(params["channels"][0], set(params.get("market_tickers") or []) or None)
            stream.channels[channel.name] = channel
            stream._by_command[row["msg"]["id"]] = channel
            if channel.name == "orderbook_delta":
                for ticker in channel.tickers or []:
                    stream.books.setdefault(ticker, LocalBook())
    for row in rows:
        if row["dir"] == "in" and not skip(row):
            stream._dispatch(json.dumps(row["msg"]))


class TestKalshi:
    def test_capabilities(self):
        assert set(KalshiStream.has) == set(CAPABILITIES)
        assert KalshiStream.has["watch_order_book"] and KalshiStream.has["watch_positions"]
        assert KalshiStream.has["watch_balance"] is False

    def test_silence_is_not_death_on_kalshi(self, kalshi, us_creds):
        # Found in the live soak: a quiet demo market went 60 s without data on a
        # healthy connection. Kalshi's keepalive is protocol pings; the others' is data.
        assert kalshi.idle_timeout is None
        assert PolymarketMarketStream().idle_timeout == 60.0
        assert PolymarketUSPrivateStream(us_creds).idle_timeout == 60.0

    def test_handshake_is_signed_like_rest(self, kalshi, rsa_key):
        headers = kalshi.headers()
        assert headers["KALSHI-ACCESS-KEY"] == "key-1"
        rsa_key.public_key().verify(
            base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
            f"{headers['KALSHI-ACCESS-TIMESTAMP']}GET/trade-api/ws/v2".encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256(),
        )
        assert kalshi.url == "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

    async def test_real_transcript_book_matches_the_venue_snapshot(self, kalshi):
        rows = transcript("kalshi_demo_transcript.jsonl")
        cut = next(i for i, r in enumerate(rows) if r["dir"] == "in" and r["msg"].get("id") == 9)
        resnapshot = rows[cut]["msg"]
        replay_kalshi(kalshi, rows[:cut])        # everything the stream saw before the venue's snapshot
        ticker = resnapshot["msg"]["market_ticker"]
        book = kalshi.books[ticker]
        venue_yes = {D(p): D(s) for p, s in resnapshot["msg"]["yes_dollars_fp"]}
        venue_no_as_asks = {D("1") - D(p): D(s) for p, s in resnapshot["msg"]["no_dollars_fp"]}
        assert book.ready and book.bids == venue_yes and book.asks == venue_no_as_asks
        events = drain(kalshi)
        assert len([e for e in of(events, BookEvent) if e.kind == "delta"]) == 136   # seq 2..137
        assert [e.state for e in of(events, StreamStatusEvent) if e.state != "subscribed"] == []

    async def test_real_transcript_events(self, kalshi):
        replay_kalshi(kalshi, transcript("kalshi_demo_transcript.jsonl"))
        events = drain(kalshi)
        placed, cancelled, filled = of(events, OrderEvent)
        assert placed.order.status == OrderStatus.OPEN and cancelled.order.status == OrderStatus.CANCELED
        assert placed.order.id == cancelled.order.id and cancelled.order.remaining == 0
        assert filled.order.status == OrderStatus.CLOSED and filled.order.filled == D("1.00")
        (fill,) = of(events, FillEvent)
        assert fill.fill.order_id == filled.order.id and fill.fill.price == D("0.9800")
        assert fill.fill.liquidity == Liquidity.TAKER and fill.fill.timestamp == 1789659332660
        assert fill.fill.client_order_id == filled.order.client_order_id
        (position,) = of(events, PositionEvent)
        assert position.position.side == PositionSide.LONG and position.position.contracts == D("1.00")
        assert position.position.entry_price == D("0.9800")
        (trade,) = of(events, TradeEvent)
        assert trade.taker_side == Side.BUY and trade.id == fill.fill.id
        groups = of(events, VenueEvent)
        assert [g.payload["event_type"] for g in groups] == ["created", "triggered", "reset", "deleted"]
        quote = of(events, QuoteEvent)[0]
        assert quote.bid == D("0.0100") and quote.ask == D("0.9800") and quote.side == "yes" and quote.market_id.startswith("kalshi:")
        first_book = of(events, BookEvent)[0]
        assert first_book.kind == "snapshot" and first_book.best_bid == D("0.0100") and first_book.best_ask == D("0.9800")

    async def test_a_missing_delta_is_a_gap_and_the_book_waits_for_a_snapshot(self, kalshi):
        rows = transcript("kalshi_demo_transcript.jsonl")
        conn = FakeConnection()
        kalshi._ws = conn
        dropped = next(r for r in rows if r["dir"] == "in" and r["msg"].get("type") == "orderbook_delta" and r["msg"]["seq"] == 10)
        replay_kalshi(kalshi, rows, skip=lambda row: row is dropped)
        await asyncio.sleep(0)
        events = drain(kalshi)
        gap = [e for e in of(events, StreamStatusEvent) if e.state == "gap"]
        assert len(gap) == 1 and "expected seq 10, got 11" in gap[0].detail
        assert {"id": conn.sent[0]["id"], "cmd": "update_subscription", "params": {"sids": [1], "market_tickers": [dropped["msg"]["msg"]["market_ticker"]], "action": "get_snapshot"}} == conn.sent[0]
        deltas_after_gap = [e for e in of(events, BookEvent) if e.kind == "delta" and e.sequence and e.sequence > 10]
        assert all(e.sequence > 138 for e in deltas_after_gap)  # nothing applied until the snapshot at 138
        assert [e.state for e in of(events, StreamStatusEvent)][-1] == "resynced"
        assert kalshi.books[dropped["msg"]["msg"]["market_ticker"]].ready

    async def test_subscribing_before_and_after_the_server_names_the_channel(self, kalshi):
        conn = FakeConnection()
        kalshi._ws = conn
        await kalshi.watch_order_book(["A"])
        await kalshi.watch_order_book(["B"])       # before the sid is known
        assert conn.sent == [{"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": ["A"]}}]
        kalshi._dispatch(json.dumps({"type": "subscribed", "id": 1, "msg": {"channel": "orderbook_delta", "sid": 7}}))
        await asyncio.sleep(0)
        assert conn.sent[1]["params"] == {"sids": [7], "market_tickers": ["B"], "action": "add_markets"}
        await kalshi.watch_order_book(["C", "A"])
        assert conn.sent[2]["params"] == {"sids": [7], "market_tickers": ["C"], "action": "add_markets"}
        await kalshi.watch_orders()
        assert conn.sent[3]["params"] == {"channels": ["user_orders"]} and kalshi.private

    async def test_reconnect_subscribes_everything_again_with_every_market(self, kalshi):
        conn = FakeConnection()
        kalshi._ws = conn
        await kalshi.watch_ticker(["A"])
        await kalshi.watch_ticker(["B"])
        kalshi.on_disconnect()
        conn.sent.clear()
        await kalshi.on_connect()
        assert conn.sent == [{"id": 2, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": ["A", "B"]}}]

    async def test_buffer_overflow_restarts_the_subscription(self, kalshi):
        conn = FakeConnection()
        kalshi._ws = conn
        await kalshi.watch_order_book(["A"])
        kalshi._dispatch(json.dumps({"type": "subscribed", "id": 1, "msg": {"channel": "orderbook_delta", "sid": 3}}))
        kalshi._dispatch(json.dumps({"type": "orderbook_snapshot", "sid": 3, "seq": 1, "msg": {"market_ticker": "A", "yes_dollars_fp": [["0.4000", "1.00"]]}}))
        kalshi._dispatch(json.dumps({"type": "error", "sid": 3, "msg": {"code": 25, "msg": "Subscription buffer overflow"}}))
        await asyncio.sleep(0)
        assert [f["cmd"] for f in conn.sent[1:]] == ["unsubscribe", "subscribe"]
        assert not kalshi.books["A"].ready
        assert [e.state for e in of(drain(kalshi), StreamStatusEvent)][-1] == "gap"

    def test_lifecycle(self, kalshi):
        events = kalshi.handle({"type": "market_lifecycle_v2", "sid": 1, "seq": 1, "msg": {"market_ticker": "A", "event_type": "deactivated", "is_deactivated": True}})
        events += kalshi.handle({"type": "market_lifecycle_v2", "sid": 1, "seq": 2, "msg": {"market_ticker": "A", "event_type": "activated", "is_deactivated": False}})
        events += kalshi.handle({"type": "market_lifecycle_v2", "sid": 1, "seq": 3, "msg": {"market_ticker": "A", "event_type": "determined", "result": "yes", "determination_ts": 1789660000}})
        assert [(e.state, e.result) for e in events] == [("paused", None), ("open", None), ("determined", "yes")]
        assert events[2].timestamp == 1789660000000

    def test_no_leg_view(self, kalshi):
        kalshi.handle({"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": {
            "market_ticker": "A", "yes_dollars_fp": [["0.4000", "5.00"]], "no_dollars_fp": [["0.5500", "3.00"]]}})
        assert kalshi.book("kalshi:A").best_ask == D("0.4500")
        no = kalshi.book("A", side="no")
        assert no.best_bid == D("0.5500") and no.best_ask == D("0.6000")


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def replay_poly(stream: PolymarketMarketStream, rows: list[dict], *, skip=lambda i, row: False) -> None:
    for row in rows:
        if row["dir"] == "out" and "assets_ids" in row["msg"]:
            for token in row["msg"]["assets_ids"]:
                stream.tokens.add(token)
                stream.books.setdefault(token, LocalBook())
    for i, row in enumerate(rows):
        if row["dir"] == "in" and not skip(i, row):
            msg = row["msg"]
            stream._dispatch(msg if isinstance(msg, str) else json.dumps(msg))


class TestPolymarketMarket:
    def test_capabilities(self):
        assert PolymarketMarketStream.has["watch_order_book"] and PolymarketMarketStream.has["watch_market_status"] == "partial"

    async def test_real_transcript_has_no_false_gaps(self):
        stream = PolymarketMarketStream()
        stream._ws = FakeConnection()
        replay_poly(stream, transcript("polymarket_market_transcript.jsonl"))
        for token in list(stream._unverified):
            assert stream._verify(token)
        events = drain(stream)
        assert [e for e in of(events, StreamStatusEvent)] == []
        assert len([e for e in of(events, BookEvent) if e.kind == "snapshot"]) == 10
        assert len([e for e in of(events, BookEvent) if e.kind == "delta"]) == 242
        assert len(of(events, TradeEvent)) == 2 and stream._ws.sent == []

    async def test_one_trade_split_across_two_messages_is_not_a_gap(self):
        rows = transcript("polymarket_market_transcript.jsonl")
        stream = PolymarketMarketStream()
        stream._ws = FakeConnection()
        replay_poly(stream, rows[:57])   # through the two same-stamp changes and the next stamp
        assert not [e for e in of(drain(stream), StreamStatusEvent) if e.state == "gap"]

    async def test_a_missed_change_is_caught_by_the_venue_top_of_book(self):
        rows = transcript("polymarket_market_transcript.jsonl")
        token = "33202224610280334678651164710853322679834574319557821785419566678346528011717"

        def drop_removal(i, row):
            msg = row["msg"]
            return isinstance(msg, dict) and msg.get("event_type") == "price_change" and any(
                c["asset_id"] == token and c["price"] == "0.12" and c["size"] == "0" for c in msg["price_changes"]
            )

        def no_resend(i, row):
            msg = row["msg"]
            return drop_removal(i, row) or (isinstance(msg, dict) and msg.get("event_type") == "book" and msg.get("asset_id") == token and i > 40)

        stream = PolymarketMarketStream()
        conn = FakeConnection()
        stream._ws = conn
        replay_poly(stream, rows[:60], skip=no_resend)
        await asyncio.sleep(0)
        gaps = [e for e in of(drain(stream), StreamStatusEvent) if e.state == "gap"]
        assert len(gaps) == 1 and gaps[0].key == token and "venue 0.11/0.13" in gaps[0].detail
        assert conn.sent == [{"assets_ids": [token], "operation": "unsubscribe"}, {"assets_ids": [token], "operation": "subscribe"}]
        assert not stream.books[token].ready

    async def test_subscription_frames(self):
        from synpath.trading.polymarket import MarketTokens

        stream = PolymarketMarketStream()
        # Two markets the catalog already knows: no Gamma round trip needed.
        stream.catalog.remember(MarketTokens(gamma_id="10", condition_id="0xa", yes_token="1", no_token="2"))
        stream.catalog.remember(MarketTokens(gamma_id="20", condition_id="0xb", yes_token="3", no_token="4"))
        conn = FakeConnection()
        await stream.watch_order_book(["polymarket:10"])
        assert conn.sent == []                    # recorded, not yet connected
        stream._ws = conn
        await stream.on_connect()
        await stream.watch_trades(["20"])
        await stream.unwatch(["10"])
        assert conn.sent == [
            {"assets_ids": ["1", "2"], "type": "market", "custom_feature_enabled": True},
            {"assets_ids": ["3", "4"], "operation": "subscribe"},
            {"assets_ids": ["1", "2"], "operation": "unsubscribe"},
        ]
        assert stream.book("polymarket:20", side="no") is stream.books["4"]
        assert stream.app_ping == "PING"

    def test_lifecycle_and_tick_size(self):
        stream = PolymarketMarketStream()
        events = stream.handle([
            {"event_type": "market_resolved", "market": "0xm", "winning_asset_id": "1", "timestamp": "1789659000000"},
            {"event_type": "tick_size_change", "market": "0xm", "asset_id": "1", "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1789659000000"},
            {"event_type": "best_bid_ask", "market": "0xm", "asset_id": "1", "best_bid": "0.4", "best_ask": "0.41", "timestamp": "1789659000000"},
            "PONG",
        ])
        status, tick, quote = events
        assert isinstance(status, MarketStatusEvent) and status.state == "determined" and status.result == "1"
        assert isinstance(tick, VenueEvent) and tick.name == "tick_size_change"
        assert isinstance(quote, QuoteEvent) and quote.ask == D("0.41")


DOC_USER_ORDER = {
    "event_type": "order", "id": "0xorder", "owner": "api-key", "market": "0xm", "asset_id": "123", "side": "BUY",
    "order_owner": "api-key", "original_size": "10", "size_matched": "0", "price": "0.52", "associate_trades": None,
    "outcome": "Yes", "type": "PLACEMENT", "created_at": "1782753357", "expiration": "0", "order_type": "GTC",
    "status": "LIVE", "maker_address": "0xwallet", "timestamp": "1782753357257",
}
DOC_USER_TRADE = {
    "event_type": "trade", "type": "TRADE", "id": "trade-1", "taker_order_id": "0xtaker", "market": "0xm", "asset_id": "123",
    "side": "BUY", "size": "10", "fee_rate_bps": "0", "price": "0.52", "status": "MATCHED", "match_time": "1782753357",
    "last_update": "1782753357", "outcome": "Yes", "owner": "someone", "trade_owner": "someone", "maker_address": "0xother",
    "transaction_hash": None, "bucket_index": 0, "trader_side": "MAKER", "timestamp": "1782753357257",
    "maker_orders": [{"order_id": "0xorder", "owner": "api-key", "maker_address": "0xwallet", "matched_amount": "4",
                      "price": "0.52", "fee_rate_bps": "0", "asset_id": "123", "outcome": "Yes", "outcome_index": 0, "side": "SELL"}],
}


class TestPolymarketUser:
    async def test_auth_frame_and_market_filter(self):
        from synpath.trading.polymarket import MarketTokens

        stream = PolymarketUserStream(api_key="api-key", api_secret="s3cret", api_passphrase="pp")
        stream.catalog.remember(MarketTokens(gamma_id="5", condition_id="0xm", yes_token="1", no_token="2"))
        conn = FakeConnection()
        stream._ws = conn
        await stream.watch_orders(["polymarket:5"])
        await stream.on_connect()
        assert conn.sent[-1] == {"auth": {"apiKey": "api-key", "secret": "s3cret", "passphrase": "pp"}, "type": "user", "markets": ["0xm"]}
        assert conn.sent[0] == {"operation": "subscribe", "markets": ["0xm"]}
        assert stream.private and PolymarketUserStream.has["watch_my_trades"]

    async def test_credentials_can_come_from_the_trading_adapter(self):
        class Adapter:
            wallet, signer = "0xwallet", type("S", (), {"address": "0xsigner"})()
            _api_key = _api_secret = _api_passphrase = None

            async def ensure_api_credentials(self):
                self._api_key, self._api_secret, self._api_passphrase = "derived", "sec", "pass"

        stream = PolymarketUserStream(trading=Adapter())
        stream._ws = FakeConnection()
        await stream.on_connect()
        assert stream._ws.sent[0]["auth"]["apiKey"] == "derived" and "0xwallet" in stream.wallets

    def test_orders_and_settling_fills(self):
        stream = PolymarketUserStream(api_key="api-key", api_secret="s", api_passphrase="p", wallets={"0xwallet"})
        placed, cancelled = stream.handle([DOC_USER_ORDER, {**DOC_USER_ORDER, "type": "CANCELLATION"}])
        assert placed.order.status == OrderStatus.OPEN and placed.native == "PLACEMENT"
        assert cancelled.order.status == OrderStatus.CANCELED and cancelled.order.remaining == 0
        (matched,) = stream.handle(DOC_USER_TRADE)
        (confirmed,) = stream.handle({**DOC_USER_TRADE, "status": "CONFIRMED"})
        assert matched.fill.id == confirmed.fill.id == "trade-1:0xorder"
        assert matched.fill.settlement == SettlementState.MATCHED and confirmed.fill.settlement == SettlementState.CONFIRMED
        assert matched.fill.amount == D("4") and matched.fill.side == Side.SELL


# ---------------------------------------------------------------------------
# Polymarket US
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ed_key():
    return ed25519.Ed25519PrivateKey.generate()


@pytest.fixture
def us_creds(ed_key):
    raw = ed_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
    return PolymarketUSCredentials(key_id="kid", secret_key=base64.b64encode(raw).decode())


SLUG = "tec-mlb-nlchamp-2026-09-27-atl"


class TestPolymarketUS:
    def test_capabilities(self):
        assert PolymarketUSPrivateStream.has["watch_balance"] and PolymarketUSMarketStream.has["watch_order_book"]

    def test_handshake_signs_the_endpoint_path(self, us_creds, ed_key):
        stream = PolymarketUSPrivateStream(us_creds)
        headers = stream.headers()
        assert stream.url == "wss://api.polymarket.us/v1/ws/private"
        ed_key.public_key().verify(base64.b64decode(headers["X-PM-Signature"]), f"{headers['X-PM-Timestamp']}GET/v1/ws/private".encode())

    async def test_subscriptions_split_at_one_hundred_and_come_back_after_reconnect(self, us_creds):
        stream = PolymarketUSMarketStream(us_creds)
        conn = FakeConnection()
        stream._ws = conn
        await stream.watch_order_book([f"m{i}" for i in range(250)])
        assert [len(f["subscribe"]["marketSlugs"]) for f in conn.sent] == [100, 100, 50]
        assert {f["subscribe"]["subscriptionType"] for f in conn.sent} == {"SUBSCRIPTION_TYPE_MARKET_DATA"}
        conn.sent.clear()
        await stream.on_connect()
        assert [len(f["subscribe"]["marketSlugs"]) for f in conn.sent] == [100, 100, 50]

    def test_market_data_is_a_snapshot_and_state_changes_once(self, us_creds):
        stream = PolymarketUSMarketStream(us_creds)
        message = {"requestId": "md-1", "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA", "marketData": {
            "marketSlug": SLUG,
            "bids": [{"px": {"value": "0.555", "currency": "USD"}, "qty": "0.50"}, {"px": {"value": "0.550", "currency": "USD"}, "qty": "2.50"}],
            "offers": [{"px": {"value": "0.560", "currency": "USD"}, "qty": "0.80"}],
            "state": "MARKET_STATE_OPEN", "stats": {"lastTradePx": {"value": "0.55"}}, "transactTime": "2026-09-17T12:00:00Z"}}
        book, status = stream.handle(message)
        assert isinstance(book, BookEvent) and book.kind == "snapshot" and book.best_bid == D("0.555") and book.best_ask == D("0.560")
        assert isinstance(status, MarketStatusEvent) and status.state == "open"
        (again,) = stream.handle(message)
        assert isinstance(again, BookEvent)
        _, halted = stream.handle({**message, "marketData": {**message["marketData"], "state": "MARKET_STATE_HALTED"}})
        assert halted.state == "paused" and halted.native == "MARKET_STATE_HALTED"
        assert stream.book(SLUG, side="no").best_bid == D("0.440")

    def test_lite_trades_heartbeats_and_errors(self, us_creds):
        stream = PolymarketUSMarketStream(us_creds)
        (quote,) = stream.handle({"marketDataLite": {"marketSlug": SLUG, "bestBid": {"value": "0.54"}, "bestAsk": {"value": "0.56"}, "lastTradePx": {"value": "0.55"}}})
        (trade,) = stream.handle({"trade": {"marketSlug": SLUG, "price": {"value": "0.555"}, "quantity": {"value": "0.50"},
                                            "tradeTime": "2026-09-17T12:00:00Z", "maker": {"side": "ORDER_SIDE_BUY"}, "taker": {"side": "ORDER_SIDE_SELL"}}})
        assert quote.bid == D("0.54") and trade.amount == D("0.50") and trade.taker_side == Side.SELL
        assert stream.handle({"heartbeat": {}}) == []
        assert stream.handle({"requestId": "md-1", "error": "unknown market"}) == []
        (err,) = of(drain(stream), StreamStatusEvent)
        assert err.state == "error" and err.key == "md-1"

    def test_private_orders_fills_positions_balances(self, us_creds):
        stream = PolymarketUSPrivateStream(us_creds)
        order = {"id": "o1", "marketSlug": SLUG, "side": "ORDER_SIDE_SELL", "type": "ORDER_TYPE_LIMIT", "price": {"value": "0.17"},
                 "quantity": 10, "leavesQuantity": 6, "cumQuantity": 4, "state": "ORDER_STATE_PARTIALLY_FILLED",
                 "intent": "ORDER_INTENT_BUY_SHORT", "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL"}
        (snap,) = stream.handle({"requestId": "order-1", "orderSubscriptionSnapshot": {"orders": [order], "eof": True}})
        assert snap.native == "snapshot" and snap.order.market_id == f"polymarket_us:{SLUG}"
        assert snap.order.side == Side.SELL and snap.order.price == D("0.17")
        update, fill = stream.handle({"orderSubscriptionUpdate": {"execution": {
            "id": "e1", "order": order, "lastShares": "4", "lastPx": {"value": "0.16"}, "type": "EXECUTION_TYPE_PARTIAL_FILL",
            "tradeId": "t1", "aggressor": True, "transactTime": "2026-09-17T12:00:00Z"}}})
        assert isinstance(update, OrderEvent) and update.native == "EXECUTION_TYPE_PARTIAL_FILL"
        assert isinstance(fill, FillEvent) and fill.fill.id == "t1" and fill.fill.price == D("0.16") and fill.fill.side == Side.SELL
        assert fill.fill.liquidity == Liquidity.TAKER and fill.fill.market_id == f"polymarket_us:{SLUG}"
        (cancel,) = stream.handle({"orderUpdate": {"execution": {"id": "e2", "order": {**order, "state": "ORDER_STATE_CANCELED"}, "type": "EXECUTION_TYPE_CANCELED"}}})
        assert cancel.order.status == OrderStatus.CANCELED
        (position,) = stream.handle({"positionSubscription": {
            "marketSlug": SLUG, "beforePosition": {"netPositionDecimal": "1.0000"},
            "afterPosition": {"netPositionDecimal": "-2.0000", "cost": {"value": "-0.34"}}, "updateTime": "2026-09-17T12:00:00Z",
            "entryType": "LEDGER_ENTRY_TYPE_ORDER_EXECUTION"}})
        assert isinstance(position, PositionEvent) and position.position.side == PositionSide.SHORT and position.position.contracts == D("2")
        (docs_balance,) = stream.handle({"accountBalancesSnapshot": {"balances": [{"currentBalance": 1000.0, "currency": "USD", "buyingPower": 850.0}]}})
        (sdk_balance,) = stream.handle({"accountBalanceSubscriptionUpdate": {"balance": 990.5, "buyingPower": 840.5}})
        (change,) = stream.handle({"accountBalancesUpdate": {"balanceChange": {"afterBalance": {"currentBalance": 980, "buyingPower": 830}}}})
        assert isinstance(docs_balance, BalanceEvent) and docs_balance.balance.buying_power == D("850.0")
        assert sdk_balance.balance.total == D("990.5") and change.balance.available == D("830")

    async def test_private_subscriptions(self, us_creds):
        stream = PolymarketUSPrivateStream(us_creds)
        conn = FakeConnection()
        stream._ws = conn
        await stream.watch_orders()
        await stream.watch_positions([SLUG])
        await stream.watch_balance()
        assert [f["subscribe"]["subscriptionType"] for f in conn.sent] == [
            "SUBSCRIPTION_TYPE_ORDER", "SUBSCRIPTION_TYPE_POSITION", "SUBSCRIPTION_TYPE_ACCOUNT_BALANCE",
        ]
        assert "marketSlugs" not in conn.sent[0]["subscribe"] and conn.sent[1]["subscribe"]["marketSlugs"] == [SLUG]
        assert len({f["subscribe"]["requestId"] for f in conn.sent}) == 3
