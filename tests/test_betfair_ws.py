"""Betfair's Exchange Stream API, spoken against a fake TLS socket.

The market messages are hand-built to the shapes in Betfair's Exchange
Stream API documentation (and betfairlightweight's streaming fixtures). The
order change messages are the documentation's own examples (a market-level
snapshot, a reconnection image, a runner removal, a VAR void), the VAR
example's bare runner changes wrapped in an `ocm`. Nothing here opens a
connection to Betfair.
"""
from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal

import pytest

from synpath import AuthenticationError, BetfairMarketStream, BetfairUserStream
from synpath.betfair import BetfairSession
from synpath.trading.types import OrderStatus, PositionSide, Side
from synpath.ws.base import (
    BookEvent, FillEvent, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, StreamStatusEvent,
)
from synpath.ws.betfair import (
    DELAYED_CONFLATE_MS, MAX_MARKETS, LineSocket, apply_ladder, changed, yes_levels, yes_price,
)

from conftest import load

pytestmark = pytest.mark.anyio

MESSAGES = load("betfair_stream.json")
ARSENAL = "betfair:1.234567890-1096"
CHELSEA = "betfair:1.234567890-55190"


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    for name in ("BETFAIR_APP_KEY", "BETFAIR_USERNAME", "BETFAIR_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)
    await asyncio.sleep(0.01)


class FakeSocket:
    def __init__(self):
        self.sent: list = []

    async def send(self, text):
        self.sent.append(json.loads(text))


def a_session() -> BetfairSession:
    return BetfairSession(app_key="APP-KEY-DELAYED", session_token="SESSION-TOKEN-1")


def stream_with(**kwargs) -> BetfairMarketStream:
    return BetfairMarketStream(session=a_session(), **kwargs)


def message(name: str, **changes) -> str:
    return json.dumps({**MESSAGES[name], **changes})


async def authenticated(stream):
    """A stream past authentication: the frame sent, Betfair's SUCCESS read."""
    stream._ws = FakeSocket()
    stream._dispatch(message("connection"))
    await stream.on_connect()
    auth = stream._ws.sent[-1]
    assert auth == {"op": "authentication", "id": auth["id"], "appKey": "APP-KEY-DELAYED", "session": "SESSION-TOKEN-1"}
    stream._dispatch(message("auth_success", id=auth["id"]))
    await settle()
    return stream


class TestRecords:
    def test_ladder_levels_read_on_yes(self):
        ladder: dict = {}
        apply_ladder(ladder, [[0, 2.0, 10], [1, 2.02, 5]])
        assert yes_levels(ladder) == [(Decimal("0.5000000000"), Decimal("20.0")), (yes_price(2.02), Decimal("10.10"))]

    def test_a_zero_stake_deletes_the_level(self):
        ladder: dict = {}
        apply_ladder(ladder, [[0, 2.0, 10], [1, 1.99, 5]])
        apply_ladder(ladder, [[1, 1.99, 0]])
        assert list(ladder) == [0]

    def test_changed_levels(self):
        from synpath.ws.base import BookLevel

        old = (BookLevel(Decimal("0.5"), Decimal("1")), BookLevel(Decimal("0.4"), Decimal("2")))
        new = (BookLevel(Decimal("0.5"), Decimal("3")),)
        assert set(changed(old, new)) == {BookLevel(Decimal("0.5"), Decimal("3")), BookLevel(Decimal("0.4"), Decimal("0"))}


class TestFraming:
    async def test_lines_end_in_crlf_both_ways(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b'{"op":"connection","connectionId":"c1"}\r\n{"op":"status","id":1}\r\n')

        class Writer:
            def __init__(self):
                self.data = b""

            def write(self, data):
                self.data += data

            async def drain(self):
                pass

            def close(self):
                pass

            async def wait_closed(self):
                pass

        writer = Writer()
        socket = LineSocket(reader, writer)
        assert json.loads(await socket.recv())["op"] == "connection"
        assert json.loads(await socket.recv())["op"] == "status"
        await socket.send('{"op":"heartbeat","id":9}')
        assert writer.data == b'{"op":"heartbeat","id":9}\r\n'
        reader.feed_eof()
        with pytest.raises(ConnectionError):
            await socket.recv()

    async def test_the_first_message_must_come_within_the_timeout(self):
        socket = LineSocket(asyncio.StreamReader(), writer=None, first_timeout=0.01)
        with pytest.raises(asyncio.TimeoutError):
            await socket.recv()

    async def test_the_stream_opens_a_tls_socket_not_a_websocket(self):
        opened = {}

        async def open_connection(host, port, **kwargs):
            opened.update(host=host, port=port, **kwargs)
            return asyncio.StreamReader(), None

        stream = stream_with(open_connection=open_connection)
        socket = await stream._open()
        assert isinstance(socket, LineSocket)
        assert opened["host"] == "stream-api.betfair.com" and opened["port"] == 443
        assert opened["ssl"] is not None and opened["server_hostname"] == "stream-api.betfair.com"

    async def test_a_whole_connection_through_the_socket(self):
        """Connect, authenticate, subscribe, read an image: the real run loop."""
        reader = asyncio.StreamReader()
        written: list[dict] = []

        class Writer:
            def write(self, data):
                for line in data.decode().split("\r\n"):
                    if line:
                        written.append(json.loads(line))
                        if written[-1]["op"] == "authentication":
                            reader.feed_data((message("auth_success", id=written[-1]["id"]) + "\r\n").encode())
                        if written[-1]["op"] == "marketSubscription":
                            reader.feed_data((message("subscribe_success", id=written[-1]["id"]) + "\r\n").encode())
                            reader.feed_data((message("image", id=written[-1]["id"]) + "\r\n").encode())

            async def drain(self):
                pass

            def close(self):
                reader.feed_eof()

            async def wait_closed(self):
                pass

        async def open_connection(host, port, **kwargs):
            reader.feed_data((message("connection") + "\r\n").encode())
            return reader, Writer()

        stream = stream_with(open_connection=open_connection)
        await stream.watch_order_book([ARSENAL])
        stream.start()
        events = []
        async for event in stream:
            events.append(event)
            if isinstance(event, BookEvent):
                break
        await stream.close()
        assert [w["op"] for w in written] == ["authentication", "marketSubscription"]
        assert events[-1].market_id == ARSENAL and events[-1].kind == "snapshot"
        assert any(isinstance(e, StreamStatusEvent) and e.state == "subscribed" for e in events)


class TestAuthentication:
    def test_no_account_is_refused_at_construction(self):
        with pytest.raises(AuthenticationError, match="BETFAIR_APP_KEY"):
            BetfairMarketStream()

    async def test_the_subscription_follows_a_successful_authentication(self):
        stream = stream_with()
        await stream.watch_order_book([ARSENAL])
        await authenticated(stream)
        subscription = stream._ws.sent[-1]
        assert subscription["op"] == "marketSubscription"
        assert subscription["marketFilter"] == {"marketIds": ["1.234567890"]}
        assert subscription["marketDataFilter"] == {"fields": ["EX_BEST_OFFERS", "EX_MARKET_DEF"], "ladderLevels": 3}
        assert subscription["heartbeatMs"] == 5000 and "conflateMs" not in subscription
        assert "clk" not in subscription and "initialClk" not in subscription
        assert stream.connection_id == "002-051134157842-432409"

    async def test_nothing_is_subscribed_before_authentication(self):
        stream = stream_with()
        stream._ws = FakeSocket()
        await stream.watch_order_book([ARSENAL])
        assert stream._ws.sent == []

    async def test_a_refused_session_is_dropped_so_the_next_connection_logs_in(self):
        stream = stream_with()
        stream._ws = FakeSocket()
        await stream.on_connect()
        stream._dispatch(message("auth_failure", id=stream._auth_id))
        assert stream.session._token is None
        assert drain(stream)[-1].state == "error"

    async def test_a_refused_app_key_fails_for_good(self):
        stream = stream_with()
        stream._ws = FakeSocket()
        await stream.on_connect()
        stream._dispatch(message("app_key_failure", id=stream._auth_id))
        await settle()
        states = [e.state for e in drain(stream) if isinstance(e, StreamStatusEvent)]
        assert "failed" in states and stream._closing


class TestBooks:
    async def started(self, **kwargs):
        stream = stream_with(**kwargs)
        await stream.watch_order_book([ARSENAL])
        await stream.watch_ticker([CHELSEA])
        await authenticated(stream)
        stream._dispatch(message("subscribe_success", id=stream._sub_id))
        drain(stream)
        return stream

    async def test_an_image_then_deltas(self):
        stream = await self.started()
        stream._dispatch(message("image"))
        events = drain(stream)
        snapshot = next(e for e in events if isinstance(e, BookEvent))
        assert snapshot.market_id == ARSENAL and snapshot.kind == "snapshot"
        # Resting backs (batl, what a taker can lay into) are YES bids; resting lays asks.
        assert snapshot.best_bid == yes_price(2.1) and snapshot.best_ask == yes_price(2.08)
        assert snapshot.bids[0].size == Decimal("640.25") * Decimal("2.1")
        quote = next(e for e in events if isinstance(e, QuoteEvent))
        assert quote.market_id == CHELSEA and quote.bid == yes_price(3.7) and quote.ask == yes_price(3.65)
        assert stream.initial_clk == "G1jHgcgNHPgjjcwRj5LFHw==" and stream.clk == "AAAAAAAAAAAAAA=="

        stream._dispatch(message("delta"))
        delta = next(e for e in drain(stream) if isinstance(e, BookEvent))
        assert delta.kind == "delta" and stream.clk == "AAAAAAAAAAAAAB=="
        # Level 0 of batb moved from 2.08 to 2.10: the old price is gone, the new one in.
        asks = {level.price: level.size for level in delta.asks}
        assert asks[yes_price(2.1)] == Decimal("300.0") * Decimal("2.1") and asks[yes_price(2.08)] == 0
        # batl level 2 (2.14) deleted by a zero stake.
        bids = {level.price: level.size for level in delta.bids}
        assert bids[yes_price(2.14)] == 0 and bids[yes_price(2.1)] == Decimal("700.0") * Decimal("2.1")
        assert yes_price(2.12) not in bids  # unchanged levels are not repeated
        book = stream.book(ARSENAL)
        assert book.ready and yes_price(2.14) not in {level.price for level in book.levels()[0]}
        assert stream.book(ARSENAL, "no").best_ask == 1 - book.best_bid

    async def test_a_heartbeat_moves_the_clock_only(self):
        stream = await self.started()
        stream._dispatch(message("image"))
        drain(stream)
        stream._dispatch(message("heartbeat"))
        assert drain(stream) == [] and stream.clk == "AAAAAAAAAAAAAD=="

    async def test_reconnect_resubscribes_with_the_clock_and_applies_the_delta(self):
        stream = await self.started()
        stream._dispatch(message("image"))
        stream._dispatch(message("delta"))
        drain(stream)
        stream.on_disconnect()
        assert not stream.book(ARSENAL).ready
        await authenticated(stream)
        resubscription = stream._ws.sent[-1]
        assert resubscription["initialClk"] == "G1jHgcgNHPgjjcwRj5LFHw==" and resubscription["clk"] == "AAAAAAAAAAAAAB=="
        stream._dispatch(message("resub_delta"))
        events = drain(stream)
        book = next(e for e in events if isinstance(e, BookEvent))
        assert book.kind == "snapshot" and stream.book(ARSENAL).ready
        bid_prices = [level.price for level in book.bids]
        assert yes_price(2.14) in bid_prices and yes_price(2.12) in bid_prices
        assert any(isinstance(e, StreamStatusEvent) and e.state == "resynced" for e in events)

    async def test_a_changed_market_set_starts_a_fresh_image(self):
        stream = await self.started()
        stream._dispatch(message("image"))
        await stream.watch_order_book(["betfair:1.234567891-47972"])
        subscription = stream._ws.sent[-1]
        assert subscription["marketFilter"]["marketIds"] == ["1.234567890", "1.234567891"]
        assert "clk" not in subscription

    async def test_a_delayed_key_is_warned_about_once(self, caplog):
        stream = await self.started()
        with caplog.at_level(logging.WARNING, logger="synpath.ws.betfair"):
            stream._dispatch(message("image"))
            stream._dispatch(message("resub_delta"))
        warnings = [r for r in caplog.records if "conflated" in r.getMessage()]
        assert len(warnings) == 1 and str(DELAYED_CONFLATE_MS) in warnings[0].getMessage()
        assert stream.idle_timeout >= DELAYED_CONFLATE_MS / 1000

    async def test_options_reach_the_subscription(self):
        stream = await self.started(ladder_levels=10, conflate_ms=0, heartbeat_ms=1000)
        subscription = stream._ws.sent[-1]
        assert subscription["marketDataFilter"]["ladderLevels"] == 10 and subscription["conflateMs"] == 0
        with pytest.raises(ValueError):
            stream_with(ladder_levels=11)


class TestStatus:
    async def test_runner_status_changes(self):
        stream = stream_with()
        await stream.watch_market_status(["betfair:1.234567890"])
        await authenticated(stream)
        stream._dispatch(message("image"))
        opened = [e for e in drain(stream) if isinstance(e, MarketStatusEvent)]
        assert {e.market_id for e in opened} == {ARSENAL, CHELSEA, "betfair:1.234567890-58805"}
        assert {e.state for e in opened} == {"open"}
        stream._dispatch(message("status_change"))
        changes = {e.market_id: e for e in drain(stream) if isinstance(e, MarketStatusEvent)}
        assert changes[ARSENAL].state == "paused" and changes[ARSENAL].info["in_play"] is True
        assert changes["betfair:1.234567890-58805"].state == "settled"
        assert changes["betfair:1.234567890-58805"].result == "void"


class TestSubscriptionCap:
    async def test_no_more_than_200_markets(self):
        stream = stream_with()
        await stream.watch_order_book([f"betfair:1.{n}-1" for n in range(MAX_MARKETS + 5)])
        assert len(stream.wanted) == MAX_MARKETS
        status = [e for e in drain(stream) if isinstance(e, StreamStatusEvent)]
        assert status and status[0].state == "error" and "1.204" in status[0].detail
        assert stream.book("betfair:1.204-1") is None

    async def test_betfairs_own_refusal_is_reported(self):
        stream = stream_with()
        await stream.watch_order_book([ARSENAL])
        await authenticated(stream)
        stream._dispatch(message("subscription_limit", id=stream._sub_id))
        assert "SUBSCRIPTION_LIMIT_EXCEEDED" in drain(stream)[-1].detail


class TestFeeds:
    def test_the_engine_gets_a_market_stream_and_an_account_stream(self):
        from synpath.engine.feeds import default_streams
        from synpath.trading.credentials import BetfairCredentials

        creds = BetfairCredentials(app_key="k", username="u", password="p")
        streams = default_streams({"betfair": object()}, {"betfair": creds})
        assert isinstance(streams["betfair"].market, BetfairMarketStream)
        assert isinstance(streams["betfair"].private, BetfairUserStream)
        assert default_streams({"betfair": object()}, {}) == {}


ORDERS = load("betfair_trading_stream.json")
REMOVAL_RUNNER = "betfair:1.102151675-6113662"


def order_message(name: str, **changes) -> str:
    return json.dumps({**ORDERS[name], **changes})


async def order_stream(*kinds, **kwargs) -> BetfairUserStream:
    stream = BetfairUserStream(session=a_session(), **kwargs)
    for kind in kinds or ("orders", "fills", "positions"):
        await getattr(stream, {"orders": "watch_orders", "fills": "watch_my_trades",
                               "positions": "watch_positions"}[kind])()
    await authenticated(stream)
    stream._dispatch(order_message("subscribe_success", id=stream._sub_id))
    drain(stream)
    return stream


class TestOrderStream:
    async def test_the_order_subscription_follows_authentication(self):
        stream = await order_stream(strategy_refs=["betstrategy1"])
        subscription = stream._ws.sent[-1]
        documented = ORDERS["subscription_example"]
        assert subscription["op"] == documented["op"] and subscription["segmentationEnabled"] is True
        assert subscription["orderFilter"] == {"includeOverallPosition": True, "customerStrategyRefs": ["betstrategy1"]}
        assert "clk" not in subscription

    async def test_nothing_is_subscribed_until_something_is_watched(self):
        stream = BetfairUserStream(session=a_session())
        await authenticated(stream)
        assert [frame["op"] for frame in stream._ws.sent] == ["authentication"]

    async def test_a_snapshot_reports_the_resting_bet_without_a_fill(self):
        stream = await order_stream()
        stream._dispatch(order_message("market_snapshot"))
        events = drain(stream)
        [event] = [e for e in events if isinstance(e, OrderEvent)]
        order = event.order
        assert order.id == "215144775671" and order.market_id == "betfair:1.174743281-30246"
        assert order.side == Side.BUY and order.status == OrderStatus.OPEN and order.price == yes_price(990)
        assert order.quantity == Decimal("1980") and order.created_at == 1603894536000
        assert not [e for e in events if isinstance(e, FillEvent)]

    async def test_a_bet_matching_is_a_fill_then_a_runner_removal_is_not(self):
        stream = await order_stream()
        stream._dispatch(order_message("rule4_placed"))
        placed = drain(stream)
        assert [type(e).__name__ for e in placed] == ["OrderEvent", "PositionEvent"]
        assert placed[0].order.status == OrderStatus.OPEN and placed[1].position.contracts == 0

        stream._dispatch(order_message("rule4_matched"))
        fill, order, position = drain(stream)
        assert isinstance(fill, FillEvent) and fill.fill.id == "10822867886:2"
        assert fill.fill.quantity == Decimal("24") and fill.fill.price == yes_price(12)
        assert order.order.status == OrderStatus.CLOSED and order.order.filled == Decimal("24")
        assert order.order.last_fill_quantity == Decimal("24")
        assert position.position.market_id == REMOVAL_RUNNER and position.position.inventory_yes == Decimal("24")
        assert position.position.side == PositionSide.LONG

        # A runner removed: the matched price is reduced to 9.47, nothing new matched.
        stream._dispatch(order_message("rule4_reduced"))
        events = drain(stream)
        assert not [e for e in events if isinstance(e, FillEvent)]
        [position] = [e.position for e in events if isinstance(e, PositionEvent)]
        assert position.inventory_yes == Decimal("2") * Decimal("9.47")
        assert position.info["matched_backs"] == [["9.47", "2"]]

    async def test_a_reconnection_image_is_the_baseline(self):
        stream = await order_stream()
        stream._dispatch(order_message("reconnect_image"))
        events = drain(stream)
        assert not [e for e in events if isinstance(e, FillEvent)]
        [order] = [e.order for e in events if isinstance(e, OrderEvent)]
        assert order.filled == Decimal("4.75") * 12 and order.remaining == Decimal("0.25") * 12
        positions = {e.position.market_id: e.position for e in events if isinstance(e, PositionEvent)}
        assert positions["betfair:1.125657695-48756"].inventory_yes == Decimal("2.8")
        assert stream.initial_clk == ORDERS["reconnect_image"]["initialClk"]

        # The rest matched while disconnected: the next image has the bet gone and the position whole.
        stream._dispatch(order_message("reconnect_image_matched"))
        events = drain(stream)
        positions = {e.position.market_id: e.position for e in events if isinstance(e, PositionEvent)}
        assert positions["betfair:1.125657760-151478"].inventory_yes == Decimal("60")
        assert positions["betfair:1.125670254-5643663"].contracts == 0
        assert stream.orders == {}

    async def test_a_reconnect_resumes_from_the_clock(self):
        stream = await order_stream()
        stream._dispatch(order_message("reconnect_image"))
        await authenticated(stream)
        resubscribe = stream._ws.sent[-1]
        assert resubscribe["initialClk"] == ORDERS["reconnect_image"]["initialClk"]
        assert resubscribe["clk"] == ORDERS["reconnect_image"]["clk"]

    async def test_a_var_void_and_a_lapse_end_bets_without_fills(self):
        stream = await order_stream()
        stream._dispatch(order_message("var_lay_matched"))
        events = drain(stream)
        status = [e for e in events if isinstance(e, StreamStatusEvent)]
        assert status[0].state == "error" and status[0].reconcile_required and "503" in status[0].detail
        [fill] = [e.fill for e in events if isinstance(e, FillEvent)]
        assert fill.side == Side.SELL and fill.quantity == Decimal("4") and fill.price == Decimal("0.5")
        [position] = [e.position for e in events if isinstance(e, PositionEvent)]
        assert position.side == PositionSide.SHORT and position.inventory_no == Decimal("8")

        stream._dispatch(order_message("var_back_placed"))
        drain(stream)
        stream._dispatch(order_message("var_lay_voided"))
        stream._dispatch(order_message("var_back_lapsed"))
        events = drain(stream)
        assert not [e for e in events if isinstance(e, FillEvent)]
        assert [e.state for e in events if isinstance(e, StreamStatusEvent)] == ["resynced"]
        voided, lapsed = [e.order for e in events if isinstance(e, OrderEvent)]
        assert voided.status == OrderStatus.CANCELED and voided.filled == 0 and voided.info["sizeVoided"] == 2
        assert lapsed.status == OrderStatus.CANCELED and lapsed.info["sizeLapsed"] == 2 and lapsed.remaining == 0

    async def test_only_the_markets_watched_are_passed_on(self):
        stream = BetfairUserStream(session=a_session())
        await stream.watch_orders(["betfair:1.102151675"])
        await authenticated(stream)
        stream._dispatch(order_message("market_snapshot"))
        stream._dispatch(order_message("rule4_placed"))
        orders = [e.order for e in drain(stream) if isinstance(e, OrderEvent)]
        assert [o.market_id for o in orders] == [REMOVAL_RUNNER]

    async def test_a_heartbeat_carries_the_streams_health(self):
        stream = await order_stream()
        stream._dispatch(order_message("heartbeat_503"))
        [status] = drain(stream)
        assert status.state == "error" and stream.clk == "AAAAAAAA"
