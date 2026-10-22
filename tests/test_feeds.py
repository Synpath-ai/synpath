"""Streams wired into the engine: books reach parents, order records drive them, fills book."""
from __future__ import annotations

import asyncio
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.bucket import Bucket, BucketMember
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.feeds import Feeds, VenueStreams
from synpath.engine.paper import PaperVenue
from synpath.trading.types import Account, Fill, OrderRequest, OrderStatus, OrderType, Side
from synpath.ws.base import BookEvent, BookLevel, FillEvent, LocalBook, OrderEvent, StreamStatusEvent

pytestmark = pytest.mark.anyio

K, P = "kalshi:KX-A", "polymarket:123"
ACCOUNTS = {"kalshi": Account(venue="kalshi", name="paper"), "polymarket": Account(venue="polymarket", name="paper")}


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeStream:
    """Stands in for a venue stream: keeps LocalBooks, records subscriptions,
    yields whatever the test pushes."""

    def __init__(self, venue: str, name: str, *, market: bool = True, private: bool = False):
        self.venue, self.name = venue, name
        self.has = {"watch_order_book": market, "watch_trades": market, "watch_orders": private,
                    "watch_my_trades": private}
        self.trades_watched: list[list[str]] = []
        self.books: dict[str, LocalBook] = {}
        self.watched: list[list[str]] = []
        self.subscribed: list[str] = []
        self.queue: asyncio.Queue = asyncio.Queue()
        self.started = self.closed = False

    def start(self):
        self.started = True
        return self

    async def close(self):
        self.closed = True
        await self.queue.put(None)

    async def watch_order_book(self, market_ids):
        self.watched.append(list(market_ids))
        for m in market_ids:
            self.books.setdefault(m, LocalBook())

    async def watch_trades(self, market_ids):
        self.trades_watched.append(list(market_ids))

    async def watch_orders(self, market_ids=None):
        self.subscribed.append("orders")

    async def watch_my_trades(self, market_ids=None):
        self.subscribed.append("fills")

    def book(self, market_id, side="yes"):
        return self.books.get(market_id)

    def snapshot(self, market_id, *, bids=(), asks=()) -> BookEvent:
        book = self.books[market_id]
        book.replace([(D(p), D(s)) for p, s in bids], [(D(p), D(s)) for p, s in asks])
        return BookEvent(venue=self.venue, market_id=market_id, kind="snapshot",
                         bids=tuple(BookLevel(D(p), D(s)) for p, s in bids),
                         asks=tuple(BookLevel(D(p), D(s)) for p, s in asks),
                         best_bid=book.best_bid, best_ask=book.best_ask)

    def __aiter__(self):
        return self

    async def __anext__(self):
        event = await self.queue.get()
        if event is None:
            raise StopAsyncIteration
        return event


class Clock:
    def __init__(self):
        self.t = 1_800_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
async def setup(tmp_path: Path):
    clock = Clock()
    kalshi, poly = PaperVenue(venue="kalshi", clock=clock), PaperVenue(venue="polymarket", clock=clock)
    engine = Engine({"kalshi": kalshi, "polymarket": poly},
                    EngineConfig(journal_path=str(tmp_path / "feeds.db"), require_lease=True, managed_tick_s=0.01),
                    risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_orders_per_minute=None,
                                    closing_soon_s=None, max_open_orders=None),
                    accounts=ACCOUNTS, clock=clock)
    await engine.start()
    k_stream = FakeStream("kalshi", "kalshi", private=True)                     # one connection, both kinds
    p_market, p_user = FakeStream("polymarket", "market"), FakeStream("polymarket", "user", market=False, private=True)
    feeds = Feeds(engine, {"kalshi": VenueStreams(market=k_stream, private=k_stream),
                           "polymarket": VenueStreams(market=p_market, private=p_user)})
    await feeds.start()
    yield engine, feeds, kalshi, poly, k_stream, p_market, p_user
    await feeds.close()
    await engine.stop()


def stop_request(**kw) -> OrderRequest:
    base = dict(market_id=K, side=Side.SELL, quantity=D("10"), type=OrderType.STOP_MARKET, stop_price=D("0.40"),
                book="alpha")
    return OrderRequest(**{**base, **kw})


class TestSubscribing:
    async def test_start_subscribes_private_channels_once_per_connection(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        assert k_stream.started and p_market.started and p_user.started
        assert k_stream.subscribed == ["orders", "fills"] and p_user.subscribed == ["orders", "fills"]
        assert p_market.subscribed == [], "a market-only stream is not asked for orders"
        assert len(feeds.distinct()) == 3

    async def test_a_new_parent_s_market_is_subscribed_on_its_venue(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        await engine.submit(stop_request())
        assert await feeds.sync() == 1
        assert k_stream.watched == [[K]] and p_market.watched == []
        assert k_stream.trades_watched == [[K]], "its prints too: a last-trade stop hears every one"
        assert engine.books[K] is k_stream.books[K], "the engine reads the stream's own book object"
        assert await feeds.sync() == 0, "already watched"

    async def test_a_bucket_subscribes_every_member_on_its_own_venue(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        bucket = await engine.save_bucket(Bucket(book="alpha", name="b", members=[
            BucketMember(market_id=K), BucketMember(market_id=P)]))
        await engine.submit(OrderRequest(market_id=bucket.market_id, side=Side.BUY, quantity=D("10"), type="market",
                                         price=D("0.45"), book="alpha", params={"min_stay_s": 0}))
        await feeds.sync()
        assert k_stream.watched == [[K]] and p_market.watched == [[P]]

    async def test_restored_parents_are_subscribed_at_start(self, setup, tmp_path):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        await engine.submit(stop_request())
        fresh_stream = FakeStream("kalshi", "kalshi", private=True)
        fresh = Feeds(engine, {"kalshi": VenueStreams(market=fresh_stream, private=fresh_stream)})
        await fresh.start()
        assert fresh_stream.watched == [[K]]


class TestBooks:
    async def test_a_stop_fires_from_a_streamed_book_and_not_before_the_snapshot(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        kalshi.set_book(K, bids=[(D("0.38"), D("100"))])          # where the child will actually trade
        parent = await engine.submit(stop_request())
        await feeds.sync()
        managed = engine.managed_order_controller.get(parent.id)

        await feeds.dispatch("kalshi", k_stream, BookEvent(venue="kalshi", market_id=K, kind="delta"))
        assert managed.state == "waiting", "no snapshot yet: the book is not ready and reads as no book"

        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.45", "100")], asks=[("0.47", "100")]))
        assert managed.state == "waiting"
        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.39", "100")], asks=[("0.41", "100")]))
        assert managed.state in ("working", "done") and len(managed.children) == 1

    async def test_a_book_invalidated_after_a_gap_does_not_fire_a_stop(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        parent = await engine.submit(stop_request())
        await feeds.sync()
        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.45", "100")], asks=[("0.47", "100")]))
        k_stream.books[K].bids = {D("0.30"): D("100")}               # stale levels left behind by a gap
        k_stream.books[K].invalidate()
        await feeds.dispatch("kalshi", k_stream, BookEvent(venue="kalshi", market_id=K, kind="delta"))
        assert engine.managed_order_controller.get(parent.id).state == "waiting"


class TestMarks:
    """A held position is marked at the price it could be closed at on the
    live book the engine holds for it: the bid for a long, the ask for a short."""

    async def fill(self, engine, side, quantity="5", price="0.40", fill_id="F1"):
        await engine.on_fill(Fill(id=fill_id, order_id="O1", venue="kalshi", account=ACCOUNTS["kalshi"],
                                  market_id=K, side=side, price=D(price), quantity=D(quantity), timestamp=1))

    async def test_a_long_yes_is_marked_at_the_bid_once_its_book_is_subscribed(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        await self.fill(engine, Side.BUY)
        assert await feeds.sync() == 1 and k_stream.watched == [[K]], "a held market is subscribed with no order on it"
        [position] = engine.positions()
        assert position.mark_price is None, "no snapshot yet: no mark rather than a guessed one"

        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.44", "10")], asks=[("0.48", "10")]))
        [position] = engine.positions()
        assert position.mark_price == D("0.44") and position.unrealized_pnl == D("0.20")
        assert engine.pnl("account")["total"].unrealized == D("0.20")

    async def test_a_short_yes_which_is_a_long_no_is_marked_at_the_ask(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        await self.fill(engine, Side.SELL, price="0.60")              # Buy NO at 0.40
        await feeds.sync()
        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.44", "10")], asks=[("0.48", "10")]))
        [position] = engine.positions()
        # Closing buys YES back at 0.48, which is selling NO at 0.52: 0.12 a contract on 5.
        assert position.mark_price == D("0.48") and position.unrealized_pnl == D("0.60")

    async def test_the_last_trade_marks_when_nobody_is_on_the_closing_side_or_the_book_is_not_live(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        await self.fill(engine, Side.BUY)
        await feeds.sync()
        [position] = engine.positions()
        assert position.mark_price is None, "no book and no trade: no mark"

        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, asks=[("0.48", "10")]))
        await engine.on_trade({"market_id": K, "price": "0.47", "quantity": "1"})
        [position] = engine.positions()
        assert position.mark_price == D("0.47"), "no bid: a long is marked at the last trade"

        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.44", "10")], asks=[("0.48", "10")]))
        assert engine.positions()[0].mark_price == D("0.44")
        k_stream.books[K].invalidate()
        [position] = engine.positions()
        assert position.mark_price == D("0.47") and position.unrealized_pnl == D("0.35")


class TestFillOutcome:
    async def test_a_fill_carries_its_order_s_outcome_and_action(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        kalshi.set_book(K, bids=[(D("0.40"), D("100"))], asks=[(D("0.45"), D("100"))])
        order = await engine.submit(OrderRequest(market_id=K, outcome="yes", action="sell", quantity=D("5"),
                                                 price=D("0.44"), account=ACCOUNTS["kalshi"]))
        assert (order.outcome, order.action) == ("yes", "sell")
        for fill in kalshi.on_trade(K, price=D("0.44"), quantity=D("10"), taker_side=Side.BUY):
            await engine.on_fill(fill)
        [fill] = await engine.journal.fills()
        assert (fill.side, fill.outcome, fill.action) == (Side.SELL, "yes", "sell")


class TestAccountStreams:
    async def test_an_order_update_on_the_private_stream_completes_the_parent(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        parent = await engine.submit(stop_request())
        await feeds.sync()
        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.39", "100")], asks=[("0.41", "100")]))
        managed = engine.managed_order_controller.get(parent.id)
        child = managed.children[0]
        venue_order = kalshi.orders[child.order_id]
        assert managed.filled == venue_order.filled, "the placement's answer is the first record"

        done = venue_order.model_copy(update={"filled": D("10"), "remaining": D("0"), "status": OrderStatus.CLOSED})
        await feeds.dispatch("kalshi", k_stream, OrderEvent(venue="kalshi", order=done))
        assert managed.filled == D("10") and managed.state == "done"

    async def test_a_fill_event_books_in_the_ledger_and_leaves_the_parent_alone(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        parent = await engine.submit(stop_request())
        await feeds.sync()
        kalshi.set_book(K, bids=[])                                   # nothing to cross: the child rests
        await feeds.dispatch("kalshi", k_stream, k_stream.snapshot(K, bids=[("0.39", "100")], asks=[("0.41", "100")]))
        managed = engine.managed_order_controller.get(parent.id)
        child = managed.children[0]
        before = managed.filled
        fill = Fill(id="f-1", order_id=child.order_id, venue="kalshi", account=ACCOUNTS["kalshi"], market_id=K,
                    side=Side.SELL, price=D("0.39"), quantity=D("4"), timestamp=1)
        await feeds.dispatch("kalshi", k_stream, FillEvent(venue="kalshi", fill=fill))
        assert managed.filled == before, "fill events do not reach parents"
        assert any(p.contracts == D("-4") for p in engine.ledger.positions.values())

    async def test_reconcile_required_polls_that_venue_now(self, setup, monkeypatch):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        polled: list[str] = []

        async def poll(venue, adapter=None):
            polled.append(venue)
        monkeypatch.setattr(engine, "poll", poll)
        await feeds.dispatch("polymarket", p_user, StreamStatusEvent(venue="polymarket", stream="user",
                                                                      state="resynced", reconcile_required=True))
        await feeds.dispatch("polymarket", p_user, StreamStatusEvent(venue="polymarket", stream="user", state="connected"))
        assert polled == ["polymarket"]


class TestStreamEnding:
    async def test_a_stream_that_ends_by_itself_does_not_end_its_pump(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        task = asyncio.create_task(feeds.pump("polymarket", p_user))
        await p_user.queue.put(None)                  # the venue ended the stream
        await asyncio.sleep(0.05)
        assert not task.done(), "a host waiting on its first finished task must not stop here"
        feeds.closing = True
        await asyncio.wait_for(task, 3)


class TestRunning:
    async def test_background_pumps_every_stream_and_ends_when_they_close(self, setup):
        engine, feeds, kalshi, poly, k_stream, p_market, p_user = setup
        loops = feeds.background()
        assert set(loops) == {"feeds-sync", "feed-kalshi-kalshi", "feed-polymarket-market", "feed-polymarket-user"}
        loops.pop("feeds-sync").close()
        tasks = [asyncio.create_task(c) for c in loops.values()]
        await p_user.queue.put(StreamStatusEvent(venue="polymarket", stream="user", state="connected"))
        await feeds.close()
        await asyncio.wait_for(asyncio.gather(*tasks), 2)
        assert feeds.dispatched == 1

    async def test_the_engine_names_every_loop_a_host_must_run(self, setup):
        engine, *_ = setup
        loops = engine.background()
        try:
            assert set(loops) == {"lease", "sweep", "poll", "managed", "dispatch"}
        finally:
            for coro in loops.values():
                coro.close()
