"""A bucket order across a restart: what the venues did while the process was down is the truth."""
from __future__ import annotations

from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.bucket import Bucket, BucketMember
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.paper import PaperVenue
from synpath.trading.types import Account, Order, OrderRequest, OrderStatus, OrderType, Side, TimeInForce

pytestmark = pytest.mark.anyio

K, P = "kalshi:KX-A", "polymarket:123"
ACCOUNTS = {"kalshi": Account(venue="kalshi", name="paper"), "polymarket": Account(venue="polymarket", name="paper")}


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Clock:
    def __init__(self):
        self.t = 1_800_000_000.0

    def __call__(self):
        return self.t


def new_engine(path: str, kalshi: PaperVenue, poly: PaperVenue, clock: Clock) -> Engine:
    engine = Engine(
        {"kalshi": kalshi, "polymarket": poly},
        EngineConfig(journal_path=path, require_lease=True, managed_tick_s=0.01),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_orders_per_minute=None, closing_soon_s=None,
                        max_open_orders=None),
        accounts=ACCOUNTS, clock=clock,
    )
    for venue in (kalshi, poly):
        venue.listeners.clear()
        venue.order_listeners.clear()
        venue.subscribe(engine.on_fill)
        venue.subscribe_orders(engine.on_order)
    return engine


async def resting_legs(tmp_path: Path):
    """An engine with a bucket order whose two legs rest, one per venue."""
    clock = Clock()
    kalshi, poly = PaperVenue(venue="kalshi", clock=clock), PaperVenue(venue="polymarket", clock=clock)
    path = str(tmp_path / "recover.db")
    engine = new_engine(path, kalshi, poly, clock)
    await engine.start()
    bucket = await engine.save_bucket(Bucket(book="alpha", name="t", members=[
        BucketMember(market_id=K), BucketMember(market_id=P)]))
    try:
        for venue, instrument, price, size in ((kalshi, K, "0.41", "30"), (poly, P, "0.42", "100")):
            venue.set_book(instrument, asks=[(D(price), D(size))])
            engine.set_book(instrument, venue.books[instrument])
            venue.set_book(instrument, asks=[(D("0.50"), D("100"))])   # nothing at the leg's price: it rests
        parent = await engine.submit(OrderRequest(market_id=bucket.market_id, side=Side.BUY, quantity=D("50"),
                                                  type=OrderType.MARKET, price=D("0.45"), book="alpha",
                                                  params={"min_stay_s": 0}))
        assert [o.quantity for o in kalshi.orders.values()] == [D("30")]
        assert [o.quantity for o in poly.orders.values()] == [D("20")]
    except BaseException:
        await engine.journal.close()
        raise
    return engine, kalshi, poly, clock, path, parent


def open_orders(paper: PaperVenue):
    return [o for o in paper.orders.values() if not o.is_terminal]


class TestRestart:
    async def test_a_fill_while_down_is_counted_once_and_nothing_is_placed_twice(self, tmp_path):
        engine, kalshi, poly, clock, path, parent = await resting_legs(tmp_path)
        await engine.journal.close()                                   # the process dies here

        kalshi.on_trade(K, price=D("0.41"), quantity=D("30"), taker_side=Side.SELL)   # the Kalshi leg fills meanwhile
        assert kalshi.orders[list(kalshi.orders)[0]].status == OrderStatus.CLOSED

        restarted = new_engine(path, kalshi, poly, clock)
        recovery = await restarted.start()
        try:
            assert recovery.managed == 1
            revived = restarted.managed_order_controller.get(parent.id)
            assert revived.state == "working" and revived.known_filled == 0, "nothing is trusted before the venues are asked"

            await restarted.managed_order_controller.on_timer()                          # first event after the restart: resync
            assert revived.known_filled == D("30")
            assert revived.as_order().filled == D("30") and revived.report()["per_venue"]
            assert len(kalshi.orders) == 1 and len(poly.orders) == 1, "the fill was learned, not bought again"
            assert [o.quantity for o in open_orders(poly)] == [D("20")]

            await kalshi.deliver()                                     # the stream catches up with the same fill
            assert revived.known_filled == D("30"), "delivered on top of what the venue said, not added to it"
            assert len(kalshi.orders) == 1 and len(poly.orders) == 1

            poly.on_trade(P, price=D("0.42"), quantity=D("20"), taker_side=Side.SELL)
            await poly.deliver()
            assert revived.state == "done" and revived.known_filled == D("50")
            assert revived.as_order().average_price == (D("0.41") * 30 + D("0.42") * 20) / 50
        finally:
            await restarted.stop()

    async def test_a_leg_cancelled_while_down_is_re_placed_within_the_remainder(self, tmp_path):
        engine, kalshi, poly, clock, path, parent = await resting_legs(tmp_path)
        await engine.journal.close()
        await kalshi.cancel_order(list(kalshi.orders)[0])              # the venue, or a human, pulled it

        restarted = new_engine(path, kalshi, poly, clock)
        await restarted.start()
        try:
            revived = restarted.managed_order_controller.get(parent.id)
            restarted.set_book(K, kalshi.books[K])
            restarted.set_book(P, poly.books[P])
            kalshi.set_book(K, asks=[(D("0.41"), D("100"))])
            restarted.set_book(K, kalshi.books[K])
            kalshi.set_book(K, asks=[(D("0.50"), D("100"))])
            await restarted.managed_order_controller.on_timer()
            live_k = open_orders(kalshi)
            assert len(live_k) == 1 and live_k[0].price == D("0.41")
            resting = sum(o.quantity - o.filled for o in open_orders(kalshi) + open_orders(poly))
            assert resting <= D("50")
            assert revived.state == "working"
        finally:
            await restarted.stop()

    async def test_a_child_the_snapshot_missed_is_adopted_from_the_journal(self, tmp_path):
        engine, kalshi, poly, clock, path, parent = await resting_legs(tmp_path)
        # An intent whose answer was lost: the engine's own recovery adopts
        # the venue order and journals it with the parent tag, but the
        # parent's snapshot never saw it. Stand in for that by writing it.
        stray = await kalshi.create_order(OrderRequest(market_id=K, side=Side.BUY, quantity=D("5"), price=D("0.40"),
                                                       type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                                                       account=ACCOUNTS["kalshi"], book="alpha",
                                                       tags={"parent": parent.id}))
        await engine.journal.upsert_order(stray)
        await engine.journal.close()

        restarted = new_engine(path, kalshi, poly, clock)
        await restarted.start()
        try:
            revived = restarted.managed_order_controller.get(parent.id)
            assert len(revived.children) == 2
            await restarted.managed_order_controller.on_timer()
            assert len(revived.children) == 3 and revived.child_of(stray.id, "kalshi") is not None
            assert sum(c.quantity for c in revived.live_children) <= D("50")
            assert restarted.managed_order_controller.parent_of(stray.id, "kalshi") is revived
        finally:
            await restarted.stop()


async def test_a_finished_bucket_order_is_still_reported_after_a_restart(tmp_path: Path):
    engine, kalshi, poly, clock, path, parent = await resting_legs(tmp_path)
    try:
        await engine.cancel(parent.id)
    finally:
        await engine.stop()
    again = new_engine(path, kalshi, poly, clock)
    await again.start()
    try:
        assert again.managed_order_controller.get(parent.id) is None, "a finished parent is not reloaded as live"
        bucket_id = parent.market_id.split(":", 1)[1]
        found = await again.bucket_orders(bucket_id)
        assert [p.id for p in found] == [parent.id]
        one = await again.bucket_order(parent.id)
        assert one is not None and one.as_order().status == OrderStatus.CANCELED
        assert one.report()["order_id"] == parent.id and one.report()["quantity"] == "50"
        assert await again.bucket_order("nope") is None
    finally:
        await again.stop()


@pytest.mark.parametrize("ending", ["canceled", "expired"])
async def test_pending_cancellation_resumes_after_sqlite_restart_without_new_orders(tmp_path, monkeypatch, ending):
    engine, kalshi, poly, clock, path, order = await resting_legs(tmp_path)
    parent = engine.managed_order_controller.get(order.id)
    original_cancellations = {venue.venue: venue.cancel_order for venue in (kalshi, poly)}
    for venue in (kalshi, poly):
        async def acknowledge_only(order_id, *, market_id=None, current=None, venue=venue):
            return await venue.fetch_order(order_id)
        monkeypatch.setattr(venue, "cancel_order", acknowledge_only)

    await engine.managed_order_controller.run(
        parent, lambda ctx: parent.cancel(ctx, "pending restart test", end=ending),
    )
    assert parent.state == "cancelling" and len(parent.live_children) == 2
    await engine.journal.close()

    # The venue makes progress while this process cannot receive notifications.
    # Restart must learn that progress before retrying the remaining cancellation.
    kalshi.on_trade(K, price=D("0.41"), quantity=D("10"), taker_side=Side.SELL)
    await original_cancellations["kalshi"](parent.children[0].order_id)
    for venue in (kalshi, poly):
        monkeypatch.setattr(venue, "cancel_order", original_cancellations[venue.venue])

    refreshed = []
    for venue in (kalshi, poly):
        original_fetch = venue.fetch_order
        async def record_refresh(order_id, *, original_fetch=original_fetch, venue=venue):
            refreshed.append((venue.venue, order_id))
            return await original_fetch(order_id)
        monkeypatch.setattr(venue, "fetch_order", record_refresh)

    restarted = new_engine(path, kalshi, poly, clock)
    await restarted.start()
    try:
        revived = restarted.managed_order_controller.get(order.id)
        assert revived.state == "cancelling" and not revived._resynced
        assert revived.pending_final_state == ending
        # Supply attractive books: stopping parents must still refuse to allocate.
        kalshi.set_book(K, asks=[(D("0.40"), D("100"))])
        poly.set_book(P, asks=[(D("0.40"), D("100"))])
        restarted.set_book(K, kalshi.books[K])
        restarted.set_book(P, poly.books[P])
        await restarted.managed_order_controller.on_timer()

        assert revived.state == ending and not revived.live_children
        assert revived._resynced
        assert set(refreshed) >= {(child.venue, child.order_id) for child in parent.children}
        assert revived.filled == D("10") and revived.remaining == D("40")
        assert len(kalshi.orders) == 1 and len(poly.orders) == 1
        assert open_orders(kalshi) == [] and open_orders(poly) == []
        stored = await restarted.journal.order(revived.venue, order.id)
        assert stored.status == OrderStatus(ending) and stored.filled == D("10")
    finally:
        await restarted.stop()
