"""A market order on a bucket, driven through the real engine on two paper venues."""
from __future__ import annotations

from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.bucket import Bucket, BucketMember
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.paper import BookState, PaperVenue, quadratic_fee
from synpath.errors import BadRequest
from synpath.trading.types import Account, OrderRequest, OrderStatus, OrderType, Side

pytestmark = pytest.mark.anyio

K, P = "kalshi:KX-A", "polymarket:123"
ACCOUNT_K = Account(venue="kalshi", name="paper")
ACCOUNT_P = Account(venue="polymarket", name="paper")


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Clock:
    def __init__(self, start: float = 1_800_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def tick(self, seconds: float) -> None:
        self.t += seconds


async def build(tmp_path: Path, *, kalshi_fees=None, flip_p: bool = False):
    clock = Clock()
    kalshi = PaperVenue(venue="kalshi", clock=clock, **({"fees": kalshi_fees} if kalshi_fees else {}))
    poly = PaperVenue(venue="polymarket", clock=clock)
    engine = Engine(
        {"kalshi": kalshi, "polymarket": poly},
        EngineConfig(journal_path=str(tmp_path / "routed.db"), require_lease=True, managed_tick_s=0.01),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_orders_per_minute=None, closing_soon_s=None,
                        max_open_orders=None),
        accounts={"kalshi": ACCOUNT_K, "polymarket": ACCOUNT_P}, clock=clock,
    )
    for venue in (kalshi, poly):
        venue.subscribe(engine.on_fill)
        venue.subscribe_orders(engine.on_order)
    await engine.start()
    bucket = await engine.save_bucket(Bucket(book="alpha", name="t", members=[
        BucketMember(market_id=K), BucketMember(market_id=P, flip=flip_p)]))
    return engine, kalshi, poly, clock, bucket


@pytest.fixture
async def setup(tmp_path: Path):
    engine, kalshi, poly, clock, bucket = await build(tmp_path)
    yield engine, kalshi, poly, clock, bucket
    await engine.stop()


def book(paper: PaperVenue, engine: Engine, instrument: str, *, bids=(), asks=()) -> None:
    paper.set_book(instrument, bids=bids, asks=asks)
    engine.set_book(instrument, paper.books[instrument])


async def deliver(*papers: PaperVenue) -> None:
    for paper in papers:
        await paper.deliver()


def request(bucket: Bucket, **kw) -> OrderRequest:
    base = dict(market_id=bucket.market_id, side=Side.BUY, amount=D("50"), price=D("0.45"), book="alpha",
                type=OrderType.MARKET, params={"min_stay_s": 0})
    if "params" in kw:
        kw["params"] = {**base["params"], **kw["params"]}
    return OrderRequest(**{**base, **kw})


def open_orders(paper: PaperVenue):
    return [o for o in paper.orders.values() if not o.is_terminal]


class TestPlacing:
    async def test_legs_land_on_both_venues_and_the_parent_reports_in_bucket_terms(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        parent = await engine.submit(request(bucket))
        assert parent.held_by.value == "engine" and parent.market_id == bucket.market_id
        assert [(o.side, o.price, o.amount) for o in kalshi.orders.values()] == [(Side.BUY, D("0.41"), D("30"))]
        assert [(o.side, o.price, o.amount) for o in poly.orders.values()] == [(Side.BUY, D("0.42"), D("20"))]

        await deliver(kalshi, poly)
        managed = engine.orders.get(parent.id)
        assert managed.state == "done" and managed.filled == D("50")
        assert managed.as_order().average_price == (D("0.41") * 30 + D("0.42") * 20) / 50
        report = managed.report()
        assert report["filled"] == "50" and report["unfilled"] == "0" and report["stop_reason"] == ""
        assert {(r["venue"], r["filled"]) for r in report["per_venue"]} == {("kalshi", "30"), ("polymarket", "20")}

        children = await engine.journal.orders_for_parent(parent.id)
        assert sorted(c.venue for c in children) == ["kalshi", "polymarket"]

    async def test_fees_decide_which_venue_goes_first(self, tmp_path):
        engine, kalshi, poly, clock, bucket = await build(tmp_path, kalshi_fees=quadratic_fee())
        try:
            # 0.41 on Kalshi costs 0.41 + 0.07 * 0.41 * 0.59 = 0.4269 net; 0.42 on Polymarket costs 0.42.
            book(kalshi, engine, K, asks=[(D("0.41"), D("100"))])
            book(poly, engine, P, asks=[(D("0.42"), D("100"))])
            await engine.submit(request(bucket, amount=D("20")))
            assert kalshi.orders == {} and [o.amount for o in poly.orders.values()] == [D("20")]
        finally:
            await engine.stop()

    async def test_a_flipped_member_is_sold_at_one_minus(self, tmp_path):
        engine, kalshi, poly, clock, bucket = await build(tmp_path, flip_p=True)
        try:
            # Polymarket's YES bid at 0.70 is a NO ask at 0.30: the cheapest way to buy the bucket.
            book(poly, engine, P, bids=[(D("0.70"), D("50"))])
            book(kalshi, engine, K, asks=[(D("0.35"), D("50"))])
            parent = await engine.submit(request(bucket, amount=D("40"), price=D("0.40")))
            assert [(o.side, o.price, o.amount) for o in poly.orders.values()] == [(Side.SELL, D("0.70"), D("40"))]
            assert kalshi.orders == {}
            await deliver(kalshi, poly)
            managed = engine.orders.get(parent.id)
            assert managed.state == "done" and managed.as_order().average_price == D("0.30")
        finally:
            await engine.stop()


class TestWorking:
    async def test_a_partial_fill_leaves_the_remainder_resting_and_the_rest_completes_it(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        kalshi.set_book(K, asks=[(D("0.41"), D("10"))])     # someone else took 20 before our leg arrived
        parent = await engine.submit(request(bucket))
        await deliver(kalshi, poly)
        managed = engine.orders.get(parent.id)
        assert managed.state == "working" and managed.filled == D("30")
        resting = open_orders(kalshi)
        assert [(o.price, o.amount, o.filled) for o in resting] == [(D("0.41"), D("30"), D("10"))]
        poly_id = list(poly.orders)[0]

        # The remaining 20 are what the Kalshi leg still rests for; nothing new is put out.
        await engine.on_book({"market_id": K})
        assert len(kalshi.orders) == 1 and list(poly.orders)[0] == poly_id

        kalshi.on_trade(K, price=D("0.41"), amount=D("20"), taker_side=Side.SELL)
        await deliver(kalshi)
        assert managed.state == "done" and managed.filled == D("50")

    async def test_only_the_leg_whose_price_changed_is_replaced(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        kalshi.set_book(K, asks=[(D("0.50"), D("100"))])   # the venues have nothing at those prices: legs rest
        poly.set_book(P, asks=[(D("0.50"), D("100"))])
        parent = await engine.submit(request(bucket))
        await deliver(kalshi, poly)
        managed = engine.orders.get(parent.id)
        first_k, first_p = list(kalshi.orders)[0], list(poly.orders)[0]

        engine.set_book(K, BookState(asks=[(D("0.40"), D("30"))]))
        await engine.on_book({"market_id": K})
        await deliver(kalshi, poly)
        assert kalshi.orders[first_k].status == OrderStatus.CANCELED
        assert [(o.price, o.amount) for o in open_orders(kalshi)] == [(D("0.40"), D("30"))]
        assert list(poly.orders) == [first_p] and poly.orders[first_p].status == OrderStatus.OPEN
        assert managed.rounds == 1

    async def test_the_minimum_stay_holds_a_leg_still(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        kalshi.set_book(K, asks=[(D("0.50"), D("100"))])
        poly.set_book(P, asks=[(D("0.50"), D("100"))])
        await engine.submit(request(bucket, params={"min_stay_s": 10}))
        first_k = list(kalshi.orders)[0]

        engine.set_book(K, BookState(asks=[(D("0.40"), D("30"))]))
        await engine.on_book({"market_id": K})
        assert kalshi.orders[first_k].status == OrderStatus.OPEN, "not sat long enough"

        clock.tick(11)
        await engine.on_book({"market_id": K})
        await deliver(kalshi)
        assert kalshi.orders[first_k].status == OrderStatus.CANCELED
        assert [o.price for o in open_orders(kalshi)] == [D("0.40")]


class TestStopping:
    async def test_nothing_inside_the_limit_stops_it_and_says_so(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.50"), D("100"))])
        book(poly, engine, P, asks=[(D("0.52"), D("100"))])
        parent = await engine.submit(request(bucket))
        managed = engine.orders.get(parent.id)
        assert parent.status == OrderStatus.CANCELED
        assert managed.stop_reason == "worst_price" and managed.report()["unfilled"] == "50"
        assert kalshi.orders == {} and poly.orders == {}

    async def test_max_rounds_stops_it_with_a_partial(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        kalshi.set_book(K, asks=[(D("0.41"), D("10"))])
        poly.set_book(P, asks=[(D("0.50"), D("100"))])
        # A fill re-plans but changes no leg, so it is not a round; the book move is round 1.
        parent = await engine.submit(request(bucket, params={"max_rounds": 1}))
        await deliver(kalshi, poly)
        managed = engine.orders.get(parent.id)
        assert managed.filled == D("10") and managed.state == "working" and managed.rounds == 0
        engine.set_book(K, BookState(asks=[(D("0.40"), D("30"))]))
        await engine.on_book({"market_id": K})
        await deliver(kalshi, poly)
        assert managed.state == "canceled" and managed.stop_reason == "max_rounds"
        assert open_orders(kalshi) == [] and open_orders(poly) == []

    async def test_cancelling_the_parent_pulls_every_leg_on_every_venue(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        kalshi.set_book(K, asks=[(D("0.50"), D("100"))])
        poly.set_book(P, asks=[(D("0.50"), D("100"))])
        parent = await engine.submit(request(bucket))
        assert open_orders(kalshi) and open_orders(poly)
        await engine.orders.cancel(parent.id)
        assert open_orders(kalshi) == [] and open_orders(poly) == []
        assert engine.orders.get(parent.id).state == "canceled"

    async def test_a_halt_on_one_member_venue_reaches_it(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        book(kalshi, engine, K, asks=[(D("0.41"), D("30"))])
        book(poly, engine, P, asks=[(D("0.42"), D("100"))])
        kalshi.set_book(K, asks=[(D("0.50"), D("100"))])
        poly.set_book(P, asks=[(D("0.50"), D("100"))])
        parent = await engine.submit(request(bucket))
        halted = await engine.orders.on_halt("test", scope="polymarket")
        assert halted == 1
        assert open_orders(kalshi) == [] and open_orders(poly) == []
        assert engine.orders.get(parent.id).state == "canceled"


class TestPosition:
    async def test_the_rollup_nets_members_in_bucket_terms(self, tmp_path):
        engine, kalshi, poly, clock, bucket = await build(tmp_path, flip_p=True)
        try:
            book(poly, engine, P, bids=[(D("0.70"), D("50"))])       # NO ask 0.30 on the flipped member
            book(kalshi, engine, K, asks=[(D("0.35"), D("50"))])
            await engine.submit(request(bucket, amount=D("60"), price=D("0.40")))
            await deliver(kalshi, poly)
            position = await engine.bucket_position(bucket.id)
            assert position["side"] == "long" and position["contracts"] == "60"
            by_market = {m["market_id"]: m for m in position["members"]}
            # 50 were cheaper on Polymarket's NO side at 0.30; the last 10 came from Kalshi at 0.35.
            assert by_market[P]["contracts"] == "-50" and by_market[P]["bucket_contracts"] == "50"
            assert by_market[P]["entry_price"] == "0.30", "sold YES at 0.70 is long the bucket at 0.30"
            assert by_market[K]["contracts"] == "10" and by_market[K]["entry_price"] == "0.35"
            assert D(position["entry_price"]) == (D("0.30") * 50 + D("0.35") * 10) / 60
            assert (await engine.bucket_position(bucket.id, book="other"))["side"] == "flat"
        finally:
            await engine.stop()


class TestRefusals:
    async def test_only_a_priced_market_order_may_target_a_bucket(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        with pytest.raises(BadRequest, match="market order"):
            await engine.submit(request(bucket, type=OrderType.LIMIT))
        with pytest.raises(BadRequest, match="worst price"):
            await engine.submit(request(bucket, price=None))

    async def test_an_unknown_or_archived_bucket_is_refused(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        with pytest.raises(BadRequest):
            await engine.submit(request(bucket, market_id="bucket:nope"))
        await engine.journal.archive_bucket(bucket.id)
        with pytest.raises(BadRequest):
            await engine.submit(request(bucket))

    async def test_a_member_on_a_venue_without_an_adapter_is_refused(self, setup):
        engine, kalshi, poly, clock, bucket = setup
        other = await engine.save_bucket(Bucket(book="alpha", name="u", members=[
            BucketMember(market_id=K), BucketMember(market_id="polymarket_us:slug")]))
        with pytest.raises(BadRequest, match="polymarket_us"):
            await engine.submit(request(other))
