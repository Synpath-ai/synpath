"""The router on the Rust core against the router in Python.

`plan` merges and walks on the core when it is installed, and keeps the
leg sizing, the floors and the re-walks in Python. These tests plan random
buckets both ways -- random members, flips, venue rules, fee curves and
floors, books of every kind `plan` takes -- and require the same plan, to
the last digit of every price and amount.
"""
from __future__ import annotations

import random
from decimal import Decimal as D

import pytest

from synpath import _native
from synpath.bucket import Bucket, BucketMember
from synpath.engine import router
from synpath.trading.types import Precision, Side
from synpath.types import OrderBook, OrderLevel
from synpath.ws.base import PyLocalBook

pytestmark = pytest.mark.skipif(_native.core is None, reason="the Rust core is not built")

IDS = ["kalshi:KX-1", "polymarket:123", "opinion:77", "polymarket_us:tec-a"]


def python_plan(monkeypatch, *args, **kwargs):
    monkeypatch.setattr(_native, "core", None)
    try:
        return router.plan(*args, **kwargs)
    finally:
        monkeypatch.undo()


def as_tuple(plan: router.Plan):
    return (
        plan.side, plan.amount, plan.limit, plan.unfilled, plan.reason,
        [(l.market_id, l.flip, l.side, str(l.price), str(l.amount), str(l.bucket_price), str(l.net_price),
          str(l.fee), str(l.fee_floor)) for l in plan.legs],
    )


def random_book(rng: random.Random, kind: str, member: BucketMember):
    def level(low, high):
        price = D(rng.randint(low, high)) / rng.choice([100, 1000])
        size = D(rng.randint(0, 5000)) / rng.choice([1, 10, 100])
        return price, size
    bids = [level(5, 49) for _ in range(rng.randint(0, 40))]
    asks = [level(51, 95) for _ in range(rng.randint(0, 40))]
    if kind in ("rust", "python"):
        book = _native.core.LocalBook() if kind == "rust" else PyLocalBook()
        book.replace(bids, asks)
        return book
    if kind == "orderbook":
        return OrderBook(
            market_id=member.market_id, venue=member.market_id.split(":")[0],
            bids=sorted((OrderLevel(price=float(p), size=float(s)) for p, s in bids if s > 0), key=lambda l: -l.price),
            asks=sorted((OrderLevel(price=float(p), size=float(s)) for p, s in asks if s > 0), key=lambda l: l.price),
        )
    return router.view(_rust_book(bids, asks), member)


def _rust_book(bids, asks):
    book = _native.core.LocalBook()
    book.replace(bids, asks)
    return book


def random_case(rng: random.Random):
    members = [BucketMember(market_id=m, flip=rng.random() < 0.4) for m in rng.sample(IDS, rng.randint(2, 4))]
    bucket = Bucket(book="b", name="t", members=members)
    kinds = ["rust", "python", "orderbook", "view"]
    books = {m.market_id: random_book(rng, rng.choice(kinds), m) for m in members if rng.random() < 0.9}
    precision = {}
    for m in members:
        precision[m.market_id] = Precision(
            tick=rng.choice([D("0.01"), D("0.001")]), min_amount=D(rng.choice([1, 5, 10])),
            whole_contracts=rng.random() < 0.4, amount_step=rng.choice([None, D("0.01"), D("0.1")]),
            min_notional=rng.choice([None, D("5")]),
        )
    rates = {m.market_id: rng.choice([D("0"), D("0.07"), D("0.0175"), D("0.02")]) for m in members}
    long_digits = rng.random() < 0.1

    def fee(market_id, price, contracts):
        charged = rates[market_id] * contracts * price * (1 - price)
        if long_digits:
            charged = charged / 3           # many digits: the products overflow Python's 28
        return charged

    floors = {m.market_id: D("0.25") for m in members if rng.random() < 0.3}
    side = rng.choice([Side.BUY, Side.SELL])
    amount = D(rng.randint(1, 20000)) / rng.choice([1, 10])
    limit = D(rng.randint(30, 99)) / 100 if side == Side.BUY else D(rng.randint(1, 70)) / 100
    return bucket, books, precision, fee, dict(side=side, amount=amount, limit=limit, floors=floors)


@pytest.mark.parametrize("seed", range(300))
def test_random_plans_match(seed, monkeypatch):
    rng = random.Random(seed)
    bucket, books, precision, fee, kwargs = random_case(rng)
    rust = router.plan(bucket, books, precision, fee, **kwargs)
    python = python_plan(monkeypatch, bucket, books, precision, fee, **kwargs)
    assert as_tuple(rust) == as_tuple(python)


def test_the_core_does_the_walk(monkeypatch):
    """Guards the test above: most cases must actually run on the core."""
    calls = {"python": 0}
    walk = router._walk_levels

    def counted(*args, **kwargs):
        calls["python"] += 1
        return walk(*args, **kwargs)

    monkeypatch.setattr(router, "_walk_levels", counted)
    for seed in range(100):
        rng = random.Random(seed)
        bucket, books, precision, fee, kwargs = random_case(rng)
        router.plan(bucket, books, precision, fee, **kwargs)
    assert calls["python"] < 25        # only the long-digit cases fall back


@pytest.mark.parametrize("flip", [False, True])
def test_a_rust_book_is_viewed_alike(flip, monkeypatch):
    rng = random.Random(3)
    bids = [(D(rng.randint(1, 49)) / 100, D(rng.randint(0, 50))) for _ in range(30)]
    asks = [(D(rng.randint(51, 99)) / 100, D(rng.randint(0, 50))) for _ in range(30)]
    member = BucketMember(market_id=IDS[0], flip=flip)
    fast = router.view(_rust_book(bids, asks), member)
    monkeypatch.setattr(_native, "core", None)
    slow_book = PyLocalBook()
    slow_book.replace(bids, asks)
    slow = router.view(slow_book, member)
    assert fast == slow
    assert [(str(p), str(s)) for p, s in fast.asks] == [(str(p), str(s)) for p, s in slow.asks]


def test_a_fee_that_is_a_float_is_left_to_python(monkeypatch):
    bucket = Bucket(book="b", name="t", members=[BucketMember(market_id=IDS[0]), BucketMember(market_id=IDS[1])])
    books = {m.market_id: _rust_book([(D("0.4"), D("10"))], [(D("0.6"), D("10"))]) for m in bucket.members}
    precision = {m.market_id: Precision(tick=D("0.01"), min_amount=D("1")) for m in bucket.members}
    with pytest.raises(TypeError):
        router.plan(bucket, books, precision, lambda m, p, c: 0.01, side=Side.BUY, amount=D(5), limit=D("0.9"))
