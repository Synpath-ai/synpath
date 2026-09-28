"""The router is pure: books in, a plan out, no venue touched."""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from synpath.bucket import Bucket, BucketMember
from synpath.engine.router import merge, plan
from synpath.errors import BadRequest
from synpath.trading.types import Precision, Side
from synpath.types import OrderBook, OrderLevel

K, P = "kalshi:KX-A", "polymarket:123"


def book(market_id, side="yes", *, asks=(), bids=()):
    venue = market_id.split(":")[0]
    return OrderBook(market_id=market_id, venue=venue, side=side,
                     asks=[OrderLevel(price=p, size=s) for p, s in asks],
                     bids=[OrderLevel(price=p, size=s) for p, s in bids])


def bucket(flip_p=False):
    return Bucket(book="alpha", name="t", members=[BucketMember(market_id=K), BucketMember(market_id=P, flip=flip_p)])


NO_FEE = lambda market_id, price, contracts: D("0")
PREC = {K: Precision(tick=D("0.01"), min_amount=D("1"), whole_contracts=True),
        P: Precision(tick=D("0.001"), min_amount=D("5"), amount_step=D("0.01"))}


class TestMerge:
    def test_asks_sort_cheapest_first_across_venues(self):
        asks, _ = merge(bucket(), {K: book(K, asks=[(0.42, 100)]), P: book(P, asks=[(0.41, 50), (0.43, 10)])}, NO_FEE)
        assert [(l.market_id, l.price) for l in asks] == [(P, D("0.41")), (K, D("0.42")), (P, D("0.43"))]

    def test_a_flipped_member_is_read_on_the_no_side(self):
        # A NO book is taken as is; a YES book is turned into the NO book:
        # NO asks are 1 - YES bids. A NO book for an unflipped member is an error.
        asks, _ = merge(bucket(flip_p=True), {P: book(P, "no", asks=[(0.3, 1)])}, NO_FEE)
        assert asks[0].price == D("0.3")
        asks, bids = merge(bucket(flip_p=True), {P: book(P, "yes", bids=[(0.7, 5)], asks=[(0.75, 2)])}, NO_FEE)
        assert [(l.price, l.size) for l in asks] == [(D("0.3"), D("5"))]
        assert [(l.price, l.size) for l in bids] == [(D("0.25"), D("2"))]
        with pytest.raises(ValueError):
            merge(bucket(), {P: book(P, "no", asks=[(0.3, 1)])}, NO_FEE)

    def test_fees_change_the_order(self):
        fee = lambda m, price, n: D("0.02") * n if m == K else D("0")
        asks, _ = merge(bucket(), {K: book(K, asks=[(0.41, 10)]), P: book(P, asks=[(0.42, 10)])}, fee)
        assert [l.market_id for l in asks] == [P, K]
        assert asks[1].net_price == D("0.43")


class TestPlan:
    def test_walks_best_net_first_and_never_over_allocates(self):
        books = {K: book(K, asks=[(0.41, 30)]), P: book(P, asks=[(0.42, 100)])}
        out = plan(bucket(), books, PREC, NO_FEE, side=Side.BUY, amount=D("50"), limit=D("0.45"))
        assert {(l.market_id, l.amount) for l in out.legs} == {(K, D("30")), (P, D("20"))}
        assert out.allocated == D("50") and out.unfilled == 0 and out.reason == ""
        assert out.expected_net_price == (D("0.41") * 30 + D("0.42") * 20) / 50

    def test_stops_at_the_limit(self):
        books = {K: book(K, asks=[(0.41, 10), (0.50, 100)]), P: book(P, asks=[(0.60, 100)])}
        out = plan(bucket(), books, PREC, NO_FEE, side=Side.BUY, amount=D("50"), limit=D("0.45"))
        assert [(l.market_id, l.amount) for l in out.legs] == [(K, D("10"))]
        assert out.unfilled == D("40") and out.reason == "worst_price"

    def test_limit_is_on_the_net_price(self):
        fee = lambda m, price, n: D("0.05") * n
        books = {K: book(K, asks=[(0.42, 10)])}
        out = plan(bucket(), books, PREC, fee, side=Side.BUY, amount=D("10"), limit=D("0.45"))
        assert out.legs == [] and out.reason == "worst_price"

    def test_a_leg_under_the_minimum_is_dropped_and_its_size_moves_on(self):
        # Polymarket minimum is 5; only 3 available there at the best price.
        books = {P: book(P, asks=[(0.40, 3)]), K: book(K, asks=[(0.41, 100)])}
        out = plan(bucket(), books, PREC, NO_FEE, side=Side.BUY, amount=D("20"), limit=D("0.45"))
        assert [(l.market_id, l.amount) for l in out.legs] == [(K, D("20"))]
        assert out.unfilled == 0

    def test_whole_contracts_round_down(self):
        books = {K: book(K, asks=[(0.41, 7.5)])}
        out = plan(bucket(), books, PREC, NO_FEE, side=Side.BUY, amount=D("7.5"), limit=D("0.45"))
        assert out.legs[0].amount == D("7") and out.unfilled == D("0.5")

    def test_sell_walks_bids_richest_first_and_rounds_price_up(self):
        books = {K: book(K, bids=[(0.60, 10)]), P: book(P, bids=[(0.615, 10)])}
        out = plan(bucket(), books, PREC, NO_FEE, side=Side.SELL, amount=D("15"), limit=D("0.55"))
        assert [(l.market_id, l.amount, l.price) for l in out.legs] == [(P, D("10"), D("0.615")), (K, D("5"), D("0.60"))]

    def test_a_flipped_member_gets_the_opposite_side_at_one_minus(self):
        # Bucket buy at 0.30 on the NO book == sell YES at 0.70 on the venue.
        books = {P: book(P, "no", asks=[(0.30, 10)]), K: book(K, asks=[(0.35, 10)])}
        out = plan(bucket(flip_p=True), books, PREC, NO_FEE, side=Side.BUY, amount=D("10"), limit=D("0.40"))
        leg = out.legs[0]
        assert (leg.market_id, leg.flip, leg.side, leg.price, leg.bucket_price) == (P, True, Side.SELL, D("0.70"), D("0.30"))

    def test_buy_price_rounds_down_to_the_tick(self):
        books = {K: book(K, asks=[(0.4149, 10)])}
        out = plan(bucket(), books, PREC, NO_FEE, side=Side.BUY, amount=D("10"), limit=D("0.45"))
        assert out.legs[0].price == D("0.41")

    def test_no_liquidity_says_so(self):
        out = plan(bucket(), {}, PREC, NO_FEE, side=Side.BUY, amount=D("10"), limit=D("0.45"))
        assert out.legs == [] and out.unfilled == D("10") and out.reason == "liquidity"


class TestExports:
    def test_the_bucket_is_a_top_level_name(self):
        import synpath
        assert synpath.Bucket is Bucket and synpath.BucketMember is BucketMember


class TestBucketModel:
    def test_check_refuses_bare_ids_duplicates_and_singletons(self):
        with pytest.raises(BadRequest):
            Bucket(book="a", name="n", members=[BucketMember(market_id=K)]).check()
        with pytest.raises(BadRequest):
            Bucket(book="a", name="n", members=[BucketMember(market_id=K), BucketMember(market_id=K)]).check()
        with pytest.raises(BadRequest):
            Bucket(book="a", name="n", members=[BucketMember(market_id=K), BucketMember(market_id="KX-B")]).check()
        bucket().check()

    def test_orientation_map_is_its_own_inverse(self):
        m = BucketMember(market_id=P, flip=True)
        side, price = m.to_member(Side.BUY, D("0.30"))
        assert (side, price) == (Side.SELL, D("0.70"))
        assert m.to_bucket(side, price) == (Side.BUY, D("0.30"))
        assert m.book_side() == "no" and BucketMember(market_id=K).book_side() == "yes"

    def test_ids_and_venues(self):
        b = bucket()
        assert b.market_id == f"bucket:{b.id}" and b.venues() == {"kalshi", "polymarket"}
