"""Myriad against the real venue: run with `pytest -m live`. No key needed."""
from __future__ import annotations

import pytest

from synpath import Myriad

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Myriad() as myriad:
        yield myriad


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40, sort="volume")


def test_the_catalog_holds_both_kinds_and_no_points(markets):
    assert markets and all(m.status == "open" for m in markets)
    assert {m.info["trading_model"] for m in markets} <= {"ob", "amm"}
    assert not any(m.info["points"] for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, sort="volume", cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_an_order_book_market(venue):
    market = next(iter(venue.fetch_markets(limit=40, trading_model="ob")))
    yes, no = venue.fetch_order_book(market.id), venue.fetch_order_book(market.id, side="no")
    if yes.best_ask and no.best_bid:
        assert yes.best_ask.price + no.best_bid.price == pytest.approx(1.0)
    assert venue.fetch_trades(market.id, limit=20) is not None
    assert venue.fetch_fee_schedule(market.id).fee_type == "bps_by_price"
    assert all(c.price_source == "sampled_mid" for c in venue.fetch_ohlcv(market.id, limit=12))


def test_an_amm_market_book_brackets_its_price(venue):
    market = next(iter(venue.fetch_markets(limit=40, trading_model="amm")))
    book = venue.fetch_order_book(market.id)
    assert book.info["depth_source"] == "amm" and book.best_bid.price <= market.yes.quote.mid <= book.best_ask.price


def test_sports_and_events(venue):
    assert all("sports" in m.tags for m in venue.fetch_markets(limit=10, topic="sports"))
    events = venue.fetch_events(limit=10)
    assert events and all(e.markets for e in events)
