"""Limitless against the real venue: run with `pytest -m live`. No key needed."""
from __future__ import annotations

import pytest

from synpath import Limitless

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Limitless() as lmts:
        yield lmts


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_is_open_and_tokenised(markets):
    assert markets and all(m.status == "open" and m.yes.venue_token_id and m.no.venue_token_id for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_book_trades_candles_and_fees(venue, markets):
    market = max(markets, key=lambda m: m.stats.volume_total or 0)
    book = venue.fetch_order_book(market.id)
    assert book.bids or book.asks
    assert venue.fetch_trades(market.id, limit=5) is not None
    assert all(c.price_source == "sampled_mid" for c in venue.fetch_ohlcv(market.id, limit=12))
    assert venue.fetch_fee_schedule(market.id).fee_type == "limitless_curve"


def test_events_hold_their_markets(venue):
    events = venue.fetch_events(limit=5)
    assert events and all(e.markets for e in events)
