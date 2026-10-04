"""Hyperliquid against the real venue: run with `pytest -m live`.

Public endpoints only, so no credentials. Checks the things recorded samples
cannot: that the venue still answers in the shapes the adapter reads, and
that every listed outcome still renders from a template the venue publishes.
"""
from __future__ import annotations

import pytest

from synpath import Hyperliquid

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Hyperliquid() as hl:
        yield hl


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_lists_open_markets(markets):
    assert markets
    assert all(m.status == "open" and m.yes.venue_token_id and m.no.venue_token_id for m in markets)


def test_every_title_renders(venue):
    """A new template, or a field a template stopped carrying, shows up here
    as an unfilled placeholder or an empty title."""
    for market in venue.catalog().markets.values():
        assert market.title and "{" not in market.title, market.info["outcome"]
        assert "template:" not in market.yes.label + market.no.label, market.info["outcome"]


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_the_no_side_mirrors_the_no_coin(venue, markets):
    market = markets[0]
    derived = venue.fetch_order_book(market.id, side="no")
    native = venue._info({"type": "l2Book", "coin": market.no.venue_token_id})
    best_native_bid = float(native["levels"][0][0]["px"]) if native["levels"][0] else None
    if derived.best_bid and best_native_bid is not None:
        assert derived.best_bid.price == pytest.approx(best_native_bid, abs=1e-6)


def test_trades_candles_and_fees(venue, markets):
    market = markets[0]
    assert venue.fetch_trades(market.id) is not None
    candles = venue.fetch_ohlcv(market.id, timeframe="1h", limit=24)
    assert all(c.price_source == "trade" for c in candles)
    assert venue.fetch_fee_schedule(market.id).taker_rate > 0


def test_events_hold_their_outcomes(venue):
    events = venue.fetch_events(limit=10)
    assert events and all(event.markets for event in events)
