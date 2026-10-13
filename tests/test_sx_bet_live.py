"""SX Bet against the real venue: run with `pytest -m live`. No key needed."""
from __future__ import annotations

import pytest

from synpath import SxBet

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with SxBet() as sx:
        yield sx


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_is_open_and_named(markets):
    assert markets and all(m.status == "open" and m.yes.label and m.no.label for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_a_traded_market_has_a_book_trades_candles_and_fees(venue):
    tape = venue._get("/trades-v3/public", {"perPage": 20})["trades"]
    market_hash = next(t["marketHash"] for t in tape if not t.get("isParlay"))
    market = venue.fetch_market(market_hash)
    assert market.venue_market_id == market_hash.lower()
    venue.fetch_order_book(market.id)
    assert venue.fetch_trades(market.id, limit=5) is not None
    assert all(c.price_source == "trade" for c in venue.fetch_ohlcv(market.id, limit=12))
    assert venue.fetch_fee_schedule(market.id).fee_type == "profit_on_win"


def test_events_hold_their_markets(venue):
    events = venue.fetch_events(limit=5)
    assert events and all(e.markets and {m.event_id for m in e.markets} == {e.id} for e in events)


def test_search_finds_fixtures(venue):
    markets = venue.fetch_markets(query="real madrid", limit=5)
    assert all("real madrid" in m.title.lower() for m in markets)
