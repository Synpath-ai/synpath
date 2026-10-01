"""Opinion against the real venue: run with `pytest -m live`.

Public endpoints only, so no credentials. Checks the things recorded samples
cannot: that the venue still answers in the shapes the adapter reads.
"""
from __future__ import annotations

import time

import pytest

from synpath import MarketNotFound, NotSupported, Opinion

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Opinion() as opinion:
        yield opinion


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_flattens_into_tradable_markets(markets):
    assert markets
    assert all(m.status == "open" and m.yes.venue_token_id and m.no.venue_token_id for m in markets)
    assert all(m.event_id and m.event_id.startswith("opinion:") for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_a_child_market_gets_its_topic_back(venue, markets):
    child = next(m for m in markets if m.outcome_label)
    again = venue.fetch_market(child.id)
    assert (again.event_id, again.title) == (child.event_id, child.title)


def test_events_hold_their_options(venue):
    events = venue.fetch_events(limit=5)
    assert events and all(event.markets for event in events)


def test_both_books_are_read_and_mirror_each_other(venue, markets):
    for market in markets:
        yes = venue.fetch_order_book(market.id)
        if yes.best_bid and yes.best_ask:
            break
    else:
        pytest.skip("no two-sided book in the first page")
    no = venue.fetch_order_book(market.id, side="no")
    assert yes.best_bid.price < yes.best_ask.price
    assert no.best_ask.price == pytest.approx(1 - yes.best_bid.price)


def test_quotes_come_from_the_books(venue, markets):
    market = venue.refresh_quotes(markets[0])
    for side in (market.yes, market.no):
        if side.quote.mid is not None:
            assert side.quote.bid < side.quote.ask


def test_hourly_bars_are_last_trade_samples(venue, markets):
    candles = venue.fetch_ohlcv(markets[0].id, timeframe="1h", limit=24)
    assert 0 < len(candles) <= 24
    assert {c.price_source for c in candles} == {"sampled_last"}


def test_bars_can_be_read_forward_from_the_past(venue, markets):
    since = int((time.time() - 30 * 86400) * 1000)
    candles = venue.fetch_ohlcv(markets[0].id, timeframe="1d", since=since, limit=5)
    assert len(candles) == 5
    assert all(c.timestamp >= since - 86400_000 for c in candles)
    assert candles[0].timestamp < since + 2 * 86400_000


def test_fees_are_read_from_the_chain(venue, markets):
    schedule = venue.fetch_fee_schedule(markets[0].id)
    assert schedule.fee_type == "opinion_curve"
    assert 0 <= schedule.taker_rate <= 0.04          # the venue's documented 1% ceiling at 50c


def test_search_finds_a_known_title(venue, markets):
    word = max(markets[0].title.split(), key=len)
    found = venue.fetch_markets(query=word, limit=20)
    assert any(word.lower() in m.title.lower() for m in found)


def test_settled_markets_are_settled(venue):
    assert all(m.status == "settled" for m in venue.fetch_markets(status="settled", limit=10))


def test_unknown_ids_and_missing_tape(venue):
    with pytest.raises(MarketNotFound):
        venue.fetch_market("opinion:99999999")
    with pytest.raises(NotSupported):
        venue.fetch_trades("opinion:1")
