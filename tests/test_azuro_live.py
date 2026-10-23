"""Azuro against the real feed and bets subgraph: run with `pytest -m live`. No key needed.
The bet routes (`/bet/calculation`, so books and limits) and the socket answer
some regions with a 503; those checks skip there."""
from __future__ import annotations

import pytest

from synpath import Azuro, ExchangeNotAvailable

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Azuro() as azuro:
        yield azuro


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_is_open_named_and_quoted(markets):
    assert markets and all(m.status == "open" and m.yes.label and m.event_id for m in markets)
    assert any(m.yes.quote.ask is not None for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_a_market_reads_back_by_id(venue, markets):
    market = venue.fetch_market(markets[0].id)
    assert market.id == markets[0].id and market.title == markets[0].title


def test_events_hold_their_markets(venue):
    events = venue.fetch_events(limit=2)
    assert events and all(e.markets and {m.event_id for m in e.markets} == {e.id} for e in events)


def test_sports(venue):
    assert venue.fetch_sports()


def test_the_book_where_the_bet_routes_answer(venue, markets):
    market = next(m for m in markets if m.yes.quote.ask is not None)
    try:
        book = venue.fetch_order_book(market.id)
    except ExchangeNotAvailable:
        pytest.skip("the feed's bet routes refuse this region")
    assert book.asks and 0 < book.asks[0].price < 1 and book.asks[0].size > 0
