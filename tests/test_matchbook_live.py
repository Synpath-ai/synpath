"""Matchbook against the real venue: run with `pytest -m live`. No account needed.

Kept light: a handful of reads, inside the events group's 700 a minute."""
from __future__ import annotations

import pytest

from synpath import Matchbook

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Matchbook() as mb:
        yield mb


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(sport="Soccer", limit=20)


def test_the_catalog_is_open_and_named(markets):
    assert markets and all(m.status == "open" and m.yes.label for m in markets)
    assert all(m.id.startswith("matchbook:") and m.event_id for m in markets)


def test_books_are_yes_and_never_crossed(venue, markets):
    books = venue.fetch_order_books([m.id for m in markets])
    assert books
    for book in books.values():
        if book.bids and book.asks:
            assert book.bids[0].price < book.asks[0].price
        assert all(0 < level.price < 1 and level.size > 0 for level in book.bids + book.asks)


def test_one_runners_prices(venue, markets):
    book = venue.fetch_order_book(markets[0].id, depth=5)
    assert book.market_id == markets[0].id and len(book.bids) <= 5


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(sport="Soccer", limit=20, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}
