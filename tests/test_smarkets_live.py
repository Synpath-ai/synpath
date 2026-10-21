"""Smarkets against the real venue: run with `pytest -m live`. No account needed.

Kept light: Smarkets allows anonymous callers about 60 requests a minute, and
20 a minute on `/quotes/`."""
from __future__ import annotations

import pytest

from synpath import Smarkets

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Smarkets(login=False) as smk:
        yield smk


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(domain="football", scope="single_event", limit=20)


def test_the_catalog_is_open_and_named(markets):
    assert markets and all(m.status in ("open", "unopened") and m.yes.label for m in markets)
    assert all(m.id.startswith("smarkets:") and m.event_id for m in markets)


def test_books_are_yes_and_never_crossed(venue, markets):
    books = venue.fetch_order_books([m.id for m in markets])
    assert books
    for book in books.values():
        assert book.info["currency"] == "GBP" and book.info["delayed"] is True
        if book.bids and book.asks:
            assert book.bids[0].price < book.asks[0].price
        assert all(0 < level.price < 1 and level.size > 0 for level in book.bids + book.asks)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(domain="football", scope="single_event", limit=20, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}
