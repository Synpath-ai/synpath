"""Novig against the real venue's public catalog: run with `pytest -m live`.
No key needed. Kept light: the edge throttles each IP."""
from __future__ import annotations

import pytest

from synpath import Novig

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Novig() as nv:
        yield nv


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(market_type="MONEY", limit=20)


def test_the_catalog_is_open_and_named(markets):
    assert markets and all(m.status in ("open", "closed") and m.yes.label and m.no.label for m in markets)
    assert all(m.id.startswith("novig:") and m.event_id and m.title for m in markets)


def test_books_are_yes_and_never_crossed(venue, markets):
    quoted = [m for m in markets if m.yes.quote.bid is not None][:5]
    books = venue.fetch_order_books([m.id for m in quoted])
    assert books
    for market in quoted:
        book = books[market.id]
        if book.bids and book.asks:
            assert book.bids[0].price < book.asks[0].price
        assert all(0 < level.price < 1 and level.size > 0 for level in book.bids + book.asks)


def test_the_tape_reads_oldest_first(venue, markets):
    traded = next((m for m in markets if (m.stats.volume_total or 0) > 0), None)
    if traded is None:
        pytest.skip("nothing traded on the first page")
    trades = venue.fetch_trades(traded.id, limit=20)
    assert trades == sorted(trades, key=lambda t: t.timestamp)
    assert all(0 < t.price < 1 and t.side in ("buy", "sell") for t in trades)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(market_type="MONEY", limit=20, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}
