"""predict.fun against the real venue: run with `pytest -m live`. Needs
PREDICT_FUN_API_KEY (mainnet); skips without it."""
from __future__ import annotations

import pytest

from synpath import PredictFun
from synpath.predict_fun import api_key_from

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not api_key_from(None), reason="PREDICT_FUN_API_KEY is not set"),
]


@pytest.fixture(scope="module")
def venue():
    with PredictFun() as pf:
        yield pf


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_is_quoted(markets):
    assert markets and all(m.status == "open" and m.yes.venue_token_id for m in markets)
    assert any(m.yes.quote.bid is not None for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_the_book_agrees_with_the_listing(venue, markets):
    market = next(m for m in markets if m.yes.quote.bid is not None)
    book = venue.fetch_order_book(market.id)
    assert book.best_bid is not None


def test_trades_candles_and_fees(venue, markets):
    market = markets[0]
    assert venue.fetch_trades(market.id, limit=5) is not None
    assert all(c.price_source == "sampled_mid" for c in venue.fetch_ohlcv(market.id, limit=12))
    assert venue.fetch_fee_schedule(market.id).taker_rate is not None


def test_events_hold_their_markets(venue):
    events = venue.fetch_events(limit=5)
    assert events and all(e.markets for e in events)
