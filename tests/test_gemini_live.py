"""Gemini Predictions against the real venue: run with `pytest -m live`. No
key needed; every call is a public read."""
from __future__ import annotations

import asyncio

import pytest

from synpath import Gemini
from synpath.ws.base import BookEvent
from synpath.ws.gemini import GeminiMarketStream

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def venue():
    with Gemini() as gemini:
        yield gemini


@pytest.fixture(scope="module")
def markets(venue):
    return venue.fetch_markets(limit=40)


def test_the_catalog_is_open_and_named(markets):
    assert markets and all(m.status == "open" and m.yes.label and m.id.startswith("gemini:GEMI-") for m in markets)


def test_paging_neither_repeats_nor_drops(venue, markets):
    following = venue.fetch_markets(limit=40, cursor=markets.next_cursor)
    assert not {m.id for m in markets} & {m.id for m in following}


def test_sports_filters_return_their_family(venue):
    found = venue.fetch_markets(sport="basketball", sports_market_type="moneyline", limit=10)
    assert all(m.info["sports_market"]["type"] == "moneyline" for m in found)


def test_a_market_has_a_book_trades_candles_and_fees(venue, markets):
    market = venue.fetch_market(markets[0].id)
    assert market.id == markets[0].id
    book = venue.fetch_order_book(market.id)
    assert all(0 < level.price < 1 for level in book.bids + book.asks)
    assert venue.fetch_trades(market.id, limit=5) is not None
    venue.fetch_ohlcv(market.id, timeframe="1h", limit=6)
    assert venue.fetch_fee_schedule(market.id).taker_rate in (0.07, 0.05)


def test_the_stream_sends_a_book(markets):
    async def go():
        async with GeminiMarketStream() as stream:
            await stream.watch_order_book([markets[0].id])
            async for event in stream:
                if isinstance(event, BookEvent):
                    return event

    event = asyncio.run(asyncio.wait_for(go(), timeout=30))
    assert event.kind == "snapshot" and event.market_id == markets[0].id
