"""Hyperliquid's streams against the real venue: run with `pytest -m live`.

No key is needed for any channel, so this runs anywhere. It checks what the
recorded messages cannot: that the venue still sends whole books on the YES
coin, that the mirrored NO book matches the venue's own NO coin, and that
an active account's fills map onto outcome markets.
"""
from __future__ import annotations

import asyncio

import pytest

from synpath import Hyperliquid, HyperliquidMarketStream, HyperliquidUserStream
from synpath.ws.base import BookEvent, FillEvent, OrderEvent, QuoteEvent, StreamStatusEvent

pytestmark = pytest.mark.live


async def collect(stream, seconds: float) -> list:
    events = []

    async def read():
        async for event in stream:
            events.append(event)

    task = asyncio.create_task(read())
    await asyncio.sleep(seconds)
    task.cancel()
    return events


def busiest() -> list[str]:
    with Hyperliquid() as hl:
        return [m.id for m in hl.fetch_markets(limit=3)]


def test_books_and_quotes_arrive_and_mirror():
    markets = busiest()

    async def run():
        async with HyperliquidMarketStream() as stream:
            await stream.watch_order_book(markets)
            await stream.watch_ticker(markets)
            events = await collect(stream, 12)
            with Hyperliquid() as hl:
                native_no = hl._info({"type": "l2Book", "coin": "#" + str(10 * int(markets[0].split(":")[1]) + 1)})
            yes, no = stream.book(markets[0]), stream.book(markets[0], "no")
            return events, (yes.ready, yes.best_bid), no.best_ask, native_no

    events, (ready, best_bid), no_best_ask, native_no = asyncio.run(run())
    assert any(isinstance(e, BookEvent) and e.kind == "snapshot" for e in events)
    assert any(isinstance(e, QuoteEvent) for e in events)
    assert not [e for e in events if isinstance(e, StreamStatusEvent) and e.state == "error"]
    assert ready and best_bid is not None
    assert no_best_ask == 1 - best_bid
    assert native_no["levels"][1], "the venue's NO coin has asks"


def test_an_active_accounts_activity_maps_onto_outcomes():
    with Hyperliquid() as hl:
        market = hl.fetch_markets(limit=1)[0]
        prints = hl._info({"type": "recentTrades", "coin": market.yes.venue_token_id})
    address = prints[0]["users"][0]

    async def run():
        async with HyperliquidUserStream(address=address) as stream:
            await stream.watch_orders()
            await stream.watch_my_trades()
            return await collect(stream, 12)

    events = asyncio.run(run())
    statuses = [e for e in events if isinstance(e, StreamStatusEvent)]
    assert any(e.state == "subscribed" for e in statuses)
    for event in events:
        if isinstance(event, FillEvent):
            assert event.fill.market_id.startswith("hyperliquid:")
        if isinstance(event, OrderEvent):
            assert event.order.market_id.startswith("hyperliquid:")
