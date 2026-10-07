"""Limitless's market stream against the real venue: run with `pytest -m live`. No key needed."""
from __future__ import annotations

import asyncio

import pytest

from synpath import Limitless, LimitlessMarketStream
from synpath.ws.base import BookEvent

pytestmark = pytest.mark.live


def test_a_book_arrives_and_mirrors():
    market = max(Limitless().fetch_markets(limit=40), key=lambda m: m.stats.volume_total or 0)

    async def go():
        async with LimitlessMarketStream() as stream:
            await stream.watch_order_book([market.id])
            async for event in stream:
                if isinstance(event, BookEvent):
                    yes, no = stream.book(market.id), stream.book(market.id, "no")
                    assert yes.ready and event.kind == "snapshot"
                    if yes.best_bid is not None:
                        assert no.best_ask == 1 - yes.best_bid
                    return

    asyncio.run(asyncio.wait_for(go(), 60))
