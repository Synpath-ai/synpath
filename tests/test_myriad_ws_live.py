"""Myriad's market stream against the real venue: run with `pytest -m live`.
Public channels: no key needed."""
from __future__ import annotations

import asyncio

import pytest

from synpath import Myriad, MyriadMarketStream
from synpath.ws.base import BookEvent

pytestmark = pytest.mark.live


def test_a_book_arrives_and_mirrors():
    market = max(Myriad().fetch_markets(limit=100, trading_model="ob"), key=lambda m: m.stats.volume_24h or 0)

    async def go():
        async with MyriadMarketStream() as stream:
            await stream.watch_order_book([market.id])
            async for event in stream:
                if isinstance(event, BookEvent) and event.kind == "snapshot" and event.side == "no":
                    yes, no = stream.book(market.id), stream.book(market.id, "no")
                    assert yes.ready and no.ready
                    if yes.best_ask is not None and no.best_bid is not None:
                        assert yes.best_ask + no.best_bid == 1
                    return

    asyncio.run(asyncio.wait_for(go(), 60))
