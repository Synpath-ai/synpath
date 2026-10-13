"""SX Bet's market stream against the real venue: run with `pytest -m live`.
Every SX Bet channel needs a token, so this needs `SX_BET_API_KEY`."""
from __future__ import annotations

import asyncio
import os

import pytest

from synpath import SxBet, SxBetMarketStream
from synpath.ws.base import BookEvent

pytestmark = pytest.mark.live

KEY = os.environ.get("SX_BET_API_KEY")


@pytest.mark.skipif(not KEY, reason="SX_BET_API_KEY not set")
def test_a_book_arrives_and_mirrors():
    tape = SxBet()._get("/trades-v3/public", {"perPage": 20})["trades"]
    market_id = "sx_bet:" + next(t["marketHash"] for t in tape if not t.get("isParlay")).lower()

    async def go():
        async with SxBetMarketStream(api_key=KEY) as stream:
            await stream.watch_order_book([market_id])
            async for event in stream:
                if isinstance(event, BookEvent):
                    yes, no = stream.book(market_id), stream.book(market_id, "no")
                    assert yes.ready and event.kind == "snapshot"
                    if yes.best_bid is not None:
                        assert no.best_ask == 1 - yes.best_bid
                    return

    asyncio.run(asyncio.wait_for(go(), 60))
