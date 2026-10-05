"""predict.fun's market stream against the real venue: run with `pytest -m live`.
Needs PREDICT_FUN_API_KEY; skips without it. The wallet stream needs a wallet
JWT and is checked with the trading adapter."""
from __future__ import annotations

import asyncio

import pytest

from synpath import PredictFun, PredictFunMarketStream
from synpath.predict_fun import api_key_from
from synpath.ws.base import BookEvent, StreamStatusEvent

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not api_key_from(None), reason="PREDICT_FUN_API_KEY is not set"),
]


def test_books_arrive_and_the_heartbeat_keeps_the_connection():
    markets = [m.id for m in PredictFun().fetch_markets(limit=3)]

    async def run():
        stream = PredictFunMarketStream()
        events = []

        async def read():
            async for event in stream:
                events.append(event)

        async with stream:
            await stream.watch_order_book(markets)
            task = asyncio.create_task(read())
            await asyncio.sleep(35)    # past two server heartbeats
            book = stream.book(markets[0])
            state = (book.ready, book.best_bid, stream.stats.disconnects)
            task.cancel()
        return events, state

    events, (ready, best_bid, disconnects) = asyncio.run(run())
    assert any(isinstance(e, BookEvent) for e in events)
    assert not [e for e in events if isinstance(e, StreamStatusEvent) and e.state in ("error", "disconnected")]
    assert ready and best_bid is not None and disconnects == 0
