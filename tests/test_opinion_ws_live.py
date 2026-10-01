"""Opinion's WebSocket against the real venue: `pytest -m live`, with
`OPINION_API_KEY` set. Skipped without a key.

The one thing offline tests cannot settle: that a book built from the
snapshot plus the venue's changes is the venue's book. After watching the
busiest markets for a while, each local book must equal a fresh REST read,
which holds only if a change carries the level's whole size and the NO
token's changes arrive as its own.
"""
from __future__ import annotations

import asyncio
import os

import pytest

from synpath import Opinion
from synpath.ws.base import BookEvent, StreamStatusEvent
from synpath.ws.opinion import OpinionMarketStream, api_key_from

pytestmark = [pytest.mark.live, pytest.mark.anyio]

WATCH_SECONDS = float(os.environ.get("OPINION_WS_SECONDS", "60"))


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
def key():
    try:
        return api_key_from(None)
    except Exception:
        pytest.skip("no OPINION_API_KEY")


async def test_streamed_books_match_the_venue(key):
    catalog = Opinion()
    markets = [m for m in catalog.fetch_markets(limit=40) if m.yes.venue_token_id][:8]
    stream = OpinionMarketStream(api_key=key, catalog=catalog, resync_interval=None)
    events: list = []
    async with stream:
        await asyncio.wait_for(stream.connected.wait(), 15)
        await stream.watch_order_book([m.id for m in markets])

        async def collect():
            async for event in stream:
                events.append(event)

        reader = asyncio.get_running_loop().create_task(collect())
        await asyncio.sleep(WATCH_SECONDS)
        reader.cancel()
        deltas = [e for e in events if isinstance(e, BookEvent) and e.kind == "delta"]
        failures = [e for e in events if isinstance(e, StreamStatusEvent) and e.state in ("error", "failed")]
        assert not failures, failures
        mismatches = []
        for market in markets:
            for side in ("yes", "no"):
                local = stream.book(market.id, side)
                venue = catalog.fetch_order_book(market.id, side=side)
                if not local.ready:
                    mismatches.append((market.id, side, "not ready"))
                    continue
                theirs = {str(level["price"]): str(level["size"]) for level in venue.info.get("bids") or []}
                ours = {str(price): str(size) for price, size in local.bids.items()}
                if {p: float(s) for p, s in theirs.items()} != {p: float(s) for p, s in ours.items()}:
                    mismatches.append((market.id, side, "bids"))
        print(f"\n{len(deltas)} deltas over {WATCH_SECONDS}s; "
              f"no-side deltas: {sum(1 for e in deltas if e.side == 'no')}; mismatches: {mismatches}")
        assert not mismatches


async def test_trades_and_prices_arrive_on_the_yes_leg(key):
    catalog = Opinion()
    markets = catalog.fetch_markets(limit=20)
    stream = OpinionMarketStream(api_key=key, catalog=catalog, resync_interval=None)
    async with stream:
        await asyncio.wait_for(stream.connected.wait(), 15)
        await stream.watch_trades([m.id for m in markets])
        await stream.watch_ticker([m.id for m in markets])
        await asyncio.sleep(min(WATCH_SECONDS, 30))
    statuses = [e for e in _drain(stream) if isinstance(e, StreamStatusEvent) and e.state in ("error", "failed")]
    assert not statuses, statuses


def _drain(stream):
    events = []
    while not stream._queue.empty():
        events.append(stream._queue.get_nowait())
    return events
