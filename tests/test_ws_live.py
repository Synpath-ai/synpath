"""Streams against the real venues. Deselected by default.

`pytest -m live tests/test_ws_live.py` reads Polymarket's public market
channel. `pytest -m demo tests/test_ws_live.py` places and cancels an order
on the Kalshi demo environment and checks that what the stream reports
agrees with a REST read taken afterwards.
"""
from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from synpath.trading import OrderRequest, OrderStatus, Side
from synpath.trading.credentials import load_credentials
from synpath.ws.base import BookEvent, OrderEvent, StreamStatusEvent

D = Decimal
DOTENV = Path(__file__).resolve().parents[1] / ".env"


async def _collect(stream, until, timeout):
    events = []

    async def consume():
        async for event in stream:
            events.append(event)
            if until(events):
                return

    try:
        await asyncio.wait_for(consume(), timeout)
    except asyncio.TimeoutError:
        pass
    return events


@pytest.mark.live
def test_polymarket_market_channel_keeps_books_without_gaps():
    from synpath.ws.polymarket import PolymarketMarketStream

    rows = httpx.get("https://gamma-api.polymarket.com/markets/keyset", params={
        "closed": "false", "limit": 10, "order": "volume24hr", "ascending": "false"}, timeout=20).json()
    tokens = [t for m in (rows.get("markets") or rows)[:5] for t in json.loads(m["clobTokenIds"])]

    async def run():
        stream = PolymarketMarketStream()
        await stream.watch_order_book(tokens)
        try:
            events = await _collect(stream, lambda events: False, 20)
            return events, {t: stream.books[t].ready for t in tokens}
        finally:
            await stream.close()

    events, ready = asyncio.run(run())
    snapshots = {e.instrument_id for e in events if isinstance(e, BookEvent) and e.kind == "snapshot"}
    assert snapshots == set(tokens)
    assert not [e for e in events if isinstance(e, StreamStatusEvent) and e.state in ("gap", "error")]
    assert all(ready.values())


@pytest.mark.demo
def test_kalshi_order_events_agree_with_rest():
    from synpath.trading.kalshi import KalshiTrading
    from synpath.ws.kalshi import KalshiStream

    creds = load_credentials(dotenv=DOTENV if DOTENV.exists() else None).get("kalshi")
    if creds is None or creds.env != "demo":
        pytest.skip("needs Kalshi demo credentials")

    async def run():
        rest = KalshiTrading(creds)
        ticker, cursor = None, None
        while ticker is None:
            page = await rest._call("GET", "/markets", params={"status": "open", "limit": 200, "cursor": cursor})
            for market in page["markets"]:
                bid, ask = market.get("yes_bid_dollars"), market.get("yes_ask_dollars")
                if bid and ask and D("0") < D(bid) < D("0.5") < D(ask) < D("1"):
                    ticker = market["ticker"]
                    break
            cursor = page.get("cursor")
            if not cursor and ticker is None:
                pytest.skip("no demo market with a two-sided book")
        stream = KalshiStream(creds)
        await stream.watch_order_book([ticker])
        await stream.watch_orders()
        stream.start()
        await asyncio.wait_for(stream.connected.wait(), 10)
        await asyncio.sleep(2)
        order = await rest.create_order(OrderRequest(instrument_id=f"{ticker}:yes", side=Side.BUY, amount=D("1"), price=D("0.02")))
        await asyncio.sleep(2)
        await rest.cancel_order(order.id, market_id=ticker)
        events = await _collect(stream, lambda es: any(
            isinstance(e, OrderEvent) and e.order.id == order.id and e.order.status == OrderStatus.CANCELED for e in es), 15)
        await asyncio.sleep(1)
        after = await rest.fetch_order(order.id)
        await stream.close()
        await rest.close()
        return ticker, order, events, after, stream

    ticker, order, events, after, stream = asyncio.run(run())
    mine = [e.order for e in events if isinstance(e, OrderEvent) and e.order.id == order.id]
    assert [o.status for o in mine][0] == OrderStatus.OPEN and mine[-1].status == OrderStatus.CANCELED
    last = mine[-1]
    assert (last.status, last.filled, last.remaining, last.price, last.side) == (after.status, after.filled, after.remaining, after.price, after.side)
    assert any(isinstance(e, BookEvent) and e.kind == "snapshot" and e.market_id == ticker for e in events)
    assert not [e for e in events if isinstance(e, StreamStatusEvent) and e.state in ("gap", "error")]
