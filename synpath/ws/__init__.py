"""synpath.ws -- venue WebSockets as typed, self-healing event streams.

Every public name here is also exported from the top-level package (the
base event class as `synpath.StreamEvent`).

```python
from synpath import KalshiStream, PolymarketMarketStream

async with PolymarketMarketStream() as stream:
    await stream.watch_order_book(["<token id>"])
    async for event in stream:
        print(event)
```

Every stream reconnects on its own, sends its subscriptions again, reports
gaps it can detect and recovers from them, and marks a reconnect on private
channels `reconcile_required`. See `synpath.ws.base` for the promises.

Part of the base install: `pip install synpath`. The
Polymarket US exchange API's gRPC streams also need `synpath[grpc]` and the
venue's protos; see `synpath.ws.grpc`.
"""
from __future__ import annotations

from .base import (
    BalanceEvent, BookEvent, BookLevel, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent,
    QuoteEvent, Stream, StreamStats, StreamStatusEvent, TradeEvent, VenueEvent,
)


def __getattr__(name: str):
    if name == "KalshiStream":
        from .kalshi import KalshiStream
        return KalshiStream
    if name in ("PolymarketMarketStream", "PolymarketUserStream"):
        from . import polymarket
        return getattr(polymarket, name)
    if name in ("PolymarketUSMarketStream", "PolymarketUSPrivateStream"):
        from . import polymarket_us
        return getattr(polymarket_us, name)
    if name.startswith("PolymarketUSExchange"):
        from . import polymarket_us_exchange
        return getattr(polymarket_us_exchange, name)
    raise AttributeError(name)


__all__ = [
    "Stream", "StreamStats", "Event", "BookEvent", "BookLevel", "QuoteEvent", "TradeEvent", "OrderEvent",
    "FillEvent", "PositionEvent", "BalanceEvent", "MarketStatusEvent", "VenueEvent", "StreamStatusEvent",
    "LocalBook", "KalshiStream", "PolymarketMarketStream", "PolymarketUserStream", "PolymarketUSMarketStream",
    "PolymarketUSPrivateStream", "PolymarketUSExchangeOrderStream", "PolymarketUSExchangeDropCopyStream",
    "PolymarketUSExchangeTradeCaptureStream", "PolymarketUSExchangePositionChangeStream",
    "PolymarketUSExchangeInstrumentStream", "PolymarketUSExchangePositionStream",
    "PolymarketUSExchangeMarketDataStream", "PolymarketUSExchangeBalanceLedgerStream",
]
