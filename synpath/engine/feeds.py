"""Feeds: the venues' streams, wired into a running engine.

The engine decides; it does not listen. Something has to subscribe the
venues' WebSockets and hand what they say to the engine, or a stop never sees
a price and a parent never hears that its child filled. That is this module.

Two kinds of stream per venue, and where each one goes:

* **Market data** (books and public prints). Every market a live engine-held
  order watches is subscribed on its venue's market stream. The stream keeps a
  `LocalBook` up to date in place; the engine is handed that same object once
  (`Engine.set_book`) and told about every change (`Engine.on_book`), so a
  parent always reads the book as the stream last left it. A book the stream
  has not snapshotted yet, or lost confidence in after a gap, is not `ready`,
  and parents read it as no book at all.
* **This account's orders and fills.** Order updates go to `Engine.on_order`,
  which is how parents learn what their children filled: the venue's order
  record, one channel, latest overwrites. Fill events go to `Engine.on_fill`,
  which books them in the ledger and nothing else.

Subscriptions follow the engine: every `sync_interval_s` the markets its live
parents watch are compared with what is subscribed, and the difference is
subscribed. A stream that reports `reconcile_required` (a reconnect with
private channels, a gap it cannot replay) makes the engine poll that venue's
REST state straight away, rather than waiting for the next poll.

Venues without a stream here are still traded: orders reach the engine
through its poll loop, only more slowly. That covers the paper venue, which
delivers its own events, and the Polymarket US exchange API, whose streams are
gRPC and an optional install.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Coroutine, Mapping

from .. import ids
from ..ws.base import BookEvent, FillEvent, OrderEvent, StreamStatusEvent, TradeEvent

log = logging.getLogger("synpath.engine.feeds")


@dataclass
class VenueStreams:
    """One venue's streams. `private` may be the same object as `market`
    (Kalshi carries both on one connection) or absent (no credentials)."""

    market: Any | None = None
    private: Any | None = None


def default_streams(adapters: Mapping[str, Any], credentials: Mapping[str, Any]) -> dict[str, VenueStreams]:
    """The library's own streams for each configured venue that has one.
    Kalshi and Polymarket US need credentials even for books; Polymarket's
    books are public and its user stream authenticates through the adapter."""
    out: dict[str, VenueStreams] = {}
    for venue, adapter in adapters.items():
        creds = credentials.get(venue)
        if venue == "kalshi" and creds is not None:
            from ..ws.kalshi import KalshiStream
            stream = KalshiStream(creds)
            out[venue] = VenueStreams(market=stream, private=stream)
        elif venue == "polymarket":
            from ..ws.polymarket import PolymarketMarketStream, PolymarketUserStream
            out[venue] = VenueStreams(
                market=PolymarketMarketStream(),
                private=PolymarketUserStream(trading=adapter) if creds is not None else None,
            )
        elif venue == "polymarket_us" and creds is not None:
            from ..ws.polymarket_us import PolymarketUSMarketStream, PolymarketUSPrivateStream
            out[venue] = VenueStreams(market=PolymarketUSMarketStream(creds), private=PolymarketUSPrivateStream(creds))
        elif venue == "opinion" and creds is not None:
            from ..ws.opinion import OpinionMarketStream, OpinionUserStream
            catalog = getattr(adapter, "catalog", None)
            out[venue] = VenueStreams(
                market=OpinionMarketStream(api_key=creds.api_key, catalog=catalog),
                private=OpinionUserStream(api_key=creds.api_key, catalog=catalog),
            )
        else:
            log.info("synpath.engine.feeds: no stream for %s; its orders reach the engine by polling", venue)
    return out


def _has(stream: Any, capability: str) -> bool:
    return bool((getattr(stream, "has", None) or {}).get(capability)) and callable(getattr(stream, capability, None))


class Feeds:
    def __init__(self, engine: Any, streams: Mapping[str, VenueStreams], *, sync_interval_s: float = 1.0):
        self.engine = engine
        self.streams: dict[str, VenueStreams] = dict(streams)
        self.sync_interval_s = sync_interval_s
        self.watching: dict[str, set[str]] = {venue: set() for venue in self.streams}
        self.dispatched = 0
        self.closing = False

    def distinct(self) -> list[tuple[str, Any]]:
        """Every stream object once, with the venue it belongs to."""
        seen: set[int] = set()
        out: list[tuple[str, Any]] = []
        for venue, pair in self.streams.items():
            for stream in (pair.market, pair.private):
                if stream is not None and id(stream) not in seen:
                    seen.add(id(stream))
                    out.append((venue, stream))
        return out

    # -- subscribing ----------------------------------------------------------

    async def start(self) -> None:
        """Start every stream, subscribe this account's orders and fills, and
        the books every live parent already watches (restored ones included)."""
        for _, stream in self.distinct():
            stream.start()
        for venue, pair in self.streams.items():
            private = pair.private
            if private is None:
                continue
            for capability in ("watch_orders", "watch_my_trades"):
                if _has(private, capability):
                    try:
                        await getattr(private, capability)()
                    except Exception as exc:
                        log.warning("synpath.engine.feeds: %s %s failed: %s", venue, capability, exc)
        await self.sync()

    def wanted(self) -> dict[str, set[str]]:
        """Markets live parents watch, by venue, on venues with a market stream."""
        out: dict[str, set[str]] = {}
        for market_id in list(self.engine.orders.by_market):
            if not self.engine.orders.watching(market_id):
                continue
            venue, _ = ids.split(market_id)
            if venue is None or venue not in self.streams or self.streams[venue].market is None:
                continue
            out.setdefault(venue, set()).add(market_id)
        return out

    async def sync(self) -> int:
        """Subscribe what is wanted and not yet watched. Returns how many."""
        added = 0
        for venue, markets in self.wanted().items():
            new = sorted(markets - self.watching[venue])
            if not new:
                continue
            stream = self.streams[venue].market
            try:
                await stream.watch_order_book(new)
            except Exception as exc:
                log.warning("synpath.engine.feeds: subscribing %s on %s failed: %s", new, venue, exc)
                continue
            self.watching[venue].update(new)
            for market_id in new:
                self.register(stream, market_id)
            added += len(new)
        return added

    def register(self, stream: Any, market_id: str) -> None:
        """Hand the engine the stream's own book object for this market."""
        book = stream.book(market_id)
        if book is not None and self.engine.books.get(market_id) is not book:
            self.engine.set_book(market_id, book)

    # -- delivering -----------------------------------------------------------

    async def dispatch(self, venue: str, stream: Any, event: Any) -> None:
        self.dispatched += 1
        if isinstance(event, BookEvent):
            self.register(stream, event.market_id)
            await self.engine.on_book(event)
        elif isinstance(event, TradeEvent):
            await self.engine.on_trade(event)
        elif isinstance(event, OrderEvent):
            await self.engine.on_order(event.order)
        elif isinstance(event, FillEvent):
            await self.engine.on_fill(event.fill)
        elif isinstance(event, StreamStatusEvent):
            if event.reconcile_required and venue in self.engine.adapters:
                await self.engine.poll(venue)
            if event.state == "failed":
                log.error("synpath.engine.feeds: %s %s failed for good: %s", venue, event.stream, event.detail)

    async def pump(self, venue: str, stream: Any) -> None:
        """Deliver one stream's events until it ends. One bad event is logged
        and skipped; it does not stop the stream.

        A stream that ends on its own (the venue refused it for good) does not
        end the pump until the engine stops or the feeds close: a host that
        waits for the first of its tasks to finish must not take a lost
        stream for a stopped engine. That venue's orders keep arriving by
        polling."""
        async for event in stream:
            try:
                await self.dispatch(venue, stream, event)
            except Exception:
                log.exception("synpath.engine.feeds: handling a %s event from %s failed", type(event).__name__, venue)
        if self.closing:
            return
        log.warning("synpath.engine.feeds: the %s %s stream ended; that venue is polled until a restart",
                    venue, getattr(stream, "name", "stream"))
        while self.engine.running and not self.closing:
            await asyncio.sleep(1.0)

    async def _sync_loop(self) -> None:
        while self.engine.running:
            await asyncio.sleep(self.sync_interval_s)
            try:
                await self.sync()
            except Exception:
                log.exception("synpath.engine.feeds: sync failed")

    def background(self) -> dict[str, Coroutine[Any, Any, None]]:
        """The coroutines a host schedules alongside `Engine.background()`."""
        loops: dict[str, Coroutine[Any, Any, None]] = {"feeds-sync": self._sync_loop()}
        for venue, stream in self.distinct():
            loops[f"feed-{venue}-{getattr(stream, 'name', 'stream')}"] = self.pump(venue, stream)
        return loops

    async def close(self) -> None:
        self.closing = True
        for venue, stream in self.distinct():
            try:
                await stream.close()
            except Exception as exc:
                log.debug("synpath.engine.feeds: closing %s failed: %s", venue, exc)
