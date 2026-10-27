"""Feeds: the venues' streams, wired into a running engine.

The engine decides; it does not listen. Something has to subscribe the
venues' WebSockets and hand what they say to the engine, or a stop never sees
a price and a parent never hears that its child filled. That is this module.

Two kinds of stream per venue, and where each one goes:

* **Market data** (books and public prints). Every market a live engine-held
  order watches, or this account holds a position in (the book marks it,
  `Engine.exit_price`), is subscribed on its venue's market stream. The stream keeps a
  `LocalBook` up to date in place, and its public prints are subscribed
  beside it (a last-trade stop and a position's mark read them); the engine is handed that same object once
  (`Engine.set_book`) and told about every change (`Engine.on_book`), so a
  parent always reads the book as the stream last left it. Subscribing also
  counts as a change: a market the shared connection already carried gets no
  fresh snapshot from the venue, so its parents read the book they are handed
  straight away rather than at the next tick. A book the stream
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

`synpath serve` shares these market streams with its `/ws/market` socket
(`synpath.server.market_stream`) through `Feeds.listeners`, so the screen and
the stops read one connection per venue. A book is handed to the engine only
for markets the engine itself watches.

Venues without a stream here are still traded: orders reach the engine
through its poll loop, only more slowly. That covers the paper venue, which
delivers its own events, and the Polymarket US exchange API, whose streams are
gRPC and an optional install.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Mapping

from .. import ids
from ..ws.base import BookEvent, FillEvent, OrderEvent, StreamStatusEvent, TradeEvent

log = logging.getLogger("synpath.engine.feeds")


@dataclass
class VenueStreams:
    """One venue's streams. `private` may be the same object as `market`
    (Kalshi carries both on one connection) or absent (no credentials)."""

    market: Any | None = None
    private: Any | None = None


def default_streams(
    adapters: Mapping[str, Any], credentials: Mapping[str, Any], market_data: Mapping[str, Any] | None = None,
) -> dict[str, VenueStreams]:
    """The library's own streams for each configured venue that has one.
    Kalshi and Polymarket US need credentials even for books; Polymarket's
    books are public and its user stream authenticates through the adapter.

    `market_data` (what `load_market_data` returns) adds a market stream for
    each venue configured with only its API key: no adapter trades there, but
    a parent on another venue can still watch its books."""
    out: dict[str, VenueStreams] = {}
    for venue, adapter in adapters.items():
        streams = venue_streams(venue, credentials.get(venue), adapter)
        if streams is None:
            log.info("synpath.engine.feeds: no stream for %s; its orders reach the engine by polling", venue)
            continue
        out[venue] = streams
    for venue, key in (market_data or {}).items():
        if venue not in out and venue not in adapters:
            stream = market_stream(venue, key)
            if stream is not None:
                out[venue] = VenueStreams(market=stream)
    return out


def market_stream(venue: str, creds: Any) -> Any | None:
    """One venue's market-data stream alone, for a reader with no adapter
    (the server's market socket, a venue configured with only its API key).
    `creds` is the venue's trading credentials or its `MarketDataKey`;
    `None` where the venue streams nothing without credentials it does not
    have."""
    streams = venue_streams(venue, creds, None, private=False)
    return streams.market if streams is not None else None


def venue_streams(venue: str, creds: Any, adapter: Any | None, *, private: bool = True) -> VenueStreams | None:
    """One venue's streams from its credentials (and adapter, which some
    private streams authenticate through). `private=False` builds the market
    stream only, and so does a `MarketDataKey` (an API key with no account
    behind it): Opinion, predict.fun and SX Bet stream books on the key
    alone. `None` where the venue has no stream these credentials open."""
    from ..trading.credentials import MARKET_DATA_KEYS, MarketDataKey
    if isinstance(creds, MarketDataKey):
        if venue not in MARKET_DATA_KEYS:
            # Public venues stream without any key; a key is no credential here.
            return None
        private = False
    if venue == "kalshi" and creds is not None:
        from ..ws.kalshi import KalshiStream
        stream = KalshiStream(creds)
        return VenueStreams(market=stream, private=stream if private else None)
    if venue == "polymarket":
        from ..ws.polymarket import PolymarketMarketStream, PolymarketUserStream
        return VenueStreams(
            market=PolymarketMarketStream(),
            private=PolymarketUserStream(trading=adapter) if private and creds is not None and adapter is not None else None,
        )
    if venue == "polymarket_us" and creds is not None:
        from ..ws.polymarket_us import PolymarketUSMarketStream, PolymarketUSPrivateStream
        return VenueStreams(market=PolymarketUSMarketStream(creds),
                            private=PolymarketUSPrivateStream(creds) if private else None)
    if venue == "opinion" and creds is not None:
        from ..ws.opinion import OpinionMarketStream, OpinionUserStream
        catalog = getattr(adapter, "catalog", None)
        if catalog is None:
            # The key also lifts the REST limit the stream seeds its books through.
            from ..opinion import Opinion
            catalog = Opinion(api_key=creds.api_key)
        return VenueStreams(
            market=OpinionMarketStream(api_key=creds.api_key, catalog=catalog),
            private=OpinionUserStream(api_key=creds.api_key, catalog=catalog) if private else None,
        )
    if venue == "hyperliquid":
        from ..ws.hyperliquid import HyperliquidMarketStream, HyperliquidUserStream
        address = getattr(creds, "address", None)
        testnet = getattr(creds, "testnet", False)
        return VenueStreams(
            market=HyperliquidMarketStream(testnet=testnet),
            private=HyperliquidUserStream(address=address, testnet=testnet) if private and address else None,
        )
    if venue == "predict_fun" and creds is not None and not getattr(creds, "testnet", False):
        # Mainnet only: the venue documents no test-network WebSocket.
        from ..ws.predict_fun import PredictFunMarketStream, PredictFunUserStream
        jwt = getattr(adapter, "jwt", None)
        return VenueStreams(
            market=PredictFunMarketStream(api_key=creds.api_key),
            private=PredictFunUserStream(api_key=creds.api_key, jwt=jwt) if private and jwt else None,
        )
    if venue == "limitless":
        from ..ws.limitless import LimitlessMarketStream, LimitlessUserStream
        token_id, secret = getattr(creds, "token_id", None), getattr(creds, "secret", None)
        catalog = getattr(adapter, "catalog", None)
        return VenueStreams(
            market=LimitlessMarketStream(),
            private=LimitlessUserStream(token_id=token_id, secret=secret, catalog=catalog)
            if private and token_id and secret else None,
        )
    if venue == "sx_bet" and creds is not None:
        # Every SX Bet channel, public ones included, needs the API key.
        from ..ws.sx_bet import SxBetMarketStream, SxBetUserStream
        testnet = bool(getattr(creds, "testnet", False))
        catalog = getattr(adapter, "catalog", None)
        if catalog is None:
            from ..sx_bet import SxBet
            catalog = SxBet(api_key=creds.api_key, testnet=testnet)
        address = getattr(creds, "address", None)
        if private and not address:
            log.warning("synpath.engine.feeds: sx_bet has an API key but no account address, so its orders "
                        "reach the engine by polling; set SX_BET_ACCOUNT_ADDRESS to the wallet's 0x address "
                        "for the account stream")
        return VenueStreams(
            market=SxBetMarketStream(api_key=creds.api_key, testnet=testnet, catalog=catalog),
            private=SxBetUserStream(api_key=creds.api_key, testnet=testnet, address=address)
            if private and address else None,
        )
    if venue == "myriad" and creds is not None:
        # Books are public; the account's orders and positions channels take a
        # subscription token the adapter signs for, fills the public per-wallet tape.
        from ..ws.myriad import MyriadMarketStream, MyriadUserStream
        catalog = getattr(adapter, "catalog", None)
        address = getattr(creds, "address", None)
        token = getattr(adapter, "subscription_token", None)
        if private and not address:
            log.warning("synpath.engine.feeds: myriad has an API key but no wallet address, so its orders "
                        "reach the engine by polling; set MYRIAD_ACCOUNT_ADDRESS for the account stream")
        return VenueStreams(
            market=MyriadMarketStream(catalog=catalog),
            private=MyriadUserStream(address=address, token=token) if private and address and token else None,
        )
    if venue == "betfair" and creds is not None:
        # Every Betfair stream, books included, logs in with the account;
        # both share the trading adapter's session, so one login serves all.
        from ..ws.betfair import BetfairMarketStream, BetfairUserStream
        session = getattr(adapter, "session", None)
        return VenueStreams(
            market=BetfairMarketStream(credentials=creds, session=session),
            private=BetfairUserStream(credentials=creds, session=session) if private else None,
        )
    if venue == "gemini":
        # Books are public; the account stream signs its handshake with
        # the API key, which must be account-scoped and time-based.
        from ..ws.gemini import GeminiMarketStream, GeminiUserStream
        sandbox = getattr(creds, "env", "prod") == "sandbox"
        return VenueStreams(
            market=GeminiMarketStream(sandbox=sandbox),
            private=GeminiUserStream(creds) if private and creds is not None else None,
        )
    if venue == "smarkets":
        # A polled stream on REST quotes (Smarkets' socket stream is for
        # approved market makers), on the trading adapter's session when
        # there is one, so the login undelays the quotes. No account
        # stream; the engine polls the orders.
        from ..ws.smarkets import SmarketsMarketStream
        session = getattr(adapter, "session", None)
        return VenueStreams(market=SmarketsMarketStream(credentials=creds, session=session), private=None)
    if venue == "novig":
        # The websocket takes a subaccount's trading key; without one the
        # market stream polls the public book.
        from ..ws.novig import NovigMarketStream, NovigUserStream
        catalog = getattr(adapter, "catalog", None)
        keyed = bool(getattr(creds, "key_id", None))
        return VenueStreams(
            market=NovigMarketStream(credentials=creds, catalog=catalog),
            private=NovigUserStream(credentials=creds, trading=adapter, catalog=catalog) if private and keyed else None,
        )
    if venue == "matchbook":
        # Polled, as Matchbook has no push API: books from public REST
        # reads, with no session so the polls are not the account's. No
        # account stream; the engine polls the offers.
        from ..ws.matchbook import MatchbookMarketStream
        return VenueStreams(market=MatchbookMarketStream(), private=None)
    if venue == "prophetx" and getattr(creds, "trades", False):
        # Every ProphetX channel is private: both streams register on the
        # trading adapter's session, which ProphetX counts per key.
        from ..ws.prophetx import ProphetXMarketStream, ProphetXUserStream
        session = getattr(adapter, "session", None)
        catalog = getattr(adapter, "catalog", None)
        resolver = getattr(adapter, "side", None)
        return VenueStreams(
            market=ProphetXMarketStream(credentials=creds, session=session, catalog=catalog),
            private=ProphetXUserStream(credentials=creds, session=session, catalog=catalog, resolver=resolver)
            if private else None,
        )
    if venue == "azuro":
        # Public odds over the feed's socket; bets are read back by
        # polling, since the feed streams no account channel.
        from ..ws.azuro import AzuroMarketStream
        chain = getattr(creds, "chain", "polygon")
        return VenueStreams(market=AzuroMarketStream(chain=chain, catalog=getattr(adapter, "catalog", None)),
                            private=None)
    return None


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
        self.listeners: list[Callable[[str, Any, Any], None]] = []
        """Called with `(venue, stream, event)` for every event, after the
        engine has it: how the server's market socket shares these
        connections instead of opening its own. A listener must not block."""

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
        """Markets live parents watch, and markets with an open position (so
        the engine can mark it from the book, `Engine.exit_price`), by venue,
        on venues with a market stream."""
        out: dict[str, set[str]] = {}
        watched = [market_id for market_id in list(self.engine.managed_order_controller.by_market) if self.engine.managed_order_controller.watching(market_id)]
        held = [state.market_id for state in self.engine.ledger.open_positions()]
        for market_id in watched + held:
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
            await self.watch_trades(venue, stream, new)
            self.watching[venue].update(new)
            for market_id in new:
                self.register(stream, market_id)
                # The venue sends no fresh snapshot for a market the
                # connection already carried (the screen's, say): the
                # parents read the book they were just handed, now.
                await self.engine.managed_order_controller.on_book(market_id)
            added += len(new)
        return added

    async def watch_trades(self, venue: str, stream: Any, market_ids: list[str]) -> None:
        """The public prints too, where the venue streams them on their own
        channel (Kalshi, Polymarket US; Polymarket's come with its book): a
        stop that triggers on the last trade hears every one, and a position
        whose book has no bid or ask is marked at it. A failure is logged;
        the book stays subscribed."""
        if not _has(stream, "watch_trades"):
            return
        if getattr(type(stream), "watch_trades", None) is getattr(type(stream), "watch_order_book", None):
            return
        try:
            await stream.watch_trades(market_ids)
        except Exception as exc:
            log.warning("synpath.engine.feeds: subscribing trades for %s on %s failed: %s", market_ids, venue, exc)

    def register(self, stream: Any, market_id: str) -> None:
        """Hand the engine the stream's own book object for this market."""
        book = stream.book(market_id)
        if book is not None and self.engine.books.get(market_id) is not book:
            self.engine.set_book(market_id, book)

    # -- delivering -----------------------------------------------------------

    async def dispatch(self, venue: str, stream: Any, event: Any) -> None:
        self.dispatched += 1
        if isinstance(event, BookEvent):
            # Only the books the engine watches are handed to it. A market
            # someone else subscribed on this shared stream may be unwatched
            # later, and its book would then sit in the engine still marked
            # ready while nothing updates it.
            if event.market_id in self.watching.get(venue, ()):
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
            self.tell_listeners(venue, stream, event)
        if self.closing:
            return
        log.warning("synpath.engine.feeds: the %s %s stream ended; that venue is polled until a restart",
                    venue, getattr(stream, "name", "stream"))
        while self.engine.running and not self.closing:
            await asyncio.sleep(1.0)

    def tell_listeners(self, venue: str, stream: Any, event: Any) -> None:
        for listener in self.listeners:
            try:
                listener(venue, stream, event)
            except Exception:
                log.exception("synpath.engine.feeds: a listener failed on a %s event from %s", type(event).__name__, venue)

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
