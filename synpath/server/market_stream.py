"""`/ws/market`: live books, trades and quotes from the library's own venue streams.

One socket per client, many markets on it. The server keeps **one venue
connection per venue** and shares it between every subscriber and the
engine's feeds, so the screen and the stops read the same book:

* A venue the engine trades already has a market stream in
  `synpath.engine.feeds.Feeds`. The hub subscribes markets on that same
  object and hears its events through `Feeds.listeners`; it never opens a
  second connection to that venue and never closes the engine's.
* Any other venue gets a connection the hub opens on the first subscription
  (from serve's credentials, `synpath.engine.feeds.market_stream`) and closes
  when the last subscriber leaves.

Every market is reference-counted by the sockets subscribed to it. When the
last one leaves, the market is unwatched on the venue (on streams that can
unwatch; Kalshi and Polymarket US cannot, so their venue subscription stays
until the connection closes) unless the engine watches it too: the engine's
markets are held for as long as the engine holds them.

What a client receives is the library's own state, not a re-derived book:
the stream's maintained `LocalBook` for the market (sent on subscribe, then
after every change, at most every `book_interval_s`), and its `TradeEvent`,
`QuoteEvent`, `MarketStatusEvent` and `StreamStatusEvent` as they come. The
venue stream reconnects on its own; a book it is not sure of is sent as
`ready: false` with no levels, and the fresh snapshot after a reconnect is
sent as soon as it lands. The wire format is in docs/server.md.

A **bucket** (`bucket:<id>`, `synpath.bucket`) subscribes like a market. The
hub subscribes each member on the shared venue connections through a
`MemberFeed`, an internal subscriber that forwards to the client's session,
and sends the members' books merged in bucket terms by the library's router
(`synpath.server.bucket_books`): one `bucket_book` frame per change, and each
member's public trades as `trade` frames in bucket terms. A bucket costs no
venue connection a market would not.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Literal

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .. import ids
from ..bucket import Bucket, bucket_id_of, is_bucket_id
from ..ws.base import BookEvent, MarketStatusEvent, QuoteEvent, StreamStatusEvent, TradeEvent, now_ms

log = logging.getLogger("synpath.server")

CHANNELS: dict[str, str] = {"book": "watch_order_book", "trades": "watch_trades", "quotes": "watch_ticker"}
"""A client's channel name, and the stream capability that carries it."""

OUTBOX_LIMIT = 10_000
"""Messages a client may fall behind by before its socket is closed (1013)."""

CLOSE_UNAUTHORIZED = 4401
CLOSE_TOO_SLOW = 1013

Side = Literal["yes", "no"]
StreamFactory = Callable[[str], Any]
"""Builds a venue's market stream, or returns `None` where it cannot (no credentials)."""


class SubscribeRefused(Exception):
    """A subscription the hub cannot serve; `code` goes to the client."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# Wire messages
# ---------------------------------------------------------------------------

def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def book_message(venue: str, market_id: str, side: Side, book: Any, depth: int | None) -> dict[str, Any]:
    """The stream's maintained book for one market as it stands now. A book
    the stream has not snapshotted, or stopped trusting, has no levels."""
    ready = book is not None and book.ready
    bids, asks = book.levels(depth) if ready else ((), ())
    return {
        "type": "book", "venue": venue, "market_id": market_id, "side": side, "ready": ready,
        "bids": [{"price": str(level.price), "size": str(level.size)} for level in bids],
        "asks": [{"price": str(level.price), "size": str(level.size)} for level in asks],
        "best_bid": _text(book.best_bid) if ready else None,
        "best_ask": _text(book.best_ask) if ready else None,
        "sequence": book.sequence if book is not None else None,
        "timestamp": book.timestamp if book is not None else None,
        "sent_at": now_ms(),
    }


def trade_message(event: TradeEvent) -> dict[str, Any]:
    return {
        "type": "trade", "venue": event.venue, "market_id": event.market_id, "id": event.id,
        "price": str(event.price), "quantity": str(event.quantity),
        "taker_side": event.taker_side.value if event.taker_side is not None else None,
        "timestamp": event.timestamp, "received_at": event.received_at,
    }


def quote_message(event: QuoteEvent) -> dict[str, Any]:
    return {
        "type": "quote", "venue": event.venue, "market_id": event.market_id, "side": event.side,
        "bid": _text(event.bid), "ask": _text(event.ask), "bid_size": _text(event.bid_size),
        "ask_size": _text(event.ask_size), "last": _text(event.last), "volume": _text(event.volume),
        "open_interest": _text(event.open_interest), "timestamp": event.timestamp, "received_at": event.received_at,
    }


def market_status_message(event: MarketStatusEvent) -> dict[str, Any]:
    return {
        "type": "market_status", "venue": event.venue, "market_id": event.market_id, "state": event.state,
        "native": event.native, "result": event.result, "timestamp": event.timestamp,
        "received_at": event.received_at,
    }


def stream_status_message(event: StreamStatusEvent) -> dict[str, Any]:
    return {"type": "status", "venue": event.venue, "state": event.state, "detail": event.detail,
            "received_at": event.received_at}


def error_message(code: str, message: str, market_id: str | None = None) -> dict[str, Any]:
    return {"type": "error", "code": code, "message": message, "market_id": market_id}


# ---------------------------------------------------------------------------
# One client
# ---------------------------------------------------------------------------

@dataclass
class Subscription:
    """One client's interest in one market."""

    market_id: str
    venue: str
    side: Side
    depth: int | None
    channels: frozenset[str]


class Session:
    """One connected client: what it subscribed, and what is waiting to be
    sent to it. Events are queued in order; books are only marked dirty and
    read from the stream when sent, so a fast book costs one message per
    send interval, not one per venue update."""

    def __init__(self, principal: Any | None = None) -> None:
        self.principal = principal
        """Who the client's access token belongs to; bucket visibility is checked against it."""
        self.subscriptions: dict[str, Subscription] = {}
        self.outbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=OUTBOX_LIMIT)
        self.dirty_books: set[str] = set()
        self.wake = asyncio.Event()
        self.overflowed = False

    def wants(self, market_id: str, channel: str) -> Subscription | None:
        subscription = self.subscriptions.get(market_id)
        return subscription if subscription is not None and channel in subscription.channels else None

    def send(self, message: dict[str, Any]) -> None:
        try:
            self.outbox.put_nowait(message)
        except asyncio.QueueFull:
            self.overflowed = True
        self.wake.set()

    def mark_book_dirty(self, market_id: str) -> None:
        self.dirty_books.add(market_id)
        self.wake.set()


@dataclass(eq=False)
class BucketSubscription(Subscription):
    """One client's interest in a bucket: the definition it streams, the
    internal subscriber holding the members, and what is known about each
    member beyond its book."""

    bucket: Bucket = None  # type: ignore[assignment]
    owner: Session = None  # type: ignore[assignment]
    listener: "MemberFeed | None" = None
    member_states: dict[str, Any] = field(default_factory=dict)

    # Held in sets by identity: two subscriptions to one bucket are two clients.
    __hash__ = object.__hash__
    __eq__ = object.__eq__


class MemberFeed(Session):
    """The members of one bucket subscription, as the hub sees them: an
    ordinary subscriber on each member market, so sharing and reference
    counting are the hub's own. What it hears goes to the client's session
    in bucket terms: a member's book change marks the bucket's book dirty, a
    member's trade becomes a bucket trade."""

    def __init__(self, hub: "MarketStreams", subscription: BucketSubscription):
        super().__init__(subscription.owner.principal)
        self.hub = hub
        self.bucket_subscription = subscription

    def mark_book_dirty(self, market_id: str) -> None:
        self.bucket_subscription.owner.mark_book_dirty(self.bucket_subscription.market_id)

    def send(self, message: dict[str, Any]) -> None:
        from .bucket_books import MemberState, bucket_trade_message

        subscription, owner = self.bucket_subscription, self.bucket_subscription.owner
        kind = message.get("type")
        if kind == "subscribed":
            if "book" not in message.get("channels", ()):
                subscription.member_states[message["market_id"]] = MemberState(message["market_id"], "no_book_stream")
        elif kind == "trade":
            if "trades" in subscription.channels:
                member = subscription.bucket.member(message["market_id"])
                owner.send(bucket_trade_message(subscription.bucket, member, message))
        elif kind == "error" and message.get("code") == "stream_ended":
            # The hub dropped this subscriber's markets on that venue: the
            # bucket is no longer whole, so the client is told and dropped,
            # as for a market.
            if owner.subscriptions.get(subscription.market_id) is subscription:
                owner.subscriptions.pop(subscription.market_id, None)
                self.hub.forget_bucket_subscription(subscription)
            self.hub.release_later(self)
            owner.send(error_message("stream_ended", message.get("message") or "a member's market stream ended",
                                     subscription.market_id))
        elif kind in ("status", "market_status"):
            owner.send(message)


# ---------------------------------------------------------------------------
# One venue connection
# ---------------------------------------------------------------------------

@dataclass
class VenueConnection:
    """One venue's market stream and who is subscribed to what on it.

    `owned` connections were opened by the hub and are closed when nobody is
    left; the others belong to the engine's feeds and live as long as it."""

    venue: str
    stream: Any
    owned: bool
    sessions_by_market: dict[str, set[Session]] = field(default_factory=dict)
    watched_channels: dict[str, set[str]] = field(default_factory=dict)
    pump: asyncio.Task | None = None

    def sessions(self) -> set[Session]:
        return {session for sessions in self.sessions_by_market.values() for session in sessions}

    def supported(self, channels: frozenset[str]) -> frozenset[str]:
        has = getattr(self.stream, "has", None) or {}
        return frozenset(c for c in channels if has.get(CHANNELS[c]) and callable(getattr(self.stream, CHANNELS[c], None)))

    async def watch(self, market_id: str, channels: frozenset[str]) -> None:
        """Subscribe the channels this market is not yet watched on."""
        already = self.watched_channels.setdefault(market_id, set())
        for channel in sorted(channels - already):
            await getattr(self.stream, CHANNELS[channel])([market_id])
            already.add(channel)

    async def unwatch(self, market_id: str) -> None:
        self.watched_channels.pop(market_id, None)
        unwatch = getattr(self.stream, "unwatch", None)
        if callable(unwatch):
            await unwatch([market_id])


# ---------------------------------------------------------------------------
# The hub
# ---------------------------------------------------------------------------

class MarketStreams:
    """Every venue market connection the server's sockets use, shared and
    reference-counted. See the module docstring for the sharing rules."""

    def __init__(self, open_stream: StreamFactory, *, feeds: Any | None = None, enabled: bool = True,
                 book_interval_s: float = 0.1):
        self.open_stream = open_stream
        self.feeds = feeds
        self.enabled = enabled
        self.book_interval_s = book_interval_s
        self.connections: dict[str, VenueConnection] = {}
        self.venue_locks: dict[str, asyncio.Lock] = {}
        self.closing = False
        self.releasing: set[asyncio.Task] = set()
        self.buckets: Any | None = None
        """`synpath.server.bucket_books.BucketBooks`, where the server holds buckets."""
        self.bucket_subscriptions: dict[str, set[BucketSubscription]] = {}
        """Bucket id -> every client subscription streaming it, for a definition change."""
        if feeds is not None:
            self.adopt_engine_streams(feeds)

    def adopt_engine_streams(self, feeds: Any) -> None:
        """Share the engine's market streams instead of opening our own."""
        for venue, pair in feeds.streams.items():
            if pair.market is not None:
                self.connections[venue] = VenueConnection(venue=venue, stream=pair.market, owned=False)
        feeds.listeners.append(self.publish)

    # -- subscribing ----------------------------------------------------------

    async def subscribe(self, session: Session, market_id: str, channels: frozenset[str], side: Side,
                        depth: int | None) -> Subscription:
        """Add one market to a session: open or reuse the venue's connection,
        watch what is not yet watched, then queue the acknowledgement and the
        current book. A bucket id subscribes the bucket's members instead."""
        if is_bucket_id(market_id):
            return await self.subscribe_bucket(session, market_id, channels, side, depth)
        venue = self.venue_of(market_id)
        async with self.venue_lock(venue):
            connection = await self.connection_for(venue)
            granted = connection.supported(channels)
            try:
                await connection.watch(market_id, granted)
            except Exception as exc:
                await self.release_if_unused(connection, market_id)
                raise SubscribeRefused("subscribe_failed", f"{venue} refused {market_id}: {exc}") from exc
            connection.sessions_by_market.setdefault(market_id, set()).add(session)
        subscription = Subscription(market_id=market_id, venue=venue, side=side, depth=depth, channels=granted)
        session.subscriptions[market_id] = subscription
        session.send({"type": "subscribed", "venue": venue, "market_id": market_id, "side": side,
                      "channels": sorted(granted), "unsupported": sorted(channels - granted)})
        if "book" in granted:
            session.mark_book_dirty(market_id)
        return subscription

    async def unsubscribe(self, session: Session, market_id: str) -> bool:
        subscription = session.subscriptions.pop(market_id, None)
        if subscription is None:
            return False
        if isinstance(subscription, BucketSubscription):
            self.forget_bucket_subscription(subscription)
            session.dirty_books.discard(market_id)
            if subscription.listener is not None:
                await self.leave(subscription.listener)
            return True
        async with self.venue_lock(subscription.venue):
            connection = self.connections.get(subscription.venue)
            if connection is not None:
                connection.sessions_by_market.get(market_id, set()).discard(session)
                await self.release_if_unused(connection, market_id)
        session.dirty_books.discard(market_id)
        return True

    # -- buckets --------------------------------------------------------------

    async def subscribe_bucket(self, session: Session, market_id: str, channels: frozenset[str], side: Side,
                               depth: int | None) -> Subscription:
        """Stream a bucket: check it exists and the client may see it, then
        subscribe its members on the shared connections and queue the
        acknowledgement and the merged book. `depth` is levels per member
        and side; the merged book holds every one of them."""
        if self.buckets is None:
            raise SubscribeRefused("buckets_unavailable", "this server holds no buckets (no engine)")
        if side != "yes":
            raise SubscribeRefused("bad_request", "a bucket's book is already in bucket terms: subscribe with side `yes`")
        bucket = await self.buckets.load(bucket_id_of(market_id))
        if bucket is None:
            raise SubscribeRefused("bucket_not_found", f"no bucket {market_id}")
        if session.principal is not None and not self.buckets.visible_to(session.principal, bucket):
            raise SubscribeRefused("forbidden", "this key may not view every venue in that bucket")
        if market_id in session.subscriptions:
            await self.unsubscribe(session, market_id)
        granted = channels & frozenset({"book", "trades"})
        subscription = BucketSubscription(market_id=market_id, venue="bucket", side=side, depth=depth,
                                          channels=granted, bucket=bucket, owner=session)
        session.subscriptions[market_id] = subscription
        self.bucket_subscriptions.setdefault(bucket.id, set()).add(subscription)
        session.send({"type": "subscribed", "venue": "bucket", "market_id": market_id, "side": side,
                      "channels": sorted(granted), "unsupported": sorted(channels - granted),
                      "bucket": bucket.model_dump(mode="json")})
        await self.attach_members(subscription)
        return subscription

    async def attach_members(self, subscription: BucketSubscription) -> None:
        """Read the members' fee schedules, then subscribe every member's book
        (and trades, if asked) through a fresh `MemberFeed`. A member the hub
        cannot stream is recorded and reported; the bucket stays not ready.
        A member switched off (`enabled=False`) is not subscribed at all."""
        from .bucket_books import MemberState

        bucket, owner = subscription.bucket, subscription.owner
        routable = [member for member in bucket.members if member.enabled]
        await self.buckets.fees.prepare(bucket.model_copy(update={"members": routable}))
        listener = MemberFeed(self, subscription)
        subscription.listener = listener
        member_channels = frozenset({"book"} | ({"trades"} if "trades" in subscription.channels else set()))
        for member in routable:
            try:
                await self.subscribe(listener, member.market_id, member_channels, "yes", None)
            except SubscribeRefused as exc:
                subscription.member_states[member.market_id] = MemberState(member.market_id, exc.code)
                owner.send(error_message(exc.code, f"{member.market_id}: {exc}", subscription.market_id))
        owner.mark_book_dirty(subscription.market_id)

    async def refresh_bucket(self, bucket: Bucket) -> None:
        """A bucket's definition changed (renamed, members edited or switched
        on/off, archived or restored): every client streaming it moves to the
        new definition, members added or switched on are subscribed and members
        dropped or switched off released."""
        for subscription in list(self.bucket_subscriptions.get(bucket.id, ())):
            old = subscription.listener
            subscription.bucket = bucket
            subscription.member_states = {}
            await self.attach_members(subscription)
            if old is not None:
                await self.leave(old)

    def forget_bucket_subscription(self, subscription: BucketSubscription) -> None:
        held = self.bucket_subscriptions.get(subscription.bucket.id)
        if held is not None:
            held.discard(subscription)
            if not held:
                del self.bucket_subscriptions[subscription.bucket.id]

    def member_book(self, market_id: str) -> Any | None:
        """The shared stream's maintained YES book for a market, if a
        connection to its venue holds one."""
        venue, _ = ids.split(market_id)
        connection = self.connections.get(venue) if venue else None
        return connection.stream.book(market_id) if connection is not None else None

    def release_later(self, session: Session) -> None:
        task = asyncio.get_running_loop().create_task(self.leave(session), name="synpath-market-release")
        self.releasing.add(task)
        task.add_done_callback(self.releasing.discard)

    async def leave(self, session: Session) -> None:
        """A client went away: drop everything it held."""
        for market_id in list(session.subscriptions):
            await self.unsubscribe(session, market_id)

    def venue_of(self, market_id: str) -> str:
        venue, native = ids.split(market_id)
        if venue is None or not native:
            raise SubscribeRefused("bad_market_id", f"{market_id!r} is not a Synpath id such as `kalshi:KXFOO-25`")
        return venue

    def venue_lock(self, venue: str) -> asyncio.Lock:
        return self.venue_locks.setdefault(venue, asyncio.Lock())

    async def connection_for(self, venue: str) -> VenueConnection:
        """The venue's shared connection, opening one if nobody has yet."""
        if not self.enabled:
            raise SubscribeRefused("streams_disabled", "this server runs with --no-streams; live market data is off")
        connection = self.connections.get(venue)
        if connection is not None:
            return connection
        stream = self.open_stream(venue)
        if stream is None:
            raise SubscribeRefused(
                "credentials_required",
                f"live {venue} market data needs {venue} credentials in the server's environment or .env "
                "(see `synpath doctor`), or the venue has no market stream",
            )
        connection = VenueConnection(venue=venue, stream=stream, owned=True)
        stream.start()
        connection.pump = asyncio.get_running_loop().create_task(self.pump(connection), name=f"synpath-market-{venue}")
        self.connections[venue] = connection
        return connection

    async def release_if_unused(self, connection: VenueConnection, market_id: str) -> None:
        """The last subscriber left a market: unwatch it unless the engine
        watches it, and close a connection of our own nobody uses."""
        if connection.sessions_by_market.get(market_id):
            return
        connection.sessions_by_market.pop(market_id, None)
        if not self.engine_watches(connection.venue, market_id):
            try:
                await connection.unwatch(market_id)
            except Exception as exc:
                log.warning("synpath.server: unwatching %s on %s failed: %s", market_id, connection.venue, exc)
        if connection.owned and not connection.sessions_by_market:
            await self.close_connection(connection)

    def engine_watches(self, venue: str, market_id: str) -> bool:
        return self.feeds is not None and market_id in self.feeds.watching.get(venue, ())

    async def close_connection(self, connection: VenueConnection) -> None:
        if self.connections.get(connection.venue) is connection:
            del self.connections[connection.venue]
        try:
            await connection.stream.close()
        except Exception as exc:
            log.debug("synpath.server: closing the %s market stream failed: %s", connection.venue, exc)
        if connection.pump is not None and connection.pump is not asyncio.current_task():
            connection.pump.cancel()
            await asyncio.gather(connection.pump, return_exceptions=True)

    # -- delivering -----------------------------------------------------------

    async def pump(self, connection: VenueConnection) -> None:
        """Deliver a connection of our own until it ends. The stream
        reconnects by itself; it ends only when closed or refused for good,
        and then its subscribers are told and dropped, so their next
        subscribe opens a fresh connection."""
        async for event in connection.stream:
            self.publish(connection.venue, connection.stream, event)
        if self.closing or self.connections.get(connection.venue) is not connection:
            return
        del self.connections[connection.venue]
        for session in connection.sessions():
            for market_id in list(connection.sessions_by_market):
                session.subscriptions.pop(market_id, None)
            session.send(error_message("stream_ended", f"the {connection.venue} market stream ended; subscribe again"))

    def publish(self, venue: str, stream: Any, event: Any) -> None:
        """Route one venue event to the sessions that asked for it. Never
        blocks: it only queues messages and marks books dirty."""
        connection = self.connections.get(venue)
        if connection is None or connection.stream is not stream:
            return
        if isinstance(event, BookEvent):
            for session in connection.sessions_by_market.get(event.market_id, ()):
                if session.wants(event.market_id, "book"):
                    session.mark_book_dirty(event.market_id)
        elif isinstance(event, TradeEvent):
            self.send_to_market(connection, event.market_id, "trades", trade_message(event))
        elif isinstance(event, QuoteEvent):
            for session in connection.sessions_by_market.get(event.market_id, ()):
                subscription = session.wants(event.market_id, "quotes")
                if subscription is not None and subscription.side == event.side:
                    session.send(quote_message(event))
        elif isinstance(event, MarketStatusEvent):
            for session in connection.sessions_by_market.get(event.market_id, ()):
                session.send(market_status_message(event))
        elif isinstance(event, StreamStatusEvent):
            self.send_stream_status(connection, event)

    def send_to_market(self, connection: VenueConnection, market_id: str, channel: str, message: dict[str, Any]) -> None:
        for session in connection.sessions_by_market.get(market_id, ()):
            if session.wants(market_id, channel):
                session.send(message)

    def send_stream_status(self, connection: VenueConnection, event: StreamStatusEvent) -> None:
        """The connection's state to everyone on it, and every book re-sent:
        after a drop or a gap the stream no longer trusts its books (they go
        out as not ready), and after a reconnect the fresh snapshots follow."""
        message = stream_status_message(event)
        for market_id, sessions in connection.sessions_by_market.items():
            for session in sessions:
                if session.wants(market_id, "book"):
                    session.mark_book_dirty(market_id)
        for session in connection.sessions():
            session.send(message)

    def book_now(self, subscription: Subscription) -> dict[str, Any]:
        if isinstance(subscription, BucketSubscription):
            return self.bucket_book_now(subscription)
        connection = self.connections.get(subscription.venue)
        book = connection.stream.book(subscription.market_id, subscription.side) if connection is not None else None
        return book_message(subscription.venue, subscription.market_id, subscription.side, book, subscription.depth)

    def bucket_book_now(self, subscription: BucketSubscription) -> dict[str, Any]:
        from .bucket_books import bucket_book_message

        bucket = subscription.bucket
        books = {member.market_id: self.member_book(member.market_id) for member in bucket.enabled_members()}
        return bucket_book_message(bucket, books, self.buckets.fees, depth=subscription.depth,
                                   member_states=subscription.member_states)

    async def close(self) -> None:
        self.closing = True
        await asyncio.gather(*self.releasing, return_exceptions=True)
        for connection in list(self.connections.values()):
            if connection.owned:
                await self.close_connection(connection)
        self.connections.clear()


# ---------------------------------------------------------------------------
# The socket
# ---------------------------------------------------------------------------

def parse_subscribe(message: dict[str, Any]) -> tuple[str, frozenset[str], Side, int | None]:
    """A subscribe request's market, channels, side and depth, or `SubscribeRefused`."""
    market_id = message.get("market_id")
    if not isinstance(market_id, str) or not market_id:
        raise SubscribeRefused("bad_request", "subscribe needs `market_id`")
    channels = message.get("channels") or ["book"]
    if isinstance(channels, str):
        channels = [c for c in channels.split(",") if c]
    unknown = set(channels) - set(CHANNELS)
    if unknown:
        raise SubscribeRefused("bad_request", f"unknown channels {sorted(unknown)}; choose from {sorted(CHANNELS)}")
    side = message.get("side") or "yes"
    if side not in ("yes", "no"):
        raise SubscribeRefused("bad_request", "`side` is `yes` or `no`")
    depth = message.get("depth")
    if depth is not None and (not isinstance(depth, int) or isinstance(depth, bool) or depth < 1):
        raise SubscribeRefused("bad_request", "`depth` is a positive integer")
    return market_id, frozenset(channels), side, depth


async def handle_request(hub: MarketStreams, session: Session, message: Any) -> None:
    """One client frame: `{"op": "subscribe", ...}` or `{"op": "unsubscribe", "market_id"}`."""
    if not isinstance(message, dict):
        session.send(error_message("bad_request", "send a JSON object with `op`"))
        return
    op = message.get("op")
    market_id = message.get("market_id") if isinstance(message.get("market_id"), str) else None
    try:
        if op == "subscribe":
            await hub.subscribe(session, *parse_subscribe(message))
        elif op == "unsubscribe":
            if await hub.unsubscribe(session, market_id or ""):
                session.send({"type": "unsubscribed", "market_id": market_id})
            else:
                session.send(error_message("not_subscribed", f"{market_id!r} is not subscribed here", market_id))
        else:
            session.send(error_message("bad_request", "`op` is `subscribe` or `unsubscribe`"))
    except SubscribeRefused as exc:
        session.send(error_message(exc.code, str(exc), market_id))


async def receive_requests(socket: WebSocket, hub: MarketStreams, session: Session) -> None:
    while True:
        raw = await socket.receive_text()
        try:
            message = json.loads(raw)
        except ValueError:
            session.send(error_message("bad_request", "frames are JSON"))
            continue
        await handle_request(hub, session, message)


async def send_updates(socket: WebSocket, hub: MarketStreams, session: Session) -> None:
    """Queued events in order, then every dirty book as it stands now; then
    wait at least `book_interval_s`, so books coalesce on a busy market."""
    while True:
        await session.wake.wait()
        session.wake.clear()
        if session.overflowed:
            await socket.close(code=CLOSE_TOO_SLOW, reason="this client fell too far behind")
            return
        while not session.outbox.empty():
            await socket.send_text(json.dumps(session.outbox.get_nowait()))
        dirty, session.dirty_books = session.dirty_books, set()
        for market_id in dirty:
            subscription = session.subscriptions.get(market_id)
            if subscription is not None:
                await socket.send_text(json.dumps(hub.book_now(subscription)))
        if hub.book_interval_s:
            await asyncio.sleep(hub.book_interval_s)


def initial_requests(socket: WebSocket) -> list[dict[str, Any]]:
    """Subscriptions named on the URL: `?market_id=…&channels=book,trades&side=yes&depth=16`,
    `market_id` repeatable. The socket then takes the same as messages."""
    params = socket.query_params
    depth = params.get("depth")
    common = {"channels": params.get("channels") or "book", "side": params.get("side") or "yes",
              "depth": int(depth) if depth and depth.isdigit() else depth}
    return [{"op": "subscribe", "market_id": market_id, **common} for market_id in params.getlist("market_id")]


def add_market_socket(app: FastAPI, hub: MarketStreams) -> None:
    """`/ws/market` on `app`, authenticated like `/trading/ws/events`: the
    access token as `?key=` or `Authorization: Bearer`, any valid key."""
    from .trading import socket_principal

    app.state.market_streams = hub

    @app.websocket("/ws/market")
    async def market(socket: WebSocket) -> None:
        # Accept first, then close with the reason: a close before accepting
        # becomes an HTTP 403 on the handshake, and a browser cannot read why.
        await socket.accept()
        store = getattr(socket.app.state, "control", None)
        principal = await socket_principal(socket, store, socket.query_params.get("key")) if store else None
        if principal is None:
            await socket.close(code=CLOSE_UNAUTHORIZED, reason="a valid access token is required")
            return
        await serve_session(socket, hub, principal)


async def serve_session(socket: WebSocket, hub: MarketStreams, principal: Any | None = None) -> None:
    """Run one client until it leaves; then release everything it held."""
    session = Session(principal)
    sender = asyncio.create_task(send_updates(socket, hub, session))
    try:
        for request in initial_requests(socket):
            await handle_request(hub, session, request)
        receiver = asyncio.create_task(receive_requests(socket, hub, session))
        done, _ = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
    except WebSocketDisconnect:
        pass
    except Exception:  # pragma: no cover - a broken socket is not a venue problem
        log.debug("synpath.server: the market socket ended", exc_info=True)
    finally:
        sender.cancel()
        # Released in a task of its own: when the server cancels this handler
        # (shutdown, a test client closing) the release must still finish,
        # and an await here would be cancelled with the handler.
        hub.release_later(session)
