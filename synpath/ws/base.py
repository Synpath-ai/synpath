"""The streaming layer's shared machinery: events, local books, one connection.

A `Stream` owns one WebSocket to one venue endpoint. It connects, sends the
venue's subscription frames, turns every message into typed events, and puts
them on a queue the caller reads with `async for event in stream`. When the
connection drops it reconnects with jittered exponential backoff and sends
every subscription again; the caller's loop keeps running.

Three promises, because an execution engine is built on them:

* **Nothing is lost silently.** A stream that can detect a missed message
  (a sequence gap on Kalshi, a best bid or ask that disagrees with the local
  book on Polymarket) emits a `StreamStatusEvent(state="gap")` and
  resynchronizes. A reconnect on a channel that cannot be replayed (every
  private channel) emits `reconcile_required=True`, so the engine knows to
  read the venue's REST state before trusting its own.
* **A book is either right or marked not ready.** Deltas that arrive while a
  book is waiting for its snapshot are not applied to a stale base.
* **The loop never dies on one bad message.** A payload that cannot be read
  becomes a `StreamStatusEvent(state="error")`; the connection stays up.

Events are frozen, slotted dataclasses rather than pydantic models: a busy
book emits thousands a second, and the orders, fills, positions and balances
they carry are already validated trading models.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, AsyncIterator, Awaitable, Callable, Literal

from ..base import Capability, complete_capabilities
from ..trading.types import Balance, Fill, Order, Position, Side

log = logging.getLogger("synpath.ws")

ZERO = Decimal("0")
ONE = Decimal("1")


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    size: Decimal
    """For a delta, the level's size after the change; zero means removed."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    venue: str
    received_at: int = field(default_factory=now_ms)


@dataclass(frozen=True, slots=True, kw_only=True)
class BookEvent(Event):
    """A book changed. `kind="snapshot"` replaces the book; `kind="delta"`
    lists only the levels that changed, each with its new size. Prices are in
    the terms of `side`: a NO book is what NO costs. `best_bid` and
    `best_ask` are the local book's after the change."""

    market_id: str
    """Synpath id, `venue:native`."""
    side: Literal["yes", "no"] = "yes"
    kind: Literal["snapshot", "delta"]
    bids: tuple[BookLevel, ...] = ()
    asks: tuple[BookLevel, ...] = ()
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    sequence: int | None = None
    timestamp: int | None = None
    info: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class QuoteEvent(Event):
    """Top of book and market statistics, as the venue summarizes them, in
    the terms of `side`."""

    market_id: str
    side: Literal["yes", "no"] = "yes"
    bid: Decimal | None = None
    ask: Decimal | None = None
    bid_size: Decimal | None = None
    ask_size: Decimal | None = None
    last: Decimal | None = None
    volume: Decimal | None = None
    open_interest: Decimal | None = None
    timestamp: int | None = None
    info: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class TradeEvent(Event):
    """A public print, in the YES price. `taker_side` is what the aggressor
    did on the YES leg (`buy` took YES, `sell` took NO) where the venue says,
    `None` where it does not."""

    market_id: str
    id: str | None
    price: Decimal
    amount: Decimal
    taker_side: Side | None = None
    timestamp: int | None = None
    info: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderEvent(Event):
    """One of this account's orders, as the venue now reports it. `native`
    names the venue's own event (placement, update, cancellation, snapshot)."""

    order: Order
    native: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class FillEvent(Event):
    """One of this account's fills. On Polymarket the same fill arrives again
    as its settlement state moves from `matched` to `confirmed` or `failed`."""

    fill: Fill


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionEvent(Event):
    position: Position


@dataclass(frozen=True, slots=True, kw_only=True)
class BalanceEvent(Event):
    balance: Balance


MarketState = Literal["created", "open", "paused", "closed", "determined", "settled", "updated"]


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketStatusEvent(Event):
    """A market opened, paused, resumed, closed, resolved or settled. `native`
    keeps the venue's own word; `result` is the outcome once determined."""

    market_id: str
    state: MarketState
    native: str | None = None
    result: str | None = None
    timestamp: int | None = None
    info: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, kw_only=True)
class VenueEvent(Event):
    """Something venue-specific with no unified shape: a Kalshi order group
    tripping, a Polymarket tick size change, an RFQ."""

    name: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: int | None = None


StreamState = Literal["connected", "disconnected", "connect_failed", "subscribed", "gap", "resynced", "error", "failed"]


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamStatusEvent(Event):
    """The stream itself: connection lifecycle, gaps and recoveries, errors.

    `failed` is final: the venue refused the stream in a way retrying cannot
    fix (no permission, a request it rejects), and iteration ends after it.

    `reconcile_required` is set when events may have been missed that the
    venue cannot replay -- a reconnect with private subscriptions, or a gap
    on a channel with no snapshot to recover from. Read the venue's REST
    state before trusting anything derived from the stream."""

    stream: str
    state: StreamState
    detail: str = ""
    key: str | None = None
    reconcile_required: bool = False


# ---------------------------------------------------------------------------
# Local book
# ---------------------------------------------------------------------------

class LocalBook:
    """One side of a market's book: price -> size on each side, plus readiness.

    `ready` is false until a snapshot has been applied and becomes false again
    when the stream loses confidence in it (a gap, a reconnect); a book that
    is not ready ignores deltas rather than applying them to a stale base.
    """

    __slots__ = ("bids", "asks", "ready", "sequence", "timestamp")

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.ready = False
        self.sequence: int | None = None
        self.timestamp: int | None = None

    def replace(self, bids: list[tuple[Decimal, Decimal]], asks: list[tuple[Decimal, Decimal]]) -> None:
        self.bids = {p: s for p, s in bids if s > 0}
        self.asks = {p: s for p, s in asks if s > 0}
        self.ready = True

    def set(self, side: Literal["bid", "ask"], price: Decimal, size: Decimal) -> Decimal:
        levels = self.bids if side == "bid" else self.asks
        if size > 0:
            levels[price] = size
        else:
            levels.pop(price, None)
        return size if size > 0 else ZERO

    def add(self, side: Literal["bid", "ask"], price: Decimal, delta: Decimal) -> Decimal:
        levels = self.bids if side == "bid" else self.asks
        return self.set(side, price, levels.get(price, ZERO) + delta)

    def invalidate(self) -> None:
        self.ready = False

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    def levels(self, depth: int | None = None) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
        """Bids best first (descending), asks best first (ascending)."""
        bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])
        if depth is not None:
            bids, asks = bids[:depth], asks[:depth]
        return tuple(BookLevel(p, s) for p, s in bids), tuple(BookLevel(p, s) for p, s in asks)

    def mirrored(self, face_value: Decimal = ONE) -> "LocalBook":
        """The other side's view: bids become asks at `1 - p`."""
        other = LocalBook()
        other.bids = {face_value - p: s for p, s in self.asks.items()}
        other.asks = {face_value - p: s for p, s in self.bids.items()}
        other.ready, other.sequence, other.timestamp = self.ready, self.sequence, self.timestamp
        return other


def D(value: Any) -> Decimal:
    if isinstance(value, dict):
        value = value.get("value")
    return Decimal(str(value))


def maybe_D(value: Any) -> Decimal | None:
    if isinstance(value, dict):
        value = value.get("value")
    if value in (None, ""):
        return None
    return Decimal(str(value))


# ---------------------------------------------------------------------------
# Stream
# ---------------------------------------------------------------------------

Connector = Callable[..., Awaitable[Any]]

_CLOSED = object()


@dataclass
class StreamStats:
    connects: int = 0
    disconnects: int = 0
    messages: int = 0
    gaps: int = 0
    resyncs: int = 0
    errors: int = 0
    last_message_at: int | None = None


class Stream:
    """One venue WebSocket, kept alive, turned into events.

    Subclasses provide the endpoint (`url`, `headers()`), what to send after
    connecting (`on_connect()`), how to read a message (`handle()`), and what
    to forget when the connection drops (`on_disconnect()`). Subscribing is
    recording an intent: it is sent now if connected and again after every
    reconnect.
    """

    venue: str = ""
    name: str = "stream"
    has: dict[str, Capability] = {}
    private: bool = False
    """Carries account data the venue cannot replay after a disconnect."""
    app_ping: str | None = None
    """A text frame the venue wants as its heartbeat, sent every `ping_interval`."""
    data_heartbeat: bool = False
    """Whether the venue's keepalive arrives as data this stream can see (a
    `PONG` text frame, a heartbeat message). Only then does silence mean a
    dead connection: a venue that keeps the socket alive with protocol ping
    frames can be quiet for minutes on a slow market, and there the
    WebSocket library's own ping/pong is what detects a dead peer."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        complete_capabilities(cls)

    def __init__(
        self,
        url: str,
        *,
        connect: Connector | None = None,
        ping_interval: float = 10.0,
        idle_timeout: float | None | Literal["auto"] = "auto",
        backoff_initial: float = 0.5,
        backoff_max: float = 30.0,
        max_queue: int = 0,
    ):
        self.url = url
        self._connect = connect
        self.ping_interval = ping_interval
        self.idle_timeout = (60.0 if self.data_heartbeat else None) if idle_timeout == "auto" else idle_timeout
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._last_seen = 0
        self._stale = ""
        self._ws: Any = None
        self._task: asyncio.Task | None = None
        self._closing = False
        self.stats = StreamStats()
        self.connected = asyncio.Event()

    # -- venue hooks ----------------------------------------------------------

    def headers(self) -> dict[str, str]:
        """Handshake headers, built fresh for every connection attempt."""
        return {}

    async def on_connect(self) -> None:
        """Send every recorded subscription."""

    def on_disconnect(self) -> None:
        """Forget connection-scoped state: subscription ids, sequence numbers, book readiness."""

    def handle(self, message: Any) -> list[Event]:
        """One decoded message as events."""
        return []

    # -- lifecycle ------------------------------------------------------------

    def start(self) -> "Stream":
        if self._task is None or self._task.done():
            self._closing = False
            self._task = asyncio.get_running_loop().create_task(self._run(), name=f"synpath-{self.venue}-{self.name}")
        return self

    async def close(self) -> None:
        self._closing = True
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # pragma: no cover - closing a dead socket
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self._queue.put_nowait(_CLOSED)

    async def __aenter__(self) -> "Stream":
        return self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[Event]:
        return self.events()

    async def events(self) -> AsyncIterator[Event]:
        """Every event, in the order it was produced, until `close()`. Starts
        the stream if it has not been started; a closed stream stays closed."""
        if self._task is None and not self._closing:
            self.start()
        while True:
            item = await self._queue.get()
            if item is _CLOSED:
                return
            yield item

    def emit(self, event: Event) -> None:
        self._queue.put_nowait(event)

    def status(self, state: StreamState, detail: str = "", *, key: str | None = None, reconcile: bool = False) -> None:
        if state == "gap":
            self.stats.gaps += 1
        elif state == "resynced":
            self.stats.resyncs += 1
        elif state == "error":
            self.stats.errors += 1
        self.emit(StreamStatusEvent(
            venue=self.venue, stream=self.name, state=state, detail=detail, key=key, reconcile_required=reconcile,
        ))

    async def send(self, frame: Any) -> bool:
        """Send if connected; `False` when the frame will instead go out on the next connect."""
        ws = self._ws
        if ws is None:
            return False
        await ws.send(frame if isinstance(frame, str) else json.dumps(frame))
        return True

    def backoff(self, attempt: int) -> float:
        base = min(self.backoff_max, self.backoff_initial * (2 ** attempt))
        return base * (0.5 + random.random() / 2)

    async def _open(self) -> Any:
        if self._connect is not None:
            return await self._connect(self.url, additional_headers=self.headers())
        import websockets

        return await websockets.connect(
            self.url, additional_headers=self.headers(),
            ping_interval=None if self.app_ping else 20, max_size=None,
        )

    async def _run(self) -> None:
        attempt = 0
        while not self._closing:
            try:
                ws = await self._open()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.status("connect_failed", f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(self.backoff(attempt))
                attempt += 1
                continue
            attempt = 0
            self._ws = ws
            reconnect = self.stats.connects > 0
            self.stats.connects += 1
            self.connected.set()
            self.status("connected", "reconnected" if reconnect else "", reconcile=reconnect and self.private)
            pinger = watchdog = None
            reason = "closed"
            self._last_seen, self._stale = now_ms(), ""
            try:
                await self.on_connect()
                if self.app_ping:
                    pinger = asyncio.get_running_loop().create_task(self._pinger(ws))
                if self.idle_timeout:
                    watchdog = asyncio.get_running_loop().create_task(self._watchdog(ws))
                while not self._closing:
                    if self.idle_timeout:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout)
                    else:
                        raw = await ws.recv()
                    self._dispatch(raw)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                reason = f"no message for {self.idle_timeout}s"
            except Exception as exc:
                reason = self._stale or f"{type(exc).__name__}: {exc}"
            finally:
                for task in (pinger, watchdog):
                    if task:
                        task.cancel()
                self._ws = None
                self.connected.clear()
                self.stats.disconnects += 1
                self.on_disconnect()
            if self._closing:
                return
            self.status("disconnected", reason)
            try:
                await ws.close()
            except Exception:
                pass
            await asyncio.sleep(self.backoff(0))

    async def _watchdog(self, ws: Any) -> None:
        """Silence measured on the wall clock, not the event loop's.

        `asyncio`'s timers stop while the machine is suspended, so a laptop
        that sleeps for twenty minutes wakes with a socket the venue closed
        long ago and an idle timer that believes no time has passed. The
        operating system can take another quarter of an hour to notice the
        connection is gone. This wakes with the loop, sees the gap in real
        time, and drops the connection so the stream reconnects.
        """
        step = max(1.0, (self.idle_timeout or 0) / 4)
        while True:
            await asyncio.sleep(step)
            silent = (now_ms() - self._last_seen) / 1000
            if self.idle_timeout and silent > self.idle_timeout:
                self._stale = f"no message for {round(silent)}s (wall clock)"
                try:
                    await ws.close()
                except Exception:  # pragma: no cover - closing a dead socket
                    pass
                return

    async def _pinger(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            await ws.send(self.app_ping)

    def _dispatch(self, raw: Any) -> None:
        self.stats.messages += 1
        self._last_seen = now_ms()
        self.stats.last_message_at = now_ms()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            message = json.loads(raw)
        except ValueError:
            message = raw
        try:
            for event in self.handle(message):
                self.emit(event)
        except Exception as exc:
            log.debug("synpath.ws %s %s: unreadable message %r", self.venue, self.name, raw, exc_info=True)
            self.status("error", f"unreadable message: {type(exc).__name__}: {exc}")
