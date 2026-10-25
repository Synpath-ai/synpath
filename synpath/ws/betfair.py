"""Betfair's Exchange Stream API: runner books and market status, and the
account's bets, matches and positions, live.

Not a WebSocket. The stream is one TLS socket to `stream-api.betfair.com:443`
carrying JSON messages, one per line, each ended by CRLF. `LineSocket` wraps
the socket in the `recv`/`send`/`close` shape the base `Stream` drives, so the
reconnects, backoff and wall-clock watchdog are the same as every other venue's.

The protocol, as this stream speaks it:

* The server opens with `{"op": "connection"}`. The client authenticates with
  `{"op": "authentication", "appKey", "session"}` straight away: Betfair drops
  a connection that has not sent a first message within 15 seconds.
* `{"op": "marketSubscription"}` names the Betfair markets (at most 200 on a
  connection) and asks for the best offers and the market definition. A new
  subscription replaces the old one rather than adding to it, so every change
  of markets resends the whole set.
* Data arrives as `{"op": "mcm"}` market change messages. `ct` says what kind:
  `SUB_IMAGE` (the whole state), `RESUB_DELTA` (what changed while
  disconnected), `HEARTBEAT` (nothing changed); none means an update. A market
  with `img: true` replaces its cache. Best offers are positional ladders,
  `batb` (best available to back) and `batl` (to lay), as `[level, odds,
  stake]`; a stake of 0 deletes the level.
* Every `mcm` carries `clk` (and the first `initialClk`). A reconnect
  resubscribes with both, and Betfair answers with a `RESUB_DELTA` from where
  the stream left off instead of a fresh image.

Books are read on YES exactly as the REST adapter reads them: a resting back
(`batl`, what a taker can lay into) is a bid at 1/odds, a resting lay (`batb`)
an ask, and a stake at odds O is stake·O contracts.

**A delayed application key is conflated to 180,000 ms**: Betfair sends at most
one update per market every three minutes, whatever `conflateMs` asks. The
books are still right, only late. The stream warns once when it sees it.

Betfair publishes no individual trades, so there is no `watch_trades`.

**The account** (`BetfairUserStream`) is the same protocol on its own
connection: `{"op": "orderSubscription"}` and `{"op": "ocm"}` order change
messages, per Betfair market (`oc`) and runner (`orc`). Each changed bet
(`uo`) is sent whole; the matched ladders per runner (`mb`, `ml`, `[odds,
stake]`, a stake of 0 deleting the point) are deltas. A new subscription's
image carries only bets still unmatched in part; a bet that finishes sends
its `EC` (execution complete) once, then leaves the stream.
"""
from __future__ import annotations

import asyncio
import logging
import ssl
from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from .. import ids
from ..base import Capability
from ..errors import AuthenticationError
from ..trading.types import OrderStatus
from .base import (
    BookEvent, BookLevel, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent, Stream,
    now_ms,
)

log = logging.getLogger("synpath.ws.betfair")

VENUE = "betfair"
STREAM_HOST = "stream-api.betfair.com"
STREAM_PORT = 443

MAX_MARKETS = 200
"""Markets one connection may subscribe, by default, before Betfair refuses
with SUBSCRIPTION_LIMIT_EXCEEDED."""

MAX_LADDER_LEVELS = 10
"""Deepest best-offers ladder Betfair streams."""

FIRST_MESSAGE_TIMEOUT = 15.0
"""Seconds Betfair waits for, and this stream allows for, a connection's first message."""

DELAYED_CONFLATE_MS = 180_000
"""What Betfair conflates a delayed key's stream to: one update per market every three minutes."""

LINE_LIMIT = 2 ** 24
"""Largest line read: a subscription image of many markets is one line."""

ONE = Decimal(1)
PRICE_PLACES = Decimal("1e-10")

APP_KEY_ERRORS = frozenset({"INVALID_APP_KEY", "NO_APP_KEY", "NOT_AUTHORIZED"})
SESSION_ERRORS = frozenset({"INVALID_SESSION_INFORMATION", "NO_SESSION"})

RESULTS = {"WINNER": "yes", "LOSER": "no", "PLACED": "yes", "REMOVED": "void"}


class LineSocket:
    """A TLS socket carrying CRLF-delimited JSON, shaped like a WebSocket.

    The first `recv` waits at most `first_timeout`: a server that accepts the
    connection and then says nothing is treated as a failed connection, not
    as a quiet market."""

    def __init__(self, reader: asyncio.StreamReader, writer: Any, *, first_timeout: float = FIRST_MESSAGE_TIMEOUT):
        self.reader, self.writer = reader, writer
        self._first_timeout: float | None = first_timeout

    async def recv(self) -> str:
        timeout, self._first_timeout = self._first_timeout, None
        try:
            line = await asyncio.wait_for(self.reader.readuntil(b"\r\n"), timeout)
        except asyncio.IncompleteReadError:
            raise ConnectionError("Betfair closed the stream") from None
        return line.decode("utf-8").rstrip("\r\n")

    async def send(self, text: str) -> None:
        self.writer.write(text.encode("utf-8") + b"\r\n")
        await self.writer.drain()

    async def close(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:  # pragma: no cover - closing a dead socket
            pass


def yes_price(odds: Any) -> Decimal:
    """Decimal odds as the YES price 1/O, to ten places."""
    return (ONE / Decimal(str(odds))).quantize(PRICE_PLACES)


def yes_levels(ladder: dict[int, tuple[Decimal, Decimal]]) -> list[tuple[Decimal, Decimal]]:
    """A positional Betfair ladder as YES (price, contracts) pairs."""
    return [(yes_price(odds), stake * odds) for odds, stake in ladder.values() if stake > 0 and odds > 1]


def apply_ladder(ladder: dict[int, tuple[Decimal, Decimal]], updates: Any) -> None:
    """`[level, odds, stake]` updates onto a positional ladder; stake 0 deletes."""
    for update in updates or []:
        level, odds, stake = int(update[0]), Decimal(str(update[1])), Decimal(str(update[2]))
        if stake == 0:
            ladder.pop(level, None)
        else:
            ladder[level] = (odds, stake)


def state_of(market_status: str | None, runner_status: str | None) -> tuple[str, str | None]:
    """A runner's stream state and result from its market's and its own status."""
    if runner_status in RESULTS:
        return "settled", RESULTS[runner_status]
    return {"OPEN": "open", "SUSPENDED": "paused", "CLOSED": "closed", "INACTIVE": "created"}.get(
        market_status or "", "updated"), None


@dataclass
class RunnerCache:
    back: dict[int, tuple[Decimal, Decimal]] = field(default_factory=dict)
    """`batb`: what a taker can back at, resting lays. YES asks."""
    lay: dict[int, tuple[Decimal, Decimal]] = field(default_factory=dict)
    """`batl`: what a taker can lay at, resting backs. YES bids."""


@dataclass
class MarketCache:
    definition: dict[str, Any] = field(default_factory=dict)
    runners: dict[tuple[int, float], RunnerCache] = field(default_factory=dict)


def runner_key(selection_id: Any, handicap: Any) -> tuple[int, float]:
    return int(selection_id), float(handicap or 0)


def runner_id(market_id: str, key: tuple[int, float]) -> str:
    from ..betfair import runner_native_id

    return ids.qualify(VENUE, runner_native_id(market_id, key[0], key[1]))


class _BetfairStream(Stream):
    """One Exchange Stream API connection: the TLS socket, authentication and
    Betfair's status messages. The market and the order streams are each
    one subscription on their own connection."""

    venue = VENUE
    data_heartbeat = True

    def __init__(
        self, *, credentials: Any = None, session: Any = None, host: str = STREAM_HOST, port: int = STREAM_PORT,
        heartbeat_ms: int = 5000, open_connection: Callable[..., Any] | None = None,
        ssl_context: ssl.SSLContext | None = None, **kwargs: Any,
    ):
        from ..betfair import session_from

        self.session = session or session_from(credentials)
        if self.session is None:
            raise AuthenticationError(
                f"{VENUE}: the stream needs an account: set BETFAIR_APP_KEY, BETFAIR_USERNAME and "
                f"BETFAIR_PASSWORD, or pass credentials= or session="
            )
        super().__init__(f"tls://{host}:{port}", **kwargs)
        self.host, self.port = host, port
        self.heartbeat_ms = heartbeat_ms
        self._open_connection = open_connection or asyncio.open_connection
        self._ssl = ssl_context or ssl.create_default_context()
        self.authenticated = False
        self.connection_id: str | None = None
        self.initial_clk: str | None = None
        self.clk: str | None = None
        self._next_id = 0
        self._auth_id: int | None = None
        self._sub_id: int | None = None

    # -- connection -----------------------------------------------------------

    async def _open(self) -> Any:
        reader, writer = await self._open_connection(
            self.host, self.port, ssl=self._ssl, server_hostname=self.host, limit=LINE_LIMIT,
        )
        return LineSocket(reader, writer)

    def _id(self) -> int:
        self._next_id += 1
        return self._next_id

    async def on_connect(self) -> None:
        self.authenticated = False
        token = await asyncio.to_thread(self.session.token)
        self._auth_id = self._id()
        await self.send({"op": "authentication", "id": self._auth_id, "appKey": self.session.app_key, "session": token})

    def on_disconnect(self) -> None:
        self.authenticated = False

    async def resubscribe(self) -> None:
        """Send the subscription, once authenticated."""

    def subscribed(self) -> None:
        """Betfair accepted the subscription."""

    # -- status messages ------------------------------------------------------

    def _on_status(self, message: dict[str, Any]) -> list[Event]:
        if message.get("statusCode") == "SUCCESS":
            if message.get("id") == self._auth_id:
                self.authenticated = True
                self._later(self.resubscribe())
            elif message.get("id") == self._sub_id:
                self.subscribed()
            return []
        code = str(message.get("errorCode") or "")
        detail = f"{code}: {message.get('errorMessage') or ''}".strip(": ")
        if code in APP_KEY_ERRORS:
            self.status("failed", f"Betfair refused the application key for streaming ({detail})")
            self._later(self.close())
        elif code in SESSION_ERRORS:
            # The next connection logs in again.
            self.session.invalidate()
            self.status("error", f"session refused ({detail}); logging in again")
        else:
            self.status("error", f"Betfair refused: {detail}")
        return []

    def _later(self, coroutine: Any) -> asyncio.Task | None:
        try:
            return asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:
            coroutine.close()
            return None


class BetfairMarketStream(_BetfairStream):
    """Runner books (best offers) and market status for Betfair markets, over
    the Exchange Stream API. Needs the account's application key and a
    session, as every Betfair call does.

    With a delayed key, Betfair conflates the stream to one update per market
    every 180,000 ms (three minutes), whatever `conflate_ms` asks; the stream
    logs a warning once when it sees that.

    ```python
    async with BetfairMarketStream() as stream:       # BETFAIR_* from the environment
        await stream.watch_order_book(["betfair:1.234567890-47972"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        # Top of book from the best offers; no last traded price is subscribed.
        "watch_ticker": "partial",
        # Betfair publishes no individual trades.
        "watch_trades": False,
        "watch_market_status": True,
    }

    def __init__(
        self, *, credentials: Any = None, session: Any = None, host: str = STREAM_HOST, port: int = STREAM_PORT,
        ladder_levels: int = 3, conflate_ms: int | None = None, heartbeat_ms: int = 5000, delayed: bool = True,
        open_connection: Callable[..., Any] | None = None, ssl_context: ssl.SSLContext | None = None, **kwargs: Any,
    ):
        if not 1 <= ladder_levels <= MAX_LADDER_LEVELS:
            raise ValueError(f"ladder_levels must be 1 to {MAX_LADDER_LEVELS}")
        # A delayed stream can be silent for a whole conflation window, so
        # silence means a dead socket only after one window and some slack.
        kwargs.setdefault("idle_timeout", DELAYED_CONFLATE_MS / 1000 + 60 if delayed else max(60.0, heartbeat_ms * 3 / 1000))
        super().__init__(credentials=credentials, session=session, host=host, port=port, heartbeat_ms=heartbeat_ms,
                         open_connection=open_connection, ssl_context=ssl_context, **kwargs)
        self.ladder_levels = ladder_levels
        self.conflate_ms = conflate_ms
        self.delayed = delayed
        self.markets: dict[str, MarketCache] = {}
        """Betfair market id -> its cache, kept across reconnects for the resubscribe delta."""
        self.books: dict[str, LocalBook] = {}
        """Runner's Synpath id -> its YES book."""
        self.tickers: set[str] = set()
        self.statuses: set[str] | None = None
        """Betfair market ids whose status changes are passed on; `None` while not watching."""
        self.wanted: set[str] = set()
        """Betfair market ids the subscription carries."""
        self.whole_markets: set[str] = set()
        """Betfair markets watched as a whole: every runner's book is kept."""
        self._clk_for: frozenset[str] = frozenset()
        """The market set `clk` belongs to: a resubscribe only resumes that one."""
        self._last_state: dict[str, tuple[str, str | None]] = {}
        self._warned_delayed = False

    def on_disconnect(self) -> None:
        super().on_disconnect()
        for book in self.books.values():
            book.invalidate()

    # -- intents --------------------------------------------------------------

    def _split(self, market_ids: list[str]) -> list[tuple[str, tuple[int, float] | None]]:
        """(Betfair market id, runner key or None for the whole market) per id."""
        from ..betfair import parse_runner_id

        out = []
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            if "-" in native:
                market, selection, handicap = parse_runner_id(native)
                out.append((market, runner_key(selection, handicap)))
            else:
                out.append((native, None))
        return out

    def _want(self, markets: list[str]) -> list[str]:
        """Add Betfair markets to the subscription, up to the cap; the ones
        past it are reported and left out."""
        refused = []
        for market in markets:
            if market in self.wanted:
                continue
            if len(self.wanted) >= MAX_MARKETS:
                refused.append(market)
                continue
            self.wanted.add(market)
        if refused:
            self.status("error", f"more than {MAX_MARKETS} markets on one connection; not subscribed: {', '.join(refused)}")
        return refused

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Runner books, best offers to `ladder_levels` deep. A Betfair market
        id (`betfair:1.234567890`) keeps every runner's book in it."""
        targets = self._split(market_ids)
        refused = set(self._want([market for market, _ in targets]))
        for market, key in targets:
            if market in refused:
                continue
            if key is None:
                self.whole_markets.add(market)
            else:
                self.books.setdefault(runner_id(market, key), LocalBook())
        await self.resubscribe()

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book on every change, for runners."""
        targets = self._split(market_ids)
        refused = set(self._want([market for market, _ in targets]))
        self.tickers.update(runner_id(m, key) for m, key in targets if key is not None and m not in refused)
        await self.resubscribe()

    async def watch_market_status(self, market_ids: list[str] | None = None) -> None:
        """Status changes (suspended, reopened, closed, settled) of each runner
        in these markets. Betfair streams only markets it is asked for, so
        there is no "every market"."""
        targets = self._split(market_ids or [])
        refused = set(self._want([market for market, _ in targets]))
        self.statuses = (self.statuses or set()) | {m for m, _ in targets if m not in refused}
        await self.resubscribe()

    async def unwatch(self, market_ids: list[str]) -> None:
        for market, key in self._split(market_ids):
            if key is not None:
                self.books.pop(runner_id(market, key), None)
                self.tickers.discard(runner_id(market, key))
                continue
            self.whole_markets.discard(market)
            for synpath_id in [s for s in self.books if s.startswith(f"{VENUE}:{market}-")]:
                self.books.pop(synpath_id, None)
            self.wanted.discard(market)
            self.markets.pop(market, None)
            if self.statuses:
                self.statuses.discard(market)
        await self.resubscribe()

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(ids.qualify(VENUE, ids.native(VENUE, market_id)))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    # -- subscription ---------------------------------------------------------

    def subscription(self) -> dict[str, Any]:
        """The one subscription frame for every market wanted. It resumes from
        `clk` only when the market set is the one the clock belongs to."""
        markets = frozenset(self.wanted)
        frame: dict[str, Any] = {
            "op": "marketSubscription",
            "id": self._id(),
            "marketFilter": {"marketIds": sorted(markets)},
            "marketDataFilter": {"fields": ["EX_BEST_OFFERS", "EX_MARKET_DEF"], "ladderLevels": self.ladder_levels},
            "heartbeatMs": self.heartbeat_ms,
        }
        if self.conflate_ms is not None:
            frame["conflateMs"] = self.conflate_ms
        if self.clk and self.initial_clk and markets == self._clk_for:
            frame["initialClk"], frame["clk"] = self.initial_clk, self.clk
        else:
            self.initial_clk = self.clk = None
            self._clk_for = markets
        return frame

    async def resubscribe(self) -> None:
        if not self.authenticated or not self.wanted:
            return
        frame = self.subscription()
        self._sub_id = frame["id"]
        await self.send(frame)

    # -- messages -------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        op = message.get("op")
        if op == "connection":
            self.connection_id = message.get("connectionId")
            return []
        if op == "status":
            return self._on_status(message)
        if op == "mcm":
            return self._on_market_changes(message)
        return []

    def subscribed(self) -> None:
        self.status("subscribed", f"{len(self.wanted)} markets")

    def _on_market_changes(self, message: dict[str, Any]) -> list[Event]:
        if message.get("initialClk"):
            self.initial_clk = str(message["initialClk"])
        if message.get("clk"):
            self.clk = str(message["clk"])
        self._warn_if_delayed(message.get("conflateMs"))
        kind = message.get("ct")
        if kind == "HEARTBEAT":
            return []
        if kind == "SUB_IMAGE" and message.get("segmentType") in (None, "SEG_START"):
            self.markets.clear()
        events: list[Event] = []
        for change in message.get("mc") or []:
            if isinstance(change, dict) and change.get("id"):
                events += self._on_market_change(change, image=bool(change.get("img")))
        if kind in ("SUB_IMAGE", "RESUB_DELTA") and message.get("segmentType") in (None, "SEG_END"):
            events += self._recover()
        return events

    def _warn_if_delayed(self, conflate_ms: Any) -> None:
        if self._warned_delayed or not conflate_ms or int(conflate_ms) < DELAYED_CONFLATE_MS:
            return
        self._warned_delayed = True
        log.warning(
            "synpath.ws betfair: the stream is conflated to %s ms, as Betfair does for a delayed application "
            "key: books update at most once every three minutes per market", conflate_ms,
        )

    def _on_market_change(self, change: dict[str, Any], *, image: bool) -> list[Event]:
        market = str(change["id"])
        cache = MarketCache() if image else self.markets.setdefault(market, MarketCache())
        if image:
            self.markets[market] = cache
        events: list[Event] = []
        if isinstance(change.get("marketDefinition"), dict):
            cache.definition = change["marketDefinition"]
            events += self._status_changes(market, cache.definition)
        touched: list[tuple[int, float]] = []
        for runner_change in change.get("rc") or []:
            key = runner_key(runner_change["id"], runner_change.get("hc"))
            runner = cache.runners.setdefault(key, RunnerCache())
            apply_ladder(runner.back, runner_change.get("batb"))
            apply_ladder(runner.lay, runner_change.get("batl"))
            touched.append(key)
        if image:
            # Runners an image does not mention have no offers.
            touched = list(cache.runners)
        for key in touched:
            events += self._book_events(market, key, cache.runners[key], snapshot=image)
        return events

    def _watched(self, market: str, synpath_id: str) -> bool:
        return synpath_id in self.books or synpath_id in self.tickers or market in self.whole_markets

    def _book_events(self, market: str, key: tuple[int, float], runner: RunnerCache, *, snapshot: bool) -> list[Event]:
        synpath_id = runner_id(market, key)
        if not self._watched(market, synpath_id):
            return []
        bids, asks = yes_levels(runner.lay), yes_levels(runner.back)
        events: list[Event] = []
        book = self.books.get(synpath_id)
        if book is None and market in self.whole_markets:
            book = self.books.setdefault(synpath_id, LocalBook())
        # A delta for a book that lost its base is held in the cache until
        # the resubscribe delta or a new image completes it (`_recover`).
        if book is not None and (book.ready or snapshot):
            kind = "delta" if book.ready and not snapshot else "snapshot"
            recovering = not book.ready and book.timestamp is not None
            old_bids, old_asks = book.levels() if kind == "delta" else ((), ())
            book.replace(bids, asks)
            book.timestamp = now_ms()
            new_bids, new_asks = book.levels()
            if kind == "delta":
                new_bids, new_asks = changed(old_bids, new_bids), changed(old_asks, new_asks)
            events.append(BookEvent(
                venue=VENUE, market_id=synpath_id, side="yes", kind=kind, bids=new_bids, asks=new_asks,
                best_bid=book.best_bid, best_ask=book.best_ask, info={"delayed": self.delayed, "clk": self.clk},
            ))
            if recovering:
                self.status("resynced", f"book {synpath_id}", key=synpath_id)
        if synpath_id in self.tickers:
            best_bid = max(bids, default=None, key=lambda level: level[0])
            best_ask = min(asks, default=None, key=lambda level: level[0])
            events.append(QuoteEvent(
                venue=VENUE, market_id=synpath_id,
                bid=best_bid[0] if best_bid else None, bid_size=best_bid[1] if best_bid else None,
                ask=best_ask[0] if best_ask else None, ask_size=best_ask[1] if best_ask else None,
                info={"delayed": self.delayed},
            ))
        return events

    def _recover(self) -> list[Event]:
        """After an image or a resubscribe delta the cache is current again:
        every book that lost its base is rebuilt from it and marked ready."""
        events: list[Event] = []
        for market, cache in self.markets.items():
            for key, runner in cache.runners.items():
                synpath_id = runner_id(market, key)
                book = self.books.get(synpath_id)
                if book is not None and not book.ready:
                    events += self._book_events(market, key, runner, snapshot=True)
        return events

    def _status_changes(self, market: str, definition: dict[str, Any]) -> list[Event]:
        if self.statuses is None or (self.statuses and market not in self.statuses):
            return []
        events: list[Event] = []
        market_status = definition.get("status")
        for runner in definition.get("runners") or []:
            synpath_id = runner_id(market, runner_key(runner.get("id"), runner.get("hc")))
            state, result = state_of(market_status, runner.get("status"))
            if self._last_state.get(synpath_id) == (state, result):
                continue
            self._last_state[synpath_id] = (state, result)
            events.append(MarketStatusEvent(
                venue=VENUE, market_id=synpath_id, state=state,  # type: ignore[arg-type]
                native=f"{market_status}/{runner.get('status')}", result=result,
                info={"in_play": definition.get("inPlay"), "bet_delay": definition.get("betDelay"),
                      "version": definition.get("version")},
            ))
        return events


def changed(old: tuple[BookLevel, ...], new: tuple[BookLevel, ...]) -> tuple[BookLevel, ...]:
    """The levels that differ between two sides, a removed one at size 0."""
    before = {level.price: level.size for level in old}
    after = {level.price: level.size for level in new}
    out = [BookLevel(price, size) for price, size in after.items() if before.get(price) != size]
    out += [BookLevel(price, Decimal(0)) for price in before if price not in after]
    return tuple(out)


# ---------------------------------------------------------------------------
# The account's orders
# ---------------------------------------------------------------------------

BET_MEMORY = 8192
"""Bets whose matched amount the order stream remembers, most recent first."""

STREAM_SIDES = {"B": "BACK", "L": "LAY"}
STREAM_STATUS = {"E": "EXECUTABLE", "EC": "EXECUTION_COMPLETE"}
STREAM_PERSISTENCE = {"L": "LAPSE", "P": "PERSIST", "MOC": "MARKET_ON_CLOSE"}
STREAM_ORDER_TYPES = {"L": "LIMIT", "MOC": "MARKET_ON_CLOSE", "LOC": "LIMIT_ON_CLOSE"}


def current_row(order: dict[str, Any], *, market: str, selection: Any, handicap: Any) -> dict[str, Any]:
    """A stream order (`uo`) in the shape `listCurrentOrders` reports, so the
    one reader turns both into an `Order`."""
    from ..types import iso

    def when(key: str) -> str | None:
        return iso(int(order[key])) if order.get(key) else None

    return {
        "betId": str(order.get("id") or ""), "marketId": market, "selectionId": int(selection),
        "handicap": float(handicap or 0), "priceSize": {"price": order.get("p"), "size": order.get("s")},
        "bspLiability": order.get("bsp"), "side": STREAM_SIDES.get(str(order.get("side")), order.get("side")),
        "status": STREAM_STATUS.get(str(order.get("status")), order.get("status")),
        "persistenceType": STREAM_PERSISTENCE.get(str(order.get("pt")), order.get("pt")),
        "orderType": STREAM_ORDER_TYPES.get(str(order.get("ot")), order.get("ot")),
        "placedDate": when("pd"), "matchedDate": when("md"), "cancelledDate": when("cd"), "lapsedDate": when("ld"),
        "lapseStatusReasonCode": order.get("lsrc"),
        "averagePriceMatched": order.get("avp") or 0, "sizeMatched": order.get("sm") or 0,
        "sizeRemaining": order.get("sr") or 0, "sizeLapsed": order.get("sl") or 0,
        "sizeCancelled": order.get("sc") or 0, "sizeVoided": order.get("sv") or 0,
        "regulatorCode": order.get("rc"), "regulatorAuthCode": order.get("rac"),
        "customerOrderRef": order.get("rfo") or None, "customerStrategyRef": order.get("rfs") or None,
    }


@dataclass
class RunnerPosition:
    backs: dict[Decimal, Decimal] = field(default_factory=dict)
    """`mb`: matched backing stake by matched odds. YES contracts."""
    lays: dict[Decimal, Decimal] = field(default_factory=dict)
    """`ml`: matched laying (backer's) stake by matched odds. NO contracts."""


def apply_matches(ladder: dict[Decimal, Decimal], updates: Any) -> None:
    """`[odds, stake]` points onto a matched ladder; stake 0 removes the point."""
    for odds, stake in updates or []:
        price, size = Decimal(str(odds)), Decimal(str(stake))
        if size == 0:
            ladder.pop(price, None)
        else:
            ladder[price] = size


class BetfairUserStream(_BetfairStream):
    """The account's bets as they change, what matches on them, and the
    matched position on each runner, from the Exchange Stream API's order
    subscription. Needs the account's application key and a session, as
    every Betfair call does (it can share `BetfairTrading.session`).

    Betfair sends each changed bet in full, so every change is an
    `OrderEvent`; what newly matched on it (its matched stake going up) is
    a `FillEvent` at the odds the increase averaged, with the bet id and
    its matched stake so far as the fill's id. Betfair's matched ladders per
    runner (`mb`, `ml`) are the `PositionEvent`s. The first image after a
    connection carries only the bets still unmatched in part, and is the
    baseline the fills are measured from; a reconnect resumes from the last
    clock when Betfair can (`RESUB_DELTA`) and is reported with
    `reconcile_required`.

    ```python
    async with BetfairUserStream(session=trading.session) as stream:
        await stream.watch_orders()
        async for event in stream:
            ...
    ```
    """

    name = "user"
    private = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True, "watch_positions": True}

    def __init__(self, *, credentials: Any = None, session: Any = None, account_name: str = "default",
                 strategy_refs: list[str] | None = None, **kwargs: Any):
        """`strategy_refs` limits the bets streamed to those placed with these
        `customerStrategyRef`s (the overall position is not filtered)."""
        kwargs.setdefault("idle_timeout", max(60.0, kwargs.get("heartbeat_ms", 5000) * 3 / 1000))
        super().__init__(credentials=credentials, session=session, **kwargs)
        from ..trading.types import Account

        self.account = Account(venue=VENUE, name=account_name)
        self.strategy_refs = list(strategy_refs or [])
        self.kinds: set[str] = set()
        """What is watched: orders, fills, positions."""
        self.only: set[str] | None = None
        """Betfair market ids or runner ids passed on; `None` for every one."""
        self.orders: dict[str, Any] = {}
        """Bet id -> the `Order` as last reported, while it has an unmatched part."""
        self.matched: OrderedDict[str, tuple[Decimal, Decimal]] = OrderedDict()
        """Bet id -> (matched stake, average matched odds) as last reported,
        the most recent `BET_MEMORY` bets."""
        self.positions: dict[tuple[str, tuple[int, float]], RunnerPosition] = {}
        self._degraded = False

    # -- intents --------------------------------------------------------------

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every bet on the account, or those on these runners or Betfair
        markets. Betfair streams the whole account; the rest is dropped here."""
        await self._watch("orders", market_ids)

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """What matches on the account's bets, read off their changes."""
        await self._watch("fills", market_ids)

    async def watch_positions(self, market_ids: list[str] | None = None) -> None:
        """The matched position on each runner the account has bet on."""
        await self._watch("positions", market_ids)

    async def _watch(self, kind: str, market_ids: list[str] | None) -> None:
        if market_ids is None:
            self.only = None
        elif self.only is not None or not self.kinds:
            self.only = (self.only or set()) | {ids.qualify(VENUE, ids.native(VENUE, m)) for m in market_ids}
        new = kind not in self.kinds
        self.kinds.add(kind)
        if new and len(self.kinds) == 1:
            await self.resubscribe()

    def _wanted(self, market: str, synpath_id: str) -> bool:
        return self.only is None or synpath_id in self.only or f"{VENUE}:{market}" in self.only

    # -- subscription ---------------------------------------------------------

    def subscription(self) -> dict[str, Any]:
        """The order subscription, resuming from the last clock when there is one."""
        order_filter: dict[str, Any] = {"includeOverallPosition": True}
        if self.strategy_refs:
            order_filter["customerStrategyRefs"] = self.strategy_refs
        frame: dict[str, Any] = {
            "op": "orderSubscription", "id": self._id(), "orderFilter": order_filter,
            "segmentationEnabled": True, "heartbeatMs": self.heartbeat_ms,
        }
        if self.clk and self.initial_clk:
            frame["initialClk"], frame["clk"] = self.initial_clk, self.clk
        return frame

    async def resubscribe(self) -> None:
        if not self.authenticated or not self.kinds:
            return
        frame = self.subscription()
        self._sub_id = frame["id"]
        await self.send(frame)

    def subscribed(self) -> None:
        self.status("subscribed", "orders")

    # -- messages -------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        op = message.get("op")
        if op == "connection":
            self.connection_id = message.get("connectionId")
            return []
        if op == "status":
            return self._on_status(message)
        if op == "ocm":
            return self._on_order_changes(message)
        return []

    def _on_order_changes(self, message: dict[str, Any]) -> list[Event]:
        if message.get("initialClk"):
            self.initial_clk = str(message["initialClk"])
        if message.get("clk"):
            self.clk = str(message["clk"])
        self._health(message.get("status"))
        if message.get("ct") == "HEARTBEAT":
            return []
        image = message.get("ct") == "SUB_IMAGE"
        if image and message.get("segmentType") in (None, "SEG_START"):
            self.orders.clear()
            self.positions.clear()
        events: list[Event] = []
        for change in message.get("oc") or []:
            if isinstance(change, dict) and change.get("id"):
                events += self._on_market(change, image=image)
        return events

    def _health(self, status: Any) -> None:
        """Betfair marks a change message 503 while its order data may be
        incomplete, and drops the mark once it has caught up."""
        if status == 503 and not self._degraded:
            self._degraded = True
            self.status("error", "Betfair says the order stream is delayed (503): bets may be missing until it "
                                 "catches up", reconcile=True)
        elif status is None and self._degraded:
            self._degraded = False
            self.status("resynced", "the order stream caught up")

    def _on_market(self, change: dict[str, Any], *, image: bool) -> list[Event]:
        market = str(change["id"])
        if change.get("fullImage"):
            for key in [k for k in self.positions if k[0] == market]:
                self.positions.pop(key, None)
        events: list[Event] = []
        for runner_change in change.get("orc") or []:
            if isinstance(runner_change, dict) and runner_change.get("id") is not None:
                events += self._on_runner(market, runner_change, image=image or bool(change.get("fullImage")))
        if change.get("closed"):
            for key in [k for k in self.positions if k[0] == market]:
                self.positions.pop(key, None)
        return events

    def _on_runner(self, market: str, change: dict[str, Any], *, image: bool) -> list[Event]:
        from ..trading.betfair import order_of_current

        key = runner_key(change["id"], change.get("hc"))
        synpath_id = runner_id(market, key)
        wanted = self._wanted(market, synpath_id)
        events: list[Event] = []
        for row in change.get("uo") or []:
            if not isinstance(row, dict) or not row.get("id"):
                continue
            current = current_row(row, market=market, selection=key[0], handicap=key[1])
            order = order_of_current(current, account=self.account)
            order = order.model_copy(update={
                "created_at": int(row["pd"]) if row.get("pd") else order.created_at,
                "updated_at": int(row.get("md") or row.get("cd") or row.get("ld") or 0) or order.updated_at,
            })
            fill = self._fill(order, row, baseline=image)
            if order.status in (OrderStatus.OPEN, OrderStatus.PENDING):
                self.orders[order.id] = order
            else:
                # Its matched amount is kept: a runner removal or a void can
                # still change a finished bet, and that is not a new match.
                self.orders.pop(order.id, None)
            if not wanted:
                continue
            if fill is not None and "fills" in self.kinds:
                events.append(FillEvent(venue=VENUE, fill=fill))
            if "orders" in self.kinds:
                if fill is not None:
                    order = order.model_copy(update={"last_fill_price": fill.price, "last_fill_quantity": fill.quantity})
                events.append(OrderEvent(venue=VENUE, order=order, native=str(row.get("status") or "")))
        if "mb" in change or "ml" in change or change.get("fullImage"):
            position = self.positions.setdefault((market, key), RunnerPosition())
            if change.get("fullImage"):
                position.backs.clear()
                position.lays.clear()
            apply_matches(position.backs, change.get("mb"))
            apply_matches(position.lays, change.get("ml"))
            if wanted and "positions" in self.kinds:
                events.append(PositionEvent(venue=VENUE, position=self._position(synpath_id, position)))
        return events

    def _fill(self, order: Any, row: dict[str, Any], *, baseline: bool) -> Any:
        """What newly matched on a bet since it was last seen, or `None`. A bet
        first seen in an image is the baseline; one first seen in an update
        matched from nothing."""
        from ..trading.types import Fill, Liquidity

        matched, average = Decimal(str(row.get("sm") or 0)), Decimal(str(row.get("avp") or 0))
        before = self.matched.get(order.id)
        self.matched[order.id] = (matched, average)
        self.matched.move_to_end(order.id)
        while len(self.matched) > BET_MEMORY:
            self.matched.popitem(last=False)
        if before is None and baseline:
            return None
        old_stake, old_odds = before or (Decimal(0), Decimal(0))
        stake = matched - old_stake
        contracts = matched * average - old_stake * old_odds
        if stake <= 0 or contracts <= 0:
            return None
        return Fill(
            id=f"{order.id}:{matched.normalize()}", order_id=order.id, client_order_id=order.client_order_id,
            venue=VENUE, account=self.account, market_id=order.market_id, side=order.side,
            price=(stake / contracts).quantize(PRICE_PLACES), quantity=contracts, liquidity=Liquidity.UNKNOWN,
            timestamp=int(row.get("md") or 0) or now_ms(),
            info={"bet_id": order.id, "size_matched": str(matched), "average_odds": str(average)},
        )

    def _position(self, synpath_id: str, position: RunnerPosition) -> Any:
        from ..trading.types import Position, PositionSide

        yes = sum((odds * stake for odds, stake in position.backs.items()), Decimal(0))
        no = sum((odds * stake for odds, stake in position.lays.items()), Decimal(0))
        stake = sum(position.backs.values(), Decimal(0)) + sum(position.lays.values(), Decimal(0))
        margin = sum(position.backs.values(), Decimal(0)) + sum(
            (stake_ * (odds - 1) for odds, stake_ in position.lays.items()), Decimal(0))
        net, traded = yes - no, yes + no
        return Position(
            venue=VENUE, account=self.account, market_id=synpath_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net), inventory_yes=yes, inventory_no=no,
            entry_price=(stake / traded).quantize(PRICE_PLACES) if traded else None, margin=margin,
            timestamp=now_ms(),
            info={"matched_backs": [[str(o), str(s)] for o, s in sorted(position.backs.items())],
                  "matched_lays": [[str(o), str(s)] for o, s in sorted(position.lays.items())]},
        )
