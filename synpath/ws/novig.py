"""Novig's websocket: books, the tape, market status and the account's own
orders and positions; and, without a key, the public book polled.

**One connection, signed.** `wss://api.novig.com/v3/ws` (Paper:
`wss://api.paper.novig.com/v3/ws`) takes a subaccount's `trading` or
`trading::read` key: the upgrade request is signed like any route (NOVIG-V3,
`GET /v3/ws`). Requests are JSON verbs with increasing nonces; `subscribe`
sets one channel per market (`book`, `bbo`, `trades` or `lifecycle`, the
first three including `lifecycle`) and answers with a snapshot. Deltas carry
a `seq` per market and channel; a skipped one is a gap, recovered with a
`snapshot` request that leaves the subscriptions as they are.

**Books.** Each outcome's ladder is a list of resting orders. Read on YES
as the REST book is: orders on the YES outcome are bids, orders on the NO
outcome asks at one minus their price, sizes in dollar contracts (the
venue's `qty` over 100). The snapshot carries the top twenty price levels
per outcome; deltas (`add`, `update`, `remove`) apply per order.

**One channel per market.** A market watched for its book and its trades
is subscribed on `book`, and its trades are read off the book's fills (a
resting order's quantity going down for a fill), without the tape's ids.

**Without a key**, `NovigMarketStream` polls the public book route instead
(`GET /v3/public/catalog/markets/{id}/book`), and the tape and the market
for trades and status, turning the difference between one answer and the
next into the same events. The edge caches those answers for up to fifteen
seconds, so a poll sees changes that late.

**The account** (`NovigUserStream`): the `orders` and `positions` channels
of the key's subaccount, a snapshot then events (`open`, `fill`, `cancel`,
`reject`), and a heartbeat every 15 s that names the last seq of each so a
dropped message is noticed. The `fill` event carries no fee; read it from
`NovigTrading.fetch_my_trades`.

Message shapes follow Novig's documentation and OpenAPI document.
"""
from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from typing import Any

import httpx

from .. import ids
from ..base import Capability, RateLimiter
from ..errors import AuthenticationError
from ..trading.types import Account, OrderStatus, Side
from .base import (
    BookEvent, BookLevel, Event, FillEvent, LocalBook, MarketStatusEvent, OrderEvent, PositionEvent, QuoteEvent,
    Stream, TradeEvent, now_ms,
)
from .betfair import changed  # shared: the levels that differ between two book sides

log = logging.getLogger("synpath.ws.novig")

VENUE = "novig"
WS_URLS = {"production": "wss://api.novig.com/v3/ws", "paper": "wss://api.paper.novig.com/v3/ws"}
WS_PATH = "/v3/ws"
SCALE = Decimal(100)
ONE = Decimal("1")

DEFAULT_INTERVAL = 5.0
"""Seconds between polls without a key."""

MAX_POLLED = 200
"""Markets one polled stream reads: a book request each per poll."""

SUBSCRIBE_CHUNK = 16
"""Markets one subscribe names. A `book` subscription weighs 16 tokens a
market against a `stream` bucket of 512 that refills at 32 a second."""

STREAM_RATE = 32.0
WEIGHT = {"lifecycle": 1, "trades": 4, "bbo": 8, "book": 16}

MARKET_STATES = {"OPEN": "open", "CLOSED": "closed", "SETTLED": "settled"}
"""A market's status as a stream state."""

LIFECYCLE_STATES = {"OPEN": "open", "CLOSE": "closed", "GRADE": "settled"}
"""What each lifecycle transition makes of a market; the others (`START`,
`END`, `GOLIVE`, `UNLIVE`) leave its status and are passed on as `updated`."""


def market_of(market_id: str) -> str:
    return ids.native(VENUE, market_id)


def contracts(qty: Any) -> Decimal:
    return Decimal(int(qty or 0)) / SCALE


def yes_level(price: Any, *, on_yes: bool) -> tuple[str, Decimal]:
    """An outcome's resting price as a YES book side and price."""
    value = Decimal(str(price))
    return ("bid", value) if on_yes else ("ask", ONE - value)


def trade_event(row: dict[str, Any], *, yes_outcome: str, market: str, source: str = "tape") -> TradeEvent | None:
    """One print, in the YES price with the taker's side: the tape names the
    resting order's outcome and price, so a print on YES is a taker selling
    YES, a print on NO a taker buying it."""
    try:
        paid = Decimal(str(row["price"]))
        quantity = contracts(row["qty"])
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None
    on_yes = str(row.get("outcomeId")) == yes_outcome
    return TradeEvent(
        venue=VENUE, market_id=ids.qualify(VENUE, market), id=row.get("tradeId"),
        price=paid if on_yes else ONE - paid, quantity=quantity, taker_side=Side.SELL if on_yes else Side.BUY,
        timestamp=row.get("ts"), info={**row, "source": source},
    )


def lifecycle_kind(delta: Any) -> tuple[str, str | None]:
    """(transition, status after) from a lifecycle delta: an object
    `{kind, status}`, or the bare transition the documentation shows."""
    if isinstance(delta, dict):
        return str(delta.get("kind") or ""), delta.get("status")
    return str(delta), None


class _Signed(Stream):
    """The connection both streams share: the signed upgrade and the nonces."""

    venue = VENUE

    def __init__(self, *, credentials: Any = None, env: str | None = None, url: str | None = None,
                 signer: Any = None, **kwargs: Any):
        env = env or getattr(credentials, "env", None) or "production"
        if env not in WS_URLS:
            raise ValueError(f"{VENUE}: env must be production or paper, got {env!r}")
        self.env = env
        if signer is None and getattr(credentials, "key_id", None) and getattr(credentials, "private_key_pem", None):
            from ..trading.novig_signing import NovigSigner

            signer = NovigSigner(credentials.key_id, credentials.private_key_pem)
        self.signer = signer
        """The subaccount's trading key, which signs the upgrade; `None` without one."""
        super().__init__(url or WS_URLS[env], **kwargs)
        self._nonce = 0

    def headers(self) -> dict[str, str]:
        if self.signer is None:
            return {}
        return self.signer.headers("GET", WS_PATH)

    def nonce(self) -> int:
        self._nonce += 1
        return self._nonce

    def on_disconnect(self) -> None:
        # Nonces only have to grow within a connection.
        self._nonce = 0

    def _later(self, coroutine: Any) -> asyncio.Task | None:
        try:
            return asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:
            coroutine.close()
            return None

    def _error(self, message: dict[str, Any]) -> list[Event]:
        code, text = message.get("code"), message.get("message")
        self.status("error", f"venue refused request {message.get('nonce')}: {code} {text}".strip())
        return []


class Throttled(Exception):
    """The edge answered 429 to one polled read."""


class PollSocket:
    """One "connection" of polls: `recv` waits out the interval (not before the
    first poll) and returns the next poll's answer as JSON text."""

    def __init__(self, stream: "NovigMarketStream"):
        self.stream = stream
        self._first = True
        self._closed = False

    async def recv(self) -> str:
        if not self._first:
            await asyncio.sleep(self.stream.interval)
        self._first = False
        if self._closed:
            raise ConnectionError("closed")
        return json.dumps(await self.stream.poll())

    async def send(self, text: str) -> None:
        """Subscriptions are local: what to poll is read from the stream."""

    async def close(self) -> None:
        self._closed = True


class NovigMarketStream(_Signed):
    """Books, top of book, the tape and market status for Novig markets.

    With a trading key (`credentials`, a `NovigCredentials`) this is the
    venue's websocket; without one it polls the public REST book every
    `interval` seconds.

    ```python
    async with NovigMarketStream(credentials=creds) as stream:
        await stream.watch_order_book(["novig:01a1577c-d666-7e81-852e-68bed1446801"])
        async for event in stream:
            ...
    ```
    """

    name = "market"
    has: dict[str, Capability] = {
        "watch_order_book": True,
        "watch_ticker": True,
        "watch_trades": True,
        "watch_market_status": True,
    }

    def __init__(
        self, *, credentials: Any = None, catalog: Any = None, interval: float = DEFAULT_INTERVAL,
        status_every: int = 6, client: httpx.AsyncClient | None = None, limiter: RateLimiter | None | str = "shared",
        api_url: str | None = None, **kwargs: Any,
    ):
        super().__init__(credentials=credentials, **kwargs)
        self.polling = self.signer is None
        """No trading key: the public book is polled instead of the websocket."""
        if self.polling:
            # Polled: a change shows up at the next poll, not as it happens.
            self.has = {**type(self).has, "watch_order_book": "partial", "watch_ticker": "partial",
                        "watch_trades": "partial", "watch_market_status": "partial"}
            self.idle_timeout = max(60.0, interval * 3 + 30)
        if interval <= 0:
            raise ValueError("interval must be positive")
        self.interval = float(interval)
        self.status_every = max(1, int(status_every))
        self.catalog = catalog
        """A `Novig` read adapter: which outcome of each market is YES. Made when first needed."""
        self.credentials = credentials
        self._client = client
        self._own_client = client is None
        self.api_url = api_url
        from ..novig import LIMITER

        self.limiter = LIMITER if limiter == "shared" else limiter
        self.books: dict[str, LocalBook] = {}
        """Market id -> its YES book."""
        self.tickers: set[str] = set()
        self.trades: set[str] = set()
        self.statuses: set[str] = set()
        self.sides: dict[str, tuple[str, str]] = {}
        """Market id -> (YES outcome, NO outcome)."""
        self.orders: dict[str, dict[str, tuple[str, Decimal, int]]] = {}
        """Market id -> order id -> (outcome, price, qty): the resting orders a book is summed from."""
        self.seqs: dict[tuple[str, str], int] = {}
        self.channels: dict[str, str] = {}
        """Market id -> the channel subscribed on this connection."""
        self.polls = 0
        self._top: dict[str, tuple[Any, ...]] = {}
        self._seen_trades: dict[str, set[str]] = {}
        self._last_status: dict[str, str] = {}
        self._bbo_state: dict[str, dict[str, Any]] = {}
        """Market id -> outcome id -> its best bid, from the `bbo` channel."""

    # -- outcomes -------------------------------------------------------------

    def _catalog(self) -> Any:
        if self.catalog is None:
            from ..novig import Novig

            self.catalog = Novig(env=self.env, api_url=self.api_url, credentials=self.credentials)
        return self.catalog

    async def _resolve(self, markets: list[str]) -> list[str]:
        """Learn each market's YES and NO outcomes; markets the venue does not
        know are reported and left out."""
        known = []
        for market in markets:
            if market not in self.sides:
                try:
                    self.sides[market] = await asyncio.to_thread(self._catalog().outcome_ids, market)
                except Exception as exc:
                    self.status("error", f"cannot watch {market}: {type(exc).__name__}: {exc}", key=market)
                    continue
            known.append(market)
        return known

    # -- intents --------------------------------------------------------------

    def channel_for(self, market: str) -> str | None:
        """The one channel a market needs: `book` serves books, tickers and
        (read off its fills) trades; otherwise the lightest that serves."""
        if market in self.books:
            return "book"
        if market in self.trades:
            return "trades"
        if market in self.tickers:
            return "bbo"
        if market in self.statuses:
            return "lifecycle"
        return None

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Each market's book, kept per order and read on YES."""
        markets = await self._resolve([market_of(m) for m in market_ids])
        for market in markets:
            self.books.setdefault(market, LocalBook())
        await self._subscribe(markets)

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book whenever it changes."""
        markets = await self._resolve([market_of(m) for m in market_ids])
        self.tickers.update(markets)
        await self._subscribe(markets)

    async def watch_trades(self, market_ids: list[str] | None = None) -> None:
        """Every execution on these markets."""
        markets = await self._resolve([market_of(m) for m in market_ids or []])
        self.trades.update(markets)
        await self._subscribe(markets)

    async def watch_market_status(self, market_ids: list[str] | None = None) -> None:
        """Each market's transitions: opened, closed, graded, and the event
        starting, going in play (every resting order voided) or leaving it."""
        markets = await self._resolve([market_of(m) for m in market_ids or []])
        self.statuses.update(markets)
        await self._subscribe(markets)

    async def unwatch(self, market_ids: list[str]) -> None:
        dropped = []
        for market_id in market_ids:
            market = market_of(market_id)
            self.books.pop(market, None)
            self.tickers.discard(market)
            self.trades.discard(market)
            self.statuses.discard(market)
            self.orders.pop(market, None)
            self._top.pop(market, None)
            if market in self.channels:
                self.channels.pop(market)
                dropped.append(f"market:{market}")
        if dropped and not self.polling:
            await self.send({"nonce": self.nonce(), "unsubscribe": dropped})

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(market_of(market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    def watched(self) -> list[str]:
        return sorted(set(self.books) | self.tickers | self.trades | self.statuses)

    # -- connection -----------------------------------------------------------

    async def _open(self) -> Any:
        if not self.polling:
            return await super()._open()
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True,
                                             headers={"Accept": "application/json"})
        return PollSocket(self)

    async def close(self) -> None:
        await super().close()
        if self._client is not None and self._own_client:
            await self._client.aclose()
            self._client = None

    async def on_connect(self) -> None:
        self.channels.clear()
        await self._subscribe(self.watched())

    def on_disconnect(self) -> None:
        super().on_disconnect()
        self.channels.clear()
        self.seqs.clear()
        for book in self.books.values():
            book.invalidate()

    async def _subscribe(self, markets: list[str]) -> None:
        """Set each market's channel where it changed, a few markets a
        request, paced under the stream throttle."""
        if self.polling:
            if len(self.watched()) > MAX_POLLED:
                self.status("error", f"more than {MAX_POLLED} markets on one polled stream; the newest are not polled")
            return
        changes = {}
        for market in markets:
            channel = self.channel_for(market)
            if channel and self.channels.get(market) != channel:
                changes[market] = channel
        items = sorted(changes.items())
        for start in range(0, len(items), SUBSCRIBE_CHUNK):
            chunk = dict(items[start:start + SUBSCRIBE_CHUNK])
            sent = await self.send({"nonce": self.nonce(), "subscribe": {"markets": chunk}})
            if not sent:
                return
            self.channels.update(chunk)
            for market in chunk:
                book = self.books.get(market)
                if book is not None:
                    book.invalidate()
            if start + SUBSCRIBE_CHUNK < len(items):
                await asyncio.sleep(sum(WEIGHT[c] for c in chunk.values()) / STREAM_RATE)

    # -- polling --------------------------------------------------------------

    async def _get(self, path: str, params: Any = None) -> Any:
        if self.limiter is not None:
            await self.limiter.acquire_async()
        from ..novig import API_URLS

        base = (self.api_url or API_URLS[self.env]).rstrip("/")
        assert self._client is not None
        response = await self._client.get(f"{base}/v3/public/catalog{path}", params=params)
        if response.status_code == 429:
            raise Throttled(path)
        if response.status_code >= 400:
            raise ConnectionError(f"Novig answered {response.status_code} on {path}")
        return response.json()

    async def poll(self) -> dict[str, Any]:
        """One round of reads: the book of every market watched for its book or
        ticker, the tape of those watched for trades, and every
        `status_every` polls the markets watched for status."""
        markets = self.watched()[:MAX_POLLED]
        self.polls += 1
        if not markets:
            return {"op": "idle", "at": now_ms()}
        message: dict[str, Any] = {"op": "poll", "at": now_ms(), "books": {}, "trades": {}, "markets": {},
                                   "throttled": []}

        async def read(kind: str, market: str, path: str, params: Any = None) -> None:
            # A throttled read keeps what the last poll saw; the next one retries.
            try:
                body = await self._get(path, params)
            except Throttled:
                message["throttled"].append(path)
                return
            message[kind][market] = (body or {}).get("items") or [] if kind == "trades" else body

        for market in markets:
            if market in self.books or market in self.tickers:
                await read("books", market, f"/markets/{market}/book")
            if market in self.trades:
                await read("trades", market, f"/markets/{market}/trades", {"limit": 100})
        if (self.polls - 1) % self.status_every == 0:
            for market in sorted(self.statuses):
                await read("markets", market, f"/markets/{market}")
        return message

    def _polled(self, message: dict[str, Any]) -> list[Event]:
        events: list[Event] = []
        info = {"polled_at": message.get("at")}
        if message.get("throttled"):
            log.debug("synpath.ws novig: throttled at the edge, kept the last answer for %s", message["throttled"])
        for market, body in (message.get("books") or {}).items():
            sides = self.sides.get(market)
            if not sides or not isinstance(body, dict):
                continue
            bids: dict[Decimal, Decimal] = {}
            asks: dict[Decimal, Decimal] = {}
            for outcome, rows in (body.get("orders") or {}).items():
                for row in rows or []:
                    side, price = yes_level(row["price"], on_yes=outcome == sides[0])
                    levels = bids if side == "bid" else asks
                    levels[price] = levels.get(price, Decimal(0)) + contracts(row.get("qty"))
            events += self._book_replaced(market, list(bids.items()), list(asks.items()),
                                          {**info, "seq": body.get("seq")})
        for market, rows in (message.get("trades") or {}).items():
            sides = self.sides.get(market)
            seen = self._seen_trades.get(market)
            fresh = [r for r in rows if isinstance(r, dict) and r.get("tradeId")]
            ids_now = {str(r["tradeId"]) for r in fresh}
            if sides and seen is not None:
                for row in sorted(fresh, key=lambda r: r.get("ts") or 0):
                    if str(row["tradeId"]) not in seen:
                        event = trade_event(row, yes_outcome=sides[0], market=market)
                        if event:
                            events.append(event)
            self._seen_trades[market] = (seen or set()) | ids_now
        for market, body in (message.get("markets") or {}).items():
            if isinstance(body, dict):
                status = str(body.get("status") or "")
                if self._last_status.get(market) not in (None, status):
                    events.append(MarketStatusEvent(
                        venue=VENUE, market_id=ids.qualify(VENUE, market),
                        state=MARKET_STATES.get(status, "updated"),  # type: ignore[arg-type]
                        native=status, info=info,
                    ))
                self._last_status[market] = status
        return events

    def _book_replaced(self, market: str, bids: list[tuple[Decimal, Decimal]], asks: list[tuple[Decimal, Decimal]],
                       info: dict[str, Any], sequence: int | None = None) -> list[Event]:
        """A whole book: a snapshot when the local book was not ready, else the
        levels that changed."""
        events: list[Event] = []
        synpath_id = ids.qualify(VENUE, market)
        book = self.books.get(market)
        if book is not None:
            snapshot = not book.ready
            recovering = snapshot and book.timestamp is not None
            old_bids, old_asks = book.levels()
            book.replace(bids, asks)
            book.timestamp = now_ms()
            book.sequence = sequence
            new_bids, new_asks = book.levels()
            if snapshot:
                events.append(BookEvent(
                    venue=VENUE, market_id=synpath_id, side="yes", kind="snapshot", bids=new_bids, asks=new_asks,
                    best_bid=book.best_bid, best_ask=book.best_ask, sequence=sequence, info=info,
                ))
                if recovering:
                    self.status("resynced", f"book {synpath_id}", key=synpath_id)
            else:
                bid_changes, ask_changes = changed(old_bids, new_bids), changed(old_asks, new_asks)
                if bid_changes or ask_changes:
                    events.append(BookEvent(
                        venue=VENUE, market_id=synpath_id, side="yes", kind="delta", bids=bid_changes,
                        asks=ask_changes, best_bid=book.best_bid, best_ask=book.best_ask, sequence=sequence,
                        info=info,
                    ))
        events += self._ticker(market, max(bids, default=None, key=lambda level: level[0]),
                               min(asks, default=None, key=lambda level: level[0]), info)
        return events

    def _ticker(self, market: str, best_bid: Any, best_ask: Any, info: dict[str, Any]) -> list[Event]:
        if market not in self.tickers:
            return []
        top = (best_bid, best_ask)
        if self._top.get(market) == top:
            return []
        self._top[market] = top
        return [QuoteEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, market),
            bid=best_bid[0] if best_bid else None, bid_size=best_bid[1] if best_bid else None,
            ask=best_ask[0] if best_ask else None, ask_size=best_ask[1] if best_ask else None, info=info,
        )]

    # -- websocket messages ---------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        if message.get("op") == "poll":
            return self._polled(message)
        if message.get("code") and "snapshot" not in message and "delta" not in message:
            return self._error(message)
        events: list[Event] = []
        if isinstance(message.get("subscribed"), dict):
            markets = sorted((message["subscribed"].get("markets") or {}).keys())
            if markets:
                self.status("subscribed", ", ".join(markets))
        for market, body in (message.get("snapshot") or {}).items():
            if isinstance(body, dict):
                events += self._snapshot(str(market), body, message.get("ts"))
        for market, body in (message.get("delta") or {}).items():
            if isinstance(body, dict):
                events += self._delta(str(market), body, message.get("ts"))
        return events

    def _in_order(self, market: str, channel: str, seq: Any) -> bool:
        """Whether a delta follows the last seq of its market and channel. A
        skipped one is a gap: the book is marked not ready and a snapshot
        asked for."""
        if seq is None:
            return True
        key = (market, channel)
        last = self.seqs.get(key)
        self.seqs[key] = int(seq)
        if last is None or int(seq) == last + 1:
            return True
        if int(seq) <= last:
            self.seqs[key] = last
            return False
        self.status("gap", f"{channel} {market}: seq {last} then {seq}", key=ids.qualify(VENUE, market))
        book = self.books.get(market)
        if book is not None and channel == "book":
            book.invalidate()
        current = self.channels.get(market) or channel
        self._later(self.send({"nonce": self.nonce(), "snapshot": {"markets": {market: current}}}))
        return channel != "book"

    def _snapshot(self, market: str, body: dict[str, Any], ts: Any) -> list[Event]:
        events: list[Event] = []
        sides = self.sides.get(market)
        for channel in ("book", "bbo", "trades", "lifecycle"):
            part = body.get(channel)
            if isinstance(part, dict) and part.get("seq") is not None:
                self.seqs[(market, channel)] = int(part["seq"])
        book_part = body.get("book")
        if isinstance(book_part, dict) and sides:
            orders: dict[str, tuple[str, Decimal, int]] = {}
            for outcome, rows in (book_part.get("orders") or {}).items():
                for row in rows or []:
                    order_id = str(row.get("orderId") or row.get("order") or "")
                    if order_id and row.get("price") is not None:
                        orders[order_id] = (str(outcome), Decimal(str(row["price"])), int(row.get("qty") or 0))
            self.orders[market] = orders
            bids, asks = self._summed(market)
            events += self._book_replaced(market, bids, asks, {"ts": ts}, sequence=book_part.get("seq"))
        bbo = body.get("bbo")
        if isinstance(bbo, dict) and sides:
            events += self._bbo(market, bbo.get("bbo") or {}, ts, replace=True)
        trades = body.get("trades")
        if isinstance(trades, dict) and sides and market in self.trades:
            for batch in trades.get("trades") or []:
                for row in (batch or {}).get("deltas") or []:
                    event = trade_event(row, yes_outcome=sides[0], market=market)
                    if event:
                        events.append(event)
        lifecycle = body.get("lifecycle")
        if isinstance(lifecycle, dict) and market in self.statuses and lifecycle.get("status"):
            status = str(lifecycle["status"])
            if self._last_status.get(market) != status:
                self._last_status[market] = status
                events.append(MarketStatusEvent(
                    venue=VENUE, market_id=ids.qualify(VENUE, market),
                    state=MARKET_STATES.get(status, "updated"),  # type: ignore[arg-type]
                    native=status, timestamp=ts, info={"event_id": body.get("eventId"), "snapshot": True},
                ))
        return events

    def _summed(self, market: str) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
        yes = self.sides[market][0]
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for outcome, price, qty in self.orders.get(market, {}).values():
            side, level = yes_level(price, on_yes=outcome == yes)
            levels = bids if side == "bid" else asks
            levels[level] = levels.get(level, Decimal(0)) + contracts(qty)
        return list(bids.items()), list(asks.items())

    def _delta(self, market: str, body: dict[str, Any], ts: Any) -> list[Event]:
        events: list[Event] = []
        sides = self.sides.get(market)
        if not sides:
            return events
        part = body.get("book")
        if isinstance(part, dict) and self._in_order(market, "book", part.get("seq")):
            events += self._book_delta(market, part, ts)
        part = body.get("bbo")
        if isinstance(part, dict) and self._in_order(market, "bbo", part.get("seq")):
            events += self._bbo(market, {str(d.get("outcomeId")): d.get("bbo") for d in part.get("deltas") or []
                                         if isinstance(d, dict)}, ts, replace=False)
        part = body.get("trades")
        if isinstance(part, dict) and self._in_order(market, "trades", part.get("seq")) and market in self.trades:
            for row in part.get("deltas") or []:
                event = trade_event(row, yes_outcome=sides[0], market=market) if isinstance(row, dict) else None
                if event:
                    events.append(event)
        part = body.get("lifecycle")
        if isinstance(part, dict) and self._in_order(market, "lifecycle", part.get("seq")) and market in self.statuses:
            for delta in part.get("deltas") or []:
                kind, after = lifecycle_kind(delta)
                if after:
                    self._last_status[market] = str(after)
                events.append(MarketStatusEvent(
                    venue=VENUE, market_id=ids.qualify(VENUE, market),
                    state=LIFECYCLE_STATES.get(kind, "updated"),  # type: ignore[arg-type]
                    native=kind, timestamp=ts,
                    info={"status": after, "event_id": body.get("eventId"),
                          "voids_resting_orders": kind in ("GOLIVE", "UNLIVE")},
                ))
        return events

    def _book_delta(self, market: str, part: dict[str, Any], ts: Any) -> list[Event]:
        """Per-order changes as level changes, and the fills among them as
        trades when the market's trades are watched."""
        book = self.books.get(market)
        orders = self.orders.setdefault(market, {})
        yes = self.sides[market][0]
        touched: dict[tuple[str, Decimal], None] = {}
        filled: dict[str, tuple[str, Decimal, int]] = {}
        for delta in part.get("deltas") or []:
            if not isinstance(delta, dict):
                continue
            kind = delta.get("kind")
            order_id = str(delta.get("orderId") or delta.get("order") or "")
            if kind == "add":
                outcome = str(delta.get("outcomeId") or delta.get("outcome") or "")
                price, qty = Decimal(str(delta["price"])), int(delta.get("qty") or 0)
                if order_id in filled:
                    # A partial fill shown as a remove and an add of the rest.
                    gone = filled[order_id]
                    filled[order_id] = (gone[0], gone[1], gone[2] - qty)
                orders[order_id] = (outcome, price, qty)
                touched[yes_level(price, on_yes=outcome == yes)] = None
            elif kind == "update" and order_id in orders:
                outcome, price, qty = orders[order_id]
                remaining = int(delta.get("remaining") or 0)
                filled[order_id] = (outcome, price, filled.get(order_id, (outcome, price, 0))[2] + qty - remaining)
                orders[order_id] = (outcome, price, remaining)
                touched[yes_level(price, on_yes=outcome == yes)] = None
            elif kind == "remove" and order_id in orders:
                outcome, price, qty = orders.pop(order_id)
                if delta.get("reason") == "fill":
                    filled[order_id] = (outcome, price, filled.get(order_id, (outcome, price, 0))[2] + qty)
                touched[yes_level(price, on_yes=outcome == yes)] = None
        events: list[Event] = []
        if book is not None and book.ready:
            bids, asks = self._summed(market)
            sizes = {("bid", p): s for p, s in bids} | {("ask", p): s for p, s in asks}
            changes_bid, changes_ask = [], []
            for side, price in touched:
                size = sizes.get((side, price), Decimal(0))
                book.set(side, price, size)  # type: ignore[arg-type]
                (changes_bid if side == "bid" else changes_ask).append(BookLevel(price, size))
            book.timestamp = now_ms()
            book.sequence = part.get("seq")
            if changes_bid or changes_ask:
                events.append(BookEvent(
                    venue=VENUE, market_id=ids.qualify(VENUE, market), side="yes", kind="delta",
                    bids=tuple(changes_bid), asks=tuple(changes_ask), best_bid=book.best_bid, best_ask=book.best_ask,
                    sequence=part.get("seq"), timestamp=ts,
                ))
            best_bid = max(bids, default=None, key=lambda level: level[0])
            best_ask = min(asks, default=None, key=lambda level: level[0])
            events += self._ticker(market, best_bid, best_ask, {"ts": ts})
        if market in self.trades:
            for order_id, (outcome, price, qty) in filled.items():
                if qty > 0:
                    event = trade_event({"outcomeId": outcome, "price": str(price), "qty": qty, "ts": ts,
                                         "orderId": order_id}, yes_outcome=yes, market=market, source="book")
                    if event:
                        events.append(event)
        return events

    def _bbo(self, market: str, levels: dict[str, Any], ts: Any, *, replace: bool) -> list[Event]:
        """Each outcome's best bid as YES top of book."""
        yes, no = self.sides[market]
        state = self._bbo_state.setdefault(market, {})
        if replace:
            state.clear()
        for outcome, level in levels.items():
            if level:
                state[outcome] = level
            else:
                state.pop(outcome, None)
        bid = state.get(yes)
        ask = state.get(no)
        best_bid = (Decimal(str(bid["price"])), contracts(bid.get("qty"))) if bid else None
        best_ask = (ONE - Decimal(str(ask["price"])), contracts(ask.get("qty"))) if ask else None
        return self._ticker(market, best_bid, best_ask, {"ts": ts})


class NovigUserStream(_Signed):
    """The subaccount's orders, fills and positions, from the websocket's
    private channels. Needs the subaccount's trading (or trading-read) key.

    An order event names its order; which market and side it is on comes from
    the order itself, kept from the snapshot and each `open`, and read from
    `trading` (a `NovigTrading`) for an order first seen in a fill or a
    cancel. Events are produced in the order they arrive.
    """

    name = "user"
    private = True
    data_heartbeat = True
    has: dict[str, Capability] = {"watch_orders": True, "watch_my_trades": True, "watch_positions": True}

    def __init__(self, *, credentials: Any = None, trading: Any = None, catalog: Any = None,
                 account_name: str = "default", **kwargs: Any):
        super().__init__(credentials=credentials, **kwargs)
        if self.signer is None:
            raise AuthenticationError(
                f"{VENUE}: the account stream needs the subaccount's trading key: NOVIG_KEY_ID and "
                "NOVIG_PRIVATE_KEY_PATH"
            )
        self.credentials = credentials
        self.trading = trading
        self.catalog = catalog if catalog is not None else getattr(trading, "catalog", None)
        self.account = Account(venue=VENUE, name=account_name, subaccount=getattr(credentials, "key_id", None))
        self.channels: set[str] = set()
        self.orders: dict[str, Any] = {}
        """Order id -> the `Order` as last reported."""
        self.holdings: dict[str, dict[str, dict[str, Any]]] = {}
        """Market id -> outcome id -> position row."""
        self.seqs: dict[str, int] = {}
        self._queue_in: asyncio.Queue | None = None
        self._worker: asyncio.Task | None = None

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        """Every order on the subaccount; the venue has no per-market channel."""
        await self._add({"orders"})

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        """Every fill on the subaccount (on the orders channel)."""
        await self._add({"orders"})

    async def watch_positions(self, market_ids: list[str] | None = None) -> None:
        await self._add({"positions"})

    async def _add(self, channels: set[str]) -> None:
        new = channels - self.channels
        self.channels |= channels
        if new:
            await self.send({"nonce": self.nonce(), "subscribe": {"private": sorted(self.channels)}})

    async def on_connect(self) -> None:
        self.seqs.clear()
        if self.channels:
            await self.send({"nonce": self.nonce(), "subscribe": {"private": sorted(self.channels)}})

    async def close(self) -> None:
        if self._worker is not None:
            self._worker.cancel()
            self._worker = None
        await super().close()

    # -- messages -------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        if message.get("code") and "snapshot" not in message and "delta" not in message:
            return self._error(message)
        if isinstance(message.get("heartbeat"), dict):
            self._heartbeat(message["heartbeat"])
            return []
        if "orders" not in message and "positions" not in message:
            return []
        # Reading a market's sides or an unknown order may need the network,
        # so messages are worked through one at a time, in order.
        if self._queue_in is None:
            self._queue_in = asyncio.Queue()
        self._queue_in.put_nowait(message)
        if self._worker is None or self._worker.done():
            self._worker = self._later(self._work())
        return []

    async def _work(self) -> None:
        assert self._queue_in is not None
        while not self._queue_in.empty():
            message = self._queue_in.get_nowait()
            try:
                for event in await self.process(message):
                    self.emit(event)
            except Exception as exc:
                log.debug("synpath.ws novig: unreadable message %r", message, exc_info=True)
                self.status("error", f"unreadable message: {type(exc).__name__}: {exc}")

    def _heartbeat(self, beat: dict[str, Any]) -> None:
        for channel in ("orders", "positions"):
            seq = beat.get(channel)
            if seq is not None and channel in self.seqs and int(seq) > self.seqs[channel]:
                self._gap(channel, f"heartbeat says {seq}, last seen {self.seqs[channel]}")

    def _gap(self, channel: str, detail: str) -> None:
        self.status("gap", f"{channel}: {detail}", key=channel, reconcile=True)
        self._later(self.send({"nonce": self.nonce(), "snapshot": {"private": [channel]}}))

    def _follows(self, channel: str, seq: Any) -> bool:
        if seq is None:
            return True
        last = self.seqs.get(channel)
        self.seqs[channel] = int(seq)
        if last is not None and int(seq) <= last:
            self.seqs[channel] = last
            return False
        if last is not None and int(seq) != last + 1:
            self._gap(channel, f"seq {last} then {seq}")
        return True

    async def _yes(self, market: str) -> str:
        if self.catalog is None:
            from ..novig import Novig

            self.catalog = Novig(env=self.env, credentials=self.credentials)
        return (await asyncio.to_thread(self.catalog.outcome_ids, market))[0]

    async def process(self, message: dict[str, Any]) -> list[Event]:
        """One private message as events: a snapshot of the resting orders and
        positions, or a batch of order events and position changes."""
        from ..trading.novig import order_of_row, position_of

        events: list[Event] = []
        orders = message.get("orders")
        if isinstance(orders, dict):
            if "open" in orders:
                self.seqs["orders"] = int(orders.get("seq") or 0)
                for row in orders.get("open") or []:
                    order = order_of_row(row, yes_outcome=await self._yes(str(row["marketId"])), account=self.account)
                    self.orders[order.id] = order
                    events.append(OrderEvent(venue=VENUE, order=order, native="snapshot"))
            elif self._follows("orders", orders.get("seq")):
                # Fills matched on entry come before the order's `open` in its
                # batch: learn the order from the `open` first.
                for delta in orders.get("deltas") or []:
                    opening = isinstance(delta, dict) and delta.get("kind") == "open"
                    if opening and delta.get("orderId") not in self.orders:
                        self.orders[str(delta["orderId"])] = order_of_row(
                            {**delta, "status": "PENDING", "remaining": delta.get("qty")},
                            yes_outcome=await self._yes(str(delta["marketId"])), account=self.account)
                for delta in orders.get("deltas") or []:
                    if isinstance(delta, dict):
                        events += await self._order_event(delta, order_of_row)
        positions = message.get("positions")
        if isinstance(positions, dict):
            rows = None
            if "positions" in positions:
                self.seqs["positions"] = int(positions.get("seq") or 0)
                self.holdings.clear()
                rows = positions.get("positions") or []
            elif self._follows("positions", positions.get("seq")):
                rows = positions.get("deltas") or []
            changed_markets: list[str] = []
            for row in rows or []:
                market, outcome = str(row.get("marketId") or ""), str(row.get("outcomeId") or "")
                held = self.holdings.setdefault(market, {})
                if int(row.get("qty") or 0) == 0:
                    held.pop(outcome, None)
                else:
                    held[outcome] = row
                if market not in changed_markets:
                    changed_markets.append(market)
            for market in changed_markets:
                position = position_of(market, list(self.holdings.get(market, {}).values()),
                                       yes_outcome=await self._yes(market), account=self.account)
                events.append(PositionEvent(venue=VENUE, position=position))
                if not self.holdings.get(market):
                    self.holdings.pop(market, None)
        return events

    async def _known(self, order_id: str) -> Any:
        order = self.orders.get(order_id)
        if order is None and self.trading is not None:
            try:
                order = await self.trading.fetch_order(order_id)
            except Exception as exc:
                self.status("error", f"order {order_id} is not known to this stream: {type(exc).__name__}: {exc}",
                            reconcile=True)
                return None
            self.orders[order_id] = order
        return order

    async def _order_event(self, delta: dict[str, Any], order_of_row: Any) -> list[Event]:
        from ..trading.novig import fill_of_row

        kind = str(delta.get("kind") or "")
        order_id = str(delta.get("orderId") or "")
        if kind == "open":
            row = {**delta, "status": "OPEN"}
            order = order_of_row(row, yes_outcome=await self._yes(str(delta["marketId"])), account=self.account)
            previous = self.orders.get(order_id)
            if previous is not None and previous.filled:
                # Fills matched on entry arrive before the `open`.
                order = order.model_copy(update={
                    "quantity": previous.filled + order.remaining, "filled": previous.filled,
                })
            self.orders[order_id] = order
            return [OrderEvent(venue=VENUE, order=order, native="open")]
        order = await self._known(order_id)
        if order is None:
            if self.trading is None:
                self.status("error", f"{kind} on order {order_id}, which this stream has not seen; pass trading= "
                                     "to read it", reconcile=True)
            return []
        market = ids.native(VENUE, order.market_id)
        if kind == "fill":
            fill = fill_of_row(delta, yes_outcome=await self._yes(market), market_id=market, account=self.account)
            remaining = contracts(delta.get("remaining"))
            filled = order.filled + fill.quantity
            quantity = max(order.quantity, filled + remaining)
            status = OrderStatus.CLOSED if remaining == 0 else OrderStatus.OPEN
            order = order.model_copy(update={
                "filled": filled, "remaining": remaining, "quantity": quantity, "status": status,
                "last_fill_price": fill.price, "last_fill_quantity": fill.quantity,
            })
            self.orders[order_id] = order
            return [FillEvent(venue=VENUE, fill=fill), OrderEvent(venue=VENUE, order=order, native="fill")]
        if kind in ("cancel", "reject"):
            status = OrderStatus.CANCELED if kind == "cancel" else OrderStatus.REJECTED
            order = order.model_copy(update={"status": status, "remaining": Decimal("0"),
                                             "info": {**order.info, "reason": delta.get("reason")}})
            self.orders.pop(order_id, None)
            return [OrderEvent(venue=VENUE, order=order, native=f"{kind}:{delta.get('reason') or ''}")]
        return []
