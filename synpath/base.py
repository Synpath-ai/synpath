"""The exchange interface, plus the HTTP plumbing every adapter shares.

Deliberate split, and the whole file is arranged around it:

  * **Pure normalizers** live in each venue module as free functions taking a
    raw payload and returning a unified type. They touch no network, so they
    are testable against a recorded payload and portable to another runtime.
  * **The client** does HTTP, pacing and error mapping, and nothing else.

Adapters compose the two. Nothing in this package reaches across venues — a
cross-venue concern (matching the same question on two exchanges, routing an
order) belongs a layer up, never in an adapter.
"""
from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Literal

import httpx

from .errors import (
    AuthenticationError,
    BadRequest,
    ExchangeError,
    ExchangeNotAvailable,
    MarketNotFound,
    NetworkError,
    NotSupported,
    RateLimitExceeded,
    RequestTimeout,
)
from . import ids
from .types import BookSide, Candle, Event, FeeSchedule, Market, OrderBook, Series, Trade

Capability = Literal[True, False, "partial"]
"""What a venue can do, in the shape ccxt's `has` uses.

  True       — supported; the method's docstring says how the venue answers it
  "partial"  — supported, but the result is thinner than the type suggests;
               the method's docstring says exactly how
  False      — not available; calling raises `NotSupported`
"""

CAPABILITIES: tuple[str, ...] = (
    # -- read --
    "fetch_markets",
    "fetch_market",
    "fetch_markets_by_ids",
    "fetch_events",
    "sort",
    "fetch_order_book",
    "fetch_order_books",
    "fetch_trades",
    "fetch_ohlcv",
    "fetch_series",
    "fetch_fee_schedule",
    "search",
    "watch_order_book",
    "watch_ticker",
    "watch_trades",
    "watch_market_status",
    "match_market",
    "match_event",
    # -- trade: what the venue itself holds and answers. Anything the
    # execution engine builds on top (a stop, an iceberg, a bracket) is the
    # engine's capability, reported by the engine, never claimed here. --
    "create_order",
    "create_orders",
    "cancel_order",
    "cancel_orders",
    "cancel_all_orders",
    "edit_order",
    "fetch_order",
    "fetch_open_orders",
    "fetch_orders",
    "fetch_my_trades",
    "fetch_positions",
    "fetch_balance",
    "fetch_settlements",
    "fetch_queue_position",
    "fetch_fee_estimate",
    "rfq",
    "split_merge",
    "watch_orders",
    "watch_my_trades",
    "watch_positions",
    "watch_balance",
)
"""Every question `has` can be asked, for every venue.

This list is the single place a capability is named. `Exchange.__init_subclass__`
fills a venue's `has` from it, so a venue that says nothing about a capability
gets `False` rather than a hole. `has[key]` therefore never raises KeyError on
any venue — which matters, because the whole point of `has` is to be safe to
read *before* you know whether something is supported.

The read-only adapters answer `False` to every trading key; a trading
adapter for the same venue subclasses one and says what it adds.
"""

LEGAL_CAPABILITY_VALUES = frozenset({True, False, "partial"})

MARKET_STATUSES: tuple[str, ...] = ("open", "closed", "settled", "all")
"""The status words every venue understands, with one meaning each.

  open    — listed and trading
  closed  — trading stopped, outcome not yet final
  settled — outcome final and paid
  all     — no filter

A venue that cannot filter by one of these raises `NotSupported` for it. What
it must not do is accept the word and quietly answer a different question:
`status="settled"` returning live markets on one venue and settled ones on
another is the kind of disagreement a unified API exists to remove.
"""

MARKET_SORTS: tuple[str, ...] = ("volume", "liquidity", "newest")
"""How a page of markets can be ordered.

Polymarket orders at the venue. Kalshi and Polymarket US accept a sort
parameter and ignore it, so on those the page is ordered here after it is
read, the way ccxt and pmxt do it: `sort` then means "this page, ordered by",
not "the first page of the whole catalog ordered by". Each adapter's
`fetch_markets` docstring says which venue figure the key reads.
"""

MAX_PAGE_LIMIT = 100
"""Rows one catalog call can return, on every venue.

Uniform rather than per-venue. Polymarket's keyset endpoint hard-caps at 100 no
matter what is asked, so a higher ceiling elsewhere would mean `limit=500`
returning 500 rows on one venue and 100 on another, with nothing saying it had
been reduced. Ask for more than this and it is clamped; the cursor is how you
get the rest.
"""

MAX_RETRY_WAIT = 10.0
"""Longest this library will sleep inside one call, in seconds.

A venue's `Retry-After` is a hint, not an instruction. Honouring a large one
literally would let the venue park a caller's thread for as long as it likes --
and in the server that thread belongs to a pool shared with every other
request, so a handful of rate-limited calls would take the whole service down.
The header's real value is still on the raised `RateLimitExceeded`, so a caller
that wants to wait longer can decide that for itself.
"""


class RateLimiter:
    """Token bucket, shared by every client for one venue in the process.

    Shared on purpose. A venue counts requests per account and per IP, not per
    client object, so six loops each politely under the limit still add up to
    being over it. Making the process rather than the object the thing that
    stays inside the budget is the only version that works.
    """

    def __init__(self, rate_per_second: float, burst: int | None = None):
        self.rate = rate_per_second
        self.capacity = float(burst if burst is not None else max(1, int(rate_per_second)))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    def _take_or_wait(self) -> float:
        """Take a token if one is there, else say how long until one is.

        The bookkeeping under the lock, shared by the blocking and the async
        paths so the two cannot drift apart on how a bucket refills.
        """
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
            self._updated = now
            if self._tokens >= 1:
                self._tokens -= 1
                return 0.0
            return (1 - self._tokens) / self.rate

    def acquire(self) -> None:
        while True:
            wait = self._take_or_wait()
            if wait == 0.0:
                return
            time.sleep(wait)  # outside the lock, so waiters do not serialise on it

    async def acquire_async(self) -> None:
        """The same budget from an event loop, without blocking it.

        The trading stack is async, and a `time.sleep` inside the loop would
        stall every socket the engine holds for the length of the wait -- the
        exact moment a stop needs to fire. Same bucket, same arithmetic, an
        `await` instead of a block.
        """
        import asyncio

        while True:
            wait = self._take_or_wait()
            if wait == 0.0:
                return
            await asyncio.sleep(wait)


def venue_reason(parsed: Any, text: str) -> str:
    """The venue's own reason, in words, from the error payloads venues send:
    `{"error": {"code", "message", "details"}}` (Kalshi), `{"error": "..."}`
    (Polymarket's CLOB), or a top-level `message` / `detail`. Falls back to
    the raw text, shortened."""
    if isinstance(parsed, dict):
        error = parsed.get("error", parsed.get("errors"))
        if isinstance(error, dict):
            message = error.get("message") or error.get("msg") or error.get("code")
            details = error.get("details") or error.get("detail")
            if message:
                if isinstance(details, str) and details and details != message:
                    return f"{message} ({details})"
                return str(message)
        if isinstance(error, str) and error:
            return error
        for key in ("message", "errorMsg", "error_message", "detail", "msg", "reason"):
            value = parsed.get(key)
            if isinstance(value, str) and value:
                return value
    text = " ".join((text or "").split())
    return text[:200]


def _map_status(response: httpx.Response, label: str) -> None:
    """Turn a non-2xx response into the typed error a caller can branch on.
    The message is `<venue>: <the venue's reason>`; the raw payload and the
    status stay on the exception as `body` and `status`."""
    code = response.status_code
    if code < 400:
        return
    if code == 429:
        retry = response.headers.get("retry-after")
        raise RateLimitExceeded(
            f"{label}: rate limited",
            retry_after=float(retry) if retry and retry.replace(".", "", 1).isdigit() else None,
        )
    parsed: Any = None
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    reason = venue_reason(parsed, response.text)
    if code == 404:
        what = reason if "not found" in reason.lower() else (f"not found: {reason}" if reason else "not found")
        raise MarketNotFound(f"{label}: {what}", body=parsed, status=code)
    if code in (502, 503, 504):
        raise ExchangeNotAvailable(f"{label}: unavailable ({code})", body=parsed, status=code)
    if 500 <= code:
        raise ExchangeNotAvailable(f"{label}: server error ({code}){': ' + reason if reason else ''}", body=parsed, status=code)
    if code in (401, 403):
        raise AuthenticationError(f"{label}: credentials refused ({code}){': ' + reason if reason else ''}", body=parsed, status=code)
    if code in (400, 409, 422):
        raise BadRequest(f"{label}: {reason or f'rejected ({code})'}", body=parsed, status=code)
    raise ExchangeError(f"{label}: unexpected status {code}{': ' + reason if reason else ''}", body=parsed, status=code)


class HttpClient:
    """Paced, retrying HTTP with venue errors mapped to this library's types."""

    def __init__(
        self,
        base_url: str,
        *,
        limiter: RateLimiter | None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
        attempts: int = 4,
        venue: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.limiter = limiter
        self.attempts = attempts
        self.venue = venue
        """Named at the front of every error this client raises, so a caller
        reads `kalshi: insufficient balance`, not a class name and a path."""
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """One call, paced and retried on rate limits only.

        A 429 is retried with doubling backoff because it clears on its own. A
        404 or a 400 is raised immediately: waiting will not make the request
        valid, and quietly retrying it wastes the caller's budget.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        label = self.venue or f"{self.__class__.__name__} {method} {path}"
        delay = 0.5
        last: RateLimitExceeded | None = None
        for attempt in range(self.attempts):
            if self.limiter is not None:
                self.limiter.acquire()
            try:
                response = self._client.request(method, url, **kwargs)
            except httpx.TimeoutException as exc:
                raise RequestTimeout(f"{label}: {exc}") from exc
            except httpx.HTTPError as exc:
                raise NetworkError(f"{label}: {exc}") from exc
            try:
                _map_status(response, label)
            except RateLimitExceeded as exc:
                last = exc
                if attempt == self.attempts - 1:
                    break
                time.sleep(min(exc.retry_after or delay, MAX_RETRY_WAIT))
                delay *= 2
                continue
            return response.json()
        assert last is not None
        raise last

    def get(self, path: str, params: Any = None) -> Any:
        return self.request("GET", path, params=_clean(params))

    def post(self, path: str, json: Any = None) -> Any:
        return self.request("POST", path, json=json)

    def close(self) -> None:
        self._client.close()


class AsyncHttpClient:
    """`HttpClient` for the async trading stack: same pacing, retries and error
    mapping, on `httpx.AsyncClient`.

    The two share `_map_status` and the retry rule so a status code cannot
    mean one thing on the read path and another on the trading path. Kept
    separate rather than made one class with two personalities: a method
    that is sometimes a coroutine is the kind of interface that works until
    it does not.
    """

    def __init__(
        self,
        base_url: str,
        *,
        limiter: RateLimiter | None,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
        attempts: int = 4,
        venue: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.limiter = limiter
        self.attempts = attempts
        self.venue = venue
        """Named at the front of every error this client raises, so a caller
        reads `kalshi: insufficient balance`, not a class name and a path."""
        self._client = client or httpx.AsyncClient(
            timeout=timeout, follow_redirects=True, headers=headers,
        )

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """One call, paced and retried on rate limits only. See `HttpClient.request`."""
        import asyncio

        url = path if path.startswith("http") else f"{self.base_url}{path}"
        label = self.venue or f"{self.__class__.__name__} {method} {path}"
        delay = 0.5
        last: RateLimitExceeded | None = None
        for attempt in range(self.attempts):
            if self.limiter is not None:
                await self.limiter.acquire_async()
            try:
                response = await self._client.request(method, url, **kwargs)
            except httpx.TimeoutException as exc:
                raise RequestTimeout(f"{label}: {exc}") from exc
            except httpx.HTTPError as exc:
                raise NetworkError(f"{label}: {exc}") from exc
            try:
                _map_status(response, label)
            except RateLimitExceeded as exc:
                last = exc
                if attempt == self.attempts - 1:
                    break
                await asyncio.sleep(min(exc.retry_after or delay, MAX_RETRY_WAIT))
                delay *= 2
                continue
            if not response.content:
                return None
            return response.json()
        assert last is not None
        raise last

    async def get(self, path: str, params: Any = None, **kwargs: Any) -> Any:
        return await self.request("GET", path, params=_clean(params), **kwargs)

    async def post(self, path: str, json: Any = None, **kwargs: Any) -> Any:
        return await self.request("POST", path, json=json, **kwargs)

    async def put(self, path: str, json: Any = None, **kwargs: Any) -> Any:
        return await self.request("PUT", path, json=json, **kwargs)

    async def delete(self, path: str, **kwargs: Any) -> Any:
        return await self.request("DELETE", path, **kwargs)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


def check_status(status: str) -> str:
    """Validate a status word against the shared vocabulary.

    Raised rather than passed through, because every venue has its own status
    names and forwarding an unrecognised one means each venue decides for
    itself what it meant.
    """
    if status not in MARKET_STATUSES:
        raise BadRequest(
            f"unknown status {status!r}; expected one of {', '.join(MARKET_STATUSES)}"
        )
    return status


def check_sort(sort: str | None, *, venue: str, supported: bool) -> str | None:
    """Validate a sort key, and refuse one the venue cannot honour."""
    if sort is None:
        return None
    if sort not in MARKET_SORTS:
        raise BadRequest(
            f"unknown sort {sort!r}; expected one of {', '.join(MARKET_SORTS)}"
        )
    if not supported:
        raise NotSupported(
            f"{venue}: cannot sort a listing. Its catalog endpoint accepts a sort "
            f"parameter and ignores it, so honouring this would mean returning "
            f"unsorted rows as if they were sorted."
        )
    return sort


def page_limit(limit: int | None) -> int | None:
    """Clamp a requested page size to what every venue can actually serve."""
    return None if limit is None else max(1, min(limit, MAX_PAGE_LIMIT))


def sort_page(items: list, key: Callable[[Any], float | None]) -> list:
    """Order one page by a numeric key, largest first, rows without the figure last.

    Stable, so rows that tie keep the venue's order. A row whose figure the
    venue did not publish goes to the end rather than being read as zero,
    which would rank an unreported market below a genuinely empty one.
    """
    return sorted(items, key=lambda item: (key(item) is None, -(key(item) or 0.0)))


def _clean(params: Any) -> Any:
    """Drop `None` values so an unset optional never becomes the string "None".

    Accepts a list of pairs as well as a mapping, because a repeated parameter
    (`?id=1&id=2`) cannot be expressed as a dict and Gamma's batch lookup wants
    exactly that.
    """
    if params is None:
        return None
    if isinstance(params, dict):
        return {key: value for key, value in params.items() if value is not None}
    return [(key, value) for key, value in params if value is not None]


def complete_capabilities(cls: type) -> None:
    """Fill `cls.has` from `CAPABILITIES`, refusing typos and illegal values.

    Shared by the read adapters and the trading adapters, so both answer
    every question the same way. See `Exchange.__init_subclass__`.
    """
    declared = cls.__dict__.get("has", {})
    if not isinstance(declared, dict):
        raise TypeError(f"{cls.__name__}.has must be a dict, got {type(declared).__name__}")

    unknown = sorted(set(declared) - set(CAPABILITIES))
    if unknown:
        raise TypeError(
            f"{cls.__name__}.has declares unknown {'capabilities' if len(unknown) > 1 else 'capability'} "
            f"{unknown}. Known: {sorted(CAPABILITIES)}. "
            f"Add it to synpath.base.CAPABILITIES if it is real, or fix the spelling."
        )
    illegal = {
        key: value for key, value in declared.items()
        if value not in LEGAL_CAPABILITY_VALUES
    }
    if illegal:
        raise TypeError(
            f"{cls.__name__}.has has illegal values {illegal}; "
            f"each must be True, False or 'partial'."
        )

    # Start from the nearest ancestor that declared one, so subclassing an
    # adapter (a sandbox or demo venue) inherits rather than resets.
    inherited: dict[str, Capability] = {}
    for parent in cls.__mro__[1:]:
        parent_has = parent.__dict__.get("has")
        if isinstance(parent_has, dict) and parent_has:
            inherited = parent_has
            break
    cls.has = {
        key: declared.get(key, inherited.get(key, False)) for key in CAPABILITIES
    }


class Exchange(ABC):
    """What every venue adapter offers.

    Methods are named after ccxt's so the reflexes transfer, with the tiers
    prediction markets have and spot exchanges do not (`fetch_events`,
    `fetch_series`) added alongside.

    Check `has` before calling. A capability the venue lacks raises
    `NotSupported` rather than returning an empty list, so "this venue cannot"
    is never mistaken for "there is nothing".
    """

    id: str
    name: str
    has: dict[str, Capability] = {}
    """What this venue can do. Always complete: see `__init_subclass__`.

    A subclass declares only what differs from "no". Everything in
    `CAPABILITIES` it stays silent about is filled in as `False`.
    """
    face_value: float = 1.0
    book_model: str = "native_per_outcome"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Complete the venue's capability map, and reject a broken one.

        Three failures are made impossible here rather than left to review:

        * **A gap.** Anything in `CAPABILITIES` the subclass did not mention
          becomes `False`. A caller can read any capability on any venue.
        * **A typo.** `fetch_orderbooks` for `fetch_order_books` used to be a
          silent no-op that left the real key defaulting to `False` — a venue
          quietly advertising that it cannot do something it can. It now fails
          at import.
        * **A nonsense value.** `"yes"` or `1` instead of a real capability
          value fails at import too, so the server never serializes one.

        Import time is the right moment for all three: the venue is broken for
        every caller, so it should not be constructible at all.
        """
        super().__init_subclass__(**kwargs)
        complete_capabilities(cls)

    # -- catalog --------------------------------------------------------------

    @abstractmethod
    def fetch_markets(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        status: str = "open",
        sort: str | None = None,
    ) -> list[Market]:
        """One page of markets. `cursor` continues a previous page.

        Paging is cursor-based on both venues and offset is deliberately not
        offered: the underlying set changes between calls, and offset paging
        silently drops or repeats rows when it does.

        `sort` is one of `MARKET_SORTS` on a venue whose `has["sort"]` is true,
        and raises otherwise.
        """

    def fetch_markets_by_ids(self, ids: list[str]) -> list[Market]:
        """Many markets in one request, in the order asked for.

        Both venues answer a batch of ids in one call, which is the difference
        between one request and one per market when refreshing prices: 40
        markets measured at 0.2s batched against 11.6s one at a time. Ids the
        venue no longer serves are left out rather than raising -- a market
        closing is normal, and deciding what that means is the caller's.
        """
        raise NotSupported(f"{self.id}: fetch_markets_by_ids")

    @abstractmethod
    def fetch_events(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        status: str = "open",
    ) -> list[Event]:
        """Events with their markets nested."""

    def fetch_market(self, market_id: str) -> Market:
        """One market by id."""
        raise NotSupported(f"{self.id}: fetch_market")

    # -- market data ----------------------------------------------------------

    @abstractmethod
    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        """Resting orders on one side of a market, priced as that side sees
        them. `side="no"` is what NO costs; on a `shared_complement` venue that
        is the YES book mirrored and the result says so in `derived`."""

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, keyed by Synpath market id.

        One round trip on a venue with a batch endpoint, one request per
        market on a venue without; the docstring on each adapter says which,
        because the difference is a shared rate budget.
        """
        raise NotSupported(f"{self.id}: fetch_order_books")

    @abstractmethod
    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> list[Trade]:
        """Executions, oldest first. `since` is milliseconds since epoch."""

    def fetch_ohlcv(
        self,
        market_id: str,
        *,
        timeframe: str = "1h",
        since: int | None = None,
        until: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        """Candles in the YES price, oldest first. Read `Candle.price_source`
        before using them."""
        raise NotSupported(f"{self.id}: fetch_ohlcv")

    # -- ids ------------------------------------------------------------------

    def qualify(self, native_id: str) -> str:
        """This venue's Synpath id for a native id: `venue:native`."""
        return ids.qualify(self.id, native_id)

    def native(self, market_id: str) -> str:
        """The venue's own id from a Synpath id or a bare native id. An id
        naming another venue is refused."""
        return ids.native(self.id, market_id)

    # -- reference ------------------------------------------------------------

    def fetch_series(self, series_id: str) -> Series:
        """One series, with its fee schedule where the venue publishes one."""
        raise NotSupported(f"{self.id}: fetch_series")

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """What trading this market costs, before trading it."""
        raise NotSupported(f"{self.id}: fetch_fee_schedule")

    # -- housekeeping ---------------------------------------------------------

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.id}>"


Exchange.has = {key: False for key in CAPABILITIES}

_UNDECLARED = {
    name for name in vars(Exchange)
    if name.startswith("fetch_") and name not in CAPABILITIES
}
if _UNDECLARED:  # pragma: no cover - a development-time guard
    raise TypeError(
        f"Exchange defines {sorted(_UNDECLARED)} but CAPABILITIES does not name "
        f"them, so no venue could advertise them. Add them to CAPABILITIES."
    )


TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "6h": 21600, "1d": 86400,
}


def timeframe_seconds(timeframe: str) -> int:
    try:
        return TIMEFRAME_SECONDS[timeframe]
    except KeyError:
        raise BadRequest(
            f"unknown timeframe {timeframe!r}; expected one of {', '.join(TIMEFRAME_SECONDS)}"
        ) from None


def enough_bars(candles: list[Any], *, since: int | None, limit: int | None, strict: bool = False) -> bool:
    """Whether a forward read from `since` can stop fetching: it has `limit`
    bars. `strict` asks for one more, for bars bucketed from samples, where
    the last bar is only whole once a later one has started."""
    if since is None or not limit:
        return False
    return len(candles) > limit if strict else len(candles) >= limit


def pick_bars(candles: list[Any], *, since: int | None, limit: int | None) -> list[Any]:
    """Which `limit` bars a call returns: the first ones from `since` when it
    is given (a forward read, as ccxt does), the newest ones otherwise."""
    if not limit:
        return candles
    return candles[:limit] if since is not None else candles[-limit:]
