"""Matchbook market data as a stream, polled from its public REST events.

**Not a socket.** Matchbook has no push API: no WebSocket, no SSE, no FIX. So
this stream polls `GET /edge/rest/events?ids=...` with prices and turns the
difference between one answer and the next into the same events every other
venue's stream emits. `PollSocket` gives the polling the `recv`/`send`/
`close` shape the base `Stream` drives, so its reconnects, backoff and
watchdog are unchanged: an HTTP failure is a dropped connection, and the
next poll after it is a fresh snapshot.

Polling has a budget, and this stream keeps to it:

* One request reads twenty Matchbook events with every market, runner and
  price in them, so the books, the tickers and the statuses of everything
  watched come from the same reads. Every request goes through the REST
  adapter's `LIMITER` (Matchbook's events group allows 700 a minute).
* Every GET is chargeable on Matchbook's API plans (£100 per million a
  calendar month). The default interval, 5 s, is 12 requests a minute for up
  to twenty events: about 520,000 a month, if left running. Raise `interval`
  to spend less; changes show up seconds late either way.

Books are read on YES exactly as the REST adapter reads them: `lay` prices
(resting backs) are YES bids, `back` prices (resting lays) YES asks, at 1/O,
sizes in contracts (stake times odds).

Matchbook publishes no trades, so there is no `watch_trades`.
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
from .base import BookEvent, Event, LocalBook, MarketStatusEvent, QuoteEvent, Stream, now_ms
from .betfair import changed, yes_price

log = logging.getLogger("synpath.ws.matchbook")

VENUE = "matchbook"

DEFAULT_INTERVAL = 5.0
"""Seconds between polls: 12 requests a minute for up to twenty events."""

MAX_EVENTS = 200
"""Matchbook events one stream polls; ten requests a poll at most."""


def yes_levels(prices: Any, side: str) -> list[tuple[Decimal, Decimal]]:
    """One side's price rows as YES (price, contracts) pairs."""
    out = []
    for row in prices or []:
        if not row or row.get("side") != side:
            continue
        odds, amount = Decimal(str(row.get("decimal-odds") or row.get("odds") or 0)), \
            Decimal(str(row.get("available-amount") or 0))
        if odds > 1 and amount > 0:
            out.append((yes_price(odds), amount * odds))
    return out


def state_of(event_status: str | None, market_status: str | None, runner: dict[str, Any]) -> tuple[str, str | None]:
    """A runner's stream state and result from the event's, market's and its own status."""
    if runner.get("withdrawn") or runner.get("status") == "paid":
        return "settled", "void" if runner.get("withdrawn") else None
    if "graded" in (event_status, market_status):
        return "settled", None
    if "closed" in (event_status, market_status, runner.get("status")):
        return "closed", None
    if "suspended" in (event_status, market_status, runner.get("status")):
        return "paused", None
    if market_status == "open":
        return "open", None
    return "updated", None


class PollSocket:
    """One "connection" of polls: `recv` waits out the interval (not before the
    first poll) and returns the next poll's answer as JSON text."""

    def __init__(self, stream: "MatchbookMarketStream"):
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


class MatchbookMarketStream(Stream):
    """Runner books, top of book and market status for Matchbook markets,
    polled from REST. No account needed.

    ```python
    async with MatchbookMarketStream() as stream:
        await stream.watch_order_book(["matchbook:34569494493900023-34569495835700023-34569495836701023"])
        async for event in stream:
            ...
    ```
    """

    venue = VENUE
    name = "market"
    data_heartbeat = True
    has: dict[str, Capability] = {
        # Polled: a change shows up at the next poll, not as it happens.
        "watch_order_book": "partial",
        "watch_ticker": "partial",
        "watch_trades": False,
        "watch_market_status": "partial",
    }

    def __init__(
        self, *, credentials: Any = None, session: Any = None, interval: float = DEFAULT_INTERVAL,
        depth: int | None = None, currency: str | None = None, client: httpx.AsyncClient | None = None,
        base_url: str | None = None, limiter: RateLimiter | None | str = "shared", **kwargs: Any,
    ):
        """No argument is needed. `credentials` or `session` send a session
        token on every poll, which counts the polls against the account."""
        from ..matchbook import API_URL, LIMITER, PRICE_DEPTH, session_from

        if session is None and credentials is not None:
            session = session_from(credentials)
        self.session = session
        if interval <= 0:
            raise ValueError("interval must be positive")
        self.interval = float(interval)
        self.depth = depth or PRICE_DEPTH
        self.currency = currency
        # Silence means a stuck poll only after a few intervals.
        kwargs.setdefault("idle_timeout", max(60.0, self.interval * 3 + 30))
        self.base_url = (base_url or API_URL).rstrip("/")
        super().__init__(f"{self.base_url}/edge/rest/events", **kwargs)
        self._client = client
        self._own_client = client is None
        self.limiter = LIMITER if limiter == "shared" else limiter
        self.books: dict[str, LocalBook] = {}
        """Runner's Synpath id -> its YES book."""
        self.tickers: set[str] = set()
        self.whole_markets: set[tuple[str, str]] = set()
        """(event id, market id) watched as a whole: every runner's book is kept."""
        self.statuses: set[tuple[str, str]] | None = None
        """(event id, market id) whose status changes are passed on; `None` while not watching."""
        self.wanted: dict[str, set[str]] = {}
        """Matchbook event id -> market ids polled in it."""
        self.polls = 0
        self._top: dict[str, tuple[Any, ...]] = {}
        self._last_state: dict[str, tuple[str, str | None]] = {}

    # -- connection -----------------------------------------------------------

    async def _open(self) -> Any:
        if self._client is None:
            from ..smarkets import user_agent

            self._client = httpx.AsyncClient(
                timeout=30.0, follow_redirects=True,
                headers={"User-Agent": user_agent(), "Accept": "application/json"},
            )
        return PollSocket(self)

    async def close(self) -> None:
        await super().close()
        if self._client is not None and self._own_client:
            await self._client.aclose()
            self._client = None

    def on_disconnect(self) -> None:
        for book in self.books.values():
            book.invalidate()

    async def _get(self, params: dict[str, Any]) -> Any:
        if self.limiter is not None:
            await self.limiter.acquire_async()
        from ..smarkets import user_agent

        headers = {"User-Agent": user_agent(), "Accept": "application/json"}
        if self.session is not None:
            headers["session-token"] = await asyncio.to_thread(self.session.token)
        assert self._client is not None
        response = await self._client.get(self.url, params={k: v for k, v in params.items() if v is not None},
                                          headers=headers)
        if response.status_code == 401 and self.session is not None:
            self.session.invalidate()
        if response.status_code == 429:
            raise ConnectionError("rate limited on events (429); Matchbook blocks for ten minutes past its limit")
        if response.status_code >= 400:
            raise ConnectionError(f"Matchbook answered {response.status_code} on events")
        return response.json()

    async def poll(self) -> dict[str, Any]:
        """One round of reads: every event watched, twenty a request, with
        its markets, runners and prices."""
        from ..matchbook import ALL_STATES, EVENTS_PER_REQUEST, PRICE_PARAMS, batched

        events = sorted(self.wanted)
        self.polls += 1
        if not events:
            return {"op": "idle", "at": now_ms()}
        message: dict[str, Any] = {"op": "poll", "at": now_ms(), "events": []}
        for chunk in batched(events, EVENTS_PER_REQUEST):
            body = await self._get({
                "ids": ",".join(chunk), "states": ALL_STATES, "per-page": len(chunk), **PRICE_PARAMS,
                "include-prices": "true", "price-depth": self.depth, "currency": self.currency,
            })
            message["events"] += [_trimmed(e, self.wanted.get(str(e.get("id")), set()))
                                  for e in (body or {}).get("events") or [] if isinstance(e, dict)]
        message["polled"] = events
        return message

    # -- intents --------------------------------------------------------------

    def _split(self, market_ids: list[str]) -> list[tuple[str, str, str | None]]:
        """(event id, market id, runner id or None for the whole market) per id."""
        from ..matchbook import parse_market_id, parse_runner_id

        out = []
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            if native.count("-") == 2:
                out.append(parse_runner_id(native))
            else:
                event, market = parse_market_id(native)
                out.append((event, market, None))
        return out

    def _want(self, targets: list[tuple[str, str, str | None]]) -> set[str]:
        refused = set()
        for event, market, _ in targets:
            if event not in self.wanted and len(self.wanted) >= MAX_EVENTS:
                refused.add(event)
                continue
            self.wanted.setdefault(event, set()).add(market)
        if refused:
            self.status("error", f"more than {MAX_EVENTS} events on one stream; not polled: {', '.join(sorted(refused))}")
        return refused

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Runner books at every poll. A `<event id>-<market id>` pair keeps
        every runner's book in that market."""
        targets = self._split(market_ids)
        refused = self._want(targets)
        for event, market, runner in targets:
            if event in refused:
                continue
            if runner is None:
                self.whole_markets.add((event, market))
            else:
                self.books.setdefault(_synpath_id(event, market, runner), LocalBook())

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book whenever a poll finds it changed, for runners."""
        targets = self._split(market_ids)
        refused = self._want(targets)
        self.tickers.update(_synpath_id(e, m, r) for e, m, r in targets if r is not None and e not in refused)

    async def watch_market_status(self, market_ids: list[str] | None = None) -> None:
        """Status changes (suspended, reopened, closed, a runner withdrawn) of
        each runner in these markets, at every poll."""
        targets = self._split(market_ids or [])
        refused = self._want(targets)
        self.statuses = (self.statuses or set()) | {(e, m) for e, m, _ in targets if e not in refused}

    async def unwatch(self, market_ids: list[str]) -> None:
        for event, market, runner in self._split(market_ids):
            prefix = f"{VENUE}:{event}-{market}-"
            if runner is not None:
                synpath_id = _synpath_id(event, market, runner)
                self.books.pop(synpath_id, None)
                self.tickers.discard(synpath_id)
                self._top.pop(synpath_id, None)
            else:
                self.whole_markets.discard((event, market))
                for synpath_id in [s for s in (*self.books, *self.tickers) if s.startswith(prefix)]:
                    self.books.pop(synpath_id, None)
                    self.tickers.discard(synpath_id)
                    self._top.pop(synpath_id, None)
                if self.statuses:
                    self.statuses.discard((event, market))
            still = any(s.startswith(prefix) for s in (*self.books, *self.tickers)) \
                or (event, market) in self.whole_markets or (event, market) in (self.statuses or set())
            if not still and event in self.wanted:
                self.wanted[event].discard(market)
                if not self.wanted[event]:
                    self.wanted.pop(event)

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(ids.qualify(VENUE, ids.native(VENUE, market_id)))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    # -- messages -------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict) or message.get("op") != "poll":
            return []
        events: list[Event] = []
        seen: set[str] = set()
        for event in message.get("events") or []:
            event_id = str(event.get("id"))
            for market in event.get("markets") or []:
                market_id = str(market.get("id"))
                whole = (event_id, market_id) in self.whole_markets
                watched_status = self.statuses is not None and (not self.statuses or (event_id, market_id) in self.statuses)
                for runner in market.get("runners") or []:
                    synpath_id = _synpath_id(event_id, market_id, str(runner.get("id")))
                    if whole:
                        self.books.setdefault(synpath_id, LocalBook())
                    if synpath_id in self.books or synpath_id in self.tickers:
                        seen.add(synpath_id)
                        events += self._book_events(synpath_id, runner.get("prices") or [], message.get("at"))
                    if watched_status:
                        events += self._status_change(synpath_id, event.get("status"), market.get("status"), runner)
        # A runner a poll no longer lists (its market closed) has an empty book.
        polled = set(message.get("polled") or [])
        for synpath_id in sorted(set(self.books) | self.tickers):
            if synpath_id not in seen and synpath_id.split(":", 1)[1].split("-")[0] in polled:
                events += self._book_events(synpath_id, [], message.get("at"))
        return events

    def _book_events(self, synpath_id: str, prices: list[dict[str, Any]], at: Any) -> list[Event]:
        bids, asks = yes_levels(prices, "lay"), yes_levels(prices, "back")
        currency = next((row.get("currency") for row in prices if row.get("currency")), self.currency)
        info = {"polled_at": at, "currency": currency}
        events: list[Event] = []
        book = self.books.get(synpath_id)
        if book is not None:
            snapshot = not book.ready
            recovering = snapshot and book.timestamp is not None
            old_bids, old_asks = book.levels()
            book.replace(bids, asks)
            book.timestamp = now_ms()
            new_bids, new_asks = book.levels()
            if snapshot:
                events.append(BookEvent(
                    venue=VENUE, market_id=synpath_id, side="yes", kind="snapshot", bids=new_bids, asks=new_asks,
                    best_bid=book.best_bid, best_ask=book.best_ask, info=info,
                ))
                if recovering:
                    self.status("resynced", f"book {synpath_id}", key=synpath_id)
            else:
                bid_changes, ask_changes = changed(old_bids, new_bids), changed(old_asks, new_asks)
                if bid_changes or ask_changes:
                    events.append(BookEvent(
                        venue=VENUE, market_id=synpath_id, side="yes", kind="delta", bids=bid_changes,
                        asks=ask_changes, best_bid=book.best_bid, best_ask=book.best_ask, info=info,
                    ))
        if synpath_id in self.tickers:
            best_bid = max(bids, default=None, key=lambda level: level[0])
            best_ask = min(asks, default=None, key=lambda level: level[0])
            top = (best_bid, best_ask)
            if self._top.get(synpath_id) != top:
                self._top[synpath_id] = top
                events.append(QuoteEvent(
                    venue=VENUE, market_id=synpath_id,
                    bid=best_bid[0] if best_bid else None, bid_size=best_bid[1] if best_bid else None,
                    ask=best_ask[0] if best_ask else None, ask_size=best_ask[1] if best_ask else None,
                    info=info,
                ))
        return events

    def _status_change(self, synpath_id: str, event_status: Any, market_status: Any, runner: dict[str, Any]) -> list[Event]:
        state, result = state_of(event_status, market_status, runner)
        if self._last_state.get(synpath_id) == (state, result):
            return []
        self._last_state[synpath_id] = (state, result)
        return [MarketStatusEvent(
            venue=VENUE, market_id=synpath_id, state=state,  # type: ignore[arg-type]
            native=f"{market_status}/{runner.get('status')}", result=result,
            info={"event_status": event_status, "withdrawn": bool(runner.get("withdrawn"))},
        )]


def _trimmed(event: dict[str, Any], markets: set[str]) -> dict[str, Any]:
    """An event with only the markets watched in it, so a poll's message stays small."""
    return {**event, "markets": [m for m in event.get("markets") or [] if str(m.get("id")) in markets]}


def _synpath_id(event: str, market: str, runner: str) -> str:
    return ids.qualify(VENUE, f"{event}-{market}-{runner}")
