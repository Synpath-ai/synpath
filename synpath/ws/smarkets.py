"""Smarkets market data as a stream, polled from its public REST quotes.

**Not a socket.** Smarkets' streaming API (the protobuf feed on
`api.smarkets.com:3701` that `smk_python_sdk` speaks) is offered to approved
API users and market makers, not to anonymous readers or an ordinary account
session, and Smarkets has no public WebSocket. So this stream polls
`GET /v3/markets/{ids}/quotes/` and turns the difference between one answer
and the next into the same events every other venue's stream emits.
`PollSocket` gives the polling the `recv`/`send`/`close` shape the base
`Stream` drives, so its reconnects, backoff and watchdog are unchanged: an
HTTP failure is a dropped connection, and the next poll after it is a fresh
snapshot.

Polling has a budget, and this stream keeps to it:

* `/quotes/` allows 20 requests per 60 s anonymously (50 for API users) and
  takes 200 Smarkets markets per request. Every quotes request here goes
  through `QUOTES_LIMITER`, the one the REST adapter uses, so a stream and an
  adapter in one process share it. The default interval, 5 s, is 12 requests
  a minute for up to 200 markets.
* Status (`/v3/markets/{ids}/` and their contracts) is read every
  `status_every` polls through the general `LIMITER`, and only while
  `watch_market_status` is on.

Books are read on YES exactly as the REST adapter reads them: `bids` (resting
backs) are YES bids, `offers` (resting lays) YES asks, at price/10,000, sizes
in contracts (pot/10,000 GBP). Without a login the quotes are delayed;
every event says so in `info["delayed"]`.

Smarkets' recent-trades tape is short and unpaged, so there is no
`watch_trades`; read it with `Smarkets.fetch_trades`.
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

from .betfair import changed  # shared: the levels that differ between two book sides

log = logging.getLogger("synpath.ws.smarkets")

VENUE = "smarkets"
SCALE = Decimal(10_000)

DEFAULT_INTERVAL = 5.0
"""Seconds between polls: 12 quotes requests a minute, inside the anonymous 20."""

MAX_MARKETS = 1000
"""Smarkets markets one stream polls; five quotes requests a poll at most."""


def yes_levels(rows: Any) -> list[tuple[Decimal, Decimal]]:
    """Smarkets [price bps, quantity] rows as YES (price, contracts) pairs."""
    out = []
    for row in rows or []:
        bps, quantity = int((row or {}).get("price") or 0), int((row or {}).get("quantity") or 0)
        if 0 < bps < 10_000 and quantity > 0:
            out.append((Decimal(bps) / SCALE, Decimal(quantity) / SCALE))
    return out


def state_of(market_state: str | None, contract_state: str | None) -> tuple[str, str | None]:
    """A contract's stream state and result from its market's and its own state."""
    from ..smarkets import result_of

    result = result_of(market_state, contract_state)
    if result is not None or market_state == "settled":
        return "settled", result
    if market_state == "new" or contract_state == "new":
        return "created", None
    if market_state == "halted" or contract_state in ("halted", "unavailable") or market_state == "unavailable":
        return "paused", None
    if market_state in ("open", "live"):
        return "open", None
    return "updated", None


class PollSocket:
    """One "connection" of polls: `recv` waits out the interval (not before the
    first poll) and returns the next poll's answer as JSON text."""

    def __init__(self, stream: "SmarketsMarketStream"):
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


class SmarketsMarketStream(Stream):
    """Contract books, top of book and market status for Smarkets markets,
    polled from REST quotes. No account needed; `credentials` (else
    `SMARKETS_*` from the environment) log in for undelayed quotes.

    ```python
    async with SmarketsMarketStream() as stream:
        await stream.watch_order_book(["smarkets:164973231-456060183"])
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
        self, *, credentials: Any = None, session: Any = None, login: bool | None = None,
        interval: float = DEFAULT_INTERVAL, status_every: int = 6, client: httpx.AsyncClient | None = None,
        base_url: str | None = None, quotes_limiter: RateLimiter | None | str = "shared",
        limiter: RateLimiter | None | str = "shared", **kwargs: Any,
    ):
        from ..smarkets import API_URL, LIMITER, QUOTES_LIMITER, session_from

        if session is None and login is not False:
            session = session_from(credentials)
        self.session = session
        if interval <= 0:
            raise ValueError("interval must be positive")
        self.interval = float(interval)
        self.status_every = max(1, int(status_every))
        # Silence means a stuck poll only after a few intervals.
        kwargs.setdefault("idle_timeout", max(60.0, self.interval * 3 + 30))
        self.base_url = (base_url or API_URL).rstrip("/")
        super().__init__(f"{self.base_url}/v3/markets/{{ids}}/quotes/", **kwargs)
        self._client = client
        self._own_client = client is None
        self.quotes_limiter = QUOTES_LIMITER if quotes_limiter == "shared" else quotes_limiter
        self.limiter = LIMITER if limiter == "shared" else limiter
        self.books: dict[str, LocalBook] = {}
        """Contract's Synpath id -> its YES book."""
        self.tickers: set[str] = set()
        self.whole_markets: set[str] = set()
        """Smarkets markets watched as a whole: every contract's book is kept."""
        self.statuses: set[str] | None = None
        """Smarkets market ids whose status changes are passed on; `None` while not watching."""
        self.wanted: set[str] = set()
        """Smarkets market ids polled."""
        self.contract_market: dict[str, str] = {}
        """Contract id -> Smarkets market id, for markets watched whole."""
        self.polls = 0
        self._top: dict[str, tuple[Any, ...]] = {}
        self._last_state: dict[str, tuple[str, str | None]] = {}

    @property
    def delayed(self) -> bool:
        return self.session is None

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

    async def _get(self, path: str, *, quotes: bool = False, params: Any = None) -> Any:
        limiter = self.quotes_limiter if quotes else self.limiter
        if limiter is not None:
            await limiter.acquire_async()
        from ..smarkets import user_agent

        # On every request, so a client passed in gets it too: Smarkets'
        # Cloudflare front refuses the HTTP libraries' default agents.
        headers = {"User-Agent": user_agent(), "Accept": "application/json"}
        if self.session is not None:
            token = await asyncio.to_thread(self.session.token)
            headers["Authorization"] = f"Session-Token {token}"
        assert self._client is not None
        response = await self._client.get(f"{self.base_url}{path}", params=params, headers=headers)
        if response.status_code == 401 and self.session is not None:
            self.session.invalidate()
        if response.status_code == 429:
            raise ConnectionError(f"rate limited on {path.split('/')[2]} (429)")
        if response.status_code >= 400:
            raise ConnectionError(f"Smarkets answered {response.status_code} on {path}")
        return response.json()

    async def poll(self) -> dict[str, Any]:
        """One round of reads: quotes for every market wanted, and every
        `status_every` polls the markets' and contracts' states."""
        from ..smarkets import (
            CONTRACT_MARKETS_PER_REQUEST, MARKETS_PER_REQUEST, QUOTE_MARKETS_PER_REQUEST, batched,
        )

        markets = sorted(self.wanted)
        self.polls += 1
        if not markets:
            return {"op": "idle", "at": now_ms()}
        message: dict[str, Any] = {"op": "poll", "at": now_ms(), "markets": markets, "quotes": {}}
        unresolved = [m for m in self.whole_markets if m not in set(self.contract_market.values())]
        read_status = self.statuses is not None and (self.polls - 1) % self.status_every == 0
        status_markets = sorted(self.statuses or self.wanted) if read_status else []
        contract_markets = sorted(set(unresolved) | set(status_markets))
        if contract_markets:
            contracts: list[dict[str, Any]] = []
            for chunk in batched(contract_markets, CONTRACT_MARKETS_PER_REQUEST):
                body = await self._get(f"/v3/markets/{','.join(chunk)}/contracts/", params={"include_hidden": "true"})
                contracts += (body or {}).get("contracts") or []
            message["contracts"] = contracts
        if status_markets:
            rows: list[dict[str, Any]] = []
            for chunk in batched(status_markets, MARKETS_PER_REQUEST):
                body = await self._get(f"/v3/markets/{','.join(chunk)}/", params={"include_hidden": "true"})
                rows += (body or {}).get("markets") or []
            message["market_states"] = {str(m["id"]): m.get("state") for m in rows if m.get("id")}
        for chunk in batched(markets, QUOTE_MARKETS_PER_REQUEST):
            message["quotes"].update(await self._get(f"/v3/markets/{','.join(chunk)}/quotes/", quotes=True) or {})
        return message

    # -- intents --------------------------------------------------------------

    def _split(self, market_ids: list[str]) -> list[tuple[str, str | None]]:
        """(Smarkets market id, contract id or None for the whole market) per id."""
        from ..smarkets import market_of, parse_contract_id

        out = []
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            out.append(parse_contract_id(native) if "-" in native else (market_of(native), None))
        return out

    def _want(self, markets: list[str]) -> set[str]:
        refused = set()
        for market in markets:
            if market in self.wanted:
                continue
            if len(self.wanted) >= MAX_MARKETS:
                refused.add(market)
                continue
            self.wanted.add(market)
        if refused:
            self.status("error", f"more than {MAX_MARKETS} markets on one stream; not polled: {', '.join(sorted(refused))}")
        return refused

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """Contract books at every poll. A Smarkets market id
        (`smarkets:164973231`) keeps every contract's book in it."""
        targets = self._split(market_ids)
        refused = self._want([market for market, _ in targets])
        for market, contract in targets:
            if market in refused:
                continue
            if contract is None:
                self.whole_markets.add(market)
            else:
                self.books.setdefault(_synpath_id(market, contract), LocalBook())

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """Top of book whenever a poll finds it changed, for contracts."""
        targets = self._split(market_ids)
        refused = self._want([market for market, _ in targets])
        self.tickers.update(_synpath_id(m, c) for m, c in targets if c is not None and m not in refused)

    async def watch_market_status(self, market_ids: list[str] | None = None) -> None:
        """State changes (halted, reopened, settled, voided) of each contract in
        these markets, read every `status_every` polls."""
        targets = self._split(market_ids or [])
        refused = self._want([market for market, _ in targets])
        self.statuses = (self.statuses or set()) | {m for m, _ in targets if m not in refused}

    async def unwatch(self, market_ids: list[str]) -> None:
        for market, contract in self._split(market_ids):
            if contract is not None:
                synpath_id = _synpath_id(market, contract)
                self.books.pop(synpath_id, None)
                self.tickers.discard(synpath_id)
                self._top.pop(synpath_id, None)
                if not any(s.startswith(f"{VENUE}:{market}-") for s in (*self.books, *self.tickers)) \
                        and market not in self.whole_markets and market not in (self.statuses or set()):
                    self.wanted.discard(market)
                continue
            self.whole_markets.discard(market)
            for synpath_id in [s for s in (*self.books, *self.tickers) if s.startswith(f"{VENUE}:{market}-")]:
                self.books.pop(synpath_id, None)
                self.tickers.discard(synpath_id)
                self._top.pop(synpath_id, None)
            self.wanted.discard(market)
            if self.statuses:
                self.statuses.discard(market)
            for contract_id in [c for c, m in self.contract_market.items() if m == market]:
                self.contract_market.pop(contract_id, None)

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
        contracts = message.get("contracts")
        if contracts is not None:
            for contract in contracts:
                market = str(contract.get("market_id"))
                if market in self.whole_markets:
                    self.contract_market[str(contract["id"])] = market
            if "market_states" in message:
                events += self._status_changes(message["market_states"], contracts)
        quotes = message.get("quotes") or {}
        polled = set(message.get("markets") or [])
        for contract_id, market in list(self.contract_market.items()):
            if market in self.whole_markets:
                self.books.setdefault(_synpath_id(market, contract_id), LocalBook())
        for synpath_id in sorted(set(self.books) | self.tickers):
            market, contract_id = synpath_id.split(":", 1)[1].split("-", 1)
            if market not in polled:
                continue
            events += self._book_events(synpath_id, contract_id, quotes.get(contract_id) or {}, message.get("at"))
        return events

    def _book_events(self, synpath_id: str, contract_id: str, quote: dict[str, Any], at: Any) -> list[Event]:
        bids, asks = yes_levels(quote.get("bids")), yes_levels(quote.get("offers"))
        events: list[Event] = []
        info = {"delayed": self.delayed, "polled_at": at, "currency": "GBP"}
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

    def _status_changes(self, market_states: dict[str, Any], contracts: list[dict[str, Any]]) -> list[Event]:
        events: list[Event] = []
        for contract in contracts:
            market = str(contract.get("market_id"))
            if self.statuses is None or (self.statuses and market not in self.statuses):
                continue
            market_state, contract_state = market_states.get(market), contract.get("state_or_outcome")
            synpath_id = _synpath_id(market, str(contract["id"]))
            state, result = state_of(market_state, contract_state)
            if self._last_state.get(synpath_id) == (state, result):
                continue
            self._last_state[synpath_id] = (state, result)
            events.append(MarketStatusEvent(
                venue=VENUE, market_id=synpath_id, state=state,  # type: ignore[arg-type]
                native=f"{market_state}/{contract_state}", result=result,
                info={"in_play": market_state == "live" or contract_state == "live",
                      "reduction_factor": contract.get("reduction_factor")},
            ))
        return events


def _synpath_id(market: str, contract: str) -> str:
    return ids.qualify(VENUE, f"{market}-{contract}")
