"""Smarkets: a UK betting exchange, read from its public HTTP API.

**No account is needed for market data.** Events, markets, contracts, quotes,
volumes, last executed prices and recent trades all answer an anonymous GET.
Without a session the quotes are *delayed* (Smarkets says so, without saying
by how much) and the rate limits are lower; every book and quote read here
records which in its `info` (`delayed`). An optional login
(`SMARKETS_USERNAME`, `SMARKETS_PASSWORD`) sends a session token for
undelayed quotes and the account's own limits; see `SmarketsSession`.
Betting needs approved API access from Smarkets (a one-off £150 fee) and is
not offered here yet.

Five things about this venue that shape the adapter:

**A Smarkets market is many contracts, and each contract is a Synpath
market.** "Full-time result" on Fulham v Crystal Palace has three contracts:
Fulham, Crystal Palace, the Draw. Each answers its own yes/no question and is
a Synpath market, `smarkets:164973231-456060183` (market id, contract id). The
Smarkets market with all its contracts is the Synpath event,
`smarkets:164973231`, as a Betfair market is. The Smarkets *event* (the
fixture) is the parent of that grouping and rides in
`info["smarkets_event"]`: one fixture holds a hundred markets (result, correct
score, totals), which are separate questions, not outcomes of one.

**Prices are already probabilities, and a bid is a YES bid.** Smarkets prices
in basis points of 100% (5000 = 50%, decimal odds 2.0) and names its book
after the resting orders: `bids` are resting buys (backs), `offers` resting
sells (lays). Backing a contract is buying its YES, so `bids` are YES bids
and `offers` YES asks at price/10,000, with no odds to invert. The best bid is
below the best offer: a crossing order would have matched.

**Sizes are pots in 1/10,000 GBP.** A quote's `quantity` is the total pot
(back stake plus lay liability) were the order matched, in 1/100 of a penny:
quantity 100,000 at 5000 is £10 of pot, £5 of back stake. A pot of £Q pays
£Q to the winning side, so it is Q contracts paying 1 each. Every level is
converted that way and the venue's raw prices and quantities stay in the
book's `info` (`bids`, `offers`). Amounts are GBP whatever the account's
currency.

**Volumes are whole pounds, per Smarkets market.** `/volumes/` reports, per
Smarkets market (not per contract), the back stakes plus lay liabilities
matched, in GBP. Not in 1/10,000 GBP like quantities: a market with
`volume: 376` had executions summing to more than £21 in its recent-trades
tape, and the minimum stake is £0.05, so a scale of 10,000 is impossible. It
rides on the Synpath event and in each contract's `info`, never as the
contract's own volume.

**Commission on net market winnings.** Smarkets charges a percentage (2% on
the standard tier) of an account's net winnings in each Smarkets market, at
settlement. See `FeeSchedule.estimate` (`net_market_winnings`).

Smarkets' API terms limit its use to viewing prices and betting on your own
account; odds scraping is prohibited. Requests carry a `synpath/<version>`
User-Agent: Smarkets' Cloudflare front refuses the default ones of HTTP
libraries (python-httpx, curl) with a 403 page.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Iterator
from urllib.parse import quote as urlquote

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
)
from .errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported, RateLimitExceeded
from .types import BookSide, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote, Trade, iso

VENUE = "smarkets"
API_URL = "https://api.smarkets.com"

CURRENCY = "GBP"
PRICE_SCALE = 10_000
"""Prices are basis points of 100%: 5000 is 50%."""
QUANTITY_SCALE = 10_000
"""Quantities are 1/100 of a penny: 10,000 is £1 of pot, one contract."""

LIMITER = RateLimiter(1.0, burst=5)
"""Smarkets allows 1200 requests per 60 s on an account (API users) and less
anonymously: an anonymous `/v3/events/` answers `X-RateLimit-Limit: 60`. One a
second, shared process-wide, stays inside the anonymous budget."""

QUOTES_LIMITER = RateLimiter(20 / 60, burst=3)
"""`/quotes/` has its own, stricter limit: 50 per 60 s for API users and, as
observed anonymously, 20 per 60 s. Shared by the adapter and the stream."""

SESSION_IDLE_LIMIT = 25 * 60.0
"""A session token lasts 30 minutes from its last authenticated request; one
unused for longer than this is replaced by a fresh login."""

# Batch sizes, as the OpenAPI document caps each route's ids.
EVENTS_PER_REQUEST = 300
EVENT_MARKETS_PER_REQUEST = 50
MARKETS_PER_REQUEST = 300
CONTRACT_MARKETS_PER_REQUEST = 100
QUOTE_MARKETS_PER_REQUEST = 200
VOLUME_MARKETS_PER_REQUEST = 300
LAST_PRICE_MARKETS_PER_REQUEST = 100
TRADE_CONTRACTS_PER_REQUEST = 300

EVENT_PAGE = 20
"""Smarkets events read per listing request. Football fixtures hold about a
hundred markets each, so a page of contracts rarely needs more."""

MAX_PAGES = 20
"""Most event pages one listing call reads while filling a page."""

DEFAULT_SCOPES = ("single_event", "outright", "antepost")
"""Event scopes listed by default: the kinds that hold markets. Roots and
categories (Football, Premier League) are tree nodes without markets."""

STATES = {
    "open": ("upcoming", "live"),
    "closed": ("ended", "suspended"),
    "settled": ("settled", "cancelled"),
    "all": ("new", "upcoming", "live", "ended", "settled", "cancelled", "suspended"),
}
"""Smarkets event states for each status a listing asks for."""

DEFAULT_COMMISSION = 0.02
"""Smarkets' standard commission on net market winnings."""

CONTRACT_RESULTS: dict[str, str] = {
    "winner": "yes", "loser": "no", "voided": "void", "reduced": "void", "deadheat": "dead_heat",
}
"""A contract's outcome as the YES result. A reduced contract was withdrawn
(a non-runner) and its bets voided, the matched ones on other contracts
reduced by its factor. A dead heat pays the winning stakes divided by the
number of contracts tied, so it is neither yes nor no."""

USER_AGENT = "synpath (+https://github.com/Synpath-ai/synpath)"


def user_agent() -> str:
    try:
        from . import __version__

        return f"synpath/{__version__} (+https://github.com/Synpath-ai/synpath)"
    except ImportError:  # pragma: no cover - during package import
        return USER_AGENT


# ---------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------

def contract_native_id(market_id: Any, contract_id: Any) -> str:
    """`164973231-456060183`: the Smarkets market id, then the contract id."""
    return f"{market_id}-{contract_id}"


def parse_contract_id(native: str) -> tuple[str, str]:
    """(Smarkets market id, contract id) from a contract's native id."""
    market_id, sep, contract_id = native.partition("-")
    if not sep or not market_id.isdigit() or not contract_id.isdigit():
        raise BadRequest(
            f"{VENUE}: {native!r} is not a contract id; expected '<market id>-<contract id>', "
            f"e.g. smarkets:164973231-456060183 (a bare market id names an event)"
        )
    return market_id, contract_id


def market_of(native: str) -> str:
    """The Smarkets market id from a contract id or a bare market id."""
    if "-" in native:
        return parse_contract_id(native)[0]
    if not native.isdigit():
        raise BadRequest(f"{VENUE}: {native!r} is not a Smarkets market or contract id")
    return native


# ---------------------------------------------------------------------------
# Pure normalizers
# ---------------------------------------------------------------------------

def iso_ms(value: Any) -> int | None:
    """Milliseconds from Smarkets' timestamps, which carry up to nanoseconds."""
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    head, dot, rest = text.partition(".")
    if dot:
        digits = "".join(ch for ch in rest if ch.isdigit())
        zone = rest[len(digits):]
        text = f"{head}.{digits[:6]}{zone}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        from datetime import timezone

        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def price(bps: Any) -> float:
    """Basis points of 100% as a probability."""
    return round(int(bps) / PRICE_SCALE, 6)


def contracts(quantity: Any) -> float:
    """A quantity (pot in 1/10,000 GBP) as contracts paying £1 each."""
    return round(int(quantity) / QUANTITY_SCALE, 6)


def levels(rows: Any) -> list[OrderLevel]:
    out = []
    for row in rows or []:
        bps, quantity = (row or {}).get("price"), (row or {}).get("quantity")
        if not bps or not quantity or int(quantity) <= 0 or not 0 < int(bps) < PRICE_SCALE:
            continue
        out.append(OrderLevel(price=price(bps), size=contracts(quantity)))
    return out


def raw_levels(rows: Any) -> list[list[int]]:
    """The venue's own [price bps, quantity] pairs."""
    return [[row["price"], row["quantity"]] for row in rows or [] if row and row.get("price") is not None]


def last_price(row: dict[str, Any] | None) -> tuple[float | None, int | None]:
    """(last executed YES price, when) from a `last_executed_prices` row. The
    price is a percentage string ("50", "23.45"); "0" means never traded."""
    if not row:
        return None, None
    value = row.get("last_executed_price")
    try:
        percent = float(value) if value is not None else 0.0
    except ValueError:
        return None, None
    if not 0 < percent < 100:
        return None, None
    return round(percent / 100, 6), iso_ms(row.get("timestamp"))


def quotes_of(book: dict[str, Any] | None, last: dict[str, Any] | None = None) -> tuple[Quote, Quote]:
    """YES and NO top of book from a contract's quotes: bids are YES bids,
    offers YES asks."""
    bids, asks = levels((book or {}).get("bids")), levels((book or {}).get("offers"))
    bid = max(bids, key=lambda level: level.price, default=None)
    ask = min(asks, key=lambda level: level.price, default=None)
    mid = round((bid.price + ask.price) / 2, 6) if bid and ask else None
    traded, traded_at = last_price(last)
    yes = Quote(bid=bid.price if bid else None, bid_size=bid.size if bid else None,
                ask=ask.price if ask else None, ask_size=ask.size if ask else None, mid=mid,
                last=traded, last_timestamp=traded_at if traded is not None else None,
                last_datetime=iso(traded_at) if traded is not None else None)
    no = Quote(bid=round(1 - ask.price, 6) if ask else None, bid_size=ask.size if ask else None,
               ask=round(1 - bid.price, 6) if bid else None, ask_size=bid.size if bid else None,
               mid=round(1 - mid, 6) if mid is not None else None,
               last=round(1 - traded, 6) if traded is not None else None,
               last_timestamp=traded_at if traded is not None else None,
               last_datetime=iso(traded_at) if traded is not None else None)
    return yes, no


def result_of(market_state: str | None, contract_state: str | None) -> str | None:
    if market_state == "voided":
        return "void"
    return CONTRACT_RESULTS.get(contract_state or "")


def status_of(market_state: str | None, contract_state: str | None, event_state: str | None = None) -> tuple[str, bool]:
    """(normalized status, accepting orders) from the market's, contract's and
    event's states.

    A halted market or contract is still open: Smarkets halts for a goal or a
    red card and reopens, which is a pause, not a change of lifecycle. A
    contract with an outcome (winner, loser, voided, reduced) is settled even
    while its market trades on."""
    if contract_state in CONTRACT_RESULTS or market_state in ("settled", "voided"):
        return "settled", False
    if event_state in ("settled", "cancelled"):
        return "settled", False
    if market_state == "new" or contract_state == "new":
        return "unopened", False
    if event_state in ("ended",):
        return "closed", False
    if market_state in ("open", "live") and contract_state in (None, "open", "live"):
        return "open", True
    return "open", False


def _tags(event: dict[str, Any], market: dict[str, Any]) -> list[str]:
    kind = event.get("type")
    names: list[Any] = []
    if isinstance(kind, dict):
        names += [kind.get("domain"), kind.get("scope")]
    elif kind:
        names.append(kind)
    names.append(market.get("category"))
    seen: dict[str, None] = {}
    for name in names:
        if name and str(name).strip():
            seen.setdefault(str(name).strip().lower(), None)
    return list(seen)


def _event_info(event: dict[str, Any]) -> dict[str, Any] | None:
    """The Smarkets event without its SEO text template."""
    if not event:
        return None
    return {key: value for key, value in event.items() if key != "seo_description"}


def _close(event: dict[str, Any], market: dict[str, Any]) -> int | None:
    """When trading is expected to stop: the event's scheduled end where it has
    one, else its start for a market that does not trade in-play. A market
    that trades in-play has no known close until the event ends."""
    end = iso_ms(event.get("end_date"))
    if end:
        return end
    if not market.get("inplay_enabled"):
        return iso_ms(event.get("start_datetime"))
    return None


def normalize_market(
    event: dict[str, Any] | None, market: dict[str, Any], contract: dict[str, Any], *,
    quote: dict[str, Any] | None = None, last: dict[str, Any] | None = None, volume: dict[str, Any] | None = None,
    event_state: str | None = None, delayed: bool = True,
) -> Market:
    """One contract of a Smarkets market as a binary market: YES is "this
    contract wins"."""
    event = event or {}
    market_id, contract_id = str(market["id"]), str(contract["id"])
    native = contract_native_id(market_id, contract_id)
    market_state, contract_state = market.get("state"), contract.get("state_or_outcome")
    event_state = event_state or event.get("state")
    status, accepting = status_of(market_state, contract_state, event_state)
    yes_quote, no_quote = quotes_of(quote, last)
    name = str(contract.get("name") or contract_id)
    close = _close(event, market)
    tags = _tags(event, market)
    traded_gbp = (volume or {}).get("volume")
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, market_id),
        title=" — ".join(str(part) for part in (event.get("name"), market.get("name"), name) if part),
        description=market.get("description") or event.get("description") or None,
        slug=contract.get("slug"),
        yes=Outcome(label=name, quote=yes_quote),
        no=Outcome(label=f"Not {name}", quote=no_quote),
        status=status,  # type: ignore[arg-type]
        native_status=f"{market_state}/{contract_state}" if market_state or contract_state else None,
        active=accepting,
        # A handicap line can half-win, a reduced contract scales the others:
        # neither is a plain yes/no contract.
        market_type="unknown" if market.get("category") == "handicap" else "binary",
        open_timestamp=None,
        open_datetime=None,
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=iso_ms(contract.get("outcome_timestamp")),
        resolution_datetime=iso(iso_ms(contract.get("outcome_timestamp"))),
        # Prices sit on an odds ladder in basis points, so the step in YES
        # price varies along it.
        tick_size=None,
        face_value=1.0,
        book_model="shared_complement",
        # Smarkets reports volume per market, not per contract: see info.
        stats=MarketStats(),
        url=None,
        image_url=None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=name,
        neg_risk=False,
        settlement_sources=[],
        info={
            "market_id": market_id,
            "contract_id": contract_id,
            "smarkets_event_id": str(market.get("event_id") or event.get("id") or "") or None,
            "contract": contract,
            "market": market,
            "smarkets_event": _event_info(event),
            "quote": quote,
            "market_state": market_state,
            "contract_state": contract_state,
            "event_state": event_state,
            "result": result_of(market_state, contract_state),
            "reduction_factor": contract.get("reduction_factor"),
            "in_play": market_state == "live" or contract_state == "live" or event_state == "live",
            "inplay_enabled": market.get("inplay_enabled"),
            "bet_delay": market.get("bet_delay"),
            "market_type": (market.get("market_type") or {}).get("name"),
            "contract_type": (contract.get("contract_type") or {}).get("name"),
            "winner_count": market.get("winner_count"),
            "start_datetime": iso_ms(event.get("start_datetime")),
            "full_slug": event.get("full_slug"),
            "market_volume_gbp": traded_gbp,
            "market_double_stake_volume_gbp": (volume or {}).get("double_stake_volume"),
            "delayed": delayed,
            "currency": CURRENCY,
        },
    )


def _visible(contract: dict[str, Any]) -> bool:
    return not contract.get("hidden")


def normalize_event(
    event: dict[str, Any] | None, market: dict[str, Any], contracts_: list[dict[str, Any]], *,
    quotes: dict[str, Any] | None = None, last: dict[str, dict[str, Any]] | None = None,
    volume: dict[str, Any] | None = None, event_state: str | None = None, delayed: bool = True,
) -> Event:
    """A Smarkets market with its contracts: the grouping of contract markets."""
    event = event or {}
    quotes, last = quotes or {}, last or {}
    market_id = str(market["id"])
    markets = [
        normalize_market(event, market, contract, quote=quotes.get(str(contract["id"])),
                         last=last.get(str(contract["id"])), volume=volume, event_state=event_state, delayed=delayed)
        for contract in contracts_
    ]
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "unopened", "closed", "settled") if s in statuses), None)
    if status is None:
        status, _ = status_of(market.get("state"), None, event_state or event.get("state"))
    winners = market.get("winner_count")
    close = _close(event, market)
    tags = _tags(event, market)
    return Event(
        id=ids.qualify(VENUE, market_id),
        venue=VENUE,
        venue_event_id=market_id,
        title=" — ".join(str(p) for p in (event.get("name"), market.get("name")) if p) or market_id,
        description=market.get("description") or None,
        slug=market.get("slug"),
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=market.get("state"),
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        # One winner among the contracts is exclusive; a place market or a
        # handicap is not.
        mutually_exclusive=(winners == 1) if winners is not None and market.get("category") != "handicap" else None,
        close_timestamp=close,
        close_datetime=iso(close),
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            "smarkets_event": _event_info(event),
            "smarkets_event_id": str(market.get("event_id") or event.get("id") or "") or None,
            "market_type": (market.get("market_type") or {}).get("name"),
            "category": market.get("category"),
            "winner_count": winners,
            "complete": market.get("complete"),
            "in_play": market.get("state") == "live",
            "inplay_enabled": market.get("inplay_enabled"),
            "bet_delay": market.get("bet_delay"),
            "volume_gbp": (volume or {}).get("volume"),
            "double_stake_volume_gbp": (volume or {}).get("double_stake_volume"),
            "delayed": delayed,
            "currency": CURRENCY,
        },
    )


def normalize_order_book(
    book: dict[str, Any] | None, *, market_id: str, side: BookSide = "yes", depth: int | None = None,
    delayed: bool = True,
) -> OrderBook:
    """A contract's quotes read on YES: bids are YES bids, offers YES asks, at
    price/10,000, sizes in contracts (pot/10,000). `side="no"` mirrors it."""
    book = book or {}
    bids = sorted(levels(book.get("bids")), key=lambda level: level.price, reverse=True)
    asks = sorted(levels(book.get("offers")), key=lambda level: level.price)
    derived = side == "no"
    if derived:
        bids, asks = (
            [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in asks],
            [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in bids],
        )
    elif side != "yes":
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks,
        timestamp=None, datetime=None, book_model="shared_complement", derived=derived,
        # Smarkets does not document how deep `/quotes/` goes; anonymous
        # reads have shown at most three levels a side.
        depth_scope="unknown",
        info={"bids": raw_levels(book.get("bids")), "offers": raw_levels(book.get("offers")),
              "currency": CURRENCY, "delayed": delayed},
    )


def normalize_trade(row: dict[str, Any], index: int = 0) -> Trade:
    """A recent trade: price in YES, quantity (the pot) in contracts. `side`
    is what the taker did: `buy` backed the contract (took YES), `sell` laid
    it. Smarkets gives trades no id, so one is made from the contract, the
    nanosecond timestamp and the row's position."""
    market_id, contract_id = str(row["market_id"]), str(row["contract_id"])
    stamp = iso_ms(row.get("timestamp")) or 0
    side = row.get("side")
    return Trade(
        id=f"{contract_id}-{row.get('timestamp') or stamp}-{index}",
        market_id=ids.qualify(VENUE, contract_native_id(market_id, contract_id)),
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=price(row["price"]),
        quantity=contracts(row["quantity"]),
        side=side if side in ("buy", "sell") else "unknown",
        info={"raw": row, "currency": CURRENCY, "id_source": "synthesized"},
    )


def fee_schedule_of(market_id: str, rate: float = DEFAULT_COMMISSION) -> FeeSchedule:
    """Commission on net winnings per Smarkets market, makers and takers alike."""
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="net_market_winnings",
        taker_rate=rate, maker_rate=rate,
        info={
            "charged": "at settlement, on the account's net winnings in the whole Smarkets market",
            "rate_source": "Smarkets' standard tier" if rate == DEFAULT_COMMISSION else "configured",
            "not_included": "tiered or promotional rates on the account (Smarkets publishes no per-market rate)",
        },
    )


def error_type(body: Any) -> str | None:
    """Smarkets' own error code: `{"error_type": "...", "data": ...}`."""
    if isinstance(body, dict) and body.get("error_type"):
        return str(body["error_type"])
    return None


def batched(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


# ---------------------------------------------------------------------------
# Session (optional)
# ---------------------------------------------------------------------------

class SmarketsSession:
    """An account's session token, kept in memory only. Optional: every read
    here works without one, with delayed quotes and lower limits.

    Logs in on first use with `POST /v3/sessions/` (username and password).
    The token goes on requests as `Authorization: Session-Token <token>`. It
    lasts 30 minutes from its last authenticated request, so one idle for
    `SESSION_IDLE_LIMIT` is replaced by a fresh login. Smarkets allows 5
    logins per 300 s; one lock means one login per session object. An
    account with two-factor login is refused with a clear error: this
    library does not complete the second factor.
    """

    def __init__(
        self, *, username: str | None = None, password: str | None = None, session_token: str | None = None,
        client: httpx.Client | None = None, timeout: float = 30.0, clock: Any = time.monotonic,
        base_url: str = API_URL,
    ):
        self.username = username
        self._password = password
        self._token = session_token
        self._clock = clock
        self._used = clock() if session_token else 0.0
        self._lock = threading.Lock()
        self._client = client
        self._timeout = timeout
        self.base_url = base_url.rstrip("/")

    def __repr__(self) -> str:
        return f"SmarketsSession(username={self.username!r}, token={'set' if self._token else 'none'})"

    def token(self) -> str:
        with self._lock:
            if not self._token or self._clock() - self._used > SESSION_IDLE_LIMIT:
                self._login()
            self._used = self._clock()
            assert self._token
            return self._token

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Session-Token {self.token()}"}

    def invalidate(self) -> None:
        with self._lock:
            self._token = None

    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self._timeout, headers={"User-Agent": user_agent()})
        return self._client

    def _login(self) -> None:
        if not self.username or not self._password:
            raise AuthenticationError(
                f"{VENUE}: no session; set SMARKETS_USERNAME and SMARKETS_PASSWORD, or pass session_token= "
                f"(market data needs neither)"
            )
        try:
            response = self._http().post(
                f"{self.base_url}/v3/sessions/", json={"username": self.username, "password": self._password},
                headers={"Accept": "application/json", "User-Agent": user_agent()},
            )
        except httpx.HTTPError as exc:
            from .errors import NetworkError

            raise NetworkError(f"{VENUE}: login unreachable: {type(exc).__name__}") from None
        try:
            body = response.json()
        except ValueError:
            body = None
        code = error_type(body)
        if response.status_code == 429:
            raise RateLimitExceeded(f"{VENUE}: too many logins (Smarkets allows 5 per 300 s)")
        if response.status_code >= 400:
            raise AuthenticationError(f"{VENUE}: login refused: {code or response.status_code}",
                                      status=response.status_code, body={"error_type": code})
        body = body or {}
        if body.get("factor") not in (None, "complete"):
            raise AuthenticationError(
                f"{VENUE}: the login needs a second factor ({body.get('factor')}); this library does not "
                f"complete it. Log in elsewhere and pass session_token="
            )
        if not body.get("token"):
            raise AuthenticationError(f"{VENUE}: the login answered without a token")
        self._token = str(body["token"])

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


class SmarketsAuth(httpx.Auth):
    """Puts the session token on every request and retries once with a fresh
    login when Smarkets says the token is no longer valid."""

    requires_response_body = True

    def __init__(self, session: SmarketsSession):
        self.session = session

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        request.headers.update(self.session.headers())
        response = yield request
        if response.status_code == 401:
            self.session.invalidate()
            request.headers.update(self.session.headers())
            yield request


def session_from(credentials: Any = None, **overrides: Any) -> SmarketsSession | None:
    """A session from credentials, else from `SMARKETS_*` in the environment
    or a `.env`; `None` when no login is configured (the usual case)."""
    if credentials is None:
        credentials = credentials_from_env()
    if credentials is None:
        return None
    return SmarketsSession(username=credentials.username, password=credentials.password, **overrides)


def credentials_from_env() -> Any:
    import os

    from .trading.credentials import load_smarkets, read_dotenv

    merged: dict[str, str] = {}
    if os.path.exists(".env"):
        merged.update(read_dotenv(".env"))
    merged.update(os.environ)
    return load_smarkets(merged)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Smarkets(Exchange):
    """Smarkets, read-only. No account needed.

    ```python
    import synpath

    smk = synpath.Smarkets()
    markets = smk.fetch_markets(domain="football", limit=10)
    book = smk.fetch_order_book(markets[0].id)
    book.info["delayed"]                      # True without a login
    ```
    """

    id = VENUE
    name = "Smarkets"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        # Smarkets lists events by id, start time or display order only.
        "sort": False,
        "fetch_order_book": True,
        # Two hundred Smarkets markets a request, every contract of each.
        "fetch_order_books": True,
        # The most recent trades per contract only; no paging back in time.
        "fetch_trades": "partial",
        # The executions time series answers 503 CHARTS_UNAVAILABLE.
        "fetch_ohlcv": False,
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, credentials: Any = None, session: SmarketsSession | None = None, session_token: str | None = None,
        login: bool | None = None, commission: float = DEFAULT_COMMISSION, timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER, quotes_limiter: RateLimiter | None = QUOTES_LIMITER,
        client: Any = None, session_client: httpx.Client | None = None, base_url: str = API_URL,
    ):
        """No argument is needed: market data is public. `credentials` (else
        `SMARKETS_*` from the environment, when `login` is not False) or
        `session_token` log in for undelayed quotes. Nothing is sent until the
        first call. `commission` is the rate fee schedules report."""
        overrides: dict[str, Any] = {"client": session_client, "timeout": timeout, "base_url": base_url}
        if session is None and session_token:
            session = SmarketsSession(session_token=session_token, **overrides)
        if session is None and login is not False:
            session = session_from(credentials, **overrides)
        self.session = session
        self.commission = commission
        if client is None:
            client = httpx.Client(timeout=timeout, follow_redirects=True)
        client.headers["User-Agent"] = user_agent()
        client.headers["Accept"] = "application/json"
        if self.session is not None:
            client.auth = SmarketsAuth(self.session)
        self.http = HttpClient(base_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.quotes_http = HttpClient(base_url, limiter=quotes_limiter, timeout=timeout, client=client, venue=VENUE)

    @property
    def delayed(self) -> bool:
        """Quotes read without a session are delayed, Smarkets says."""
        return self.session is None

    def _get(self, path: str, params: Any = None, *, quotes: bool = False) -> Any:
        try:
            return (self.quotes_http if quotes else self.http).get(path, params)
        except ExchangeError as exc:
            code = error_type(exc.body)
            if exc.status == 403 and code is None:
                raise AuthenticationError(
                    f"{VENUE}: refused by Smarkets' Cloudflare front (403); it blocks some clients and "
                    f"countries", status=403,
                ) from None
            raise

    # -- raw reads ----------------------------------------------------------

    def fetch_smarkets_events(self, event_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Smarkets events (fixtures, outrights) by id."""
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(event_ids)), EVENTS_PER_REQUEST):
            body = self._get(f"/v3/events/{','.join(chunk)}/", {"with_new_type": "true"})
            for event in (body or {}).get("events") or []:
                out[str(event["id"])] = event
        return out

    def _event_markets(self, event_ids: list[str], *, market_type: str | list[str] | None = None) -> list[dict[str, Any]]:
        """Every visible market of these events, events in the order given,
        each event's markets in Smarkets' display order."""
        types = ",".join([market_type] if isinstance(market_type, str) else market_type) if market_type else None
        rows: list[dict[str, Any]] = []
        for chunk in batched(list(dict.fromkeys(event_ids)), EVENT_MARKETS_PER_REQUEST):
            body = self._get(f"/v3/events/{','.join(chunk)}/markets/", {"market_types": types})
            rows += [m for m in (body or {}).get("markets") or [] if isinstance(m, dict) and m.get("id")]
        order = {event_id: i for i, event_id in enumerate(event_ids)}
        return sorted(rows, key=lambda m: order.get(str(m.get("event_id")), len(order)))

    def _markets(self, market_ids: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(market_ids)), MARKETS_PER_REQUEST):
            body = self._get(f"/v3/markets/{','.join(chunk)}/", {"include_hidden": "true"})
            for market in (body or {}).get("markets") or []:
                out[str(market["id"])] = market
        return out

    def _contracts(self, market_ids: list[str], *, include_hidden: bool = False) -> dict[str, list[dict[str, Any]]]:
        """Contracts by Smarkets market id, in Smarkets' display order (highest first)."""
        out: dict[str, list[dict[str, Any]]] = {m: [] for m in market_ids}
        for chunk in batched(list(dict.fromkeys(market_ids)), CONTRACT_MARKETS_PER_REQUEST):
            params = {"include_hidden": "true"} if include_hidden else None
            body = self._get(f"/v3/markets/{','.join(chunk)}/contracts/", params)
            for contract in (body or {}).get("contracts") or []:
                out.setdefault(str(contract["market_id"]), []).append(contract)
        for rows in out.values():
            rows.sort(key=lambda c: (-(c.get("display_order") or 0), int(c["id"])))
        return out

    def fetch_quotes(self, market_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Raw quotes by contract id for these Smarkets markets, two hundred
        markets a request: `{contract_id: {"bids": [...], "offers": [...]}}`."""
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(market_ids)), QUOTE_MARKETS_PER_REQUEST):
            out.update(self._get(f"/v3/markets/{','.join(chunk)}/quotes/", quotes=True) or {})
        return out

    def fetch_volumes(self, market_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Traded volume in GBP by Smarkets market id."""
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(market_ids)), VOLUME_MARKETS_PER_REQUEST):
            for row in (self._get(f"/v3/markets/{','.join(chunk)}/volumes/") or {}).get("volumes") or []:
                out[str(row["market_id"])] = row
        return out

    def fetch_last_executed_prices(self, market_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Last executed price rows by contract id."""
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(market_ids)), LAST_PRICE_MARKETS_PER_REQUEST):
            body = self._get(f"/v3/markets/{','.join(chunk)}/last_executed_prices/") or {}
            for rows in (body.get("last_executed_prices") or {}).values():
                for row in rows or []:
                    out[str(row["contract_id"])] = row
        return out

    def fetch_event_states(self, event_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Live state (scores, clock, period) of Smarkets events, by id."""
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(event_ids)), EVENTS_PER_REQUEST):
            for row in (self._get(f"/v3/events/{','.join(chunk)}/states/") or {}).get("event_states") or []:
                out[str(row["id"])] = row
        return out

    # -- catalog ------------------------------------------------------------

    def _event_page(
        self, *, states: tuple[str, ...], domain: str | list[str] | None, scope: str | list[str] | None,
        after: str | None, limit: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        """One page of bettable Smarkets events by id after `after`, and
        whether more follow. Repeated parameters are fine at Smarkets' front
        with a real User-Agent."""
        params: list[tuple[str, Any]] = [("state", s) for s in states]
        domains = [domain] if isinstance(domain, str) else list(domain or [])
        scopes = [scope] if isinstance(scope, str) else list(scope or DEFAULT_SCOPES)
        params += [("type_domain", d) for d in domains] + [("type_scope", s) for s in scopes]
        params += [("with_new_type", "true"), ("sort", "id"), ("limit", str(limit))]
        if after:
            params.append(("pagination_last_id", after))
        body = self._get("/v3/events/", params) or {}
        events = [e for e in body.get("events") or [] if isinstance(e, dict) and e.get("id")]
        more = bool((body.get("pagination") or {}).get("next_page")) and bool(events)
        return events, more

    def _search(self, query: str) -> list[str]:
        body = self._get(f"/v3/search/{urlquote(query, safe='')}/") or {}
        return [str(i) for i in body.get("search_result_event_ids") or []]

    def _walk(
        self, *, query: str | None, states: tuple[str, ...], domain: Any, scope: Any, market_type: Any,
        cursor: str | None,
    ) -> Iterator[tuple[dict[str, Any], list[dict[str, Any]], int]]:
        """(event, its markets from the first one to read, index of that market)
        from `cursor` on, events in id order. `query` reads Smarkets' search
        instead, in its own order, filtered to the states asked for."""
        after, skip_markets, _ = _cursor(cursor)
        if query:
            found = self._search(query)
            if after:
                found = found[found.index(after) + 1:] if after in found else []
            events = self.fetch_smarkets_events(found)
            ordered = [events[i] for i in found if i in events and events[i].get("state") in states
                       and events[i].get("bettable")]
            pages: Iterator[list[dict[str, Any]]] = iter([ordered])
        else:
            pages = self._event_pages(states=states, domain=domain, scope=scope, after=after)
        first = True
        for page in pages:
            page = [e for e in page if e.get("bettable", True)]
            if not page:
                continue
            markets = self._event_markets([str(e["id"]) for e in page], market_type=market_type)
            by_event: dict[str, list[dict[str, Any]]] = {}
            for market in markets:
                by_event.setdefault(str(market["event_id"]), []).append(market)
            for event in page:
                start = skip_markets if first else 0
                first = False
                yield event, by_event.get(str(event["id"]), [])[start:], start

    def _event_pages(self, *, states: tuple[str, ...], domain: Any, scope: Any, after: str | None) -> Iterator[list[dict[str, Any]]]:
        """Pages of events after `after`. The first page starts *at* the event
        a cursor resumes in, so it is read from the event before it."""
        for _ in range(MAX_PAGES):
            events, more = self._event_page(states=states, domain=domain, scope=scope, after=after, limit=EVENT_PAGE)
            yield events
            if not more:
                return
            after = str(events[-1]["id"])

    def _listing_args(self, status: str) -> tuple[str, ...]:
        check_status(status)
        return STATES[status]

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sort: str | None = None, domain: str | list[str] | None = None,
        scope: str | list[str] | None = None, market_type: str | list[str] | None = None,
    ) -> Page[Market]:
        """One page of contracts (each a market), Smarkets events in id order,
        each event's markets in display order, with their quotes, last
        executed prices and market volumes. `query` is Smarkets' search;
        `domain` (`football`, `politics`) and `scope` (`single_event`,
        `outright`) are Smarkets' event types, `market_type` its market type
        names (`WINNER_3_WAY`). `status` picks the events' states: open is
        upcoming or live, closed ended or suspended, settled settled or
        cancelled."""
        states = self._listing_args(status)
        check_sort(sort, venue=VENUE, supported=False)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        previous, _, skip_contracts = _cursor(cursor)
        picked: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        first_market = True
        for event, markets, start in self._walk(query=query, states=states, domain=domain, scope=scope,
                                                market_type=market_type, cursor=cursor):
            # One request (a hundred markets) usually covers a whole fixture.
            contracts_by_market = self._contracts([str(m["id"]) for m in markets]) if markets else {}
            for offset, market in enumerate(markets):
                rows = [c for c in contracts_by_market.get(str(market["id"]), []) if _visible(c)]
                skip = skip_contracts if first_market else 0
                first_market = False
                rows = rows[skip:]
                room = wanted - len(picked)
                if len(rows) > room:
                    picked += [(event, market, contract) for contract in rows[:room]]
                    return Page(self._priced(picked), next_cursor=f"{previous or ''}|{start + offset}|{skip + room}")
                picked += [(event, market, contract) for contract in rows]
                if len(picked) >= wanted:
                    return Page(self._priced(picked), next_cursor=f"{previous or ''}|{start + offset + 1}|0")
            first_market = False
            previous = str(event["id"])
        return Page(self._priced(picked), next_cursor=None)

    def _priced(self, picked: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]) -> list[Market]:
        if not picked:
            return []
        market_ids = list(dict.fromkeys(str(market["id"]) for _, market, _ in picked))
        quotes = self.fetch_quotes(market_ids)
        last = self.fetch_last_executed_prices(market_ids)
        volumes = self.fetch_volumes(market_ids)
        return [
            normalize_market(event, market, contract, quote=quotes.get(str(contract["id"])),
                             last=last.get(str(contract["id"])), volume=volumes.get(str(market["id"])),
                             delayed=self.delayed)
            for event, market, contract in picked
        ]

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", domain: str | list[str] | None = None, scope: str | list[str] | None = None,
        market_type: str | list[str] | None = None,
    ) -> Page[Event]:
        """One page of Smarkets markets (each a Synpath event), with their
        contracts priced. Filters as `fetch_markets`."""
        states = self._listing_args(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        picked: list[tuple[dict[str, Any], dict[str, Any]]] = []
        next_cursor = None
        previous, _, _ = _cursor(cursor)
        for event, markets, start in self._walk(query=query, states=states, domain=domain, scope=scope,
                                                market_type=market_type, cursor=cursor):
            room = wanted - len(picked)
            picked += [(event, market) for market in markets[:room]]
            if len(markets) > room:
                next_cursor = f"{previous or ''}|{start + room}|0"
                break
            previous = str(event["id"])
            if len(picked) >= wanted:
                next_cursor = f"{previous}|0|0"
                break
        return Page(self._grouped(picked), next_cursor=next_cursor)

    def _grouped(self, picked: list[tuple[dict[str, Any], dict[str, Any]]], *, include_hidden: bool = False) -> list[Event]:
        if not picked:
            return []
        market_ids = [str(market["id"]) for _, market in picked]
        contracts_by_market = self._contracts(market_ids, include_hidden=include_hidden)
        quotes = self.fetch_quotes(market_ids)
        last = self.fetch_last_executed_prices(market_ids)
        volumes = self.fetch_volumes(market_ids)
        return [
            normalize_event(event, market, [c for c in contracts_by_market.get(str(market["id"]), [])
                                            if include_hidden or _visible(c)],
                            quotes=quotes, last=last, volume=volumes.get(str(market["id"])), delayed=self.delayed)
            for event, market in picked
        ]

    def fetch_event(self, event_id: str) -> Event:
        """One Smarkets market with its contracts, by `smarkets:<market id>`,
        open or settled."""
        market_id = market_of(self.native(event_id))
        markets = self._markets([market_id])
        if market_id not in markets:
            raise MarketNotFound(f"{VENUE}: no market {market_id}")
        market = markets[market_id]
        events = self.fetch_smarkets_events([str(market["event_id"])])
        return self._grouped([(events.get(str(market["event_id"]), {}), market)], include_hidden=True)[0]

    def fetch_market(self, market_id: str) -> Market:
        """One contract by id, open or settled."""
        found = self.fetch_markets_by_ids([market_id])
        if not found:
            raise MarketNotFound(f"{VENUE}: no contract {self.native(market_id)}")
        return found[0]

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Contracts in the order asked for, open or settled: markets three
        hundred a request, their events, contracts a hundred markets a
        request, then quotes, last prices and volumes. Ids Smarkets does not
        know are left out."""
        wanted = list(dict.fromkeys(parse_contract_id(self.native(m)) for m in market_ids))
        smarkets_ids = list(dict.fromkeys(market for market, _ in wanted))
        markets = self._markets(smarkets_ids)
        known = [m for m in smarkets_ids if m in markets]
        if not known:
            return []
        events = self.fetch_smarkets_events([str(markets[m]["event_id"]) for m in known])
        contracts_by_market = self._contracts(known, include_hidden=True)
        quotes, last, volumes = self.fetch_quotes(known), self.fetch_last_executed_prices(known), self.fetch_volumes(known)
        out: list[Market] = []
        for market_id, contract_id in wanted:
            market = markets.get(market_id)
            contract = next((c for c in contracts_by_market.get(market_id, []) if str(c["id"]) == contract_id), None)
            if market is None or contract is None:
                continue
            out.append(normalize_market(
                events.get(str(market["event_id"])), market, contract, quote=quotes.get(contract_id),
                last=last.get(contract_id), volume=volumes.get(market_id), delayed=self.delayed,
            ))
        return out

    # -- market data --------------------------------------------------------

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        """A contract's quotes on YES."""
        books = self.fetch_order_books([market_id], side=side, depth=depth)
        if not books:
            raise MarketNotFound(f"{VENUE}: no quotes for {self.native(market_id)} (unknown, or its market has closed)")
        return next(iter(books.values()))

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many contracts, two hundred Smarkets markets a request.
        A Smarkets market id (an event) gives every contract in it. Contracts
        Smarkets has no quotes for are left out."""
        natives = [self.native(m) for m in dict.fromkeys(market_ids)]
        quotes = self.fetch_quotes([market_of(n) for n in natives])
        out: dict[str, OrderBook] = {}
        for native in natives:
            if "-" not in native:
                # A whole market: every contract that has quotes. The quotes
                # do not say which market a contract is in, so ask.
                for contract in self._contracts([native], include_hidden=True).get(native, []):
                    contract_id = str(contract["id"])
                    if contract_id in quotes:
                        synpath_id = self.qualify(contract_native_id(native, contract_id))
                        out[synpath_id] = normalize_order_book(quotes[contract_id], market_id=synpath_id, side=side,
                                                               depth=depth, delayed=self.delayed)
                continue
            market, contract_id = parse_contract_id(native)
            if contract_id not in quotes:
                continue
            synpath_id = self.qualify(native)
            out[synpath_id] = normalize_order_book(quotes[contract_id], market_id=synpath_id, side=side, depth=depth,
                                                   delayed=self.delayed)
        return out

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> list[Trade]:
        """A contract's most recent trades, oldest first. Smarkets keeps only a
        short recent tape here (a handful per contract) and offers no paging,
        so `cursor` is refused and `since` filters what the tape holds."""
        if cursor:
            raise NotSupported(f"{VENUE}: the recent-trades tape has no paging")
        market, contract_id = parse_contract_id(self.native(market_id))
        body = self._get(f"/v3/contracts/{contract_id}/recent_trades/") or {}
        rows = [r for r in body.get("recent_trades") or [] if str(r.get("contract_id")) == contract_id]
        trades = [normalize_trade(row, i) for i, row in enumerate(rows)]
        trades.sort(key=lambda t: t.timestamp)
        if since is not None:
            trades = [t for t in trades if t.timestamp >= since]
        return trades[-limit:] if limit else trades

    def fetch_ohlcv(self, market_id: str, **_: Any) -> list:
        raise NotSupported(
            f"{VENUE}: Smarkets' executions time series answers CHARTS_UNAVAILABLE; use fetch_trades for the "
            f"recent tape"
        )

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """Commission on net winnings per Smarkets market. Smarkets publishes no
        per-market rate, so this reads nothing: the rate is the adapter's
        `commission` (2%, the standard tier)."""
        return fee_schedule_of(market_of(self.native(market_id)), self.commission)

    def close(self) -> None:
        self.http.close()
        if self.session is not None:
            self.session.close()


def _cursor(cursor: str | None) -> tuple[str | None, int, int]:
    """`after|market|contract`: the event id the listing resumes after (empty
    for the start), how many markets of the next event were read, and how
    many contracts of the market after them."""
    if not cursor:
        return None, 0, 0
    try:
        after, markets, contracts_ = cursor.split("|")
        if after and not after.isdigit():
            raise ValueError
        return after or None, max(0, int(markets)), max(0, int(contracts_))
    except ValueError:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued") from None
