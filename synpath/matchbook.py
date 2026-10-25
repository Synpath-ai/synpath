"""Matchbook: a back/lay betting exchange, read from its public REST API.

**No account is needed for market data.** Sports, events, markets, runners
and their prices all answer an anonymous GET on `api.matchbook.com`. A login
(`MATCHBOOK_USERNAME`, `MATCHBOOK_PASSWORD`) is what betting needs, and the
trading adapter (`synpath.trading.matchbook`) makes it; this adapter sends a
session only when given one (`credentials=`, `session=`, or `login=True` to
read the environment). See `MatchbookSession`.

Five things about this venue that shape the adapter:

**A Matchbook market is many runners, and each runner is a Synpath market.**
"Match Odds" on Arsenal vs Everton has three runners: Arsenal, Everton, the
Draw. Each answers its own yes/no question and is a Synpath market,
`matchbook:34569494493900023-34569495835700023-34569495836701023` (event id,
market id, runner id: Matchbook addresses a runner under its event and market,
so the id carries all three). The Matchbook market with all its runners is the
Synpath event, `matchbook:<event id>-<market id>`, as a Betfair market is. The
Matchbook *event* (the fixture or race) is the parent of that grouping and
rides in `info["matchbook_event"]`.

**Back and lay, read as YES, the way Betfair reads them.** Backing a runner at
decimal odds O is buying its YES at 1/O; laying it is selling YES at 1/O.
Matchbook names each price after what a *taker* can do with it: a `back`
price is one a taker can back at, a resting lay, so on YES it is an **ask**;
a `lay` price is one a taker can lay at, a resting back, so it is a **bid**.
The best back price (the highest odds) sits below the best lay price, which
keeps the YES bid under the YES ask.

**Sizes are backer's stakes in the price's currency.** `available-amount` is
the stake a backer can match at that price, on both sides (a lay price's
amount is the backer's stake laid against, not the layer's liability: a back
of £879.92 at 1.04 on one side of a two-way market mirrors a lay of £35.19 at
26 on the other, both 915 contracts). A stake S at odds O is S·O contracts
paying 1 each, so every level is converted that way, and the venue's own odds
and amounts stay in the book's `info` (`back`, `lay`). The currency is the
one Matchbook picks for the request's region unless `currency=` asks for
another; every book and quote records it.

**No public trades.** Matchbook publishes the volume matched per runner, never
individual trades, so `fetch_trades` and candles are not offered.

**Commission on net market winnings.** Matchbook charges a percentage of an
account's net winnings in each market at settlement: 2% for customers in the
UK, Ireland, the Channel Islands and the Isle of Man, 4% elsewhere, by its
terms. See `FeeSchedule.estimate` (`net_market_winnings`).

Every GET is chargeable on Matchbook's API plans (£100 per million a calendar
month) and rate limited per account or IP by group: events 700 a minute,
navigation 100, reports 40. A limit exceeded blocks the caller for ten
minutes. Reads here are batched to keep the count down. Matchbook's terms
allow viewing prices and betting on your own account; redistributing its
prices or event data needs its written consent.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
)
from .betfair import CLASSIC_LADDER, contracts, yes_price
from .errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported, RateLimitExceeded
from .smarkets import user_agent
from .types import BookSide, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote, iso

VENUE = "matchbook"
API_URL = "https://api.matchbook.com"
EDGE = "/edge/rest"
SESSION_PATH = "/bpapi/rest/security/session"

LIMITER = RateLimiter(5.0, burst=10)
"""Matchbook's events group allows 700 requests a minute per account or IP;
five a second, shared process-wide, stays well inside it."""

NAVIGATION_LIMITER = RateLimiter(1.0, burst=3)
"""The navigation group (sports, the navigation tree) allows 100 a minute."""

SESSION_REFRESH_AFTER = 5 * 3600.0
"""A session lasts about six hours; one older than this is replaced by a fresh
login before the next call rather than reused until it fails."""

ODDS_LADDER: tuple[Decimal, ...] = CLASSIC_LADDER
"""Every decimal price Matchbook's exchange ladder has, 1.01 to 1000 in the
same bands as Betfair's CLASSIC ladder (350 ticks). Offers are placed on it;
mirrored prices on binary markets can sit between its ticks."""

PRICE_DEPTH = 3
"""Price levels a side read by default, Matchbook's own default."""

EVENT_PAGE = 20
"""Matchbook events read per listing request, each with every market and its
prices: a football fixture carries forty or so markets."""

EVENTS_PER_REQUEST = 20
"""Events one by-id request asks for (`ids=`), so a page holds them all."""

MAX_PAGES = 20
"""Most event pages one listing call reads while filling a page."""

OPEN_STATES = "open,suspended"
ALL_STATES = "open,suspended,closed,graded"
"""Matchbook event and market states. A closed or graded event is listed
without its markets; those are read by id."""

RUNNER_STATES = "open,suspended,closed,paid"
"""Runner states, for reading the runners of a closed market (Matchbook lists
only open and suspended ones by default)."""

DEFAULT_COMMISSION = 0.02
"""Matchbook's commission on net market winnings for UK, Irish, Channel
Islands and Isle of Man customers; 4% elsewhere (pass `commission=0.04`)."""

PRICE_PARAMS = {"odds-type": "DECIMAL", "exchange-type": "back-lay"}
"""Prices as decimal odds on a back/lay book, whatever the account prefers."""

TAG_ORDER = ("SPORT", "COMPETITION", "COUNTRY", "LOCATION")
"""Which of an event's meta-tags become Synpath tags, in this order. DATE and
OTHER tags ("Live Betting", "October 24th 2026") are left out."""


# ---------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------

def runner_native_id(event_id: Any, market_id: Any, runner_id: Any) -> str:
    """`<event id>-<market id>-<runner id>`."""
    return f"{event_id}-{market_id}-{runner_id}"


def market_native_id(event_id: Any, market_id: Any) -> str:
    """`<event id>-<market id>`: a Matchbook market, the Synpath event."""
    return f"{event_id}-{market_id}"


def _parts(native: str) -> list[str]:
    parts = native.split("-")
    if not all(part.isdigit() for part in parts):
        raise BadRequest(f"{VENUE}: {native!r} is not a Matchbook id")
    return parts


def parse_runner_id(native: str) -> tuple[str, str, str]:
    """(event id, market id, runner id) from a runner's native id."""
    parts = native.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise BadRequest(
            f"{VENUE}: {native!r} is not a runner id; expected '<event id>-<market id>-<runner id>' "
            f"(a '<event id>-<market id>' pair names an event)"
        )
    return parts[0], parts[1], parts[2]


def parse_market_id(native: str) -> tuple[str, str]:
    """(event id, market id) from a runner id or a market pair."""
    parts = _parts(native)
    if len(parts) not in (2, 3):
        raise BadRequest(f"{VENUE}: {native!r} is not a Matchbook market; expected '<event id>-<market id>'")
    return parts[0], parts[1]


# ---------------------------------------------------------------------------
# Pure normalizers
# ---------------------------------------------------------------------------

def iso_ms(value: Any) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def ladder_levels(prices: Any, side: str) -> list[OrderLevel]:
    """One side's price rows as YES levels: price 1/O, size in contracts."""
    out = []
    for row in prices or []:
        if not row or row.get("side") != side:
            continue
        odds, amount = row.get("decimal-odds") or row.get("odds"), row.get("available-amount")
        if not odds or not amount or float(odds) <= 1 or float(amount) <= 0:
            continue
        out.append(OrderLevel(price=yes_price(odds), size=contracts(amount, odds)))
    return out


def raw_levels(prices: Any, side: str) -> list[list[float]]:
    """The venue's own [odds, available amount] pairs on one side."""
    return [[row.get("decimal-odds") or row.get("odds"), row.get("available-amount")]
            for row in prices or [] if row and row.get("side") == side]


def price_currency(prices: Any) -> str | None:
    return next((str(row["currency"]) for row in prices or [] if row and row.get("currency")), None)


def quotes_of(runner: dict[str, Any] | None) -> tuple[Quote, Quote]:
    """YES and NO top of book from a runner's prices. Matchbook publishes no
    last traded price, so `last` stays empty."""
    prices = (runner or {}).get("prices")
    bids, asks = ladder_levels(prices, "lay"), ladder_levels(prices, "back")
    bid = max(bids, key=lambda level: level.price, default=None)
    ask = min(asks, key=lambda level: level.price, default=None)
    mid = round((bid.price + ask.price) / 2, 10) if bid and ask else None
    yes = Quote(bid=bid.price if bid else None, bid_size=bid.size if bid else None,
                ask=ask.price if ask else None, ask_size=ask.size if ask else None, mid=mid)
    no = Quote(bid=round(1 - ask.price, 10) if ask else None, bid_size=ask.size if ask else None,
               ask=round(1 - bid.price, 10) if bid else None, ask_size=bid.size if bid else None,
               mid=round(1 - mid, 10) if mid is not None else None)
    return yes, no


def status_of(
    event_status: str | None, market_status: str | None, runner_status: str | None = None,
    withdrawn: bool = False,
) -> tuple[str, bool]:
    """(normalized status, accepting orders) from the event's, market's and
    runner's statuses.

    A suspended market or runner is still open: Matchbook suspends for a goal
    or the off of a race and reopens, which is a pause, not a change of
    lifecycle. A runner withdrawn (a non-runner, `paid` once its bets are
    voided) is settled though its market trades on. A closed market has
    stopped trading; Matchbook does not publish its result."""
    if withdrawn or runner_status == "paid":
        return "settled", False
    if "graded" in (event_status, market_status):
        return "settled", False
    if "closed" in (event_status, market_status, runner_status):
        return "closed", False
    if "suspended" in (event_status, market_status, runner_status):
        return "open", False
    if market_status == "open" and runner_status in (None, "open"):
        return "open", True
    return "open", False


def result_of(runner: dict[str, Any]) -> str | None:
    """`void` for a withdrawn runner; Matchbook publishes no other result."""
    return "void" if runner.get("withdrawn") else None


def plain_line(market: dict[str, Any]) -> bool:
    """Whether every bet on the market either wins or loses. An Asian line
    (`-0.5/1.0`) can half-win, and a whole line (a handicap of -1, a total of
    2) can push and be refunded, so neither is a plain yes/no contract."""
    if market.get("asian-handicap"):
        return False
    line = market.get("handicap")
    if line is None or market.get("type") != "binary":
        return True
    return (Decimal(str(line)) * 2) % 2 == 1


def _tags(event: dict[str, Any]) -> list[str]:
    tags = [t for t in event.get("meta-tags") or [] if isinstance(t, dict) and t.get("type") in TAG_ORDER]
    tags.sort(key=lambda t: TAG_ORDER.index(t["type"]))
    seen: dict[str, None] = {}
    for tag in tags:
        name = str(tag.get("name") or "").strip()
        if name:
            seen.setdefault(name.lower(), None)
    return list(seen)


def _event_info(event: dict[str, Any] | None) -> dict[str, Any] | None:
    """The Matchbook event without its markets."""
    if not event:
        return None
    return {key: value for key, value in event.items() if key != "markets"}


def _close(market: dict[str, Any]) -> int | None:
    """When trading is expected to stop: the start, for a market that does not
    turn in-play. One that bets in-play has no known close until it closes."""
    if market.get("allow-live-betting"):
        return None
    return iso_ms(market.get("start"))


def normalize_market(
    event: dict[str, Any] | None, market: dict[str, Any], runner: dict[str, Any], *, currency: str | None = None,
) -> Market:
    """One runner of a Matchbook market as a binary market: YES is "this
    runner's bet wins"."""
    event = event or {}
    event_id = str(market.get("event-id") or event.get("id"))
    market_id, runner_id = str(market["id"]), str(runner["id"])
    native = runner_native_id(event_id, market_id, runner_id)
    event_status, market_status, runner_status = event.get("status"), market.get("status"), runner.get("status")
    withdrawn = bool(runner.get("withdrawn"))
    status, accepting = status_of(event_status, market_status, runner_status, withdrawn)
    yes_quote, no_quote = quotes_of(runner)
    name = str(runner.get("name") or runner_id)
    close = _close(market)
    tags = _tags(event)
    matched = runner.get("volume")
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, market_native_id(event_id, market_id)),
        title=" — ".join(str(part) for part in (event.get("name"), market.get("name"), name) if part),
        description=None,
        slug=None,
        yes=Outcome(label=name, quote=yes_quote),
        no=Outcome(label=f"Not {name}", quote=no_quote),
        status=status,  # type: ignore[arg-type]
        native_status=f"{market_status}/{runner_status}" if market_status or runner_status else None,
        active=accepting,
        market_type="binary" if plain_line(market) else "unknown",
        open_timestamp=None,
        open_datetime=None,
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=None,
        resolution_datetime=None,
        # The ladder is in odds, so its step in YES price varies along it.
        tick_size=None,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(
            volume_total=float(matched) if matched else None,
            volume_unit="collateral" if matched else None,
        ),
        url=None,
        image_url=None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=name,
        neg_risk=False,
        settlement_sources=[],
        info={
            "event_id": event_id,
            "market_id": market_id,
            "runner_id": runner_id,
            "runner": {key: value for key, value in runner.items() if key != "prices"},
            "market": {key: value for key, value in market.items() if key != "runners"},
            "matchbook_event": _event_info(event),
            "event_status": event_status,
            "market_status": market_status,
            "runner_status": runner_status,
            "withdrawn": withdrawn,
            "result": result_of(runner),
            "in_play": bool(market.get("live")),
            "allow_live_betting": market.get("allow-live-betting"),
            "market_type": market.get("market-type"),
            "line": runner.get("handicap", market.get("handicap")),
            "asian_handicap": runner.get("asian-handicap") or market.get("asian-handicap"),
            "number_of_winners": market.get("number-of-winners"),
            "market_volume": market.get("volume"),
            "last_price_update": iso_ms(runner.get("last-price-update-time")),
            "start": iso_ms(market.get("start") or event.get("start")),
            "currency": price_currency(runner.get("prices")) or currency,
        },
    )


def normalize_event(event: dict[str, Any] | None, market: dict[str, Any], *, currency: str | None = None) -> Event:
    """A Matchbook market with its runners: the grouping of runner markets."""
    event = event or {}
    event_id = str(market.get("event-id") or event.get("id"))
    market_id = str(market["id"])
    markets = [normalize_market(event, market, runner, currency=currency) for runner in market.get("runners") or []]
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "unopened", "closed", "settled") if s in statuses), None)
    if status is None:
        status, _ = status_of(event.get("status"), market.get("status"))
    winners = market.get("number-of-winners")
    close = _close(market)
    tags = _tags(event)
    return Event(
        id=ids.qualify(VENUE, market_native_id(event_id, market_id)),
        venue=VENUE,
        venue_event_id=market_native_id(event_id, market_id),
        title=" — ".join(str(p) for p in (event.get("name"), market.get("name")) if p) or market_id,
        description=None,
        slug=None,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=market.get("status"),
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        # One winner among the runners is exclusive; a place market or a line
        # that can push is not.
        mutually_exclusive=(winners == 1) if winners is not None and plain_line(market) else None,
        close_timestamp=close,
        close_datetime=iso(close),
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            "matchbook_event": _event_info(event),
            "event_id": event_id,
            "market_type": market.get("market-type"),
            "type": market.get("type"),
            "number_of_winners": winners,
            "line": market.get("handicap"),
            "asian_handicap": market.get("asian-handicap"),
            "in_play": bool(market.get("live")),
            "allow_live_betting": market.get("allow-live-betting"),
            "volume": market.get("volume"),
            "back_overround": market.get("back-overround"),
            "lay_overround": market.get("lay-overround"),
            "currency": next((price_currency(r.get("prices")) for r in market.get("runners") or []
                              if price_currency(r.get("prices"))), currency),
        },
    )


def normalize_order_book(
    prices: Any, *, market_id: str, side: BookSide = "yes", depth: int | None = None,
    currency: str | None = None, info: dict[str, Any] | None = None,
) -> OrderBook:
    """A runner's prices read on YES: lay prices (resting backs) are bids, back
    prices (resting lays) asks, at 1/O, sizes in contracts. `side="no"`
    mirrors it."""
    bids = sorted(ladder_levels(prices, "lay"), key=lambda level: level.price, reverse=True)
    asks = sorted(ladder_levels(prices, "back"), key=lambda level: level.price)
    derived = side == "no"
    if derived:
        bids, asks = (
            [OrderLevel(price=round(1 - level.price, 10), size=level.size) for level in asks],
            [OrderLevel(price=round(1 - level.price, 10), size=level.size) for level in bids],
        )
    elif side != "yes":
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks,
        timestamp=None, datetime=None, book_model="shared_complement", derived=derived,
        # As many levels a side as were asked for (`price-depth`).
        depth_scope="top_n",
        info={
            "back": raw_levels(prices, "back"),
            "lay": raw_levels(prices, "lay"),
            "currency": price_currency(prices) or currency,
            # Binary markets include the prices mirrored from the other runner.
            "mirrored_prices": True,
            **(info or {}),
        },
    )


def fee_schedule_of(market_id: str, rate: float = DEFAULT_COMMISSION) -> FeeSchedule:
    """Commission on net winnings per Matchbook market, makers and takers alike."""
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="net_market_winnings",
        taker_rate=rate, maker_rate=rate,
        info={
            "charged": "at settlement, on the account's net winnings in the whole Matchbook market",
            "rate_source": "Matchbook's UK and Ireland rate" if rate == DEFAULT_COMMISSION else "configured",
            "not_included": "the 4% rate outside the UK, Ireland, the Channel Islands and the Isle of Man, "
                            "and volume commission on API or commercial accounts",
        },
    )


def error_messages(body: Any) -> str | None:
    """Matchbook's reasons: `{"errors": [{"messages": [...], "codes": [...]}]}`."""
    if not isinstance(body, dict):
        return None
    texts = [str(m) for error in body.get("errors") or [] if isinstance(error, dict)
             for m in error.get("messages") or []]
    return "; ".join(texts) or None


def error_codes(body: Any) -> list[str]:
    if not isinstance(body, dict):
        return []
    return [str(c) for error in body.get("errors") or [] if isinstance(error, dict)
            for c in error.get("codes") or [] if c]


def batched(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def start_s(event: dict[str, Any]) -> int:
    """An event's start in whole seconds, the unit of the listing's `after`."""
    return (iso_ms(event.get("start")) or 0) // 1000


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class MatchbookSession:
    """An account's session token, kept in memory only.

    Logs in on first use with `POST /bpapi/rest/security/session` (username
    and password); the token goes on every request as the `session-token`
    header. A session lasts about six hours, so one older than
    `SESSION_REFRESH_AFTER` is replaced before the next call, and a 401 logs
    in again once. Matchbook asks for one login per session, not one per
    request; one lock means one login per session object however many
    threads ask. An account with two-factor login is refused with a clear
    error: log in elsewhere and pass `session_token=`.
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
        self._issued = clock() if session_token else 0.0
        self._lock = threading.Lock()
        self._client = client
        self._timeout = timeout
        self.base_url = base_url.rstrip("/")
        self.account: dict[str, Any] | None = None
        """The account the login answered with (currency, balance), when this
        session logged in itself."""

    def __repr__(self) -> str:
        return f"MatchbookSession(username={self.username!r}, token={'set' if self._token else 'none'})"

    def token(self) -> str:
        with self._lock:
            if not self._token or self._clock() - self._issued > SESSION_REFRESH_AFTER:
                self._login()
            assert self._token
            return self._token

    def headers(self) -> dict[str, str]:
        return {"session-token": self.token()}

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
                f"{VENUE}: no session; set MATCHBOOK_USERNAME and MATCHBOOK_PASSWORD, or pass session_token= "
                f"(market data needs neither)"
            )
        try:
            response = self._http().post(
                f"{self.base_url}{SESSION_PATH}", json={"username": self.username, "password": self._password},
                headers={"Accept": "application/json", "Content-Type": "application/json", "User-Agent": user_agent()},
            )
        except httpx.HTTPError as exc:
            from .errors import NetworkError

            raise NetworkError(f"{VENUE}: login unreachable: {type(exc).__name__}") from None
        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code == 429:
            raise RateLimitExceeded(f"{VENUE}: too many logins; Matchbook blocks for ten minutes past its limit")
        if "LOGIN_MFA_REQUIRED" in error_codes(body):
            raise AuthenticationError(
                f"{VENUE}: the account has two-factor login, which this library does not complete. Log in "
                f"elsewhere and pass session_token="
            )
        if response.status_code >= 400:
            reason = error_messages(body) or str(response.status_code)
            raise AuthenticationError(f"{VENUE}: login refused: {reason}", status=response.status_code,
                                      body={"errors": (body or {}).get("errors") if isinstance(body, dict) else None})
        body = body if isinstance(body, dict) else {}
        if not body.get("session-token"):
            raise AuthenticationError(f"{VENUE}: the login answered without a session token")
        self._token, self._issued = str(body["session-token"]), self._clock()
        account = body.get("account")
        self.account = account if isinstance(account, dict) else None

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


class MatchbookAuth(httpx.Auth):
    """Puts the session token on every request and retries once with a fresh
    login when Matchbook answers 401."""

    requires_response_body = True

    def __init__(self, session: MatchbookSession):
        self.session = session

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        request.headers.update(self.session.headers())
        response = yield request
        if response.status_code == 401:
            self.session.invalidate()
            request.headers.update(self.session.headers())
            yield request


def session_from(credentials: Any = None, **overrides: Any) -> MatchbookSession | None:
    """A session from credentials, else from `MATCHBOOK_*` in the environment
    or a `.env`; `None` when no login is configured."""
    if credentials is None:
        credentials = credentials_from_env()
    if credentials is None:
        return None
    return MatchbookSession(username=credentials.username, password=credentials.password, **overrides)


def credentials_from_env() -> Any:
    import os

    from .trading.credentials import load_matchbook, read_dotenv

    merged: dict[str, str] = {}
    if os.path.exists(".env"):
        merged.update(read_dotenv(".env"))
    merged.update(os.environ)
    return load_matchbook(merged)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Matchbook(Exchange):
    """Matchbook, read-only. No account needed.

    ```python
    import synpath

    mb = synpath.Matchbook()
    markets = mb.fetch_markets(sport="Soccer", limit=10)
    book = mb.fetch_order_book(markets[0].id)
    book.info["back"]                          # the venue's own odds and stakes
    ```
    """

    id = VENUE
    name = "Matchbook"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # Twenty Matchbook events a request, every market and price of each.
        "fetch_markets_by_ids": True,
        # Matchbook lists events by start time only.
        "sort": False,
        "fetch_order_book": True,
        "fetch_order_books": True,
        # Matchbook publishes no individual trades, only volume per runner.
        "fetch_trades": False,
        "fetch_ohlcv": False,
        "fetch_series": False,
        "fetch_fee_schedule": True,
        # The events API filters by sport, tag and id; it has no text search.
        "search": False,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, credentials: Any = None, session: MatchbookSession | None = None, session_token: str | None = None,
        login: bool | None = None, currency: str | None = None, commission: float = DEFAULT_COMMISSION,
        depth: int = PRICE_DEPTH, timeout: float = 30.0, limiter: RateLimiter | None = LIMITER,
        navigation_limiter: RateLimiter | None = NAVIGATION_LIMITER, client: Any = None,
        session_client: httpx.Client | None = None, base_url: str = API_URL,
    ):
        """No argument is needed: market data is public, and nothing is sent
        until the first call. `credentials`, `session_token` or `login=True`
        (`MATCHBOOK_*` from the environment) send a session on every read,
        which counts those reads against the account. `currency` asks for
        prices in USD, EUR, GBP, AUD, CAD or HKD instead of the region's
        default; `depth` is the price levels read a side; `commission` the
        rate fee schedules report."""
        overrides: dict[str, Any] = {"client": session_client, "timeout": timeout, "base_url": base_url}
        if session is None and session_token:
            session = MatchbookSession(session_token=session_token, **overrides)
        if session is None and (credentials is not None or login):
            session = session_from(credentials, **overrides)
        self.session = session
        self.currency = currency
        self.commission = commission
        self.depth = depth
        if client is None:
            client = httpx.Client(timeout=timeout, follow_redirects=True)
        client.headers["User-Agent"] = user_agent()
        client.headers["Accept"] = "application/json"
        if self.session is not None:
            client.auth = MatchbookAuth(self.session)
        self.http = HttpClient(base_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.navigation_http = HttpClient(base_url, limiter=navigation_limiter, timeout=timeout, client=client,
                                          venue=VENUE)

    def _get(self, path: str, params: Any = None, *, navigation: bool = False) -> Any:
        try:
            return (self.navigation_http if navigation else self.http).get(f"{EDGE}{path}", params)
        except ExchangeError as exc:
            reason = error_messages(exc.body)
            if reason:
                raise type(exc)(f"{VENUE}: {reason}", body=exc.body, status=exc.status) from None
            raise

    def _price_params(self, depth: int | None = None) -> dict[str, Any]:
        return {**PRICE_PARAMS, "include-prices": "true", "price-depth": depth or self.depth,
                "currency": self.currency}

    # -- raw reads ----------------------------------------------------------

    def fetch_sports(self) -> list[dict[str, Any]]:
        """Matchbook's sports, active ones: `[{"name": "Soccer", "id": 15, ...}]`."""
        out: list[dict[str, Any]] = []
        for _ in range(10):
            body = self._get("/lookups/sports", {"offset": len(out), "per-page": 100, "order": "name asc"},
                             navigation=True) or {}
            rows = [s for s in body.get("sports") or [] if isinstance(s, dict)]
            out += rows
            if not rows or len(out) >= int(body.get("total") or 0):
                break
        return out

    def fetch_matchbook_events(self, event_ids: list[str], *, depth: int | None = None) -> dict[str, dict[str, Any]]:
        """Matchbook events by id with their markets and prices, open or closed
        (a closed one comes without markets)."""
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(event_ids)), EVENTS_PER_REQUEST):
            body = self._get("/events", {
                "ids": ",".join(chunk), "states": ALL_STATES, "per-page": len(chunk), **self._price_params(depth),
            }) or {}
            for event in body.get("events") or []:
                out[str(event["id"])] = event
        return out

    def fetch_runner_prices(self, event_id: str, market_id: str, runner_id: str, *, depth: int | None = None) -> list[dict[str, Any]]:
        """One runner's prices, `[{"side": "back", "odds": 2.0, "available-amount": 20, ...}]`."""
        params = {**PRICE_PARAMS, "depth": depth or self.depth, "currency": self.currency}
        body = self._get(f"/events/{event_id}/markets/{market_id}/runners/{runner_id}/prices", params) or {}
        return list(body.get("prices") or [])

    def _closed_market(self, event_id: str, market_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """A market Matchbook no longer lists under its event (it closed), with
        its runners in every state, and the event."""
        try:
            event = self._get(f"/events/{event_id}") or {}
            market = self._get(f"/events/{event_id}/markets/{market_id}", self._price_params()) or {}
        except MarketNotFound:
            return None
        if not market.get("id"):
            return None
        if not market.get("runners"):
            body = self._get(f"/events/{event_id}/markets/{market_id}/runners", {
                "states": RUNNER_STATES, "include-withdrawn": "true", **self._price_params(),
            }) or {}
            market = {**market, "runners": body.get("runners") or []}
        return event, market

    def _market(self, event_id: str, market_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """(event, market) for one Matchbook market, open or closed."""
        event = self.fetch_matchbook_events([event_id]).get(event_id)
        market = next((m for m in (event or {}).get("markets") or [] if str(m.get("id")) == market_id), None)
        if event is not None and market is not None:
            return event, market
        return self._closed_market(event_id, market_id)

    # -- catalog ------------------------------------------------------------

    def _sport_ids(self, names_or_ids: list[str]) -> list[str]:
        """Sport ids, resolving names ("Soccer", "horse racing") by asking the
        venue. A name it does not have is refused rather than ignored."""
        named = [n for n in names_or_ids if not n.isdigit()]
        if not named:
            return names_or_ids
        by_name = {str(s["name"]).lower(): str(s["id"]) for s in self.fetch_sports() if s.get("name")}
        unknown = [n for n in named if n.lower() not in by_name]
        if unknown:
            raise BadRequest(f"{VENUE}: no sport named {', '.join(unknown)}")
        return [n if n.isdigit() else by_name[n.lower()] for n in names_or_ids]

    def _filters(self, sport: Any, tag: Any) -> dict[str, Any]:
        def listed(value: Any) -> list[str]:
            return [str(v) for v in ([value] if isinstance(value, (str, int)) else value)]

        params: dict[str, Any] = {}
        if sport:
            params["sport-ids"] = ",".join(self._sport_ids(listed(sport)))
        if tag:
            params["tag-url-names"] = ",".join(listed(tag))
        return params

    def _walk(self, filters: dict[str, Any], cursor: str | None) -> Iterator[tuple[dict[str, Any], str, int, int, int]]:
        """Open events in start-time order from `cursor`, each with its place:
        (event, anchor, position from the anchor, first market, first runner).

        Matchbook pages its listing by offset. Offsets here count from an
        anchor, a start time in whole seconds, since `after` includes the
        events starting at it: each event is placed from its own start where
        the page shows every event before it at that start, so a change to
        the catalog earlier in the listing (an in-play match ending) cannot
        shift the place, and dozens of fixtures kicking off at the same second
        still page through."""
        anchor, position, market_skip, runner_skip = _cursor(cursor)
        for _ in range(MAX_PAGES):
            params = {**filters, "states": OPEN_STATES, "per-page": EVENT_PAGE, "offset": position,
                      "after": anchor, **self._price_params()}
            body = self._get("/events", params) or {}
            rows = [e for e in body.get("events") or [] if isinstance(e, dict) and e.get("id")]
            first_start = str(start_s(rows[0])) if rows else None
            at: dict[str, int] = {}
            for index, event in enumerate(rows):
                start = str(start_s(event))
                before = at.get(start, 0)
                at[start] = before + 1
                if start == anchor or (position and start == first_start):
                    # Events at this start may sit before the page: count from the anchor.
                    place = (anchor or "", position + index)
                else:
                    place = (start, before)
                first = index == 0
                yield event, place[0], place[1], market_skip if first else 0, runner_skip if first else 0
            if len(rows) < EVENT_PAGE:
                return
            last = str(start_s(rows[-1]))
            if last != anchor and (last != first_start or not position):
                anchor, position = last, at[last]
            else:
                # The page is all one start that may have begun before it: keep counting.
                position += len(rows)
            market_skip = runner_skip = 0

    def _markets_of(self, event: dict[str, Any], market_type: Any) -> list[dict[str, Any]]:
        types = {market_type} if isinstance(market_type, str) else set(market_type or [])
        return [m for m in event.get("markets") or [] if isinstance(m, dict) and m.get("id")
                and (not types or m.get("market-type") in types)]

    def _listing_args(self, status: str, query: str | None, sort: str | None) -> None:
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: Matchbook lists the markets of open events only; read a closed one by id")
        if query:
            raise NotSupported(f"{VENUE}: Matchbook has no text search; filter by sport= or tag=")
        check_sort(sort, venue=VENUE, supported=False)

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sort: str | None = None, sport: str | list[str] | None = None,
        tag: str | list[str] | None = None, market_type: str | list[str] | None = None,
    ) -> Page[Market]:
        """One page of runners (each a market), events soonest first, each
        event's markets and runners in Matchbook's order, with their prices.
        `sport` takes ids or names ("Soccer"), `tag` Matchbook's tag url
        names (`england-premier-league`), `market_type` its market types
        (`one_x_two`, `total`). Withdrawn runners are left out. Only open
        events are listed; `fetch_market` reads a closed runner by id."""
        self._listing_args(status, query, sort)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        picked: list[Market] = []
        for event, anchor, position, first_market, first_runner in self._walk(self._filters(sport, tag), cursor):
            markets = self._markets_of(event, market_type)
            for offset, market in enumerate(markets[first_market:], start=first_market):
                runners = [r for r in market.get("runners") or [] if not r.get("withdrawn")]
                skip = first_runner if offset == first_market else 0
                rows = runners[skip:]
                room = wanted - len(picked)
                picked += [normalize_market(event, market, r, currency=self.currency) for r in rows[:room]]
                if len(rows) > room:
                    return Page(picked, next_cursor=f"{anchor}|{position}|{offset}|{skip + room}")
                if len(picked) >= wanted:
                    following = offset + 1 < len(markets)
                    cursor_ = f"{anchor}|{position}|{offset + 1}|0" if following else f"{anchor}|{position + 1}|0|0"
                    return Page(picked, next_cursor=cursor_)
        return Page(picked, next_cursor=None)

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sport: str | list[str] | None = None, tag: str | list[str] | None = None,
        market_type: str | list[str] | None = None,
    ) -> Page[Event]:
        """One page of Matchbook markets (each a Synpath event) with their
        runners priced. Filters as `fetch_markets`."""
        self._listing_args(status, query, None)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        picked: list[Event] = []
        for event, anchor, position, first_market, _ in self._walk(self._filters(sport, tag), cursor):
            markets = self._markets_of(event, market_type)[first_market:]
            room = wanted - len(picked)
            picked += [normalize_event(event, market, currency=self.currency) for market in markets[:room]]
            if len(markets) > room:
                return Page(picked, next_cursor=f"{anchor}|{position}|{first_market + room}|0")
            if len(picked) >= wanted:
                return Page(picked, next_cursor=f"{anchor}|{position + 1}|0|0")
        return Page(picked, next_cursor=None)

    def fetch_event(self, event_id: str) -> Event:
        """One Matchbook market with its runners, by
        `matchbook:<event id>-<market id>`, open or closed."""
        event_native, market_native = parse_market_id(self.native(event_id))
        found = self._market(event_native, market_native)
        if found is None:
            raise MarketNotFound(f"{VENUE}: no market {market_native_id(event_native, market_native)}")
        return normalize_event(found[0], found[1], currency=self.currency)

    def fetch_market(self, market_id: str) -> Market:
        """One runner by id, open or closed."""
        found = self.fetch_markets_by_ids([market_id])
        if not found:
            raise MarketNotFound(f"{VENUE}: no runner {self.native(market_id)}")
        return found[0]

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Runners in the order asked for: twenty Matchbook events a request,
        with every market and price, then one read per closed market. Ids
        Matchbook does not know are left out."""
        wanted = list(dict.fromkeys(parse_runner_id(self.native(m)) for m in market_ids))
        events = self.fetch_matchbook_events([e for e, _, _ in wanted])
        closed: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]] | None] = {}
        out: list[Market] = []
        for event_id, market_id, runner_id in wanted:
            event = events.get(event_id)
            market = next((m for m in (event or {}).get("markets") or [] if str(m.get("id")) == market_id), None)
            if market is None:
                if event_id not in events:
                    continue
                if (event_id, market_id) not in closed:
                    closed[(event_id, market_id)] = self._closed_market(event_id, market_id)
                found = closed[(event_id, market_id)]
                if found is None:
                    continue
                event, market = found
            runner = next((r for r in market.get("runners") or [] if str(r.get("id")) == runner_id), None)
            if runner is not None:
                out.append(normalize_market(event, market, runner, currency=self.currency))
        return out

    # -- market data --------------------------------------------------------

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        """A runner's prices on YES, `depth` levels a side (the adapter's
        `depth`, three by default), from its prices alone: one small read."""
        event_id, market, runner = parse_runner_id(self.native(market_id))
        try:
            prices = self.fetch_runner_prices(event_id, market, runner, depth=depth)
        except MarketNotFound:
            raise MarketNotFound(f"{VENUE}: no runner {self.native(market_id)}") from None
        synpath_id = self.qualify(runner_native_id(event_id, market, runner))
        return normalize_order_book(prices, market_id=synpath_id, side=side, depth=depth, currency=self.currency)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many runners, twenty Matchbook events a request. A
        `<event id>-<market id>` pair (an event) gives every runner in it.
        Runners Matchbook does not list (closed markets) are left out."""
        natives = [self.native(m) for m in dict.fromkeys(market_ids)]
        events = self.fetch_matchbook_events([_parts(n)[0] for n in natives], depth=depth)
        out: dict[str, OrderBook] = {}
        for native in natives:
            parts = _parts(native)
            if len(parts) not in (2, 3):
                raise BadRequest(f"{VENUE}: {native!r} is not a runner or market id")
            event = events.get(parts[0]) or {}
            market = next((m for m in event.get("markets") or [] if str(m.get("id")) == parts[1]), None)
            if market is None:
                continue
            for runner in market.get("runners") or []:
                if len(parts) == 3 and str(runner.get("id")) != parts[2]:
                    continue
                synpath_id = self.qualify(runner_native_id(parts[0], parts[1], runner["id"]))
                status, _ = status_of(event.get("status"), market.get("status"), runner.get("status"),
                                      bool(runner.get("withdrawn")))
                out[synpath_id] = normalize_order_book(
                    runner.get("prices"), market_id=synpath_id, side=side, depth=depth, currency=self.currency,
                    info={"market_status": market.get("status"), "runner_status": runner.get("status"),
                          "status": status, "in_play": bool(market.get("live")),
                          "last_price_update": iso_ms(runner.get("last-price-update-time"))},
                )
        return out

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> list:
        raise NotSupported(
            f"{VENUE}: Matchbook publishes no individual trades, only the volume matched per runner "
            f"(in a market's stats)"
        )

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """Commission on net winnings per Matchbook market. Matchbook publishes
        no per-market rate, so this reads nothing: the rate is the adapter's
        `commission` (2%, the UK and Ireland rate). A runner id or a market
        pair both name the market."""
        event_id, market = parse_market_id(self.native(market_id))
        return fee_schedule_of(market_native_id(event_id, market), self.commission)

    def close(self) -> None:
        self.http.close()
        if self.session is not None:
            self.session.close()


def _cursor(cursor: str | None) -> tuple[str | None, int, int, int]:
    """`anchor|position|market|runner`: the start second the walk resumes from
    (empty for the start), the event's position from it, and the first market
    and runner of that event to read."""
    if not cursor:
        return None, 0, 0, 0
    try:
        anchor, position, market, runner = cursor.split("|")
        if anchor and not anchor.isdigit():
            raise ValueError
        return anchor or None, max(0, int(position)), max(0, int(market)), max(0, int(runner))
    except ValueError:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued") from None

