"""Betfair Exchange: a back/lay betting exchange, read from its Betting API.

Every call needs an account. Betfair has no anonymous market data: each request
carries the account's application key (`X-Application`) and a session token
(`X-Authentication`) from a login. This adapter logs in on the first call and
keeps the token in memory only. See `BetfairSession`.

**A delayed application key is what this adapter is built for.** Every account
gets one for free; it serves the same market data as a live key, 1 to 180
seconds late, three price levels deep, without the full ladder or the traded
volume per selection. Every book and quote read here says so in its `info`
(`delayed: true`). A live key (paid, for betting) changes none of the code.

Five things about this venue that shape the adapter:

**A Betfair market is many runners, and each runner is a Synpath market.**
"Match Odds" on Arsenal v Chelsea has three runners (selections): Arsenal,
Chelsea, the Draw. Each one answers its own yes/no question ("does this runner
win?") and is a Synpath market, `betfair:1.234567890-47972` (market id,
selection id, and the handicap after a second dash when it is not zero). The
Betfair market with all its runners is the Synpath event, `betfair:1.234567890`,
the way a Polymarket neg-risk event holds one market per candidate and an
Opinion multi topic one per option. The Betfair event (the fixture) is the
parent of that grouping and rides in `info["betfair_event"]`: one fixture holds
dozens of Betfair markets (match odds, totals, correct score), which are
separate questions, not outcomes of one.

**Back and lay, read as YES.** Backing a runner at decimal odds O is buying its
YES at 1/O; laying it is selling YES at 1/O. Betfair names its book after what a
*taker* can do: `availableToBack` lists the prices a taker can back at, which
are resting lay orders, so on YES they are **asks** at 1/O; `availableToLay`
lists the prices a taker can lay at, resting back orders, so they are **bids**.
The best back price (the highest odds) is always below the best lay price,
which is what keeps the YES bid under the YES ask.

**Sizes are stakes in the account's currency.** A back stake S at odds O wins
S·(O-1) and returns S·O, so it is S·O contracts that pay 1 each, costing S.
Every level is converted to contracts the same way (stake times odds), and the
venue's own odds and stakes stay in the book's `info` (`back`, `lay`), so no
price is ever invented off Betfair's ladder.

**No public trades.** Betfair publishes how much has matched at each price and
the last traded price, never individual trades, so `fetch_trades` and candles
are not offered rather than reconstructed.

**Commission on net market winnings.** Betfair charges a percentage (the
market's base rate, 5% on most .com markets) of what an account wins on a
market net of what it lost there, at settlement, before any discount or the
Expert Fee. See `FeeSchedule.estimate` (`net_market_winnings`).

Betfair's API is refused from several countries (the US, Germany, France, the
Netherlands, Singapore, India, Turkey, the UAE and others) and its keys are for
an account holder's own betting: showing Betfair prices to anyone else needs a
data licence from Betfair. Bring your own key and account.
"""
from __future__ import annotations

import threading
import time
from decimal import Decimal
from datetime import datetime
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
)
from .errors import (
    AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported, RateLimitExceeded,
)
from .types import (
    BookSide, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote, iso,
)

VENUE = "betfair"

JURISDICTIONS: dict[str, dict[str, str]] = {
    "com": {"identity": "identitysso.betfair.com", "cert": "identitysso-cert.betfair.com", "api": "api.betfair.com"},
    "it": {"identity": "identitysso.betfair.it", "cert": "identitysso-cert.betfair.it", "api": "api.betfair.it"},
    "es": {"identity": "identitysso.betfair.es", "cert": "identitysso-cert.betfair.es", "api": "api.betfair.es"},
    "ro": {"identity": "identitysso.betfair.ro", "cert": "identitysso-cert.betfair.ro", "api": "api.betfair.ro"},
    # Australian and New Zealand accounts log in on their own host and bet
    # through the global API.
    "com.au": {"identity": "identitysso.betfair.com.au", "cert": "identitysso-cert.betfair.com.au",
               "api": "api.betfair.com"},
}
"""Where an account logs in and calls the API, by the Betfair site it is
registered on. An Italian account cannot log in on .com, and so on."""

BETTING_PATH = "/exchange/betting/rest/v1.0"

LIMITER = RateLimiter(5.0, burst=10)
"""Betfair publishes no request rate for market data, only a weight per
request; kept modest and shared process-wide."""

SESSION_REFRESH_AFTER = 4 * 3600.0
"""Seconds after which the session is extended with `keepAlive` before the
next call. A session lasts 12 hours (24 on UK and Irish accounts) from its
last keepAlive, and API calls do not extend it, so a long-running process
has to."""

MAX_WEIGHT = 200
"""A request's projection weight times its number of markets may not pass
this, or Betfair refuses the whole request with `TOO_MUCH_DATA`."""

CATALOGUE_WEIGHTS: dict[str, int] = {
    "COMPETITION": 0, "EVENT": 0, "EVENT_TYPE": 0, "MARKET_START_TIME": 0, "RUNNER_DESCRIPTION": 0,
    "MARKET_DESCRIPTION": 1, "RUNNER_METADATA": 1,
}
BOOK_WEIGHTS: dict[str, int] = {"EX_BEST_OFFERS": 5, "EX_TRADED": 17, "EX_ALL_OFFERS": 17}
BOOK_COMBINED_WEIGHTS: dict[frozenset[str], int] = {
    frozenset({"EX_BEST_OFFERS", "EX_TRADED"}): 20,
    frozenset({"EX_ALL_OFFERS", "EX_TRADED"}): 32,
}
"""Betfair's weights for the pairs it prices below their sum."""
BOOK_BASE_WEIGHT = 2
"""`listMarketBook` with no price projection still weighs 2 a market."""

LISTING_PROJECTION = ("EVENT", "EVENT_TYPE", "COMPETITION", "MARKET_START_TIME", "MARKET_DESCRIPTION",
                      "RUNNER_DESCRIPTION")
LOOKUP_PROJECTION = LISTING_PROJECTION + ("RUNNER_METADATA",)
BOOK_PRICE_DATA = ("EX_BEST_OFFERS",)
"""The only price projection a delayed key serves in full: three levels a side."""

MAX_CATALOGUE_RESULTS = 1000
"""`listMarketCatalogue` returns at most this many markets, weight permitting."""

MAX_PAGES = 50
"""Most catalogue requests one listing call makes while filling a page."""

DEFAULT_COMMISSION = 0.05
"""The Market Base Rate on most .com markets, used when a market's own
`marketBaseRate` was not read."""

SORTS = {"volume": "MAXIMUM_TRADED", "liquidity": "MAXIMUM_AVAILABLE"}
"""Betfair sorts its catalogue by matched or available money; it has no
listing time to sort by."""

SESSION_ERRORS = frozenset({"INVALID_SESSION_INFORMATION", "NO_SESSION"})
AUTH_ERRORS = SESSION_ERRORS | {"INVALID_APP_KEY", "NO_APP_KEY", "ACCESS_DENIED", "APP_KEY_CREATION_FAILED"}
RATE_ERRORS = frozenset({"TOO_MANY_REQUESTS"})

CLASSIC_LADDER_BANDS: tuple[tuple[str, str, str], ...] = (
    ("1.01", "2", "0.01"), ("2", "3", "0.02"), ("3", "4", "0.05"), ("4", "6", "0.1"), ("6", "10", "0.2"),
    ("10", "20", "0.5"), ("20", "30", "1"), ("30", "50", "2"), ("50", "100", "5"), ("100", "1000", "10"),
)
"""Betfair's CLASSIC odds ladder: from each band's start to its end, in its step."""


def _classic_ladder() -> tuple[Decimal, ...]:
    ticks: list[Decimal] = []
    for start, end, step in CLASSIC_LADDER_BANDS:
        price, stop, tick = Decimal(start), Decimal(end), Decimal(step)
        while price < stop:
            ticks.append(price)
            price += tick
    ticks.append(Decimal("1000"))
    return tuple(ticks)


CLASSIC_LADDER: tuple[Decimal, ...] = _classic_ladder()
"""Every price Betfair accepts on a CLASSIC market, 1.01 to 1000 (350 ticks)."""
_LADDER_SET = frozenset(CLASSIC_LADDER)


def on_ladder(odds: Any) -> bool:
    """Whether `odds` is a price Betfair's CLASSIC ladder has."""
    try:
        return Decimal(str(odds)) in _LADDER_SET
    except ArithmeticError:
        return False


# ---------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------

def handicap_text(handicap: Any) -> str:
    """A handicap as it appears in an id: `-1.5`, `2`, `0.25`."""
    value = Decimal(str(handicap or 0)).normalize()
    return format(value, "f")


def runner_native_id(market_id: str, selection_id: Any, handicap: Any = 0) -> str:
    """`1.234567890-47972`, or `1.234567890-47972--1.5` for a handicap runner."""
    native = f"{market_id}-{int(selection_id)}"
    if handicap not in (None, 0, 0.0) and Decimal(str(handicap)) != 0:
        native += f"-{handicap_text(handicap)}"
    return native


def parse_runner_id(native: str) -> tuple[str, int, float]:
    """(Betfair market id, selection id, handicap) from a runner's native id.
    The market id is `<exchange>.<number>` and contains no dash, so the first
    dash ends it; a second dash starts the handicap, which may be negative."""
    market_id, sep, rest = native.partition("-")
    if not sep or "." not in market_id:
        raise BadRequest(
            f"{VENUE}: {native!r} is not a runner id; expected '<market id>-<selection id>', "
            f"e.g. betfair:1.234567890-47972 (a bare market id names an event)"
        )
    selection, sep, handicap = rest.partition("-")
    try:
        return market_id, int(selection), float(handicap) if sep else 0.0
    except ValueError:
        raise BadRequest(f"{VENUE}: {native!r} is not a runner id") from None


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


def yes_price(odds: Any) -> float:
    """Decimal odds as the YES price they imply, 1/O."""
    return round(1 / float(odds), 10)


def contracts(stake: Any, odds: Any) -> float:
    """A stake at decimal odds as contracts paying 1 each: stake times odds."""
    return round(float(stake) * float(odds), 6)


def ladder_levels(rows: Any) -> list[OrderLevel]:
    """Betfair price/size rows as YES levels: price 1/O, size in contracts."""
    out = []
    for row in rows or []:
        odds, stake = (row or {}).get("price"), (row or {}).get("size")
        if not odds or not stake or float(odds) <= 1 or float(stake) <= 0:
            continue
        out.append(OrderLevel(price=yes_price(odds), size=contracts(stake, odds)))
    return out


def raw_levels(rows: Any) -> list[list[float]]:
    """The venue's own [odds, stake] pairs, kept beside the converted levels."""
    return [[row["price"], row["size"]] for row in rows or [] if row and row.get("price") is not None]


RESULTS: dict[str, str] = {"WINNER": "yes", "LOSER": "no", "PLACED": "yes", "REMOVED": "void"}
"""A runner's final status as the YES result. A removed runner (a non-runner)
has its bets voided. PLACED is the place leg of an each-way market."""


def status_of(market_status: str | None, runner_status: str | None) -> tuple[str, bool]:
    """(normalized status, accepting orders) from the market's and runner's statuses.

    A suspended market is still open: Betfair suspends for a goal, a red card
    or the start of a race and reopens minutes later, which is a halt, not a
    change of lifecycle. A runner removed from a live market is settled (void)
    though its market trades on."""
    if runner_status in ("REMOVED", "WINNER", "LOSER", "PLACED"):
        return "settled", False
    if market_status == "CLOSED":
        return ("settled" if runner_status in RESULTS else "closed"), False
    if market_status == "INACTIVE":
        return "unopened", False
    if market_status == "SUSPENDED" or runner_status == "HIDDEN":
        return "open", False
    if market_status == "OPEN":
        return "open", True
    return "open", False


def runner_book_of(book: dict[str, Any] | None, selection_id: int, handicap: float) -> dict[str, Any] | None:
    for runner in (book or {}).get("runners") or []:
        if int(runner.get("selectionId", -1)) == selection_id and float(runner.get("handicap") or 0) == handicap:
            return runner
    return None


def quotes_of(runner_book: dict[str, Any] | None) -> tuple[Quote, Quote]:
    """YES and NO top of book from a runner's best offers. The last traded
    price has no time on Betfair, so it stays in `info` rather than `last`."""
    ex = (runner_book or {}).get("ex") or {}
    bids, asks = ladder_levels(ex.get("availableToLay")), ladder_levels(ex.get("availableToBack"))
    bid = max(bids, key=lambda level: level.price, default=None)
    ask = min(asks, key=lambda level: level.price, default=None)
    mid = round((bid.price + ask.price) / 2, 10) if bid and ask else None
    yes = Quote(bid=bid.price if bid else None, bid_size=bid.size if bid else None,
                ask=ask.price if ask else None, ask_size=ask.size if ask else None, mid=mid)
    no = Quote(bid=round(1 - ask.price, 10) if ask else None, bid_size=ask.size if ask else None,
               ask=round(1 - bid.price, 10) if bid else None, ask_size=bid.size if bid else None,
               mid=round(1 - mid, 10) if mid is not None else None)
    return yes, no


def _lower_tags(*names: Any) -> list[str]:
    seen: dict[str, None] = {}
    for name in names:
        if name and str(name).strip():
            seen.setdefault(str(name).strip().lower(), None)
    return list(seen)


def normalize_market(
    row: dict[str, Any], runner: dict[str, Any], book: dict[str, Any] | None = None, *,
    delayed: bool = True, currency: str | None = None,
) -> Market:
    """One runner of a catalogue row (with its market's book, when read) as a
    binary market: YES is "this runner's bet wins"."""
    market_id = str(row["marketId"])
    selection_id, handicap = int(runner["selectionId"]), float(runner.get("handicap") or 0)
    native = runner_native_id(market_id, selection_id, handicap)
    description = row.get("description") or {}
    event, competition, event_type = row.get("event") or {}, row.get("competition") or {}, row.get("eventType") or {}
    runner_book = runner_book_of(book, selection_id, handicap)
    market_status = (book or {}).get("status")
    runner_status = (runner_book or {}).get("status")
    status, accepting = status_of(market_status, runner_status)
    yes_quote, no_quote = quotes_of(runner_book)
    name = str(runner.get("runnerName") or selection_id)
    title_parts = [event.get("name"), row.get("marketName"), name]
    close = iso_ms(description.get("suspendTime") or description.get("marketTime") or row.get("marketStartTime"))
    matched = (runner_book or {}).get("totalMatched")
    betting_type = description.get("bettingType")
    tags = _lower_tags(event_type.get("name"), competition.get("name"))
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, market_id),
        title=" — ".join(str(part) for part in title_parts if part),
        description=description.get("rules") or None,
        slug=None,
        yes=Outcome(label=name, quote=yes_quote),
        no=Outcome(label=f"Not {name}", quote=no_quote),
        status=status,  # type: ignore[arg-type]
        native_status=f"{market_status}/{runner_status}" if market_status or runner_status else None,
        active=accepting,
        # A handicap or line runner can half-win or be voided on the line, so
        # it is not a plain yes/no contract.
        market_type="binary" if betting_type in (None, "ODDS") else "unknown",
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
            "market_id": market_id,
            "selection_id": selection_id,
            "handicap": handicap,
            "runner": runner,
            "runner_book": runner_book,
            "market": {key: value for key, value in row.items() if key != "runners"},
            "market_status": market_status,
            "runner_status": runner_status,
            "in_play": (book or {}).get("inplay"),
            "bet_delay": (book or {}).get("betDelay"),
            "version": (book or {}).get("version"),
            "result": RESULTS.get(runner_status or ""),
            "last_traded_odds": (runner_book or {}).get("lastPriceTraded"),
            "betting_type": betting_type,
            "market_type": description.get("marketType"),
            "market_base_rate": description.get("marketBaseRate"),
            "price_ladder": (description.get("priceLadderDescription") or {}).get("type"),
            "market_start_time": iso_ms(row.get("marketStartTime")),
            "betfair_event": event or None,
            "competition": competition or None,
            "event_type": event_type or None,
            "delayed": bool((book or {}).get("isMarketDataDelayed", delayed)),
            "currency": currency or "account",
        },
    )


def normalize_book_only(book: dict[str, Any], runner_book: dict[str, Any], **kwargs: Any) -> Market:
    """A runner known only from its book: a closed market drops out of
    Betfair's catalogue but its book (and the result) stays readable."""
    row = {"marketId": book["marketId"], "runners": [runner_book]}
    return normalize_market(row, runner_book, book, **kwargs)


def normalize_event(row: dict[str, Any], book: dict[str, Any] | None = None, **kwargs: Any) -> Event:
    """A Betfair market with its runners: the grouping of runner markets."""
    market_id = str(row["marketId"])
    markets = [normalize_market(row, runner, book, **kwargs) for runner in row.get("runners") or []]
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "unopened", "closed", "settled") if s in statuses), "open")
    description = row.get("description") or {}
    event, competition, event_type = row.get("event") or {}, row.get("competition") or {}, row.get("eventType") or {}
    winners = (book or {}).get("numberOfWinners")
    plain = description.get("bettingType") in (None, "ODDS")
    close = iso_ms(description.get("suspendTime") or description.get("marketTime") or row.get("marketStartTime"))
    tags = _lower_tags(event_type.get("name"), competition.get("name"))
    return Event(
        id=ids.qualify(VENUE, market_id),
        venue=VENUE,
        venue_event_id=market_id,
        title=" — ".join(str(p) for p in (event.get("name"), row.get("marketName")) if p) or market_id,
        description=description.get("rules") or None,
        slug=None,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=(book or {}).get("status"),
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        # One winner among the runners is exclusive; a place market (several
        # winners) or a handicap market is not.
        mutually_exclusive=(winners == 1) if plain and winners is not None else None,
        close_timestamp=close,
        close_datetime=iso(close),
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            "betfair_event": event or None, "competition": competition or None, "event_type": event_type or None,
            "market_type": description.get("marketType"), "betting_type": description.get("bettingType"),
            "number_of_winners": winners, "market_base_rate": description.get("marketBaseRate"),
            "in_play": (book or {}).get("inplay"), "bet_delay": (book or {}).get("betDelay"),
            "version": (book or {}).get("version"), "total_matched": row.get("totalMatched"),
            "delayed": bool((book or {}).get("isMarketDataDelayed", kwargs.get("delayed", True))),
        },
    )


def normalize_order_book(
    book: dict[str, Any], runner_book: dict[str, Any], *, market_id: str, side: BookSide = "yes",
    depth: int | None = None, delayed: bool = True, currency: str | None = None,
) -> OrderBook:
    """A runner's best offers read on YES: resting backs (`availableToLay`)
    are bids, resting lays (`availableToBack`) asks, at 1/O, sizes in
    contracts. `side="no"` mirrors it."""
    ex = runner_book.get("ex") or {}
    bids = sorted(ladder_levels(ex.get("availableToLay")), key=lambda level: level.price, reverse=True)
    asks = sorted(ladder_levels(ex.get("availableToBack")), key=lambda level: level.price)
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
        # Best offers only: three levels a side on a delayed key.
        depth_scope="top_n",
        info={
            "back": raw_levels(ex.get("availableToBack")),
            "lay": raw_levels(ex.get("availableToLay")),
            "currency": currency or "account",
            "delayed": bool(book.get("isMarketDataDelayed", delayed)),
            "market_status": book.get("status"),
            "runner_status": runner_book.get("status"),
            "in_play": book.get("inplay"),
            "bet_delay": book.get("betDelay"),
            "version": book.get("version"),
            "last_traded_odds": runner_book.get("lastPriceTraded"),
            "last_match_time": iso_ms(book.get("lastMatchTime")),
        },
    )


def fee_schedule_of(market_id: str, base_rate_percent: Any = None, *, default: float = DEFAULT_COMMISSION) -> FeeSchedule:
    """Commission at the market's base rate (Betfair publishes it as a
    percentage), charged on net market winnings, makers and takers alike."""
    rate = float(base_rate_percent) / 100 if base_rate_percent is not None else default
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="net_market_winnings",
        taker_rate=rate, maker_rate=rate,
        info={
            "charged": "at settlement, on the account's net winnings in the whole Betfair market",
            "base_rate_source": "marketBaseRate" if base_rate_percent is not None else "default",
            "not_included": "the account's discount rate and Betfair's Expert Fee",
        },
    )


def request_weight(projections: tuple[str, ...] | list[str], *, book: bool) -> int:
    """What one market weighs in a request with these projections."""
    if not book:
        return sum(CATALOGUE_WEIGHTS.get(p, 0) for p in projections)
    wanted = frozenset(projections)
    if wanted in BOOK_COMBINED_WEIGHTS:
        return BOOK_COMBINED_WEIGHTS[wanted]
    return sum(BOOK_WEIGHTS.get(p, 0) for p in wanted) or BOOK_BASE_WEIGHT


def markets_per_request(weight: int, *, cap: int = MAX_CATALOGUE_RESULTS) -> int:
    """How many markets one request can carry at this weight."""
    return min(cap, MAX_WEIGHT // weight) if weight else cap


def batched(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def api_error_code(body: Any) -> str | None:
    """Betfair's own error code from a refused call:
    `{"faultcode", "faultstring", "detail": {"APINGException": {"errorCode"}}}`."""
    if not isinstance(body, dict):
        return None
    detail = body.get("detail") or {}
    exception = detail.get("APINGException") or detail.get("AccountAPINGException") if isinstance(detail, dict) else None
    if isinstance(exception, dict) and exception.get("errorCode"):
        return str(exception["errorCode"])
    return str(body["errorCode"]) if body.get("errorCode") else None


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class BetfairSession:
    """An account's application key and session token, kept in memory only.

    Logs in on first use: by certificate when `cert_path` and `key_path` are
    given (Betfair's non-interactive login, for bots), else interactively with
    the username and password. The token is never written to disk, never
    logged and never shown by `repr`. A session lasts 12 hours (24 on UK and
    Irish accounts) and API calls do not extend it, so `token()` extends it
    with `keepAlive` once it is `SESSION_REFRESH_AFTER` old, and logs in again
    if that fails. Betfair caps logins at 100 a minute; one lock means one
    login per session object however many threads ask.
    """

    def __init__(
        self, *, app_key: str, username: str | None = None, password: str | None = None,
        cert_path: str | None = None, key_path: str | None = None, jurisdiction: str = "com",
        session_token: str | None = None, client: httpx.Client | None = None, timeout: float = 30.0,
        clock: Any = time.monotonic,
    ):
        if jurisdiction not in JURISDICTIONS:
            raise BadRequest(f"{VENUE}: unknown jurisdiction {jurisdiction!r}; expected one of {', '.join(JURISDICTIONS)}")
        if not app_key:
            raise AuthenticationError(f"{VENUE}: every call needs an application key; set BETFAIR_APP_KEY")
        self.app_key = app_key
        self.username = username
        self._password = password
        self.cert_path, self.key_path = cert_path, key_path
        self.jurisdiction = jurisdiction
        self._token = session_token
        self._clock = clock
        self._issued = clock() if session_token else 0.0
        self._lock = threading.Lock()
        self._client = client
        self._timeout = timeout

    def __repr__(self) -> str:
        login = "certificate" if self.uses_certificate else "interactive"
        return f"BetfairSession(jurisdiction={self.jurisdiction!r}, login={login!r}, token={'set' if self._token else 'none'})"

    @property
    def uses_certificate(self) -> bool:
        return bool(self.cert_path and self.key_path)

    @property
    def hosts(self) -> dict[str, str]:
        return JURISDICTIONS[self.jurisdiction]

    def token(self) -> str:
        """A live session token: the cached one, extended or renewed as needed."""
        with self._lock:
            if not self._token:
                self._login()
            elif self._clock() - self._issued > SESSION_REFRESH_AFTER:
                self._extend()
            assert self._token
            return self._token

    def headers(self) -> dict[str, str]:
        return {"X-Application": self.app_key, "X-Authentication": self.token(), "Accept": "application/json"}

    def invalidate(self) -> None:
        """Forget the token, after Betfair said it is no longer valid."""
        with self._lock:
            self._token = None

    # -- identity calls ---------------------------------------------------------

    def _http(self) -> httpx.Client:
        if self._client is None:
            cert = (self.cert_path, self.key_path) if self.uses_certificate else None
            self._client = httpx.Client(timeout=self._timeout, cert=cert)  # type: ignore[arg-type]
        return self._client

    def _identity_post(self, url: str, *, headers: dict[str, str], data: dict[str, str] | None = None) -> dict[str, Any]:
        try:
            response = self._http().post(url, headers=headers, data=data)
        except httpx.HTTPError as exc:
            from .errors import NetworkError

            raise NetworkError(f"{VENUE}: login host unreachable: {type(exc).__name__}") from None
        if response.status_code == 429:
            raise RateLimitExceeded(f"{VENUE}: too many logins (Betfair allows 100 a minute)")
        if response.status_code >= 400:
            raise AuthenticationError(
                f"{VENUE}: the login host refused the request ({response.status_code}); Betfair's API is "
                f"blocked from some countries", status=response.status_code,
            )
        try:
            return response.json()
        except ValueError:
            raise AuthenticationError(f"{VENUE}: the login host did not answer in JSON") from None

    def _login(self) -> None:
        if not self.username or not self._password:
            raise AuthenticationError(
                f"{VENUE}: no session; set BETFAIR_USERNAME and BETFAIR_PASSWORD (and optionally "
                f"BETFAIR_CERT_PATH / BETFAIR_KEY_PATH for certificate login), or pass session_token="
            )
        form = {"username": self.username, "password": self._password}
        headers = {"X-Application": self.app_key, "Accept": "application/json"}
        if self.uses_certificate:
            reply = self._identity_post(f"https://{self.hosts['cert']}/api/certlogin", headers=headers, data=form)
            status, token = reply.get("loginStatus"), reply.get("sessionToken")
        else:
            reply = self._identity_post(f"https://{self.hosts['identity']}/api/login", headers=headers, data=form)
            token = reply.get("token")
            # A refused interactive login answers 200 with the reason in `error`.
            status = reply.get("status") if reply.get("status") == "SUCCESS" else (reply.get("error") or reply.get("status"))
        if status != "SUCCESS" or not token:
            raise AuthenticationError(f"{VENUE}: login refused: {status or 'no reason given'}", body={"status": status})
        self._token, self._issued = str(token), self._clock()

    def _extend(self) -> None:
        """`keepAlive`, falling back to a fresh login when Betfair refuses it."""
        headers = {"X-Application": self.app_key, "X-Authentication": self._token or "", "Accept": "application/json"}
        try:
            reply = self._identity_post(f"https://{self.hosts['identity']}/api/keepAlive", headers=headers)
        except AuthenticationError:
            reply = {}
        if reply.get("status") == "SUCCESS":
            self._token, self._issued = str(reply.get("token") or self._token), self._clock()
            return
        self._token = None
        self._login()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


class BetfairAuth(httpx.Auth):
    """Puts the session's headers on every Betting API request, and retries a
    request once with a fresh login when Betfair says the session expired."""

    requires_response_body = True

    def __init__(self, session: BetfairSession):
        self.session = session

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        request.headers.update(self.session.headers())
        response = yield request
        if response.status_code >= 400 and _session_expired(response):
            self.session.invalidate()
            request.headers.update(self.session.headers())
            yield request


def _session_expired(response: httpx.Response) -> bool:
    try:
        return api_error_code(response.json()) in SESSION_ERRORS
    except ValueError:
        return False


def session_from(credentials: Any = None, **overrides: Any) -> BetfairSession | None:
    """A session from credentials, else from `BETFAIR_*` in the environment
    or a `.env`; `None` when no application key is configured anywhere."""
    if credentials is None:
        credentials = credentials_from_env()
    if credentials is None:
        return None
    return BetfairSession(
        app_key=credentials.app_key, username=credentials.username, password=credentials.password,
        cert_path=credentials.cert_path, key_path=credentials.key_path,
        jurisdiction=credentials.jurisdiction, **overrides,
    )


def credentials_from_env() -> Any:
    import os

    from .trading.credentials import load_betfair, read_dotenv

    merged: dict[str, str] = {}
    if os.path.exists(".env"):
        merged.update(read_dotenv(".env"))
    merged.update(os.environ)
    return load_betfair(merged)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Betfair(Exchange):
    """Betfair Exchange, read-only, on the account's own application key.

    ```python
    import synpath

    bf = synpath.Betfair()                  # BETFAIR_APP_KEY, _USERNAME, _PASSWORD
    markets = bf.fetch_markets(event_type="Soccer", limit=10)
    book = bf.fetch_order_book(markets[0].id)
    book.info["delayed"]                     # True on a delayed key
    ```
    """

    id = VENUE
    name = "Betfair Exchange"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # A catalogue and a book request per batch, batched by weight.
        "fetch_markets_by_ids": True,
        # By matched or available money, one page; no listing time to sort by.
        "sort": "partial",
        "fetch_order_book": True,
        # Forty markets a request, every runner of each.
        "fetch_order_books": True,
        # Betfair publishes no individual trades, only volume per price.
        "fetch_trades": False,
        "fetch_ohlcv": False,
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, credentials: Any = None, session: BetfairSession | None = None, session_token: str | None = None,
        delayed: bool = True, currency: str | None = None, commission: float = DEFAULT_COMMISSION,
        timeout: float = 30.0, limiter: RateLimiter | None = LIMITER, client: Any = None,
        identity_client: httpx.Client | None = None,
    ):
        """`credentials` (else `BETFAIR_*` from the environment) give the
        application key and the login; `session_token` reuses a token from a
        login made elsewhere. Nothing is sent until the first call.

        `delayed` says whether the key is a delayed one, for books whose
        response does not say (`isMarketDataDelayed` wins when it is there).
        `currency` asks for amounts in that currency instead of the account's.
        `commission` is the base rate assumed where a market's own is unread."""
        overrides: dict[str, Any] = {"client": identity_client, "timeout": timeout}
        if session_token:
            overrides["session_token"] = session_token
        self.session = session or session_from(credentials, **overrides)
        self.delayed = delayed
        self.currency = currency
        self.commission = commission
        jurisdiction = self.session.jurisdiction if self.session else "com"
        if client is None:
            client = httpx.Client(timeout=timeout, follow_redirects=True)
        client.auth = BetfairAuth(self.session) if self.session else _MissingCredentials()
        base = f"https://{JURISDICTIONS[jurisdiction]['api']}{BETTING_PATH}"
        self.http = HttpClient(base, limiter=limiter, timeout=timeout, client=client, venue=VENUE, attempts=2)

    def _call(self, operation: str, body: dict[str, Any]) -> Any:
        """One Betting API operation, with Betfair's error code in the error."""
        try:
            return self.http.post(f"/{operation}/", json=body)
        except ExchangeError as exc:
            code = api_error_code(exc.body)
            if code is None:
                raise
            if code in AUTH_ERRORS:
                raise AuthenticationError(f"{VENUE}: {operation} refused: {code}", body=exc.body, status=exc.status) from None
            if code in RATE_ERRORS:
                raise RateLimitExceeded(f"{VENUE}: {operation} refused: {code}") from None
            raise BadRequest(f"{VENUE}: {operation} refused: {code}", body=exc.body, status=exc.status) from None

    # -- catalog ------------------------------------------------------------

    def fetch_event_types(self) -> list[dict[str, Any]]:
        """Betfair's sports (event types) with their open market counts:
        `[{"eventType": {"id": "1", "name": "Soccer"}, "marketCount": 1234}]`."""
        return list(self._call("listEventTypes", {"filter": {}}) or [])

    def fetch_betfair_events(
        self, *, event_type: str | list[str] | None = None, competition: str | list[str] | None = None,
        query: str | None = None,
    ) -> list[dict[str, Any]]:
        """Betfair's own events (fixtures, races), as the venue lists them. Each
        holds many Betfair markets; see `fetch_events` for those."""
        market_filter = self._filter(query=query, event_type=event_type, competition=competition)
        return list(self._call("listEvents", {"filter": market_filter}) or [])

    def _event_type_ids(self, names_or_ids: list[str]) -> list[str]:
        """Event type ids, resolving names ("Soccer", "horse racing") by asking
        the venue. A name it does not have is refused rather than ignored."""
        named = [n for n in names_or_ids if not n.isdigit()]
        if not named:
            return names_or_ids
        by_name = {str(r["eventType"]["name"]).lower(): str(r["eventType"]["id"])
                   for r in self.fetch_event_types() if isinstance(r, dict) and r.get("eventType")}
        unknown = [n for n in named if n.lower() not in by_name]
        if unknown:
            raise BadRequest(f"{VENUE}: no event type named {', '.join(unknown)}")
        return [n if n.isdigit() else by_name[n.lower()] for n in names_or_ids]

    def _filter(
        self, *, query: str | None = None, event_type: str | list[str] | None = None,
        competition: str | list[str] | None = None, event: str | list[str] | None = None,
        market_type: str | list[str] | None = None,
    ) -> dict[str, Any]:
        """A Betfair `MarketFilter` from this adapter's arguments."""
        def listed(value: Any) -> list[str]:
            return [str(v) for v in ([value] if isinstance(value, (str, int)) else value)]

        market_filter: dict[str, Any] = {}
        if query:
            market_filter["textQuery"] = query
        if event_type:
            market_filter["eventTypeIds"] = self._event_type_ids(listed(event_type))
        if competition:
            market_filter["competitionIds"] = listed(competition)
        if event:
            market_filter["eventIds"] = listed(event)
        if market_type:
            market_filter["marketTypeCodes"] = listed(market_type)
        return market_filter

    def _catalogue(
        self, market_filter: dict[str, Any], *, projection: tuple[str, ...], sort: str, max_results: int,
    ) -> list[dict[str, Any]]:
        rows = self._call("listMarketCatalogue", {
            "filter": market_filter, "marketProjection": list(projection), "sort": sort, "maxResults": max_results,
        })
        return [row for row in rows if isinstance(row, dict) and row.get("marketId")] if isinstance(rows, list) else []

    def _walk(self, market_filter: dict[str, Any], cursor: str | None) -> Iterator[tuple[dict[str, Any], str, int, int]]:
        """Catalogue rows in start-time order from `cursor`, each with its
        position: (row, start time, index among rows with that start time,
        first runner to read). Betfair has no cursor; its catalogue is paged
        here by start time, from the last start time read, skipping the
        markets at that time already returned. More markets than one request
        holds starting at the very same second are read only up to that many;
        narrow the filter (by event type or competition) to see the rest."""
        start, skip, runner_skip = _cursor(cursor)
        size = markets_per_request(request_weight(LISTING_PROJECTION, book=False))
        for _ in range(MAX_PAGES):
            page_filter = dict(market_filter)
            if start:
                page_filter["marketStartTime"] = {"from": start}
            rows = self._catalogue(page_filter, projection=LISTING_PROJECTION, sort="FIRST_TO_START", max_results=size)
            seen_at: dict[str, int] = {}
            fresh = 0
            for row in rows:
                row_start = str(row.get("marketStartTime") or "")
                index = seen_at.get(row_start, 0)
                seen_at[row_start] = index + 1
                if row_start == start and index < skip:
                    continue
                fresh += 1
                yield row, row_start, index, runner_skip if (row_start == start and index == skip) else 0
            if len(rows) < size or not rows or not fresh:
                return
            last = str(rows[-1].get("marketStartTime") or "")
            # The rows counted at `last` include any skipped ones, since a
            # request from `start` sees every market at `start` from the first.
            start, skip, runner_skip = last, seen_at.get(last, 0), 0

    def _books(self, market_ids: list[str], *, price_data: tuple[str, ...] = BOOK_PRICE_DATA) -> dict[str, dict[str, Any]]:
        """Books for these Betfair markets, batched so no request passes the weight limit."""
        size = markets_per_request(request_weight(price_data, book=True))
        books: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(market_ids)), size):
            body: dict[str, Any] = {"marketIds": chunk}
            if price_data:
                body["priceProjection"] = {"priceData": list(price_data), "virtualise": True}
            if self.currency:
                body["currencyCode"] = self.currency
            for book in self._call("listMarketBook", body) or []:
                if isinstance(book, dict) and book.get("marketId"):
                    books[str(book["marketId"])] = book
        return books

    def _normalize_kwargs(self) -> dict[str, Any]:
        return {"delayed": self.delayed, "currency": self.currency}

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sort: str | None = None, event_type: str | list[str] | None = None,
        competition: str | list[str] | None = None, event: str | list[str] | None = None,
        market_type: str | list[str] | None = None,
    ) -> Page[Market]:
        """One page of runners (each a market), soonest-starting first, with
        their best offers. `query` is Betfair's text search; `event_type` takes
        ids or names ("Soccer"), `competition`, `event` and `market_type`
        (`MATCH_ODDS`) Betfair's ids and codes. `sort="volume"` (matched money)
        or `"liquidity"` (available money) orders at the venue and returns one
        page. Only open markets are listed: a closed one leaves Betfair's
        catalogue, and `fetch_market` still reads it by id."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: Betfair lists only markets that have not closed; read a closed one by id")
        check_sort(sort, venue=VENUE, supported=True)
        if sort is not None and sort not in SORTS:
            raise NotSupported(f"{VENUE}: cannot sort by {sort}; Betfair sorts by matched or available money only")
        if sort is not None and cursor:
            raise BadRequest(f"{VENUE}: a sorted listing is one page; it has no cursor")
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        market_filter = self._filter(query=query, event_type=event_type, competition=competition, event=event,
                                     market_type=market_type)
        if sort is not None:
            rows = self._catalogue(market_filter, projection=LISTING_PROJECTION, sort=SORTS[sort],
                                   max_results=min(wanted, markets_per_request(request_weight(LISTING_PROJECTION, book=False))))
            picked = [(row, runner) for row in rows for runner in row.get("runners") or []][:wanted]
            return Page(self._with_books(picked), next_cursor=None)
        picked: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for row, start, index, first_runner in self._walk(market_filter, cursor):
            runners = (row.get("runners") or [])[first_runner:]
            room = wanted - len(picked)
            picked += [(row, runner) for runner in runners[:room]]
            if len(runners) > room:
                return Page(self._with_books(picked), next_cursor=f"{start}|{index}|{first_runner + room}")
            if len(picked) >= wanted:
                return Page(self._with_books(picked), next_cursor=f"{start}|{index + 1}|0")
        return Page(self._with_books(picked), next_cursor=None)

    def _with_books(self, picked: list[tuple[dict[str, Any], dict[str, Any]]]) -> list[Market]:
        books = self._books([str(row["marketId"]) for row, _ in picked]) if picked else {}
        return [normalize_market(row, runner, books.get(str(row["marketId"])), **self._normalize_kwargs())
                for row, runner in picked]

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", event_type: str | list[str] | None = None,
        competition: str | list[str] | None = None, event: str | list[str] | None = None,
        market_type: str | list[str] | None = None,
    ) -> Page[Event]:
        """One page of Betfair markets, each with its runners, soonest first.
        Filters as `fetch_markets`."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: Betfair lists only markets that have not closed")
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        market_filter = self._filter(query=query, event_type=event_type, competition=competition, event=event,
                                     market_type=market_type)
        rows: list[dict[str, Any]] = []
        next_cursor = None
        for row, start, index, _ in self._walk(market_filter, cursor):
            rows.append(row)
            if len(rows) >= wanted:
                next_cursor = f"{start}|{index + 1}|0"
                break
        books = self._books([str(row["marketId"]) for row in rows]) if rows else {}
        events = [normalize_event(row, books.get(str(row["marketId"])), **self._normalize_kwargs()) for row in rows]
        return Page(events, next_cursor=next_cursor)

    def fetch_market(self, market_id: str) -> Market:
        """One runner by id, open or closed."""
        found = self.fetch_markets_by_ids([market_id])
        if not found:
            raise MarketNotFound(f"{VENUE}: no runner {self.native(market_id)}")
        return found[0]

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Runners in the order asked for: one catalogue request per hundred
        Betfair markets and one book request per forty. A runner whose market
        has closed is read from its book alone (no names, but the result)."""
        wanted = [parse_runner_id(self.native(m)) for m in market_ids]
        betfair_ids = list(dict.fromkeys(market for market, _, _ in wanted))
        size = markets_per_request(request_weight(LOOKUP_PROJECTION, book=False))
        rows: dict[str, dict[str, Any]] = {}
        for chunk in batched(betfair_ids, size):
            for row in self._catalogue({"marketIds": chunk}, projection=LOOKUP_PROJECTION, sort="FIRST_TO_START",
                                       max_results=len(chunk)):
                rows[str(row["marketId"])] = row
        books = self._books(betfair_ids)
        out: list[Market] = []
        for market, selection, handicap in dict.fromkeys(wanted):
            row, book = rows.get(market), books.get(market)
            runner = next((r for r in (row or {}).get("runners") or []
                           if int(r.get("selectionId", -1)) == selection and float(r.get("handicap") or 0) == handicap), None)
            if row is not None and runner is not None:
                out.append(normalize_market(row, runner, book, **self._normalize_kwargs()))
                continue
            runner_book = runner_book_of(book, selection, handicap)
            if book is not None and runner_book is not None:
                out.append(normalize_book_only(book, runner_book, **self._normalize_kwargs()))
        return out

    # -- market data --------------------------------------------------------

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        """A runner's best offers (three levels a side on a delayed key) on YES."""
        books = self.fetch_order_books([market_id], side=side, depth=depth)
        if not books:
            raise MarketNotFound(f"{VENUE}: no book for {self.native(market_id)}")
        return next(iter(books.values()))

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many runners: forty Betfair markets a request (best offers
        weigh 5 a market, against Betfair's limit of 200). Runners Betfair does
        not know are left out."""
        wanted = [parse_runner_id(self.native(m)) for m in dict.fromkeys(market_ids)]
        books = self._books([market for market, _, _ in wanted])
        out: dict[str, OrderBook] = {}
        for market, selection, handicap in wanted:
            book = books.get(market)
            runner_book = runner_book_of(book, selection, handicap)
            if book is None or runner_book is None:
                continue
            synpath_id = self.qualify(runner_native_id(market, selection, handicap))
            out[synpath_id] = normalize_order_book(
                book, runner_book, market_id=synpath_id, side=side, depth=depth,
                delayed=self.delayed, currency=self.currency,
            )
        return out

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> list:
        raise NotSupported(
            f"{VENUE}: Betfair publishes no individual trades, only the volume matched at each price "
            f"and the last traded price (in a book's info)"
        )

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """Commission at the market's base rate, read from its catalogue entry.
        A runner id or a Betfair market id (an event) both name the market."""
        native = self.native(market_id)
        betfair_id = parse_runner_id(native)[0] if "-" in native else native
        rows = self._catalogue({"marketIds": [betfair_id]}, projection=("MARKET_DESCRIPTION",),
                               sort="FIRST_TO_START", max_results=1)
        rate = ((rows[0].get("description") or {}).get("marketBaseRate")) if rows else None
        return fee_schedule_of(betfair_id, rate, default=self.commission)

    def close(self) -> None:
        self.http.close()
        if self.session is not None:
            self.session.close()


class _MissingCredentials(httpx.Auth):
    """Stands in for a session when none is configured, so the adapter can be
    built (and asked what it supports) without an account; the first request
    says what to set."""

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        raise AuthenticationError(
            f"{VENUE}: every Betfair call needs an account: set BETFAIR_APP_KEY, BETFAIR_USERNAME and "
            f"BETFAIR_PASSWORD (or pass credentials=)"
        )
        yield request  # pragma: no cover - a generator, as httpx expects


def _cursor(cursor: str | None) -> tuple[str | None, int, int]:
    """`start|skip|runner`: the start time the walk resumes at, how many
    markets starting then were already read, and the first runner of the next."""
    if not cursor:
        return None, 0, 0
    parts = cursor.split("|")
    try:
        start, skip, runner = parts
        return start or None, max(0, int(skip)), max(0, int(runner))
    except ValueError:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued") from None

