"""ProphetX: a US peer-to-peer sports exchange (formerly Prophet Exchange), read
from its Trading API or its Market Data API.

**Every call needs a key; two kinds read market data.** The Trading API's key
pair (`PROPHETX_ACCESS_KEY`, `PROPHETX_SECRET_KEY`) is self-service for any
KYC-approved account (prophetx.co, API Integration). It is exchanged at `POST
/auth/login` for a bearer token that lasts ten minutes and is renewed with
`POST /auth/refresh`; see `ProphetXSession`. Its routes are `/mm/...` and
`/v4/mm/...`. The Market Data API's affiliate key (`PROPHETX_AFFILIATE_KEY`) is
issued on request by ProphetX's market data team and sent as it is, without
`Bearer`, on `/affiliate/...` and `/v4/affiliate/...`. With both set the
affiliate key reads market data, so the session's slots are left for trading.
Both APIs answer at `https://cash.api.prophetx.co/partner` and, on the sandbox
(`PROPHETX_SANDBOX=1`), at `https://api.sandbox.prophetx.dev/partner`.

Four things about this venue that shape the adapter:

**A line is a binary market with two named sides.** A ProphetX market at one
strike (a moneyline; a spread or a total at one number) has two sides, and
each side is its own buy-only instrument, named by its `strike_id`. The first
side ProphetX lists is this library's YES and the second its NO; the names stay
on the outcomes as their labels. A Synpath market is
`prophetx:<event id>-<YES strike_id>`, `prophetx:44285780-90de769f4f4977a04446ed203e957351`,
and the sport event is its event, `prophetx:44285780`. A spread or total can
push (the stake is returned), as on SX Bet.

**Prices are American odds.** A side priced `-A` risks `A` to win 100, so its
implied probability is `A / (A + 100)`; one priced `+A` risks 100 to win `A`,
so `100 / (A + 100)`; `+100` and `-100` are both one half. Values strictly
between -100 and +100 do not exist. `implied_probability` converts exactly,
as a fraction; read prices are that fraction as a float to eight places.

**The book is what a taker can buy, on each side.** Each side's levels are
built from orders resting on the other side: a level on side one at price `p`
offers YES at `implied(p)` (a YES ask), and a level on side two at `q` offers
NO at `implied(q)`, which is selling YES at `1 - implied(q)` (a YES bid). A
level's `value` is the USD a taker can stake at it; a stake `s` at
probability `P` returns `s / P`, so the level holds `value / P` contracts
paying 1 each. ProphetX lists each side's ten best levels.

**No public tape over REST, and no published fee rate.** Executions are
broadcast on the WebSocket only (`ProphetXMarketStream.watch_trades`). The fee
is charged per order and shown on each order and fill; the API documentation
publishes no rate, only a maker rebate on monthly volume.
"""
from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime
from fractions import Fraction
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
)
from .errors import AuthenticationError, BadRequest, MarketNotFound, NetworkError, NotSupported
from .types import BookSide, Event, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote

VENUE = "prophetx"
API_URL = "https://cash.api.prophetx.co/partner"
SANDBOX_URL = "https://api.sandbox.prophetx.dev/partner"
CURRENCY = "USD"

LIMITER = RateLimiter(1.0, burst=2)
"""The default tier allows one request a second on almost every endpoint, per
key; one a second, shared process-wide, stays inside it."""

EVENTS_PER_REQUEST = 20
"""Events one `get_multiple_markets` request asks for. ProphetX documents no
cap; twenty keeps a request's answer a reasonable size."""

STRIKES_PER_REQUEST = 20
"""Strike ids one `get_strikes` request takes, as the reference caps it."""

MAX_TOURNAMENTS = 200
"""Most tournaments one listing walks through."""

TOKEN_RENEW_S = 8 * 60
"""Renew the ten-minute access token at eight minutes, as ProphetX advises,
when its answer gives no expiry."""

OPEN_STATES = ("not_started", "live")
"""The event states `get_sport_events` lists: every other state is unlisted."""

PAUSED_STATES = ("suspended", "delayed", "interrupted")
"""Event states in which trading is held but the event is not over."""

ENDED_STATES = ("ended", "closed", "cancelled", "postponed", "abandoned")


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------

def implied_probability(price: Any) -> Fraction | None:
    """American odds as the implied probability a buyer pays, exactly.

    `-A` (a favourite) risks `A` to win 100: `A / (A + 100)`. `+A` (an
    underdog) risks 100 to win `A`: `100 / (A + 100)`. Both `+100` and `-100`
    are 1/2. `None` for a missing price or one strictly between -100 and +100,
    which American odds do not have (ProphetX's placeholder levels carry
    those)."""
    if price is None or isinstance(price, bool):
        return None
    try:
        american = Fraction(str(price))
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if american >= 100:
        return Fraction(100) / (american + 100)
    if american <= -100:
        return -american / (-american + 100)
    return None


def probability(price: Any) -> float | None:
    """American odds as a price in [0, 1], to eight places."""
    exact = implied_probability(price)
    return round(float(exact), 8) if exact is not None else None


def american_of(probability_: Fraction) -> Fraction:
    """The inverse of `implied_probability`: the American odds whose implied
    probability is `probability_` (0 < p < 1), exactly. Above one half it is
    the favourite's `-100p / (1 - p)`, at or below it `100(1 - p) / p`, so
    even money is `+100`, the one ProphetX's ladder lists."""
    if not 0 < probability_ < 1:
        raise BadRequest(f"{VENUE}: a probability strictly between 0 and 1 has American odds, not {probability_}")
    if probability_ > Fraction(1, 2):
        return -100 * probability_ / (1 - probability_)
    return 100 * (1 - probability_) / probability_


# ---------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------

def line_native_id(event_id: Any, yes_strike_id: Any) -> str:
    """`44285780-90de769f…`: the sport event, then the YES side's strike id."""
    return f"{event_id}-{yes_strike_id}"


def parse_line_id(native: str) -> tuple[str, str]:
    """(event id, YES strike id) from a market's native id."""
    event_id, sep, strike_id = native.partition("-")
    if not sep or not event_id.isdigit() or not strike_id:
        raise BadRequest(
            f"{VENUE}: {native!r} is not a market id; expected '<event id>-<strike id>', "
            f"e.g. prophetx:44285780-90de769f4f4977a04446ed203e957351 (a bare event id names an event)"
        )
    return event_id, strike_id


def event_of(native: str) -> str:
    """The sport event id from a market id or a bare event id."""
    if "-" in native:
        return parse_line_id(native)[0]
    if not native.isdigit():
        raise BadRequest(f"{VENUE}: {native!r} is not a ProphetX event or market id")
    return native


# ---------------------------------------------------------------------------
# Pure normalizers
# ---------------------------------------------------------------------------

def iso_ms(value: Any) -> int | None:
    """Milliseconds from ProphetX's timestamps: ISO 8601 strings (with up to
    nine fraction digits), or epoch seconds, milliseconds or nanoseconds."""
    if value in (None, "", 0):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = int(value)
        if number > 10 ** 17:
            return number // 1_000_000
        if number > 10 ** 14:
            return number // 1000
        return number if number > 10 ** 11 else number * 1000
    text = str(value).replace("Z", "+00:00")
    head, dot, rest = text.partition(".")
    if dot:
        digits = rest[:len(rest) - len(rest.lstrip("0123456789"))]
        text = f"{head}.{digits[:6].ljust(6, '0')}{rest[len(digits):]}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        from datetime import timezone

        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def groups_of(selections: Any) -> list[list[dict[str, Any]]]:
    """A line's two sides, each a list of price levels. v3/v4 nest a list per
    side; a flat list (older versions) is one level per side."""
    rows = [s for s in selections or [] if isinstance(s, (list, dict))]
    if rows and all(isinstance(s, dict) for s in rows):
        return [[s] for s in rows]
    return [[level for level in s if isinstance(level, dict)] for s in rows if isinstance(s, list)]


def lines_of(market: dict[str, Any]) -> list[dict[str, Any]]:
    """A market's lines, each with exactly two sides: its `market_strikes`
    entries where it has them (each carrying the market's own fields), else
    the market itself. A side with no liquidity still lists one placeholder
    level naming its strike. Markets with other than two sides (a three-way
    moneyline) are left out: they are not binary."""
    strikes = [s for s in market.get("market_strikes") or [] if isinstance(s, dict)]
    base = {k: v for k, v in market.items() if k != "market_strikes"}
    units = [{**base, **strike} for strike in strikes] or [base]
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for unit in units:
        sides = groups_of(unit.get("selections"))
        if len(sides) != 2 or not all(sides):
            continue
        yes_id, no_id = sides[0][0].get("strike_id"), sides[1][0].get("strike_id")
        if not yes_id or not no_id or str(yes_id) in seen:
            continue
        seen.add(str(yes_id))
        out.append({**unit, "_sides": sides, "_yes": str(yes_id), "_no": str(no_id)})
    return out


def side_levels(levels: list[dict[str, Any]], strike_id: str) -> list[tuple[float, float, dict[str, Any]]]:
    """(probability, contracts, raw) for one side's real levels on its strike:
    a stake of `value` at probability `P` buys `value / P` contracts. `value`
    is the USD a taker can stake; `quantity` (the makers' original stake) is
    read only where a level has no `value`."""
    out = []
    for level in levels:
        if str(level.get("strike_id") or strike_id) != strike_id:
            continue
        exact = implied_probability(level.get("price"))
        stake = level.get("value") if level.get("value") is not None else level.get("quantity")
        try:
            stake_f = float(stake)
        except (TypeError, ValueError):
            continue
        if exact is None or stake_f <= 0:
            continue
        out.append((round(float(exact), 8), round(stake_f / float(exact), 6), level))
    return out


def book_of(line: dict[str, Any]) -> tuple[list[OrderLevel], list[OrderLevel]]:
    """A line's YES bids and asks: side one's levels are YES asks at their
    implied probability, side two's YES bids at one minus theirs. Best first."""
    one, two = line["_sides"]
    asks = [OrderLevel(price=p, size=size) for p, size, _ in side_levels(one, line["_yes"])]
    bids = [OrderLevel(price=round(1 - p, 8), size=size) for p, size, _ in side_levels(two, line["_no"])]
    return sorted(bids, key=lambda level: level.price, reverse=True), sorted(asks, key=lambda level: level.price)


def status_of(event: dict[str, Any], line: dict[str, Any] | None = None) -> tuple[str, bool]:
    """(normalized status, accepting orders) from the event's and market's states."""
    state = str(event.get("status") or "").lower()
    market_state = str((line or {}).get("status") or "").lower()
    if state in ENDED_STATES:
        return "closed", False
    if state in PAUSED_STATES or market_state in ("suspended", "closed", "inactive", "settled"):
        return "open", False
    return "open", True


def _tags(event: dict[str, Any]) -> list[str]:
    seen: dict[str, None] = {}
    for word in (event.get("sport_name"), event.get("tournament_name")):
        if word and str(word).strip():
            seen.setdefault(str(word).strip().lower(), None)
    return list(seen)


def event_title(event: dict[str, Any]) -> str:
    return str(event.get("display_name") or event.get("name") or event.get("event_id") or "")


def _label(selection: dict[str, Any], fallback: str) -> str:
    return str(selection.get("name") or selection.get("display_name") or fallback)


def normalize_market(event: dict[str, Any] | None, line: dict[str, Any]) -> Market:
    """One line: its first side as YES, its second as NO, each under the
    venue's name for it, priced from the levels it lists."""
    event = event or {}
    event_id = str(event.get("event_id") or line.get("event_id") or line.get("sport_event_id") or "")
    one, two = line["_sides"]
    yes_label, no_label = _label(one[0], "Yes"), _label(two[0], "No")
    native = line_native_id(event_id, line["_yes"])
    bids, asks = book_of(line)
    bid, ask = (bids[0] if bids else None), (asks[0] if asks else None)
    mid = round((bid.price + ask.price) / 2, 8) if bid and ask else None
    yes = Quote(bid=bid.price if bid else None, bid_size=bid.size if bid else None,
                ask=ask.price if ask else None, ask_size=ask.size if ask else None, mid=mid)
    no = Quote(bid=round(1 - ask.price, 8) if ask else None, bid_size=ask.size if ask else None,
               ask=round(1 - bid.price, 8) if bid else None, ask_size=bid.size if bid else None,
               mid=round(1 - mid, 8) if mid is not None else None)
    status, accepting = status_of(event, line)
    tags = _tags(event)
    market_name = line.get("display_name") or line.get("name")
    scheduled = iso_ms(event.get("scheduled"))
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, event_id) if event_id else None,
        title=" — ".join(str(p) for p in (event_title(event), market_name, yes_label) if p),
        description=f"{market_name or 'Market'}: {yes_label} or {no_label}.",
        slug=None,
        yes=Outcome(label=yes_label, quote=yes, venue_token_id=line["_yes"]),
        no=Outcome(label=no_label, quote=no, venue_token_id=line["_no"]),
        status=status,  # type: ignore[arg-type]
        native_status=str(event.get("status") or "") or None,
        active=accepting,
        open_timestamp=None,
        open_datetime=None,
        close_timestamp=None,
        close_datetime=None,
        resolution_timestamp=None,
        resolution_datetime=None,
        # Prices sit on a ladder of American odds, so the step in YES price
        # varies along it.
        tick_size=None,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(),
        url=None,
        image_url=None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=str(event["tournament_id"]) if event.get("tournament_id") is not None else None,
        outcome_label=yes_label,
        neg_risk=False,
        settlement_sources=[],
        info={
            "event_id": event_id or None,
            "market_id": line.get("id"),
            "yes_strike_id": line["_yes"],
            "no_strike_id": line["_no"],
            "yes_outcome_id": one[0].get("outcome_id"),
            "no_outcome_id": two[0].get("outcome_id"),
            "strike": line.get("strike", line.get("line")),
            "type": line.get("type"),
            "sub_type": line.get("sub_type"),
            "category_name": line.get("category_name"),
            "player_id": line.get("player_id"),
            "scheduled": scheduled,
            "selections": [one, two],
            "market": {k: v for k, v in line.items() if not k.startswith("_") and k != "selections"},
            "event": event or None,
            "currency": CURRENCY,
        },
    )


def normalize_event(event: dict[str, Any], markets: list[dict[str, Any]]) -> Event:
    """A sport event with every binary line of its markets."""
    lines = [line for market in markets for line in lines_of(market)]
    normalized = [normalize_market(event, line) for line in lines]
    status, _ = status_of(event)
    event_id = str(event.get("event_id") or "")
    tags = _tags(event)
    scheduled = iso_ms(event.get("scheduled"))
    return Event(
        id=ids.qualify(VENUE, event_id),
        venue=VENUE,
        venue_event_id=event_id,
        title=event_title(event) or event_id,
        description=None,
        slug=None,
        markets=normalized,
        status=status,  # type: ignore[arg-type]
        native_status=str(event.get("status") or "") or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=str(event["tournament_id"]) if event.get("tournament_id") is not None else None,
        mutually_exclusive=None,
        close_timestamp=None,
        close_datetime=None,
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            "scheduled": scheduled,
            "sport": event.get("sport_name"),
            "tournament": event.get("tournament_name"),
            "tournament_id": event.get("tournament_id"),
            "competitors": event.get("competitors") or [],
            "live_disabled": event.get("live_disabled"),
            "sub_type": event.get("sub_type"),
        },
    )


def normalize_order_book(
    line: dict[str, Any], *, market_id: str, side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """A line's book read on YES; `side="no"` mirrors it. Sizes in contracts."""
    bids, asks = book_of(line)
    derived = side == "no"
    if derived:
        bids, asks = (
            [OrderLevel(price=round(1 - level.price, 8), size=level.size) for level in asks],
            [OrderLevel(price=round(1 - level.price, 8), size=level.size) for level in bids],
        )
    elif side != "yes":
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    one, two = line["_sides"]
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks,
        timestamp=None, datetime=None, book_model="shared_complement", derived=derived,
        # ProphetX lists each side's ten best levels.
        depth_scope="top_n",
        info={"yes": one, "no": two, "yes_strike_id": line["_yes"], "no_strike_id": line["_no"], "currency": CURRENCY},
    )


def unwrap(payload: Any) -> Any:
    """The answer inside ProphetX's `{"data": ...}` envelope, where there is one."""
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def markets_by_event(payload: Any) -> dict[str, list[dict[str, Any]]]:
    """`get_multiple_markets` keyed by event id. ProphetX says a few answers
    come back as a flat list instead, grouped here by each market's event."""
    data = unwrap(payload)
    out: dict[str, list[dict[str, Any]]] = {}
    if isinstance(data, dict):
        for key, rows in data.items():
            if isinstance(rows, list):
                out[str(key)] = [r for r in rows if isinstance(r, dict)]
            elif isinstance(rows, dict) and isinstance(rows.get("markets"), list):
                out[str(key)] = [r for r in rows["markets"] if isinstance(r, dict)]
    elif isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                key = row.get("event_id") or row.get("sport_event_id")
                out.setdefault(str(key), []).append(row)
    return out


def markets_of(payload: Any) -> list[dict[str, Any]]:
    """`get_markets` for one event: `{"event_id", "markets": [...]}`, or a bare list."""
    data = unwrap(payload)
    rows = data.get("markets") if isinstance(data, dict) else data
    return [r for r in rows or [] if isinstance(r, dict)]


def batched(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class ProphetXSession:
    """A Trading API session from an access key and secret key, kept in memory.

    `POST /auth/login` trades the key pair for an access token (ten minutes)
    and a refresh token (three days); the access token is renewed with `POST
    /auth/refresh` before it expires, and a fresh login is made only when the
    refresh token has lapsed or is refused. ProphetX counts live sessions per
    access key and a login does not replace an earlier one, so one session
    object should serve one process: the read adapter, the trading adapter and
    their streams share it. Usable from threads (`token`) and from an event
    loop (`token_async`)."""

    def __init__(
        self, *, access_key: str, secret_key: str, base_url: str = API_URL, client: httpx.Client | None = None,
        timeout: float = 30.0, clock: Any = time.time,
    ):
        self.access_key = access_key
        self._secret = secret_key
        self.base_url = base_url.rstrip("/")
        self._client = client
        self._timeout = timeout
        self._clock = clock
        self._access: str | None = None
        self._refresh: str | None = None
        self._access_until = 0.0
        self._refresh_until = 0.0
        self._lock = threading.Lock()
        self._async_lock: asyncio.Lock | None = None

    def __repr__(self) -> str:
        return f"ProphetXSession(base_url={self.base_url!r}, token={'set' if self._access else 'none'})"

    # -- state ----------------------------------------------------------------

    def _fresh(self) -> bool:
        return bool(self._access) and self._clock() < self._access_until

    def _step(self) -> tuple[str, dict[str, Any]]:
        """The call to make now: a refresh while the refresh token lasts, else a login."""
        if self._refresh and self._clock() < self._refresh_until:
            return "/auth/refresh", {"refresh_token": self._refresh}
        return "/auth/login", {"access_key": self.access_key, "secret_key": self._secret}

    def _apply(self, path: str, status: int, body: Any) -> bool:
        """Take the answer to a login or refresh. `False` when a refresh was
        refused and a login should follow."""
        if status == 429:
            from .errors import RateLimitExceeded

            raise RateLimitExceeded(f"{VENUE}: too many logins")
        if status >= 400:
            if path == "/auth/refresh":
                self._refresh = None
                return False
            code = body.get("error") if isinstance(body, dict) else None
            raise AuthenticationError(f"{VENUE}: login refused: {code or status}", status=status,
                                      body={"error": code} if code else None)
        data = unwrap(body) or {}
        token = data.get("access_token") if isinstance(data, dict) else None
        if not token:
            raise AuthenticationError(f"{VENUE}: the login answered without an access token")
        now = self._clock()
        self._access = str(token)
        # An expiry already past (a clock that disagrees with ProphetX's) is
        # ignored rather than trusted into a login on every call.
        expires = _epoch_s(data.get("access_expire_time"))
        renew = now + TOKEN_RENEW_S
        self._access_until = min(expires - 60, renew) if expires and expires - 60 > now else renew
        if data.get("refresh_token"):
            self._refresh = str(data["refresh_token"])
            until = _epoch_s(data.get("refresh_expire_time"))
            self._refresh_until = until - 60 if until and until - 60 > now else now + 2 * 86400
        return True

    def invalidate(self) -> None:
        """Forget the access token (ProphetX said 401); the refresh token is kept."""
        self._access = None
        self._access_until = 0.0

    # -- sync -----------------------------------------------------------------

    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self._timeout)
        return self._client

    def token(self) -> str:
        with self._lock:
            for _ in range(2):
                if self._fresh():
                    break
                path, body = self._step()
                try:
                    response = self._http().post(f"{self.base_url}{path}", json=body)
                except httpx.HTTPError as exc:
                    raise NetworkError(f"{VENUE}: login unreachable: {type(exc).__name__}") from None
                self._apply(path, response.status_code, _json(response))
            assert self._access
            return self._access

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token()}"}

    # -- async ----------------------------------------------------------------

    async def token_async(self, client: httpx.AsyncClient) -> str:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        async with self._async_lock:
            for _ in range(2):
                if self._fresh():
                    break
                path, body = self._step()
                try:
                    response = await client.post(f"{self.base_url}{path}", json=body)
                except httpx.HTTPError as exc:
                    raise NetworkError(f"{VENUE}: login unreachable: {type(exc).__name__}") from None
                self._apply(path, response.status_code, _json(response))
            assert self._access
            return self._access

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


def _epoch_s(value: Any) -> float | None:
    """An expiry in epoch seconds, from seconds, milliseconds or nanoseconds."""
    ms_ = iso_ms(value)
    return ms_ / 1000 if ms_ else None


def _json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None


class ProphetXAuth(httpx.Auth):
    """Puts the session's bearer token on every request and retries once with
    a renewed one when ProphetX answers 401."""

    def __init__(self, session: ProphetXSession):
        self.session = session

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        request.headers.update(self.session.headers())
        response = yield request
        if response.status_code == 401:
            self.session.invalidate()
            request.headers.update(self.session.headers())
            yield request


class NoKey(httpx.Auth):
    """Refuses to send: every ProphetX route needs a key."""

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        raise AuthenticationError(
            f"{VENUE}: every ProphetX call needs a key; set PROPHETX_ACCESS_KEY and PROPHETX_SECRET_KEY "
            f"(self-service at prophetx.co, API Integration) or PROPHETX_AFFILIATE_KEY (ProphetX's Market Data API)"
        )
        yield request  # pragma: no cover - unreachable


def credentials_from_env() -> Any:
    import os

    from .trading.credentials import load_prophetx, read_dotenv

    merged: dict[str, str] = {}
    if os.path.exists(".env"):
        merged.update(read_dotenv(".env"))
    merged.update(os.environ)
    return load_prophetx(merged)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class ProphetX(Exchange):
    """ProphetX, read-only. Needs a Trading API key pair or a Market Data key.

    ```python
    import synpath

    px = synpath.ProphetX()                   # PROPHETX_* from the environment
    markets = px.fetch_markets(limit=10)
    book = px.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "ProphetX"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        # ProphetX lists tournaments and events in its own order and has no sort.
        "sort": False,
        "fetch_order_book": True,
        # Twenty events a request, every line of each.
        "fetch_order_books": True,
        # Executions are broadcast on the WebSocket only.
        "fetch_trades": False,
        "fetch_ohlcv": False,
        "fetch_series": False,
        # The API documentation publishes no fee rate.
        "fetch_fee_schedule": False,
        # No search endpoint: event, tournament and competitor names are
        # matched here, over the open events.
        "search": "partial",
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, credentials: Any = None, affiliate_key: str | None = None, session: ProphetXSession | None = None,
        sandbox: bool | None = None, base_url: str | None = None, timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER, client: Any = None, session_client: httpx.Client | None = None,
        login: bool | None = None,
    ):
        """`credentials` (else `PROPHETX_*` from the environment, when `login`
        is not False), `affiliate_key` or `session` say how to authenticate;
        nothing is sent until the first call. With an affiliate key, market
        data is read from the Market Data API; otherwise from the Trading API
        with the session."""
        if credentials is None and login is not False and session is None and affiliate_key is None:
            credentials = credentials_from_env()
        if sandbox is None:
            sandbox = bool(getattr(credentials, "sandbox", False))
        self.sandbox = sandbox
        url = base_url or (SANDBOX_URL if sandbox else API_URL)
        self.affiliate_key = affiliate_key or getattr(credentials, "affiliate_key", None)
        if session is None and getattr(credentials, "access_key", None) and getattr(credentials, "secret_key", None):
            session = ProphetXSession(access_key=credentials.access_key, secret_key=credentials.secret_key,
                                      base_url=url, client=session_client, timeout=timeout)
        self.session = session
        if client is None:
            client = httpx.Client(timeout=timeout, follow_redirects=True)
        client.headers["Accept"] = "application/json"
        if self.affiliate_key:
            client.headers["Authorization"] = self.affiliate_key
        elif self.session is not None:
            client.auth = ProphetXAuth(self.session)
        else:
            client.auth = NoKey()
        self.http = HttpClient(url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)

    @property
    def api(self) -> str:
        """`affiliate` (the Market Data API) or `mm` (the Trading API)."""
        return "affiliate" if self.affiliate_key else "mm"

    def _get(self, path: str, params: Any = None) -> Any:
        """GET `path` on whichever API this adapter reads: `{api}` in the path
        becomes `affiliate` or `mm`."""
        return self.http.get(path.format(api=self.api), params)

    # -- raw reads ----------------------------------------------------------

    def fetch_tournaments(self, *, active: bool = True) -> list[dict[str, Any]]:
        """ProphetX's tournaments (leagues), those with open events by default."""
        data = unwrap(self._get("/{api}/get_tournaments", {"has_active_events": "true" if active else None})) or {}
        rows = data.get("tournaments") if isinstance(data, dict) else data
        return [t for t in rows or [] if isinstance(t, dict) and t.get("id") is not None]

    def fetch_sport_events(
        self, *, tournament_id: Any = None, event_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Open sport events (not started or live), of one tournament or by id."""
        params: list[tuple[str, Any]] = []
        if tournament_id is not None:
            params.append(("tournament_id", str(tournament_id)))
        params += [("event_ids", str(e)) for e in event_ids or []]
        data = unwrap(self._get("/{api}/get_sport_events", params or None)) or {}
        rows = data.get("sport_events") if isinstance(data, dict) else data
        return [e for e in rows or [] if isinstance(e, dict) and e.get("event_id") is not None]

    def fetch_event_markets(self, event_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Raw v4 markets by event id, twenty events a request."""
        out: dict[str, list[dict[str, Any]]] = {}
        for chunk in batched(list(dict.fromkeys(str(e) for e in event_ids)), EVENTS_PER_REQUEST):
            if len(chunk) == 1:
                out[chunk[0]] = markets_of(self._get("/v4/{api}/get_markets", {"event_id": chunk[0]}))
                continue
            payload = self._get("/v4/{api}/get_multiple_markets", [("event_ids", e) for e in chunk])
            out.update(markets_by_event(payload))
        return out

    def fetch_strikes(self, strike_ids: list[str]) -> dict[str, dict[str, Any]]:
        """`strike_id` -> its event, market and type, twenty a request. Trading
        API only: the Market Data API has no such route."""
        if self.session is None:
            raise NotSupported(f"{VENUE}: strike lookup is a Trading API route; it needs the access key pair")
        # With an affiliate key the client's own header is that key; this
        # route takes the session's bearer token instead.
        headers = self.session.headers() if self.affiliate_key else None
        out: dict[str, dict[str, Any]] = {}
        for chunk in batched(list(dict.fromkeys(strike_ids)), STRIKES_PER_REQUEST):
            rows = unwrap(self.http.request("GET", "/v4/mm/get_strikes", params={"strike_ids": ",".join(chunk)},
                                            headers=headers)) or []
            for row in rows if isinstance(rows, list) else []:
                if isinstance(row, dict) and row.get("strike_id"):
                    out[str(row["strike_id"])] = row
        return out

    # -- catalog ------------------------------------------------------------

    def _events_with_markets(self, events: list[dict[str, Any]]) -> list[Event]:
        if not events:
            return []
        markets = self.fetch_event_markets([str(e["event_id"]) for e in events])
        return [normalize_event(e, markets.get(str(e["event_id"]), [])) for e in events]

    def _walk_events(
        self, *, query: str | None, tournament_id: Any, start: int,
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        """(tournament index, its open events) from tournament `start` on, in
        ProphetX's order; filtered to names matching `query` when given."""
        if tournament_id is not None:
            tournaments = [{"id": tournament_id}]
        else:
            tournaments = self.fetch_tournaments()[:MAX_TOURNAMENTS]
        needle = (query or "").strip().lower()
        for index in range(start, len(tournaments)):
            events = self.fetch_sport_events(tournament_id=tournaments[index]["id"])
            if needle:
                events = [e for e in events if needle in _haystack(e, tournaments[index])]
            yield index, events

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sort: str | None = None, tournament_id: Any = None,
    ) -> Page[Market]:
        """One page of open lines, tournament by tournament in ProphetX's
        order, each event's markets in its own order. `query` matches event,
        tournament and competitor names; `tournament_id` reads one tournament."""
        _open_only(status)
        check_sort(sort, venue=VENUE, supported=False)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        start, skip = _cursor(cursor)
        out: list[Market] = []
        for index, events in self._walk_events(query=query, tournament_id=tournament_id, start=start):
            markets = [m for event in self._events_with_markets(events) for m in event.markets]
            batch = markets[skip if index == start else 0:]
            room = wanted - len(out)
            out += batch[:room]
            if len(batch) > room:
                taken = (skip if index == start else 0) + room
                return Page(out, next_cursor=f"{index}:{taken}")
            if len(out) >= wanted:
                return Page(out, next_cursor=f"{index + 1}:0")
        return Page(out, next_cursor=None)

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", tournament_id: Any = None,
    ) -> Page[Event]:
        """Open sport events with their lines, one tournament per page (the
        cursor walks ProphetX's tournaments with open events)."""
        _open_only(status)
        wanted = page_limit(limit)
        start, _ = _cursor(cursor)
        for index, events in self._walk_events(query=query, tournament_id=tournament_id, start=start):
            if not events:
                continue
            normalized = self._events_with_markets(events[:wanted] if wanted else events)
            following = None if tournament_id is not None else f"{index + 1}:0"
            return Page(normalized, next_cursor=following)
        return Page([], next_cursor=None)

    def fetch_event(self, event_id: str) -> Event:
        """One open sport event with its lines, by `prophetx:<event id>`."""
        native = event_of(self.native(event_id))
        events = self.fetch_sport_events(event_ids=[native])
        event = next((e for e in events if str(e.get("event_id")) == native), None)
        if event is None:
            raise MarketNotFound(f"{VENUE}: no open event {native}")
        return self._events_with_markets([event])[0]

    def lines(self, event_id: str) -> list[dict[str, Any]]:
        """An event's raw binary lines (`lines_of` each market), for the streams
        and the trading adapter, which need both strike ids of a line."""
        native = event_of(self.native(event_id))
        markets = self.fetch_event_markets([native]).get(native, [])
        return [{**line, "event_id": line.get("event_id") or native} for m in markets for line in lines_of(m)]

    def fetch_market(self, market_id: str) -> Market:
        """One line by id."""
        found = self.fetch_markets_by_ids([market_id])
        if not found:
            raise MarketNotFound(f"{VENUE}: no open line {self.native(market_id)}")
        return found[0]

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Lines in the order asked for, their events twenty a request. Lines
        ProphetX no longer lists are left out."""
        wanted = list(dict.fromkeys(parse_line_id(self.native(m)) for m in market_ids))
        event_ids = list(dict.fromkeys(e for e, _ in wanted))
        events = {str(e["event_id"]): e for e in self.fetch_sport_events(event_ids=event_ids)}
        markets = self.fetch_event_markets(event_ids)
        by_id: dict[tuple[str, str], Market] = {}
        for event_id in event_ids:
            for market in markets.get(event_id, []):
                for line in lines_of(market):
                    by_id[(event_id, line["_yes"])] = normalize_market(events.get(event_id) or {"event_id": event_id}, line)
        return [by_id[key] for key in wanted if key in by_id]

    # -- market data --------------------------------------------------------

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        books = self.fetch_order_books([market_id], side=side, depth=depth)
        if not books:
            raise MarketNotFound(f"{VENUE}: no open line {self.native(market_id)}")
        return next(iter(books.values()))

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many lines, twenty events a request. An event id gives
        every line of the event. Lines ProphetX no longer lists are left out."""
        natives = [self.native(m) for m in dict.fromkeys(market_ids)]
        markets = self.fetch_event_markets([event_of(n) for n in natives])
        out: dict[str, OrderBook] = {}
        for native in natives:
            event_id = event_of(native)
            strike = parse_line_id(native)[1] if "-" in native else None
            for market in markets.get(event_id, []):
                for line in lines_of(market):
                    if strike is not None and line["_yes"] != strike:
                        continue
                    synpath_id = self.qualify(line_native_id(event_id, line["_yes"]))
                    out[synpath_id] = normalize_order_book(line, market_id=synpath_id, side=side, depth=depth)
        return out

    def fetch_trades(self, market_id: str, **_: Any) -> list:
        raise NotSupported(f"{VENUE}: executions are broadcast on the WebSocket only; use ProphetXMarketStream.watch_trades")

    def fetch_ohlcv(self, market_id: str, **_: Any) -> list:
        raise NotSupported(f"{VENUE}: ProphetX serves no price history")

    def fetch_fee_schedule(self, market_id: str) -> Any:
        raise NotSupported(f"{VENUE}: the API documentation publishes no fee rate; each order and fill carries its fee")

    def close(self) -> None:
        self.http.close()


def _haystack(event: dict[str, Any], tournament: dict[str, Any]) -> str:
    words = [event.get("name"), event.get("display_name"), event.get("tournament_name"), tournament.get("name")]
    words += [c.get("name") for c in event.get("competitors") or [] if isinstance(c, dict)]
    words += [c.get("display_name") for c in event.get("competitors") or [] if isinstance(c, dict)]
    return " ".join(str(w) for w in words if w).lower()


def _open_only(status: str) -> None:
    check_status(status)
    if status != "open":
        raise NotSupported(f"{VENUE}: ProphetX lists only events not started or live")


def _cursor(cursor: str | None) -> tuple[int, int]:
    """`tournament:skip` — the index of the tournament the listing resumes in,
    and how many of its lines were already returned."""
    if not cursor:
        return 0, 0
    head, sep, tail = cursor.partition(":")
    if not sep or not head.isdigit() or not tail.isdigit():
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    return int(head), int(tail)


__all__ = [
    "ProphetX", "ProphetXSession", "implied_probability", "probability", "american_of", "normalize_market",
    "normalize_event", "normalize_order_book", "lines_of",
]
