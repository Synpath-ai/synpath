"""Novig: a US sports exchange, read from its public catalog.

**No key is needed for market data.** Events, markets with their outcomes and
top of book, order books and the trade tape all answer an unsigned GET under
`/v3/public/`. The edge network in front caches each answer for a few
seconds and throttles per IP. A key (`NOVIG_KEY_ID`, see `NovigCredentials`)
adds only the history routes, which serve markets no longer in the open
catalog; `fetch_market` falls back to them when one is given.

Two environments with the same API and separate ids: Production
(`api.novig.com`, real money) and Paper (`api.paper.novig.com`, play money).

Four things about this venue that shape the adapter:

**A market is a binary question with two named outcomes.** "EDM -1.5"
against "TOR +1.5", "Over 6.5" against "Under 6.5", "Yes" against "No" on a
prop. A three-way soccer result is three Yes/No markets already. The
outcome Novig lists first (the lower `outcomeId`: the Over, the Yes, the side
a spread's description names) is this library's YES and the other its NO,
as on SX Bet; the names stay on the outcomes as their labels. The order is
fixed for the market's life, since outcome ids are. An event is one game, or
one futures question, with its markets under it.

**Every order buys an outcome, so one book serves both sides.** Each outcome
has a ladder of resting bids; a bid on one outcome at `p` is an offer of the
other at `1 - p`. Read on YES, YES bids are bids and NO bids are asks at one
minus their price. Prices sit on a grid of 0.001 near the ends and 0.005 in
the middle (see `synpath.trading.novig_signing`).

**Quantities are in cents.** The venue's contract pays 1 cent, so its `qty`
is a hundredth of this library's dollar contract: every size, volume and open
interest here is divided by 100. Its own numbers stay in `info`.

**The taker pays `c * P * (1 - P)` per contract**, at the market's own
coefficient (0.06 today), and the maker earns a share of it back (the maker
credit). Game markets charge only while the event is in play; futures
markets on some leagues always. See `fee_schedule_of`.

Restricted states, KYC and geolocation apply to trading on Production only;
reading the catalog needs none of it.
"""
from __future__ import annotations

import threading
import time
from decimal import Decimal
from typing import Any

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
    pick_bars, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote,
    Trade, iso,
)

VENUE = "novig"
API_URL = "https://api.novig.com"
PAPER_API_URL = "https://api.paper.novig.com"
API_URLS = {"production": API_URL, "paper": PAPER_API_URL}

QTY_SCALE = 100
"""The venue's contract pays 1 cent: 100 of them are one dollar contract."""

LIMITER = RateLimiter(4.0, burst=8)
"""The public routes are throttled per IP at the edge, at a rate the venue
does not publish; kept modest, shared process-wide."""

BOOK_DEPTH = 20
"""Price levels per outcome the book route serves at most (its default)."""

PAGE = 5000
"""Rows per listing request: the venue's maximum."""

EVENT_PAGE = 20
"""Events one `fetch_events` page reads when no limit is given: one request
for the page, then one for each event's markets."""

MAX_PAGES = 20
"""Most listing pages one walk reads."""

MAX_TRADE_PAGES = 10
"""Most tape pages one candle read walks back through."""

EVENTS_TTL = 30.0
"""Seconds the open events (for market titles and states) are reused."""

OPEN_EVENTS = ("OPEN_PREGAME", "OPEN_INGAME", "DELAYED", "CLOSED_PREGAME")
CLOSED_EVENTS = ("FINAL", "CANCELED")
EVENT_STATUSES: dict[str, tuple[str, ...]] = {"open": OPEN_EVENTS, "closed": CLOSED_EVENTS, "all": ()}
"""The event states a listing's `status` asks for (`all`: no filter). The
public catalog holds events until their markets settle, and nothing after."""

TRADABLE_EVENTS = frozenset({"OPEN_PREGAME", "OPEN_INGAME", "DELAYED"})


# ---------------------------------------------------------------------------
# Pure normalizers
# ---------------------------------------------------------------------------

def contracts(qty: Any) -> float:
    """The venue's 1-cent contracts as dollar contracts."""
    return round(int(qty or 0) / QTY_SCALE, 6)


def price(value: Any) -> float | None:
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def sides(market: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """(YES outcome, NO outcome): the one Novig lists first, by outcome id,
    and the other."""
    outcomes = sorted((o for o in market.get("outcomes") or [] if isinstance(o, dict) and o.get("outcomeId")),
                      key=lambda o: str(o["outcomeId"]))
    if len(outcomes) != 2:
        raise ExchangeError(f"{VENUE}: market {market.get('marketId')} has {len(outcomes)} outcomes, not two")
    return outcomes[0], outcomes[1]


def side_ids(market: dict[str, Any]) -> tuple[str, str]:
    yes, no = sides(market)
    return str(yes["outcomeId"]), str(no["outcomeId"])


def quotes_of(market: dict[str, Any]) -> tuple[Quote, Quote]:
    """YES and NO top of book from each outcome's best bid: YES is bid at its
    own best bid and offered at one minus NO's. `last` is the latest trade."""
    yes, no = sides(market)
    yes_bid, no_bid = yes.get("bid") or {}, no.get("bid") or {}
    bid, bid_size = price(yes_bid.get("price")), contracts(yes_bid.get("qty")) if yes_bid else None
    no_price = price(no_bid.get("price"))
    ask = round(1 - no_price, 6) if no_price is not None else None
    ask_size = contracts(no_bid.get("qty")) if no_bid else None
    mid = round((bid + ask) / 2, 6) if bid is not None and ask is not None else None
    last = price(yes.get("last"))
    if last is None and price(no.get("last")) is not None:
        last = round(1 - price(no.get("last")), 6)  # type: ignore[operator]
    yes_quote = Quote(bid=bid, bid_size=bid_size, ask=ask, ask_size=ask_size, mid=mid, last=last)
    no_quote = Quote(
        bid=round(1 - ask, 6) if ask is not None else None, bid_size=ask_size,
        ask=round(1 - bid, 6) if bid is not None else None, ask_size=bid_size,
        mid=round(1 - mid, 6) if mid is not None else None, last=round(1 - last, 6) if last is not None else None,
    )
    return yes_quote, no_quote


def result_of(market: dict[str, Any]) -> tuple[str | None, str | None]:
    """(result on YES, fair-market price of YES) from the outcomes' grades:
    `yes` or `no` for a winner, `void` for a push or a fair-market-value
    settlement, whose YES price rides as the second value."""
    yes, no = sides(market)
    grade_yes, grade_no = str(yes.get("status") or "TBD"), str(no.get("status") or "TBD")
    if grade_yes == "WIN" or grade_no == "LOSS":
        return "yes", None
    if grade_no == "WIN" or grade_yes == "LOSS":
        return "no", None
    if grade_yes == "PUSH" or grade_no == "PUSH":
        return "void", None
    if grade_yes not in ("TBD", ""):
        return "void", grade_yes
    return None, None


def status_of(market_status: str | None, event_status: str | None) -> tuple[str, bool]:
    """(normalized status, accepting orders) from the market's and its
    event's states. A market of a finished or cancelled event waits for its
    grade: closed."""
    if market_status == "SETTLED" or event_status == "SETTLED":
        return "settled", False
    if market_status == "CLOSED" or event_status in CLOSED_EVENTS:
        return "closed", False
    if market_status == "OPEN":
        return "open", event_status in TRADABLE_EVENTS or event_status is None
    return "closed", False


def tags_of(event: dict[str, Any] | None) -> list[str]:
    seen: dict[str, None] = {}
    for word in ((event or {}).get("sport"), (event or {}).get("league")):
        if word and str(word).strip():
            seen.setdefault(str(word).strip().lower(), None)
    return list(seen)


def normalize_market(market: dict[str, Any], event: dict[str, Any] | None = None) -> Market:
    """One market: the outcome Novig lists first as YES, the other as NO,
    each under the venue's name for it. The title is the event, then the
    market."""
    market_id = str(market["marketId"])
    yes, no = sides(market)
    event = event or {}
    event_status = event.get("status")
    status, accepting = status_of(market.get("status"), event_status)
    yes_quote, no_quote = quotes_of(market)
    result, fmv = result_of(market)
    kind, strike = market.get("marketType"), market.get("strike")
    one, two = str(yes.get("name") or "Yes"), str(no.get("name") or "No")
    starts = market.get("startsTs") or event.get("startsTs")
    tags = tags_of(event)
    title = " — ".join(str(p) for p in (event.get("description"), market.get("description")) if p) or market_id
    void = "a push refunds every fill" if market.get("voids") == "PUSH" else "a void settles at fair market value"
    fee = market.get("fee") or {}
    return Market(
        id=ids.qualify(VENUE, market_id),
        venue=VENUE,
        venue_market_id=market_id,
        event_id=ids.qualify(VENUE, str(market["eventId"])) if market.get("eventId") else None,
        title=title,
        description=f"{kind or 'Market'}: {one} or {two}; {void}.",
        slug=None,
        yes=Outcome(label=one, quote=yes_quote, venue_token_id=str(yes["outcomeId"]),
                    info={"status": yes.get("status")}),
        no=Outcome(label=two, quote=no_quote, venue_token_id=str(no["outcomeId"]),
                   info={"status": no.get("status")}),
        status=status,  # type: ignore[arg-type]
        native_status=f"{market.get('status')}/{event_status}" if event_status else market.get("status"),
        active=accepting,
        open_timestamp=None,
        open_datetime=None,
        # Game markets trade on in play, so no close is known before the end.
        close_timestamp=None,
        close_datetime=None,
        resolution_timestamp=market.get("settledTs"),
        resolution_datetime=iso(market.get("settledTs")),
        # The grid steps by 0.001 near the ends and 0.005 in the middle.
        tick_size=None,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(
            volume_total=contracts(market["volume"]) if market.get("volume") is not None else None,
            open_interest=contracts(market["openInterest"]) if market.get("openInterest") is not None else None,
            volume_unit="contracts",
        ),
        url=None,
        image_url=None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=one,
        neg_risk=False,
        settlement_sources=[],
        info={
            "market": market,
            "event": event or None,
            "market_type": kind,
            "strike": strike,
            "voids": market.get("voids"),
            "fee": fee or None,
            "starts": starts,
            "event_status": event_status,
            "in_play": event_status == "OPEN_INGAME",
            "yes_outcome_id": str(yes["outcomeId"]),
            "no_outcome_id": str(no["outcomeId"]),
            "result": result,
            "fmv_yes": fmv,
            "volume_qty": market.get("volume"),
            "open_interest_qty": market.get("openInterest"),
            "currency": "USD",
        },
    )


def normalize_event(event: dict[str, Any], markets: list[dict[str, Any]]) -> Event:
    """One event (a game, or a futures question) with its markets."""
    event_id = str(event["eventId"])
    normalized = [normalize_market(m, event) for m in markets]
    statuses = {m.status for m in normalized}
    status = next((s for s in ("open", "closed", "settled") if s in statuses), None)
    if status is None:
        status, _ = status_of(None, event.get("status"))
        status = "open" if event.get("status") in OPEN_EVENTS else status
    tags = tags_of(event)
    return Event(
        id=ids.qualify(VENUE, event_id),
        venue=VENUE,
        venue_event_id=event_id,
        title=str(event.get("description") or event_id),
        description=None,
        slug=None,
        markets=normalized,
        status=status,  # type: ignore[arg-type]
        native_status=event.get("status"),
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        # A game's markets are separate questions (moneyline, spreads, totals).
        mutually_exclusive=None,
        close_timestamp=None,
        close_datetime=None,
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            "sport": event.get("sport"), "league": event.get("league"), "starts": event.get("startsTs"),
            "in_play": event.get("status") == "OPEN_INGAME", "event": event,
        },
    )


def book_levels(rows: Any, *, yes: bool) -> list[OrderLevel]:
    """One outcome's resting orders, summed by price, as YES levels: on YES a
    bid at its price, on NO an ask at one minus it."""
    by_price: dict[str, int] = {}
    for row in rows or []:
        if not isinstance(row, dict) or row.get("price") in (None, ""):
            continue
        qty = int(row.get("qty") or 0)
        if qty > 0:
            by_price[str(row["price"])] = by_price.get(str(row["price"]), 0) + qty
    out = []
    for raw, qty in by_price.items():
        value = float(raw)
        if 0 < value < 1:
            out.append(OrderLevel(price=round(value if yes else 1 - value, 6), size=contracts(qty)))
    return out


def normalize_order_book(
    book: dict[str, Any], *, yes_outcome: str, no_outcome: str, market_id: str, side: BookSide = "yes",
    depth: int | None = None,
) -> OrderBook:
    """The market's two ladders read on YES; `side="no"` mirrors it. Sizes in
    dollar contracts."""
    orders = book.get("orders") or {}
    bids = sorted(book_levels(orders.get(yes_outcome), yes=True), key=lambda level: level.price, reverse=True)
    asks = sorted(book_levels(orders.get(no_outcome), yes=False), key=lambda level: level.price)
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
        # The route serves at most twenty price levels per outcome.
        depth_scope="top_n",
        info={"seq": book.get("seq"), "yes_outcome_id": yes_outcome, "no_outcome_id": no_outcome,
              "orders": orders},
    )


def normalize_trade(row: dict[str, Any], *, yes_outcome: str, market_id: str) -> Trade:
    """One execution from the tape, in the YES price with the taker's side.

    The tape names the resting order's outcome and price (`tradeId` is the
    resting order's fill), so the taker bought the other outcome at one minus
    it: a print on YES at `p` is a taker selling YES at `p`, a print on NO at
    `q` a taker buying YES at `1 - q`."""
    paid = float(row.get("price") or 0)
    on_yes = str(row.get("outcomeId")) == yes_outcome
    stamp = int(row.get("ts") or 0)
    return Trade(
        id=str(row.get("tradeId") or row.get("takeId") or stamp),
        market_id=market_id,
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=round(paid if on_yes else 1 - paid, 6),
        quantity=contracts(row.get("qty")),
        side="sell" if on_yes else "buy",
        info=row,
    )


def candles_from_trades(trades: list[Trade], *, interval_seconds: int) -> list[Candle]:
    """Prints bucketed into bars, oldest first, with their volume in contracts."""
    buckets: dict[int, list[Trade]] = {}
    for trade in trades:
        buckets.setdefault(trade.timestamp // 1000 // interval_seconds * interval_seconds, []).append(trade)
    candles = []
    for start in sorted(buckets):
        rows = sorted(buckets[start], key=lambda t: t.timestamp)
        prices = [t.price for t in rows]
        candles.append(Candle(
            timestamp=start * 1000, datetime=iso(start * 1000) or "",
            open=prices[0], high=max(prices), low=min(prices), close=prices[-1],
            volume=round(sum(t.quantity for t in rows), 6), trade_count=len(rows), price_source="trade",
            info={},
        ))
    return candles


def fee_schedule_of(market: dict[str, Any]) -> FeeSchedule:
    """The market's own schedule: a taker pays `c * P * (1 - P)` per dollar
    contract, a maker earns `makerCredit` of that back (a negative maker
    rate). `charged` says when: `WHEN_LIVE` (every game market) only while
    the event is in play, `ALWAYS` in every phase. The rates are the in-play
    ones, so before kickoff on a game market the estimate is an upper bound:
    a fill matched pregame pays nothing."""
    fee = market.get("fee") or {}
    coefficient = float(fee.get("coefficient") or 0)
    credit = float(fee.get("makerCredit") or 0)
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=str(market.get("marketId") or ""), fee_type="quadratic_theta",
        taker_rate=coefficient, maker_rate=-round(coefficient * credit, 9), exponent=1.0,
        info={"charged": fee.get("charged"), "coefficient": fee.get("coefficient"),
              "maker_credit": fee.get("makerCredit"), "currency": "USD",
              "rounding": "half up to $0.00001, no minimum"},
    )


def error_code(body: Any) -> str | None:
    """Novig's stable error code: `{"code": "...", "message": "..."}`."""
    if isinstance(body, dict) and body.get("code"):
        return str(body["code"])
    return None


def _words_in(text: str, query: str) -> bool:
    words = [w for w in query.lower().split() if w]
    haystack = text.lower()
    return bool(words) and all(w in haystack for w in words)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Novig(Exchange):
    """Novig, read-only. No key needed.

    ```python
    import synpath

    nv = synpath.Novig()                      # Production; Novig(env="paper") for Paper
    markets = nv.fetch_markets(league="NFL", limit=10)
    book = nv.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Novig"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # One request each: the catalog has no batch read by id.
        "fetch_markets_by_ids": True,
        # The catalog lists newest first and has no sort.
        "sort": False,
        "fetch_order_book": True,
        # One request per market.
        "fetch_order_books": True,
        "fetch_trades": True,
        # Built from the public tape; the venue's own candles need a key.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        # Words matched against the open events' descriptions here: the
        # venue has no search route.
        "search": "partial",
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, env: str | None = None, api_url: str | None = None, credentials: Any = None,
        timeout: float = 30.0, limiter: RateLimiter | None = LIMITER, client: Any = None,
        signed_client: Any = None,
    ):
        """No argument is needed: the catalog is public. `env` picks
        Production (the default) or Paper, whose ids differ. `credentials`
        (a `NovigCredentials`) signs the history reads that `fetch_market`
        falls back to for a market no longer in the open catalog."""
        env = env or getattr(credentials, "env", None) or "production"
        if env not in API_URLS:
            raise BadRequest(f"{VENUE}: env must be production or paper, got {env!r}")
        self.env = env
        self.api_url = (api_url or API_URLS[env]).rstrip("/")
        if client is None:
            client = httpx.Client(timeout=timeout, follow_redirects=True, headers={"Accept": "application/json"})
        self.http = HttpClient(self.api_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.signed: HttpClient | None = None
        key_id = getattr(credentials, "key_id", None) or getattr(credentials, "management_key_id", None)
        pem = getattr(credentials, "private_key_pem", None) if getattr(credentials, "key_id", None) \
            else getattr(credentials, "management_private_key_pem", None)
        if key_id and pem:
            from .trading.novig_signing import NovigAuth, NovigSigner

            auth = NovigAuth(NovigSigner(key_id, pem))
            if signed_client is None:
                signed_client = httpx.Client(timeout=timeout, follow_redirects=True,
                                             headers={"Accept": "application/json"})
            signed_client.auth = auth
            self.signed = HttpClient(self.api_url, limiter=limiter, timeout=timeout, client=signed_client, venue=VENUE)
        self._sides: dict[str, tuple[str, str]] = {}
        self._events: dict[str, dict[str, Any]] = {}
        self._events_at = 0.0
        self._lock = threading.Lock()

    def _get(self, path: str, params: Any = None) -> Any:
        return self.http.get(f"/v3/public/catalog{path}", params)

    # -- raw reads ----------------------------------------------------------

    def fetch_novig_market(self, market_id: str) -> dict[str, Any]:
        """One market as the venue answers it: the open catalog, else (with a
        key) market history, which keeps every market."""
        native = self.native(market_id)
        try:
            market = self._get(f"/markets/{native}")
        except MarketNotFound:
            if self.signed is None:
                raise MarketNotFound(
                    f"{VENUE}: no open market {native} (a key reads closed and settled ones from history)"
                ) from None
            market = self.signed.get(f"/v3/history/markets/{native}")
        if isinstance(market, dict):
            self.remember(market)
        return market

    def fetch_novig_event(self, event_id: str) -> dict[str, Any] | None:
        native = self.native(event_id)
        try:
            return self._get(f"/events/{native}")
        except MarketNotFound:
            if self.signed is None:
                return None
            try:
                return self.signed.get(f"/v3/history/events/{native}")
            except MarketNotFound:
                return None

    def outcome_ids(self, market_id: str) -> tuple[str, str]:
        """(YES outcome id, NO outcome id) of a market, read once and kept:
        a market's outcomes never change."""
        native = self.native(market_id)
        with self._lock:
            known = self._sides.get(native)
        if known is None:
            self.fetch_novig_market(native)
            known = self._sides[native]
        return known

    def remember(self, market: dict[str, Any]) -> None:
        """Keep a market's outcome ids from a payload already in hand."""
        if market.get("marketId") and len(market.get("outcomes") or []) == 2:
            with self._lock:
                self._sides[str(market["marketId"])] = side_ids(market)

    def _events_by_id(self, event_ids: list[str]) -> dict[str, dict[str, Any]]:
        """The open events, read in one listing and reused for `EVENTS_TTL`
        seconds; an id not in it is read on its own."""
        wanted = list(dict.fromkeys(e for e in event_ids if e))
        stale = time.monotonic() - self._events_at > EVENTS_TTL
        missing = [e for e in wanted if stale or e not in self._events]
        if missing:
            if len(missing) > 3:
                fresh: dict[str, dict[str, Any]] = {}
                after = None
                for _ in range(MAX_PAGES):
                    body = self._get("/events", {"limit": PAGE, "after": after}) or {}
                    for event in body.get("items") or []:
                        fresh[str(event["eventId"])] = event
                    after = body.get("next")
                    if not after or not body.get("items"):
                        break
                self._events, self._events_at = fresh, time.monotonic()
                missing = [e for e in wanted if e not in self._events]
            for event_id in missing:
                event = self.fetch_novig_event(event_id)
                if event:
                    self._events[event_id] = event
        return {e: self._events[e] for e in wanted if e in self._events}

    # -- catalog ------------------------------------------------------------

    def _statuses(self, status: str) -> tuple[str, ...]:
        check_status(status)
        if status not in EVENT_STATUSES:
            raise NotSupported(
                f"{VENUE}: the public catalog holds markets until they settle; it lists open and closed ones "
                f"(status='open', 'closed' or 'all'). Read a settled market by id with fetch_market and a key"
            )
        return EVENT_STATUSES[status]

    def _normalized(self, rows: list[dict[str, Any]]) -> list[Market]:
        events = self._events_by_id([str(m.get("eventId")) for m in rows])
        out = []
        for row in rows:
            if len(row.get("outcomes") or []) != 2:
                continue
            self.remember(row)
            out.append(normalize_market(row, events.get(str(row.get("eventId")))))
        return out

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sort: str | None = None, league: str | list[str] | None = None,
        market_type: str | list[str] | None = None, event: str | None = None,
    ) -> Page[Market]:
        """One page of markets, newest first, with their top of book. `status`
        is the events' state: open (pregame, in play, delayed), closed
        (final or cancelled, awaiting their grades) or all; a market leaves
        the public catalog once it settles. `league` (`NFL`, `NBA`)
        and `market_type` (`MONEY`, `SPREAD`, `TOTAL`) are the venue's
        canonical names; `event` one event's markets. `query` matches words
        against the open events' descriptions ("Chiefs", "Bills") and returns
        every market of the events that match."""
        statuses = self._statuses(status)
        check_sort(sort, venue=VENUE, supported=False)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        params = {"league": _joined(league), "marketType": _joined(market_type),
                  "eventStatus": ",".join(statuses) or None}
        if query:
            rows: list[dict[str, Any]] = []
            for event_id in self._search(query, statuses):
                body = self._get("/markets", {**params, "event": event_id, "limit": PAGE}) or {}
                rows += body.get("items") or []
                if len(rows) >= wanted:
                    break
            return Page(self._normalized(rows[:wanted]), next_cursor=None)
        body = self._get("/markets", {**params, "event": event and self.native(event), "limit": wanted,
                                      "after": cursor}) or {}
        return Page(self._normalized(list(body.get("items") or [])), next_cursor=body.get("next") or None)

    def _search(self, query: str, statuses: tuple[str, ...]) -> list[str]:
        if not query.strip():
            raise BadRequest(f"{VENUE}: an empty search")
        matched = []
        after = None
        for _ in range(MAX_PAGES):
            body = self._get("/events", {"status": ",".join(statuses) or None, "limit": PAGE, "after": after}) or {}
            matched += [str(e["eventId"]) for e in body.get("items") or []
                        if _words_in(str(e.get("description") or ""), query)]
            after = body.get("next")
            if not after or not body.get("items"):
                break
        return matched

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", league: str | list[str] | None = None,
    ) -> Page[Event]:
        """One page of events (twenty unless `limit` says), each with its
        markets: one request for the page, then one per event. Filters as
        `fetch_markets`."""
        statuses = self._statuses(status)
        wanted = page_limit(limit) or EVENT_PAGE
        params = {"status": ",".join(statuses) or None, "league": _joined(league)}
        if query:
            found = set(self._search(query, statuses))
            body = {"items": [e for e in self._events_by_id(sorted(found)).values()]}
            following = None
        else:
            body = self._get("/events", {**params, "limit": wanted, "after": cursor}) or {}
            following = body.get("next") or None
        events = list(body.get("items") or [])[:wanted]
        out = []
        for event in events:
            markets = self._event_markets(str(event["eventId"]))
            out.append(normalize_event(event, markets))
        return Page(out, next_cursor=following)

    def _event_markets(self, event_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        after = None
        for _ in range(MAX_PAGES):
            body = self._get("/markets", {"event": event_id, "limit": PAGE, "after": after}) or {}
            rows += [m for m in body.get("items") or [] if len(m.get("outcomes") or []) == 2]
            after = body.get("next")
            if not after or not body.get("items"):
                break
        for row in rows:
            self.remember(row)
        return rows

    def fetch_event(self, event_id: str) -> Event:
        """One event with its markets, by `novig:<eventId>`."""
        native = self.native(event_id)
        event = self.fetch_novig_event(native)
        if not event:
            raise MarketNotFound(f"{VENUE}: no event {native}")
        return normalize_event(event, self._event_markets(native))

    def fetch_market(self, market_id: str) -> Market:
        """One market by id: from the open catalog, else (with a key) from
        history, which keeps settled markets and their grades."""
        market = self.fetch_novig_market(market_id)
        event = self._events_by_id([str(market.get("eventId") or "")]).get(str(market.get("eventId") or ""))
        return normalize_market(market, event)

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Markets in the order asked for, one request each; ids the venue
        does not know are left out."""
        rows = []
        for market_id in dict.fromkeys(market_ids):
            try:
                rows.append(self.fetch_novig_market(market_id))
            except MarketNotFound:
                continue
        return self._normalized(rows)

    # -- market data --------------------------------------------------------

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        """Both outcomes' ladders read on YES, up to twenty price levels each."""
        native = self.native(market_id)
        yes, no = self.outcome_ids(native)
        levels = min(depth, BOOK_DEPTH) if depth else None
        book = self._get(f"/markets/{native}/book", {"depth": levels}) or {}
        return normalize_order_book(book, yes_outcome=yes, no_outcome=no, market_id=self.qualify(native),
                                    side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, one request each (and one more for a
        market whose outcomes are not yet known)."""
        out: dict[str, OrderBook] = {}
        for market_id in dict.fromkeys(market_ids):
            try:
                book = self.fetch_order_book(market_id, side=side, depth=depth)
            except MarketNotFound:
                continue
            out[book.market_id] = book
        return out

    def _tape(self, native: str, *, limit: int, after: str | None) -> tuple[list[dict[str, Any]], str | None]:
        body = self._get(f"/markets/{native}/trades", {"limit": limit, "after": after}) or {}
        return list(body.get("items") or []), body.get("next") or None

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Trade]:
        """The market's tape, a page newest first from the venue and returned
        oldest first, in the YES price with the taker's side."""
        native = self.native(market_id)
        yes, _ = self.outcome_ids(native)
        rows, following = self._tape(native, limit=page_limit(limit) or MAX_PAGE_LIMIT, after=cursor)
        trades = [normalize_trade(row, yes_outcome=yes, market_id=self.qualify(native)) for row in rows]
        if since:
            trades = [t for t in trades if t.timestamp >= since]
        return Page(sorted(trades, key=lambda t: t.timestamp), next_cursor=following)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None, until: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price built from the public tape, with their volume
        in contracts (`price_source="trade"`). The tape is read back page by
        page until `since`, at most ten pages of 500."""
        native = self.native(market_id)
        yes, _ = self.outcome_ids(native)
        seconds = timeframe_seconds(timeframe)
        trades: list[Trade] = []
        after = None
        for _ in range(MAX_TRADE_PAGES):
            rows, after = self._tape(native, limit=500, after=after)
            batch = [normalize_trade(r, yes_outcome=yes, market_id=self.qualify(native)) for r in rows]
            trades += batch
            if not after or (since and batch and min(t.timestamp for t in batch) < since):
                break
        candles = candles_from_trades(trades, interval_seconds=seconds)
        if since:
            candles = [c for c in candles if c.timestamp + seconds * 1000 > since]
        if until:
            candles = [c for c in candles if c.timestamp <= until]
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """The market's own fee: coefficient, maker credit, and when it is charged."""
        return fee_schedule_of(self.fetch_novig_market(market_id))

    def close(self) -> None:
        self.http.close()
        if self.signed is not None:
            self.signed.close()


def _joined(value: str | list[str] | None) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else ",".join(value)


def fee_estimate(schedule: FeeSchedule, yes_price: Decimal, quantity: Decimal) -> tuple[Decimal, Decimal]:
    """(taker fee, maker fee) for `quantity` dollar contracts at a YES price.
    Symmetric in the price, so either outcome gives the same fee."""
    taker = schedule.estimate(float(yes_price), float(quantity), taker=True) or 0.0
    maker = schedule.estimate(float(yes_price), float(quantity), taker=False) or 0.0
    return Decimal(str(round(taker, 5))), Decimal(str(round(maker, 5)))
