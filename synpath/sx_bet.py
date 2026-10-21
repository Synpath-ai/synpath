"""SX Bet: a peer-to-peer sports exchange on its own chain, read from its REST API.

Market data needs no key (an SX Bet API key, when given, is sent anyway). Listing, lookup and sports routes share one budget of
500 requests a minute per IP; the order book route has its own, much larger.

Four things about this venue that shape the adapter:

**A market is a binary question with two named sides.** "Over 49.5" against
"Under 49.5", "Dallas Cowboys" against "Philadelphia Eagles". Outcome one is
this library's YES and outcome two its NO; the names stay on the outcomes as
their labels. A market can also be voided (the game is cancelled, a spread
lands on the line), in which case every bet is returned.

**A fixture is the event.** Every market names its fixture (`sportXeventId`,
`L18876509`); one game carries a moneyline, spreads and totals at several
lines, period lines and player props, each its own market keyed by its hash.

**One book, two sides, in implied probability.** A resting order bets one
outcome at a price (`percentageOdds`, a probability scaled by 1e20) and stakes
USDC (6 decimals). Read on YES, an order on outcome one is a bid at that price
and an order on outcome two is an ask at one minus it; its size in contracts is
its stake over its own price. Prices sit on a ladder of 0.125% steps.

**Fees are charged on winnings.** A taker pays 1% of the profit on a winning
bet at settlement; a maker pays nothing. See `FeeSchedule.estimate`
(`profit_on_win`). The venue can set other rates per account.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator

import re

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status,
    page_limit, pick_bars, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome,
    Page, Quote, Trade, iso,
)

API_URL = "https://api.sx.bet"
TESTNET_API_URL = "https://api.toronto.sx.bet"
VENUE = "sx_bet"

LIMITER = RateLimiter(6.0, burst=10)
"""The market routes share 500 requests a minute per IP; kept under it,
shared process-wide."""

PAGE = 100
"""Rows per listing page: the venue's maximum."""

MAX_PAGES = 200
"""Most listing pages one walk reads: 20,000 markets, against about 12,000 today."""

ODDS_SCALE = 10 ** 20
"""Prices are implied probabilities scaled by 1e20."""

USDC = 1_000_000
"""Stakes and returns are USDC in 6-decimal base units."""

TICK = 0.00125
"""The odds ladder steps by 0.125%."""

TAKER_FEE = 0.01
"""1% of the profit on a winning bet, for the taker; a maker pays nothing."""

MAX_FIND = 30
"""Hashes one `/markets/find` request takes."""

MAX_TRADE_PAGES = 20
"""Most tape pages one candle read walks back through: 2,000 trades."""

MARKET_TYPES: dict[int, str] = {
    1: "1X2", 2: "Under/Over", 3: "Asian Handicap", 17: "Both Teams To Score",
    21: "Under/Over First Period", 28: "Under/Over Including Overtime", 29: "Under/Over Rounds",
    41: "1st Period Winner", 42: "2nd Period Winner", 45: "Under/Over Second Period",
    46: "Under/Over Third Period", 52: "12", 53: "Asian Handicap Halftime", 63: "12 Halftime",
    64: "Asian Handicap First Period", 65: "Asian Handicap Second Period",
    66: "Asian Handicap Third Period", 77: "Under/Over Halftime", 88: "To Qualify",
    165: "Set Total", 166: "Under/Over Games", 201: "Asian Handicap Games",
    202: "First Period Winner Home/Away", 203: "Second Period Winner Home/Away",
    204: "Third Period Winner Home/Away", 205: "Fourth Period Winner Home/Away",
    226: "12 Including Overtime", 236: "1st 5 Innings Under/Over", 274: "Outright Winner",
    281: "1st 5 Innings Asian Handicap", 342: "Asian Handicap Including Overtime",
    835: "Asian Under/Over", 866: "Set Spread", 995: "Anytime Touchdown Scorer",
    1536: "Under/Over Maps", 1618: "1st 5 Innings Winner-12",
}
"""The venue's market types by id, as its reference names them."""

HASH = re.compile(r"0x[0-9a-f]{64}")
"""A market hash. The venue refuses a whole lookup over one malformed hash,
so they are checked here first."""

WINNERS = {1: "yes", 2: "no", 0: "void"}
"""A settled market's reported outcome: side one, side two, or void."""


def odds(value: Any) -> float | None:
    """A 1e20-scaled probability as a price in [0, 1], exact to the ladder."""
    try:
        return round(int(value) / ODDS_SCALE, 8)
    except (TypeError, ValueError):
        return None


def usdc(value: Any) -> float | None:
    try:
        return int(value) / USDC
    except (TypeError, ValueError):
        return None


def iso_ms(value: Any) -> int | None:
    """An ISO timestamp, or epoch seconds, as epoch milliseconds."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value * 1000) if value < 10 ** 11 else int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def status_of(market: dict[str, Any]) -> tuple[str, str, bool]:
    """(normalized status, native word, accepting orders)."""
    native = str(market.get("status") or "")
    if market.get("reportedDate") is not None or market.get("outcome") is not None:
        return "settled", native.lower() or "reported", False
    if native == "ACTIVE":
        return "open", "active", True
    return "closed", native.lower(), False


def event_title(market: dict[str, Any]) -> str:
    one, two = market.get("teamOneName"), market.get("teamTwoName")
    return f"{one} vs {two}" if one and two else str(one or two or market.get("sportXeventId") or "")


def tags_of(market: dict[str, Any]) -> list[str]:
    """Sport and league, lowercase, without duplicates."""
    words = [market.get("sportLabel"), market.get("leagueLabel")]
    seen: dict[str, None] = {}
    for word in words:
        if word and str(word).strip():
            seen.setdefault(str(word).strip().lower(), None)
    return list(seen)


def normalize_market(market: dict[str, Any]) -> Market:
    """One market: outcome one as YES, outcome two as NO, each under the
    venue's name for it. The title is the fixture with outcome one after it."""
    market_hash = str(market["marketHash"]).lower()
    one, two = str(market.get("outcomeOneName") or "Yes"), str(market.get("outcomeTwoName") or "No")
    status, native_status, accepting = status_of(market)
    kind = market.get("type")
    type_name = MARKET_TYPES.get(kind) if isinstance(kind, int) else None
    event_id = str(market.get("sportXeventId") or market_hash)
    game = iso_ms(market.get("gameTime"))
    reported = iso_ms(market.get("reportedDate"))
    tags = tags_of(market)
    void = market.get("outcomeVoidName")
    description = f"{type_name or 'Market'}: {one} or {two}." + (f" Void on {void}." if void else "")
    return Market(
        id=ids.qualify(VENUE, market_hash),
        venue=VENUE,
        venue_market_id=market_hash,
        event_id=ids.qualify(VENUE, event_id),
        title=f"{event_title(market)} — {one}",
        description=description,
        slug=None,
        yes=Outcome(label=one, quote=Quote()),
        no=Outcome(label=two, quote=Quote()),
        status=status,  # type: ignore[arg-type]
        native_status=native_status,
        active=accepting,
        open_timestamp=None,
        open_datetime=None,
        close_timestamp=None,
        close_datetime=None,
        resolution_timestamp=reported,
        resolution_datetime=iso(reported),
        tick_size=TICK,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(),
        url=None,
        image_url=None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=str(market["leagueId"]) if market.get("leagueId") is not None else None,
        outcome_label=one,
        neg_risk=False,
        settlement_sources=[],
        info={
            "market": market,
            "type": kind,
            "type_name": type_name,
            "line": market.get("line"),
            "main_line": market.get("mainLine"),
            "game_time": game,
            "live_enabled": market.get("liveEnabled"),
            "quarter_line": market.get("isQuarterLineMarket"),
            "void_condition": void,
            "winning_outcome": WINNERS.get(market["outcome"]) if market.get("outcome") is not None else None,
        },
    )


def normalize_event(markets: list[dict[str, Any]]) -> Event:
    """A fixture with its markets: every row given shares one `sportXeventId`."""
    first = markets[0]
    event_id = str(first.get("sportXeventId") or first["marketHash"])
    normalized = [normalize_market(m) for m in markets]
    statuses = {m.status for m in normalized}
    status = next((s for s in ("open", "closed", "settled") if s in statuses), "closed")
    tags = tags_of(first)
    return Event(
        id=ids.qualify(VENUE, event_id),
        venue=VENUE,
        venue_event_id=event_id,
        title=event_title(first),
        description=None,
        slug=None,
        markets=normalized,
        status=status,  # type: ignore[arg-type]
        native_status=None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=str(first["leagueId"]) if first.get("leagueId") is not None else None,
        mutually_exclusive=None,
        close_timestamp=None,
        close_datetime=None,
        url=None,
        image_url=None,
        settlement_sources=[],
        info={
            "game_time": iso_ms(first.get("gameTime")),
            "sport": first.get("sportLabel"), "sport_id": first.get("sportId"),
            "league": first.get("leagueLabel"), "league_id": first.get("leagueId"),
            "team_one": first.get("teamOneName"), "team_two": first.get("teamTwoName"),
        },
    )


def events_of(rows: list[dict[str, Any]]) -> list[Event]:
    """Market rows grouped into their fixtures, in the order each first appears."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("marketHash"):
            groups.setdefault(str(row.get("sportXeventId") or row["marketHash"]), []).append(row)
    return [normalize_event(group) for group in groups.values()]


def book_levels(rows: Any, *, outcome_one: bool) -> list[OrderLevel]:
    """Resting orders on one outcome as YES levels: on outcome one a bid at the
    price, on outcome two an ask at one minus it. A level's stake buys
    `stake / price` contracts of the side it bets."""
    out = []
    for row in rows or []:
        price, stake = odds((row or {}).get("percentageOdds")), usdc((row or {}).get("size"))
        if not price or stake is None or stake <= 0:
            continue
        out.append(OrderLevel(price=price if outcome_one else round(1 - price, 8), size=round(stake / price, 6)))
    return out


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str, side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """The venue's book read on YES; `side="no"` mirrors it. Sizes in contracts."""
    bids = book_levels(payload.get("outcomeOne"), outcome_one=True)
    asks = book_levels(payload.get("outcomeTwo"), outcome_one=False)
    derived = side == "no"
    if derived:
        bids, asks = (
            [OrderLevel(price=round(1 - level.price, 8), size=level.size) for level in asks],
            [OrderLevel(price=round(1 - level.price, 8), size=level.size) for level in bids],
        )
    elif side != "yes":
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    bids = sorted(bids, key=lambda level: level.price, reverse=True)
    asks = sorted(asks, key=lambda level: level.price)
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks,
        timestamp=None, datetime=None, book_model="shared_complement", derived=derived,
        depth_scope="top_n" if depth else "full", info={"version": payload.get("version")},
    )


def normalize_trade(row: dict[str, Any], *, market_id: str) -> Trade:
    """One taker bet from the tape, in the YES price with the taker's side.
    The venue gives the taker's own price: a bet on outcome two at `p` sells
    YES at `1 - p`. The contracts are the bet's return, one dollar each."""
    price = odds(row.get("weightedAverageOdds")) or 0.0
    buying = bool(row.get("isBettingOutcomeOne"))
    if not buying:
        price = 1 - price
    stamp = iso_ms(row.get("betTime")) or 0
    return Trade(
        id=str(row.get("tradeId") or stamp),
        market_id=market_id,
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=round(price, 6),
        quantity=usdc(row.get("totalReturn")) or 0.0,
        side="buy" if buying else "sell",
        info={k: v for k, v in row.items() if k not in ("userAddress", "username")},
    )


def candles_from_trades(trades: list[Trade], *, interval_seconds: int) -> list[Candle]:
    """Taker bets bucketed into bars, oldest first, with their volume in contracts."""
    buckets: dict[int, list[Trade]] = {}
    for trade in trades:
        start = trade.timestamp // 1000 // interval_seconds * interval_seconds
        buckets.setdefault(start, []).append(trade)
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


def fee_schedule_of(market_id: str) -> FeeSchedule:
    """The venue's default rates: 1% of a taker's profit on a winning bet,
    nothing for a maker. An account can carry other rates (`GET /user/fees-v3`)."""
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="profit_on_win",
        taker_rate=TAKER_FEE, maker_rate=0.0,
        info={"currency": "USDC", "charged": "at settlement, on a winning bet's profit"},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class SxBet(Exchange):
    """SX Bet, read-only. No key needed.

    ```python
    import synpath

    sx = synpath.SxBet()
    markets = sx.fetch_markets(limit=5)
    book = sx.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "SX Bet"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # Thirty hashes a request.
        "fetch_markets_by_ids": True,
        # The venue lists in its own order and has no sort.
        "sort": False,
        "fetch_order_book": True,
        # One request per market.
        "fetch_order_books": True,
        "fetch_trades": True,
        # Built from the public tape of taker bets.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, api_key: str | None = None, api_url: str = API_URL, testnet: bool = False, timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER, client: Any = None,
    ):
        """`api_key` is optional: the routes read here answer without one
        today, but the venue documents its book snapshots as keyed, so a key
        given is sent on every request."""
        if testnet and api_url == API_URL:
            api_url = TESTNET_API_URL
        if client is None:
            headers = {"x-sx-api-key": api_key} if api_key else {}
            client = httpx.Client(timeout=timeout, follow_redirects=True, headers=headers)
        self.http = HttpClient(api_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)

    def _get(self, path: str, params: Any = None, *, what: str = "") -> Any:
        """The `data` of a response; the venue wraps every answer in
        `{"status": ..., "data": ...}`."""
        try:
            payload = self.http.get(path, params)
        except MarketNotFound as exc:
            raise MarketNotFound(f"{VENUE}: no {what or 'market'}", body=exc.body, status=exc.status) from None
        if isinstance(payload, dict) and payload.get("status") == "failure":
            raise ExchangeError(f"{VENUE}: {path} failed: {payload}")
        return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload

    # -- catalog ------------------------------------------------------------

    def _listing(self, params: dict[str, Any], key: str | None) -> tuple[list[dict[str, Any]], str | None]:
        """One page of the active listing, and the key to the next. A short
        page can still have a next one, so only a missing key ends the walk."""
        data = self._get("/markets/active", {**params, "pageSize": PAGE, "paginationKey": key}) or {}
        return list(data.get("markets") or []), data.get("nextKey") or None

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = None,
    ) -> Page[Market]:
        """One page of open markets, in the venue's order. With `query`, the
        fixtures whose team names match (at most eight), every market on each."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: the venue lists only open markets; look up a settled one with fetch_market")
        check_sort(sort, venue=VENUE, supported=False)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        if query:
            rows = [row for event_id in self._search(query) for row in self._event_rows(event_id)]
            return Page([normalize_market(r) for r in rows][:wanted], next_cursor=None)
        key, skip = _cursor(cursor)
        out: list[Market] = []
        for _ in range(MAX_PAGES):
            rows, following = self._listing({}, key)
            batch = [normalize_market(r) for r in rows if r.get("marketHash")][skip:]
            room = wanted - len(out)
            out += batch[:room]
            if len(batch) > room:
                return Page(out, next_cursor=f"{key or ''}:{skip + room}")
            if not following:
                return Page(out, next_cursor=None)
            key, skip = following, 0
            if len(out) >= wanted:
                return Page(out, next_cursor=f"{key}:0")
        return Page(out, next_cursor=f"{key}:0")

    def _search(self, query: str) -> list[str]:
        if len(query.strip()) < 3:
            raise BadRequest(f"{VENUE}: a search needs at least 3 characters")
        found = self._get("/search", {"query": query.strip()[:100]}) or []
        return [str(f["eventId"]) for f in found if isinstance(f, dict) and f.get("eventId")]

    def _event_rows(self, event_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        key = None
        for _ in range(MAX_PAGES):
            page, key = self._listing({"eventId": event_id}, key)
            rows += page
            if not key:
                break
        return rows

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """Open fixtures with their markets, one league per page (the cursor
        walks the venue's active leagues). With `query`, the fixtures whose
        team names match."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: the venue lists only open events")
        wanted = page_limit(limit)
        if query:
            events = [normalize_event(rows) for rows in (self._event_rows(e) for e in self._search(query)) if rows]
            return Page(events[:wanted] if wanted else events, next_cursor=None)
        leagues = self._active_leagues()
        index = int(cursor) if cursor and cursor.isdigit() else 0
        if cursor and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        if index >= len(leagues):
            return Page([], next_cursor=None)
        rows: list[dict[str, Any]] = []
        key = None
        for _ in range(MAX_PAGES):
            page, key = self._listing({"leagueId": leagues[index]}, key)
            rows += page
            if not key:
                break
        events = events_of(rows)
        following = str(index + 1) if index + 1 < len(leagues) else None
        return Page(events[:wanted] if wanted else events, next_cursor=following)

    def _active_leagues(self) -> list[int]:
        data = self._get("/leagues/active") or []
        return [int(row["leagueId"]) for row in data if isinstance(row, dict) and row.get("leagueId") is not None]

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor:
                return

    def _find(self, hashes: list[str]) -> list[dict[str, Any]]:
        """The venue's rows for these hashes; malformed ones are skipped."""
        hashes = [h for h in hashes if HASH.fullmatch(h)]
        rows: list[dict[str, Any]] = []
        for start in range(0, len(hashes), MAX_FIND):
            chunk = hashes[start:start + MAX_FIND]
            rows += [r for r in self._get("/markets/find", {"marketHashes": ",".join(chunk)}) or [] if isinstance(r, dict)]
        return rows

    def fetch_market(self, market_id: str) -> Market:
        """One market by hash, open or settled."""
        market_hash = self.native(market_id).lower()
        rows = self._find([market_hash])
        if not rows:
            raise MarketNotFound(f"{VENUE}: no market {market_hash}")
        return normalize_market(rows[0])

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in the order asked for, thirty a request; hashes the
        venue does not know are left out."""
        hashes = list(dict.fromkeys(self.native(m).lower() for m in market_ids))
        by_hash = {str(r["marketHash"]).lower(): normalize_market(r) for r in self._find(hashes) if r.get("marketHash")}
        return [by_hash[h] for h in hashes if h in by_hash]

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with its bid and ask read from the book."""
        book = self.fetch_order_book(market.id)
        bid = book.bids[0].price if book.bids else None
        ask = book.asks[0].price if book.asks else None
        mid = round((bid + ask) / 2, 8) if bid is not None and ask is not None else None
        yes = market.yes.model_copy(update={"quote": Quote(
            bid=bid, ask=ask, mid=mid,
            bid_size=book.bids[0].size if book.bids else None, ask_size=book.asks[0].size if book.asks else None,
        )})
        no = market.no.model_copy(update={"quote": Quote(
            bid=round(1 - ask, 8) if ask is not None else None, ask=round(1 - bid, 8) if bid is not None else None,
            mid=round(1 - mid, 8) if mid is not None else None,
            bid_size=book.asks[0].size if book.asks else None, ask_size=book.bids[0].size if book.bids else None,
        )})
        return market.model_copy(update={"yes": yes, "no": no})

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        market_hash = self.native(market_id).lower()
        payload = self._get("/orderbook-v3/snapshot", {"marketHash": market_hash},
                            what=f"order book for {market_hash}") or {}
        if not isinstance(payload, dict):
            raise ExchangeError(f"{VENUE}: unexpected order book for {market_hash}")
        return normalize_order_book(payload, market_id=self.qualify(market_hash), side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, one request each (the venue's batch read needs a key)."""
        return {self.qualify(self.native(m).lower()): self.fetch_order_book(m, side=side, depth=depth)
                for m in dict.fromkeys(market_ids)}

    def _tape(self, market_hash: str, *, per_page: int, key: str | None) -> tuple[list[dict[str, Any]], str | None]:
        data = self._get("/trades-v3/public", {"marketHash": market_hash, "perPage": per_page, "nextKey": key}) or {}
        return list(data.get("trades") or []), data.get("nextKey") or None

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """The market's taker bets, a page newest first from the venue and
        returned oldest first, in the YES price with the taker's side."""
        market_hash = self.native(market_id).lower()
        rows, following = self._tape(market_hash, per_page=page_limit(limit) or 50, key=cursor)
        trades = [normalize_trade(row, market_id=self.qualify(market_hash)) for row in rows if not row.get("isParlay")]
        if since:
            trades = [t for t in trades if t.timestamp >= since]
        return Page(sorted(trades, key=lambda t: t.timestamp), next_cursor=following)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price built from the venue's tape of taker bets,
        with their volume in contracts (`price_source="trade"`). The tape is
        read back page by page until `since`, at most 2,000 bets."""
        market_hash = self.native(market_id).lower()
        seconds = timeframe_seconds(timeframe)
        trades: list[Trade] = []
        key = None
        for _ in range(MAX_TRADE_PAGES):
            rows, key = self._tape(market_hash, per_page=100, key=key)
            batch = [normalize_trade(r, market_id=self.qualify(market_hash)) for r in rows if not r.get("isParlay")]
            trades += batch
            if not key or (since and batch and min(t.timestamp for t in batch) < since):
                break
        candles = candles_from_trades(trades, interval_seconds=seconds)
        if since:
            candles = [c for c in candles if c.timestamp + seconds * 1000 > since]
        if until:
            candles = [c for c in candles if c.timestamp <= until]
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        return fee_schedule_of(self.native(market_id).lower())

    def close(self) -> None:
        self.http.close()


def _cursor(cursor: str | None) -> tuple[str | None, int]:
    """`key:skip` — the listing page's key (empty for the first page), and how
    many markets of it were already returned."""
    if not cursor:
        return None, 0
    key, sep, skip = cursor.rpartition(":")
    if not sep:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    try:
        return key or None, max(0, int(skip or 0))
    except ValueError:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued") from None
