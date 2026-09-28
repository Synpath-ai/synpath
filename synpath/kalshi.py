"""Kalshi Trade API v2.

Public read endpoints, no credentials needed.

Two facts about this venue shape the whole adapter:

**One book, two views.** A Kalshi binary market has a single order book. A bid
on NO at 0.88 *is* an ask on YES at 0.12 — the same resting order seen from the
other side. `/orderbook` returns two arrays, `yes_dollars` and `no_dollars`,
and both are **bids**. The YES ask side is the NO bid side reflected through
the market's face value. This adapter does that reflection explicitly and marks
the result `derived=True`, rather than pretending two books exist.

**Zero and one are placeholders, not prices.** An absent bid prints as 0, an
absent ask as 1, and a market that has never traded prints last = 0. All three
are read as `None` here. Passing them through is how a library ends up
reporting that a live market is worth nothing.
"""
from __future__ import annotations

import base64
import hashlib
import json
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Iterator

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort,
    check_status, enough_bars, page_limit, pick_bars, sort_page, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound, SynpathError
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, Outcome,
    OrderLevel, Page, Quote, Series, Trade, iso,
)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"

SEARCH_URL = "https://api.elections.kalshi.com"
"""Where Kalshi's text search lives.

Not part of the documented Trade API, which has no search at all: its `/events`
endpoint accepts a `query` parameter and silently ignores it, returning the
unfiltered first page. This is the endpoint kalshi.com's own search box calls,
on a different host -- `external-api.kalshi.com` answers 404 for it.

Being undocumented, it can change or start requiring credentials without
notice. When it fails, `search_markets` raises rather than falling back to
scanning the catalog: a scan for a term that matches nothing walks ~13,000
events, and turning a fast failure into a hundred-second one is not a
kindness.
"""

SEARCH_LIMITER = RateLimiter(2.0, burst=2)
"""Deliberately slower than the trade API's limiter. This host publishes no
limits and is not part of the documented surface, so it gets the conservative
end of the guess."""

VENUE = "kalshi"

LIMITER = RateLimiter(5.0, burst=5)
"""Kalshi rate-limits its public reads without documenting it in a header.
Measured: an unpaced burst starts returning 429 around 8 req/s, 5.2 req/s ran
clean, and a 429 clears in about a second. Module-level, so every client in the
process shares one budget — which is what the venue actually counts."""

MARKET_BATCH = 200
"""Tickers per `/markets` call. The ceiling is URI length, not a count (500 is
fine, 800 returns 414), so this sits well under it rather than relying on a
limit that depends on how long tickers happen to be."""

_SETTLED = {"settled", "determined", "finalized"}
_OPEN = {"active", "open"}
_UNOPENED = {"unopened", "initialized"}

SEARCH_PAGE = 50
"""Results per search page. The venue's own page size; asking for more is
ignored."""

EVENT_BATCH = 100
"""Event tickers per `/events?tickers=` call. A full page of search results is
at most `MAX_PAGE_LIMIT` rows, so one batch covers it; 100 tickers measured at
under 1,900 characters of URL."""

VENUE_EVENT_PAGE = 200
"""The most events Kalshi returns per `/events` call. Used when walking the
whole catalog, where there is no page contract to honour and every extra
request is spent rate budget."""

EVENT_PAGE = 25

MAX_FILL_PAGES = 10
"""Most event pages one `fetch_markets` call will read while filling a page.

A status filter can leave an event page with few matching markets, and filling
`limit` from sparse pages must not turn into walking the catalog inside one
call. Past this, the call returns what it has with a cursor to continue, which
is what a short page with a cursor means."""
"""Events fetched per underlying request while filling a market page.

Kalshi averages about nine markets per event, so 25 events covers a full
`MAX_PAGE_LIMIT` page of markets in one request most of the time, while a page
that overflows is resumed mid-way rather than re-walked.
"""

FACE_VALUE_CACHE = 2048
"""How many markets' face values to remember. Bounded because the server keeps
one adapter for the life of the process."""

CANDLE_INTERVALS = {"1m": 1, "1h": 60, "1d": 1440}
"""The only period_interval values Kalshi accepts, in minutes."""

MAX_CANDLES = 5000
"""Most bars Kalshi answers per candlestick request; a wider window is refused
with `max candlesticks: 5000` (about 3.5 days of 1m, 208 days of 1h). Longer
windows are read in pieces of `MAX_CANDLES - 1` periods and joined."""

HISTORICAL_CURSOR = "historical:"
"""Prefix of a `fetch_trades` cursor that has moved past the live tape into
`/historical/trades`. Kalshi keeps recent data on its live endpoints and moves
anything older than a cutoff (see `/historical/cutoff`) to `/historical/*`:
markets settled before it, and trades created before it, are no longer on the
live endpoints at all. A plain cursor is the live tape's own."""

ARCHIVE_STATUSES = {"settled", "all"}
"""Statuses whose listings continue past the live catalog into
`/historical/markets`, where Kalshi keeps markets settled before its cutoff.
The live `/events` endpoint still lists those events, but with no markets
inside them."""

ARCHIVE_PAGE = 1000
"""Most markets `/historical/markets` returns per call; 1001 is a 400."""

CUTOFF_TTL = 3600.0
"""Seconds to remember the historical cutoff. Kalshi moves it forward in steps
of weeks, so asking once an hour is plenty."""


# ---------------------------------------------------------------------------
# Pure normalizers. No network, no client state — a recorded payload in, a
# unified type out. Tested directly, and portable to any other runtime.
# ---------------------------------------------------------------------------

def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ts(value: Any) -> int | None:
    """Kalshi timestamps: ISO 8601 strings, or unix seconds on some endpoints."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value * 1000)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def quoted(value: Any, *, face_value: float = 1.0) -> float | None:
    """A price, or `None` when Kalshi's placeholder means the side is empty.

    Real resting orders sit strictly inside (0, face_value). A 0 means nobody
    is bidding, a face-value ask means nobody is offering, and a 0 ask has been
    observed on an otherwise empty book. None of the three is a price.
    """
    price = to_float(value)
    if price is None or price <= 0 or price >= face_value:
        return None
    return price


def status_of(market: dict[str, Any]) -> tuple[str, bool]:
    """(normalized status, accepting orders)."""
    native = str(market.get("status") or "").lower()
    if native in _SETTLED:
        return "settled", False
    if native == "closed":
        return "closed", False
    if native in _UNOPENED:
        return "unopened", False
    if native in _OPEN:
        return "open", True
    return "unopened", False


def tick_size_of(market: dict[str, Any]) -> float | None:
    """The minimum price increment, read from the venue's own price ladder.

    `price_ranges` describes the ladder as ranges with a step. A market can in
    principle use a finer step near the extremes, so the smallest step present
    is the one an order has to satisfy everywhere.
    """
    steps = [to_float(r.get("step")) for r in (market.get("price_ranges") or [])]
    valid = [s for s in steps if s]
    return min(valid) if valid else None


def market_url(series_ticker: str | None, event_ticker: str | None, ticker: str | None = None) -> str:
    """The kalshi.com page for an event; markets sit on it, there is no per-market page.

    The path is `/markets/<series>/<slug>/<event>` and the slug segment is not
    checked — any value redirects to the canonical one — so the series ticker
    stands in for it.
    """
    series = (series_ticker or (event_ticker or "").split("-")[0]).lower()
    event = (event_ticker or "").lower()
    url = f"https://kalshi.com/markets/{series}/{series}/{event}"
    return f"{url}/{ticker.lower()}" if ticker else url


def normalize_quotes(
    market: dict[str, Any], face_value: float,
) -> tuple[Quote, Quote, float | None]:
    """YES and NO quotes from one market payload, plus the 24h price change.

    The NO sizes are not published separately and do not need to be: in a
    shared book the orders resting on NO's bid are the same orders resting on
    YES's ask, so the sizes mirror along with the prices.
    """
    yes_bid = quoted(market.get("yes_bid_dollars"), face_value=face_value)
    yes_ask = quoted(market.get("yes_ask_dollars"), face_value=face_value)
    no_bid = quoted(market.get("no_bid_dollars"), face_value=face_value)
    no_ask = quoted(market.get("no_ask_dollars"), face_value=face_value)
    yes_bid_size = to_float(market.get("yes_bid_size_fp"))
    yes_ask_size = to_float(market.get("yes_ask_size_fp"))

    last = quoted(market.get("last_price_dollars"), face_value=face_value)
    last_ts = parse_ts(market.get("updated_time"))
    previous = quoted(market.get("previous_price_dollars"), face_value=face_value)
    change = round(last - previous, 6) if last is not None and previous is not None else None

    def mid(bid: float | None, ask: float | None) -> float | None:
        return round((bid + ask) / 2, 6) if bid is not None and ask is not None else None

    yes = Quote(
        bid=yes_bid, bid_size=yes_bid_size, ask=yes_ask, ask_size=yes_ask_size,
        mid=mid(yes_bid, yes_ask),
        last=last, last_timestamp=last_ts if last is not None else None,
        last_datetime=iso(last_ts) if last is not None else None,
    )
    no_last = round(face_value - last, 6) if last is not None else None
    no = Quote(
        bid=no_bid, bid_size=yes_ask_size, ask=no_ask, ask_size=yes_bid_size,
        mid=mid(no_bid, no_ask),
        last=no_last, last_timestamp=last_ts if no_last is not None else None,
        last_datetime=iso(last_ts) if no_last is not None else None,
    )
    return yes, no, change


def normalize_market(market: dict[str, Any], event: dict[str, Any] | None = None) -> Market:
    """One Kalshi market payload as a unified `Market`."""
    event = event or {}
    ticker = market["ticker"]
    face_value = to_float(market.get("notional_value_dollars")) or 1.0
    status, active = status_of(market)
    yes_quote, no_quote, change = normalize_quotes(market, face_value)
    series_id = event.get("series_ticker") or ticker.split("-")[0]
    event_ticker = market.get("event_ticker") or event.get("event_ticker")

    outcome_label = market.get("yes_sub_title") or market.get("subtitle") or None
    yes_label = outcome_label or "Yes"
    no_label = market.get("no_sub_title") or "No"
    yes = Outcome(label=yes_label, quote=yes_quote, price_change_24h=change)
    no = Outcome(label=no_label, quote=no_quote, price_change_24h=-change if change is not None else None)

    liquidity = to_float(market.get("liquidity_dollars"))
    rules = "\n\n".join(
        text for text in (market.get("rules_primary"), market.get("rules_secondary")) if text
    )
    return Market(
        id=ids.qualify(VENUE, ticker),
        venue=VENUE,
        venue_market_id=ticker,
        event_id=ids.qualify(VENUE, event_ticker) if event_ticker else None,
        title=market.get("title") or ticker,
        description=rules or None,
        slug=ticker.lower(),
        yes=yes,
        no=no,
        status=status,
        native_status=market.get("status"),
        active=active,
        market_type=market.get("market_type") or "binary",
        open_timestamp=parse_ts(market.get("open_time")),
        open_datetime=iso(parse_ts(market.get("open_time"))),
        close_timestamp=parse_ts(market.get("close_time")),
        close_datetime=iso(parse_ts(market.get("close_time"))),
        resolution_timestamp=parse_ts(market.get("expected_expiration_time")),
        resolution_datetime=iso(parse_ts(market.get("expected_expiration_time"))),
        tick_size=tick_size_of(market),
        face_value=face_value,
        book_model="shared_complement",
        stats=MarketStats(
            volume_24h=to_float(market.get("volume_24h_fp")),
            volume_total=to_float(market.get("volume_fp")),
            # Kalshi reports 0.0000 here on every open market sampled, so a zero
            # is an absent figure rather than an illiquid book. Storing the zero
            # would mean publishing a number the venue is not actually claiming.
            liquidity=liquidity or None,
            open_interest=to_float(market.get("open_interest_fp")),
            volume_unit="contracts",
            liquidity_unit="collateral",
            as_of=parse_ts(market.get("updated_time")),
        ),
        url=market_url(series_id, event_ticker, ticker),
        category=event.get("category"),
        tags=[t for t in [event.get("category")] if t],
        series_id=series_id,
        outcome_label=outcome_label,
        neg_risk=event.get("mutually_exclusive"),
        # Kalshi publishes these per event, so a market only has them when it
        # was read with its event -- which every path in this adapter does.
        settlement_sources=list(event.get("settlement_sources") or []),
        info=market,
    )


def normalize_event(event: dict[str, Any]) -> Event:
    markets = event.get("markets") or []
    unified = [normalize_market(m, event) for m in markets]
    closes = [m.close_timestamp for m in unified if m.close_timestamp]
    statuses = {m.status for m in unified}
    for candidate in ("open", "closed", "settled", "unopened"):
        if candidate in statuses:
            status = candidate
            break
    else:
        status = "unopened"
    ticker = event["event_ticker"]
    return Event(
        id=ids.qualify(VENUE, ticker),
        venue=VENUE,
        venue_event_id=ticker,
        title=event.get("title") or ticker,
        description=event.get("sub_title"),
        slug=ticker.lower(),
        markets=unified,
        status=status,  # type: ignore[arg-type]
        category=event.get("category"),
        tags=[t for t in [event.get("category")] if t],
        series_id=event.get("series_ticker"),
        mutually_exclusive=event.get("mutually_exclusive"),
        close_timestamp=max(closes) if closes else None,
        close_datetime=iso(max(closes)) if closes else None,
        settlement_sources=list(event.get("settlement_sources") or []),
        url=market_url(event.get("series_ticker"), ticker),
        info={k: v for k, v in event.items() if k != "markets"},
    )


def normalize_order_book(
    payload: dict[str, Any], *, ticker: str, side: str, face_value: float = 1.0,
    depth: int | None = None,
) -> OrderBook:
    """The shared book, presented from one side.

    `/orderbook` gives `yes_dollars` and `no_dollars`, both bid ladders. For the
    YES view: YES bids are `yes_dollars` as-is, and YES asks are the NO bids
    reflected — a NO bid at 0.88 is a YES ask at `face_value - 0.88`. The NO
    view is the same operation with the arrays swapped.
    """
    book = payload.get("orderbook_fp") or payload.get("orderbook") or {}
    own = book.get(f"{side}_dollars") or book.get(side) or []
    other = book.get(("no" if side == "yes" else "yes") + "_dollars") or []

    def levels(rows: Any) -> list[OrderLevel]:
        out = []
        for row in rows or []:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            price, size = to_float(row[0]), to_float(row[1])
            if price is None or size is None or size <= 0:
                continue
            out.append(OrderLevel(price=price, size=size))
        return out

    bids = sorted(levels(own), key=lambda level: level.price, reverse=True)
    asks = sorted(
        (OrderLevel(price=round(face_value - level.price, 6), size=level.size)
         for level in levels(other)),
        key=lambda level: level.price,
    )
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    return OrderBook(
        market_id=ids.qualify(VENUE, ticker),
        side=side,  # type: ignore[arg-type]
        venue=VENUE,
        bids=bids,
        asks=asks,
        book_model="shared_complement",
        derived=True,
        depth_scope="top_n" if depth else "full",
        info=payload,
    )


def normalize_trade(trade: dict[str, Any], *, face_value: float = 1.0) -> Trade:
    """One execution, in the YES price, with what the taker did on the YES leg.

    Kalshi prints both legs of every trade (`yes_price_dollars` and
    `no_price_dollars`) because one fill creates a position on both sides, and
    `taker_side` says which leg the aggressor bought. A taker who bought NO at
    0.30 is reported as `side="sell"` at 0.70: the same order, seen from the
    YES leg every other price in this library is quoted on.
    """
    ticker = trade.get("ticker") or ""
    taker = str(trade.get("taker_side") or trade.get("taker_outcome_side") or "").lower()
    price = to_float(trade.get("yes_price_dollars"))
    if price is None:
        no_price = to_float(trade.get("no_price_dollars"))
        price = round(face_value - no_price, 6) if no_price is not None else None
    timestamp = parse_ts(trade.get("created_time")) or 0
    return Trade(
        id=str(trade.get("trade_id") or ""),
        market_id=ids.qualify(VENUE, ticker),
        timestamp=timestamp,
        datetime=iso(timestamp) or "",
        price=price or 0.0,
        amount=to_float(trade.get("count_fp")) or to_float(trade.get("count")) or 0.0,
        side="buy" if taker == "yes" else "sell" if taker == "no" else "unknown",
        info=trade,
    )


def reflect_candle(candle: Candle, *, face_value: float = 1.0) -> Candle:
    """A YES-denominated bar seen from the NO side.

    Prices invert, and the extremes swap with them: the period's highest YES
    price is the period's *lowest* NO price. Bid and ask swap for the same
    reason — the best NO bid is the reflection of the best YES ask.
    """
    def flip(price: float | None) -> float | None:
        return round(face_value - price, 6) if price is not None else None

    return candle.model_copy(update={
        "open": flip(candle.open),
        "high": flip(candle.low),
        "low": flip(candle.high),
        "close": flip(candle.close),
        "bid_close": flip(candle.ask_close),
        "ask_close": flip(candle.bid_close),
    })


def normalize_candle(candle: dict[str, Any], *, interval_seconds: int) -> Candle:
    """One Kalshi candlestick, labelled by where its OHLC came from.

    Kalshi returns three blocks per period: `price` (executions), `yes_bid` and
    `yes_ask` (the book). In a period with no trades the `price` block carries
    only `previous_dollars` — there is no open, high, low or close, because
    nothing traded. This builds the OHLC from executions when they exist and
    falls back to the bid/ask midpoint otherwise, saying which in
    `price_source` and leaving `volume` null rather than reporting a traded
    price that does not exist.
    """
    end_ts = int(candle.get("end_period_ts") or 0)
    start_ms = (end_ts - interval_seconds) * 1000
    price = candle.get("price") or {}
    bid = candle.get("yes_bid") or {}
    ask = candle.get("yes_ask") or {}

    def field(block: dict[str, Any], name: str) -> float | None:
        # Live bars name prices `close_dollars`; bars from `/historical/...`
        # name the same dollar amounts `close`.
        value = block.get(f"{name}_dollars")
        return to_float(value if value is not None else block.get(name))

    bid_close, ask_close = field(bid, "close"), field(ask, "close")
    traded = field(price, "close")
    if traded is not None:
        ohlc = (field(price, "open"), field(price, "high"), field(price, "low"), traded)
        source = "trade"
    else:
        def midpoint(name: str) -> float | None:
            low, high = field(bid, name), field(ask, name)
            return round((low + high) / 2, 6) if low is not None and high is not None else None

        ohlc = (midpoint("open"), midpoint("high"), midpoint("low"), midpoint("close"))
        source = "bid_ask_mid"

    volume = to_float(candle.get("volume_fp") if candle.get("volume_fp") is not None else candle.get("volume"))
    return Candle(
        timestamp=start_ms,
        datetime=iso(start_ms) or "",
        open=ohlc[0], high=ohlc[1], low=ohlc[2], close=ohlc[3],
        volume=volume if source == "trade" else (volume if volume else None),
        price_source=source,  # type: ignore[arg-type]
        bid_close=bid_close,
        ask_close=ask_close,
        info=candle,
    )


def normalize_series(series: dict[str, Any]) -> Series:
    ticker = series.get("ticker") or ""
    fee = None
    if series.get("fee_type"):
        fee = FeeSchedule(
            venue=VENUE,
            scope="series",
            scope_id=ticker,
            fee_type=str(series["fee_type"]),
            multiplier=to_float(series.get("fee_multiplier")),
            rounding="up_to_cent",
            info=series,
        )
    return Series(
        id=ticker,
        venue=VENUE,
        title=series.get("title"),
        category=series.get("category"),
        tags=[str(t) for t in (series.get("tags") or [])],
        fee=fee,
        settlement_sources=list(series.get("settlement_sources") or []),
        info=series,
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Kalshi(Exchange):
    """Kalshi, read-only.

    ```python
    import synpath

    kalshi = synpath.Kalshi()
    markets = kalshi.fetch_markets(limit=10)
    book = kalshi.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Kalshi"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        # /events accepts `order=`, `sort=` and `order_by=` and ignores all
        # three, so the page is ordered here after it is read. See
        # `fetch_markets` for which venue figure each key reads.
        "sort": True,
        "fetch_order_book": True,
        # No batch endpoint: one request per market, both sides of it from
        # the same response. See `fetch_order_books`.
        "fetch_order_books": True,
        "fetch_trades": True,
        "fetch_ohlcv": True,
        "fetch_series": True,
        "fetch_fee_schedule": True,
        # Real server-side search, though on an undocumented host. See SEARCH_URL.
        "search": True,
        "watch_order_book": False,
        # Whether a market here is the same question as one somewhere else is
        # not a question this venue can be asked. See `synpath.match_market`.
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        search_url: str = SEARCH_URL,
        timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER,
        search_limiter: RateLimiter | None = SEARCH_LIMITER,
        client: Any = None,
    ):
        self.http = HttpClient(base_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.search = HttpClient(
            search_url, limiter=search_limiter, timeout=timeout, client=client, venue=VENUE,
        )
        self._face_values: OrderedDict[str, float] = OrderedDict()
        self._cutoff: tuple[float, dict[str, int]] | None = None

    # -- catalog ------------------------------------------------------------

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of events with their markets nested.

        With `query`, this asks the venue's search, the same as
        `fetch_markets(query=...)`. It used to filter a single page locally,
        so the same argument found ten markets through one method and zero
        events through the other.
        """
        if query:
            return self.search_events(query, limit=limit, cursor=cursor, status=status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        if cursor and cursor.startswith(HISTORICAL_CURSOR):
            check_status(status)
            events, more = self._archived_events(cursor.removeprefix(HISTORICAL_CURSOR) or None, wanted)
            return Page(_with_status(events, status), next_cursor=HISTORICAL_CURSOR + more if more else None)
        payload = self._event_page(cursor=cursor, status=status, limit=wanted)
        events = _with_status(_live_events(payload), status)
        next_cursor = payload.get("cursor") or None
        if next_cursor is None and status in ARCHIVE_STATUSES:
            next_cursor = HISTORICAL_CURSOR     # the archive comes next
        return Page(events[:wanted], next_cursor=next_cursor)

    def _archived_markets(self, cursor: str | None, limit: int) -> tuple[list[Market], str | None]:
        """One page of markets settled before Kalshi's historical cutoff, newest
        first, and the venue's cursor for the next."""
        payload = self.http.get("/historical/markets", {"limit": min(limit, ARCHIVE_PAGE), "cursor": cursor})
        markets = [normalize_market(raw) for raw in payload.get("markets") or []]
        return markets, payload.get("cursor") or None

    def _archived_events(self, cursor: str | None, limit: int) -> tuple[list[Event], str | None]:
        """One page of archived events: a page of archived markets grouped by
        event, each event's own fields read from the live `/events` (which
        still has them, just not their markets). An event whose markets
        straddle two pages comes back on both, each with its own markets."""
        # `limit` markets, so never more than `limit` events.
        payload = self.http.get("/historical/markets", {"limit": min(limit, ARCHIVE_PAGE), "cursor": cursor})
        grouped: dict[str, list[dict[str, Any]]] = {}
        for raw in payload.get("markets") or []:
            if raw.get("event_ticker"):
                grouped.setdefault(raw["event_ticker"], []).append(raw)
        raw_events = self._raw_events_by_ticker(list(grouped))
        events = [
            normalize_event({**raw_events.get(ticker, {"event_ticker": ticker}), "markets": markets})
            for ticker, markets in grouped.items()
        ]
        return events, payload.get("cursor") or None

    def _event_page(self, *, cursor: str | None, status: str, limit: int) -> dict[str, Any]:
        """One raw page of events from the venue."""
        check_status(status)
        return self.http.get("/events", {
            "limit": min(limit, 200),
            "with_nested_markets": "true",
            # "all" is this library's word for "do not filter", not one of
            # Kalshi's own statuses -- sending it verbatim is a 400.
            "status": None if status == "all" else status,
            "cursor": cursor,
        })

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open", sort: str | None = None,
    ) -> Page[Market]:
        """One page of markets, flattened out of the events that hold them.

        Kalshi has no market-level catalog endpoint, so this walks event pages
        and unpacks them. Within a page, markets are ordered by ticker, and the
        cursor records the last ticker returned rather than a row number. Kalshi's
        own event cursor works the same way -- it carries the last event's
        ticker -- so the whole walk resumes "after the last thing you saw".

        That is what keeps a walk correct while the catalog changes underneath
        it. A row number shifts when a market is added or removed ahead of it,
        repeating one market or skipping another; "after ticker X" does not.
        A walk never repeats a market and never skips one that existed when it
        started. A market listed mid-walk that sorts before the cursor is picked
        up on the next walk.

        `query` is handed to `search_markets`, which asks the venue.

        `sort` orders the page after it is read, because the venue ignores
        every sort parameter it accepts. It is "this page, ordered by", the
        way ccxt and pmxt do it, not "the top of the whole catalog". Keys:
        `volume` is the venue's 24-hour contract volume; `liquidity` is the
        size resting at the touch, bid plus ask, the only liquidity figure the
        catalog publishes (`liquidity_dollars` is 0 on every open market);
        `newest` is the open time. A market without the figure sorts last.
        """
        check_sort(sort, venue=VENUE, supported=bool(self.has["sort"]))
        if query:
            page = self.search_markets(query, limit=limit, cursor=cursor, status=status)
            if sort:
                return Page(sort_page(page, sort_key(sort)), next_cursor=page.next_cursor)
            return page
        check_status(status)
        wanted = page_limit(limit)
        fingerprint = query_fingerprint(status=status, kind="markets")
        event_cursor, after = decode_market_cursor(cursor, fingerprint)
        if after is not None and not isinstance(after, str):
            raise BadRequest(f"kalshi: malformed cursor {cursor!r}")
        collected: list[Market] = []
        next_cursor: str | None = None
        pages_read = 0
        live_done = bool(event_cursor and event_cursor.startswith(HISTORICAL_CURSOR))

        while not live_done:
            payload = self._event_page(
                cursor=event_cursor, status=status, limit=EVENT_PAGE,
            )
            pages_read += 1
            # Kalshi's status filter selects events, not markets: an event it
            # calls settled can still hold markets that are trading. Filtering
            # the markets themselves is what makes status="settled" return
            # settled markets, as it does on every other venue.
            markets = sorted(
                _with_status(
                    [
                        market
                        for event in (payload.get("events") or [])
                        for market in normalize_event(event).markets
                    ],
                    status,
                ),
                key=lambda market: market.venue_market_id,
            )
            if after is not None:
                markets = [market for market in markets if market.venue_market_id > after]
            room = len(markets) if wanted is None else wanted - len(collected)
            taken = markets[:max(room, 0)]
            collected.extend(taken)

            if len(taken) < len(markets):
                # Stopped part-way through this event page; resume after the
                # last market handed out, whatever else has changed meanwhile.
                last = taken[-1].venue_market_id if taken else after
                next_cursor = encode_market_cursor(event_cursor, last, fingerprint)
                break
            page_cursor = payload.get("cursor")
            if not page_cursor:
                live_done, event_cursor = True, HISTORICAL_CURSOR
                break
            event_cursor, after = page_cursor, None
            if wanted is None or len(collected) >= wanted or pages_read >= MAX_FILL_PAGES:
                next_cursor = encode_market_cursor(event_cursor, None, fingerprint)
                break

        if live_done and next_cursor is None and status in ARCHIVE_STATUSES:
            # Past the live catalog: markets settled before the historical
            # cutoff, which the live events still list but no longer contain.
            archive = (event_cursor or HISTORICAL_CURSOR).removeprefix(HISTORICAL_CURSOR) or None
            while wanted is None or len(collected) < wanted:
                if pages_read >= MAX_FILL_PAGES:
                    next_cursor = encode_market_cursor(HISTORICAL_CURSOR + (archive or ""), None, fingerprint)
                    break
                room = ARCHIVE_PAGE if wanted is None else wanted - len(collected)
                markets, more = self._archived_markets(archive, room)
                pages_read += 1
                collected.extend(_with_status(markets, status))
                if not more:
                    break
                archive = more
                if wanted is None or len(collected) >= wanted:
                    next_cursor = encode_market_cursor(HISTORICAL_CURSOR + more, None, fingerprint)
                    break

        if sort:
            collected = sort_page(collected, sort_key(sort))
        return Page(collected, next_cursor=next_cursor)

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in one request, in the order asked for.

        Kalshi's ceiling here is URI length rather than a count, so batches are
        sized by `MARKET_BATCH` instead of relying on a limit that depends on
        how long tickers happen to be. The event context each market would get
        from a listing is not fetched: this is the path a price loop uses, and
        an extra request per batch for category and tags is not what it came
        for.
        """
        tickers = [self.native(market_id) for market_id in market_ids]
        found: dict[str, Market] = {}
        for start in range(0, len(tickers), MARKET_BATCH):
            batch = tickers[start:start + MARKET_BATCH]
            payload = self.http.get(
                "/markets", {"tickers": ",".join(batch), "limit": len(batch)},
            )
            for raw in payload.get("markets") or []:
                if raw.get("ticker"):
                    found[raw["ticker"]] = normalize_market(raw)
        # Markets settled before the historical cutoff are only on `/historical/markets`.
        missing = [ticker for ticker in dict.fromkeys(tickers) if ticker not in found]
        for start in range(0, len(missing), MARKET_BATCH):
            batch = missing[start:start + MARKET_BATCH]
            payload = self.http.get(
                "/historical/markets", {"tickers": ",".join(batch), "limit": len(batch)},
            )
            for raw in payload.get("markets") or []:
                if raw.get("ticker"):
                    found[raw["ticker"]] = normalize_market(raw)
        return [found[ticker] for ticker in tickers if ticker in found]

    def fetch_market(self, market_id: str) -> Market:
        """One market, live or settled. A market that settled before Kalshi's
        historical cutoff is gone from `/markets` and read from
        `/historical/markets` instead."""
        ticker = self.native(market_id)
        raw = self._raw_market(ticker)
        if not raw:
            raise MarketNotFound(f"kalshi: no market {ticker}")
        market = normalize_market(raw, self._event_of(raw))
        self._remember_face_value(market.venue_market_id, market.face_value)
        return market

    def _raw_market(self, ticker: str) -> dict[str, Any] | None:
        """The venue's market record, from the live endpoint or, when that has
        no such market, the historical one."""
        try:
            return self.http.get(f"/markets/{ticker}").get("market")
        except MarketNotFound:
            pass
        try:
            return self.http.get(f"/historical/markets/{ticker}").get("market")
        except MarketNotFound:
            return None

    def historical_cutoff(self) -> dict[str, int]:
        """Kalshi's historical cutoffs, in epoch milliseconds: `trades` (trades
        created before it are only on `/historical/trades`) and `markets`
        (markets settled before it are only on `/historical/markets`).
        Remembered for `CUTOFF_TTL` seconds."""
        now = time.monotonic()
        if self._cutoff and now - self._cutoff[0] < CUTOFF_TTL:
            return self._cutoff[1]
        payload = self.http.get("/historical/cutoff")
        cutoff = {
            "trades": parse_ts(payload.get("trades_created_ts")) or 0,
            "markets": parse_ts(payload.get("market_settled_ts")) or 0,
        }
        self._cutoff = (now, cutoff)
        return cutoff

    def _reaches_historical_trades(self, since: int | None) -> bool:
        """Whether a trade read starting at `since` can need trades older than
        the cutoff. When the cutoff cannot be read, assume it can: one extra
        request is better than silently missing trades."""
        if since is None:
            return True
        try:
            return since < self.historical_cutoff()["trades"]
        except SynpathError:
            return True

    def _event_of(self, market: dict[str, Any]) -> dict[str, Any] | None:
        """The market's parent event, for the fields that only live there.

        `/markets/{ticker}` carries no category, tags or mutual-exclusivity, so
        without this the same market would come back with different content
        depending on whether it was fetched singly or listed -- and `neg_risk`
        would read as `unknown` rather than its real value, which under this
        library's tri-state convention means something different.

        Enrichment only: if the event cannot be read, the market is still
        returned rather than the whole call failing.
        """
        ticker = market.get("event_ticker")
        if not ticker:
            return None
        try:
            return self.http.get(f"/events/{ticker}").get("event")
        except SynpathError:
            return None

    def _remember_face_value(self, ticker: str, face_value: float) -> None:
        """Cache a face value, oldest evicted first.

        Bounded because the server holds one adapter for the life of the
        process: an unbounded dict would accumulate an entry for every market
        ever seen, including ones that settled weeks ago.
        """
        self._face_values[ticker] = face_value
        self._face_values.move_to_end(ticker)
        while len(self._face_values) > FACE_VALUE_CACHE:
            self._face_values.popitem(last=False)

    def search_markets(
        self, query: str, *, limit: int | None = None, cursor: str | None = None,
        status: str = "open",
    ) -> Page[Market]:
        """Markets matching `query`, most relevant first.

        Two requests per page, whatever the catalog size: one to the search
        endpoint, one batch call to the documented API for the full events
        behind the hits. Reading whole events rather than bare markets is what
        gives search results the same `neg_risk`, category and series a listing
        returns; search hits alone carry no mutual-exclusivity flag.

        Results stay in relevance order, so paging deep into a result set is
        best-effort: if the venue re-ranks between two calls, a market can be
        repeated or missed. For a complete walk, use `fetch_markets()` without
        a query.
        """
        check_status(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        fingerprint = query_fingerprint(q=query, status=status, kind="search")
        page_cursor, offset = decode_market_cursor(cursor, fingerprint)
        offset = 0 if offset is None else offset
        if not isinstance(offset, int):
            raise BadRequest(f"kalshi: malformed cursor {cursor!r}")

        hits, venue_next = self._search_page(query, page_cursor)
        rows: list[tuple[str, str]] = [
            (market["ticker"], hit["event_ticker"])
            for hit in hits
            if hit.get("event_ticker")
            for market in (hit.get("markets") or [])
            if market.get("ticker")
        ]
        window = rows[offset:offset + wanted]
        archived: dict[str, list[str]] = {}
        for ticker, event in window:
            archived.setdefault(event, []).append(ticker)
        found = self._events_by_ticker([event for _, event in window],
                                       archived=archived if status != "open" else None)
        by_ticker = {
            market.venue_market_id: market
            for event in found.values()
            for market in event.markets
        }
        markets = _with_status(
            [by_ticker[ticker] for ticker, _ in window if ticker in by_ticker], status,
        )
        offset += len(window)

        if offset < len(rows):
            next_cursor = encode_market_cursor(page_cursor, offset, fingerprint)
        elif venue_next:
            next_cursor = encode_market_cursor(venue_next, 0, fingerprint)
        else:
            next_cursor = None
        return Page(markets, next_cursor=next_cursor)

    def search_events(
        self, query: str, *, limit: int | None = None, cursor: str | None = None,
        status: str = "open",
    ) -> Page[Event]:
        """Events matching `query`, most relevant first, with markets nested.

        The same two requests as `search_markets`, paged by event instead of
        by market.
        """
        check_status(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        fingerprint = query_fingerprint(q=query, status=status, kind="search_events")
        page_cursor, offset = decode_market_cursor(cursor, fingerprint)
        offset = 0 if offset is None else offset
        if not isinstance(offset, int):
            raise BadRequest(f"kalshi: malformed cursor {cursor!r}")

        hits, venue_next = self._search_page(query, page_cursor)
        tickers = list(dict.fromkeys(
            hit["event_ticker"] for hit in hits if hit.get("event_ticker")
        ))
        window = tickers[offset:offset + wanted]
        archived: dict[str, list[str]] = {}
        for hit in hits:
            for market in hit.get("markets") or []:
                if hit.get("event_ticker") in window and market.get("ticker"):
                    archived.setdefault(hit["event_ticker"], []).append(market["ticker"])
        found = self._events_by_ticker(window, archived=archived if status != "open" else None)
        events = _with_status([found[ticker] for ticker in window if ticker in found], status)
        offset += len(window)

        if offset < len(tickers):
            next_cursor = encode_market_cursor(page_cursor, offset, fingerprint)
        elif venue_next:
            next_cursor = encode_market_cursor(venue_next, 0, fingerprint)
        else:
            next_cursor = None
        return Page(events, next_cursor=next_cursor)

    def _search_page(
        self, query: str, cursor: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One page of the venue's search, checked before it is trusted.

        The endpoint is undocumented, so its most likely failure is a change of
        shape that still answers 200. Read with a default, a renamed field
        becomes an empty result with no cursor -- indistinguishable from a
        term that matched nothing, which is the one wrong answer nobody would
        think to question. A genuine no-match still carries `current_page: []`,
        so requiring the field costs nothing on the honest path.
        """
        payload = self.search.get("/v1/search/series", {"query": query, "cursor": cursor})
        hits = payload.get("current_page") if isinstance(payload, dict) else None
        if not isinstance(hits, list):
            raise ExchangeError(
                "kalshi: the search endpoint answered in a shape this library does "
                "not recognise (no `current_page` list). It is undocumented and may "
                "have changed. Walk fetch_markets() without a query instead."
            )
        return hits, payload.get("next_cursor") or None

    def _events_by_ticker(
        self, tickers: list[str], *, archived: dict[str, list[str]] | None = None,
    ) -> dict[str, Event]:
        """Full events with nested markets, keyed by event ticker.

        Uses `tickers=`, not `event_ticker=`. The latter looks right, answers
        200, and is silently ignored: the venue returns its unfiltered first
        page, which reads as results rather than as an error. The batch also
        comes back in the venue's order rather than the order asked for, so the
        caller re-indexes by ticker to keep relevance order.
        """
        raw_events = self._raw_events_by_ticker(tickers)
        if archived:
            # An event settled before the historical cutoff comes back with no
            # markets; its markets are on `/historical/markets`, one batch by ticker.
            wanted = [ticker for event, found in raw_events.items() if not found.get("markets")
                      for ticker in archived.get(event, [])]
            for start in range(0, len(wanted), MARKET_BATCH):
                batch = wanted[start:start + MARKET_BATCH]
                payload = self.http.get("/historical/markets", {"tickers": ",".join(batch), "limit": len(batch)})
                for raw in payload.get("markets") or []:
                    event = raw_events.get(raw.get("event_ticker") or "")
                    if event is not None:
                        event.setdefault("markets", [])
                        event["markets"] = [*(event["markets"] or []), raw]
        return {ticker: normalize_event(raw) for ticker, raw in raw_events.items()}

    def _raw_events_by_ticker(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        """The venue's event records with nested markets, keyed by ticker."""
        unique = list(dict.fromkeys(tickers))
        found: dict[str, dict[str, Any]] = {}
        for start in range(0, len(unique), EVENT_BATCH):
            batch = unique[start:start + EVENT_BATCH]
            payload = self.http.get("/events", {
                "tickers": ",".join(batch),
                "with_nested_markets": "true",
                "limit": len(batch),
            })
            for raw in payload.get("events") or []:
                if raw.get("event_ticker"):
                    found[raw["event_ticker"]] = raw
        return found

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        """Every event the venue exposes, paging until the cursor runs out.

        Pages at the venue's maximum rather than through `fetch_events`, whose
        page size is the library's shared 100: an iterator has no page contract
        to honour, and the difference is 65 requests for the catalog against
        130.
        """
        cursor: str | None = None
        while True:
            payload = self._event_page(cursor=cursor, status=status, limit=VENUE_EVENT_PAGE)
            yield from _with_status(_live_events(payload), status)
            cursor = payload.get("cursor")
            if not cursor or not payload.get("events"):
                break
        if status not in ARCHIVE_STATUSES:
            return
        archive: str | None = None
        while True:
            events, archive = self._archived_events(archive, VENUE_EVENT_PAGE)
            yield from _with_status(events, status)
            if not archive:
                return

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        ticker = self.native(market_id)
        _check_side(side)
        payload = self.http.get(f"/markets/{ticker}/orderbook", {"depth": depth})
        return normalize_order_book(
            payload, ticker=ticker, side=side,
            face_value=self._face_value(ticker), depth=depth,
        )

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, keyed by Synpath market id.

        Kalshi has no batch endpoint, so this is one request per market,
        against the same shared rate budget every other call draws on.
        """
        _check_side(side)
        books: dict[str, OrderBook] = {}
        for ticker in dict.fromkeys(self.native(m) for m in market_ids):
            payload = self.http.get(f"/markets/{ticker}/orderbook", {"depth": depth})
            books[ids.qualify(VENUE, ticker)] = normalize_order_book(
                payload, ticker=ticker, side=side, face_value=self._face_value(ticker), depth=depth,
            )
        return books

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """Executions for a market, newest page first, each page oldest first.

        Kalshi serves recent trades on `/markets/trades` and moves those older
        than its cutoff to `/historical/trades`. Paging walks both as one tape:
        when the live tape runs out and the window reaches before the cutoff,
        the same page is filled from the historical one and the cursor carries
        on there (it then starts with `historical:`).
        """
        ticker = self.native(market_id)
        wanted = min(limit or 100, 1000)
        min_ts = int(since / 1000) if since else None
        rows: list[dict[str, Any]] = []
        next_cursor: str | None
        if cursor and cursor.startswith(HISTORICAL_CURSOR):
            next_cursor = cursor
        else:
            payload = self.http.get("/markets/trades", {
                "ticker": ticker, "limit": wanted, "min_ts": min_ts, "cursor": cursor,
            })
            rows = payload.get("trades") or []
            next_cursor = payload.get("cursor") or None
            if next_cursor is None and self._reaches_historical_trades(since):
                next_cursor = HISTORICAL_CURSOR
        if next_cursor and next_cursor.startswith(HISTORICAL_CURSOR) and len(rows) < wanted:
            payload = self.http.get("/historical/trades", {
                "ticker": ticker, "limit": wanted - len(rows), "min_ts": min_ts,
                "cursor": next_cursor.removeprefix(HISTORICAL_CURSOR) or None,
            })
            rows += payload.get("trades") or []
            more = payload.get("cursor")
            next_cursor = HISTORICAL_CURSOR + more if more else None
        face_value = self._face_value(ticker)
        trades = [normalize_trade(t, face_value=face_value) for t in rows]
        return Page(sorted(trades, key=lambda trade: trade.timestamp), next_cursor=next_cursor)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Candles for a market, in the YES price.

        Kalshi accepts three periods only — 1m, 1h and 1d. Anything else raises
        rather than being silently rounded to a period you did not ask for.
        A window wider than Kalshi's 5,000 bars per request is read in pieces
        (`MAX_CANDLES`), one request each.

        With `since`, bars are read forward from it and the first `limit` are
        returned, fetching only as many pieces as that takes. Without it,
        `limit` also sets how far back to look: the newest `limit` periods
        ending at `until` (or now). Kalshi emits a bar only for periods where
        something moved, so a thin market can return fewer bars than asked
        for -- that is the venue having no more, not a truncated response.
        A window of more than one piece is first clipped to the market's own
        life, open to close, so no request is spent on time it did not exist.

        A market that settled before Kalshi's historical cutoff has its bars on
        `/historical/markets/{ticker}/candlesticks`, read when the live
        endpoint no longer knows the market.
        """
        if timeframe not in CANDLE_INTERVALS:
            raise BadRequest(
                f"kalshi: timeframe {timeframe!r} not offered; "
                f"supported: {', '.join(CANDLE_INTERVALS)}"
            )
        ticker = self.native(market_id)
        seconds = timeframe_seconds(timeframe)
        end = int((until or _now_ms()) / 1000)
        start = int(since / 1000) if since else end - seconds * (limit or 100)
        series_id = ticker.split("-")[0]
        path = f"/series/{series_id}/markets/{ticker}/candlesticks"
        span = seconds * (MAX_CANDLES - 1)
        if end - start > span:
            start, end = self._clip_to_life(ticker, start, end, seconds)
        by_time: dict[int, Candle] = {}
        for piece_start in range(start, max(end, start + 1), span):
            if enough_bars(list(by_time), since=since, limit=limit):
                break
            window = {"start_ts": piece_start, "end_ts": min(piece_start + span, end),
                      "period_interval": CANDLE_INTERVALS[timeframe]}
            try:
                payload = self.http.get(path, window)
            except MarketNotFound:
                if path.startswith("/historical/"):
                    raise
                # Settled before the historical cutoff: its bars moved with it.
                path = f"/historical/markets/{ticker}/candlesticks"
                payload = self.http.get(path, window)
            for raw in payload.get("candlesticks") or []:
                candle = normalize_candle(raw, interval_seconds=seconds)
                by_time[candle.timestamp] = candle      # a bar on a piece boundary comes back twice
        candles = [by_time[stamp] for stamp in sorted(by_time)]
        return pick_bars(candles, since=since, limit=limit)

    def _clip_to_life(self, ticker: str, start: int, end: int, seconds: int) -> tuple[int, int]:
        """`start`..`end` (epoch seconds) narrowed to when the market existed,
        with a period of slack each side. Unchanged when the market or its
        dates cannot be read: the candle request will say why."""
        try:
            raw = self._raw_market(ticker) or {}
        except SynpathError:
            return start, end
        opened = parse_ts(raw.get("open_time") or raw.get("created_time"))
        closed = parse_ts(raw.get("close_time"))
        if opened:
            start = max(start, opened // 1000 - seconds)
        if closed:
            end = min(end, closed // 1000 + seconds)
        return start, max(start, end)

    # -- reference ----------------------------------------------------------

    def fetch_series(self, series_id: str) -> Series:
        payload = self.http.get(f"/series/{series_id}")
        raw = payload.get("series")
        if not raw:
            raise MarketNotFound(f"kalshi: no series {series_id}")
        return normalize_series(raw)

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """The fee schedule that applies to a market.

        Kalshi attaches fees to the *series*, not the market, so this resolves
        the series from the ticker and reads it there. Knowing the cost before
        trading is the difference between a price comparison and a real edge.
        """
        ticker = self.native(market_id)
        series = self.fetch_series(ticker.split("-")[0])
        if series.fee is None:
            raise MarketNotFound(f"kalshi: series {series.id} publishes no fee schedule")
        return series.fee

    # -- internals ----------------------------------------------------------

    def _face_value(self, ticker: str) -> float:
        """This market's face value, asked of the venue once and remembered.

        Both venues pay 1.00 per contract today. The value is still read rather
        than assumed, because every complement transform depends on it and a
        wrong constant would silently invert prices.
        """
        cached = self._face_values.get(ticker)
        if cached is not None:
            self._face_values.move_to_end(ticker)
            return cached
        try:
            # Deliberately not `fetch_market`, which also reads the parent event
            # for category and tags. Those are display fields a book or trade
            # read has no use for, and the extra round trip is a third of this
            # venue's measured request budget.
            raw = self._raw_market(ticker) or {}
            value = to_float(raw.get("notional_value_dollars")) or 1.0
        except MarketNotFound:
            value = 1.0
        self._remember_face_value(ticker, value)
        return value

    def close(self) -> None:
        self.http.close()
        self.search.close()


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def sort_key(sort: str):
    """The figure a sort key reads on this venue. See `Kalshi.fetch_markets`."""
    if sort == "volume":
        return lambda market: market.stats.volume_24h
    if sort == "liquidity":
        def resting_at_touch(market):
            quote = market.yes.quote if market.yes else None
            if quote is None or (quote.bid_size is None and quote.ask_size is None):
                return None
            return (quote.bid_size or 0.0) + (quote.ask_size or 0.0)
        return resting_at_touch
    return lambda market: market.open_timestamp


def _live_events(payload: dict[str, Any]) -> list[Event]:
    """The events of a live `/events` page that still hold markets. One settled
    before the historical cutoff is listed with none; it is read from the
    archive instead (see `ARCHIVE_STATUSES`), not returned empty here."""
    return [normalize_event(raw) for raw in payload.get("events") or [] if raw.get("markets")]


def _with_status(items: list, status: str) -> list:
    """Keep what is actually in `status`, judged by the item's own status.

    The venue's filter is applied first to keep pages small, but it is not the
    last word: Kalshi's selects events, so a "settled" page can carry markets
    that are still trading. Everything is checked against the normalized status
    before it is returned.
    """
    if status == "all":
        return items
    return [item for item in items if item.status == status]


def _matches(query: str, *fields: str | None) -> bool:
    needle = query.lower()
    return any(needle in (field or "").lower() for field in fields)


def query_fingerprint(**terms: Any) -> str:
    """A short, stable tag for the query a cursor belongs to."""
    material = json.dumps(terms, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(material.encode()).hexdigest()[:8]


def encode_market_cursor(
    page_cursor: str | None, position: int | str | None, fingerprint: str,
) -> str:
    """Pack "which venue page, where in it, and for which query".

    `position` is the last ticker returned for a listing, or a row offset for
    search, where relevance order has no stable key to resume after.

    Kalshi has no market-level catalog endpoint: markets are reached by paging
    events and unpacking them. A page holds a variable number of markets, so a
    cursor that only remembered the page would skip every market the caller's
    `limit` trimmed off the end -- which is what it did, losing 87% of the
    catalog on a walk with `limit=5`.

    The offset lives in the cursor rather than on the adapter, so paging
    survives a restart, works from another process, and cannot be disturbed by
    a concurrent caller. The fingerprint is what makes that safe: an offset
    counts rows of one particular query, so resuming with a different `status`
    would land somewhere arbitrary. Recording which query the offset belongs to
    turns that from silent misalignment into a clear error.
    """
    packed = json.dumps(
        {"c": page_cursor, "o": position, "f": fingerprint}, separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(packed.encode()).decode().rstrip("=")


def decode_market_cursor(
    cursor: str | None, fingerprint: str,
) -> tuple[str | None, int | str | None]:
    """`(page cursor, position in that page)`, checked against this query.

    A cursor this library did not issue is rejected rather than guessed at: the
    alternative is silently restarting the walk, which reads as duplicate data
    rather than as an error.
    """
    if not cursor:
        return None, None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        state = json.loads(base64.urlsafe_b64decode(padded.encode()))
        position = state["o"]
        page_cursor = state["c"]
        carried = state["f"]
    except Exception:
        raise BadRequest(
            f"kalshi: {cursor!r} is not a cursor this API issued; "
            f"pass the `next_cursor` from a previous page, or omit it to start over"
        ) from None
    position_ok = (
        position is None
        or isinstance(position, str)
        or (isinstance(position, int) and not isinstance(position, bool) and position >= 0)
    )
    if not position_ok or (page_cursor is not None and not isinstance(page_cursor, str)):
        raise BadRequest(f"kalshi: malformed cursor {cursor!r}")
    if carried != fingerprint:
        raise BadRequest(
            "kalshi: this cursor belongs to a different query -- the offset it "
            "carries counts rows of the search and status it was issued for. "
            "Keep those arguments the same while paging, or start over without "
            "a cursor."
        )
    return page_cursor, position


def _check_side(side: str) -> None:
    if side not in ("yes", "no"):
        raise BadRequest(f"kalshi: unknown side {side!r}; expected 'yes' or 'no'")
