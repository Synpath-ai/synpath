"""Polymarket: Gamma for the catalog, CLOB for books, Data API for trades.

Public reads, no credentials.

Three things this adapter does that a thin wrapper does not:

**Prices come from the book, not the catalog.** Gamma's `bestBid`, `bestAsk`
and `outcomePrices` are cached summaries that lag during fast trading — on an
in-play match Gamma has read 39c while the CLOB book asked 52c at the same
instant, and the site itself shows the book. Catalog calls carry Gamma's
numbers because pulling a book per market would cost one request each;
`fetch_order_book` and `refresh_quotes` read the real book.

**The CLOB returns bids ascending and asks descending.** Best bid is the *last*
bid, best ask the *last* ask. Reading `bids[0]` gives the worst price on the
book, which is a quiet way to mis-price everything.

**Each instrument owns its book.** Unlike Kalshi, YES and NO are separate CLOB
tokens with independently addressable order books, so nothing here is derived.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterator

from . import ids
from .base import (
    Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status,
    enough_bars, page_limit, pick_bars, timeframe_seconds,
)
from .errors import BadRequest, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, Outcome,
    OrderLevel, Page, Quote, Trade, iso,
)

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
DATA_URL = "https://data-api.polymarket.com"
VENUE = "polymarket"

LIMITER = RateLimiter(20.0, burst=20)
"""Gamma took 30 requests at 43/s without complaint, so this is headroom rather
than a measured ceiling: present so an undocumented limit throttles us instead
of failing us. Shared process-wide, as Kalshi's is."""

BOOK_BATCH = 500
"""Tokens per POST /books. 500 answered in 0.32s; 672 was "Payload exceeds the limit"."""

MARKET_BATCH = 100
"""Gamma enforces this: 200 ids returns `422 expected array length <= 100`."""

TRADE_PAGE = 500
"""Rows per Data API `/trades` call, its maximum."""

MAX_TRADE_PAGES = 20
"""Most tape pages one `fetch_ohlcv` call reads: 10,000 trades. A hot market
was measured printing 500 trades in 68 minutes, so this is a few hours of
one and weeks of a quiet one. Past it the call stops and marks the earliest
bar incomplete rather than walking the tape indefinitely."""

QUOTE_WINDOW = 14 * 86400
"""Widest `startTs`..`endTs` span, in seconds, one `prices-history` request is
sent. The venue refuses a wider one (`interval is too long`; 15 days measured
fine, 16 refused), so longer windows are read in pieces and joined."""

DEFAULT_BARS = 100
"""Bars looked back when neither `since` nor `limit` bounds the window."""

SEARCH_PAGE = 50
"""Events per `/public-search` page: its maximum; a larger `limit_per_type`
is answered with 50."""

MAX_SEARCH_PAGES = 5
"""Most search pages one `fetch_markets(query=...)` call reads while filling
`limit` markets. Past it the call returns what it has, with a cursor."""

MAX_TRADE_OFFSET = 10_000
"""The deepest offset the Data API's `/trades` accepts; past it the venue
answers `max historical trades offset of 10000 exceeded`."""


SORT_FIELDS = {"volume": "volume24hr", "liquidity": "liquidityNum", "newest": "startDate"}
"""This library's sort keys as Gamma's own field names."""

from .base import MAX_PAGE_LIMIT  # noqa: E402  (kept next to the cap it mirrors)
"""Gamma's keyset endpoints cap a page at 100 rows whatever is asked for, which
is why `MAX_PAGE_LIMIT` is 100 for every venue."""


# ---------------------------------------------------------------------------
# Pure normalizers
# ---------------------------------------------------------------------------

def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ts(value: Any) -> int | None:
    """Gamma sends ISO strings; the CLOB sends epoch milliseconds as a string;
    the Data API sends epoch seconds as a number. All three appear here."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        # Seconds or milliseconds, told apart by magnitude: 10^11 seconds is
        # the year 5138, so anything larger is already milliseconds.
        return int(value if value > 1e11 else value * 1000)
    text = str(value)
    if text.isdigit():
        return parse_ts(int(text))
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def json_list(value: Any) -> list[Any]:
    """Gamma returns several array fields as JSON-encoded strings."""
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def quoted(value: Any) -> float | None:
    """A price, or `None` when the book is empty on that side.

    Polymarket reports an empty side as 0 or 1 the way Kalshi does. A resting
    order sits strictly inside.
    """
    price = to_float(value)
    if price is None or price <= 0 or price >= 1:
        return None
    return price


def status_of(market: dict[str, Any]) -> tuple[str, str | None, bool]:
    """(normalized status, native status, accepting orders)."""
    accepting = bool(market.get("acceptingOrders"))
    if market.get("closed"):
        statuses = [str(s).lower() for s in json_list(market.get("umaResolutionStatuses"))]
        resolved = market.get("umaResolutionStatus") == "resolved" or "resolved" in statuses
        return ("settled" if resolved else "closed"), "closed", False
    if market.get("active"):
        return "open", "active", accepting
    return "unopened", "inactive", False


def fee_schedule_of(market: dict[str, Any]) -> FeeSchedule | None:
    """Polymarket's published per-market fee, when fees are switched on.

    The venue charges `C * rate * (P * (1 - P)) ** exponent` to the taker
    at match time (its fee docs, and the V2 client's own arithmetic), so it
    is normalized to `quadratic_theta`. Makers are never charged: with
    `takerOnly` the maker rate is a known zero, not an unknown. `rebateRate`
    is the share of fees paid back to makers daily, not a per-trade figure,
    and stays in `info`.
    """
    if not market.get("feesEnabled"):
        return None
    schedule = market.get("feeSchedule") or {}
    rate = to_float(schedule.get("rate"))
    if rate is None:
        return None
    taker_only = bool(schedule.get("takerOnly"))
    return FeeSchedule(
        venue=VENUE,
        scope="market",
        scope_id=str(market.get("id") or ""),
        fee_type="quadratic_theta",
        taker_rate=rate,
        maker_rate=0.0 if taker_only else rate,
        exponent=to_float(schedule.get("exponent")),
        info={"feeType": market.get("feeType"), "feeSchedule": schedule},
    )


def settlement_sources_of(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Gamma's `resolutionSource`, in the shape every venue reports here.

    It is one free-text field rather than a list, and it is often a bare URL
    and often empty. Normalized into `{"name", "url"}` so a caller reads one
    shape across venues; an empty field stays an empty list rather than
    becoming a source named "".
    """
    source = (raw.get("resolutionSource") or "").strip()
    if not source:
        return []
    return [{"name": source, "url": source if source.startswith("http") else None}]


def normalize_market(market: dict[str, Any], event: dict[str, Any] | None = None) -> Market:
    """One Gamma market payload as a unified `Market`."""
    event = event or {}
    market_id = str(market["id"])
    status, native, accepting = status_of(market)
    labels = [str(o) for o in json_list(market.get("outcomes"))] or ["Yes", "No"]
    token_ids = [str(t) for t in json_list(market.get("clobTokenIds"))]
    prices = [to_float(p) for p in json_list(market.get("outcomePrices"))]

    best_bid = quoted(market.get("bestBid"))
    best_ask = quoted(market.get("bestAsk"))
    last = quoted(market.get("lastTradePrice"))
    last_ts = parse_ts(market.get("updatedAt"))
    change = to_float(market.get("oneDayPriceChange"))

    if len(labels) != 2:
        raise BadRequest(f"polymarket: market {market_id} has {len(labels)} outcomes; a Synpath market is binary")

    def outcome(index: int) -> Outcome:
        token = token_ids[index] if index < len(token_ids) else None
        # Gamma publishes one summary bid/ask pair, for the first outcome only.
        # Giving the complement a mirrored copy would be inventing a quote the
        # venue never sent, so the other side carries only its own price until
        # someone asks for its book.
        if index == 0:
            bid, ask = best_bid, best_ask
            outcome_last = last
        else:
            bid = ask = None
            outcome_last = round(1 - last, 6) if last is not None else None
        mid = round((bid + ask) / 2, 6) if bid is not None and ask is not None else None
        # `outcomePrices` is Gamma's own mark for the outcome — a price, but not
        # one of the four the book defines, so it is kept in `info` rather than
        # promoted into a field it would misrepresent.
        return Outcome(
            label=labels[index],
            quote=Quote(
                bid=bid, ask=ask, mid=mid,
                last=outcome_last,
                last_timestamp=last_ts if outcome_last is not None else None,
                last_datetime=iso(last_ts) if outcome_last is not None else None,
            ),
            venue_token_id=token,
            price_change_24h=change if index == 0 else (-change if change is not None else None),
            info={"gamma_outcome_price": prices[index] if index < len(prices) else None},
        )

    event_slug = event.get("slug") or ""
    slug = market.get("slug") or ""
    url = (
        f"https://polymarket.com/event/{event_slug}/{slug}" if event_slug and slug
        else f"https://polymarket.com/market/{slug}" if slug else None
    )
    tags = [
        str(tag.get("slug") or tag.get("label") or "").lower()
        for tag in (event.get("tags") or []) if isinstance(tag, dict)
    ]
    return Market(
        id=ids.qualify(VENUE, market_id),
        venue=VENUE,
        venue_market_id=market_id,
        event_id=ids.qualify(VENUE, str(event["id"])) if event.get("id") is not None else None,
        title=market.get("question") or market_id,
        description=market.get("description"),
        slug=slug or None,
        yes=outcome(0),
        no=outcome(1),
        status=status,  # type: ignore[arg-type]
        native_status=native,
        active=accepting,
        open_timestamp=parse_ts(market.get("startDate")),
        open_datetime=iso(parse_ts(market.get("startDate"))),
        close_timestamp=parse_ts(market.get("endDate")),
        close_datetime=iso(parse_ts(market.get("endDate"))),
        resolution_timestamp=parse_ts(market.get("endDate")),
        resolution_datetime=iso(parse_ts(market.get("endDate"))),
        tick_size=to_float(market.get("orderPriceMinTickSize")),
        face_value=1.0,
        book_model="native_per_outcome",
        stats=MarketStats(
            volume_24h=to_float(market.get("volume24hr")),
            volume_total=to_float(market.get("volumeNum")) or to_float(market.get("volume")),
            liquidity=to_float(market.get("liquidityNum")) or to_float(market.get("liquidity")),
            # Gamma publishes no open interest for a market.
            open_interest=None,
            # Both figures are USDC notional here, which is *not* the unit
            # Kalshi reports volume in. Comparing the two raw numbers across
            # venues is a category error, so each says which it is.
            volume_unit="collateral",
            liquidity_unit="collateral",
            as_of=parse_ts(market.get("updatedAt")),
        ),
        url=url,
        image_url=market.get("image") or market.get("icon"),
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=market.get("groupItemTitle") or None,
        neg_risk=market.get("negRisk"),
        settlement_sources=settlement_sources_of(market) or settlement_sources_of(event),
        info=market,
    )


def normalize_event(event: dict[str, Any]) -> Event:
    markets = [normalize_market(m, event) for m in (event.get("markets") or [])]
    statuses = {m.status for m in markets}
    for candidate in ("open", "closed", "settled", "unopened"):
        if candidate in statuses:
            status = candidate
            break
    else:
        status = "unopened"
    tags = [
        str(tag.get("slug") or tag.get("label") or "").lower()
        for tag in (event.get("tags") or []) if isinstance(tag, dict)
    ]
    event_id = str(event["id"])
    return Event(
        id=ids.qualify(VENUE, event_id),
        venue=VENUE,
        venue_event_id=event_id,
        title=event.get("title") or event_id,
        description=event.get("description"),
        slug=event.get("slug"),
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status="closed" if event.get("closed") else "active" if event.get("active") else None,
        category=(tags or [None])[0],
        tags=tags,
        mutually_exclusive=(
            bool(event["negRisk"]) if event.get("negRisk") is not None else None
        ),
        close_timestamp=parse_ts(event.get("endDate")),
        close_datetime=iso(parse_ts(event.get("endDate"))),
        url=f"https://polymarket.com/event/{event.get('slug')}" if event.get("slug") else None,
        image_url=event.get("image") or event.get("icon"),
        settlement_sources=settlement_sources_of(event),
        info={k: v for k, v in event.items() if k != "markets"},
    )


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str = "", side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """A CLOB book for one token, sorted best-first, labelled with the market
    and side the token belongs to.

    The venue sends bids ascending and asks descending — worst price first on
    both sides. They are re-sorted here so `bids[0]` and `asks[0]` mean what
    every other order book in the world means.
    """
    def levels(rows: Any) -> list[OrderLevel]:
        out = []
        for row in rows or []:
            price, size = to_float(row.get("price")), to_float(row.get("size"))
            if price is None or size is None or size <= 0:
                continue
            out.append(OrderLevel(price=price, size=size))
        return out

    bids = sorted(levels(payload.get("bids")), key=lambda level: level.price, reverse=True)
    asks = sorted(levels(payload.get("asks")), key=lambda level: level.price)
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    timestamp = parse_ts(payload.get("timestamp"))
    return OrderBook(
        market_id=market_id or str(payload.get("market") or ""),
        side=side,
        venue=VENUE,
        bids=bids,
        asks=asks,
        timestamp=timestamp,
        datetime=iso(timestamp),
        book_model="native_per_outcome",
        derived=False,
        depth_scope="top_n" if depth else "full",
        info=payload,
    )


def normalize_trade(trade: dict[str, Any], *, market_id: str = "", no_token: str | None = None) -> Trade:
    """One execution in the YES price. The Data API reports a trade on the
    token it happened on; a NO trade at 0.30 is the same execution as a YES
    trade at 0.70 with the sides swapped, and is reported that way."""
    timestamp = parse_ts(trade.get("timestamp")) or 0
    side = str(trade.get("side") or "").lower()
    price = to_float(trade.get("price")) or 0.0
    on_no = no_token is not None and str(trade.get("asset") or "") == no_token
    if not on_no and no_token is None and trade.get("outcomeIndex") not in (None, 0, "0"):
        on_no = True
    if on_no:
        price = round(1 - price, 6)
        side = {"buy": "sell", "sell": "buy"}.get(side, side)
    return Trade(
        id=str(trade.get("transactionHash") or trade.get("id") or ""),
        market_id=market_id or str(trade.get("conditionId") or ""),
        timestamp=timestamp,
        datetime=iso(timestamp) or "",
        price=price,
        amount=to_float(trade.get("size")) or 0.0,
        side=side if side in ("buy", "sell") else "unknown",
        info=trade,
    )


def candles_from_price_history(
    history: list[dict[str, Any]], *, interval_seconds: int,
) -> list[Candle]:
    """Bucket Polymarket's price samples into bars.

    The venue publishes no candles — `prices-history` returns `{t, p}` samples
    of the midpoint at a fixed cadence, with no volume and no trade count. What
    comes back from bucketing them is therefore not an OHLCV bar: open, high,
    low and close are of the *quoted* price rather than executions, and volume
    is `None` rather than 0. Every bar says so in `price_source`.
    """
    buckets: dict[int, list[float]] = {}
    for point in history:
        seconds = to_float(point.get("t"))
        price = to_float(point.get("p"))
        if seconds is None or price is None:
            continue
        start = int(seconds // interval_seconds) * interval_seconds
        buckets.setdefault(start, []).append(price)

    candles = []
    for start in sorted(buckets):
        prices = buckets[start]
        candles.append(Candle(
            timestamp=start * 1000,
            datetime=iso(start * 1000) or "",
            open=prices[0], high=max(prices), low=min(prices), close=prices[-1],
            volume=None,
            trade_count=None,
            price_source="sampled_mid",
            info={"samples": len(prices)},
        ))
    return candles


def candles_from_trades(
    trades: list[dict[str, Any]], *, yes_token: str, interval_seconds: int,
    face_value: float = 1.0,
) -> list[Candle]:
    """Bars built from executions, in the YES price.

    A Polymarket market trades as two tokens, YES and NO, and the Data API
    returns both under the market's condition id with `asset` and
    `outcomeIndex` on each row. A trade on the other token is the same
    execution seen from the other side, so it is folded in at `face_value -
    price` rather than dropped -- the way an exchange-wide tape would show it,
    and the way Predexon's condition-level candles combine the two. Volume is
    in contracts (the venue's `size`), and `trade_count` counts every
    execution in the bar, both tokens.
    """
    buckets: dict[int, list[tuple[float, float, float]]] = {}
    for trade in trades:
        seconds = to_float(trade.get("timestamp"))
        price = to_float(trade.get("price"))
        size = to_float(trade.get("size"))
        if seconds is None or price is None or size is None:
            continue
        if str(trade.get("asset") or "") != yes_token:
            price = round(face_value - price, 6)
        start = int(seconds // interval_seconds) * interval_seconds
        buckets.setdefault(start, []).append((seconds, price, size))

    candles = []
    for start in sorted(buckets):
        rows = sorted(buckets[start], key=lambda row: row[0])
        prices = [row[1] for row in rows]
        candles.append(Candle(
            timestamp=start * 1000,
            datetime=iso(start * 1000) or "",
            open=prices[0], high=max(prices), low=min(prices), close=prices[-1],
            volume=round(sum(row[2] for row in rows), 6),
            trade_count=len(rows),
            price_source="trade",
            info={},
        ))
    return candles


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Polymarket(Exchange):
    """Polymarket, read-only.

    ```python
    import synpath

    poly = synpath.Polymarket()
    markets = poly.fetch_markets(query="Fed", limit=5)
    book = poly.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Polymarket"
    book_model = "native_per_outcome"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        "sort": True,
        "fetch_order_book": True,
        "fetch_order_books": True,
        "fetch_trades": True,
        # Built from the Data API's trade tape, both tokens folded into the
        # asked-for side, with volume and a trade count. The venue's own
        # `prices-history` is quote samples and is reachable as an option.
        "fetch_ohlcv": True,
        # No series tier on this venue.
        "fetch_series": False,
        "fetch_fee_schedule": True,
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
        gamma_url: str = GAMMA_URL,
        clob_url: str = CLOB_URL,
        data_url: str = DATA_URL,
        timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER,
        client: Any = None,
    ):
        self.gamma = HttpClient(gamma_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.clob = HttpClient(clob_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.data = HttpClient(data_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self._tokens: dict[str, tuple[str, str, str]] = {}
        """Per market: `(yes_token, no_token, condition_id)`, filled by every catalog read."""

    # -- catalog ------------------------------------------------------------

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of events with their markets nested.

        With `query`, this uses Gamma's server-side search, paged by its page
        number (up to `SEARCH_PAGE` events a page) and filtered to `status`.
        Without one it walks the keyset cursor: the plain `/events` endpoint
        rejects offset beyond 2000 and says in the error to use keyset, so
        keyset is the only complete way through the catalog.
        """
        if query:
            page_no, _ = _search_cursor(cursor)
            events, more = self._search_page(query, page_no, status=status,
                                             size=min(page_limit(limit) or SEARCH_PAGE, SEARCH_PAGE))
            return Page(events, next_cursor=_encode_search_cursor(page_no + 1, 0) if more else None)

        active, closed = _status_flags(status)
        wanted = page_limit(limit)
        payload = self.gamma.get("/events/keyset", {
            "limit": wanted or MAX_PAGE_LIMIT,
            "active": active,
            "closed": closed,
            "after_cursor": cursor,
        })
        events = [normalize_event(e) for e in (payload.get("events") or [])]
        return Page(
            events[:wanted] if wanted else events,
            next_cursor=payload.get("next_cursor"),
        )

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = "volume",
    ) -> Page[Market]:
        """One page of markets, by default the highest 24h volume first.

        Paged through `/markets/keyset`, not the plain `/markets` endpoint: the
        latter offers only `offset` and rejects it past 2000, so it cannot walk
        the catalog. The keyset cursor keeps the requested ordering.

        With `query` this searches events and flattens their markets, since
        Gamma's search is over events, keeping the markets in `status`. It
        reads search pages until it has `limit` markets (at most
        `MAX_SEARCH_PAGES` a call) and returns a cursor for the rest.
        """
        if query:
            wanted = page_limit(limit) or MAX_PAGE_LIMIT
            page_no, skip = _search_cursor(cursor)
            collected: list[Market] = []
            next_cursor: str | None = None
            for _ in range(MAX_SEARCH_PAGES):
                events, more = self._search_page(query, page_no, status=status, size=SEARCH_PAGE)
                markets = _with_status([m for event in events for m in event.markets], status)[skip:]
                room = wanted - len(collected)
                collected += markets[:room]
                if len(markets) > room:
                    next_cursor = _encode_search_cursor(page_no, skip + room)
                    break
                if not more:
                    next_cursor = None
                    break
                page_no, skip = page_no + 1, 0
                next_cursor = _encode_search_cursor(page_no, 0)
                if len(collected) >= wanted:
                    break
            return Page(collected, next_cursor=next_cursor)
        active, closed = _status_flags(status)
        wanted = page_limit(limit)
        order = SORT_FIELDS[check_sort(sort, venue=VENUE, supported=True)] if sort else None
        payload = self.gamma.get("/markets/keyset", {
            "limit": wanted or MAX_PAGE_LIMIT,
            "active": active,
            "closed": closed,
            "order": order,
            "ascending": "false" if order else None,
            "after_cursor": cursor,
        })
        rows = payload.get("markets") or [] if isinstance(payload, dict) else payload
        markets = [normalize_market(market) for market in rows]
        return Page(
            markets[:wanted] if wanted else markets,
            next_cursor=payload.get("next_cursor") if isinstance(payload, dict) else None,
        )

    def _search_page(self, query: str, page_no: int, *, status: str, size: int) -> tuple[list[Event], bool]:
        """One page of Gamma's event search, in `status`, and whether the
        venue has more. `events_status=active` narrows the search itself;
        Gamma ignores its `closed` counterpart, so closed is filtered here."""
        _status_flags(status)
        payload = self.gamma.get("/public-search", {
            "q": query, "limit_per_type": size, "page": page_no,
            "events_status": "active" if status == "open" else None,
        })
        events = [normalize_event(e) for e in (payload.get("events") or [])]
        if status != "all":
            events = [e.model_copy(update={"markets": _with_status(e.markets, status)}) for e in events]
            events = [e for e in events if e.markets]
        more = bool((payload.get("pagination") or {}).get("hasMore"))
        return events, more

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets, in the order asked for, closed ones included.

        Gamma caps a batch at 100, and its batch lookup quietly leaves out
        closed markets unless asked for them with `closed=true`, so the ids a
        first pass did not find are asked for again that way.
        """
        natives = [self.native(market_id) for market_id in market_ids]
        found: dict[str, Market] = {}
        for closed in (None, "true"):
            missing = [n for n in dict.fromkeys(natives) if n not in found]
            for start in range(0, len(missing), MARKET_BATCH):
                batch = missing[start:start + MARKET_BATCH]
                params = [("id", market_id) for market_id in batch] + [("limit", len(batch))]
                payload = self.gamma.get("/markets", params + ([("closed", closed)] if closed else []))
                for raw in payload if isinstance(payload, list) else []:
                    if raw.get("id") is not None:
                        found[str(raw["id"])] = self._remember(normalize_market(raw))
        return [found[market_id] for market_id in natives if market_id in found]

    def fetch_market(self, market_id: str) -> Market:
        native = self.native(market_id)
        payload = self.gamma.get(f"/markets/{native}")
        if not payload or not isinstance(payload, dict) or "id" not in payload:
            raise MarketNotFound(f"polymarket: no market {native}")
        return self._remember(normalize_market(payload))

    # -- tokens ---------------------------------------------------------------

    def _remember(self, market: Market) -> Market:
        """Keep the token ids behind a market, so a later book or tape call
        for it needs no catalog round trip."""
        if market.yes.venue_token_id and market.no.venue_token_id:
            self._tokens[market.venue_market_id] = (
                market.yes.venue_token_id, market.no.venue_token_id,
                str(market.info.get("conditionId") or ""),
            )
        return market

    def _tokens_of(self, market_id: str) -> tuple[str, str, str]:
        """`(yes_token, no_token, condition_id)` for a market, from the cache
        or one catalog read. Every venue call that touches a book or the tape
        goes through here: the venue keys those on tokens and condition ids,
        the caller holds the market id."""
        native = self.native(market_id)
        if native not in self._tokens:
            self.fetch_market(native)
        if native not in self._tokens:
            raise MarketNotFound(f"polymarket: market {native} publishes no token ids")
        return self._tokens[native]

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        while True:
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor or not page:
                return

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        """The live CLOB book on one side of a market. YES and NO are separate
        tokens here with separate books, so `side="no"` is a real book, not a
        reflection."""
        yes_token, no_token, _ = self._tokens_of(market_id)
        token = _token_for(side, yes_token, no_token)
        payload = self.clob.get("/book", {"token_id": token})
        if payload.get("error"):
            raise MarketNotFound(f"polymarket: {payload['error']} ({market_id} {side})")
        payload.setdefault("asset_id", token)
        return normalize_order_book(payload, market_id=self.qualify(market_id), side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets in one round trip, keyed by Synpath market id.

        Worth using whenever more than a couple of books are needed: 500 books
        answer in about a third of a second, where the same 500 one at a time
        is 500 requests against a shared rate budget. Markets not yet seen by
        this client cost one catalog read each first, for their token ids.
        """
        tokens: dict[str, str] = {}
        for market_id in market_ids:
            yes_token, no_token, _ = self._tokens_of(market_id)
            tokens[_token_for(side, yes_token, no_token)] = self.qualify(market_id)
        return {
            tokens[token]: book
            for token, book in self.fetch_order_books_by_token(list(tokens), depth=depth).items()
            if token in tokens
        }

    def fetch_order_books_by_token(self, token_ids: list[str], *, depth: int | None = None) -> dict[str, OrderBook]:
        """The venue's batch book call, keyed by CLOB token id, for a caller
        that already holds token ids (`Outcome.venue_token_id`) and wants
        both sides of many markets in one round trip. Polymarket only."""
        books: dict[str, OrderBook] = {}
        for start in range(0, len(token_ids), BOOK_BATCH):
            batch = token_ids[start:start + BOOK_BATCH]
            payload = self.clob.post("/books", json=[{"token_id": t} for t in batch])
            for raw in payload or []:
                token = str(raw.get("asset_id") or "")
                if token:
                    books[token] = normalize_order_book(raw, depth=depth)
        return books

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with every instrument's quote re-read from the live book.

        Catalog responses carry Gamma's cached summary, which lags during fast
        trading and only covers the first outcome. This replaces all of it with
        the real book, in one request for the whole market.
        """
        tokens = [o.venue_token_id for o in (market.yes, market.no) if o.venue_token_id]
        if not tokens:
            return market
        books = self.fetch_order_books_by_token(tokens)
        refreshed = market.model_copy(deep=True)
        for instrument in (refreshed.yes, refreshed.no):
            book = books.get(instrument.venue_token_id or "")
            if book is None:
                continue
            best_bid, best_ask = book.best_bid, book.best_ask
            bid = best_bid.price if best_bid else None
            ask = best_ask.price if best_ask else None
            last = quoted((book.info or {}).get("last_trade_price"))
            instrument.quote = Quote(
                bid=bid,
                bid_size=best_bid.size if best_bid else None,
                ask=ask,
                ask_size=best_ask.size if best_ask else None,
                mid=round((bid + ask) / 2, 6) if bid is not None and ask is not None else None,
                last=last if last is not None else instrument.quote.last,
                last_timestamp=book.timestamp if last is not None else instrument.quote.last_timestamp,
                last_datetime=book.datetime if last is not None else instrument.quote.last_datetime,
            )
        return refreshed

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """Executions for a market, newest page first, each page oldest first.

        `market_id` is the on-chain `conditionId`, which is what the Data API
        keys on. A Gamma numeric market id is resolved to it first.

        Paging ends (`next_cursor` null) when the tape runs out, when a page
        reaches back past `since`, or at the venue's deepest offset
        (`MAX_TRADE_OFFSET`): the Data API serves only about the newest 10,500
        trades of a market.
        """
        _, no_token, condition_id = self._tokens_of(market_id)
        offset = int(cursor) if cursor and cursor.isdigit() else 0
        if cursor and not cursor.isdigit():
            raise BadRequest(f"polymarket: {cursor!r} is not a cursor this API issued")
        wanted = min(limit or 100, TRADE_PAGE)
        payload = self.data.get("/trades", {
            "market": condition_id, "limit": wanted, "offset": offset or None,
        })
        rows = payload if isinstance(payload, list) else (payload.get("data") or [])
        trades = [normalize_trade(trade, market_id=self.qualify(market_id), no_token=no_token) for trade in rows]
        reached_since = False
        if since:
            reached_since = any(trade.timestamp < since for trade in trades)
            trades = [trade for trade in trades if trade.timestamp >= since]
        # The Data API pages by offset, so the next cursor is where this page
        # ended. Only meaningful while the tape is not being appended to
        # underneath us, which is why it is not the paging story for the catalog.
        following = offset + len(rows)
        done = len(rows) < wanted or reached_since or following > MAX_TRADE_OFFSET
        return Page(sorted(trades, key=lambda trade: trade.timestamp),
                    next_cursor=None if done else str(following))

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None, source: str = "trades",
    ) -> list[Candle]:
        """Bars for a market in the YES price, built from executions by default.

        `source="trades"` reads the Data API's tape for the market behind the
        token and aggregates it: every bar carries `price_source="trade"`,
        volume in contracts and a trade count, and trades on the market's
        other token are folded in at `1 - price`, since they are the same
        executions seen from the other side. That is how Predexon's
        condition-level candles are built, and it is the only OHLCV this venue
        can honestly be said to have.

        The tape pages by offset from the newest trade, so a call reads back
        until it passes the start of the window or runs into
        `MAX_TRADE_PAGES`. Past the cap the earliest bar returned is marked
        `info["complete"] = False`: it may be missing older trades. This is
        for the live edge of a market -- the last few bars -- not for deep
        history; a hot market prints several trades a second, and a day of it
        is more tape than one call should read.

        `source="quotes"` returns the venue's `prices-history` instead:
        sampled midpoints bucketed into bars, `price_source="sampled_mid"`,
        no volume. It reaches back further, and it is not made of trades. The
        venue answers at most about 15 days per request, so a longer window is
        read in 14-day pieces (`QUOTE_WINDOW`), one request each.
        """
        yes_token, _, condition_id = self._tokens_of(market_id)
        if source == "quotes":
            return self._quote_candles(yes_token, timeframe=timeframe, since=since, until=until, limit=limit)
        if source != "trades":
            raise BadRequest(f"polymarket: unknown source {source!r}; expected 'trades' or 'quotes'")
        seconds = timeframe_seconds(timeframe)
        end = int((until or _now_ms()) / 1000)
        start = int(since / 1000) if since else end - seconds * (limit or DEFAULT_BARS)
        trades, complete = self._trades_between(condition_id, start, end)
        candles = candles_from_trades(trades, yes_token=yes_token, interval_seconds=seconds)
        candles = [c for c in candles if start * 1000 <= c.timestamp <= end * 1000]
        if candles and not complete:
            candles[0].info["complete"] = False
        return pick_bars(candles, since=since, limit=limit)

    def _trades_between(self, condition_id: str, start: int, end: int) -> tuple[list[dict[str, Any]], bool]:
        """Trades in `[start, end]` seconds, newest page first, and whether the
        walk reached past `start` before the page cap."""
        collected: list[dict[str, Any]] = []
        for page in range(MAX_TRADE_PAGES):
            rows = self.data.get("/trades", {
                "market": condition_id, "limit": TRADE_PAGE, "offset": page * TRADE_PAGE,
            })
            rows = rows if isinstance(rows, list) else (rows.get("data") or [])
            if not rows:
                return collected, True
            for trade in rows:
                stamp = to_float(trade.get("timestamp"))
                if stamp is not None and start <= stamp <= end:
                    collected.append(trade)
            oldest = to_float(rows[-1].get("timestamp"))
            if oldest is not None and oldest < start:
                return collected, True
            if len(rows) < TRADE_PAGE:
                return collected, True
        return collected, False

    def _quote_candles(
        self, token_id: str, *, timeframe: str, since: int | None,
        until: int | None, limit: int | None,
    ) -> list[Candle]:
        seconds = timeframe_seconds(timeframe)
        params: dict[str, Any] = {"market": token_id, "fidelity": max(1, seconds // 60)}
        if since or until:
            # An absolute window whenever either end is given. `until` used to
            # be read only alongside `since`, so asking for bars ending a month
            # ago silently returned today's -- a backtest stepping back through
            # history would re-evaluate the present on every iteration and look
            # like it was working. The missing end is derived from `limit`,
            # counting back from the end the caller did give.
            end = int((until or _now_ms()) / 1000)
            start = int(since / 1000) if since else end - seconds * (limit or 100)
            samples: dict[float, dict[str, Any]] = {}
            candles: list[Candle] = []
            for piece_start in range(start, max(end, start + 1), QUOTE_WINDOW):
                piece = {**params, "startTs": piece_start, "endTs": min(piece_start + QUOTE_WINDOW, end)}
                for point in self.clob.get("/prices-history", piece).get("history") or []:
                    stamp = to_float(point.get("t"))
                    if stamp is not None:
                        samples[stamp] = point      # a sample on a piece boundary comes back twice
                history = [samples[stamp] for stamp in sorted(samples)]
                # The venue appends a sample at the current price whatever window
                # was asked for, so a request ending a month ago comes back with a
                # bar stamped today. Left in, that single bar is the one a backtest
                # would treat as the future it is trying not to see.
                candles = [c for c in candles_from_price_history(history, interval_seconds=seconds)
                           if start * 1000 <= c.timestamp <= end * 1000]
                if enough_bars(candles, since=since, limit=limit, strict=True):
                    break
        else:
            params["interval"] = "1d" if seconds <= 3600 else "max"
            history = self.clob.get("/prices-history", params).get("history") or []
            candles = candles_from_price_history(history, interval_seconds=seconds)
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        market = self.fetch_market(market_id)
        schedule = fee_schedule_of(market.info)
        if schedule is None:
            raise MarketNotFound(f"polymarket: market {market_id} publishes no fee schedule")
        return schedule

    def close(self) -> None:
        self.gamma.close()
        self.clob.close()
        self.data.close()


def _with_status(markets: list[Market], status: str) -> list[Market]:
    """The markets in `status`, judged by each one's own status. `closed` is
    Gamma's flag, which resolved markets carry too, so it keeps both."""
    if status == "all":
        return markets
    wanted = ("closed", "settled") if status == "closed" else (status,)
    return [market for market in markets if market.status in wanted]


def _search_cursor(cursor: str | None) -> tuple[int, int]:
    """`(search page, markets to skip on it)` from a search cursor."""
    if not cursor:
        return 1, 0
    page, _, skip = cursor.removeprefix("search:").partition(".")
    if not cursor.startswith("search:") or not page.isdigit() or not (skip or "0").isdigit() or int(page) < 1:
        raise BadRequest(f"polymarket: {cursor!r} is not a search cursor this API issued")
    return int(page), int(skip or 0)


def _encode_search_cursor(page: int, skip: int) -> str:
    return f"search:{page}.{skip}" if skip else f"search:{page}"


def _status_flags(status: str) -> tuple[str | None, str | None]:
    """Gamma's `active` / `closed` flags for one of the shared status words.

    `settled` is refused rather than approximated. Gamma's catalog carries no
    resolution status -- a closed market's `umaResolutionStatus` is absent from
    the listing payload -- so there is no way to filter for settled markets
    here, and the previous behaviour of falling through to "no filter" answered
    a completely different question: asking for settled markets returned live
    ones, while the same call on Kalshi returned settled ones.
    """
    check_status(status)
    if status == "open":
        return "true", "false"
    if status == "closed":
        return None, "true"
    if status == "settled":
        raise NotSupported(
            "polymarket: cannot filter by settled -- Gamma's catalog does not "
            "publish resolution status. Use status='closed' and check each "
            "market's `status` field."
        )
    return None, None


def _token_for(side: str, yes_token: str, no_token: str) -> str:
    if side == "yes":
        return yes_token
    if side == "no":
        return no_token
    raise BadRequest(f"polymarket: unknown side {side!r}; expected 'yes' or 'no'")


def _now_ms() -> int:
    from datetime import timezone
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
