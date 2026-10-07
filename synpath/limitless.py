"""Limitless: a Polymarket-style prediction market on Base, read from its REST API.

Market data needs no key. The venue caps every listing page at 25 rows.

Four things about this venue that shape the adapter:

**A market is named by its slug.** Every lookup takes the slug
(`btc-up-or-down-daily-p-1791308105925`), not the numeric id, so the slug is
the native id here: `limitless:<slug>`.

**Groups are events.** A *group* bundles several binary markets under one
question ("What price will Bitcoin hit on October 7?"), each option its own
market with a YES and a NO token; a neg-risk group's options exclude each
other. A group is an `Event` keyed by its slug. A standalone market is an
event of its own, under its own slug.

**One book, quoted on YES.** The venue serves each market's book on the YES
token; NO is the mirror. Sizes and volumes are 6-decimal integers (USDC and
shares alike).

**Fees fall as the price rises.** A taker buying pays a share of the tokens
bought: 3% up to 50c, falling to 0.4% near $1; selling costs 0.42%-1.5%.
Makers pay nothing. See `FeeSchedule.estimate` (`limitless_curve`).

A handful of older markets trade against an automated market maker instead of
a book (`info["trade_type"] == "amm"`); they have no order book.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator

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

API_URL = "https://api.limitless.exchange"
VENUE = "limitless"
WEB_URL = "https://limitless.exchange/markets/"

LIMITER = RateLimiter(5.0, burst=10)
"""The venue publishes no limit for its public endpoints; kept modest, shared
process-wide."""

PAGE = 25
"""Rows per listing page: the venue's maximum."""

MAX_PAGES = 100
"""Most listing pages one walk reads: 2,500 rows, against about 560 today."""

SCALE = 1_000_000
"""Sizes, volumes and amounts are 6-decimal integers (USDC, and shares)."""

TICK = 0.001
"""Prices are quoted to a tenth of a cent."""

STATUSES = {"FUNDED": "open", "LOCKED": "closed", "RESOLVED": "settled", "CREATED": "closed", "DRAFTED": "closed"}
"""The venue's lifecycle as this library's status. A market trades only while
`FUNDED` and not yet expired; `CREATED` and `DRAFTED` have not opened."""

SORT_BY = {"newest": "newest"}
"""This library's sort keys the venue can order the listing by. It has no
volume or liquidity order."""

LOOKBACKS = (("5m", 300), ("1h", 3600), ("6h", 6 * 3600), ("1d", 86400), ("1w", 7 * 86400), ("1m", 30 * 86400))
"""The price-history lookback presets, shortest first; `all` covers the rest."""

DEFAULT_BARS = 96


def to_float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def scaled(value: Any) -> float | None:
    number = to_float(value)
    return number / SCALE if number is not None else None


def iso_ms(value: Any) -> int | None:
    """An ISO timestamp or an epoch-millisecond number as epoch milliseconds."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) or str(value).isdigit():
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def status_of(market: dict[str, Any]) -> tuple[str, str, bool]:
    """(normalized status, native word, accepting orders)."""
    native = str(market.get("status") or "")
    status = STATUSES.get(native, "closed")
    if market.get("winningOutcomeIndex") is not None:
        status = "settled"
    elif status == "open" and market.get("expired"):
        status = "closed"
    return status, native.lower(), status == "open"


def tags_of(market: dict[str, Any]) -> list[str]:
    """The market's domain and category words, lowercase, without duplicates."""
    words: list[str] = []
    for prop in market.get("properties") or []:
        if isinstance(prop, dict) and prop.get("propertyKeySlug") == "domain":
            words += [str(v) for v in prop.get("value") or []]
    words += [str(c) for c in market.get("categories") or []]
    words += [str(t) for t in market.get("tags") or []]
    seen: dict[str, None] = {}
    for word in words:
        if word.strip():
            seen.setdefault(word.strip().lower(), None)
    return list(seen)


def quotes_of(market: dict[str, Any]) -> tuple[Quote, Quote]:
    """YES and NO quotes from the listing: the venue's prices for the two
    tokens, with no bid or ask (those come from the book)."""
    prices = market.get("prices") or []
    yes = to_float(prices[0]) if len(prices) > 0 else None
    no = to_float(prices[1]) if len(prices) > 1 else (round(1 - yes, 6) if yes is not None else None)
    return Quote(mid=yes, last=None), Quote(mid=no, last=None)


def polymarket_link(market: dict[str, Any]) -> str | None:
    """The Polymarket slug a market copies, where the venue names one."""
    meta = market.get("metadata") or {}
    if str(meta.get("externalProvider") or "").lower() == "polymarket" and meta.get("externalSlug"):
        return str(meta["externalSlug"])
    return None


def normalize_market(market: dict[str, Any], group: dict[str, Any] | None = None) -> Market:
    """One binary market. Inside a group, its `title` is the option
    ("↑ 92,000") and becomes `outcome_label`; the title is the group's
    question with the option after it."""
    slug = str(market["slug"])
    tokens = market.get("tokens") or {}
    yes_quote, no_quote = quotes_of(market)
    status, native_status, accepting = status_of(market)
    option = (market.get("title") or "").strip()
    if group:
        question = (group.get("title") or "").strip()
        title, label = (f"{question} — {option}" if question else option), option or None
        event_slug = str(group.get("slug") or slug)
    else:
        title, label, event_slug = option or slug, None, slug
    close = iso_ms(market.get("expirationTimestamp"))
    opened = iso_ms(market.get("startAt")) or iso_ms(market.get("createdAt"))
    tags = tags_of(market) or tags_of(group or {})
    winning = market.get("winningOutcomeIndex")
    venue_info = market.get("venue") or (group or {}).get("venue") or {}
    trade_type = str(market.get("tradeType") or (group or {}).get("tradeType") or "")
    return Market(
        id=ids.qualify(VENUE, slug),
        venue=VENUE,
        venue_market_id=slug,
        event_id=ids.qualify(VENUE, event_slug),
        title=title,
        description=market.get("description") or None,
        slug=slug,
        yes=Outcome(label="Yes", quote=yes_quote, venue_token_id=str(tokens["yes"]) if tokens.get("yes") else None),
        no=Outcome(label="No", quote=no_quote, venue_token_id=str(tokens["no"]) if tokens.get("no") else None),
        status=status,  # type: ignore[arg-type]
        native_status=native_status,
        active=accepting,
        open_timestamp=opened,
        open_datetime=iso(opened),
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=close,
        resolution_datetime=iso(close),
        tick_size=TICK,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(volume_total=scaled(market.get("volume")), volume_unit="collateral"),
        url=f"{WEB_URL}{slug}",
        image_url=market.get("imageUrl") or (group or {}).get("imageUrl") or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=market.get("stableSlug") or None,
        outcome_label=label,
        neg_risk=bool(market.get("negRiskRequestId") or venue_info.get("adapter")),
        settlement_sources=[],
        info={
            "market": {k: v for k, v in market.items() if k != "description"},
            "group_slug": group.get("slug") if group else None,
            "trade_type": trade_type,
            "exchange": venue_info.get("exchange"),
            "adapter": venue_info.get("adapter"),
            "condition_id": market.get("conditionId"),
            "polymarket_slug": polymarket_link(market),
            "winning_outcome": {0: "yes", 1: "no"}.get(winning) if winning is not None else None,
        },
    )


def markets_of(row: dict[str, Any]) -> list[Market]:
    """The binary markets in one listing row: itself, or a group's options in
    the venue's order."""
    if row.get("marketType") == "group" and row.get("markets") is not None:
        children = list(row.get("markets") or [])
        order = (row.get("metadata") or {}).get("submarketOrder") or []
        if order:
            rank = {mid: n for n, mid in enumerate(order)}
            children.sort(key=lambda m: rank.get(m.get("id"), len(rank)))
        return [normalize_market(m, row) for m in children if m.get("slug")]
    return [normalize_market(row)] if row.get("slug") else []


def normalize_event(row: dict[str, Any]) -> Event:
    """A group with its options, or a standalone market as an event of one."""
    markets = markets_of(row)
    slug = str(row["slug"])
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "closed", "settled") if s in statuses), status_of(row)[0])
    close = iso_ms(row.get("expirationTimestamp"))
    tags = tags_of(row)
    group = row.get("marketType") == "group"
    exclusive = any(m.neg_risk for m in markets) if group else None
    return Event(
        id=ids.qualify(VENUE, slug),
        venue=VENUE,
        venue_event_id=slug,
        title=row.get("title") or slug,
        description=row.get("description") or None,
        slug=slug,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=str(row.get("status") or "").lower() or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=row.get("stableSlug") or None,
        mutually_exclusive=True if exclusive else None,
        close_timestamp=close,
        close_datetime=iso(close),
        url=f"{WEB_URL}{slug}",
        image_url=row.get("imageUrl") or None,
        settlement_sources=[],
        info={k: v for k, v in row.items() if k not in ("markets", "description")},
    )


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str, side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """The venue's YES book; `side="no"` mirrors it. Sizes in shares."""
    def levels(rows: Any) -> list[OrderLevel]:
        out = []
        for row in rows or []:
            price, size = to_float((row or {}).get("price")), scaled((row or {}).get("size"))
            if price is None or size is None or size <= 0:
                continue
            out.append(OrderLevel(price=price, size=size))
        return out

    bids, asks = levels(payload.get("bids")), levels(payload.get("asks"))
    derived = side == "no"
    if derived:
        bids, asks = (
            [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in asks],
            [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in bids],
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
        depth_scope="top_n" if depth else "full", info=payload,
    )


def normalize_trade(row: dict[str, Any], *, market_id: str, no_token: str | None) -> Trade:
    """One taker fill, in the YES price with the taker's side. A fill on the
    NO token is the other side of YES at `1 - price`."""
    price = to_float(row.get("price")) or 0.0
    buying = int(row.get("side") or 0) == 0
    if no_token and str(row.get("tokenId")) == no_token:
        price, buying = 1 - price, not buying
    stamp = iso_ms(row.get("createdAt")) or 0
    return Trade(
        id=str(row.get("txHash") or stamp) + (f":{row.get('tokenId')}" if row.get("tokenId") else ""),
        market_id=market_id,
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=round(price, 6),
        amount=scaled(row.get("matchedSize")) or 0.0,
        side="buy" if buying else "sell",
        info={k: v for k, v in row.items() if k != "profile"},
    )


def candles_from_prices(points: list[dict[str, Any]], *, interval_seconds: int) -> list[Candle]:
    """The venue's YES-price samples bucketed into bars, oldest first. No
    volume behind them."""
    buckets: dict[int, list[tuple[int, float]]] = {}
    for point in points:
        stamp, price = iso_ms(point.get("timestamp")), to_float(point.get("price"))
        if stamp is None or price is None:
            continue
        start = stamp // 1000 // interval_seconds * interval_seconds
        buckets.setdefault(start, []).append((stamp, price))
    candles = []
    for start in sorted(buckets):
        prices = [p for _, p in sorted(buckets[start])]
        candles.append(Candle(
            timestamp=start * 1000, datetime=iso(start * 1000) or "",
            open=prices[0], high=max(prices), low=min(prices), close=prices[-1],
            volume=None, trade_count=None, price_source="sampled_mid", info={"samples": len(prices)},
        ))
    return candles


def fee_schedule_of(market: dict[str, Any], *, market_id: str) -> FeeSchedule:
    """The market's taker curve (`limitless_curve`); a market the venue marks
    fee-free charges nothing. Makers pay nothing."""
    charged = (market.get("metadata") or {}).get("fee", True) is not False
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="limitless_curve",
        taker_rate=0.03 if charged else 0.0, maker_rate=0.0,
        info={"fee": charged, "currency": "USDC", "sell_rate_range": [0.0042, 0.015] if charged else [0, 0]},
    )


def lookback_for(seconds: int) -> str:
    """The shortest price-history preset covering `seconds`."""
    return next((name for name, span in LOOKBACKS if span >= seconds), "all")


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Limitless(Exchange):
    """Limitless, read-only. No key needed.

    ```python
    import synpath

    lmts = synpath.Limitless()
    markets = lmts.fetch_markets(limit=5)
    book = lmts.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Limitless"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # No batch lookup by slug: one request per market.
        "fetch_markets_by_ids": True,
        # Newest only: the venue has no volume or liquidity order.
        "sort": "partial",
        "fetch_order_book": True,
        # One request per market.
        "fetch_order_books": True,
        "fetch_trades": True,
        # The venue's YES-price samples, no volume, over its lookback presets.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, api_url: str = API_URL, timeout: float = 30.0, limiter: RateLimiter | None = LIMITER,
        client: Any = None,
    ):
        if client is None:
            client = httpx.Client(timeout=timeout, follow_redirects=True)
        self.http = HttpClient(api_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)

    def _get(self, path: str, params: Any = None, *, what: str = "") -> Any:
        try:
            return self.http.get(path, params)
        except MarketNotFound as exc:
            raise MarketNotFound(f"{VENUE}: no {what or 'market'}", body=exc.body, status=exc.status) from None

    # -- catalog ------------------------------------------------------------

    def _listing(self, page: int, *, sort: str | None) -> tuple[list[dict[str, Any]], bool]:
        """One page of the active listing, and whether another follows."""
        payload = self._get("/markets/active", {
            "page": page, "limit": PAGE, "sortBy": SORT_BY[sort] if sort else None,
        }) or {}
        rows = list(payload.get("data") or []) if isinstance(payload, dict) else []
        total = int(payload.get("totalMarketsCount") or 0) if isinstance(payload, dict) else 0
        return rows, bool(rows) and page * PAGE < total

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = None,
    ) -> Page[Market]:
        """One page of open markets, a group's options counted one by one.
        `sort="newest"` orders by listing time; the venue has no volume
        order. With `query`, the venue's search answers, one page."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: the venue lists only open markets; look up a settled one with fetch_market")
        if sort is not None:
            check_sort(sort, venue=VENUE, supported=True)
            if sort not in SORT_BY:
                raise NotSupported(f"{VENUE}: cannot sort markets by {sort}; the venue orders them by listing time only")
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        if query:
            found = self._get("/markets/search", {"query": query, "limit": min(wanted, PAGE)}) or {}
            markets = [m for row in found.get("markets") or [] for m in markets_of(row)]
            return Page([m for m in markets if m.status == "open"][:wanted], next_cursor=None)
        page, skip = _cursor(cursor)
        out: list[Market] = []
        while True:
            rows, more = self._listing(page, sort=sort)
            # A group stays listed while some of its options have already
            # resolved (a price ladder settles rung by rung); those are not open.
            batch = [m for row in rows for m in markets_of(row) if m.status == "open"][skip:]
            room = wanted - len(out)
            out += batch[:room]
            if len(batch) > room:
                return Page(out, next_cursor=f"{page}:{skip + room}")
            if not more:
                return Page(out, next_cursor=None)
            page, skip = page + 1, 0
            if len(out) >= wanted:
                return Page(out, next_cursor=f"{page}:0")

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of open events: groups with their options, and standalone
        markets as events of one. With `query`, the venue's search answers."""
        check_status(status)
        if status != "open":
            raise NotSupported(f"{VENUE}: the venue lists only open events")
        if query:
            found = self._get("/markets/search", {"query": query, "limit": min(page_limit(limit) or PAGE, PAGE)}) or {}
            return Page([normalize_event(row) for row in found.get("markets") or []], next_cursor=None)
        page, _ = _cursor(cursor)
        rows, more = self._listing(page, sort=None)
        events = [normalize_event(row) for row in rows]
        wanted = page_limit(limit)
        return Page(events[:wanted] if wanted else events, next_cursor=f"{page + 1}:0" if more else None)

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor:
                return

    def _raw(self, slug: str) -> dict[str, Any]:
        raw = self._get(f"/markets/{slug}", what=f"market {slug}") or {}
        if not isinstance(raw, dict) or not raw.get("slug"):
            raise MarketNotFound(f"{VENUE}: no market {slug}")
        return raw

    def fetch_market(self, market_id: str) -> Market:
        """One market by slug, open or settled. A group's slug names an event,
        not a market (see `fetch_events`)."""
        slug = self.native(market_id)
        raw = self._raw(slug)
        if raw.get("marketType") == "group":
            raise MarketNotFound(f"{VENUE}: {slug!r} is a group, which is an event; its options are the markets")
        group = None
        if raw.get("groupSlug"):
            # An option names its group but not the question; one more read
            # gives it the same title it has in the listing.
            try:
                group = self._raw(str(raw["groupSlug"]))
            except MarketNotFound:
                group = {"slug": raw["groupSlug"]}
        return normalize_market(raw, group)

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in the order asked for, one request each; slugs the
        venue does not know are left out."""
        found = []
        for market_id in market_ids:
            try:
                found.append(self.fetch_market(market_id))
            except MarketNotFound:
                continue
        return found

    def token_index(self) -> dict[str, tuple[str, str]]:
        """Every open market's outcome tokens: token id -> (market id,
        "yes" or "no"). One walk of the listing, about 25 requests. The
        venue's order events name a token, not a market, and its lookups take
        only a slug, so this is how a stream places them."""
        index: dict[str, tuple[str, str]] = {}
        for event in self.iter_events():
            for market in event.markets:
                if market.yes.venue_token_id:
                    index[market.yes.venue_token_id] = (market.id, "yes")
                if market.no.venue_token_id:
                    index[market.no.venue_token_id] = (market.id, "no")
        return index

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with its bid and ask read from the book."""
        book = self.fetch_order_book(market.id)
        bid = book.bids[0].price if book.bids else None
        ask = book.asks[0].price if book.asks else None
        mid = round((bid + ask) / 2, 6) if bid is not None and ask is not None else market.yes.quote.mid
        yes = market.yes.model_copy(update={"quote": Quote(
            bid=bid, ask=ask, mid=mid, last=to_float(book.info.get("lastTradePrice")),
            bid_size=book.bids[0].size if book.bids else None, ask_size=book.asks[0].size if book.asks else None,
        )})
        no = market.no.model_copy(update={"quote": Quote(
            bid=round(1 - ask, 6) if ask is not None else None, ask=round(1 - bid, 6) if bid is not None else None,
            mid=round(1 - mid, 6) if mid is not None else None,
            bid_size=book.asks[0].size if book.asks else None, ask_size=book.bids[0].size if book.bids else None,
        )})
        return market.model_copy(update={"yes": yes, "no": no})

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        slug = self.native(market_id)
        payload = self._get(f"/markets/{slug}/orderbook", what=f"order book for {slug}") or {}
        if not isinstance(payload, dict):
            raise ExchangeError(f"{VENUE}: unexpected order book for {slug}")
        return normalize_order_book(payload, market_id=self.qualify(slug), side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, one request each (the venue has no batch read)."""
        return {self.qualify(self.native(m)): self.fetch_order_book(m, side=side, depth=depth)
                for m in dict.fromkeys(market_ids)}

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """The market's settled fills, a page newest first from the venue and
        returned oldest first, in the YES price with the taker's side."""
        slug = self.native(market_id)
        raw = self._raw(slug)
        no_token = str((raw.get("tokens") or {}).get("no") or "") or None
        page = int(cursor) if cursor and cursor.isdigit() else 1
        payload = self._get(f"/markets/{slug}/events", {"page": page, "limit": min(page_limit(limit) or 100, 100)}) or {}
        rows = payload.get("events") or []
        trades = [normalize_trade(row, market_id=self.qualify(slug), no_token=no_token) for row in rows]
        if since:
            trades = [t for t in trades if t.timestamp >= since]
        more = page < int(payload.get("totalPages") or 0)
        return Page(sorted(trades, key=lambda t: t.timestamp), next_cursor=str(page + 1) if more else None)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price from the venue's price samples, no volume
        (`price_source="sampled_mid"`). The venue reads back over fixed
        lookbacks (5m to 30 days, or all), clipped to the market's life."""
        slug = self.native(market_id)
        seconds = timeframe_seconds(timeframe)
        now = _now_ms()
        span = int(((until or now) - since) / 1000) if since else seconds * (limit or DEFAULT_BARS)
        payload = self._get(f"/markets/{slug}/historical-price", {"interval": lookback_for(max(span, 1))},
                            what=f"price history for {slug}") or {}
        series = payload if isinstance(payload, list) else [payload]
        points = [p for s in series if isinstance(s, dict) for p in s.get("prices") or []]
        candles = candles_from_prices(points, interval_seconds=seconds)
        if until:
            candles = [c for c in candles if c.timestamp <= until]
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        slug = self.native(market_id)
        return fee_schedule_of(self._raw(slug), market_id=slug)

    def close(self) -> None:
        self.http.close()


def _cursor(cursor: str | None) -> tuple[int, int]:
    """`page:skip` — the listing page, and how many markets of it were already returned."""
    if not cursor:
        return 1, 0
    try:
        page, _, skip = cursor.partition(":")
        return max(1, int(page)), max(0, int(skip or 0))
    except ValueError:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued") from None


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
