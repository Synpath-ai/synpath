"""predict.fun: a Polymarket-style prediction market on BNB Chain, read from its REST API.

Every request needs an API key, on mainnet (`x-api-key`, from developers.predict.fun);
the test network needs none. 240 requests a minute per key.

Four things about this venue that shape the adapter:

**A category is an event.** predict.fun groups markets into *categories* ("LoL:
Natus Vincere vs FlyQuest", "Number of CZ tweets Sep 28 - Oct 5"); each market
inside is one binary market with a YES and a NO outcome token. A category is an
`Event` here, keyed by its slug, and its markets are the markets, keyed by their
numeric id. A multi-option category is neg-risk, as on Polymarket.

**One book, quoted on YES.** The venue stores every book in YES prices; the NO
side is the mirror. The catalog listing itself carries each market's best bid
and ask, so a page of markets arrives already quoted.

**Mirrored from Polymarket.** Almost every market names the Polymarket
condition it mirrors (`polymarketConditionIds`), kept in `info` for matching.

**Fees on the taker, on the cheaper side.** `feeRateBps` (200 on every market
read so far) is charged as `rate * min(p, 1 - p)` per share on the taker; a
maker pays nothing. The collateral is USDT on BNB Chain.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status,
    page_limit, pick_bars, timeframe_seconds,
)
from .errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome,
    Page, Quote, Trade, iso,
)

API_URL = "https://api.predict.fun/v1"
TESTNET_API_URL = "https://api-testnet.predict.fun/v1"
VENUE = "predict_fun"
WEB_URL = "https://predict.fun/market/"

LIMITER = RateLimiter(3.8, burst=8)
"""The venue allows 240 requests a minute per API key; kept just under it,
shared process-wide."""

WEI = 10**18
"""Amounts and prices on trades are 18-decimal integers (USDT and shares on BNB Chain)."""

STATUSES = {
    "REGISTERED": "open", "UNPAUSED": "open", "PAUSED": "closed",
    "PRICE_PROPOSED": "closed", "PRICE_DISPUTED": "closed", "RESOLVED": "settled", "REMOVED": "closed",
}
"""The venue's market lifecycle, as this library's status. A proposed or
disputed price is the outcome being decided: trading has stopped, nothing is
paid yet."""

SORT_BY = {"volume": "VOLUME_24H_DESC"}
"""This library's sort keys the venue can order markets by. It orders
categories by publication, markets by volume only."""

CANDLE_RESOLUTIONS = {60: "1m", 300: "5m", 3600: "1h", 86400: "1d"}
"""The venue's sample widths. A timeframe between them is built from the
widest one that divides it."""

DEFAULT_BARS = 100
MAX_PAGES = 10
"""Most venue pages one catalog call reads while filling `limit` after filtering."""


def api_key_from(api_key: str | None) -> str | None:
    """The key given, else `PREDICT_FUN_API_KEY` from the environment or a `.env`."""
    if api_key:
        return api_key
    found = os.environ.get("PREDICT_FUN_API_KEY")
    if not found and os.path.exists(".env"):
        from .trading.credentials import read_dotenv

        found = read_dotenv(".env").get("PREDICT_FUN_API_KEY")
    return found or None


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


def iso_ms(value: Any) -> int | None:
    """`2026-10-05T12:00:00.000Z` as milliseconds."""
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def unwrap(payload: Any, *, what: str = "") -> Any:
    """The `data` of the venue's envelope, or its refusal as a typed error."""
    if not isinstance(payload, dict):
        raise ExchangeError(f"{VENUE}: unexpected response {str(payload)[:120]!r}")
    if payload.get("success") is False:
        code, message = payload.get("code"), payload.get("message") or payload.get("error") or "refused"
        if code == 404:
            raise MarketNotFound(f"{VENUE}: {what + ': ' if what else ''}{message}", body=payload)
        if code in (401, 403):
            raise AuthenticationError(f"{VENUE}: {message}", body=payload)
        raise BadRequest(f"{VENUE}: {message}", body=payload)
    return payload.get("data")


def outcome_sides(market: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """(YES, NO) by index set: 1 is the first outcome, 2 the second."""
    outcomes = sorted(market.get("outcomes") or [], key=lambda o: o.get("indexSet") or 0)
    yes = outcomes[0] if outcomes else {}
    no = outcomes[1] if len(outcomes) > 1 else {}
    return yes, no


def _level(value: Any) -> tuple[float | None, float | None]:
    if not isinstance(value, dict):
        return None, None
    return to_float(value.get("price")), to_float(value.get("size"))


def quotes_of(market: dict[str, Any]) -> tuple[Quote, Quote]:
    """YES and NO quotes from the best bid and ask the listing carries for the
    YES outcome; NO is its mirror (NO's bid is `1 - YES ask`)."""
    yes, _ = outcome_sides(market)
    bid, bid_size = _level(yes.get("bestBid"))
    ask, ask_size = _level(yes.get("bestAsk"))

    def mid(b: float | None, a: float | None) -> float | None:
        return round((b + a) / 2, 6) if b is not None and a is not None else None

    no_bid = round(1 - ask, 6) if ask is not None else None
    no_ask = round(1 - bid, 6) if bid is not None else None
    return (
        Quote(bid=bid, bid_size=bid_size, ask=ask, ask_size=ask_size, mid=mid(bid, ask)),
        Quote(bid=no_bid, bid_size=ask_size, ask=no_ask, ask_size=bid_size, mid=mid(no_bid, no_ask)),
    )


def status_of(market: dict[str, Any]) -> tuple[str, str, bool]:
    """(normalized status, native word, accepting orders)."""
    native = str(market.get("status") or "")
    status = STATUSES.get(native, "closed")
    trading = str(market.get("tradingStatus") or "")
    return status, native.lower(), status == "open" and trading == "OPEN"


def tags_of(category: dict[str, Any] | None) -> list[str]:
    return [str(tag.get("name")).lower() for tag in (category or {}).get("tags") or [] if tag.get("name")]


def normalize_market(market: dict[str, Any], category: dict[str, Any] | None = None) -> Market:
    """One market. Its `question` is the full proposition; inside a category of
    several markets its `title` is the option ("21-25", "Match Winner") and
    becomes `outcome_label`."""
    native = str(market["id"])
    yes, no = outcome_sides(market)
    yes_quote, no_quote = quotes_of(market)
    status, native_status, accepting = status_of(market)
    question = (market.get("question") or "").strip()
    option = (market.get("title") or "").strip()
    category = category or {}
    if question and option and option != question:
        title = f"{question}" if option.lower() in question.lower() else f"{question} — {option}"
        label = option
    else:
        title, label = question or option or native, None
    stats = market.get("stats") or {}
    precision = market.get("decimalPrecision")
    close = iso_ms(category.get("endsAt"))
    tags = tags_of(category)
    slug = market.get("categorySlug") or category.get("slug")
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, str(slug)) if slug else None,
        title=title,
        description=market.get("description") or None,
        slug=slug,
        yes=Outcome(label=str(yes.get("name") or "Yes"), quote=yes_quote, venue_token_id=yes.get("onChainId")),
        no=Outcome(label=str(no.get("name") or "No"), quote=no_quote, venue_token_id=no.get("onChainId")),
        status=status,  # type: ignore[arg-type]
        native_status=native_status,
        active=accepting,
        open_timestamp=iso_ms(market.get("createdAt")),
        open_datetime=iso(iso_ms(market.get("createdAt"))),
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=close,
        resolution_datetime=iso(close),
        tick_size=10 ** -int(precision) if isinstance(precision, int) else None,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(
            volume_24h=to_float(stats.get("volume24hUsd")),
            volume_total=to_float(stats.get("volumeTotalUsd")),
            liquidity=to_float(stats.get("totalLiquidityUsd")),
            volume_unit="collateral",
            liquidity_unit="collateral",
        ),
        url=f"{WEB_URL}{slug}" if slug else None,
        image_url=market.get("imageUrl") or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=label,
        neg_risk=bool(market.get("isNegRisk")),
        settlement_sources=[],
        info={
            "market": {k: v for k, v in market.items() if k != "description"},
            "polymarket_condition_ids": market.get("polymarketConditionIds") or [],
            "kalshi_market_ticker": market.get("kalshiMarketTicker"),
            "fee_rate_bps": market.get("feeRateBps"),
            "yield_bearing": market.get("isYieldBearing"),
        },
    )


def normalize_event(category: dict[str, Any]) -> Event:
    """One category with its markets."""
    markets = [normalize_market(m, category) for m in category.get("markets") or []]
    native = str(category.get("slug") or category.get("id"))
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "closed", "settled") if s in statuses), "closed")
    close = iso_ms(category.get("endsAt"))
    tags = tags_of(category)
    return Event(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_event_id=native,
        title=category.get("title") or native,
        description=category.get("description") or None,
        slug=category.get("slug"),
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=str(category.get("status") or "").lower() or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        # A neg-risk category's markets are its exclusive options.
        mutually_exclusive=True if category.get("isNegRisk") else None,
        close_timestamp=close,
        close_datetime=iso(close),
        url=f"{WEB_URL}{category['slug']}" if category.get("slug") else None,
        image_url=category.get("imageUrl") or None,
        settlement_sources=[],
        info={k: v for k, v in category.items() if k not in ("markets", "description")},
    )


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str, side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """The venue's book, in YES prices as it stores it; `side="no"` mirrors it."""
    def levels(rows: Any) -> list[OrderLevel]:
        out = []
        for row in rows or []:
            price, size = (to_float(row[0]), to_float(row[1])) if isinstance(row, (list, tuple)) and len(row) >= 2 else (None, None)
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
    stamp = int(payload["updateTimestampMs"]) if payload.get("updateTimestampMs") else None
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks,
        timestamp=stamp, datetime=iso(stamp), book_model="shared_complement", derived=derived,
        depth_scope="top_n" if depth else "full", info=payload,
    )


def normalize_trade(match: dict[str, Any], *, market_id: str) -> Trade:
    """One match, from the taker's side, in the YES price. The taker's
    `quoteType` is its order: `Bid` bought the outcome it names, `Ask` sold it;
    an order on NO is the other side of YES at `1 - price`."""
    taker = match.get("taker") or {}
    outcome = taker.get("outcome") or {}
    on_no = outcome.get("indexSet") == 2
    price = (to_float(match.get("priceExecuted") or taker.get("price")) or 0) / WEI
    buying = str(taker.get("quoteType") or "") == "Bid"
    if on_no:
        price, buying = 1 - price, not buying
    stamp = iso_ms(match.get("executedAt")) or 0
    return Trade(
        id=str(match.get("transactionHash") or match.get("settlementId") or stamp),
        market_id=market_id,
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=round(price, 6),
        amount=(to_float(match.get("amountFilled")) or 0) / WEI,
        side="buy" if buying else "sell",
        info={k: v for k, v in match.items() if k != "market"},
    )


def candles_from_series(series: list[dict[str, Any]], *, interval_seconds: int) -> list[Candle]:
    """The venue's `chance` samples (the probability it shows, in percent)
    bucketed into bars, oldest first. No volume behind them."""
    buckets: dict[int, list[tuple[int, float]]] = {}
    for point in series:
        x, y = point.get("x"), to_float(point.get("y"))
        if x is None or y is None:
            continue
        start = int(x) // interval_seconds * interval_seconds
        buckets.setdefault(start, []).append((int(x), y / 100))
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
    """`feeRateBps` as a schedule: a taker pays `rate * min(p, 1 - p)` per
    share, a maker nothing (`fee_type="min_price"`)."""
    bps = to_float(market.get("feeRateBps")) or 0.0
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="min_price",
        taker_rate=bps / 10_000, maker_rate=0.0,
        info={"fee_rate_bps": market.get("feeRateBps"), "currency": "USDT"},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class PredictFun(Exchange):
    """predict.fun, read-only. Needs an API key on mainnet.

    ```python
    import synpath

    pf = synpath.PredictFun()            # PREDICT_FUN_API_KEY from the environment
    markets = pf.fetch_markets(limit=5)
    book = pf.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "predict.fun"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # No batch lookup by id: one request per market.
        "fetch_markets_by_ids": True,
        # 24h volume only.
        "sort": "partial",
        "fetch_order_book": True,
        # One request for many books.
        "fetch_order_books": True,
        "fetch_trades": True,
        # The venue's probability samples, no volume.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self,
        *,
        api_key: str | None = None,
        testnet: bool = False,
        api_url: str | None = None,
        timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER,
        client: Any = None,
    ):
        """`api_key` (else `PREDICT_FUN_API_KEY`) is required on mainnet;
        `testnet=True` reads the test network, which needs none."""
        key = api_key_from(api_key)
        self.testnet = testnet
        url = api_url or (TESTNET_API_URL if testnet else API_URL)
        if client is None and key:
            client = httpx.Client(timeout=timeout, follow_redirects=True, headers={"x-api-key": key})
        self.has_key = bool(key)
        self.http = HttpClient(url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)

    def _request(self, path: str, params: Any = None) -> Any:
        """One GET. A refusal for want of a key says how to get one."""
        try:
            return self.http.get(path, params)
        except AuthenticationError as exc:
            if self.has_key:
                raise
            raise AuthenticationError(
                f"{VENUE}: mainnet needs an API key -- pass api_key= or set PREDICT_FUN_API_KEY "
                f"(create one at https://developers.predict.fun), or use testnet=True"
            ) from exc

    def _get(self, path: str, params: Any = None, *, what: str = "") -> Any:
        return unwrap(self._request(path, params), what=what)

    def _page(self, path: str, params: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
        """One page of a cursor-paged listing: its rows and the venue's cursor."""
        payload = self._request(path, params)
        data = unwrap(payload)
        return list(data or []), (payload.get("cursor") if isinstance(payload, dict) else None) or None

    # -- catalog ------------------------------------------------------------

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = "volume",
    ) -> Page[Market]:
        """One page of markets, by default the highest 24h volume first.
        `status` is `open`, `settled` or `all` (`closed` -- resolving or paused
        -- the venue cannot filter by, and it raises). With `query`, the
        venue's own search answers, one page, no cursor."""
        check_status(status)
        if status == "closed":
            raise NotSupported(f"{VENUE}: cannot filter by closed -- the venue filters open and resolved markets only")
        if sort is not None:
            check_sort(sort, venue=VENUE, supported=True)
            if sort not in SORT_BY:
                raise NotSupported(f"{VENUE}: cannot sort markets by {sort}; the venue orders them by 24h volume only")
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        if query:
            found = self._get("/search", {"query": query, "limit": wanted, "includeStats": "true",
                                          "includeResolved": "true" if status != "open" else None}) or {}
            markets = [normalize_market(m) for m in found.get("markets") or []]
            for category in found.get("categories") or []:
                markets += [normalize_market(m, category) for m in category.get("markets") or []]
            seen: dict[str, Market] = {}
            for market in markets:
                if status == "all" or market.status == status:
                    seen.setdefault(market.id, market)
            return Page(list(seen.values())[:wanted], next_cursor=None)
        rows, next_cursor = self._page("/markets", {
            "first": wanted, "after": cursor, "includeStats": "true",
            "status": {"open": "OPEN", "settled": "RESOLVED"}.get(status),
            "sort": SORT_BY[sort] if sort else None,
        })
        markets = [normalize_market(m) for m in rows]
        if status != "all":
            # The venue's OPEN filter keeps a market whose result is proposed
            # or disputed; that market is resolving, not open.
            markets = [m for m in markets if m.status == status]
        return Page(markets, next_cursor=next_cursor)

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of categories with their markets, highest 24h volume first.
        With `query`, the venue's search answers, one page."""
        check_status(status)
        if status == "closed":
            raise NotSupported(f"{VENUE}: cannot filter by closed")
        wanted = page_limit(limit) or 20
        if query:
            found = self._get("/search", {"query": query, "limit": wanted, "includeStats": "true",
                                          "includeResolved": "true" if status != "open" else None}) or {}
            events = [normalize_event(c) for c in found.get("categories") or []]
            return Page([e for e in events if status == "all" or e.status == status][:wanted], next_cursor=None)
        rows, next_cursor = self._page("/categories", {
            "first": wanted, "after": cursor, "includeStats": "true", "sort": "VOLUME_24H_DESC",
            "status": {"open": "OPEN", "settled": "RESOLVED"}.get(status),
        })
        return Page([normalize_event(c) for c in rows], next_cursor=next_cursor)

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        while True:
            page = self.fetch_events(cursor=cursor, status=status, limit=MAX_PAGE_LIMIT)
            yield from page
            cursor = page.next_cursor
            if not cursor or not page:
                return

    def fetch_market(self, market_id: str) -> Market:
        native = self.native(market_id)
        if not native.isdigit():
            raise MarketNotFound(f"{VENUE}: {native!r} is not a market id; a category slug names an event (see fetch_events)")
        market = self._get(f"/markets/{native}", {"includeStats": "true"}, what=f"market {native}")
        if not isinstance(market, dict) or market.get("id") is None:
            raise MarketNotFound(f"{VENUE}: no market {native}")
        return normalize_market(market)

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in the order asked for, one request each (the venue has
        no batch lookup); ids it does not know are left out."""
        found = []
        for market_id in market_ids:
            try:
                found.append(self.fetch_market(market_id))
            except MarketNotFound:
                continue
        return found

    def refresh_quotes(self, market: Market) -> Market:
        """`market` re-read: the venue's listing carries the live best bid and ask."""
        return self.fetch_market(market.id)

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        native = self.native(market_id)
        payload = self._get(f"/markets/{native}/orderbook", what=f"market {native}") or {}
        return normalize_order_book(payload, market_id=self.qualify(native), side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets in one request per 50."""
        natives = list(dict.fromkeys(self.native(m) for m in market_ids))
        out: dict[str, OrderBook] = {}
        for i in range(0, len(natives), 50):
            batch = natives[i:i + 50]
            rows = self._get("/markets/orderbooks", [("ids", n) for n in batch]) or []
            for row in rows:
                native = str(row.get("marketId"))
                out[self.qualify(native)] = normalize_order_book(row, market_id=self.qualify(native), side=side, depth=depth)
        return out

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """The market's matches, newest first from the venue and returned
        oldest first, in the YES price with the taker's side."""
        native = self.native(market_id)
        rows, next_cursor = self._page("/orders/matches", {
            "marketId": int(native), "first": page_limit(limit) or MAX_PAGE_LIMIT, "after": cursor,
            "executedAfter": int(since / 1000) if since else None,
        })
        trades = sorted((normalize_trade(row, market_id=self.qualify(native)) for row in rows), key=lambda t: t.timestamp)
        return Page(trades, next_cursor=next_cursor)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price from the venue's probability samples. No
        volume (`price_source="sampled_mid"`)."""
        native = self.native(market_id)
        seconds = timeframe_seconds(timeframe)
        width = max((w for w in CANDLE_RESOLUTIONS if seconds % w == 0), default=60)
        end = int((until or _now_ms()) / 1000)
        start = int(since / 1000) if since else end - seconds * (limit or DEFAULT_BARS)
        series: list[dict[str, Any]] = []
        after = None
        for _ in range(MAX_PAGES):
            payload = self._request(f"/markets/{native}/timeseries", {
                "metric": "chance", "resolution": CANDLE_RESOLUTIONS[width], "from": start, "to": end,
                "limit": 1000, "after": after,
            })
            data = unwrap(payload, what=f"market {native}") or {}
            series += data.get("series") or []
            after = payload.get("cursor") if isinstance(payload, dict) else None
            if not after or len(data.get("series") or []) < 1000:
                break
        candles = candles_from_series(series, interval_seconds=seconds)
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        native = self.native(market_id)
        market = self._get(f"/markets/{native}", what=f"market {native}") or {}
        return fee_schedule_of(market, market_id=native)

    def close(self) -> None:
        self.http.close()


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
