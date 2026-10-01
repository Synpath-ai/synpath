"""Opinion: the Open API for the catalog, books and prices; BNB Chain for fees.

Public reads, no credentials. An Opinion API key raises the rate limit from 5
to 15 requests a second and is otherwise not needed for anything here.

Four things about this venue that shape the adapter:

**A topic is either a market or a group of them.** Opinion's catalog lists
*topics*. A binary topic is one market. A categorical topic ("Which companies
will be acquired before 2027?") holds one binary child market per option, and
only the children trade. So a topic is an `Event` here, a binary topic is an
event holding itself, and `fetch_markets` flattens categorical topics into
their children. A child's own record does not name its parent; its slug does,
which is how `fetch_market` puts the event back.

**The catalog carries no prices.** No bid, ask or last on any listing, so a
`Market` from a catalog call has empty quotes, never invented ones.
`refresh_quotes` reads the books.

**Price history is the last trade, sampled.** `price-history` returns the
last traded price once an hour or once a day -- not a midpoint, not bars,
no volume. Bars built from it say so (`price_source="sampled_last"`).

**Fees are on chain.** The public API publishes no fee rate. The venue's
FeeManager contract on BNB Chain does, per token, and a public node answers it
without a key, so `fetch_fee_schedule` reads it there.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort,
    check_status, page_limit, pick_bars, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel,
    Outcome, Page, Quote, Trade, iso,
)

OPENAPI_URL = "https://openapi.opinion.trade/openapi"
RPC_URL = "https://bsc-dataseed.binance.org"
"""A public BNB Chain node, for the one contract read this adapter makes."""
VENUE = "opinion"

LIMITER = RateLimiter(5.0, burst=5)
"""The venue's published public limit: 5 requests a second per IP (15 per
API key). Shared process-wide, as every venue's is."""

PAGE = 20
"""Topics per catalog call: the venue's maximum."""

MAX_PAGES = 10
"""Most catalog pages one `fetch_markets` or `fetch_events` call reads while
filling `limit`, at 5 requests a second about two seconds. Past it the call
returns what it has, with a cursor."""

MAX_SEARCH_PAGES = 25
"""Most catalog pages one search call scans: 500 topics, more than the whole
open catalog (about 300 topics) and two seconds at the public rate limit.
The venue has no search endpoint, so a query is matched here."""

MAX_HISTORY_PAGES = 10
"""Most `price-history` calls one `fetch_ohlcv` makes walking back to `since`.
A call returns at most 1000 samples, so this is 10,000 hours (over a year) of
hourly samples."""

DEFAULT_BARS = 100
"""Bars looked back when neither `since` nor `limit` bounds the window."""

FEE_MANAGER = "0xC9063Dc52dEEfb518E5b6634A6b8D624bc5d7c36"
"""Opinion's FeeManager contract on BNB Chain, from the venue's own SDK."""

GET_FEE_RATE_SETTINGS = "0x27f68850"
"""Selector of `getFeeRateSettings(uint256 tokenId)`, which returns
`(makerFeeRateBps, takerFeeRateBps, enabled, minFeeAmount)`."""

MIN_FEE = 0.25
"""The venue's documented minimum fee per order, in USDT. The contract's own
`minFeeAmount` reads 0; the fee docs state this floor is applied when the
curve fee comes out smaller, so the documented figure is the one used."""

STATUSES = {1: ("unopened", "created"), 2: ("open", "activated"), 3: ("closed", "resolving"), 4: ("settled", "resolved")}
"""The venue's numeric status, as (normalized status, native word)."""

SORT_BY = {"volume": 5, "newest": 1}
"""This library's sort keys as the venue's `sortBy` codes: 5 is 24h volume,
descending; 1 is newest first. The venue has no liquidity ordering."""

NOT_FOUND = 10200
"""The venue's error number for an id it does not know ("Topic ID does not exist")."""


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


def seconds_ms(value: Any) -> int | None:
    """Epoch seconds as milliseconds. The venue sends 0 for a time it does not
    have (a child market's cutoff, an unresolved market's resolution), which is
    absence, not 1970."""
    number = to_float(value)
    if not number:
        return None
    return int(number * 1000)


def unwrap(payload: Any, *, what: str = "") -> Any:
    """The `result` of the venue's envelope, or the error it carries.

    Opinion answers errors with HTTP 200 and a non-zero `errno`, so the status
    code alone says nothing: an unknown id comes back as a successful response
    reading `Topic ID does not exist`. Mapped here to the same typed errors a
    4xx maps to on every other venue.
    """
    if not isinstance(payload, dict):
        raise ExchangeError(f"{VENUE}: unexpected response {str(payload)[:120]!r}")
    errno = payload.get("errno", payload.get("code", 0))
    if not errno:
        return payload.get("result")
    message = payload.get("errmsg") or payload.get("msg") or f"error {errno}"
    if errno == NOT_FOUND:
        raise MarketNotFound(f"{VENUE}: {what + ': ' if what else ''}{message}", body=payload)
    raise BadRequest(f"{VENUE}: {message}", body=payload)


def status_of(raw: dict[str, Any]) -> tuple[str, str, bool]:
    """(normalized status, native word, accepting orders)."""
    code = raw.get("status")
    status, native = STATUSES.get(code, ("closed", str(raw.get("statusEnum") or code).lower()))
    return status, native, status == "open"


def labels_of(raw: dict[str, Any]) -> list[str]:
    return [str(label).lower() for label in (raw.get("labels") or []) if label]


def market_url(raw: dict[str, Any]) -> str | None:
    slug = raw.get("slug")
    return f"https://opinion.trade/market/{slug}" if slug else None


def normalize_market(raw: dict[str, Any], parent: dict[str, Any] | None = None) -> Market:
    """One binary market -- a binary topic, or one child of a categorical
    topic with the topic as `parent` -- as a unified `Market`.

    A child's own record is thin: its title is the option ("Perplexity AI"),
    its cutoff is 0 and it carries no labels. With the parent it gets the full
    question as its title, the option as `outcome_label`, and the parent's
    close time, labels and event id.
    """
    if raw.get("marketType") == 1:
        raise BadRequest(
            f"{VENUE}: topic {raw.get('marketId')} is categorical; its child markets "
            f"trade, it does not. It is an event here: see `fetch_events`."
        )
    market_id = str(raw["marketId"])
    parent = parent or {}
    status, native, accepting = status_of(raw)
    option = (raw.get("marketTitle") or "").strip()
    if parent:
        title = f"{parent.get('marketTitle') or ''} — {option}".strip(" —")
        event_native = str(parent["marketId"])
    else:
        title = option or market_id
        event_native = market_id
    close = seconds_ms(raw.get("cutoffAt")) or seconds_ms(parent.get("cutoffAt"))
    tags = labels_of(raw) or labels_of(parent)

    def outcome(label: Any, token: Any, default: str) -> Outcome:
        # The catalog carries no prices at all, so the quote is empty rather
        # than filled from anything else. `refresh_quotes` reads the books.
        return Outcome(label=str(label or default), quote=Quote(), venue_token_id=str(token) if token else None)

    return Market(
        id=ids.qualify(VENUE, market_id),
        venue=VENUE,
        venue_market_id=market_id,
        event_id=ids.qualify(VENUE, event_native),
        title=title,
        description=raw.get("rules") or None,
        slug=raw.get("slug") or None,
        yes=outcome(raw.get("yesLabel"), raw.get("yesTokenId"), "Yes"),
        no=outcome(raw.get("noLabel"), raw.get("noTokenId"), "No"),
        status=status,  # type: ignore[arg-type]
        native_status=native,
        active=accepting,
        open_timestamp=seconds_ms(raw.get("createdAt")),
        open_datetime=iso(seconds_ms(raw.get("createdAt"))),
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=close,
        resolution_datetime=iso(close),
        # The venue's docs say four decimals inside [0.01, 0.99], and its books
        # rest orders at 0.002 and 0.998, outside that range. Unknown, so None.
        tick_size=None,
        face_value=1.0,
        book_model="native_per_outcome",
        stats=MarketStats(
            volume_24h=to_float(raw.get("volume24h")),
            volume_total=to_float(raw.get("volume")),
            liquidity=None,
            open_interest=None,
            # USDT notional, the collateral, not contracts.
            volume_unit="collateral",
            liquidity_unit=None,
        ),
        url=market_url(parent) if parent else market_url(raw),
        image_url=raw.get("thumbnailUrl") or raw.get("coverUrl") or parent.get("thumbnailUrl") or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=option if parent else None,
        neg_risk=None,
        # The rules name sources in prose; there is no structured field.
        settlement_sources=[],
        info=raw,
    )


def markets_of(topic: dict[str, Any]) -> list[Market]:
    """The tradable markets under one topic: itself when binary, its children
    when categorical."""
    if topic.get("marketType") == 1:
        return [normalize_market(child, topic) for child in topic.get("childMarkets") or []]
    return [normalize_market(topic)]


def normalize_event(topic: dict[str, Any]) -> Event:
    """One topic as an `Event`. A binary topic is an event holding one market,
    itself, under the same id: the venue has no separate event tier for it.

    A categorical topic's status is taken from its options. Its own field is
    not reliable: the detail endpoint reports a topic whose options are
    trading as `1` (created) while the listing reports it activated.
    """
    status, native, _ = status_of(topic)
    markets = markets_of(topic)
    if topic.get("marketType") == 1 and markets:
        statuses = {market.status for market in markets}
        status = next(s for s in ("open", "closed", "settled", "unopened") if s in statuses)
    tags = labels_of(topic)
    close = seconds_ms(topic.get("cutoffAt"))
    topic_id = str(topic["marketId"])
    return Event(
        id=ids.qualify(VENUE, topic_id),
        venue=VENUE,
        venue_event_id=topic_id,
        title=topic.get("marketTitle") or topic_id,
        description=topic.get("rules") or None,
        slug=topic.get("slug") or None,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=native,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        # A categorical topic does not say whether its options exclude each
        # other, and some do not ("which companies will be acquired").
        mutually_exclusive=None,
        close_timestamp=close,
        close_datetime=iso(close),
        url=market_url(topic),
        image_url=topic.get("thumbnailUrl") or topic.get("coverUrl") or None,
        settlement_sources=[],
        info={key: value for key, value in topic.items() if key != "childMarkets"},
    )


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str, side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """One token's book, best first. The venue documents bids descending and
    asks ascending; they are sorted here anyway, because a book read in the
    wrong order misprices everything."""
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
    timestamp = int(payload["timestamp"]) if payload.get("timestamp") else None
    return OrderBook(
        market_id=market_id,
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


def last_trade_of(payload: dict[str, Any]) -> tuple[float | None, int | None]:
    """(price, time in ms) of a token's last trade, or (None, None) when it has
    never traded: the venue answers that with price 0.0 and timestamp 0."""
    price = to_float(payload.get("price"))
    stamp = payload.get("timestamp") or None
    if not price or not stamp:
        return None, None
    return price, int(stamp)


def candles_from_price_history(history: list[dict[str, Any]], *, interval_seconds: int) -> list[Candle]:
    """Bucket the venue's last-trade samples into bars, oldest first.

    `price-history` returns `{t, p}` points, newest first: the last traded
    price at each hour or day. No volume, no trade count, not a midpoint. The
    bars say so in `price_source`, and their volume is `None`, not 0.
    """
    buckets: dict[int, list[tuple[float, float]]] = {}
    for point in history:
        seconds = to_float(point.get("t"))
        price = to_float(point.get("p"))
        if seconds is None or price is None:
            continue
        start = int(seconds // interval_seconds) * interval_seconds
        buckets.setdefault(start, []).append((seconds, price))
    candles = []
    for start in sorted(buckets):
        prices = [price for _, price in sorted(buckets[start])]
        candles.append(Candle(
            timestamp=start * 1000,
            datetime=iso(start * 1000) or "",
            open=prices[0], high=max(prices), low=min(prices), close=prices[-1],
            volume=None,
            trade_count=None,
            price_source="sampled_last",
            info={"samples": len(prices)},
        ))
    return candles


def fee_schedule_of(result_hex: str, *, market_id: str, token_id: str) -> FeeSchedule:
    """The FeeManager's answer for one token as a `FeeSchedule`.

    The contract returns basis points of the curve coefficient (`topic_rate`
    in the venue's fee docs): a taker pays `rate * notional * P * (1 - P)`,
    notional being `P * contracts`, which tops out at a quarter of the rate at
    50c. Makers are charged their own rate, zero on every market read so far.
    """
    words = _words(result_hex)
    if len(words) < 4:
        raise ExchangeError(f"{VENUE}: fee manager answered {result_hex[:80]!r} for {market_id}")
    maker_bps, taker_bps, enabled, min_fee = words[:4]
    on = bool(enabled)
    return FeeSchedule(
        venue=VENUE,
        scope="market",
        scope_id=market_id,
        fee_type="opinion_curve",
        taker_rate=taker_bps / 10_000 if on else 0.0,
        maker_rate=maker_bps / 10_000 if on else 0.0,
        min_fee=MIN_FEE if on else None,
        info={
            "token_id": token_id,
            "maker_fee_rate_bps": maker_bps,
            "taker_fee_rate_bps": taker_bps,
            "enabled": on,
            "min_fee_amount_onchain": min_fee / 10**18,
            "fee_manager": FEE_MANAGER,
        },
    )


def _words(result_hex: str) -> list[int]:
    body = (result_hex or "").removeprefix("0x")
    return [int(body[i:i + 64], 16) for i in range(0, len(body) - 63, 64)]


def matches(topic: dict[str, Any], query: str) -> bool:
    """Whether every word of `query` appears in the topic's title, its labels
    or one of its options' titles, ignoring case."""
    words = query.lower().split()
    text = " ".join([
        str(topic.get("marketTitle") or ""),
        " ".join(str(label) for label in topic.get("labels") or []),
        " ".join(str(child.get("marketTitle") or "") for child in topic.get("childMarkets") or []),
    ]).lower()
    return all(word in text for word in words)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Opinion(Exchange):
    """Opinion, read-only.

    ```python
    import synpath

    opinion = synpath.Opinion()
    markets = opinion.fetch_markets(limit=5)
    book = opinion.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Opinion"
    book_model = "native_per_outcome"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # One request per market: the venue has no batch lookup.
        "fetch_markets_by_ids": True,
        # Volume and newest, at the venue; it has no liquidity ordering.
        "sort": "partial",
        "fetch_order_book": True,
        # One request per market: no batch endpoint.
        "fetch_order_books": True,
        # The Open API serves a wallet's own trades, never a market's tape.
        "fetch_trades": False,
        # Last-trade samples bucketed into 1h-multiple or daily bars, no volume.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        # No search endpoint: titles, options and labels are matched here over
        # at most `MAX_SEARCH_PAGES` catalog pages.
        "search": "partial",
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self,
        *,
        api_key: str | None = None,
        openapi_url: str = OPENAPI_URL,
        rpc_url: str = RPC_URL,
        timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER,
        client: Any = None,
    ):
        """`api_key` is optional: it raises the venue's limit from 5 to 15
        requests a second. Pass a `limiter` sized to match if you use one."""
        if client is None and api_key:
            client = httpx.Client(timeout=timeout, follow_redirects=True, headers={"apikey": api_key})
        self.http = HttpClient(openapi_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self.rpc = HttpClient(rpc_url, limiter=None, timeout=timeout, venue=VENUE)
        self._tokens: dict[str, tuple[str, str]] = {}
        """Per market: `(yes_token, no_token)`, filled by every catalog read."""

    # -- catalog ------------------------------------------------------------

    def _topics(self, *, page: int, status: str, sort: str | None) -> tuple[list[dict[str, Any]], bool]:
        """One page of topics and whether the venue has more."""
        params = {
            "page": page, "limit": PAGE, "marketType": 2,
            "status": _status_param(status),
            "sortBy": SORT_BY[sort] if sort else None,
        }
        result = unwrap(self.http.get("/market", params)) or {}
        topics = result.get("list") or []
        total = result.get("total")
        more = len(topics) == PAGE and (total is None or page * PAGE < int(total))
        return topics, more

    def _walk(
        self, *, cursor: str | None, status: str, sort: str | None, query: str | None,
        wanted: int, flatten: bool,
    ) -> tuple[list[Any], str | None]:
        """Read topic pages from `cursor` until `wanted` rows are in hand, as
        markets (`flatten`) or as events, keeping those in `status` and, with a
        query, those it matches. Returns the rows and the cursor after them."""
        page, skip = _cursor(cursor)
        rows: list[Any] = []
        budget = MAX_SEARCH_PAGES if query else MAX_PAGES
        for _ in range(budget):
            topics, more = self._topics(page=page, status=status, sort=sort)
            found: list[Any] = []
            for topic in topics:
                if query and not matches(topic, query):
                    continue
                if flatten:
                    found += _with_status(markets_of(topic), status)
                else:
                    found.append(normalize_event(topic))
            found = found[skip:]
            room = wanted - len(rows)
            rows += found[:room]
            if len(found) > room:
                return rows, _encode(page, skip + room)
            if not more:
                return rows, None
            page, skip = page + 1, 0
            if len(rows) >= wanted:
                return rows, _encode(page, 0)
        return rows, _encode(page, 0)

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = "volume",
    ) -> Page[Market]:
        """One page of markets, by default the highest 24h volume first.

        Categorical topics are flattened into their child markets, so a page
        holds every option of the topics it covers. The venue pages 20 topics
        at a time; a call reads up to `MAX_PAGES` of them to fill `limit`.

        `status` is `open`, `settled` or `all`. The venue cannot filter for
        `closed` (its "resolving" markets only appear unfiltered), so that
        raises rather than answering with something else.

        With `query`, topics are matched here by title, option and label,
        since the venue has no search: up to `MAX_SEARCH_PAGES` pages a call.
        """
        if sort is not None:
            check_sort(sort, venue=VENUE, supported=True)
            if sort not in SORT_BY:
                raise NotSupported(f"{VENUE}: cannot sort by {sort}; the venue orders by volume or newest only")
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        markets, next_cursor = self._walk(
            cursor=cursor, status=status, sort=sort, query=query, wanted=wanted, flatten=True,
        )
        for market in markets:
            self._remember(market)
        return Page(markets, next_cursor=next_cursor)

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of topics as events, highest 24h volume first: a
        categorical topic with its options as markets, a binary topic holding
        itself. `query` and `status` work as on `fetch_markets`."""
        wanted = page_limit(limit) or PAGE
        events, next_cursor = self._walk(
            cursor=cursor, status=status, sort="volume", query=query, wanted=wanted, flatten=False,
        )
        for event in events:
            for market in event.markets:
                self._remember(market)
        return Page(events, next_cursor=next_cursor)

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        while True:
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor or not page:
                return

    def fetch_market(self, market_id: str) -> Market:
        """One market by id: a binary topic or a categorical topic's child.

        A child's record does not name its topic, so a second read by its slug
        (which the venue resolves to the topic) supplies the full question and
        the event id. A categorical topic's own id is not a market and raises
        `MarketNotFound`; read it with `fetch_events`.
        """
        native = self.native(market_id)
        raw = (unwrap(self.http.get(f"/market/{native}"), what=f"market {native}") or {}).get("data")
        if not isinstance(raw, dict) or raw.get("marketId") is None:
            raise MarketNotFound(f"{VENUE}: no market {native}")
        parent = None
        if raw.get("labels") is None and raw.get("slug"):
            # Children carry no labels; binary topics always do.
            topic = (unwrap(self.http.get(f"/market/slug/{raw['slug']}")) or {}).get("data") or {}
            children = {str(child.get("marketId")) for child in topic.get("childMarkets") or []}
            if topic.get("marketType") == 1 and native in children:
                parent = topic
        return self._remember(normalize_market(raw, parent))

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets, in the order asked for. One request per market (two
        for a categorical child): the venue has no batch lookup. Ids it does
        not know are left out."""
        found = []
        for market_id in market_ids:
            try:
                found.append(self.fetch_market(market_id))
            except MarketNotFound:
                continue
        return found

    # -- tokens ---------------------------------------------------------------

    def _remember(self, market: Market) -> Market:
        if market.yes.venue_token_id and market.no.venue_token_id:
            self._tokens[market.venue_market_id] = (market.yes.venue_token_id, market.no.venue_token_id)
        return market

    def _tokens_of(self, market_id: str) -> tuple[str, str]:
        """`(yes_token, no_token)` for a market, from the cache or one read.
        Books, prices and fees are keyed on tokens; callers hold market ids."""
        native = self.native(market_id)
        if native not in self._tokens:
            self.fetch_market(native)
        if native not in self._tokens:
            raise MarketNotFound(f"{VENUE}: market {native} publishes no token ids")
        return self._tokens[native]

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        """The live book on one side of a market. YES and NO are separate
        tokens with separate books, so `side="no"` is the NO token's own book.
        (The venue keeps the two in step -- each NO level is a YES level
        mirrored -- but it is read, not derived.)"""
        yes_token, no_token = self._tokens_of(market_id)
        token = _token_for(side, yes_token, no_token)
        payload = unwrap(self.http.get("/token/orderbook", {"token_id": token})) or {}
        return normalize_order_book(payload, market_id=self.qualify(market_id), side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, keyed by Synpath id. One request per
        market, against the venue's 5-a-second public limit: the venue has no
        batch book endpoint."""
        return {
            self.qualify(market_id): self.fetch_order_book(market_id, side=side, depth=depth)
            for market_id in market_ids
        }

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with both sides' quotes read from the live books, and the
        last trade from the venue's latest price. Three requests."""
        refreshed = market.model_copy(deep=True)
        yes_token = market.yes.venue_token_id
        last, last_ts = (None, None)
        if yes_token:
            last, last_ts = last_trade_of(unwrap(self.http.get("/token/latest-price", {"token_id": yes_token})) or {})
        for instrument, side in ((refreshed.yes, "yes"), (refreshed.no, "no")):
            if not instrument.venue_token_id:
                continue
            payload = unwrap(self.http.get("/token/orderbook", {"token_id": instrument.venue_token_id})) or {}
            book = normalize_order_book(payload, market_id=market.id, side=side)  # type: ignore[arg-type]
            best_bid, best_ask = book.best_bid, book.best_ask
            bid = best_bid.price if best_bid else None
            ask = best_ask.price if best_ask else None
            side_last = None if last is None else (last if side == "yes" else round(1 - last, 6))
            instrument.quote = Quote(
                bid=bid,
                bid_size=best_bid.size if best_bid else None,
                ask=ask,
                ask_size=best_ask.size if best_ask else None,
                mid=round((bid + ask) / 2, 6) if bid is not None and ask is not None else None,
                last=side_last,
                last_timestamp=last_ts if side_last is not None else None,
                last_datetime=iso(last_ts) if side_last is not None else None,
            )
        return refreshed

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """Not available. The venue's Open API returns a wallet's own trades,
        never a market's; trades for a market stream only on its WebSocket,
        which needs an API key. Raised rather than answered empty, so "cannot"
        is not read as "nothing traded"."""
        raise NotSupported(
            f"{VENUE}: fetch_trades -- the Open API has no public trade tape. The last "
            f"trade is on `refresh_quotes`; the full tape is on the venue's WebSocket."
        )

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price, built from the venue's last-trade samples.

        The venue samples the last traded price hourly or daily, so
        `timeframe` is `1h`, `4h`, `6h` or `1d`; anything finer has no data
        behind it and raises. Each bar is `price_source="sampled_last"` with
        `volume=None`: a period with no trades repeats the previous price.

        A request returns one page of samples ending at `end_at` (the venue
        ignores a start), so a call walks back page by page until it passes
        the start of the window, up to `MAX_HISTORY_PAGES` requests. How far
        back the venue keeps samples differs by market: daily ones reach back
        months, hourly ones often only the last 7 days. Bars start where the
        samples do; nothing is filled in before them.
        """
        seconds = timeframe_seconds(timeframe)
        if seconds < 3600 or (seconds < 86400 and seconds % 3600):
            raise BadRequest(f"{VENUE}: no {timeframe} bars -- the venue samples prices hourly at the finest")
        interval = "1d" if seconds >= 86400 else "1h"
        yes_token, _ = self._tokens_of(market_id)
        end = int((until or _now_ms()) / 1000)
        start = int(since / 1000) if since else end - seconds * (limit or DEFAULT_BARS)
        samples: dict[int, dict[str, Any]] = {}
        end_at = end
        for _ in range(MAX_HISTORY_PAGES):
            result = unwrap(self.http.get("/token/price-history", {
                "token_id": yes_token, "interval": interval, "end_at": end_at,
            })) or {}
            points = [p for p in result.get("history") or [] if to_float(p.get("t")) is not None]
            if not points:
                break
            for point in points:
                samples[int(point["t"])] = point
            oldest = min(int(p["t"]) for p in points)
            if oldest <= start or oldest >= end_at:
                break
            end_at = oldest - 1
        history = [samples[t] for t in sorted(samples) if start <= t <= end]
        candles = candles_from_price_history(history, interval_seconds=seconds)
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """The market's taker and maker rates, read from the venue's
        FeeManager contract on BNB Chain through a public node (no key, no
        wallet). See `FeeSchedule.estimate` for how a fee is computed."""
        yes_token, _ = self._tokens_of(market_id)
        payload = self.rpc.post("", json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": FEE_MANAGER, "data": GET_FEE_RATE_SETTINGS + int(yes_token).to_bytes(32, "big").hex()}, "latest"],
        })
        if not isinstance(payload, dict) or "result" not in payload:
            raise ExchangeError(f"{VENUE}: fee lookup failed: {(payload or {}).get('error') if isinstance(payload, dict) else payload}")
        return fee_schedule_of(payload["result"], market_id=self.native(market_id), token_id=yes_token)

    def close(self) -> None:
        self.http.close()
        self.rpc.close()


def _status_param(status: str) -> str | None:
    """The venue's `status` filter for a shared status word."""
    check_status(status)
    if status == "open":
        return "activated"
    if status == "settled":
        return "resolved"
    if status == "closed":
        raise NotSupported(
            f"{VENUE}: cannot filter by closed -- the venue filters only activated and "
            f"resolved markets. Use status='all' and check each market's `status`."
        )
    return None


def _with_status(markets: list[Market], status: str) -> list[Market]:
    """A categorical topic's children can differ from the topic: one option
    resolves while the topic stays open. Each child is judged by its own."""
    if status == "all":
        return markets
    return [market for market in markets if market.status == status]


def _cursor(cursor: str | None) -> tuple[int, int]:
    """`(topic page, rows to skip on it)` from a cursor this adapter issued."""
    if not cursor:
        return 1, 0
    page, _, skip = cursor.removeprefix("p").partition(".")
    if not cursor.startswith("p") or not page.isdigit() or not (skip or "0").isdigit() or int(page) < 1:
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    return int(page), int(skip or 0)


def _encode(page: int, skip: int) -> str:
    return f"p{page}.{skip}" if skip else f"p{page}"


def _token_for(side: str, yes_token: str, no_token: str) -> str:
    if side == "yes":
        return yes_token
    if side == "no":
        return no_token
    raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
