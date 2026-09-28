"""Polymarket US: the CFTC-regulated exchange, through its public gateway.

Public reads, no credentials.

It shares a brand with Polymarket and nothing else. This is a conventional
exchange -- one order book per market, fiat settlement, a matching engine with
a state machine -- rather than the on-chain CLOB the `polymarket` adapter
talks to. Four facts shape the adapter:

**One book, two views.** The venue lists one instrument per market, the YES
side; "to trade against an outcome, you sell YES". The NO view offered here is
the YES book reflected through the face value, the same transform the Kalshi
adapter makes, and it is marked `derived=True` rather than passed off as a
second book. The YES view is the venue's own and is not derived.

**Every data endpoint keys on the slug.** Book, best bid/offer, settlement and
price history are addressed by slug and nothing else, so the slug is the
market id here and the venue's numeric id rides along in `info`.

**The catalog carries no volume, no last trade and no sizes.** A market
payload gives best bid and best ask and nothing more about the book. Those
appear as `None` rather than zero, and `refresh_quotes` reads the book -- one
request -- for the last trade, the sizes, shares traded, open interest and the
market's live state.

**Sort parameters are accepted and ignored.** `orderBy=volume24hr` answers 200
with the same rows in the same order as no sort at all; only `orderBy=id`
changes anything. So the listing walks the catalog in id order, which keeps
offset paging stable against insertions, and `sort` orders the page here
after it is read: `newest` from the catalog's own `createdAt`, and `volume`
and `liquidity` from one best-bid/offer read per market on the page, because
the catalog carries neither figure.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Iterator

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort,
    check_status, enough_bars, page_limit, pick_bars, sort_page, timeframe_seconds,
)
from .errors import BadRequest, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, Outcome,
    OrderLevel, Page, Quote, Series, Trade, iso,
)

GATEWAY_URL = "https://gateway.polymarket.us/v1"
SITE_URL = "https://polymarket.us"
VENUE = "polymarket_us"

LIMITER = RateLimiter(15.0, burst=15)
"""The venue documents 20 requests per second per IP for the public gateway,
answering 429 with no Retry-After beyond that. Set below the ceiling because
the limit is per IP and this budget is per process: two processes on one
machine each at 20/s would be at 40/s. Shared process-wide, as the others are."""

MARKET_BATCH = 50
"""Slugs per `/markets?slug=` call. The ceiling is URI length; 50 slugs of the
lengths seen here stay under 2,500 characters."""

MAKER_THETA = -0.0125
"""The venue's published maker coefficient, a rebate. Fees are `theta * C * p
* (1 - p)`; the taker theta is on each market as `feeCoefficient`, the maker
theta is venue-wide and appears only in the fee schedule document."""

MAX_SEARCH_PAGES = 5
"""Most search pages one `search_markets` call reads while filling `limit`
markets. Past it the call returns what it has, with a cursor."""

SEARCH_SKIP = 1_000_000
"""Packs a search cursor's page and markets-to-skip into one number:
`page * SEARCH_SKIP + skip`."""

SEARCH_PAGE = 20
"""Results per search page when the caller gives no limit."""

_SETTLED_STATUSES = {"MARKET_STATUS_RESOLVED", "MARKET_STATUS_SETTLED"}
_OPEN_STATES = {"MARKET_STATE_OPEN"}
"""Book states in which the venue accepts orders. Everything else --
pre-open, suspended, halted, expired, terminated, auction -- is listed and not
trading, which is `status="open"` with `active=False`."""


# ---------------------------------------------------------------------------
# Pure normalizers. No network, no client state -- a recorded payload in, a
# unified type out.
# ---------------------------------------------------------------------------

def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def amount(value: Any) -> float | None:
    """A gateway money field, `{"value": "0.1060", "currency": "USD"}`, as a float."""
    if isinstance(value, dict):
        return to_float(value.get("value"))
    return to_float(value)


def parse_ts(value: Any) -> int | None:
    """ISO 8601, which this venue writes with nine fractional digits.

    `datetime.fromisoformat` takes at most six, so the fraction is trimmed
    first -- only the run of digits directly after the dot, never anything
    from the timezone suffix behind it. A timestamp that arrives without a
    zone is read as UTC, the same rule `types.ms` applies: reading it as the
    machine's local time put every book timestamp an hour off on a machine in
    London, and nothing in the payload's own numbers would have said so.
    Unix seconds also appear, on price history.
    """
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value if value > 1e11 else value * 1000)
    text = str(value).replace("Z", "+00:00")
    match = _FRACTION.match(text)
    if match:
        head, digits, rest = match.groups()
        text = f"{head}.{digits[:6]}{rest}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


_FRACTION = re.compile(r"^([^.]*)\.(\d+)(.*)$")


def quoted(value: Any, *, face_value: float = 1.0) -> float | None:
    """A price, or `None` when the venue's placeholder means the side is empty.

    A resolved market prints its sides at 1 and 0 and a best bid or ask of
    `null`; a live book rests strictly inside (0, face_value). None of the
    edge values is a price anyone can trade at.
    """
    price = amount(value)
    if price is None or price <= 0 or price >= face_value:
        return None
    return price


def status_of(market: dict[str, Any]) -> tuple[str, str | None, bool]:
    """(normalized status, native status, accepting orders).

    The venue's `status` is an enum (`MARKET_STATUS_OPEN`,
    `MARKET_STATUS_RESOLVED`, ...) and `closed` a flag; a resolved market is
    still `active: true` and `closed: true` at once, so the enum is read
    first and the flags only where it says nothing.
    """
    native = str(market.get("status") or market.get("ep3Status") or "").upper() or None
    if native in _SETTLED_STATUSES:
        return "settled", native, False
    if market.get("closed"):
        return "closed", native, False
    if market.get("active"):
        accepting = native is None or native.endswith("OPEN")
        return "open", native, accepting
    return "unopened", native, False


def sides_of(market: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """(YES side, NO side) by the venue's `long` flag, not by position."""
    sides = [s for s in (market.get("marketSides") or []) if isinstance(s, dict)]
    longs = [s for s in sides if s.get("long") is True]
    shorts = [s for s in sides if s.get("long") is False]
    yes = longs[0] if longs else (sides[0] if sides else {})
    no = shorts[0] if shorts else (sides[1] if len(sides) > 1 else {})
    return yes, no


def _labels(market: dict[str, Any], yes: dict[str, Any], no: dict[str, Any]) -> tuple[str, str]:
    """Display labels: the side's own text, else the market's outcomes list."""
    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except ValueError:
            outcomes = []
    outcomes = [str(o) for o in (outcomes or [])]
    yes_label = yes.get("description") or (outcomes[0] if outcomes else "Yes")
    no_label = no.get("description") or (outcomes[1] if len(outcomes) > 1 else "No")
    return str(yes_label), str(no_label)


def _tags_of(*holders: dict[str, Any] | None) -> list[str]:
    tags: list[str] = []
    for holder in holders:
        for tag in (holder or {}).get("tags") or []:
            if isinstance(tag, dict):
                text = str(tag.get("slug") or tag.get("label") or "").lower()
                if text and text not in tags:
                    tags.append(text)
    return tags


def normalize_market(market: dict[str, Any], event: dict[str, Any] | None = None) -> Market:
    """One gateway market payload as a unified `Market`."""
    event = event or {}
    slug = str(market.get("slug") or market.get("id"))
    face_value = 1.0
    status, native, accepting = status_of(market)
    yes_side, no_side = sides_of(market)
    yes_label, no_label = _labels(market, yes_side, no_side)

    # The catalog quotes the YES book's top only. The NO view is the same two
    # resting orders seen from the other side: a YES bid at 0.106 is a NO ask
    # at 0.894. Sizes and the last trade are not in the catalog at all; they
    # come from the book, through `refresh_quotes`.
    bid = quoted(market.get("bestBidQuote"), face_value=face_value)
    ask = quoted(market.get("bestAskQuote"), face_value=face_value)
    no_bid = round(face_value - ask, 6) if ask is not None else None
    no_ask = round(face_value - bid, 6) if bid is not None else None

    def mid(a: float | None, b: float | None) -> float | None:
        return round((a + b) / 2, 6) if a is not None and b is not None else None

    yes = Outcome(
        label=yes_label, quote=Quote(bid=bid, ask=ask, mid=mid(bid, ask)),
        info={k: v for k, v in yes_side.items() if k != "team"},
    )
    no = Outcome(
        label=no_label, quote=Quote(bid=no_bid, ask=no_ask, mid=mid(no_bid, no_ask)),
        info={k: v for k, v in no_side.items() if k != "team"},
    )

    event_slug = event.get("slug")
    tags = _tags_of(event, market)
    question = market.get("question") or market.get("title") or slug
    short = market.get("titleShort") or market.get("title") or None
    return Market(
        id=ids.qualify(VENUE, slug),
        venue=VENUE,
        venue_market_id=slug,
        event_id=ids.qualify(VENUE, str(event_slug)) if event_slug else None,
        title=question,
        description=market.get("description") or None,
        slug=slug,
        yes=yes,
        no=no,
        status=status,  # type: ignore[arg-type]
        native_status=native,
        active=accepting,
        market_type="binary",
        open_timestamp=parse_ts(market.get("startDate")),
        open_datetime=iso(parse_ts(market.get("startDate"))),
        close_timestamp=parse_ts(market.get("endDate")),
        close_datetime=iso(parse_ts(market.get("endDate"))),
        resolution_timestamp=parse_ts(market.get("endDate")),
        resolution_datetime=iso(parse_ts(market.get("endDate"))),
        tick_size=to_float(market.get("orderPriceMinTickSize")),
        face_value=face_value,
        book_model="shared_complement",
        # Nothing about volume, liquidity or open interest is in the catalog
        # payload. Absent is `None`; a zero would claim the venue said so.
        stats=MarketStats(
            volume_unit="contracts",
            # Every liquidity figure this venue publishes is in shares
            # (`bidShares`, `askShares` on the best bid/offer), never dollars.
            liquidity_unit="contracts",
            as_of=parse_ts(market.get("updatedAt")),
        ),
        url=f"{SITE_URL}/event/{event_slug}" if event_slug else f"{SITE_URL}/market/{slug}",
        image_url=market.get("image") or None,
        category=market.get("category") or event.get("category") or (tags or [None])[0],
        tags=tags,
        series_id=event.get("seriesSlug") or None,
        # `question` is the whole market ("National League Champion"); the
        # short title is the row inside it ("Atlanta Braves"). Where the two
        # are the same string there is no separate label to report.
        outcome_label=short if short and short != question else None,
        neg_risk=None,
        settlement_sources=[],
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
    slug = str(event.get("slug") or event.get("id"))
    tags = _tags_of(event)
    return Event(
        id=ids.qualify(VENUE, slug),
        venue=VENUE,
        venue_event_id=slug,
        title=event.get("title") or slug,
        description=event.get("description") or None,
        slug=slug,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status="closed" if event.get("closed") else "active" if event.get("active") else None,
        category=event.get("category") or (tags or [None])[0],
        tags=tags,
        series_id=event.get("seriesSlug") or None,
        # The venue publishes nothing about whether an event's markets exclude
        # one another. `None` is that answer; it is not defaulted to False.
        mutually_exclusive=None,
        close_timestamp=parse_ts(event.get("endDate")),
        close_datetime=iso(parse_ts(event.get("endDate"))),
        url=f"{SITE_URL}/event/{slug}",
        image_url=event.get("image") or None,
        settlement_sources=[],
        info={k: v for k, v in event.items() if k != "markets"},
    )


def normalize_order_book(
    payload: dict[str, Any], *, slug: str, side: str, face_value: float = 1.0,
    depth: int | None = None,
) -> OrderBook:
    """The venue's YES book, presented from one side.

    `/markets/{slug}/book` answers `marketData.bids` and `marketData.offers`,
    both best-first, on the YES instrument. The YES view is those two ladders
    as sent. The NO view reflects them: a YES offer at 0.107 is a NO bid at
    `face_value - 0.107`, and the sides swap.
    """
    data = payload.get("marketData") or payload

    def levels(rows: Any) -> list[OrderLevel]:
        out = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            price, size = amount(row.get("px")), to_float(row.get("qty"))
            if price is None or size is None or size <= 0:
                continue
            out.append(OrderLevel(price=price, size=size))
        return out

    yes_bids = levels(data.get("bids"))
    yes_asks = levels(data.get("offers"))
    if side == "yes":
        bids, asks, derived = yes_bids, yes_asks, False
    else:
        bids = [OrderLevel(price=round(face_value - level.price, 6), size=level.size) for level in yes_asks]
        asks = [OrderLevel(price=round(face_value - level.price, 6), size=level.size) for level in yes_bids]
        derived = True
    bids = sorted(bids, key=lambda level: level.price, reverse=True)
    asks = sorted(asks, key=lambda level: level.price)
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    timestamp = parse_ts(data.get("transactTime"))
    return OrderBook(
        market_id=ids.qualify(VENUE, slug),
        side=side,  # type: ignore[arg-type]
        venue=VENUE,
        bids=bids,
        asks=asks,
        timestamp=timestamp,
        datetime=iso(timestamp),
        book_model="shared_complement",
        derived=derived,
        depth_scope="top_n" if depth else "full",
        info=payload,
    )


def candles_from_price_history(
    history: list[dict[str, Any]], *, interval_seconds: int, face_value: float = 1.0,
) -> list[Candle]:
    """Bucket the venue's price samples into bars, labelled for what they are.

    `/price-history` returns `{timestamp, longPrice, shortPrice}` samples at
    an irregular cadence. The venue documents `longPrice` as the YES display
    price "normally derived from the best ask" and `shortPrice` as the NO
    display price "derived from one minus the best bid" -- and notes the two
    can sum to more than 1 because they keep the spread. So each sample is a
    bid/ask pair in disguise: `ask = longPrice`, `bid = face_value -
    shortPrice`. The bar's OHLC is of their midpoint, `price_source` says
    `bid_ask_mid`, and `bid_close`/`ask_close` carry the pair at the close.
    No trades, so no volume: `None`, not zero.
    """
    buckets: dict[int, list[tuple[float, float]]] = {}
    for point in history:
        seconds = to_float(point.get("timestamp"))
        ask = to_float(point.get("longPrice"))
        short = to_float(point.get("shortPrice"))
        if seconds is None or ask is None or short is None:
            continue
        bid = round(face_value - short, 6)
        start = int(seconds // interval_seconds) * interval_seconds
        buckets.setdefault(start, []).append((bid, ask))

    candles = []
    for start in sorted(buckets):
        pairs = buckets[start]
        mids = [round((bid + ask) / 2, 6) for bid, ask in pairs]
        candles.append(Candle(
            timestamp=start * 1000,
            datetime=iso(start * 1000) or "",
            open=mids[0], high=max(mids), low=min(mids), close=mids[-1],
            volume=None,
            trade_count=None,
            price_source="bid_ask_mid",
            bid_close=pairs[-1][0],
            ask_close=pairs[-1][1],
            info={"samples": len(pairs)},
        ))
    return candles


def reflect_candle(candle: Candle, *, face_value: float = 1.0) -> Candle:
    """A YES-denominated bar seen from the NO side: prices invert and the
    extremes swap, as do bid and ask."""
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


def fee_schedule_of(market: dict[str, Any]) -> FeeSchedule | None:
    """The venue's published fee for one market.

    Fees are `theta * C * p * (1 - p)`, the same quadratic shape as Kalshi's
    but with the coefficient published directly: the taker theta sits on each
    market as `feeCoefficient`, the maker theta is venue-wide and negative,
    a rebate paid at execution. Rounded to the cent, banker's rounding.
    """
    theta = to_float(market.get("feeCoefficient"))
    if theta is None:
        return None
    return FeeSchedule(
        venue=VENUE,
        scope="market",
        scope_id=str(market.get("slug") or market.get("id") or ""),
        fee_type="quadratic_theta",
        taker_rate=theta,
        maker_rate=MAKER_THETA,
        rounding="nearest_cent_bankers",
        info={"feeCoefficient": theta, "maker_theta": MAKER_THETA},
    )


def normalize_series(series: dict[str, Any]) -> Series:
    slug = str(series.get("slug") or series.get("id"))
    return Series(
        id=slug,
        venue=VENUE,
        title=series.get("title"),
        category=None,
        tags=[],
        # Fees on this venue are per market, not per series.
        fee=None,
        settlement_sources=[],
        info=series,
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class PolymarketUS(Exchange):
    """Polymarket US, read-only.

    ```python
    import synpath

    venue = synpath.PolymarketUS()
    markets = venue.fetch_markets(query="Fed", limit=5)
    book = venue.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Polymarket US"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        # `orderBy=volume24hr` and `orderBy=createdAt` answer 200 with the
        # rows in the venue's default order, so the page is ordered here
        # after it is read. See `fetch_markets`.
        "sort": True,
        "fetch_order_book": True,
        # No batch endpoint: one request per market, both sides of it from
        # the same response. See `fetch_order_books`.
        "fetch_order_books": True,
        # No public REST trade tape. The markets WebSocket carries trades, and
        # the book's `stats` block carries the last one.
        "fetch_trades": False,
        # Quote-derived samples, no executions, no volume.
        "fetch_ohlcv": "partial",
        "fetch_series": True,
        "fetch_fee_schedule": True,
        "search": True,
        "watch_order_book": False,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self,
        *,
        gateway_url: str = GATEWAY_URL,
        timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER,
        client: Any = None,
    ):
        self.http = HttpClient(gateway_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)

    # -- catalog ------------------------------------------------------------

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of events with their markets nested.

        With `query`, this asks the venue's search. Without one it pages the
        catalog in id order. The venue offers offset paging only, so the
        cursor carries an offset rather than a key: a market listed mid-walk
        gets a higher id and lands after the cursor, so insertions never shift
        a row; a market removed mid-walk does shift the rows after it by one,
        which offset paging cannot see. That is the venue's limit, stated
        rather than hidden.
        """
        if query:
            return self.search_events(query, limit=limit, cursor=cursor, status=status)
        active, closed = _status_flags(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        fingerprint = query_fingerprint(status=status, kind="events")
        offset = decode_cursor(cursor, fingerprint)
        payload = self.http.get("/events", {
            "limit": wanted, "offset": offset or None,
            "active": active, "closed": closed,
            "orderBy": "id", "orderDirection": "ASC",
        })
        rows = payload.get("events") or []
        events = _with_status([normalize_event(raw) for raw in rows], status)
        return Page(events, next_cursor=_next_offset(offset, len(rows), wanted, fingerprint))

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open", sort: str | None = None,
    ) -> Page[Market]:
        """One page of markets, in id order unless `sort` says otherwise.

        `query` goes to the venue's search. Paging is by offset underneath,
        for the reasons `fetch_events` gives.

        `sort` orders the page after it is read, since the venue ignores its
        own sort parameter: "this page, ordered by", not "the top of the
        catalog". `newest` reads the catalog's `createdAt` and costs nothing
        extra. `volume` and `liquidity` are not in the catalog at all, so
        each market on the page is read once from `/bbo` first -- up to a
        hundred requests for a full page, against the shared rate budget --
        and the page then carries those figures too: lifetime shares traded
        as volume, resting bid and ask shares as liquidity, open interest
        alongside. A market without the figure sorts last.
        """
        check_sort(sort, venue=VENUE, supported=bool(self.has["sort"]))
        if query:
            page = self.search_markets(query, limit=limit, cursor=cursor, status=status)
            if sort:
                return Page(self._sorted(list(page), sort), next_cursor=page.next_cursor)
            return page
        active, closed = _status_flags(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        fingerprint = query_fingerprint(status=status, kind="markets")
        offset = decode_cursor(cursor, fingerprint)
        payload = self.http.get("/markets", {
            "limit": wanted, "offset": offset or None,
            "active": active, "closed": closed,
            "orderBy": "id", "orderDirection": "ASC",
        })
        rows = payload.get("markets") or []
        markets = _with_status([normalize_market(raw) for raw in rows], status)
        if sort:
            markets = self._sorted(markets, sort)
        return Page(markets, next_cursor=_next_offset(offset, len(rows), wanted, fingerprint))

    def _sorted(self, markets: list[Market], sort: str) -> list[Market]:
        if sort == "newest":
            return sort_page(markets, lambda m: parse_ts(m.info.get("createdAt")) or m.open_timestamp)
        enriched = [self._with_bbo_stats(market) for market in markets]
        if sort == "volume":
            return sort_page(enriched, lambda m: m.stats.volume_total)
        return sort_page(enriched, lambda m: m.stats.liquidity)

    def _with_bbo_stats(self, market: Market) -> Market:
        """`market` with the figures the catalog lacks, from one `/bbo` read."""
        payload = self.http.get(f"/markets/{market.venue_market_id}/bbo")
        data = (payload.get("marketData") if isinstance(payload, dict) else None) or {}
        bid_shares, ask_shares = to_float(data.get("bidShares")), to_float(data.get("askShares"))
        liquidity = (
            (bid_shares or 0.0) + (ask_shares or 0.0)
            if bid_shares is not None or ask_shares is not None else None
        )
        return market.model_copy(update={"stats": market.stats.model_copy(update={
            "volume_total": to_float(data.get("sharesTraded")),
            "open_interest": to_float(data.get("openInterest")),
            "liquidity": liquidity,
        })})

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in one request, in the order asked for.

        Slugs are batched through `?slug=`, numeric ids through `?id=`; either
        form of id is accepted, as is an instrument id. A market the venue no
        longer lists is left out rather than raised.
        """
        keys = [_slug_of(self.native(market_id)) for market_id in market_ids]
        found: dict[str, Market] = {}
        slugs = [k for k in dict.fromkeys(keys) if not k.isdigit()]
        numeric = [k for k in dict.fromkeys(keys) if k.isdigit()]
        for field, batch_keys in (("slug", slugs), ("id", numeric)):
            for start in range(0, len(batch_keys), MARKET_BATCH):
                batch = batch_keys[start:start + MARKET_BATCH]
                payload = self.http.get(
                    "/markets", [(field, key) for key in batch] + [("limit", len(batch))],
                )
                for raw in payload.get("markets") or []:
                    market = normalize_market(raw)
                    found[market.venue_market_id] = market
                    if raw.get("id") is not None:
                        found[str(raw["id"])] = market
        return [found[key] for key in keys if key in found]

    def fetch_market(self, market_id: str) -> Market:
        key = _slug_of(self.native(market_id))
        path = f"/market/id/{key}" if key.isdigit() else f"/market/slug/{key}"
        payload = self.http.get(path)
        raw = payload.get("market") if isinstance(payload, dict) else None
        if not raw:
            raise MarketNotFound(f"{VENUE}: no market {market_id}")
        return normalize_market(raw)

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        while True:
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor or not page:
                return

    def search_events(
        self, query: str, *, limit: int | None = None, cursor: str | None = None,
        status: str = "open",
    ) -> Page[Event]:
        """Events matching `query`, most relevant first, with markets nested.

        The venue's search is paged by page number; the cursor carries it,
        tagged with the query it belongs to so a cursor from one search cannot
        be replayed into another.
        """
        check_status(status)
        wanted = page_limit(limit) or SEARCH_PAGE
        fingerprint = query_fingerprint(q=query, status=status, kind="search")
        page_no = decode_cursor(cursor, fingerprint) or 1
        payload = self.http.get("/search", {"query": query, "limit": wanted, "page": page_no})
        rows = payload.get("events") or []
        events = _with_status([normalize_event(raw) for raw in rows], status)
        next_cursor = encode_cursor(page_no + 1, fingerprint) if len(rows) >= wanted else None
        return Page(events, next_cursor=next_cursor)

    def search_markets(
        self, query: str, *, limit: int | None = None, cursor: str | None = None,
        status: str = "open",
    ) -> Page[Market]:
        """Markets matching `query`: the search's events, flattened, at most
        `limit` of them.

        The venue searches events, so this reads search pages and flattens
        their markets until it has `limit` (at most `MAX_SEARCH_PAGES` pages a
        call). The cursor records the search page and how many of its markets
        were already handed out, so an event whose markets straddle two pages
        of results is neither repeated nor cut short.
        """
        check_status(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        fingerprint = query_fingerprint(q=query, status=status, kind="search_markets")
        position = decode_cursor(cursor, fingerprint)
        page_no, skip = (divmod(position, SEARCH_SKIP) if position else (1, 0))
        collected: list[Market] = []
        next_cursor: str | None = None
        for _ in range(MAX_SEARCH_PAGES):
            payload = self.http.get("/search", {"query": query, "limit": SEARCH_PAGE, "page": page_no})
            rows = payload.get("events") or []
            markets = [m for raw in rows for m in _with_status(normalize_event(raw).markets, status)][skip:]
            room = wanted - len(collected)
            collected += markets[:room]
            if len(markets) > room:
                next_cursor = encode_cursor(page_no * SEARCH_SKIP + skip + room, fingerprint)
                break
            if len(rows) < SEARCH_PAGE:
                next_cursor = None
                break
            page_no, skip = page_no + 1, 0
            next_cursor = encode_cursor(page_no * SEARCH_SKIP, fingerprint)
            if len(collected) >= wanted:
                break
        return Page(collected, next_cursor=next_cursor)

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        slug = _slug_of(self.native(market_id))
        _check_side(side)
        payload = self.http.get(f"/markets/{slug}/book")
        return normalize_order_book(payload, slug=slug, side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, keyed by Synpath market id. No batch
        endpoint here either, so one request per market against the shared
        budget."""
        _check_side(side)
        books: dict[str, OrderBook] = {}
        for slug in dict.fromkeys(_slug_of(self.native(m)) for m in market_ids):
            payload = self.http.get(f"/markets/{slug}/book")
            books[ids.qualify(VENUE, slug)] = normalize_order_book(payload, slug=slug, side=side, depth=depth)
        return books

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with its quotes and stats re-read from the live book.

        The catalog gives best bid and ask and nothing else about the book.
        One request here fills both instruments' sizes and last trade, the
        market's shares traded and open interest, and its live state --
        `active` becomes whether the book is actually open right now.
        """
        slug = market.venue_market_id
        payload = self.http.get(f"/markets/{slug}/book")
        data = payload.get("marketData") or {}
        stats = data.get("stats") or {}
        refreshed = market.model_copy(deep=True)
        face_value = market.face_value

        yes_book = normalize_order_book(payload, slug=slug, side="yes", face_value=face_value)
        last = quoted(stats.get("lastTradePx"), face_value=face_value)
        last_ts = parse_ts(stats.get("lastTradeSetTime"))
        for index, instrument in enumerate((refreshed.yes, refreshed.no)):
            book = yes_book if index == 0 else normalize_order_book(
                payload, slug=slug, side="no", face_value=face_value,
            )
            best_bid, best_ask = book.best_bid, book.best_ask
            bid = best_bid.price if best_bid else None
            ask = best_ask.price if best_ask else None
            own_last = last if index == 0 else (
                round(face_value - last, 6) if last is not None else None
            )
            instrument.quote = Quote(
                bid=bid, bid_size=best_bid.size if best_bid else None,
                ask=ask, ask_size=best_ask.size if best_ask else None,
                mid=round((bid + ask) / 2, 6) if bid is not None and ask is not None else None,
                last=own_last,
                last_timestamp=last_ts if own_last is not None else None,
                last_datetime=iso(last_ts) if own_last is not None else None,
            )

        refreshed.stats = refreshed.stats.model_copy(update={
            "volume_total": to_float(stats.get("sharesTraded")),
            "open_interest": to_float(stats.get("openInterest")),
            "as_of": yes_book.timestamp or refreshed.stats.as_of,
        })
        state = data.get("state")
        if state:
            refreshed.native_status = str(state)
            refreshed.active = refreshed.status == "open" and str(state) in _OPEN_STATES
        return refreshed

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """Not offered over the public REST gateway.

        The venue publishes no trade tape there; executions stream on its
        markets WebSocket, and the book's `stats` block carries the last one
        (see `refresh_quotes`). Raised rather than answered empty so "cannot"
        is not read as "nothing traded".
        """
        raise NotSupported(
            f"{VENUE}: fetch_trades -- the public gateway has no trade tape. The "
            f"last trade is on the book (`refresh_quotes`); the full tape is on "
            f"the markets WebSocket."
        )

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars built from the venue's quote-derived price samples, in the YES price.

        Every bar is `price_source="bid_ask_mid"` with `volume=None`: the venue
        publishes display prices derived from the best ask and best bid, not
        executions.

        Without `since` or `until`, the venue's fixed window is used: one week
        for intraday timeframes, the whole history for daily. With either, an
        absolute window is sent and the result trimmed to it, because the
        venue can answer with samples from outside the window asked for.

        Samples are asked for at one-minute fidelity and bucketed here, except
        for daily bars, which come from the whole-history daily series (one
        small request) and are trimmed to the window. The venue's coarser
        fidelities come back empty for most windows (60 minutes over one week,
        any fidelity above one over an absolute window), while one-minute
        samples come back for every window.

        With `since` and `limit`, the window is read forward in pieces of
        `limit` periods and stops once it has `limit` bars, rather than
        downloading every one-minute sample up to now.
        """
        slug = _slug_of(self.native(market_id))
        seconds = timeframe_seconds(timeframe)
        params: dict[str, Any] = {"symbol": slug, "fidelity": 1}
        if seconds >= 86400 or not (since or until):
            if seconds <= 3600 * 6:
                params["fixedInterval"] = "INTERVAL_1W"
            else:
                params["fixedInterval"], params["fidelity"] = "INTERVAL_ALL", 1440
            payload = self.http.get("/price-history", params)
            candles = candles_from_price_history(payload.get("history") or [], interval_seconds=seconds)
            if since or until:
                end = int((until or _now_ms()) / 1000)
                start = int(since / 1000) if since else end - seconds * (limit or 100)
                candles = [c for c in candles if start * 1000 <= c.timestamp <= end * 1000]
            return pick_bars(candles, since=since, limit=limit)
        end = int((until or _now_ms()) / 1000)
        start = int(since / 1000) if since else end - seconds * (limit or 100)
        span = max(end - start, 1)
        if since and limit:
            span = max(seconds * (limit + 1), 86400)
        samples: dict[float, dict[str, Any]] = {}
        candles = []
        for piece_start in range(start, max(end, start + 1), span):
            piece = {**params, "timestamp.startTimestamp": piece_start,
                     "timestamp.endTimestamp": min(piece_start + span, end)}
            for point in self.http.get("/price-history", piece).get("history") or []:
                stamp = to_float(point.get("timestamp", point.get("t")))
                if stamp is not None:
                    samples[stamp] = point
            history = [samples[stamp] for stamp in sorted(samples)]
            # The venue can answer with samples from outside the window asked for.
            candles = [c for c in candles_from_price_history(history, interval_seconds=seconds)
                       if start * 1000 <= c.timestamp <= end * 1000]
            if enough_bars(candles, since=since, limit=limit, strict=True):
                break
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_series(self, series_id: str) -> Series:
        if series_id.isdigit():
            payload = self.http.get(f"/series/id/{series_id}")
            raw = payload.get("series") if isinstance(payload, dict) else None
            if isinstance(raw, list):
                raw = raw[0] if raw else None
        else:
            payload = self.http.get("/series", {"slug": series_id, "limit": 1})
            rows = payload.get("series") if isinstance(payload, dict) else None
            raw = rows[0] if isinstance(rows, list) and rows else None
        if not raw:
            raise MarketNotFound(f"{VENUE}: no series {series_id}")
        return normalize_series(raw)

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """The fee the venue publishes for this market: its `feeCoefficient`
        as the taker theta, the venue-wide maker rebate alongside."""
        market = self.fetch_market(market_id)
        schedule = fee_schedule_of(market.info)
        if schedule is None:
            raise MarketNotFound(f"{VENUE}: market {market_id} publishes no fee coefficient")
        return schedule

    def close(self) -> None:
        self.http.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _status_flags(status: str) -> tuple[str | None, str | None]:
    """The venue's `active` / `closed` flags for one of the shared words.

    `closed` and `settled` both ask the venue for closed markets; which of the
    two a row is comes from its own status enum, so `_with_status` decides
    after the fact. The venue's flags cannot tell them apart.
    """
    check_status(status)
    if status == "open":
        return "true", "false"
    if status in ("closed", "settled"):
        return None, "true"
    return None, None


def _with_status(items: list, status: str) -> list:
    if status == "all":
        return items
    return [item for item in items if item.status == status]


def _next_offset(offset: int, received: int, wanted: int, fingerprint: str) -> str | None:
    """A cursor for the next page, or `None` when this one was short."""
    if received < wanted:
        return None
    return encode_cursor(offset + received, fingerprint)


def query_fingerprint(**terms: Any) -> str:
    material = json.dumps(terms, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(material.encode()).hexdigest()[:8]


def encode_cursor(position: int, fingerprint: str) -> str:
    """Pack an offset (or a search page) with the query it counts rows of.

    An offset only means something for one particular query; resuming it
    under a different status or search term lands somewhere arbitrary. The
    fingerprint turns that from silent misalignment into an error.
    """
    packed = json.dumps({"o": position, "f": fingerprint}, separators=(",", ":"))
    return base64.urlsafe_b64encode(packed.encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None, fingerprint: str) -> int:
    if not cursor:
        return 0
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        state = json.loads(base64.urlsafe_b64decode(padded.encode()))
        position, carried = state["o"], state["f"]
    except Exception:
        raise BadRequest(
            f"{VENUE}: {cursor!r} is not a cursor this API issued; pass the "
            f"`next_cursor` from a previous page, or omit it to start over"
        ) from None
    if not isinstance(position, int) or isinstance(position, bool) or position < 0:
        raise BadRequest(f"{VENUE}: malformed cursor {cursor!r}")
    if carried != fingerprint:
        raise BadRequest(
            f"{VENUE}: this cursor belongs to a different query -- the offset it "
            f"carries counts rows of the search and status it was issued for. Keep "
            f"those arguments the same while paging, or start over without a cursor."
        )
    return position


def _check_side(side: str) -> None:
    if side not in ("yes", "no"):
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")


def _slug_of(market_id: str) -> str:
    """The slug or numeric id the venue keys on, from a bare native id."""
    return market_id
