"""Gemini Predictions (Gemini Titan, a CFTC-registered exchange), read from its
public HTTP API.

**No account is needed for market data.** Events, contracts, books, trades
and candles all answer an anonymous GET on `api.gemini.com`. Trading needs a
Gemini account with Prediction Markets access (US) and an API key; see
`synpath.trading.gemini`.

Four things about this venue that shape the adapter:

**An event holds contracts, and each contract is a Synpath market.** "Chicago
vs Milwaukee" is an event (`gemini:NBA-2610232000-CHI-MIL-M`) with one
contract per team, each a yes/no question traded as its own instrument. The
market id is the contract's `instrumentSymbol`, the exchange's own symbol
for books, streams and orders: `gemini:GEMI-NBA-2610232000-CHI-MIL-M-MIL`.
Gemini says not to build a symbol from its parts, so this adapter never does;
it reads them off the event.

**One book per contract, in YES space.** Gemini normalizes every book to the
YES side: a NO buyer at 0.30 is a YES ask at 0.70. The NO view is that book
mirrored (`derived=True`). Orders name their outcome (`yes`, `no`) separately
from their side, which the trading adapter maps onto the YES leg.

**Sports carry a taxonomy.** A sports event has `sportsMarket`: the sport,
the market family (`moneyline`, `spread`, `total`, `prop`, `correct_score`,
`to_advance`, `futures`), what it is about (`contest`, `team`, `player`),
the scope (`full_contest`, `half` 1, innings 1-5) and the statistic. It rides
on every market's `info["sports_market"]`, and listings filter on it
(`sport=`, `sports_market_type=`). A game event lists its spreads, totals and
props as child events, which the venue also lists on their own; a listing
returns each once, and the children's tickers are on the parent's
`info["child_events"]`.

**Fees follow the contract's listing date.** Contracts listed from 18 March
2026 pay `rate * C * P * (1 - P)` (taker 0.07, maker 0.0175), rounded up to
the cent; older ones `rate * C * min(P, 1 - P)` (taker 0.05, maker 0.01).
Gemini waives maker fees from 2 September to 31 December 2026. See
`fee_schedule_of`.

The public endpoints allow 120 requests a minute; the limiter here keeps to
two a second, shared process-wide.
"""
from __future__ import annotations

import base64
import hashlib
import json
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Iterator

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status, page_limit,
    pick_bars,
)
from .errors import BadRequest, MarketNotFound
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome, Page, Quote, Trade,
    iso,
)

VENUE = "gemini"
API_URL = "https://api.gemini.com"
SANDBOX_URL = "https://api.sandbox.gemini.com"

SYMBOL_PREFIX = "GEMI-"
"""Every prediction-market instrument symbol starts with this."""

LIMITER = RateLimiter(2.0, burst=5)
"""Gemini allows 120 public requests a minute (and recommends one a second),
with a burst of five queued. Two a second, shared by every client in the
process, stays inside it."""

EVENT_PAGE = 50
"""Events read per listing request while filling a page of markets."""

MAX_PAGES = 10
"""Most event pages one listing call reads while filling a page."""

TRADES_PAGE = 500
"""The most trades `/v1/trades` returns per request."""

SYMBOL_CACHE = 4096
"""Instrument symbols remembered with their event ticker. Bounded, because the
server keeps one adapter for the life of the process."""

STATUSES: dict[str, tuple[str, ...] | None] = {
    "open": ("active",),
    "closed": ("closed", "under_review"),
    "settled": ("settled", "invalid"),
    "all": None,
}
"""Gemini's event statuses for each status a listing asks for. `approved`
events are listed but not yet trading (`unopened`); `all` includes them."""

CANDLE_FRAMES = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1hr", "4h": "4hr", "6h": "6hr", "1d": "1day",
}
"""This library's timeframes and the `time_frame` each is on `/v2/candles`."""

TAKER_RATE = 0.07
MAKER_RATE = 0.0175
LEGACY_TAKER_RATE = 0.05
LEGACY_MAKER_RATE = 0.01
SCHEDULE_FROM = 1773792000000
"""2026-03-18T00:00:00Z: contracts listed from this day pay the P * (1 - P)
schedule; earlier ones the legacy min(P, 1 - P) schedule."""
MAKER_PROMO = (1788307200000, 1798761600000)
"""[2026-09-02, 2027-01-01) UTC: Gemini's maker fee-free promotion."""
FEE_URL = "https://www.gemini.com/fees/predictions"


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
    """Milliseconds from Gemini's ISO 8601 strings (`2026-10-23T21:00:00.000Z`)."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace("Z", "+00:00")
    head, dot, rest = text.partition(".")
    if dot:
        digits = rest[:len(rest) - len(rest.lstrip("0123456789"))]
        zone = rest[len(digits):]
        # Python 3.10's fromisoformat takes exactly 3 or 6 fraction digits.
        text = f"{head}.{digits[:6].ljust(6, '0')}{zone}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def quoted(value: Any) -> float | None:
    """A price strictly inside (0, 1), or `None`."""
    price = to_float(value)
    if price is None or price <= 0 or price >= 1:
        return None
    return price


def plain_text(value: Any) -> str | None:
    """A contract's rich-text description (a document tree of nodes) as text."""
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value or None
    parts: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("nodeType") == "text" and node.get("value"):
                parts.append(str(node["value"]))
            for child in node.get("content") or []:
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    text = "".join(parts).strip()
    return text or None


def native_symbol(native: str) -> str:
    """An instrument symbol as the venue spells it: upper case. Gemini's
    streams send them lower case; its REST accepts either."""
    return native.upper()


def candidate_event_tickers(symbol: str) -> list[str]:
    """The event tickers an instrument symbol could belong to, likeliest first.

    A symbol is `GEMI-<event ticker>-<contract ticker>`, and both tickers may
    contain hyphens (`GEMI-NBA-2610232000-CHI-MIL-M-MIL`), so the event is
    found by asking: the symbol with its last segment dropped first, then two,
    and so on. Whichever event lists the symbol is the answer; nothing is
    traded on a guess."""
    symbol = native_symbol(symbol)
    if not symbol.startswith(SYMBOL_PREFIX):
        raise BadRequest(f"{VENUE}: {symbol!r} is not an instrument symbol; they start with {SYMBOL_PREFIX!r}")
    parts = symbol[len(SYMBOL_PREFIX):].split("-")
    return ["-".join(parts[:end]) for end in range(len(parts) - 1, 0, -1)]


def status_of(event_status: Any, contract_status: Any = None, market_state: Any = None,
              expiry: int | None = None, now_ms: int | None = None) -> tuple[str, bool]:
    """(normalized status, accepting orders).

    `approved` is listed but not yet trading. An `active` contract whose
    `marketState` is `closed` has stopped trading: closed once its expiry has
    passed, a pause before it. `under_review` is awaiting its result.
    `invalid` was voided and is final."""
    status = str(contract_status or event_status or "").lower()
    if status in ("settled", "invalid"):
        return "settled", False
    if status in ("closed", "under_review"):
        return "closed", False
    if status == "approved":
        return "unopened", False
    if status == "active":
        if str(market_state or "open").lower() == "open":
            return "open", True
        import time

        now = now_ms if now_ms is not None else int(time.time() * 1000)
        return ("closed" if expiry is not None and expiry <= now else "open"), False
    return "unopened", False


def quotes_of(prices: dict[str, Any] | None) -> tuple[Quote, Quote]:
    """YES and NO top of book from a contract's `prices`.

    `bestBid` and `bestAsk` are the YES book; NO is its mirror. Sizes are not
    in the event payload. The last trade price has no time beside it, so it is
    left out of the quote (it is in `info["prices"]`): a last price is never
    reported without when it traded."""
    prices = prices or {}
    bid, ask = quoted(prices.get("bestBid")), quoted(prices.get("bestAsk"))
    mid = round((bid + ask) / 2, 6) if bid is not None and ask is not None else None
    yes = Quote(bid=bid, ask=ask, mid=mid)
    no = Quote(
        bid=round(1 - ask, 6) if ask is not None else None,
        ask=round(1 - bid, 6) if bid is not None else None,
        mid=round(1 - mid, 6) if mid is not None else None,
    )
    return yes, no


def _event_summary(event: dict[str, Any]) -> dict[str, Any]:
    """The event's own fields a market carries, without its contracts and children."""
    keep = (
        "id", "ticker", "title", "type", "category", "series", "status", "startTime", "expiryDate", "isLive",
        "gameId", "template", "sportsMarket", "participants", "sourceDetails", "subcategory", "volume",
        "volume24h", "liquidity", "tags",
    )
    return {key: event[key] for key in keep if key in event}


def _tags(event: dict[str, Any]) -> list[str]:
    names: list[Any] = [event.get("category")]
    names += list((event.get("subcategory") or {}).get("path") or [])
    names += list(event.get("tags") or [])
    sports = event.get("sportsMarket") or {}
    names += [sports.get("sport"), sports.get("type")]
    seen: dict[str, None] = {}
    for name in names:
        if name and str(name).strip():
            seen.setdefault(str(name).strip().lower(), None)
    return list(seen)


def _sources(event: dict[str, Any]) -> list[dict[str, Any]]:
    details = event.get("sourceDetails") or {}
    if not details.get("agency") and not details.get("index"):
        return []
    return [{"name": details.get("agency"), "index": details.get("index"), "url": None}]


def _exclusive(event: dict[str, Any]) -> bool | None:
    """Exactly one contract wins a moneyline (draw included where listed) or a
    correct score. Nothing else is said by the venue, so nothing else is
    claimed."""
    family = (event.get("sportsMarket") or {}).get("type")
    return True if family in ("moneyline", "correct_score") else None


def normalize_market(contract: dict[str, Any], event: dict[str, Any] | None = None, *,
                     now_ms: int | None = None) -> Market:
    """One contract as a binary market: YES is the contract's proposition."""
    event = event or {}
    symbol = native_symbol(str(contract.get("instrumentSymbol") or ""))
    event_ticker = event.get("ticker")
    label = str(contract.get("label") or contract.get("ticker") or symbol)
    title = str(event.get("title") or label)
    single = len(event.get("contracts") or []) <= 1
    if not single and label and label != title:
        title = f"{title}: {label}"
    expiry = parse_ts(contract.get("expiryDate") or event.get("expiryDate"))
    status, active = status_of(event.get("status"), contract.get("status"), contract.get("marketState"),
                               expiry, now_ms)
    yes_quote, no_quote = quotes_of(contract.get("prices"))
    opened = parse_ts(contract.get("effectiveDate") or event.get("effectiveDate"))
    resolved = parse_ts(contract.get("resolvedAt"))
    tags = _tags(event)
    description = plain_text(contract.get("description")) or plain_text(event.get("description"))
    return Market(
        id=ids.qualify(VENUE, symbol),
        venue=VENUE,
        venue_market_id=symbol,
        event_id=ids.qualify(VENUE, event_ticker) if event_ticker else None,
        title=title,
        description=description,
        slug=symbol.lower(),
        yes=Outcome(label=label, quote=yes_quote),
        no=Outcome(label="No" if label.lower() == "yes" else f"Not {label}", quote=no_quote),
        status=status,  # type: ignore[arg-type]
        native_status="/".join(str(s) for s in (contract.get("status"), contract.get("marketState")) if s) or None,
        active=active,
        market_type="binary",
        open_timestamp=opened,
        open_datetime=iso(opened),
        close_timestamp=expiry,
        close_datetime=iso(expiry),
        resolution_timestamp=None,
        resolution_datetime=None,
        tick_size=to_float(contract.get("priceIncrement")),
        face_value=1.0,
        book_model="shared_complement",
        # Gemini reports volume per event, in USD, not per contract: see the event.
        stats=MarketStats(),
        url=None,
        image_url=contract.get("imageUrl") or event.get("imageUrl") or None,
        category=event.get("category"),
        tags=tags,
        series_id=event.get("series") or None,
        outcome_label=label,
        neg_risk=None,
        settlement_sources=_sources(event),
        info={
            "contract": {k: v for k, v in contract.items() if k != "description"},
            "event": _event_summary(event),
            "event_ticker": event_ticker,
            "contract_ticker": contract.get("ticker"),
            "contract_id": contract.get("id"),
            "sports_market": event.get("sportsMarket"),
            "strike": contract.get("strike"),
            "prices": contract.get("prices"),
            "last_trade_price": to_float((contract.get("prices") or {}).get("lastTradePrice")),
            "quantity_increment": contract.get("quantityIncrement"),
            "quantity_minimum": contract.get("quantityMinimum"),
            "price_increment": contract.get("priceIncrement"),
            "price_minimum": contract.get("priceMinimum"),
            "market_state": contract.get("marketState"),
            "resolution_side": contract.get("resolutionSide"),
            "resolved_at": resolved,
            "settlement_value": contract.get("settlementValue"),
            "terms_url": contract.get("termsAndConditionsUrl") or event.get("termsLink"),
            "is_combo": contract.get("isCombo"),
            "listed_at": parse_ts(contract.get("createdAt")),
            "currency": "USD",
        },
    )


def normalize_event(event: dict[str, Any], *, now_ms: int | None = None) -> Event:
    """One Gemini event with its own contracts. Child events (a game's spreads,
    totals and props) are separate events; their tickers are in
    `info["child_events"]`."""
    ticker = str(event.get("ticker") or "")
    markets = [normalize_market(c, event, now_ms=now_ms) for c in event.get("contracts") or []
               if c.get("instrumentSymbol")]
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "unopened", "closed", "settled") if s in statuses), None)
    if status is None:
        status, _ = status_of(event.get("status"), now_ms=now_ms)
    close = parse_ts(event.get("expiryDate"))
    return Event(
        id=ids.qualify(VENUE, ticker),
        venue=VENUE,
        venue_event_id=ticker,
        title=str(event.get("title") or ticker),
        description=plain_text(event.get("description")),
        slug=event.get("slug"),
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=event.get("status"),
        category=event.get("category"),
        tags=_tags(event),
        series_id=event.get("series") or None,
        mutually_exclusive=_exclusive(event),
        close_timestamp=close,
        close_datetime=iso(close),
        url=None,
        image_url=event.get("imageUrl") or None,
        settlement_sources=_sources(event),
        info={
            **_event_summary(event),
            "sports_market": event.get("sportsMarket"),
            "child_events": [str(child.get("ticker")) for child in event.get("events") or [] if child.get("ticker")],
            "volume_usd": to_float(event.get("volume")),
            "volume_24h_usd": to_float(event.get("volume24h")),
            "liquidity_usd": to_float(event.get("liquidity")),
            "start_timestamp": parse_ts(event.get("startTime")),
            "currency": "USD",
        },
    )


def normalize_order_book(payload: dict[str, Any], *, symbol: str, side: BookSide = "yes",
                         depth: int | None = None) -> OrderBook:
    """`/v1/book/{symbol}` read on YES: bids are YES bids, asks YES asks.
    `side="no"` mirrors it."""
    def levels(rows: Any) -> list[OrderLevel]:
        out = []
        for row in rows or []:
            price, size = quoted((row or {}).get("price")), to_float((row or {}).get("amount"))
            if price is None or size is None or size <= 0:
                continue
            out.append(OrderLevel(price=price, size=size))
        return out

    bids = sorted(levels(payload.get("bids")), key=lambda level: level.price, reverse=True)
    asks = sorted(levels(payload.get("asks")), key=lambda level: level.price)
    if side == "no":
        bids, asks = (
            [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in asks],
            [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in bids],
        )
    elif side != "yes":
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    stamps = [int(row["timestamp"]) for key in ("bids", "asks") for row in payload.get(key) or []
              if str((row or {}).get("timestamp") or "").isdigit()]
    stamp = max(stamps) * 1000 if stamps else None
    return OrderBook(
        market_id=ids.qualify(VENUE, native_symbol(symbol)), side=side, venue=VENUE, bids=bids, asks=asks,
        timestamp=stamp, datetime=iso(stamp), book_model="shared_complement", derived=side == "no",
        depth_scope="top_n" if depth else "full", info=payload,
    )


def normalize_trade(row: dict[str, Any], *, symbol: str) -> Trade:
    """A public trade in the YES price. Gemini's `type` is the taker's side:
    `buy` lifted an ask (took YES), `sell` hit a bid (took NO)."""
    stamp = int(row.get("timestampms") or int(row.get("timestamp") or 0) * 1000)
    kind = row.get("type")
    return Trade(
        id=str(row.get("tid") or ""),
        market_id=ids.qualify(VENUE, native_symbol(symbol)),
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=to_float(row.get("price")) or 0.0,
        quantity=to_float(row.get("amount")) or 0.0,
        side=kind if kind in ("buy", "sell") else "unknown",
        info=row,
    )


def normalize_candle(row: list[Any] | tuple[Any, ...], *, interval_seconds: int) -> Candle:
    """One `/v2/candles` row `[start ms, open, high, low, close, volume]`.

    Gemini fills a period without trades with the last close at zero volume;
    such a bar is labelled `sampled_last`, not `trade`."""
    start, open_, high, low, close, volume = (list(row) + [None] * 6)[:6]
    traded = bool(volume)
    return Candle(
        timestamp=int(start),
        datetime=iso(int(start)) or "",
        open=to_float(open_), high=to_float(high), low=to_float(low), close=to_float(close),
        volume=to_float(volume) if traded else None,
        price_source="trade" if traded else "sampled_last",
        info={"row": list(row), "interval_seconds": interval_seconds},
    )


def fee_schedule_of(contract: dict[str, Any] | None = None, *, symbol: str = "", now_ms: int | None = None) -> FeeSchedule:
    """The schedule a contract trades on, from when it was listed.

    Listed from 18 March 2026: `rate * C * P * (1 - P)`, taker 0.07, maker
    0.0175 (`quadratic_theta`). Before: `rate * C * min(P, 1 - P)`, taker
    0.05, maker 0.01 (`min_price`). Both round the fee up to the cent. During
    Gemini's maker promotion (2 September to 31 December 2026) the maker rate
    is 0."""
    import time

    contract = contract or {}
    listed = parse_ts(contract.get("createdAt"))
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    legacy = listed is not None and listed < SCHEDULE_FROM
    promo = MAKER_PROMO[0] <= now < MAKER_PROMO[1]
    maker = 0.0 if promo else (LEGACY_MAKER_RATE if legacy else MAKER_RATE)
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=native_symbol(symbol or str(contract.get("instrumentSymbol") or "")),
        fee_type="min_price" if legacy else "quadratic_theta",
        taker_rate=LEGACY_TAKER_RATE if legacy else TAKER_RATE,
        maker_rate=maker,
        exponent=None if legacy else 1.0,
        rounding="up_to_cent",
        info={
            "schedule": "legacy (contracts listed before 2026-03-18)" if legacy else "2026-03-12",
            "listed_at": listed,
            "maker_promotion": promo,
            "source": FEE_URL,
        },
    )


def _fingerprint(**terms: Any) -> str:
    material = json.dumps(terms, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode()).hexdigest()[:8]


def encode_cursor(offset: int, skip: int, fingerprint: str) -> str:
    """The venue pages by offset only, so a cursor carries the event offset,
    how many markets of the next event were already returned, and the query
    it counts rows of."""
    packed = json.dumps({"o": offset, "s": skip, "f": fingerprint}, separators=(",", ":"))
    return base64.urlsafe_b64encode(packed.encode()).decode().rstrip("=")


def decode_cursor(cursor: str | None, fingerprint: str) -> tuple[int, int]:
    if not cursor:
        return 0, 0
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        state = json.loads(base64.urlsafe_b64decode(padded.encode()))
        offset, skip, carried = state["o"], state["s"], state["f"]
    except Exception:
        raise BadRequest(
            f"{VENUE}: {cursor!r} is not a cursor this API issued; pass the `next_cursor` from a previous "
            f"page, or omit it to start over"
        ) from None
    if not all(isinstance(n, int) and not isinstance(n, bool) and n >= 0 for n in (offset, skip)):
        raise BadRequest(f"{VENUE}: malformed cursor {cursor!r}")
    if carried != fingerprint:
        raise BadRequest(
            f"{VENUE}: this cursor belongs to a different query; keep the same filters while paging, or "
            f"start over without a cursor"
        )
    return offset, skip


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Gemini(Exchange):
    """Gemini Predictions, read-only. No account needed.

    ```python
    import synpath

    gemini = synpath.Gemini()
    markets = gemini.fetch_markets(sport="basketball", sports_market_type="moneyline", limit=10)
    book = gemini.fetch_order_book(markets[0].id)
    markets[0].info["sports_market"]       # {"sport": "basketball", "type": "moneyline", ...}
    ```
    """

    id = VENUE
    name = "Gemini"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        "fetch_markets_by_ids": True,
        # The events listing takes no sort parameter.
        "sort": False,
        "fetch_order_book": True,
        # No batch book endpoint: one request per market.
        "fetch_order_books": True,
        # Seven days of public trades, 500 a request, paged forward by trade id.
        "fetch_trades": True,
        # Fixed windows per timeframe (a day of 1m bars), no range parameters.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, base_url: str = API_URL, sandbox: bool = False, timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER, client: Any = None,
    ):
        """No argument is needed: market data is public. `sandbox=True` reads
        Gemini's sandbox (`api.sandbox.gemini.com`) instead."""
        url = SANDBOX_URL if sandbox and base_url == API_URL else base_url
        self.http = HttpClient(url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self._events_of: OrderedDict[str, str] = OrderedDict()

    # -- raw reads ----------------------------------------------------------

    def _remember(self, event: dict[str, Any]) -> None:
        ticker = event.get("ticker")
        for contract in event.get("contracts") or []:
            symbol = contract.get("instrumentSymbol")
            if symbol and ticker:
                self._events_of[native_symbol(str(symbol))] = str(ticker)
                self._events_of.move_to_end(native_symbol(str(symbol)))
        while len(self._events_of) > SYMBOL_CACHE:
            self._events_of.popitem(last=False)

    def fetch_gemini_event(self, ticker: str) -> dict[str, Any]:
        """One raw event by ticker, with its contracts and child events."""
        event = self.http.get(f"/v1/prediction-markets/events/{ticker}") or {}
        self._remember(event)
        for child in event.get("events") or []:
            self._remember(child)
        return event

    def _event_page(self, *, statuses: tuple[str, ...] | None, query: str | None, filters: dict[str, Any],
                    offset: int, limit: int) -> tuple[list[dict[str, Any]], int | None]:
        params: list[tuple[str, Any]] = [("status", s) for s in statuses or ()]
        for name, value in filters.items():
            values = value if isinstance(value, (list, tuple)) else [value]
            params += [(name, v) for v in values if v is not None]
        if query:
            params.append(("search", query))
        params += [("limit", str(limit)), ("offset", str(offset))]
        body = self.http.get("/v1/prediction-markets/events", params) or {}
        events = [e for e in body.get("data") or [] if isinstance(e, dict) and e.get("ticker")]
        for event in events:
            self._remember(event)
        total = (body.get("pagination") or {}).get("total")
        return events, total if isinstance(total, int) else None

    def _walk(self, *, statuses: tuple[str, ...] | None, query: str | None, filters: dict[str, Any],
              offset: int, ended: list[bool], page: int = EVENT_PAGE) -> Iterator[tuple[int, dict[str, Any]]]:
        """(offset, event) from `offset` on, at most `MAX_PAGES` requests.
        `ended` gets `True` once the listing has no more events."""
        for _ in range(MAX_PAGES):
            events, total = self._event_page(statuses=statuses, query=query, filters=filters, offset=offset,
                                             limit=page)
            for index, event in enumerate(events):
                yield offset + index, event
            offset += len(events)
            if len(events) < page or (total is not None and offset >= total):
                ended.append(True)
                return

    @staticmethod
    def _filters(category: Any, sport: Any, sports_market_type: Any, sports_market_subject: Any,
                 sports_market_scope: Any, sports_market_metric: Any) -> dict[str, Any]:
        return {
            "category": category, "sport": sport, "sports_market_type": sports_market_type,
            "sports_market_subject": sports_market_subject, "sports_market_scope": sports_market_scope,
            "sports_market_metric": sports_market_metric,
        }

    # -- catalog ------------------------------------------------------------

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", sort: str | None = None, category: str | list[str] | None = None,
        sport: str | list[str] | None = None, sports_market_type: str | list[str] | None = None,
        sports_market_subject: str | list[str] | None = None, sports_market_scope: str | list[str] | None = None,
        sports_market_metric: str | list[str] | None = None,
    ) -> Page[Market]:
        """One page of contracts (each a market), events in the venue's order,
        each event's contracts in its order.

        `query` is the venue's title search. `category` is one of
        `/v1/prediction-markets/categories` (`Sports`, `Crypto`, `Politics`).
        The sports filters are the venue's taxonomy: `sport` (`basketball`,
        `soccer`), `sports_market_type` (`moneyline`, `spread`, `total`,
        `prop`, `correct_score`, `to_advance`, `futures`),
        `sports_market_subject`, `sports_market_scope` and
        `sports_market_metric`; a list matches any of its values. The venue
        pages by offset, so a contract listed mid-walk can shift the rest by
        one; the cursor refuses to resume a different query."""
        check_sort(sort, venue=VENUE, supported=False)
        check_status(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        filters = self._filters(category, sport, sports_market_type, sports_market_subject, sports_market_scope,
                                sports_market_metric)
        fingerprint = _fingerprint(kind="markets", status=status, query=query, **filters)
        offset, skip = decode_cursor(cursor, fingerprint)
        out: list[Market] = []
        ended: list[bool] = []
        resume = offset
        for position, event in self._walk(statuses=STATUSES[status], query=query, filters=filters, offset=offset,
                                          ended=ended):
            # Every contract is counted for the cursor, the ones the status
            # filter drops included, so a resumed page lines up.
            contracts = normalize_event(event).markets
            already = skip if position == offset else 0
            for index in range(already, len(contracts)):
                if len(out) >= wanted:
                    return Page(out, next_cursor=encode_cursor(position, index, fingerprint))
                market = contracts[index]
                if status == "all" or market.status == status:
                    out.append(market)
            resume = position + 1
        if ended:
            return Page(out, next_cursor=None)
        # Stopped on the request budget, not at the end: a short page with a cursor.
        return Page(out, next_cursor=encode_cursor(resume, 0, fingerprint))

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None, cursor: str | None = None,
        status: str = "open", category: str | list[str] | None = None, sport: str | list[str] | None = None,
        sports_market_type: str | list[str] | None = None, sports_market_subject: str | list[str] | None = None,
        sports_market_scope: str | list[str] | None = None, sports_market_metric: str | list[str] | None = None,
    ) -> Page[Event]:
        """One page of events with their contracts. Filters as `fetch_markets`."""
        check_status(status)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        filters = self._filters(category, sport, sports_market_type, sports_market_subject, sports_market_scope,
                                sports_market_metric)
        fingerprint = _fingerprint(kind="events", status=status, query=query, **filters)
        offset, _ = decode_cursor(cursor, fingerprint)
        events, total = self._event_page(statuses=STATUSES[status], query=query, filters=filters, offset=offset,
                                         limit=wanted)
        more = len(events) == wanted and (total is None or offset + len(events) < total)
        return Page([normalize_event(e) for e in events],
                    next_cursor=encode_cursor(offset + len(events), 0, fingerprint) if more else None)

    def fetch_event(self, event_id: str) -> Event:
        """One event by ticker (`gemini:NBA-2610232000-CHI-MIL-M`), any status."""
        ticker = self.native(event_id)
        return normalize_event(self.fetch_gemini_event(ticker))

    def event_ticker_of(self, market_id: str, *, fetched: dict[str, dict[str, Any]] | None = None) -> str:
        """The event ticker an instrument symbol belongs to: remembered from a
        listing, else found by asking the candidate events in turn. Events
        read on the way are added to `fetched`, by ticker."""
        symbol = native_symbol(self.native(market_id))
        known = self._events_of.get(symbol)
        if known:
            return known
        for ticker in candidate_event_tickers(symbol):
            try:
                event = self.fetch_gemini_event(ticker)
            except MarketNotFound:
                continue
            if fetched is not None:
                fetched[str(event.get("ticker") or ticker)] = event
                for child in event.get("events") or []:
                    if child.get("ticker"):
                        fetched.setdefault(str(child["ticker"]), child)
            if symbol in self._events_of:
                return self._events_of[symbol]
        raise MarketNotFound(f"{VENUE}: no event lists {symbol}")

    def fetch_market(self, market_id: str) -> Market:
        """One contract by instrument symbol, any status."""
        found = self.fetch_markets_by_ids([market_id])
        if not found:
            raise MarketNotFound(f"{VENUE}: no contract {native_symbol(self.native(market_id))}")
        return found[0]

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Contracts in the order asked for, one event request per event
        (symbols of the same event share it). Symbols no event lists are left
        out."""
        symbols = list(dict.fromkeys(native_symbol(self.native(m)) for m in market_ids))
        events: dict[str, dict[str, Any]] = {}
        out: list[Market] = []
        for symbol in symbols:
            try:
                ticker = self.event_ticker_of(symbol, fetched=events)
            except (MarketNotFound, BadRequest):
                continue
            if ticker not in events:
                try:
                    events[ticker] = self.fetch_gemini_event(ticker)
                except MarketNotFound:
                    events[ticker] = {}
            event = events[ticker]
            contract = next((c for c in event.get("contracts") or []
                             if native_symbol(str(c.get("instrumentSymbol") or "")) == symbol), None)
            if contract is None:
                for child in event.get("events") or []:
                    contract = next((c for c in child.get("contracts") or []
                                     if native_symbol(str(c.get("instrumentSymbol") or "")) == symbol), None)
                    if contract is not None:
                        event = child
                        break
            if contract is not None:
                out.append(normalize_market(contract, event))
        return out

    def fetch_categories(self, *, status: str | None = None) -> list[str]:
        """The venue's event categories (`Sports`, `Crypto`, ...), optionally
        only those with events in one of Gemini's own statuses (`active`)."""
        body = self.http.get("/v1/prediction-markets/categories", {"status": status}) or {}
        return [str(c) for c in body.get("categories") or []]

    def fetch_strike(self, event_id: str) -> dict[str, Any]:
        """The strike of a crypto Up/Down event, once the venue sets it:
        `{"value", "type", "availableAt"}`."""
        return self.http.get(f"/v1/prediction-markets/events/{self.native(event_id)}/strike") or {}

    # -- market data --------------------------------------------------------

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        """A contract's whole book on YES (`depth` levels a side when given)."""
        symbol = native_symbol(self.native(market_id))
        params = {"limit_bids": depth or 0, "limit_asks": depth or 0}
        payload = self.http.get(f"/v1/book/{symbol}", params) or {}
        return normalize_order_book(payload, symbol=symbol, side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many contracts, one request each (the venue has no batch book)."""
        out: dict[str, OrderBook] = {}
        for market_id in dict.fromkeys(market_ids):
            book = self.fetch_order_book(market_id, side=side, depth=depth)
            out[book.market_id] = book
        return out

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Trade]:
        """Public trades, oldest first, from the last seven days (the venue's
        window). Without `since` or `cursor`, the most recent `limit`. With
        `cursor` (a trade id from `next_cursor`), the next `limit` after it;
        `since` (ms) starts from a time. `next_cursor` is set when a page came
        back full."""
        symbol = native_symbol(self.native(market_id))
        wanted = max(1, min(limit or TRADES_PAGE, TRADES_PAGE))
        params: dict[str, Any] = {"limit_trades": wanted}
        if cursor:
            if not cursor.isdigit():
                raise BadRequest(f"{VENUE}: {cursor!r} is not a trades cursor this API issued")
            params["since_tid"] = cursor
        elif since is not None:
            params["timestamp"] = int(since)
        rows = self.http.get(f"/v1/trades/{symbol}", params) or []
        trades = sorted((normalize_trade(row, symbol=symbol) for row in rows if not row.get("broken")),
                        key=lambda t: (t.timestamp, int(t.id) if t.id.isdigit() else 0))
        if since is not None:
            trades = [t for t in trades if t.timestamp >= since]
        forward = bool(cursor) or since is not None
        last_id = max((int(row["tid"]) for row in rows if str(row.get("tid") or "").isdigit()), default=None)
        next_cursor = str(last_id) if forward and len(rows) >= wanted and last_id is not None else None
        return Page(trades, next_cursor=next_cursor)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None, until: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        """Candles in the YES price, oldest first, from `/v2/candles`.

        The venue serves a fixed recent window per timeframe (about a day of
        1m bars) with no range parameters, so `since` and `until` filter that
        window. Timeframes: 1m, 5m, 15m, 30m, 1h, 4h, 6h, 1d. A bar with no
        trades repeats the last close and is labelled `sampled_last`."""
        if timeframe not in CANDLE_FRAMES:
            raise BadRequest(f"{VENUE}: unknown timeframe {timeframe!r}; expected one of {', '.join(CANDLE_FRAMES)}")
        from .base import timeframe_seconds

        seconds = timeframe_seconds(timeframe)
        symbol = native_symbol(self.native(market_id))
        rows = self.http.get(f"/v2/candles/{symbol}/{CANDLE_FRAMES[timeframe]}") or []
        candles = sorted((normalize_candle(row, interval_seconds=seconds) for row in rows
                          if isinstance(row, (list, tuple)) and row), key=lambda c: c.timestamp)
        if since is not None:
            candles = [c for c in candles if c.timestamp >= since]
        if until is not None:
            candles = [c for c in candles if c.timestamp < until]
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """The fee schedule the contract trades on, from its listing date (one
        event read). See `fee_schedule_of`."""
        market = self.fetch_market(market_id)
        return fee_schedule_of(market.info.get("contract") or {}, symbol=market.venue_market_id)

    def close(self) -> None:
        self.http.close()
