"""Hyperliquid: HIP-4 outcome markets, read from the public info API.

Public reads, no credentials: every call is a POST to `/info`.

Five things about this venue that shape the adapter:

**The catalog is one document.** `outcomeMeta` returns every listed outcome
and every multi-outcome question in one response, with no paging and no
search. It is read once and kept for `CATALOG_TTL` seconds; paging, sorting
and search happen here, over the whole catalog, so a search sees every market.

**Titles are rendered, not stored.** An outcome carries a template name
(`template:binaryPrice`) and its fields (`perp:BTC|threshold:83365|...`);
the human text lives in `outcomeTemplates`, one title and one rules text per
template. Titles and descriptions are rendered from those, exactly as the
venue's own front ends do. The protocol's recurring markets ("Recurring")
have no template; their titles are built from their fields here.

**A question is an event.** A multi-outcome question ("2026/2027 English
Premier League winner") holds one binary outcome per option plus a fallback
("Other"), and exactly one of them resolves Yes. A question is an `Event`
here, with its outcomes as markets; a standalone outcome is an event holding
itself, under the same id. Question ids carry a `q` (`hyperliquid:q198`), so
the two never collide.

**YES and NO are one book.** Each side is its own coin (`#<10 * outcome +
side>`), but the NO book is the YES book mirrored and every trade prints on
both. The YES book is read and the NO side derived (`book_model=
"shared_complement"`), one request instead of two.

**Fees are charged on closing, never on opening.** Spot's base rates (7 bps
taker, 4 bps maker at the lowest tier), scaled by the market's
`deployerFeeScale`, charged on the notional of a fill that closes a position
and at settlement. A fill that opens a position pays nothing.
"""
from __future__ import annotations

import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterator

from . import ids
from .base import (
    MAX_PAGE_LIMIT, TIMEFRAME_SECONDS, Capability, Exchange, HttpClient, RateLimiter,
    check_sort, check_status, page_limit, pick_bars, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel,
    Outcome, Page, Quote, Trade, iso,
)

INFO_URL = "https://api.hyperliquid.xyz"
TESTNET_INFO_URL = "https://api.hyperliquid-testnet.xyz"
VENUE = "hyperliquid"

LIMITER = RateLimiter(5.0, burst=10)
"""The venue weighs info requests (a book 2, a catalog 20) against 1200 a
minute per IP. Five a second keeps a burst of book reads inside it; the
heavy catalog reads are cached, not repeated."""

CATALOG_TTL = 30.0
"""Seconds a read of the catalog and the 24h figures is reused. Outcomes are
listed and settled on the scale of minutes, and every call here starts from
the catalog, so re-reading it per call would spend the rate budget on the
same document."""

TEMPLATES_TTL = 3600.0
"""Seconds the template texts are reused: they change only when the venue
adds a template."""

BASE_TAKER_RATE = 0.0007
BASE_MAKER_RATE = 0.0004
"""HyperCore's spot fees at the lowest volume tier, which outcome trading
uses. A higher tier pays less; the account's own tier is not known here."""

MIN_NOTIONAL = 10.0
"""The venue's minimum order value, in the quote token."""

DEFAULT_BARS = 100
"""Bars looked back when neither `since` nor `limit` bounds the window."""

CANDLE_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d")
"""Bar widths `candleSnapshot` serves. A timeframe this library offers that
the venue does not (6h) is built from the widest one that divides it."""

QUESTION_PREFIX = "q"
"""Marks a question's id (`q198`), so it cannot be read as an outcome's."""

TIME_FIELDS = ("time", "expiry", "scheduledStart", "scheduledDecision", "dateTime", "listingDeadline")
"""Fields that say when the thing a market is about happens, in the order
they are preferred as its close time."""

DEADLINE_FIELDS = ("resolutionDeadline", "decisionDeadline", "listingDeadline", "time", "expiry", "dateTime")
"""Fields that say when a market resolves at the latest."""


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


def coin(outcome: int | str, side: int = 0) -> str:
    """The coin one side of an outcome trades as: `#<10 * outcome + side>`,
    side 0 for YES and 1 for NO."""
    return f"#{10 * int(outcome) + side}"


def parse_fields(description: str | None) -> dict[str, str]:
    """`key:value|key:value` as a dict. A value may itself hold a colon
    (`perp:xyz:OURA`); only the first one separates. A bare word ("other")
    has no fields."""
    fields: dict[str, str] = {}
    for part in (description or "").split("|"):
        key, sep, value = part.partition(":")
        if sep and key.strip():
            fields[key.strip()] = value.strip()
    return fields


def parse_time(value: Any) -> int | None:
    """`20261005-1330` (UTC) as milliseconds, or None if it is not one."""
    text = str(value or "").strip()
    try:
        moment = datetime.strptime(text, "%Y%m%d-%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return int(moment.timestamp() * 1000)


def show_time(value: Any) -> str:
    """`20261005-1330` as `2026-10-05 13:30 UTC`; anything else unchanged."""
    ms = parse_time(value)
    if ms is None:
        return str(value)
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def render(text: str | None, fields: dict[str, str], types: dict[str, str] | None = None) -> str:
    """A template string with `{field}` filled in. Times are written out in
    full; a field the outcome does not carry is left out, not printed as a
    placeholder."""
    types = types or {}

    def fill(match: re.Match[str]) -> str:
        key = match.group(1)
        value = fields.get(key)
        if value is None:
            return ""
        return show_time(value) if types.get(key) == "dateTime" else value

    out = re.sub(r"\{([A-Za-z0-9_]+)\}", fill, text or "")
    return re.sub(r"\s{2,}", " ", out).replace(" :", ":").strip()


def split_metadata(description: str | None) -> tuple[str, dict[str, str]]:
    """A template's rules text and its trailing `metadata=key:value|...`."""
    text, sep, meta = (description or "").partition(" metadata=")
    return text.strip(), (parse_fields(meta) if sep else {})


def template_id(name: str | None) -> str | None:
    """The template an outcome or question is built from, by its name."""
    name = name or ""
    return name.removeprefix("template:") if name.startswith("template:") else None


def side_label(spec: dict[str, Any], fields: dict[str, str], default: str) -> str:
    """A side's display name: `template:{shortNameA}` rendered, `Yes` as is."""
    name = str(spec.get("name") or "").removeprefix("template:")
    return render(name, fields) or default


class Text:
    """What a market or a question says about itself, worked out once:
    title, rules, its side names, the fields behind them, and where it sits."""

    def __init__(self, raw: dict[str, Any], templates: dict[str, dict[str, Any]]):
        self.raw = raw
        self.fields = parse_fields(raw.get("description"))
        self.template = templates.get(template_id(raw.get("name")) or "")
        types = {key: kind for key, kind in (self.template or {}).get("keywords") or []}
        if self.template:
            rules, meta = split_metadata(self.template.get("description"))
            self.title = render(self.template.get("name"), self.fields, types)
            self.rules = render(rules, self.fields, types) or None
            self.category = (meta.get("category") or "").lower() or None
            sub = render(meta.get("subCategory"), self.fields) if meta.get("subCategory") else ""
            self.subcategory = sub.lower() if sub and sub != "N/A" else None
        else:
            self.title, self.rules, self.category, self.subcategory = recurring_text(raw, self.fields)
        self.fallback = raw.get("name") in ("template fallback", "Recurring Fallback")
        if self.fallback:
            self.title = "Other"


def recurring_text(raw: dict[str, Any], fields: dict[str, str]) -> tuple[str, str | None, str | None, str | None]:
    """Title, rules, category and subcategory for the protocol's own
    recurring markets, which carry fields but no template.

    `priceBinary` settles Yes when the underlying's mark price, interpolated
    to the expiry, is at or above the target; a `priceBucket` question holds
    one outcome per price range between its thresholds.
    """
    name = str(raw.get("name") or "")
    kind = fields.get("class")
    underlying = fields.get("underlying")
    expiry = show_time(fields["expiry"]) if fields.get("expiry") else None
    if kind == "priceBinary" and underlying:
        title = f"{underlying} at or above {fields.get('targetPrice')} at {expiry}"
        rules = (
            f"Resolves Yes if the {underlying} mark price, interpolated to {expiry}, is at or "
            f"above {fields.get('targetPrice')}; otherwise No. A recurring {fields.get('period') or ''} market."
        )
        return re.sub(r"\s{2,}", " ", title), re.sub(r"\s{2,}", " ", rules), "price", underlying.lower()
    if kind == "priceBucket" and underlying:
        title = f"{underlying} price range at {expiry}"
        rules = (
            f"Exactly one range resolves Yes: the one holding the {underlying} mark price at {expiry}, "
            f"split at {fields.get('priceThresholds', '').replace(',', ', ')}."
        )
        return title, rules, "price", underlying.lower()
    return name or str(raw.get("outcome") or raw.get("question") or ""), None, None, None


def bucket_label(index: int, thresholds: list[str]) -> str:
    """A recurring price-range outcome's label from its index: below the
    first threshold, between two, or at or above the last."""
    if not thresholds:
        return f"Range {index + 1}"
    if index == 0:
        return f"Below {thresholds[0]}"
    if index >= len(thresholds):
        return f"{thresholds[-1]} or above"
    return f"{thresholds[index - 1]} to {thresholds[index]}"


def times_of(fields: dict[str, str]) -> tuple[int | None, int | None]:
    """(close, resolution) in ms from a market's fields."""
    close = next((parse_time(fields[key]) for key in TIME_FIELDS if parse_time(fields.get(key))), None)
    deadline = next((parse_time(fields[key]) for key in DEADLINE_FIELDS if parse_time(fields.get(key))), None)
    return close, deadline or close


def sources_of(fields: dict[str, str]) -> list[dict[str, Any]]:
    source = fields.get("officialSource") or fields.get("priceDescription")
    return [{"name": source, "url": None}] if source else []


def fee_multiplier(scale: Any) -> float:
    """How many times the base fee a market charges, from its
    `deployerFeeScale`: `scale + max(scale, 1)`, the shape HIP-3 set. A
    market with no scale (the protocol's own) charges the base fee."""
    value = to_float(scale)
    if value is None:
        return 1.0
    return value + max(value, 1.0)


def normalize_market(
    raw: dict[str, Any],
    templates: dict[str, dict[str, Any]],
    *,
    question: dict[str, Any] | None = None,
    ctx: dict[str, Any] | None = None,
    settled: bool = False,
) -> Market:
    """One outcome as a binary `Market`.

    An outcome under a question takes the question's title and fields (the
    teams, the competition, the deadline) and names itself in
    `outcome_label` ("Arsenal", "Draw", "Other"). `ctx` is the YES coin's
    24h figures from `spotMetaAndAssetCtxs`.
    """
    outcome_id = int(raw["outcome"])
    native = str(outcome_id)
    own = Text(raw, templates)
    parent = Text(question, templates) if question else None
    fields = {**(parent.fields if parent else {}), **own.fields}
    label = None
    if parent:
        label = own.title
        if raw.get("name") == "Recurring Named Outcome":
            thresholds = [t for t in parent.fields.get("priceThresholds", "").split(",") if t]
            label = bucket_label(int(own.fields.get("index", 0)), thresholds)
        title = f"{parent.title}: {label}"
        rules = "\n\n".join(text for text in (parent.rules, own.rules) if text) or None
        category, subcategory = parent.category, parent.subcategory
        event_native = f"{QUESTION_PREFIX}{question['question']}"  # type: ignore[index]
    else:
        title, rules = own.title or native, own.rules
        category, subcategory = own.category, own.subcategory
        event_native = native
    specs = list(raw.get("sideSpecs") or [])
    yes_name = side_label(specs[0], fields, "Yes") if specs else "Yes"
    no_name = side_label(specs[1], fields, "No") if len(specs) > 1 else "No"
    close, deadline = times_of(fields)
    ctx = ctx or {}
    status = "settled" if settled else "open"
    tags = [tag for tag in (category, subcategory) if tag]
    return Market(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_market_id=native,
        event_id=ids.qualify(VENUE, event_native),
        title=title,
        description=rules,
        slug=None,
        yes=Outcome(label=yes_name, quote=Quote(), venue_token_id=coin(outcome_id, 0)),
        no=Outcome(label=no_name, quote=Quote(), venue_token_id=coin(outcome_id, 1)),
        status=status,  # type: ignore[arg-type]
        native_status=status if settled else "listed",
        active=not settled,
        open_timestamp=None,
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=deadline,
        resolution_datetime=iso(deadline),
        # Prices are limited to five significant figures, so the increment
        # depends on the price (0.0001 near 0.5, finer near the edges).
        tick_size=None,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(
            # `dayBaseVlm` counts contracts, and is the same on both sides'
            # coins: every trade prints on the YES and the NO coin.
            volume_24h=to_float(ctx.get("dayBaseVlm")),
            volume_total=None,
            liquidity=None,
            open_interest=None,
            volume_unit="contracts",
            liquidity_unit=None,
        ),
        url=None,
        image_url=None,
        category=category,
        tags=tags,
        series_id=None,
        outcome_label=label,
        neg_risk=None,
        settlement_sources=sources_of(fields),
        info={
            "outcome": raw,
            "question": {key: value for key, value in (question or {}).items()} or None,
            "fields": fields,
            "ctx": ctx or None,
            "min_notional": MIN_NOTIONAL,
            "fee_multiplier": fee_multiplier(raw.get("deployerFeeScale")),
        },
    )


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str, side: BookSide = "yes",
    depth: int | None = None, face_value: float = 1.0,
) -> OrderBook:
    """The YES coin's `l2Book`, priced for `side`.

    The venue serves at most 20 levels a side, so the book is `top_n`. The
    NO side is the YES book mirrored -- NO bids are `1 - YES asks` -- which
    is exactly what the venue's own NO coin shows; the result says it was
    derived.
    """
    rows = payload.get("levels") or [[], []]

    def levels(entries: Any) -> list[OrderLevel]:
        out = []
        for entry in entries or []:
            price, size = to_float(entry.get("px")), to_float(entry.get("sz"))
            if price is None or size is None or size <= 0:
                continue
            out.append(OrderLevel(price=price, size=size))
        return out

    bids, asks = levels(rows[0] if rows else []), levels(rows[1] if len(rows) > 1 else [])
    derived = side == "no"
    if derived:
        bids, asks = (
            [OrderLevel(price=round(face_value - level.price, 8), size=level.size) for level in asks],
            [OrderLevel(price=round(face_value - level.price, 8), size=level.size) for level in bids],
        )
    elif side != "yes":
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    bids = sorted(bids, key=lambda level: level.price, reverse=True)
    asks = sorted(asks, key=lambda level: level.price)
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    timestamp = int(payload["time"]) if payload.get("time") else None
    return OrderBook(
        market_id=market_id,
        side=side,
        venue=VENUE,
        bids=bids,
        asks=asks,
        timestamp=timestamp,
        datetime=iso(timestamp),
        book_model="shared_complement",
        derived=derived,
        depth_scope="top_n",
        info=payload,
    )


def normalize_trade(raw: dict[str, Any], *, market_id: str) -> Trade:
    """One print on the YES coin. `side` is the taker's: `B` bought YES,
    `A` sold it (bought NO)."""
    stamp = int(raw.get("time") or 0)
    side = {"B": "buy", "A": "sell"}.get(str(raw.get("side") or ""), "unknown")
    return Trade(
        id=str(raw.get("tid") or raw.get("hash") or stamp),
        market_id=market_id,
        timestamp=stamp,
        datetime=iso(stamp) or "",
        price=to_float(raw.get("px")) or 0.0,
        amount=to_float(raw.get("sz")) or 0.0,
        side=side,  # type: ignore[arg-type]
        info=raw,
    )


def normalize_candle(raw: dict[str, Any]) -> Candle:
    """One `candleSnapshot` bar: traded prices, volume in contracts."""
    start = int(raw["t"])
    return Candle(
        timestamp=start,
        datetime=iso(start) or "",
        open=to_float(raw.get("o")),
        high=to_float(raw.get("h")),
        low=to_float(raw.get("l")),
        close=to_float(raw.get("c")),
        volume=to_float(raw.get("v")),
        trade_count=int(raw["n"]) if raw.get("n") is not None else None,
        price_source="trade",
        info=raw,
    )


def merge_candles(candles: list[Candle], seconds: int) -> list[Candle]:
    """Narrower bars combined into `seconds`-wide ones, oldest first."""
    groups: dict[int, list[Candle]] = {}
    for candle in candles:
        start = candle.timestamp // (seconds * 1000) * seconds * 1000
        groups.setdefault(start, []).append(candle)
    merged = []
    for start in sorted(groups):
        bars = sorted(groups[start], key=lambda bar: bar.timestamp)
        highs = [bar.high for bar in bars if bar.high is not None]
        lows = [bar.low for bar in bars if bar.low is not None]
        volumes = [bar.volume for bar in bars if bar.volume is not None]
        counts = [bar.trade_count for bar in bars if bar.trade_count is not None]
        merged.append(Candle(
            timestamp=start, datetime=iso(start) or "",
            open=bars[0].open, high=max(highs) if highs else None, low=min(lows) if lows else None,
            close=bars[-1].close, volume=sum(volumes) if volumes else None,
            trade_count=sum(counts) if counts else None, price_source="trade",
            info={"bars": len(bars)},
        ))
    return merged


def fee_schedule_of(raw: dict[str, Any], *, market_id: str) -> FeeSchedule:
    """The fees one outcome charges, at the lowest volume tier.

    `fee_type="hyperliquid_outcome"`: a fill that closes a position pays the
    rate on its notional; a fill that opens one pays nothing. See
    `FeeSchedule.estimate`.
    """
    multiplier = fee_multiplier(raw.get("deployerFeeScale"))
    return FeeSchedule(
        venue=VENUE,
        scope="market",
        scope_id=market_id,
        fee_type="hyperliquid_outcome",
        taker_rate=round(BASE_TAKER_RATE * multiplier, 10),
        maker_rate=round(BASE_MAKER_RATE * multiplier, 10),
        info={
            "deployer_fee_scale": raw.get("deployerFeeScale"),
            "multiplier": multiplier,
            "base_taker_rate": BASE_TAKER_RATE,
            "base_maker_rate": BASE_MAKER_RATE,
            "charged_on": "closing fills and settlement; opening fills pay nothing",
            "tier": "lowest volume tier; an account on a higher tier pays less",
        },
    )


def matches(market: Market, query: str) -> bool:
    """Whether every word of `query` is in the market's title, labels, tags
    or rules, ignoring case."""
    text = " ".join([
        market.title, market.outcome_label or "", market.yes.label, market.no.label,
        " ".join(market.tags), market.description or "",
    ]).lower()
    return all(word in text for word in query.lower().split())


# ---------------------------------------------------------------------------
# The catalog
# ---------------------------------------------------------------------------

class Catalog:
    """One read of `outcomeMeta`, normalized: every market, every event."""

    def __init__(self, meta: dict[str, Any], templates: dict[str, dict[str, Any]], ctxs: dict[str, dict[str, Any]]):
        self.raw = meta
        outcomes = {int(raw["outcome"]): raw for raw in meta.get("outcomes") or []}
        questions = meta.get("questions") or []
        parent: dict[int, dict[str, Any]] = {}
        settled: set[int] = set()
        for question in questions:
            for member in [*(question.get("namedOutcomes") or []), question.get("fallbackOutcome")]:
                if member is not None:
                    parent[int(member)] = question
            settled.update(int(member) for member in question.get("settledNamedOutcomes") or [])
        self.markets: dict[str, Market] = {}
        for outcome_id, raw in outcomes.items():
            market = normalize_market(
                raw, templates, question=parent.get(outcome_id),
                ctx=ctxs.get(coin(outcome_id, 0)), settled=outcome_id in settled,
            )
            self.markets[market.venue_market_id] = market
        self.events: dict[str, Event] = {}
        for question in questions:
            members = [*(question.get("namedOutcomes") or []), question.get("fallbackOutcome")]
            markets = [self.markets[str(m)] for m in members if m is not None and str(m) in self.markets]
            event = normalize_question(question, templates, markets)
            self.events[event.venue_event_id] = event
        for outcome_id, raw in outcomes.items():
            if outcome_id not in parent:
                event = event_of_market(self.markets[str(outcome_id)])
                self.events[event.venue_event_id] = event


def normalize_question(question: dict[str, Any], templates: dict[str, dict[str, Any]], markets: list[Market]) -> Event:
    """A multi-outcome question as an `Event`. Exactly one of its outcomes,
    the fallback included, resolves Yes, as every question template states."""
    text = Text(question, templates)
    native = f"{QUESTION_PREFIX}{question['question']}"
    close, _ = times_of(text.fields)
    statuses = {market.status for market in markets}
    status = "open" if "open" in statuses or not statuses else "settled"
    return Event(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_event_id=native,
        title=text.title or native,
        description=text.rules,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status="listed" if status == "open" else "settled",
        category=text.category,
        tags=[tag for tag in (text.category, text.subcategory) if tag],
        series_id=None,
        mutually_exclusive=True,
        close_timestamp=close,
        close_datetime=iso(close),
        settlement_sources=sources_of(text.fields),
        info={"question": question, "fields": text.fields},
    )


def event_of_market(market: Market) -> Event:
    """A standalone outcome as an event holding itself, under its own id."""
    return Event(
        id=market.id,
        venue=VENUE,
        venue_event_id=market.venue_market_id,
        title=market.title,
        description=market.description,
        markets=[market],
        status=market.status,
        native_status=market.native_status,
        category=market.category,
        tags=list(market.tags),
        series_id=None,
        mutually_exclusive=None,
        close_timestamp=market.close_timestamp,
        close_datetime=market.close_datetime,
        settlement_sources=list(market.settlement_sources),
        info={"outcome": market.info.get("outcome")},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Hyperliquid(Exchange):
    """Hyperliquid outcome markets (HIP-4), read-only.

    ```python
    import synpath

    hl = synpath.Hyperliquid()
    markets = hl.fetch_markets(limit=5)
    book = hl.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Hyperliquid"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # One catalog read answers any number of ids.
        "fetch_markets_by_ids": True,
        # Volume and newest; the venue publishes no liquidity figure.
        "sort": "partial",
        "fetch_order_book": True,
        # One request per market: no batch book endpoint.
        "fetch_order_books": True,
        # The venue serves only the most recent prints (about ten), no history.
        "fetch_trades": "partial",
        # Real traded bars with volume.
        "fetch_ohlcv": True,
        "fetch_series": False,
        "fetch_fee_schedule": True,
        # Matched here over the whole catalog, which is one document.
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self,
        *,
        testnet: bool = False,
        info_url: str | None = None,
        timeout: float = 30.0,
        limiter: RateLimiter | None = LIMITER,
        client: Any = None,
    ):
        """`testnet=True` reads the venue's test network, where outcomes trade
        for test funds."""
        url = info_url or (TESTNET_INFO_URL if testnet else INFO_URL)
        self.http = HttpClient(url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)
        self._lock = threading.Lock()
        self._catalog: Catalog | None = None
        self._catalog_at = 0.0
        self._templates: dict[str, dict[str, Any]] | None = None
        self._templates_at = 0.0

    def _info(self, body: dict[str, Any]) -> Any:
        return self.http.post("/info", json=body)

    # -- catalog ------------------------------------------------------------

    def _template_texts(self) -> dict[str, dict[str, Any]]:
        if self._templates is None or time.monotonic() - self._templates_at > TEMPLATES_TTL:
            rows = self._info({"type": "outcomeTemplates"})
            if not isinstance(rows, list):
                raise ExchangeError(f"{VENUE}: unexpected outcomeTemplates response {str(rows)[:120]!r}")
            self._templates = {row["id"]: row for row in rows if isinstance(row, dict) and row.get("id")}
            self._templates_at = time.monotonic()
        return self._templates

    def catalog(self, *, fresh: bool = False) -> Catalog:
        """Every listed outcome and question, normalized, with 24h volume.
        Reused for `CATALOG_TTL` seconds unless `fresh`."""
        with self._lock:
            if fresh or self._catalog is None or time.monotonic() - self._catalog_at > CATALOG_TTL:
                meta = self._info({"type": "outcomeMeta"})
                if not isinstance(meta, dict) or "outcomes" not in meta:
                    raise ExchangeError(f"{VENUE}: unexpected outcomeMeta response {str(meta)[:120]!r}")
                self._catalog = Catalog(meta, self._template_texts(), self._contexts())
                self._catalog_at = time.monotonic()
            return self._catalog

    def _contexts(self) -> dict[str, dict[str, Any]]:
        """24h figures per outcome coin, from the spot asset contexts."""
        payload = self._info({"type": "spotMetaAndAssetCtxs"})
        rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
        return {row["coin"]: row for row in rows if isinstance(row, dict) and str(row.get("coin", "")).startswith("#")}

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = "volume",
    ) -> Page[Market]:
        """One page of markets, by default the highest 24h volume first.

        Every outcome of a question is a market of its own, the fallback
        ("Other") included. `status` is `open`, `settled` or `all`. A settled
        standalone outcome leaves the venue's listing, so `settled` holds
        only questions' outcomes that resolved while the rest still trade.
        The venue has no `closed` state between the two, and that raises.

        `query` matches every word against titles, labels, tags and rules,
        over the whole catalog.
        """
        check_status(status)
        if status == "closed":
            raise NotSupported(
                f"{VENUE}: no closed state -- an outcome trades until it settles. "
                f"Use 'open', 'settled' or 'all'."
            )
        _check_sort(sort)
        rows = [market for market in self.catalog().markets.values() if status == "all" or market.status == status]
        if query:
            rows = [market for market in rows if matches(market, query)]
        rows = _sorted(rows, sort, volume=lambda m: m.stats.volume_24h, newest=lambda m: int(m.venue_market_id))
        return _page(rows, cursor=cursor, limit=page_limit(limit) or MAX_PAGE_LIMIT)

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
    ) -> Page[Event]:
        """One page of events, highest 24h volume first: each question with
        its outcomes as markets, and each standalone outcome holding itself.
        `query` and `status` work as on `fetch_markets`."""
        check_status(status)
        if status == "closed":
            raise NotSupported(f"{VENUE}: no closed state; use 'open', 'settled' or 'all'.")
        rows = []
        for event in self.catalog().events.values():
            markets = [m for m in event.markets if status == "all" or m.status == status]
            if not markets:
                continue
            if query and not any(matches(m, query) for m in markets) and not _words_in(event.title, query):
                continue
            rows.append(event if len(markets) == len(event.markets) else event.model_copy(update={"markets": markets}))
        rows = _sorted(rows, "volume", volume=_event_volume, newest=None)
        return _page(rows, cursor=cursor, limit=page_limit(limit) or 20)

    def iter_events(self, *, status: str = "open") -> Iterator[Event]:
        cursor: str | None = None
        while True:
            page = self.fetch_events(cursor=cursor, status=status)
            yield from page
            cursor = page.next_cursor
            if not cursor or not page:
                return

    def fetch_market(self, market_id: str) -> Market:
        """One outcome by id (`hyperliquid:7173` or `7173`). A question's id
        (`q198`) names an event, not a market, and raises `MarketNotFound`;
        read it with `fetch_events`. An id not in the cached catalog is looked
        for once more in a fresh read before giving up."""
        native = self.native(market_id)
        if native.startswith(QUESTION_PREFIX):
            raise MarketNotFound(f"{VENUE}: {native} is a question; its outcomes are the markets (see fetch_events)")
        market = self.catalog().markets.get(native) or self.catalog(fresh=True).markets.get(native)
        if market is None:
            raise MarketNotFound(f"{VENUE}: no listed outcome {native}")
        return market

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets from one catalog read, in the order asked for. Ids the
        venue no longer lists are left out."""
        markets = self.catalog().markets
        found = []
        for market_id in market_ids:
            market = markets.get(self.native(market_id))
            if market is not None:
                found.append(market)
        return found

    def _outcome(self, market_id: str) -> int:
        native = self.native(market_id)
        if not native.isdigit():
            raise MarketNotFound(f"{VENUE}: {native!r} is not an outcome id")
        return int(native)

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        """The live book, priced for `side`: the YES coin's book, mirrored for
        `side="no"`. At most 20 levels a side, as the venue serves it."""
        if side not in ("yes", "no"):
            raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
        payload = self._info({"type": "l2Book", "coin": coin(self._outcome(market_id), 0)})
        if not isinstance(payload, dict):
            raise MarketNotFound(f"{VENUE}: no book for {self.native(market_id)}")
        return normalize_order_book(payload, market_id=self.qualify(self.native(market_id)), side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, keyed by Synpath id. One request per
        market: the venue has no batch book endpoint."""
        return {
            self.qualify(self.native(market_id)): self.fetch_order_book(market_id, side=side, depth=depth)
            for market_id in market_ids
        }

    def refresh_quotes(self, market: Market) -> Market:
        """`market` with both sides' quotes from the live book and the last
        print. Two requests."""
        refreshed = market.model_copy(deep=True)
        payload = self._info({"type": "l2Book", "coin": coin(self._outcome(market.id), 0)}) or {}
        prints = self._info({"type": "recentTrades", "coin": coin(self._outcome(market.id), 0)}) or []
        last = max(prints, key=lambda row: int(row.get("time") or 0)) if prints else None
        last_price = to_float(last.get("px")) if last else None
        last_ts = int(last["time"]) if last else None
        for instrument, side in ((refreshed.yes, "yes"), (refreshed.no, "no")):
            book = normalize_order_book(payload, market_id=market.id, side=side)  # type: ignore[arg-type]
            best_bid, best_ask = book.best_bid, book.best_ask
            bid = best_bid.price if best_bid else None
            ask = best_ask.price if best_ask else None
            side_last = None if last_price is None else (last_price if side == "yes" else round(1 - last_price, 8))
            instrument.quote = Quote(
                bid=bid,
                bid_size=best_bid.size if best_bid else None,
                ask=ask,
                ask_size=best_ask.size if best_ask else None,
                mid=round((bid + ask) / 2, 8) if bid is not None and ask is not None else None,
                last=side_last,
                last_timestamp=last_ts if side_last is not None else None,
                last_datetime=iso(last_ts) if side_last is not None else None,
            )
        return refreshed

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """The most recent prints, oldest first, in the YES price.

        The venue serves only its last few prints per coin (about ten) and
        no history behind them, so there is no cursor: a page is all there
        is. `since` drops older prints. The full tape streams on the venue's
        WebSocket."""
        if cursor:
            raise BadRequest(f"{VENUE}: fetch_trades has no further pages; the venue serves only recent prints")
        rows = self._info({"type": "recentTrades", "coin": coin(self._outcome(market_id), 0)}) or []
        market = self.qualify(self.native(market_id))
        trades = sorted((normalize_trade(row, market_id=market) for row in rows), key=lambda t: t.timestamp)
        if since is not None:
            trades = [trade for trade in trades if trade.timestamp >= since]
        if limit:
            trades = trades[-limit:]
        return Page(trades, next_cursor=None)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Traded bars in the YES price, with volume in contracts and the
        number of trades. The venue serves the most recent 5000 bars of a
        width; a timeframe it does not serve (6h) is built from the widest
        bar it does serve that divides it (2h)."""
        seconds = timeframe_seconds(timeframe)
        interval = timeframe if timeframe in CANDLE_INTERVALS else _divisor_interval(seconds)
        end = until or _now_ms()
        width = seconds * 1000
        # Without `since`, start on a bar boundary, so the oldest bar built
        # from narrower ones is whole rather than cut off.
        start = since if since is not None else (end - width * (limit or DEFAULT_BARS)) // width * width
        rows = self._info({"type": "candleSnapshot", "req": {
            "coin": coin(self._outcome(market_id), 0), "interval": interval,
            "startTime": int(start), "endTime": int(end),
        }}) or []
        candles = sorted((normalize_candle(row) for row in rows if isinstance(row, dict) and "t" in row), key=lambda c: c.timestamp)
        if interval != timeframe:
            candles = merge_candles(candles, seconds)
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        """The market's rates at the lowest volume tier, scaled by its
        deployer's fee scale. Charged on closing fills and at settlement,
        never on opening fills."""
        market = self.fetch_market(market_id)
        return fee_schedule_of(market.info.get("outcome") or {}, market_id=market.venue_market_id)

    def close(self) -> None:
        self.http.close()


def _check_sort(sort: str | None) -> None:
    """Refuse a sort key before any request: the venue publishes 24h volume
    and listing order, no liquidity figure."""
    check_sort(sort, venue=VENUE, supported=True)
    if sort not in (None, "volume", "newest"):
        raise NotSupported(f"{VENUE}: cannot sort by {sort}; the venue publishes 24h volume, no liquidity figure")


def _sorted(rows: list, sort: str | None, *, volume, newest) -> list:
    _check_sort(sort)
    if sort == "volume":
        return sorted(rows, key=lambda row: (volume(row) is None, -(volume(row) or 0.0)))
    if sort == "newest" and newest is not None:
        return sorted(rows, key=newest, reverse=True)
    return rows


def _page(rows: list, *, cursor: str | None, limit: int) -> Page:
    """`limit` rows from the position `cursor` names. The cursor is the
    position in this call's ordering of the catalog; the catalog is re-read
    every `CATALOG_TTL` seconds, so a page fetched across a refresh can repeat
    or skip a row when outcomes were listed or settled in between."""
    start = _cursor(cursor)
    chunk = rows[start:start + limit]
    after = start + len(chunk)
    return Page(chunk, next_cursor=f"i{after}" if after < len(rows) else None)


def _cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    if not cursor.startswith("i") or not cursor[1:].isdigit():
        raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
    return int(cursor[1:])


def _event_volume(event: Event) -> float | None:
    figures = [m.stats.volume_24h for m in event.markets if m.stats.volume_24h is not None]
    return sum(figures) if figures else None


def _words_in(text: str, query: str) -> bool:
    return all(word in text.lower() for word in query.lower().split())


def _divisor_interval(seconds: int) -> str:
    """The widest bar the venue serves that divides `seconds` evenly."""
    for interval in reversed(CANDLE_INTERVALS):
        width = TIMEFRAME_SECONDS.get(interval) or _interval_seconds(interval)
        if seconds % width == 0:
            return interval
    return "1m"


def _interval_seconds(interval: str) -> int:
    unit = {"m": 60, "h": 3600, "d": 86400}[interval[-1]]
    return int(interval[:-1]) * unit


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
