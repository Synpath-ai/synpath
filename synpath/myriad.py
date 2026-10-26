"""Myriad: an onchain prediction market on BNB Chain, Abstract and Linea, read from its REST API.

Market data needs no key. An API key and its secret raise the rate limit from
30 requests per 10 seconds to 200 a second; every keyed request is then
HMAC-signed, because the venue refuses a bare key.

Five things about this venue that shape the adapter:

**Two kinds of market behind one API.** An *order book* market (BNB Chain
only, USD1) trades signed orders that a matcher settles onchain; an *AMM*
market (BNB Chain, Abstract, Linea) trades against a fixed-product pool, one
onchain transaction per trade. Every read here asks for both
(`trading_model=all`); the venue's default is AMM only.

**A market id is unique per network and per kind.** The same number names an
order-book market and an unrelated AMM market on BNB Chain, so the native id
carries both: `myriad:56-2385` is order-book market 2385 on BNB Chain (56),
`myriad:56-amm-2385` the AMM market with that number, and
`myriad:2741-amm-213` an AMM market on Abstract. A market's slug is accepted
wherever an id is.

**Binary markets, YES first.** An order-book market has two outcomes, `0`
(YES) and `1` (NO), even when the venue names them after teams; an event
(`myriad:<uuid>`) groups several of them, mutually exclusive when the venue
marks it neg-risk. An AMM market with more than two outcomes is an event of
its own, and each outcome is a binary market on it
(`myriad:59144-amm-12-3` is outcome 3): YES is that outcome, NO the rest of
the pool. A two-outcome AMM market is one binary market, outcome `0` YES.

**Books.** An order-book outcome's book is served per outcome and already
includes what the other outcome's orders imply (a NO bid at 0.94 is a YES ask
at 0.06), so `side="no"` reads outcome 1's book rather than mirroring.
Prices and sizes are 18-decimal integers. An AMM market has no resting orders:
its book here is built from the pool, one level per cent of price, each the
shares a trade moving the price that far would fill, fees included
(`info["depth_source"] == "amm"`).

**Fees.** An order-book taker pays a share of the notional that depends on
the price, published per market as a table of basis points by cent: 3 bps at
1c, 180 bps at 50c (`bps_by_price`). Makers pay nothing. An AMM market
charges a share of the value traded, set per market (`share_of_value`).

Markets denominated in Myriad's points token (PTS) are not money and are left
out of listings, and Celo, where every market is in points, is not read.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

import httpx

from . import ids
from .base import (
    MAX_PAGE_LIMIT, Capability, Exchange, HttpClient, RateLimiter, check_sort, check_status,
    page_limit, pick_bars, timeframe_seconds,
)
from .errors import BadRequest, ExchangeError, MarketNotFound
from .types import (
    BookSide, Candle, Event, FeeSchedule, Market, MarketStats, OrderBook, OrderLevel, Outcome,
    Page, Quote, Trade, iso,
)

API_URL = "https://api-v2.myriadprotocol.com"
VENUE = "myriad"
WEB_URL = "https://myriad.markets/markets/"

LIMITER = RateLimiter(2.5, burst=5)
"""30 requests per 10 seconds per IP without a key, shared process-wide."""

KEYED_LIMITER = RateLimiter(100.0, burst=100)
"""With an API key the venue allows 200 a second; half of it, shared."""

NETWORKS: dict[int, str] = {56: "BNB Chain", 2741: "Abstract", 59144: "Linea"}
"""The networks read here. Celo (42220) lists only points markets."""

ORDER_BOOK_NETWORK = 56
"""Order-book markets run on BNB Chain only."""

POINTS = "PTS"
"""Myriad's points token. Markets in it are not money."""

PAGE = 100
"""Rows per listing page: the venue's maximum."""

MAX_PAGES = 40
"""Most listing pages one walk reads: 4,000 rows, against about 400 open today."""

SCALE = 10**18
"""Order-book prices are 1e18 fractions of a token; sizes are in the token's
smallest unit, and every order-book market's token (USD1) has 18 decimals."""

OB_DECIMALS = 18

TICK = 0.01
"""Order-book prices are whole cents."""

STATES = {"open": "open", "closed": "closed", "resolved": "settled"}
"""The venue's market state as this library's status."""

STATUS_PARAM = {"open": "open", "closed": "closed", "settled": "resolved", "all": None}

SORT_BY = {"volume": "volume_24h", "liquidity": "liquidity", "newest": "published_at"}
"""This library's sort keys as the venue's own."""

CHARTS = (("24h", 86400), ("7d", 7 * 86400), ("30d", 30 * 86400))
"""The price-chart timeframes the venue keeps per outcome, shortest first:
5-minute, 30-minute and 4-hour samples. `all` covers the rest, at 4 hours."""

DEFAULT_BARS = 96

NATIVE_ID = re.compile(r"^(?P<network>\d+)-(?:(?P<amm>amm)-)?(?P<market>\d+)(?:-(?P<outcome>\d+))?$")


@dataclass(frozen=True)
class Ref:
    """What a native id names: a market on a network, of one kind, and for a
    many-outcome AMM market, the outcome."""

    network: int
    market: int
    amm: bool = False
    outcome: int | None = None

    @property
    def model(self) -> str:
        return "amm" if self.amm else "ob"

    @property
    def native(self) -> str:
        return native_id(self.network, self.market, amm=self.amm, outcome=self.outcome)

    @property
    def market_native(self) -> str:
        """The id of the whole venue market, without an outcome."""
        return native_id(self.network, self.market, amm=self.amm)


def native_id(network: Any, market: Any, *, amm: bool, outcome: int | None = None) -> str:
    """`56-2385`, `56-amm-2385`, `2741-amm-412-3`."""
    head = f"{int(network)}-{'amm-' if amm else ''}{int(market)}"
    return head if outcome is None else f"{head}-{int(outcome)}"


def parse_id(native: str) -> Ref | None:
    """A native id's parts, or `None` for anything else (a slug)."""
    found = NATIVE_ID.match(native)
    if not found:
        return None
    outcome = found.group("outcome")
    if outcome is not None and not found.group("amm"):
        raise BadRequest(f"{VENUE}: {native!r}: an order-book market is binary; it has no outcome suffix")
    return Ref(int(found.group("network")), int(found.group("market")), amm=bool(found.group("amm")),
               outcome=int(outcome) if outcome is not None else None)


def to_float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def iso_ms(value: Any) -> int | None:
    """An ISO timestamp, or unix seconds, as epoch milliseconds."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) or str(value).isdigit():
        number = int(value)
        return number * 1000 if number < 10**11 else number
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def units(value: Any, decimals: int = OB_DECIMALS) -> float | None:
    """An amount the venue writes in the token's smallest unit (`"1000000000000000000"`)
    or, on some routes, already in tokens (`"98.00000000"`), as tokens."""
    if value in (None, ""):
        return None
    text = str(value)
    try:
        if "." in text or "e" in text.lower():
            return float(text)
        return int(text) / 10**decimals
    except ValueError:
        return None


def scaled_price(value: Any) -> float | None:
    """A 1e18-scaled price as a fraction."""
    if value in (None, ""):
        return None
    try:
        return int(str(value)) / SCALE
    except ValueError:
        return to_float(value)


def is_amm(raw: dict[str, Any]) -> bool:
    return str(raw.get("tradingModel") or "").lower() == "amm" or raw.get("executionMode") == 0


def is_points(raw: dict[str, Any]) -> bool:
    return str((raw.get("token") or {}).get("symbol") or "").upper() == POINTS


def ref_of(raw: dict[str, Any], outcome: int | None = None) -> Ref:
    return Ref(int(raw["networkId"]), int(raw["id"]), amm=is_amm(raw), outcome=outcome)


def status_of(raw: dict[str, Any]) -> tuple[str, str, bool]:
    """(normalized status, native word, accepting orders)."""
    native = str(raw.get("state") or "")
    status = STATES.get(native, "closed")
    return status, native, status == "open"


def tags_of(raw: dict[str, Any]) -> list[str]:
    """The market's topics and tag slugs, lowercase, without duplicates."""
    words = [str(t) for t in raw.get("topics") or []]
    for tag in raw.get("tags") or []:
        if isinstance(tag, dict):
            words.append(str(tag.get("slug") or tag.get("title") or ""))
        elif tag:
            words.append(str(tag))
    seen: dict[str, None] = {}
    for word in words:
        if word.strip():
            seen.setdefault(word.strip().lower(), None)
    return list(seen)


def settlement_sources_of(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """The oracle the venue names, and the resolution source when it differs."""
    sources: list[dict[str, Any]] = []
    oracle = raw.get("oracle") or {}
    if oracle.get("name"):
        sources.append({"name": str(oracle["name"]), "url": oracle.get("url") or None})
    source = str(raw.get("resolutionSource") or "").strip()
    if source and all(s.get("url") != source for s in sources):
        sources.append({"name": str(raw.get("resolutionTitle") or source), "url": source if source.startswith("http") else None})
    return sources


def amm_fees(raw: dict[str, Any]) -> tuple[float, float]:
    """(buy, sell): an AMM market's whole fee on each side, as a share of the value."""
    fees = raw.get("fees") or {}

    def total(side: str) -> float:
        part = fees.get(side) or {}
        return sum(to_float(part.get(k)) or 0.0 for k in ("fee", "treasury_fee", "distributor_fee"))

    return total("buy"), total("sell")


def _clean(price: float | None) -> float | None:
    """A quoted price inside (0, 1), or `None`: 0 and 1 are not quotes."""
    if price is None or not 0 < price < 1:
        return None
    return round(price, 6)


def _quote(bid: float | None, ask: float | None, *, mid: float | None = None) -> Quote:
    bid, ask = _clean(bid), _clean(ask)
    if bid is not None and ask is not None:
        mid = round((bid + ask) / 2, 6) if mid is None else mid
    else:
        mid = None
    return Quote(bid=bid, ask=ask, mid=_clean(mid) if mid is not None else None)


def amm_quote(price: float | None, *, buy_fee: float, sell_fee: float) -> Quote:
    """A pool's price as the venue would trade one share at the margin: buying
    costs `price / (1 - fee)`, selling returns `price * (1 - fee)`."""
    if price is None or not 0 < price < 1:
        return Quote()
    return _quote(price * (1 - sell_fee), price / (1 - buy_fee) if buy_fee < 1 else None, mid=price)


def complement(quote: Quote) -> Quote:
    """The other side's view of a quote: bid and ask swap, at `1 - p`."""
    flip = (lambda p: round(1 - p, 6) if p is not None else None)
    return Quote(bid=flip(quote.ask), ask=flip(quote.bid), mid=flip(quote.mid),
                 bid_size=quote.ask_size, ask_size=quote.bid_size)


def _outcome_info(outcome: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in outcome.items() if k != "price_charts"}


def normalize_market(raw: dict[str, Any], outcome: int | None = None) -> Market:
    """One binary market: a two-outcome market as itself (outcome 0 YES), or
    one outcome of a many-outcome AMM market (`outcome`), YES that outcome and
    NO the rest of the pool."""
    outcomes = list(raw.get("outcomes") or [])
    amm = is_amm(raw)
    many = len(outcomes) > 2
    if many and outcome is None:
        raise BadRequest(f"{VENUE}: market {raw.get('id')} has {len(outcomes)} outcomes; name one")
    ref = ref_of(raw, outcome if many else None)
    status, native_status, accepting = status_of(raw)
    buy_fee, sell_fee = amm_fees(raw) if amm else (0.0, 0.0)

    def side_quote(o: dict[str, Any]) -> Quote:
        if amm:
            return amm_quote(to_float(o.get("price")), buy_fee=buy_fee, sell_fee=sell_fee)
        return _quote(to_float(o.get("bestBid")), to_float(o.get("bestAsk")))

    if many:
        chosen = next((o for o in outcomes if int(o.get("id", -1)) == outcome), None)
        if chosen is None:
            raise MarketNotFound(f"{VENUE}: market {raw.get('id')} has no outcome {outcome}")
        yes_quote = side_quote(chosen)
        yes = Outcome(label=str(chosen.get("title") or f"Outcome {outcome}"), quote=yes_quote,
                      venue_token_id=str(chosen["tokenId"]) if chosen.get("tokenId") is not None else None,
                      price_change_24h=to_float(chosen.get("priceChange24h")), info=_outcome_info(chosen))
        no = Outcome(label=f"Not {yes.label}", quote=complement(yes_quote), info={"outcomes": "the rest of the pool"})
        title = f"{raw.get('title') or ''} — {yes.label}".strip(" —")
        label: str | None = yes.label
        event_native = ref.market_native
    else:
        first = outcomes[0] if outcomes else {}
        second = outcomes[1] if len(outcomes) > 1 else {}
        yes = Outcome(label=str(first.get("title") or "Yes"), quote=side_quote(first),
                      venue_token_id=str(first["tokenId"]) if first.get("tokenId") is not None else None,
                      price_change_24h=to_float(first.get("priceChange24h")), info=_outcome_info(first))
        no = Outcome(label=str(second.get("title") or "No"), quote=side_quote(second),
                     venue_token_id=str(second["tokenId"]) if second.get("tokenId") is not None else None,
                     price_change_24h=to_float(second.get("priceChange24h")), info=_outcome_info(second))
        title = str(raw.get("title") or raw.get("slug") or ref.native)
        label = (raw.get("shortName") or None) if raw.get("eventId") else None
        event_native = str(raw["eventId"]) if raw.get("eventId") else ref.native
    opened, close = iso_ms(raw.get("publishedAt")), iso_ms(raw.get("expiresAt"))
    resolves = iso_ms(raw.get("resolvesAt")) or close
    tags = tags_of(raw)
    token = raw.get("token") or {}
    resolved = raw.get("resolvedOutcomeId")
    winner: str | None = None
    if status == "settled" and resolved is not None and int(resolved) >= 0 and not raw.get("voided"):
        if many:
            winner = "yes" if int(resolved) == outcome else "no"
        else:
            winner = {0: "yes", 1: "no"}.get(int(resolved))
    return Market(
        id=ids.qualify(VENUE, ref.native),
        venue=VENUE,
        venue_market_id=ref.native,
        event_id=ids.qualify(VENUE, event_native),
        title=title,
        description=raw.get("description") or None,
        slug=raw.get("slug") or None,
        yes=yes,
        no=no,
        status=status,  # type: ignore[arg-type]
        native_status=native_status or None,
        active=accepting,
        market_type="binary",
        open_timestamp=opened,
        open_datetime=iso(opened),
        close_timestamp=close,
        close_datetime=iso(close),
        resolution_timestamp=resolves,
        resolution_datetime=iso(resolves),
        tick_size=None if amm else TICK,
        face_value=1.0,
        book_model="shared_complement",
        stats=MarketStats(
            volume_24h=to_float(raw.get("volume24h")), volume_total=to_float(raw.get("volume")),
            liquidity=to_float(raw.get("liquidity")), volume_unit="collateral", liquidity_unit="collateral",
        ),
        url=f"{WEB_URL}{raw['slug']}" if raw.get("slug") else None,
        image_url=raw.get("imageUrl") or None,
        category=(tags or [None])[0],
        tags=tags,
        series_id=None,
        outcome_label=label,
        neg_risk=bool(raw.get("negRisk")) if raw.get("negRisk") is not None else None,
        settlement_sources=settlement_sources_of(raw),
        info={
            "market": {k: v for k, v in raw.items() if k not in ("description", "outcomes")},
            "network_id": ref.network,
            "network": NETWORKS.get(ref.network),
            "market_id": ref.market,
            "trading_model": ref.model,
            "outcome": outcome if many else None,
            "outcomes": len(outcomes),
            "token": token,
            "points": is_points(raw),
            "event_id": raw.get("eventId"),
            "outcome_index": raw.get("outcomeIndex"),
            "in_play": raw.get("inPlay"),
            "in_play_starts_at": raw.get("inPlayStartsAt"),
            "moneyline": raw.get("moneyline"),
            "scoreboard": raw.get("scoreboard"),
            "external_sources": raw.get("externalSources") or [],
            "polymarket_ids": [str(s.get("externalMarketId")) for s in raw.get("externalSources") or []
                               if str(s.get("providerName") or "").lower() == "polymarket" and s.get("externalMarketId")],
            "voided": bool(raw.get("voided")),
            "winning_outcome": winner,
        },
    )


def markets_of(raw: dict[str, Any]) -> list[Market]:
    """The binary markets in one venue market: itself, or one per outcome of
    a many-outcome AMM market."""
    outcomes = raw.get("outcomes") or []
    if len(outcomes) > 2:
        return [normalize_market(raw, int(o["id"])) for o in outcomes if o.get("id") is not None]
    return [normalize_market(raw)] if raw.get("id") is not None else []


def normalize_event(row: dict[str, Any]) -> Event:
    """An event row (`type: "event"`) with its markets, or a market row as an
    event of its own: one market, or one per outcome of a many-outcome AMM market."""
    if row.get("type") == "event" or (row.get("markets") is not None and row.get("outcomes") is None):
        markets = [m for child in row.get("markets") or [] for m in markets_of(child)]
        native = str(row["id"])
        exclusive: bool | None = True if row.get("negRisk") else None
        info = {k: v for k, v in row.items() if k not in ("markets", "description")}
    else:
        markets = markets_of(row)
        native = ref_of(row).native
        exclusive = True if len(row.get("outcomes") or []) > 2 else None
        info = {k: v for k, v in row.items() if k not in ("description", "outcomes")}
    status_word = str(row.get("state") or "")
    statuses = {m.status for m in markets}
    status = next((s for s in ("open", "closed", "settled") if s in statuses), STATES.get(status_word, "closed"))
    close = iso_ms(row.get("expiresAt"))
    tags = tags_of(row) or (markets[0].tags if markets else [])
    return Event(
        id=ids.qualify(VENUE, native),
        venue=VENUE,
        venue_event_id=native,
        title=str(row.get("title") or row.get("slug") or native),
        description=row.get("description") or None,
        slug=row.get("slug") or None,
        markets=markets,
        status=status,  # type: ignore[arg-type]
        native_status=status_word or None,
        category=(tags or [None])[0],
        tags=tags,
        mutually_exclusive=exclusive,
        close_timestamp=close,
        close_datetime=iso(close),
        url=f"{WEB_URL}{row['slug']}" if row.get("slug") else None,
        image_url=row.get("imageUrl") or None,
        settlement_sources=markets[0].settlement_sources if markets else [],
        info=info,
    )


# ---------------------------------------------------------------------------
# Books
# ---------------------------------------------------------------------------

def book_levels(rows: Any, *, decimals: int = OB_DECIMALS) -> list[OrderLevel]:
    """`[[price, amount], ...]` in 1e18 units as levels in fractions and shares."""
    out = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price, size = scaled_price(row[0]), units(row[1], decimals)
        if price is None or size is None or size <= 0:
            continue
        out.append(OrderLevel(price=round(price, 6), size=size))
    return out


def normalize_order_book(
    payload: dict[str, Any], *, market_id: str, side: BookSide = "yes", depth: int | None = None,
) -> OrderBook:
    """One outcome's book as the venue serves it: outcome 0's for YES,
    outcome 1's for NO, each already holding what the other side's orders imply."""
    if side not in ("yes", "no"):
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    bids = sorted(book_levels(payload.get("bids")), key=lambda level: level.price, reverse=True)
    asks = sorted(book_levels(payload.get("asks")), key=lambda level: level.price)
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks, timestamp=None, datetime=None,
        book_model="shared_complement", derived=False, depth_scope="top_n" if depth else "full",
        info={"orderbook": payload, "outcome": 0 if side == "yes" else 1},
    )


def _price(pools: list[float], index: int) -> float:
    inverse = [1 / n for n in pools]
    return inverse[index] / sum(inverse)


def _after_buy(pools: list[float], index: int, collateral: float) -> tuple[list[float], float]:
    """Pools after `collateral` (net of fees) buys outcome `index`, and the shares it gets."""
    product = 1.0
    for n in pools:
        product *= n
    after = [n + collateral for n in pools]
    rest = 1.0
    for j, n in enumerate(after):
        if j != index:
            rest *= n
    after[index] = product / rest
    return after, pools[index] + collateral - after[index]


def _after_sell(pools: list[float], index: int, collateral: float) -> tuple[list[float], float]:
    """Pools after selling outcome `index` for `collateral` (before fees), and the shares it takes."""
    product = 1.0
    for n in pools:
        product *= n
    after = [n - collateral for n in pools]
    rest = 1.0
    for j, n in enumerate(after):
        if j != index:
            rest *= n
    after[index] = product / rest
    return after, after[index] - (pools[index] - collateral)


def _solve(price_at: Any, target: float, low: float, high: float, *, rising: bool) -> float:
    """The collateral at which the pool's price reaches `target`, by bisection."""
    for _ in range(80):
        middle = (low + high) / 2
        reached = price_at(middle)
        if (reached < target) == rising:
            low = middle
        else:
            high = middle
    return (low + high) / 2


def amm_book_levels(
    pools: list[float], index: int, *, buy_fee: float = 0.0, sell_fee: float = 0.0, tick: float = TICK,
) -> tuple[list[OrderLevel], list[OrderLevel]]:
    """A fixed-product pool as a book on outcome `index`: one level per `tick`
    of price, each the shares a trade moving the price into that cent fills,
    at that slice's average price with fees included. Bids best first, asks
    best first."""
    if len(pools) < 2 or any(n <= 0 for n in pools) or not 0 <= index < len(pools):
        return [], []
    start = _price(pools, index)
    steps = round(1 / tick)
    asks: list[OrderLevel] = []
    spent = shares = 0.0
    high = 1.0
    for cent in range(int(start / tick) + 1, steps):
        target = cent * tick
        while _price(_after_buy(pools, index, high)[0], index) < target:
            high *= 2
        collateral = _solve(lambda c: _price(_after_buy(pools, index, c)[0], index), target, 0.0, high, rising=True)
        got = _after_buy(pools, index, collateral)[1]
        size, paid = got - shares, (collateral - spent) / (1 - buy_fee)
        if size > 1e-12:
            asks.append(OrderLevel(price=round(paid / size, 6), size=round(size, 9)))
        spent, shares = collateral, got
    bids: list[OrderLevel] = []
    received = sold = 0.0
    ceiling = min(n for j, n in enumerate(pools) if j != index)
    for cent in range(int(-(-start // tick)) - 1, 0, -1):
        target = cent * tick
        collateral = _solve(lambda c: _price(_after_sell(pools, index, c)[0], index), target, 0.0, ceiling * (1 - 1e-12),
                            rising=False)
        taken = _after_sell(pools, index, collateral)[1]
        size, paid = taken - sold, (collateral - received) * (1 - sell_fee)
        if size > 1e-12:
            bids.append(OrderLevel(price=round(paid / size, 6), size=round(size, 9)))
        received, sold = collateral, taken
    return bids, asks


def amm_order_book(
    raw: dict[str, Any], *, market_id: str, index: int, side: BookSide = "yes", depth: int | None = None,
    native_no: bool = True,
) -> OrderBook:
    """An AMM market's book on outcome `index`, built from its pool. For
    `side="no"` on a two-outcome market (`native_no`), the other outcome's
    own pool book; on one outcome of many, the YES book mirrored."""
    if side not in ("yes", "no"):
        raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
    pools = [to_float(o.get("shares")) or 0.0 for o in raw.get("outcomes") or []]
    buy_fee, sell_fee = amm_fees(raw)
    derived = False
    if side == "no" and native_no:
        bids, asks = amm_book_levels(pools, 1 - index, buy_fee=buy_fee, sell_fee=sell_fee)
    else:
        bids, asks = amm_book_levels(pools, index, buy_fee=buy_fee, sell_fee=sell_fee)
        if side == "no":
            derived = True
            bids, asks = (
                [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in asks],
                [OrderLevel(price=round(1 - level.price, 6), size=level.size) for level in bids],
            )
    if depth:
        bids, asks = bids[:depth], asks[:depth]
    return OrderBook(
        market_id=market_id, side=side, venue=VENUE, bids=bids, asks=asks, timestamp=None, datetime=None,
        book_model="shared_complement", derived=derived, depth_scope="top_n" if depth else "full",
        info={"depth_source": "amm", "pools": pools, "buy_fee": buy_fee, "sell_fee": sell_fee, "outcome": index},
    )


# ---------------------------------------------------------------------------
# Trades, candles, fees
# ---------------------------------------------------------------------------

def _yes_leg(outcome: int, buying: bool, price: float) -> tuple[float, bool]:
    """A trade on outcome 1 is the other side of YES at `1 - price`."""
    return (price, buying) if outcome == 0 else (1 - price, not buying)


def normalize_trades(rows: list[dict[str, Any]], *, market_id: str) -> list[Trade]:
    """An order-book market's fills, one per taker, in the YES price.

    The venue lists every leg of a match, the taker's and each maker's, and
    the splits and merges of positions beside them. Only the taker pays a
    fee, so a leg with a fee is a taker's; a match where no leg paid one is
    reported once per distinct price and size, its side unknown. Splits and
    merges are not trades and are left out."""
    by_tx: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for n, row in enumerate(rows):
        if str(row.get("side") or "") in ("split", "merge"):
            continue
        by_tx.setdefault(str(row.get("txHash") or n), []).append((n, row))
    trades: list[Trade] = []
    for tx, legs in by_tx.items():
        takers = [(n, row) for n, row in legs if (units((row.get("fees") or {}).get("total")) or 0) > 0]
        seen: set[tuple[float, float]] = set()
        for position, (n, row) in enumerate(takers or legs):
            price = to_float(row.get("price"))
            quantity = units(row.get("amount"))
            if price is None or quantity is None:
                continue
            yes_price, buying = _yes_leg(int(row.get("outcome") or 0), str(row.get("side")) == "buy", price)
            if not takers:
                key = (round(yes_price, 6), round(quantity, 8))
                if key in seen:
                    continue
                seen.add(key)
            stamp = iso_ms(row.get("timestamp")) or 0
            trades.append(Trade(
                id=f"{tx}:{position}", market_id=market_id, timestamp=stamp, datetime=iso(stamp) or "",
                price=round(yes_price, 6), quantity=quantity,
                side=("buy" if buying else "sell") if takers else "unknown", info=row,
            ))
    return sorted(trades, key=lambda t: t.timestamp)


def normalize_amm_trade(row: dict[str, Any], *, market_id: str, index: int, many: bool) -> Trade | None:
    """One pool trade (`/markets/:id/events`, `buy` or `sell`) in the YES price,
    or `None` for anything else (liquidity, claims) and, on one outcome of
    many, a trade on another outcome."""
    action = str(row.get("action") or "")
    if action not in ("buy", "sell"):
        return None
    outcome = int(row.get("outcomeId") or 0)
    shares, value = to_float(row.get("shares")), to_float(row.get("value"))
    if not shares or value is None:
        return None
    buying = action == "buy"
    if many:
        if outcome != index:
            return None
        price = value / shares
    else:
        price, buying = _yes_leg(outcome, buying, value / shares)
    stamp = iso_ms(row.get("timestamp")) or 0
    return Trade(
        id=f"{row.get('txId') or stamp}:{outcome}:{action}", market_id=market_id, timestamp=stamp,
        datetime=iso(stamp) or "", price=round(price, 6), quantity=shares, side="buy" if buying else "sell",
        info={k: v for k, v in row.items() if k not in ("marketTitle", "imageUrl")},
    )


def chart_for(seconds: int) -> str:
    """The shortest price-chart timeframe covering `seconds`."""
    return next((name for name, span in CHARTS if span >= seconds), "all")


def candles_from_chart(points: list[dict[str, Any]], *, interval_seconds: int) -> list[Candle]:
    """The venue's price samples for one outcome bucketed into bars, oldest
    first. No volume behind them."""
    buckets: dict[int, list[tuple[int, float]]] = {}
    for point in points:
        stamp, price = iso_ms(point.get("timestamp")), to_float(point.get("value"))
        if stamp is None or price is None:
            continue
        price = round(price, 6)
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


def fee_schedule_of(raw: dict[str, Any], *, market_id: str) -> FeeSchedule:
    """An order-book market's taker table (`bps_by_price`), or an AMM
    market's share of the value (`share_of_value`)."""
    fees = raw.get("fees") or {}
    if is_amm(raw):
        buy, sell = amm_fees(raw)
        return FeeSchedule(
            venue=VENUE, scope="market", scope_id=market_id, fee_type="share_of_value",
            taker_rate=buy, maker_rate=None,
            info={"buy_rate": buy, "sell_rate": sell, "fees": fees, "currency": (raw.get("token") or {}).get("symbol")},
        )
    taker = [float(v) for v in fees.get("taker_fee_bps_array") or []]
    maker = [float(v) for v in fees.get("maker_fee_bps_array") or []]
    return FeeSchedule(
        venue=VENUE, scope="market", scope_id=market_id, fee_type="bps_by_price",
        taker_rate=max(taker) / 10_000 if taker else to_float(fees.get("taker_fee_bps")),
        maker_rate=max(maker) / 10_000 if maker else (to_float(fees.get("maker_fee_bps")) or 0.0) / 10_000,
        taker_bps_by_cent=taker or None, maker_bps_by_cent=maker or None,
        info={"taker_fee_bps": fees.get("taker_fee_bps"), "maker_fee_bps": fees.get("maker_fee_bps"),
              "currency": (raw.get("token") or {}).get("symbol")},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class Myriad(Exchange):
    """Myriad, read-only. No key needed.

    ```python
    import synpath

    myriad = synpath.Myriad()
    markets = myriad.fetch_markets(limit=5, topic="sports")
    book = myriad.fetch_order_book(markets[0].id)
    ```
    """

    id = VENUE
    name = "Myriad"
    book_model = "shared_complement"
    has: dict[str, Capability] = {
        "fetch_markets": True,
        "fetch_events": True,
        "fetch_market": True,
        # One request per network and kind of market.
        "fetch_markets_by_ids": True,
        "sort": True,
        "fetch_order_book": True,
        # One request per market (an AMM book is built from its pool).
        "fetch_order_books": True,
        "fetch_trades": True,
        # The venue's price samples per outcome, no volume.
        "fetch_ohlcv": "partial",
        "fetch_series": False,
        "fetch_fee_schedule": True,
        "search": True,
        "match_market": False,
        "match_event": False,
    }

    def __init__(
        self, *, api_key: str | None = None, api_secret: str | None = None, api_url: str = API_URL,
        timeout: float = 30.0, limiter: RateLimiter | None = LIMITER, client: Any = None,
    ):
        """`api_key` and `api_secret` are optional: with both, every request
        is HMAC-signed and the venue's limit rises to 200 a second (pass
        `limiter=KEYED_LIMITER`, or one of your own, to use it). The venue
        refuses a key without its signature."""
        if bool(api_key) != bool(api_secret):
            raise BadRequest(f"{VENUE}: an API key needs its secret: the venue refuses unsigned keyed requests")
        if client is None:
            auth = None
            if api_key and api_secret:
                from .trading.myriad_signing import HmacAuth

                auth = HmacAuth(api_key, api_secret)
            client = httpx.Client(timeout=timeout, follow_redirects=True, auth=auth)
        self.http = HttpClient(api_url, limiter=limiter, timeout=timeout, client=client, venue=VENUE)

    def _get(self, path: str, params: Any = None, *, what: str = "") -> Any:
        try:
            return self.http.get(path, params)
        except MarketNotFound as exc:
            raise MarketNotFound(f"{VENUE}: no {what or 'market'}", body=exc.body, status=exc.status) from None

    # -- catalog ------------------------------------------------------------

    def _params(
        self, *, status: str, sort: str | None, query: str | None, topic: Any, tag: Any, in_play: bool | None,
        trading_model: str, network: Any,
    ) -> dict[str, Any]:
        if trading_model not in ("all", "ob", "amm"):
            raise BadRequest(f"{VENUE}: trading_model is all, ob or amm, not {trading_model!r}")
        networks = network if isinstance(network, (list, tuple)) else ([network] if network else list(NETWORKS))
        for net in networks:
            if int(net) not in NETWORKS:
                raise BadRequest(f"{VENUE}: network {net} is not read; one of {', '.join(map(str, NETWORKS))}")
        return {
            "trading_model": trading_model,
            "state": STATUS_PARAM[status],
            "network_id": ",".join(str(int(n)) for n in networks),
            "sort": SORT_BY[sort] if sort else None,
            "order": "desc" if sort else None,
            "keyword": query or None,
            "topics": _joined(topic, title=True),
            "tags": _joined(tag),
            "in_play": None if in_play is None else str(bool(in_play)).lower(),
        }

    def _listing(self, params: dict[str, Any], page: int) -> tuple[list[dict[str, Any]], bool]:
        """One listing page, and whether another follows."""
        payload = self._get("/markets", {**params, "page": page, "limit": PAGE}) or {}
        rows = list(payload.get("data") or []) if isinstance(payload, dict) else []
        pagination = payload.get("pagination") or {} if isinstance(payload, dict) else {}
        return rows, bool(rows) and bool(pagination.get("hasNext"))

    def fetch_markets(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = None, topic: str | list[str] | None = None, tag: str | list[str] | None = None,
        in_play: bool | None = None, trading_model: str = "all", network: int | list[int] | None = None,
    ) -> Page[Market]:
        """One page of markets, a many-outcome AMM market counted outcome by
        outcome; points markets left out. `sort` reads the venue's 24-hour
        volume, liquidity or publication time. `query` is the venue's search
        over titles, descriptions and outcomes.

        Venue filters: `topic` ("sports", "crypto", ...), `tag` (a tag slug:
        a league, a team, an asset), `in_play` (sports markets live now),
        `trading_model` (`all`, `ob` for order books, `amm`) and `network`
        (56, 2741, 59144)."""
        check_status(status)
        check_sort(sort, venue=VENUE, supported=True)
        params = self._params(status=status, sort=sort, query=query, topic=topic, tag=tag, in_play=in_play,
                              trading_model=trading_model, network=network)
        wanted = page_limit(limit) or MAX_PAGE_LIMIT
        page, skip = _cursor(cursor)
        out: list[Market] = []
        for _ in range(MAX_PAGES):
            rows, more = self._listing(params, page)
            batch = [m for row in rows if not is_points(row) for m in markets_of(row)][skip:]
            room = wanted - len(out)
            out += batch[:room]
            if len(batch) > room:
                return Page(out, next_cursor=f"{page}:{skip + room}")
            if not more:
                return Page(out, next_cursor=None)
            page, skip = page + 1, 0
            if len(out) >= wanted:
                return Page(out, next_cursor=f"{page}:0")
        return Page(out, next_cursor=f"{page}:0")

    def iter_markets(self, **kwargs: Any) -> Iterator[Market]:
        """Every market the listing holds, page by page."""
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            page = self.fetch_markets(cursor=cursor, **kwargs)
            yield from page
            cursor = page.next_cursor
            if not cursor:
                return

    def fetch_events(
        self, *, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open",
        sort: str | None = None, topic: str | list[str] | None = None, tag: str | list[str] | None = None,
        in_play: bool | None = None, trading_model: str = "all", network: int | list[int] | None = None,
    ) -> Page[Event]:
        """One page of events: the venue's events with their markets, and
        every other market as an event of its own (a many-outcome AMM market
        holding one market per outcome). Filters as `fetch_markets`."""
        check_status(status)
        check_sort(sort, venue=VENUE, supported=True)
        params = self._params(status=status, sort=sort, query=query, topic=topic, tag=tag, in_play=in_play,
                              trading_model=trading_model, network=network)
        # The cursor is `page:size`: a page number only means something at the size it was read with.
        page, size = _cursor(cursor)
        size = size or page_limit(limit) or PAGE
        payload = self._get("/markets", {**params, "group_by_event": "true", "page": page, "limit": size}) or {}
        rows = list(payload.get("data") or []) if isinstance(payload, dict) else []
        more = bool((payload.get("pagination") or {}).get("hasNext")) if isinstance(payload, dict) else False
        events = [normalize_event(row) for row in rows if not is_points(row)]
        return Page([e for e in events if e.markets], next_cursor=f"{page + 1}:{size}" if more and rows else None)

    def iter_events(self, **kwargs: Any) -> Iterator[Event]:
        cursor: str | None = None
        for _ in range(MAX_PAGES):
            page = self.fetch_events(cursor=cursor, **kwargs)
            yield from page
            cursor = page.next_cursor
            if not cursor:
                return

    def fetch_event(self, event_id: str) -> Event:
        """One event by id or slug: a venue event (`myriad:<uuid>`), or a
        market's own (`myriad:56-2385`, `myriad:2741-amm-412`)."""
        native = self.native(event_id)
        ref = parse_id(native)
        if ref is not None:
            return normalize_event(self.raw_market(native))
        return normalize_event({**(self._get(f"/events/{native}", what=f"event {native}") or {}), "type": "event"})

    def raw_market(self, market_id: str) -> dict[str, Any]:
        """The venue's own record of a market, by id or slug."""
        native = self.native(market_id)
        ref = parse_id(native)
        if ref is None:
            raw = self._get(f"/markets/{native}", {"trading_model": "all"}, what=f"market {native}")
        else:
            raw = self._get(f"/markets/{ref.market}", {"network_id": ref.network, "trading_model": ref.model},
                            what=f"market {ref.market_native}")
        if not isinstance(raw, dict) or raw.get("id") is None:
            raise MarketNotFound(f"{VENUE}: no market {native}")
        if ref is not None and (int(raw.get("networkId") or 0) != ref.network or is_amm(raw) != ref.amm):
            raise MarketNotFound(f"{VENUE}: no market {native}")
        return raw

    def _resolve(self, market_id: str) -> tuple[Ref, dict[str, Any] | None]:
        """A market id's parts; a slug is looked up (and its record returned)."""
        native = self.native(market_id)
        ref = parse_id(native)
        if ref is not None:
            return ref, None
        raw = self.raw_market(native)
        if len(raw.get("outcomes") or []) > 2:
            raise MarketNotFound(f"{VENUE}: {native!r} has {len(raw['outcomes'])} outcomes, which makes it an "
                                 f"event; its markets are {ref_of(raw).native}-<outcome>")
        return ref_of(raw), raw

    def fetch_market(self, market_id: str) -> Market:
        """One market by id or slug, open or settled. A many-outcome AMM
        market is an event; name one of its outcomes (`...-amm-412-3`)."""
        ref, raw = self._resolve(market_id)
        raw = raw or self.raw_market(ref.market_native)
        outcomes = raw.get("outcomes") or []
        if len(outcomes) > 2 and ref.outcome is None:
            raise MarketNotFound(f"{VENUE}: {ref.native} has {len(outcomes)} outcomes, which makes it an event; "
                                 f"its markets are {ref.native}-<outcome>")
        return normalize_market(raw, ref.outcome if len(outcomes) > 2 else None)

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets in the order asked for: one request per network and
        kind of market. Ids the venue does not know are left out."""
        refs: dict[str, Ref] = {}
        for market_id in market_ids:
            try:
                ref, _ = self._resolve(market_id)
            except MarketNotFound:
                continue
            refs[market_id] = ref
        groups: dict[tuple[int, str], set[int]] = {}
        for ref in refs.values():
            groups.setdefault((ref.network, ref.model), set()).add(ref.market)
        found: dict[str, Market] = {}
        for (network, model), numbers in groups.items():
            ordered = sorted(numbers)
            for start in range(0, len(ordered), PAGE):
                chunk = ordered[start:start + PAGE]
                payload = self._get("/markets", {"ids": ",".join(map(str, chunk)), "network_id": network,
                                                 "trading_model": model, "limit": PAGE}) or {}
                for row in payload.get("data") or [] if isinstance(payload, dict) else []:
                    for market in markets_of(row):
                        found[market.venue_market_id] = market
        return [found[ref.native] for ref in refs.values() if ref.native in found]

    # -- market data --------------------------------------------------------

    def fetch_order_book(
        self, market_id: str, *, side: BookSide = "yes", depth: int | None = None,
    ) -> OrderBook:
        """An order-book market's book for one side, read from the venue; an
        AMM market's, built from its pool (one request for the pool)."""
        ref, raw = self._resolve(market_id)
        qualified = self.qualify(ref.native)
        if ref.amm:
            raw = raw or self.raw_market(ref.market_native)
            outcomes = raw.get("outcomes") or []
            many = len(outcomes) > 2
            if many and ref.outcome is None:
                raise MarketNotFound(f"{VENUE}: {ref.native} is an event; name one of its outcomes")
            return amm_order_book(raw, market_id=qualified, index=ref.outcome if many else 0, side=side,  # type: ignore[arg-type]
                                  depth=depth, native_no=not many)
        if side not in ("yes", "no"):
            raise BadRequest(f"{VENUE}: unknown side {side!r}; expected 'yes' or 'no'")
        payload = self._get(f"/markets/{ref.market}/orderbook",
                            {"network_id": ref.network, "outcome": 0 if side == "yes" else 1},
                            what=f"order book for {ref.native}") or {}
        if not isinstance(payload, dict):
            raise ExchangeError(f"{VENUE}: unexpected order book for {ref.native}")
        return normalize_order_book(payload, market_id=qualified, side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        """Books for many markets, one request each (the venue has no batch read)."""
        books: dict[str, OrderBook] = {}
        for market_id in dict.fromkeys(self.qualify(self.native(m)) for m in market_ids):
            book = self.fetch_order_book(market_id, side=side, depth=depth)
            books[book.market_id] = book
        return books

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """A page of the market's trades, newest first from the venue and
        returned oldest first, in the YES price with the taker's side. On an
        order book, one per taker fill (see `normalize_trades`); on an AMM,
        the pool's buys and sells."""
        ref, raw = self._resolve(market_id)
        qualified = self.qualify(ref.native)
        page = int(cursor) if cursor and cursor.isdigit() else 1
        size = min(page_limit(limit) or 100, 100)
        if ref.amm:
            many = ref.outcome is not None
            payload = self._get(f"/markets/{ref.market}/events", {
                "network_id": ref.network, "trading_model": "amm", "page": page, "limit": size,
                "since": since // 1000 if since else None,
            }, what=f"trades for {ref.native}") or {}
            rows = payload.get("data") or [] if isinstance(payload, dict) else []
            trades = [t for t in (normalize_amm_trade(r, market_id=qualified, index=ref.outcome or 0, many=many)
                                  for r in rows) if t is not None]
            more = bool((payload.get("pagination") or {}).get("hasNext")) if isinstance(payload, dict) else False
        else:
            payload = self._get(f"/markets/{ref.market}/trades", {
                "network_id": ref.network, "page": page, "limit": size,
            }, what=f"trades for {ref.native}") or []
            rows = payload if isinstance(payload, list) else (payload.get("data") or [])
            trades = normalize_trades(rows, market_id=qualified)
            more = len(rows) >= size
        if since:
            trades = [t for t in trades if t.timestamp >= since]
        return Page(sorted(trades, key=lambda t: t.timestamp), next_cursor=str(page + 1) if more else None)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        """Bars in the YES price from the venue's price samples, no volume
        (`price_source="sampled_mid"`): 5-minute samples over the last day,
        30-minute over a week, 4-hour beyond."""
        ref, raw = self._resolve(market_id)
        seconds = timeframe_seconds(timeframe)
        raw = raw or self.raw_market(ref.market_native)
        now = _now_ms()
        span = int(((until or now) - since) / 1000) if since else seconds * (limit or DEFAULT_BARS)
        since_start = int((now - since) / 1000) if since else span
        chart = chart_for(max(since_start, span, 1))
        index = ref.outcome if ref.outcome is not None else 0
        outcome = next((o for o in raw.get("outcomes") or [] if int(o.get("id", -1)) == index), {})
        series = next((c for c in outcome.get("price_charts") or [] if c.get("timeframe") == chart), {})
        candles = candles_from_chart(series.get("prices") or [], interval_seconds=seconds)
        if since:
            candles = [c for c in candles if c.timestamp >= since - seconds * 1000]
        if until:
            candles = [c for c in candles if c.timestamp <= until]
        return pick_bars(candles, since=since, limit=limit)

    # -- reference ----------------------------------------------------------

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        ref, raw = self._resolve(market_id)
        return fee_schedule_of(raw or self.raw_market(ref.market_native), market_id=ref.native)

    def fetch_topics(self) -> list[dict[str, Any]]:
        """The venue's topics with how many open markets each holds."""
        payload = self._get("/topics") or {}
        return list(payload.get("data") or []) if isinstance(payload, dict) else []

    def fetch_tags(self, *, type: str | None = None) -> list[dict[str, Any]]:
        """The venue's tags on open markets (`type="league"`, "team", "sport",
        "asset"), each with its market count. A tag's slug filters `fetch_markets(tag=...)`."""
        payload = self._get("/tags", {"type": type}) or {}
        return list(payload.get("data") or []) if isinstance(payload, dict) else []

    def close(self) -> None:
        self.http.close()


def _joined(value: Any, *, title: bool = False) -> str | None:
    """A filter value or list of them, comma-joined. Topics are the venue's
    capitalised words ("Sports"), so a lowercase one is capitalised."""
    if value is None:
        return None
    items = [value] if isinstance(value, str) else list(value)
    items = [str(v).strip() for v in items if str(v).strip()]
    if title:
        items = [v[:1].upper() + v[1:] for v in items]
    return ",".join(items) or None


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
