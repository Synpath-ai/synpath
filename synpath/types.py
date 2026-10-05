"""The unified shapes every venue is normalized into.

Three rules run through all of them, and they are the reason this library
exists rather than being a thin wrapper:

1. **Absence is `None`, never a number.** Kalshi prints 0 for a bid nobody is
   offering, 1 for an absent ask, and 0 for a market that has never traded.
   Those are placeholders, not prices, and they are stored as `None` here. A
   library that passes them through tells you a market is worth nothing when
   it means nobody has quoted it.

2. **No single `price` field.** `last`, `bid`, `ask` and `mid` are four
   different numbers that disagree, sometimes wildly, and collapsing them into
   one hides which you got. A thin market can show a months-old last trade at
   5c sitting on a live 0.2c/1.3c book; next to another venue's 0.05c that
   reads as a 100x disagreement between two books that both say "near zero".

3. **Every timestamp is milliseconds since epoch (`timestamp`) plus an ISO
   8601 string (`datetime`)**, the ccxt convention, and every object carries
   `info` with the venue's untouched payload.
"""
from __future__ import annotations

import math
from datetime import datetime as _dt, timezone
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator

MarketStatus = Literal["unopened", "open", "closed", "settled"]
"""Normalized lifecycle. `native_status` always carries the venue's own word.

  unopened — listed but not yet accepting orders
  open     — trading
  closed   — trading has stopped, outcome not yet final
  settled  — outcome final and paid
"""

BookSide = Literal["yes", "no"]
"""Which side of a market a book or candle series is priced for."""

BookModel = Literal["shared_complement", "native_per_outcome"]
"""How a venue stores the book behind a market.

  shared_complement  — one book serves both sides (Kalshi, Polymarket US). The
                       NO view is a transform of the YES book, not a second book.
  native_per_outcome — each side owns an independently addressable book
                       (Polymarket: one per CLOB token).
"""

PriceSource = Literal["trade", "sampled_mid", "sampled_last", "bid_ask_mid"]
"""Where a candle's OHLC actually came from. See `Candle`."""

T = TypeVar("T")


def ms(value: _dt | None) -> int | None:
    """A datetime as milliseconds since epoch, UTC. Naive input is read as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def iso(timestamp_ms: int | None) -> str | None:
    """Milliseconds since epoch as an ISO 8601 string in UTC."""
    if timestamp_ms is None:
        return None
    return _dt.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class Page(list, Generic[T]):
    """A list of results that also knows how to ask for the next page.

    A plain `list`, so every caller that only wants the rows can ignore it
    entirely. The cursor rides along as an attribute rather than being stashed
    on the exchange object, because a cursor belongs to one response, not to
    the client: two concurrent calls sharing an adapter would otherwise
    overwrite each other's position in the catalog.

    ```python
    page = kalshi.fetch_markets(limit=50)
    while page.next_cursor:
        page = kalshi.fetch_markets(limit=50, cursor=page.next_cursor)
    ```
    """

    __slots__ = ("next_cursor",)

    def __init__(self, items: Any = (), *, next_cursor: str | None = None):
        super().__init__(items)
        self.next_cursor = next_cursor

    def __repr__(self) -> str:
        return f"Page({list.__repr__(self)}, next_cursor={self.next_cursor!r})"


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _drop_computed(cls, data: Any) -> Any:
        """Let a serialized model be read back in.

        Computed fields are written on the way out but are not inputs, and
        `extra="forbid"` would reject them on the way in — so a client that
        read a response and sent it back, or any round trip through JSON,
        would fail on a field this library itself added. Only the known
        computed names are dropped; a genuine typo is still refused.
        """
        computed = cls.model_computed_fields
        if computed and isinstance(data, dict) and not computed.keys().isdisjoint(data):
            return {key: value for key, value in data.items() if key not in computed}
        return data


class Quote(_Base):
    """What one instrument is worth right now, with the provenance attached.

    Four numbers, each independently nullable, because each is absent for its
    own reason: no bids, no asks, no trades ever, or a stale book. `mid` is
    `None` whenever either side is empty — it is never quietly replaced by the
    one side that is quoted.
    """

    bid: float | None = None
    """Best bid, 0-1. `None` when nobody is bidding."""
    bid_size: float | None = None
    ask: float | None = None
    """Best ask, 0-1. `None` when nobody is offering."""
    ask_size: float | None = None
    mid: float | None = None
    """`(bid + ask) / 2`, only when both sides are quoted."""
    last: float | None = None
    """Last traded price. `None` if this instrument has never traded."""
    last_timestamp: int | None = None
    """When `last` traded, in ms. Always read it alongside `last`: on a thin
    market the last print can be months old."""
    last_datetime: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def spread(self) -> float | None:
        """`ask - bid`, or `None` if either side is empty.

        A computed field rather than a plain property, so it reaches HTTP
        clients too. An accessor that exists only in Python is an accessor
        every other language has to reimplement.
        """
        if self.bid is None or self.ask is None:
            return None
        return round(self.ask - self.bid, 6)


class Outcome(_Base):
    """One side of a market: YES or NO, with its own quote.

    An outcome has no id of its own. A market is the unit that is identified
    and traded, and the side is said on the order (`buy` takes YES, `sell`
    takes NO) or asked for on a book (`side="no"`). Every binary market on
    every venue has exactly two; the venues differ in whether the two share a
    book, which `Market.book_model` records.
    """

    label: str
    """The venue's display text ("Yes", "No", "Up", a candidate name). Never
    used to decide financial logic: which of `market.yes` / `market.no` an
    outcome sits in is."""
    quote: Quote = Field(default_factory=Quote)
    """In this outcome's own price convention: the NO quote is what NO costs."""
    venue_token_id: str | None = None
    """The venue's own id for this side, where it has one. Polymarket only:
    the CLOB token id the venue's order books and orders are keyed on."""
    price_change_24h: float | None = None
    """Absolute probability delta over 24h, when the venue publishes it."""
    info: dict[str, Any] = Field(default_factory=dict)


class MarketStats(_Base):
    """Venue-reported headline numbers, with their units spelled out.

    `volume_unit` is not decoration. Kalshi counts contracts, Polymarket counts
    collateral. Comparing the two numbers without converting is a category
    error, and every unified API that labels both "USD" invites it.
    """

    volume_24h: float | None = None
    volume_total: float | None = None
    liquidity: float | None = None
    open_interest: float | None = None
    volume_unit: Literal["contracts", "collateral"] | None = None
    liquidity_unit: Literal["contracts", "collateral"] | None = None
    as_of: int | None = None
    """When the venue says these numbers were current, in ms."""


class Market(_Base):
    """A single settleable contract — the thing that actually resolves."""

    id: str
    """Synpath's id: `venue:native`, e.g. `kalshi:KXFEDDECISION-26SEP-C25` or
    `polymarket:2252244`. One per listing on one venue. The part after the
    colon is the venue's own id, also on `venue_market_id`."""
    venue: str
    venue_market_id: str
    """The venue's own id, unprefixed: a Kalshi ticker, a Polymarket Gamma id,
    a Polymarket US slug."""
    event_id: str | None = None
    """The parent event's Synpath id, `venue:native`."""
    title: str
    description: str | None = None
    """Resolution criteria, verbatim from the venue."""
    slug: str | None = None
    yes: Outcome
    """The YES side, by position in the venue's payload rather than by label
    text: some Kalshi markets label both sides identically."""
    no: Outcome
    status: MarketStatus
    native_status: str | None = None
    """The venue's own status word, untranslated."""
    active: bool = False
    """Accepting orders right now. Derived, and deliberately separate from
    `status`: a venue can halt trading without changing lifecycle state."""
    market_type: Literal["binary", "categorical", "scalar", "unknown"] = "binary"
    open_timestamp: int | None = None
    close_timestamp: int | None = None
    resolution_timestamp: int | None = None
    """The venue's *scheduled* resolution time, not when it actually resolved."""
    open_datetime: str | None = None
    close_datetime: str | None = None
    resolution_datetime: str | None = None
    tick_size: float | None = None
    """Minimum price increment. Needed to place an order that will be accepted."""
    face_value: float = 1.0
    """What one contract pays at full settlement. Both venues pay 1.00 today.
    Every complement transform reads this rather than hardcoding 1."""
    book_model: BookModel = "native_per_outcome"
    stats: MarketStats = Field(default_factory=MarketStats)
    url: str | None = None
    image_url: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    series_id: str | None = None
    """The venue's recurring-series tier above the event. Kalshi keys its fee
    schedule on this. `None` on venues without the concept."""
    outcome_label: str | None = None
    """This market's short name inside its event.

    An event like "Fed Decision in September?" holds one market per outcome,
    and `title` is the whole question ("Will the Fed decrease interest rates by
    50+ bps after the September meeting?") while this is the label the venue
    lists it under ("50+ bps decrease"). Kalshi publishes it as the YES side's
    subtitle, Polymarket as the market's group item title. Useful for display,
    and for deciding whether two venues are offering the same option."""
    neg_risk: bool | None = None
    """Whether this market belongs to a group where a NO position converts into
    YES exposure on the others. `None` means unknown, never assumed False."""
    settlement_sources: list[dict[str, Any]] = Field(default_factory=list)
    """Who decides the outcome, as `{"name", "url"}` entries.

    Carried because `description` says what has to happen and this says who
    rules on whether it did. Two venues can list the same question and settle
    it off different sources, which is the difference between the same trade
    and two different ones. Empty when the venue names no source."""
    info: dict[str, Any] = Field(default_factory=dict)


class Event(_Base):
    """A real-world question domain grouping one or more markets."""

    id: str
    """Synpath's id: `venue:native`, e.g. `kalshi:KXFEDDECISION-26SEP`."""
    venue: str
    venue_event_id: str
    """The venue's own id, unprefixed."""
    title: str
    description: str | None = None
    slug: str | None = None
    markets: list[Market] = Field(default_factory=list)
    status: MarketStatus
    native_status: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    series_id: str | None = None
    mutually_exclusive: bool | None = None
    """`None` is a real answer: the venue did not say. Never defaulted to False."""
    close_timestamp: int | None = None
    close_datetime: str | None = None
    url: str | None = None
    image_url: str | None = None
    settlement_sources: list[dict[str, Any]] = Field(default_factory=list)
    """Who decides the outcomes under this event. On Kalshi these are published
    per event rather than per market, so a market inherits its event's."""
    info: dict[str, Any] = Field(default_factory=dict)


class OrderLevel(_Base):
    price: float
    """0-1, in the price convention of the side the book was asked for."""
    size: float


class OrderBook(_Base):
    """Resting orders on one side of a market, as that side sees them.

    On a `shared_complement` venue the NO side's book is computed from the YES
    book (`bid = face_value - ask`, and the sides swap). `derived` records that
    it happened, and `info` keeps the raw payload so the transform is auditable
    rather than invisible.
    """

    market_id: str
    """Synpath id, `venue:native`."""
    side: BookSide = "yes"
    """Which side this book is priced for. Ask for `side="no"` to see what NO
    costs; on a `shared_complement` venue that is the YES book mirrored."""
    venue: str
    bids: list[OrderLevel] = Field(default_factory=list)
    """Descending by price."""
    asks: list[OrderLevel] = Field(default_factory=list)
    """Ascending by price."""
    timestamp: int | None = None
    datetime: str | None = None
    book_model: BookModel = "native_per_outcome"
    derived: bool = False
    """True when these levels were mirrored from the complement's book."""
    depth_scope: Literal["full", "top_n", "unknown"] = "unknown"
    info: dict[str, Any] = Field(default_factory=dict)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def best_bid(self) -> OrderLevel | None:
        """Highest bid, or `None` on an empty side. Serialized, so an HTTP
        client does not have to know which end of the array is best."""
        return self.bids[0] if self.bids else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def best_ask(self) -> OrderLevel | None:
        """Lowest ask, or `None` on an empty side."""
        return self.asks[0] if self.asks else None


class Trade(_Base):
    """One execution, from the taker's point of view."""

    id: str
    market_id: str
    """Synpath id, `venue:native`."""
    timestamp: int
    datetime: str
    price: float
    """Always the YES price, 0-1. A trade where the taker bought NO at 0.30 is
    reported as `price=0.70, side="sell"`."""
    amount: float
    side: Literal["buy", "sell", "unknown"] = "unknown"
    """What the taker did on the YES leg: `buy` took YES, `sell` took NO.
    `unknown` when the venue does not say."""
    info: dict[str, Any] = Field(default_factory=dict)


# Historical types extend the live market objects without changing their
# contract. Queries use recorder receive time; Trade.timestamp remains the
# venue's execution time when available.
class HistoryMetadata(_Base):
    dataset_version: str
    time_basis: Literal["recorder_receive"] = "recorder_receive"
    processed_through_ms: int | None = None


class HistoryCoverage(_Base):
    start_ms: int
    end_ms: int
    status: Literal["available", "unavailable"]
    reason: str | None = None


class HistoricalTrade(Trade):
    """A Trade plus its recorder receive time and timestamp provenance."""

    observed_at_ms: int
    timestamp_source: Literal["venue", "recorder_fallback"]


class HistoricalOrderBook(OrderBook):
    """A full book valued at as_of_ms, possibly unchanged since timestamp.

    Inherited timestamp is the recorder time of the last book update;
    venue_timestamp_ms is the original exchange time when supplied.
    """

    as_of_ms: int
    venue_timestamp_ms: int | None = None


class HistoricalBookChange(_Base):
    """One change in the requested book view; price is exact and view-relative."""
    kind: Literal["snapshot", "delta"]
    observed_at_ms: int
    venue_timestamp_ms: int | None = None
    book_side: Literal["bid", "ask"] | None = None
    price_exact: str | None = None
    quantity_delta_exact: str | None = None
    book: HistoricalOrderBook | None = None


class HistoricalBookSegment(_Base):
    kind: Literal["data", "absent"]
    start_ms: int
    end_ms: int
    initial_book: HistoricalOrderBook | None = None
    changes: list[HistoricalBookChange] = Field(default_factory=list)
    reason: str | None = None


class OrderBookAtResponse(_Base):
    metadata: HistoryMetadata
    market_id: str
    as_of_ms: int
    book: HistoricalOrderBook | None = None
    absence_reason: str | None = None


class OrderBookRangeResponse(_Base):
    metadata: HistoryMetadata
    market_id: str
    start_ms: int
    end_ms: int
    segments: list[HistoricalBookSegment]
    next_cursor: str | None = None


class TradesRangeResponse(_Base):
    metadata: HistoryMetadata
    market_id: str
    start_ms: int
    end_ms: int
    trades: list[HistoricalTrade]
    coverage: list[HistoryCoverage]
    next_cursor: str | None = None


class Candle(_Base):
    """One OHLCV bar, labelled with where the prices came from.

    The label is the point. Kalshi's candlestick endpoint returns a traded-price
    block *and* separate bid/ask blocks, and in a period with no trades the
    traded block is empty — so an OHLC built from it is `None`, while the book
    still has a spread worth reporting. Polymarket publishes no candles at all,
    only sampled price points with no volume. Reporting all three as one
    "OHLCV" would be fiction; `price_source` and a null `volume` say which you
    are holding.
    """

    timestamp: int
    """Start of the bar, in ms."""
    datetime: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    """`None` when the venue publishes no volume for the bar, which is not the
    same as zero volume."""
    trade_count: int | None = None
    price_source: PriceSource = "trade"
    """
      trade       — built from executions in the period
      bid_ask_mid — no trades; midpoint of the venue's bid/ask bars
      sampled_mid — the venue published price samples, not bars, and these were
                    bucketed by this library (Polymarket)
      sampled_last — the same, but the samples are the last traded price, not a
                    midpoint: a period without trades repeats the last one (Opinion)
    """
    bid_close: float | None = None
    ask_close: float | None = None
    """Book state at the close of the bar, where the venue reports it."""
    info: dict[str, Any] = Field(default_factory=dict)


KALSHI_MAKER_SHARE: dict[str, float] = {
    "quadratic": 0.0,
    "quadratic_with_maker_fees": 0.25,
    "quadratic_with_combo_maker_fees": 0.5,
}
"""Kalshi's quadratic fee types: the maker fee as a share of the taker
coefficient (0.07). A plain `quadratic` series charges makers nothing."""


class FeeSchedule(_Base):
    """What trading a market costs, before you trade it.

    Most unified APIs only tell you the fee after a fill. Kalshi publishes it
    per series, and a cross-venue price comparison that ignores it is wrong by
    more than most of the edges people are looking for.
    """

    venue: str
    scope: Literal["venue", "series", "market"]
    scope_id: str
    fee_type: str
    """Kalshi: `quadratic` — fee per contract is `multiplier * p * (1 - p)`,
    which peaks at 50c and vanishes at the extremes."""
    multiplier: float | None = None
    maker_rate: float | None = None
    taker_rate: float | None = None
    rounding: str | None = None
    exponent: float | None = None
    """Polymarket: the power applied to `P * (1 - P)`. Every live market
    publishes `1`; `None` means 1."""
    min_fee: float | None = None
    """Smallest fee one taker order is charged, in collateral, where the venue
    has a floor (Opinion: 0.25 USDT). `None` means no floor."""
    info: dict[str, Any] = Field(default_factory=dict)

    def estimate(self, price: float, contracts: float, *, taker: bool = True) -> float | None:
        """Estimated fee for `contracts` at `price`.

        Returns `None` when this library does not know the formula behind
        `fee_type`, rather than guessing one. A fee estimate that is quietly
        wrong is worse than no estimate: it turns into a position.

        Known forms:

        * Kalshi's quadratic family — a taker pays `0.07 * multiplier * C * P *
          (1 - P)`, largest at 50c and vanishing at the extremes. A maker pays
          nothing on `quadratic`, a quarter of the taker coefficient on
          `quadratic_with_maker_fees` and half on
          `quadratic_with_combo_maker_fees`, as the venue's series
          documentation defines them. With `rounding="up_to_cent"` (every
          Kalshi schedule) the fee for the order is rounded up to the cent, as
          the venue charges it. Kalshi's `flat` type follows a separate table
          this library does not have, so it returns `None`.
        * `quadratic_theta` (Polymarket US, Polymarket) — `theta * C *
          (P * (1 - P)) ** exponent`, the same shape with the coefficient
          published directly: `taker_rate` and `maker_rate` hold the venue's
          thetas, and a negative maker theta is a rebate, returned here as a
          negative fee. `exponent` is 1 unless the venue says otherwise.
        * `min_price` (predict.fun) — `rate * min(P, 1 - P) * C` for a
          taker, on the cheaper side of the market; a maker pays `maker_rate`
          (zero on every market read so far).
        * `hyperliquid_outcome` (Hyperliquid) — `rate * P * C`, the notional
          of the fill, charged only when the fill closes a position (and at
          settlement); a fill that opens one pays nothing. The estimate is the
          closing charge, so for an opening order it is an upper bound.
        * `opinion_curve` (Opinion) — `rate * notional * P * (1 - P)`, notional
          being `P * C`, as the venue's fee docs define it, and never less than
          `min_fee` for a taker order. `taker_rate` and `maker_rate` hold the
          venue's curve coefficients; a zero rate is a free market, no floor.
        """
        rate = self.taker_rate if taker else self.maker_rate
        if self.fee_type in KALSHI_MAKER_SHARE and self.multiplier is not None:
            share = 1.0 if taker else KALSHI_MAKER_SHARE[self.fee_type]
            fee = round(0.07 * share * self.multiplier * contracts * price * (1 - price), 9)
            if self.rounding == "up_to_cent":
                fee = math.ceil(fee * 100 - 1e-9) / 100
            return round(fee, 6)
        if self.fee_type == "opinion_curve" and rate is not None:
            fee = rate * price * contracts * price * (1 - price)
            if taker and rate > 0 and contracts > 0 and self.min_fee is not None:
                fee = max(fee, self.min_fee)
            return round(fee, 6)
        if self.fee_type == "min_price" and rate is not None:
            return round(rate * min(price, 1 - price) * contracts, 6)
        if self.fee_type == "hyperliquid_outcome" and rate is not None:
            return round(rate * price * contracts, 6)
        if self.fee_type == "quadratic_theta" and rate is not None:
            power = 1.0 if self.exponent is None else self.exponent
            return round(rate * contracts * (price * (1 - price)) ** power, 6)
        return None


class MarketLink(_Base):
    """The other venue's market for the one a match query was anchored on.

    No confidence and no settlement verdict: matching here is a deterministic
    parse (both listings resolved to the same canonical proposition), not a
    similarity score, and whether the two actually pay out the same way is a
    disclosure a caller reads dimension by dimension, never a single "same"
    or "not_same" this library hands down. See `match_market`.
    """

    id: str
    """A Synpath id (`polymarket:2252244`) -- the other venue's listing."""
    venue: str
    side_map: dict[BookSide, BookSide]
    """Which side of *this* link is which side of the anchor: `{"yes": "yes",
    "no": "no"}` when the two agree, `{"yes": "no", "no": "yes"}` when the
    venues put the proposition on opposite sides (Kalshi's "Mashtakov wins?"
    YES is Polymarket's "Pieczonka / Mashtakov" NO)."""


class MarketMatch(_Base):
    """The answer to "what is this market on the other venue?" -- see `match_market`."""

    anchor: str
    """The Synpath id the query was anchored on."""
    event_id: str | None = None
    """The canonical event both sides of a match belong to, when the anchor parsed onto one."""
    matched: MarketLink | None = None
    """`None` means the anchor is a real listing with nothing on the other
    venue asking the same question -- not "unknown", not "no such market"."""


class EventMatch(_Base):
    """The answer to "what is this event on the other venues?" -- see `match_event`.

    A list per venue, not one id: a native event on one venue is often split
    into several on another (Kalshi's "Where will it rain on Sep 23?" is one
    native event and one card per city on the other side).
    """

    anchor: str
    event_ids: list[str] = Field(default_factory=list)
    """The canonical event(s) the anchor belongs to. Usually one."""
    events: dict[str, list[str] | None] = Field(default_factory=dict)
    """Venue name -> its native event ids on the same canonical event(s), or
    `None` when that venue lists nothing there."""


class Series(_Base):
    """A recurring grouping of events above the event tier (Kalshi only today)."""

    id: str
    venue: str
    title: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    fee: FeeSchedule | None = None
    settlement_sources: list[dict[str, Any]] = Field(default_factory=list)
    info: dict[str, Any] = Field(default_factory=dict)
