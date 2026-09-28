"""The shapes an order takes from request to settlement.

Field names follow ccxt where ccxt has them and FIX where ccxt does not.
Where the two disagree, FIX wins for state and ccxt for naming: an order has
one `status` and separate `filled` / `remaining` quantities, so "partially
filled" is a question about those numbers, not a state of its own, and a
cancel or replace in flight has a name (`pending_cancel`, `pending_replace`)
because a fill arriving while one is in flight is the common race.

Every quantity of money is `Decimal`. Every timestamp is milliseconds since
epoch. Every object keeps the venue's raw payload in `info`.

Three things the venues disagree on are carried explicitly rather than
smoothed over:

  * **Exposure versus inventory.** Kalshi and Polymarket US net a position:
    selling YES with none is a long NO. Polymarket does not: YES and NO are
    two token inventories until they are merged. `Position` carries both
    views, and a venue fills whichever it has.
  * **Fill finality.** A Polymarket fill is matched off-chain first and
    confirmed on-chain later, and can fail in between. `Fill.settlement`
    says which; the other venues report fills already final.
  * **Buying power.** Polymarket US posts margin and computes buying power;
    the others lock the full cost. `Balance.buying_power` is the figure the
    venue defines, or `None` where it defines none.
"""
from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..types import iso


class Side(str, Enum):
    """What an order does on a market, on its YES leg.

    `buy` takes the YES side, `sell` takes the NO side, and the price is
    always the YES price: selling at 0.70 is the same order as buying NO at
    0.30. On a venue that nets a position (Kalshi, Polymarket US) that is
    literally the order sent. On Polymarket, where YES and NO are separate
    tokens, `sell` buys the NO token at `1 - price`; `sell` with
    `reduce_only` sells the YES tokens held instead. `buy` with
    `reduce_only` likewise sells NO tokens held rather than buying YES.
    """

    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """What the venue holds (`limit`, `market`) and what the engine holds.

    A venue adapter accepts only the first two; every other type is a parent
    the execution engine manages, which submits limit or market children
    when its condition is met. The engine, not the adapter, reports which of
    these it offers.
    """

    LIMIT = "limit"
    MARKET = "market"
    STOP_MARKET = "stop_market"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    ICEBERG = "iceberg"
    OCO = "oco"
    BRACKET = "bracket"
    TWAP = "twap"
    PEG = "peg"
    SMART_TAKER = "smart_taker"
    RFQ_TAKER = "rfq_taker"


VENUE_ORDER_TYPES = frozenset({OrderType.LIMIT, OrderType.MARKET})


class TimeInForce(str, Enum):
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    GTD = "gtd"
    DAY = "day"
    """Not a venue concept on any of the three; the engine rewrites it to
    `gtd` at the configured session end before it reaches an adapter."""


class OrderStatus(str, Enum):
    PENDING = "pending"
    """Accepted by this library, not yet acknowledged by the venue."""
    OPEN = "open"
    """Resting at the venue. Read `filled` and `remaining` for how much."""
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    CLOSED = "closed"
    """Fully filled."""
    CANCELED = "canceled"
    """Ended by a cancel; `filled` may be non-zero."""
    REJECTED = "rejected"
    EXPIRED = "expired"
    WAITING = "waiting"
    """An engine-held parent whose condition has not been met."""
    TRIGGERED = "triggered"
    """An engine-held parent whose condition fired; its children are live."""


TERMINAL_STATUSES = frozenset({
    OrderStatus.CLOSED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED,
})


class HeldBy(str, Enum):
    VENUE = "venue"
    ENGINE = "engine"


class SettlementState(str, Enum):
    MATCHED = "matched"
    """Matched at the venue, not yet final. Polymarket only, before the chain
    confirms."""
    CONFIRMED = "confirmed"
    """Final."""
    FAILED = "failed"
    """Matched and then failed on-chain; the shares never arrived."""


class Liquidity(str, Enum):
    MAKER = "maker"
    TAKER = "taker"
    UNKNOWN = "unknown"


class PositionSide(str, Enum):
    """`long` holds YES, `short` holds NO, on the market's YES leg."""

    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, use_enum_values=False)


class Account(_Base):
    """One set of credentials at one venue, optionally one subaccount.

    Balances are held here and nowhere else: cash sits at the venue, so a
    consolidated balance is a roll-up computed on read, never something an
    order can spend from.
    """

    venue: str
    name: str = "default"
    """Which credential set, for a caller with more than one at a venue."""
    subaccount: str | None = None

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.name}" + (f":{self.subaccount}" if self.subaccount else "")


class Precision(_Base):
    """What an order for this instrument must satisfy before it is signed."""

    tick: Decimal
    """Minimum price increment. Polymarket varies it per market."""
    min_amount: Decimal = Decimal("1")
    amount_step: Decimal | None = None
    """Contract increment where the venue has one; `None` means any decimal."""
    whole_contracts: bool = False
    """Polymarket US: integers only."""
    face_value: Decimal = Decimal("1")


class OrderRequest(_Base):
    """What a caller asks for. Validated against the market before signing."""

    market_id: str
    """Synpath id, `venue:native`. A bare native id is accepted by the venue's
    own adapter."""
    side: Side
    """`buy` takes YES, `sell` takes NO. See `Side`."""
    amount: Decimal
    """Contracts."""
    type: OrderType = OrderType.LIMIT
    price: Decimal | None = None
    """Always the YES price, 0 to 1. Required for a limit order. For a market
    order, the protection price the engine will not cross; refused without
    one on a venue without native market orders."""
    stop_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    expires_at: int | None = None
    """Milliseconds since epoch. Required with `gtd`."""
    post_only: bool = False
    reduce_only: bool = False
    """Only reduce an existing position. On Polymarket this is what decides
    whether `sell` buys NO tokens or sells YES tokens held; see `Side`."""
    client_order_id: str | None = None
    """The idempotency key. Generated before the first write if absent, and
    reused on every retry of the same intent."""
    account: Account | None = None
    book: str | None = None
    """Strategy or desk the order belongs to. Internal, independent of the
    venue account; positions and P&L roll up by it."""
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    notes: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    """Venue-specific extras passed through untouched (Kalshi
    `self_trade_prevention_type`, Polymarket `signatureType`, ...)."""

    @model_validator(mode="after")
    def _consistent(self) -> "OrderRequest":
        if self.type == OrderType.LIMIT and self.price is None:
            raise ValueError("a limit order needs a price")
        if self.time_in_force == TimeInForce.GTD and self.expires_at is None:
            raise ValueError("gtd needs expires_at")
        if self.type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP) \
                and self.stop_price is None:
            raise ValueError(f"{self.type.value} needs a stop_price")
        if self.type == OrderType.STOP_LIMIT and self.price is None:
            raise ValueError("stop_limit needs the limit price to submit once triggered")
        return self


class EditRequest(_Base):
    """A change to a resting order. Fields left `None` are left alone."""

    order_id: str
    price: Decimal | None = None
    amount: Decimal | None = None
    time_in_force: TimeInForce | None = None
    expires_at: int | None = None
    client_order_id: str | None = None
    """Idempotency key for the *edit*, so a retried edit is not applied twice."""


class Order(_Base):
    """An order as the venue (or the engine) reports it."""

    id: str
    client_order_id: str | None = None
    venue: str
    account: Account | None = None
    market_id: str
    """Synpath id, `venue:native`."""
    side: Side
    """On the YES leg: `buy` is a YES order, `sell` a NO order, whatever the
    venue's own wording. `price` is the YES price."""
    type: OrderType
    time_in_force: TimeInForce
    status: OrderStatus
    held_by: HeldBy = HeldBy.VENUE
    price: Decimal | None = None
    stop_price: Decimal | None = None
    amount: Decimal
    filled: Decimal = Decimal("0")
    remaining: Decimal | None = None
    """`amount - filled` unless the venue reports otherwise."""
    average_price: Decimal | None = None
    cost: Decimal | None = None
    """Collateral committed so far."""
    fee: Decimal | None = None
    fee_currency: str | None = None
    last_fill_price: Decimal | None = None
    last_fill_amount: Decimal | None = None
    post_only: bool = False
    reduce_only: bool = False
    expires_at: int | None = None
    created_at: int | None = None
    updated_at: int | None = None
    parent_id: str | None = None
    """The engine-held parent this order is a child of."""
    queue_priority_preserved: bool | None = None
    """After an edit: whether the venue kept the order's place in the queue.
    Kalshi keeps it on a decrease and loses it on a price change; Polymarket
    always loses it because an edit is a cancel and a new order."""
    book: str | None = None
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    info: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _derive(self) -> "Order":
        if self.remaining is None:
            object.__setattr__(self, "remaining", self.amount - self.filled)
        return self

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def created_datetime(self) -> str | None:
        return iso(self.created_at)


class Fill(_Base):
    id: str
    order_id: str
    client_order_id: str | None = None
    venue: str
    account: Account | None = None
    market_id: str
    side: Side
    """On the YES leg, like `Order.side`."""
    price: Decimal
    """The YES price."""
    amount: Decimal
    fee: Decimal | None = None
    fee_currency: str | None = None
    liquidity: Liquidity = Liquidity.UNKNOWN
    settlement: SettlementState = SettlementState.CONFIRMED
    """`matched` until the chain confirms, on Polymarket. Final elsewhere."""
    timestamp: int
    info: dict[str, Any] = Field(default_factory=dict)

    @property
    def datetime(self) -> str | None:
        return iso(self.timestamp)


class Position(_Base):
    """One market's position: net on the YES leg, with the token inventories
    kept beside it on a venue that does not net."""

    venue: str
    account: Account | None = None
    market_id: str
    side: PositionSide = PositionSide.FLAT
    """`long` holds YES, `short` holds NO."""
    contracts: Decimal = Decimal("0")
    """Net exposure on the side named, positive. On a netting venue this is
    the position; on Polymarket it is `|inventory_yes - inventory_no|`."""
    inventory_yes: Decimal | None = None
    inventory_no: Decimal | None = None
    """Tokens actually held on each side, on a venue that does not net
    (Polymarket). `None` elsewhere."""
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    margin: Decimal | None = None
    """Collateral locked against this position, where the venue reports it."""
    resolved: bool = False
    final: bool = False
    """Resolved and past any dispute window. On Polymarket a resolved market
    can still be challenged; `resolved` without `final` says so."""
    won: bool | None = None
    payout: Decimal | None = None
    redeemable: Decimal | None = None
    """Payout waiting to be claimed on-chain. Polymarket only."""
    timestamp: int | None = None
    info: dict[str, Any] = Field(default_factory=dict)


class Settlement(_Base):
    venue: str
    account: Account | None = None
    market_id: str
    held: PositionSide | None = None
    """Which side was held into settlement, when the venue says."""
    result: str | None = None
    won: bool | None = None
    amount: Decimal | None = None
    cost: Decimal | None = None
    payout: Decimal | None = None
    pnl: Decimal | None = None
    timestamp: int | None = None
    info: dict[str, Any] = Field(default_factory=dict)


class Balance(_Base):
    """One currency in one venue account. Never pooled across accounts."""

    venue: str
    account: Account
    currency: str
    total: Decimal
    available: Decimal
    locked: Decimal | None = None
    """Committed to resting orders and margin, where the venue reports it.
    `None` means it does not, not zero."""
    buying_power: Decimal | None = None
    """The venue's own figure for what can still be bought, where it has one
    (Polymarket US). `None` means the venue defines none, not zero."""
    timestamp: int | None = None
    info: dict[str, Any] = Field(default_factory=dict)


class FeeEstimate(_Base):
    """What an order would cost, before it is placed."""

    venue: str
    market_id: str
    side: Side
    price: Decimal
    amount: Decimal
    taker_fee: Decimal | None = None
    maker_fee: Decimal | None = None
    """Negative where the venue pays makers."""
    builder_fee: Decimal | None = None
    currency: str = "USD"
    info: dict[str, Any] = Field(default_factory=dict)

    @field_validator("price", "amount")
    @classmethod
    def _positive(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("must be positive")
        return value
