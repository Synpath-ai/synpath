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
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from .._deprecated import QUANTITY, rename_input, renamed_fields
from ..types import iso


class Side(str, Enum):
    """What an order does on a market, on its YES leg.

    `buy` takes the YES side, `sell` takes the NO side, and the price is
    always the YES price: selling at 0.70 is the same order as buying NO at
    0.30. On a venue that nets a position (Kalshi, Polymarket US) that is
    literally the order sent. Where YES and NO are separate tokens
    (Polymarket and the like) the YES leg cannot say whether a `sell` sells
    YES tokens held or buys NO tokens: an order says which with its
    `outcome` and `action` (see `OrderRequest`).
    """

    BUY = "buy"
    SELL = "sell"


Outcome = Literal["yes", "no"]
"""The outcome an order trades: which token it buys or sells."""
Action = Literal["buy", "sell"]
"""What an order does with its outcome."""


def yes_side(outcome: str, action: str) -> Side:
    """An outcome and action on the YES leg: buying YES and selling NO are
    YES buys; selling YES and buying NO are YES sells."""
    return Side.BUY if (outcome == "yes") == (action == "buy") else Side.SELL


def default_leg(side: Side | str) -> tuple[str, str]:
    """The outcome and action a YES-leg side means when nothing else says:
    `buy` buys YES, `sell` buys NO (opening, as an order with no position
    in mind does)."""
    return ("yes", "buy") if Side(side) == Side.BUY else ("no", "buy")


def legacy_reduce_only(data: Any, *, refuse: bool) -> Any:
    """`reduce_only` is gone (16 October 2026): an order says what it does
    with `outcome` and `action`. Written before that, a reduce-only order
    sold the token its side names the other of: a reduce-only `sell` sold
    YES held, a reduce-only `buy` sold NO held. Stored and venue-reported
    data is read that way; a new request naming it is refused."""
    if not isinstance(data, dict) or "reduce_only" not in data:
        return data
    if refuse:
        raise ValueError("reduce_only is removed: say what the order does with outcome (yes/no) and action "
                         "(buy/sell), e.g. outcome=yes, action=sell sells YES held")
    data = dict(data)
    reduce_only = bool(data.pop("reduce_only"))
    side = data.get("side")
    if reduce_only and side is not None and data.get("outcome") is None and data.get("action") is None:
        data["outcome"], data["action"] = ("yes", "sell") if Side(side) == Side.SELL else ("no", "sell")
    return data


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
    """The requester's order from an accepted RFQ quote (docs/rfq-combo.md)."""
    RFQ_MAKER = "rfq_maker"
    """The quoter's order from a quote it confirmed."""


VENUE_ORDER_TYPES = frozenset({OrderType.LIMIT, OrderType.MARKET})


class TimeInForce(str, Enum):
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    GTD = "gtd"
    DAY = "day"
    """Not a venue concept on any of the three; the engine rewrites it to
    `gtd` at the configured session end before it reaches an adapter."""


class TriggerSource(str, Enum):
    """The price an engine-held stop watches.

    `touch` (the default) is the side that would fill the triggered order:
    the best ask for a buy stop, the best bid for a sell stop. `last` is the
    last public trade, `mid` the middle of the best bid and ask."""

    TOUCH = "touch"
    LAST = "last"
    MID = "mid"


STOP_TYPES = frozenset({OrderType.STOP_MARKET, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP})


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

    @model_validator(mode="before")
    @classmethod
    def _renamed(cls, data: Any) -> Any:
        # TODO(0.9): remove with synpath._deprecated.
        return rename_input(data, getattr(cls, "__synpath_renamed__", {}))


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


@renamed_fields({"min_amount": "min_quantity", "amount_step": "quantity_step"})  # TODO(0.9): remove
class Precision(_Base):
    """What an order for this instrument must satisfy before it is signed."""

    tick: Decimal
    """Minimum price increment. Polymarket varies it per market."""
    min_quantity: Decimal = Decimal("1")
    quantity_step: Decimal | None = None
    """Contract increment where the venue has one; `None` means any decimal."""
    whole_contracts: bool = False
    """Polymarket US: integers only."""
    face_value: Decimal = Decimal("1")
    min_notional: Decimal | None = None
    """Smallest order the venue takes, in collateral: what the token actually
    bought costs (Opinion: 5 USDT). `None` where the venue counts only
    contracts."""


@renamed_fields(QUANTITY)  # TODO(0.9): remove
class OrderRequest(_Base):
    """What a caller asks for. Validated against the market before signing."""

    user_id: str | None = None
    """Application ownership, assigned by the execution worker; not a venue account."""
    market_id: str
    """Synpath id, `venue:native`. A bare native id is accepted by the venue's
    own adapter."""
    side: Side
    """`buy` takes YES, `sell` takes NO, on the YES leg. See `Side`. Set
    from `outcome` and `action` when they are given and it is not."""
    outcome: Outcome | None = None
    action: Action | None = None
    """What the order does, in the caller's terms: buy or sell YES or NO.
    The engine, ledger and risk work on the YES leg (`side`, the YES
    `price`); a venue adapter reads these to send it. On a venue that nets
    (Kalshi, Polymarket US) Buy NO and Sell YES are the same order. Where YES
    and NO are separate tokens (Polymarket, Opinion, Limitless, predict.fun,
    Hyperliquid) they are not: `sell` sells the tokens held, `buy` buys them.
    Both or neither: with neither, `side` decides (`default_leg`: `buy` buys
    YES, `sell` buys NO)."""
    quantity: Decimal
    """Contracts."""
    type: OrderType = OrderType.LIMIT
    price: Decimal | None = None
    """Always the YES price, 0 to 1. Required for a limit order. For a market
    order, the protection price the engine will not cross; refused without
    one on a venue without native market orders."""
    stop_price: Decimal | None = None
    """The level a stop triggers at: a buy stop when the watched price rises
    to it, a sell stop when it falls to it."""
    trigger_source: TriggerSource = TriggerSource.TOUCH
    """What a stop watches; see `TriggerSource`. Stops only."""
    time_in_force: TimeInForce = TimeInForce.GTC
    """For a stop, how long the whole order lives, armed and after it
    triggers, as IBKR and FIX use it: `gtc`, `gtd` or `day`. A `stop_limit`'s
    child inherits it (a `day` one as `gtd` at the same session end); a
    `stop_market`'s child is `ioc` within that life. `ioc` and `fok` are
    refused on a stop."""
    expires_at: int | None = None
    """Milliseconds since epoch. Required with `gtd`."""
    post_only: bool = False
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

    @classmethod
    def stored(cls, data: Any) -> "OrderRequest":
        """An instruction read back from a journal. It was valid when it was
        accepted: rules added since (a stop's `ioc`/`fok` time in force) do
        not stop an older order from being restored, listed or retried, and
        an old `reduce_only` is read as the outcome and action it meant."""
        return cls.model_validate(data, context={"stored": True})

    @property
    def leg(self) -> tuple[str, str]:
        """`(outcome, action)`: what the order does, given or from `side`."""
        if self.outcome is not None and self.action is not None:
            return self.outcome, self.action
        return default_leg(self.side)

    @model_validator(mode="before")
    @classmethod
    def _outcome_and_action(cls, data: Any, info: ValidationInfo) -> Any:
        data = legacy_reduce_only(data, refuse=not (info.context or {}).get("stored"))
        if not isinstance(data, dict):
            return data
        outcome, action = data.get("outcome"), data.get("action")
        if (outcome is None) != (action is None):
            raise ValueError("give outcome and action together (buy or sell, yes or no), or neither")
        if outcome is not None:
            implied = yes_side(str(outcome), str(action))
            if data.get("side") is None:
                data = {**data, "side": implied}
            elif Side(data["side"]) != implied:
                raise ValueError(f"side {Side(data['side']).value} is not {action} {outcome} on the YES leg "
                                 f"({implied.value}); give one or make them agree")
        return data

    @model_validator(mode="after")
    def _consistent(self, info: ValidationInfo) -> "OrderRequest":
        if self.type == OrderType.LIMIT and self.price is None:
            raise ValueError("a limit order needs a price")
        if self.time_in_force == TimeInForce.GTD and self.expires_at is None:
            raise ValueError("gtd needs expires_at")
        if self.type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP) \
                and self.stop_price is None:
            raise ValueError(f"{self.type.value} needs a stop_price")
        if self.type == OrderType.STOP_LIMIT and self.price is None:
            raise ValueError("stop_limit needs the limit price to submit once triggered")
        if self.trigger_source != TriggerSource.TOUCH and self.type not in STOP_TYPES:
            raise ValueError(f"trigger_source is for stop orders, not {self.type.value}")
        stored = bool((info.context or {}).get("stored"))
        if self.type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT) and not stored \
                and self.time_in_force in (TimeInForce.IOC, TimeInForce.FOK):
            raise ValueError(f"a {self.type.value}'s time_in_force is how long the whole order lives, armed and "
                             f"after it triggers: gtc, gtd or day, not {self.time_in_force.value}")
        if self.type == OrderType.STOP_LIMIT:
            if self.side == Side.SELL and self.price > self.stop_price:  # type: ignore[operator]
                raise ValueError(f"a sell stop_limit's price {self.price} must be at or below its stop_price {self.stop_price}")
            if self.side == Side.BUY and self.price < self.stop_price:  # type: ignore[operator]
                raise ValueError(f"a buy stop_limit's price {self.price} must be at or above its stop_price {self.stop_price}")
        return self


@renamed_fields(QUANTITY)  # TODO(0.9): remove
class EditRequest(_Base):
    """A change to a resting order. Fields left `None` are left alone."""

    order_id: str
    price: Decimal | None = None
    quantity: Decimal | None = None
    time_in_force: TimeInForce | None = None
    expires_at: int | None = None
    client_order_id: str | None = None
    """Idempotency key for the *edit*, so a retried edit is not applied twice."""


@renamed_fields({**QUANTITY, "last_fill_amount": "last_fill_quantity"})  # TODO(0.9): remove
class Order(_Base):
    """An order as the venue (or the engine) reports it."""

    user_id: str | None = None
    id: str
    client_order_id: str | None = None
    venue: str | None
    account: Account | None = None
    execution_accounts: dict[str, Account] = Field(default_factory=dict)
    """Eligible execution accounts captured by a routed engine parent.
    Its venue and singular account are null; actual children remain venue orders."""

    @model_validator(mode="after")
    def validate_execution_identity(self):
        if self.execution_accounts:
            if self.held_by != HeldBy.ENGINE or self.venue is not None or self.account is not None:
                raise ValueError("cross-venue execution scope belongs to an engine parent without a singular venue/account")
            if any(venue != account.venue for venue, account in self.execution_accounts.items()):
                raise ValueError("execution account venue does not match its scope")
        elif self.venue is None:
            raise ValueError("an order without a venue needs its captured execution accounts")
        return self
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
    quantity: Decimal
    filled: Decimal = Decimal("0")
    remaining: Decimal | None = None
    """`quantity - filled` unless the venue reports otherwise."""
    average_price: Decimal | None = None
    cost: Decimal | None = None
    """Collateral committed so far."""
    fee: Decimal | None = None
    fee_currency: str | None = None
    last_fill_price: Decimal | None = None
    last_fill_quantity: Decimal | None = None
    post_only: bool = False
    outcome: Outcome | None = None
    action: Action | None = None
    """What the order does (see `OrderRequest.outcome`): as the caller gave it
    on an order the engine placed, as the venue reports it where YES and NO
    are separate tokens, else unknown (`None`). An order placed with a side
    alone has none: on a netting venue a sell may close YES or open NO."""
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

    @model_validator(mode="before")
    @classmethod
    def _legacy(cls, data: Any) -> Any:
        """An order stored before `reduce_only` was removed reads as the outcome and action it meant."""
        return legacy_reduce_only(data, refuse=False)

    @model_validator(mode="after")
    def _derive(self) -> "Order":
        if self.remaining is None:
            object.__setattr__(self, "remaining", self.quantity - self.filled)
        return self

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @property
    def created_datetime(self) -> str | None:
        return iso(self.created_at)


@renamed_fields(QUANTITY)  # TODO(0.9): remove
class Fill(_Base):
    id: str
    order_id: str
    client_order_id: str | None = None
    venue: str
    account: Account | None = None
    market_id: str
    side: Side
    """On the YES leg, like `Order.side`."""
    outcome: Outcome | None = None
    action: Action | None = None
    """What the fill did, its order's outcome and action (see
    `OrderRequest.outcome`), where the order is known; else unknown."""
    price: Decimal
    """The YES price."""
    quantity: Decimal
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


@renamed_fields(QUANTITY)  # TODO(0.9): remove
class Settlement(_Base):
    venue: str
    account: Account | None = None
    market_id: str
    held: PositionSide | None = None
    """Which side was held into settlement, when the venue says."""
    result: str | None = None
    won: bool | None = None
    quantity: Decimal | None = None
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


@renamed_fields(QUANTITY)  # TODO(0.9): remove
class FeeEstimate(_Base):
    """What an order would cost, before it is placed."""

    venue: str
    market_id: str
    side: Side
    price: Decimal
    quantity: Decimal
    taker_fee: Decimal | None = None
    maker_fee: Decimal | None = None
    """Negative where the venue pays makers."""
    builder_fee: Decimal | None = None
    min_fee: Decimal | None = None
    """The least one taker order pays, where the venue has a floor (Opinion:
    0.25 USDT), already applied to `taker_fee`. `None` means no floor."""
    currency: str = "USD"
    info: dict[str, Any] = Field(default_factory=dict)

    @field_validator("price", "quantity")
    @classmethod
    def _positive(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("must be positive")
        return value


# -- RFQ and combos -------------------------------------------------------------
#
# See docs/rfq-combo.md. A requester (the taker) asks for a quote on a market
# or a combo; makers (quoters) answer with a two-sided YES quote; the taker
# accepts one side; the maker confirms (a last look); the venue submits the
# two orders. Every price is a YES price, as everywhere else.


class RfqStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    """No longer quotable; `Rfq.closed_reason` says why."""


class RfqCloseReason(str, Enum):
    CANCELED = "canceled"
    """Deleted by its creator."""
    EXPIRED = "expired"
    EXECUTED = "executed"
    """A quote on it traded."""
    REPLACED = "replaced"
    """Closed by the venue because a newer RFQ took its place."""


class QuoteStatus(str, Enum):
    """`open` → `accepted` → `confirmed` → `executed`, or off the path to
    `canceled`, `expired` or `failed` before `executed`."""

    OPEN = "open"
    """Quotable: the requester may accept a side."""
    ACCEPTED = "accepted"
    """The requester took a side; the maker has not confirmed."""
    CONFIRMED = "confirmed"
    """The maker confirmed (its last look); the venue will submit the orders."""
    EXECUTED = "executed"
    """The orders were submitted. Not a fill: read the order's fills."""
    CANCELED = "canceled"
    """The maker withdrew it, or its RFQ closed."""
    EXPIRED = "expired"
    """Not accepted, or not confirmed, by its deadline."""
    FAILED = "failed"
    """The venue reported that the execution failed; nothing traded."""


QUOTE_TERMINAL_STATUSES = frozenset({
    QuoteStatus.EXECUTED, QuoteStatus.CANCELED, QuoteStatus.EXPIRED, QuoteStatus.FAILED,
})


def _yes_price(value: Decimal | None, name: str) -> Decimal | None:
    if value is not None and not (Decimal("0") < value < Decimal("1")):
        raise ValueError(f"{name} {value} is a YES price and must be inside (0, 1)")
    return value


def _positive(value: Decimal | None, name: str) -> Decimal | None:
    if value is not None and value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _one_size(quantity: Decimal | None, amount: Decimal | None, *, required: bool) -> None:
    if quantity is not None and amount is not None:
        raise ValueError("size by quantity (contracts) or amount (money), not both")
    if required and quantity is None and amount is None:
        raise ValueError("give a size: quantity (contracts) or amount (money)")
    _positive(quantity, "quantity")
    _positive(amount, "amount")


def _leg_pair(outcome: str | None, action: str | None) -> None:
    if action is not None and outcome is None:
        raise ValueError("an action needs the outcome it acts on (yes or no)")


class ComboLeg(_Base):
    """One market a combo needs, and which outcome of it."""

    market_id: str
    """Synpath id, `venue:native`."""
    outcome: Outcome
    """The outcome this leg needs. On Polymarket US `no` is `SIDE_SELL`."""
    event_id: str | None = None
    """The leg's event, where the venue wants it (Kalshi `event_ticker`)."""
    info: dict[str, Any] = Field(default_factory=dict)


class Combo(_Base):
    """A multi-leg instrument with its own market id: it pays out only if every
    leg does. Positions, P&L and settlement live on `market_id`, like any
    market's."""

    market_id: str
    """The combo's own Synpath id (`kalshi:KXMVE…`, `polymarket_us:caoc-…`)."""
    venue: str
    collection_id: str | None = None
    """Kalshi's multivariate event collection the combo was built in."""
    legs: list[ComboLeg]
    status: Literal["open", "closed", "settled"] | None = None
    created_at: int | None = None
    info: dict[str, Any] = Field(default_factory=dict)

    @field_validator("legs")
    @classmethod
    def _two_or_more(cls, legs: list[ComboLeg]) -> list[ComboLeg]:
        if len(legs) < 2:
            raise ValueError("a combo needs at least two legs")
        if len({leg.market_id for leg in legs}) != len(legs):
            raise ValueError("a combo names each market once")
        return legs


class ComboMarket(_Base):
    """One market the venue lets a combo take as a leg, from its own list of
    combo-eligible markets (Polymarket's catalogue, Polymarket US's
    `comboEnabled`). Kalshi lists eligibility by collection instead."""

    market_id: str
    """Synpath id, `venue:native`: the leg's `ComboLeg.market_id`."""
    venue: str
    title: str | None = None
    event_id: str | None = None
    """The market's event, where the venue's list names it (Polymarket US)."""
    event_title: str | None = None
    position_ids: dict[str, str] | None = None
    """Per outcome (`yes`, `no`), where a leg is named by a position id
    (Polymarket: `leg.info["position_id"]`)."""
    min_legs: int = 2
    max_legs: int | None = None
    """The most legs a combo may have on this venue, where documented."""
    status: str | None = None
    """The venue's own market status, as listed."""
    tags: list[str] = Field(default_factory=list)
    info: dict[str, Any] = Field(default_factory=dict)
    """The venue's own row."""


class Rfq(_Base):
    """A request for quote: one market or one combo, sized by contracts or by
    money, never both."""

    id: str
    venue: str
    account: Account | None = None
    market_id: str | None = None
    """The single market asked about. Exactly one of this and `combo_id`."""
    combo_id: str | None = None
    """The combo's own market id (`Combo.market_id`) when the RFQ is on a combo."""
    combo: Combo | None = None
    """The combo's legs, where known."""
    outcome: Outcome | None = None
    action: Action | None = None
    """The side asked for, where the venue asks one side (Polymarket global:
    YES, buy or sell). `None` on a two-sided request (Kalshi, Polymarket US)."""
    quantity: Decimal | None = None
    """Contracts. Exactly one of this and `amount`."""
    amount: Decimal | None = None
    """Money, in the venue's collateral (Kalshi `target_cost_dollars`,
    Polymarket US `cashOrderQty`). Not the old name for `quantity`."""
    rest_remainder: bool = False
    """Leave what a quote does not fill resting on the book."""
    status: RfqStatus = RfqStatus.OPEN
    closed_reason: RfqCloseReason | None = None
    mine: bool = True
    """Created by this account; `False` for an incoming RFQ a quoter sees."""
    created_at: int | None = None
    expires_at: int | None = None
    updated_at: int | None = None
    info: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _consistent(self) -> "Rfq":
        if (self.market_id is None) == (self.combo_id is None):
            raise ValueError("an RFQ is on a market_id or a combo_id, exactly one")
        if self.combo is not None and self.combo_id is not None and self.combo.market_id != self.combo_id:
            raise ValueError(f"combo {self.combo.market_id} is not combo_id {self.combo_id}")
        _leg_pair(self.outcome, self.action)
        # The venue may not echo a size back on an incoming RFQ; one we
        # created always has one.
        _one_size(self.quantity, self.amount, required=self.mine)
        if self.closed_reason is not None and self.status != RfqStatus.CLOSED:
            raise ValueError("closed_reason is for a closed RFQ")
        return self

    @property
    def instrument_id(self) -> str:
        """What is quoted: the market or the combo's own market id."""
        return self.market_id or self.combo_id  # type: ignore[return-value]


class RfqQuote(_Base):
    """A maker's answer to an RFQ, both sides on the YES leg.

    `ask` is the YES price the requester can buy at (Kalshi `1 - no_bid`,
    Polymarket US `buyPrice`); `bid` the YES price it can sell at (Kalshi
    `yes_bid`, Polymarket US `sellPrice`). A side the maker declined is
    `None`. Named `RfqQuote` because `synpath.Quote` is the market-data quote.
    """

    id: str
    rfq_id: str
    venue: str
    account: Account | None = None
    market_id: str | None = None
    """The instrument quoted (a market or a combo's own id), where the venue says."""
    bid: Decimal | None = None
    ask: Decimal | None = None
    bid_quantity: Decimal | None = None
    ask_quantity: Decimal | None = None
    status: QuoteStatus = QuoteStatus.OPEN
    accepted_side: Side | None = None
    """The YES-leg side the requester took, as the venue reports it."""
    confirm_by: int | None = None
    """When the maker must confirm (its last look) after an accept."""
    execute_by: int | None = None
    """When the venue submits the orders after a confirm."""
    expires_at: int | None = None
    """When the quote stops being acceptable."""
    taker_order_id: str | None = None
    """The requester's order once executed (Kalshi `rfq_creator_order_id`)."""
    maker_order_id: str | None = None
    """The quoter's order once executed (Kalshi `quote_creator_order_id`)."""
    mine: bool = False
    """Made by this account (the quoter side); `False` for one we received."""
    created_at: int | None = None
    updated_at: int | None = None
    info: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_venue(cls, data: Any) -> "RfqQuote":
        """A quote as a venue reported it. A crossed quote (`bid > ask`) is
        refused when a caller builds one, but a venue may send one: it is
        kept, and `crossed` says so, rather than breaking a whole list."""
        return cls.model_validate(data, context={"venue": True})

    @model_validator(mode="after")
    def _consistent(self, info: ValidationInfo) -> "RfqQuote":
        _yes_price(self.bid, "bid")
        _yes_price(self.ask, "ask")
        if self.bid is None and self.ask is None:
            raise ValueError("a quote has a bid, an ask or both")
        if self.crossed and not (info.context or {}).get("venue"):
            raise ValueError(f"bid {self.bid} is above ask {self.ask}")
        _positive(self.bid_quantity, "bid_quantity")
        _positive(self.ask_quantity, "ask_quantity")
        return self

    @property
    def is_terminal(self) -> bool:
        return self.status in QUOTE_TERMINAL_STATUSES

    @property
    def crossed(self) -> bool:
        """The bid is above the ask: only ever a venue's report (`from_venue`)."""
        return self.bid is not None and self.ask is not None and self.bid > self.ask

    def price_for(self, outcome: str, action: str) -> Decimal | None:
        """The YES price this quote trades at for `action` on `outcome`:
        buying YES or selling NO takes the ask, selling YES or buying NO the
        bid. `None` where the maker declined that side."""
        return self.ask if yes_side(outcome, action) == Side.BUY else self.bid

    def quantity_for(self, outcome: str, action: str) -> Decimal | None:
        return self.ask_quantity if yes_side(outcome, action) == Side.BUY else self.bid_quantity


class AcceptQuote(_Base):
    """A requester taking one side of a quote. Becomes an `rfq_taker` order."""

    quote_id: str
    rfq_id: str | None = None
    """Needed by venues that address a quote under its RFQ (Kalshi, Polymarket US)."""
    outcome: Outcome
    action: Action
    worst_price: Decimal | None = None
    """The worst YES price accepted: the highest for a YES buy (or NO sell),
    the lowest for a YES sell (or NO buy). A quote worse than it is refused
    before anything is sent."""
    quantity: Decimal | None = None
    amount: Decimal | None = None
    """A partial size, where the venue allows one (Kalshi FIX). Neither means
    the quote's full size. At most one."""
    client_order_id: str | None = None
    """The idempotency key; defaults to one derived from the quote."""
    account: Account | None = None
    book: str | None = None
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _consistent(self) -> "AcceptQuote":
        _yes_price(self.worst_price, "worst_price")
        _one_size(self.quantity, self.amount, required=False)
        return self

    @property
    def side(self) -> Side:
        """The YES-leg side taken."""
        return yes_side(self.outcome, self.action)

    def refuses(self, yes_price: Decimal) -> bool:
        """Whether a YES price is worse than `worst_price`."""
        if self.worst_price is None:
            return False
        return yes_price > self.worst_price if self.side == Side.BUY else yes_price < self.worst_price


class MakerQuote(_Base):
    """A quoter's answer to an incoming RFQ, in YES prices."""

    rfq_id: str
    bid: Decimal | None = None
    """The YES price we buy at, if the requester sells."""
    ask: Decimal | None = None
    """The YES price we sell at, if the requester buys."""
    quantity: Decimal | None = None
    """Contracts on each side; `None` means the RFQ's full size."""
    expires_at: int | None = None
    client_order_id: str | None = None
    rest_remainder: bool = False
    post_only: bool = False
    """Our order, once the quote trades, only adds liquidity (Polymarket US
    `postOnly`, participate-don't-initiate)."""
    account: Account | None = None
    book: str | None = None
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _consistent(self) -> "MakerQuote":
        _yes_price(self.bid, "bid")
        _yes_price(self.ask, "ask")
        if self.bid is None and self.ask is None:
            raise ValueError("quote a bid, an ask or both")
        if self.bid is not None and self.ask is not None and self.bid >= self.ask:
            raise ValueError(f"bid {self.bid} must be below ask {self.ask}")
        _positive(self.quantity, "quantity")
        return self
