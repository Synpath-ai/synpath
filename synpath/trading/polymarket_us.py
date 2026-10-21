"""Polymarket US order entry, on the retail API (`api.polymarket.us`).

Polymarket US is a CFTC-regulated exchange with two trading APIs. This is
the one any verified account can use: a key from polymarket.us/developer,
Ed25519-signed requests, twenty requests a second. The exchange API for
onboarded firms (private-key JWT, integer prices, a preprod environment)
is `polymarket_us_exchange`.

Facts that shape the adapter:

**One book per market, both outcomes addressable.** The market trades one
instrument, its YES side; the price on the wire is always the YES price.
But an order names its outcome and action (`OUTCOME_SIDE_NO` +
`ORDER_ACTION_BUY` is buying NO), so an order for `{slug}:no` is sent as
that, priced at `1 - q`, and reads back as the same NO order. Positions
net: long YES, or short YES (the venue's own word for holding NO), with
margin rather than a second inventory.

**Order entry is asynchronous.** Creating, cancelling or modifying returns
an id and nothing about the outcome; the matching engine decides after.
Results here are `pending`, `pending_cancel` or read back, never assumed.
Batched cancels and modifies echo the ids sent, which the venue says is not
a confirmation.

**No client order id.** The venue assigns order ids; a `client_order_id`
is kept on the returned `Order` for the caller's own records and is not
sent. The venue nets YES and NO, so Sell YES and Buy NO are the same order
on the YES leg; to flatten a position the venue offers close-position.
"""
from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ..base import AsyncHttpClient, Capability
from .. import ids
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported
from ..polymarket_us import fee_schedule_of, parse_ts
from ..types import Page
from .base import TradingExchange
from .credentials import PolymarketUSCredentials
from .errors import InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected, RfqNotFound
from .limiter import BudgetLimiter, Priority
from .money import D, complement, validate_quantity, validate_price
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Precision, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
    AcceptQuote, Combo, ComboLeg, MakerQuote, QuoteStatus, Rfq, RfqCloseReason, RfqQuote, RfqStatus,
)

log = logging.getLogger("synpath.trading.polymarket_us")

VENUE = "polymarket_us"
API_URL = "https://api.polymarket.us"
GATEWAY_URL = "https://gateway.polymarket.us"

BATCH = 20
PRICE_FLOOR = Decimal("0.01")
PRICE_CEILING = Decimal("0.99")
"""The exchange's absolute price limits. A price outside them still gets an
order id and is then rejected, so it is refused here first."""

MARKET_TTL_S = 300.0

RFQ_PACE_PER_SECOND = 1.0
"""The venue's per-endpoint limit on its RFQ list and create calls."""
PACED_ENDPOINTS: dict[tuple[str, str], str] = {
    ("GET", "/v1/rfqs"): "rfqs",
    ("GET", "/v1/rfqs/quotes"): "rfq_quotes",
    ("POST", "/v1/rfqs"): "rfq_create",
    ("POST", "/v1/combos"): "rfq_create",
}
"""Endpoints the venue limits to one request a second, each to its pace
group (docs/rfq-combo.md, Polymarket US): `GET /v1/rfqs`, `GET
/v1/rfqs/quotes`, and `POST /v1/rfqs` with `POST /v1/combos`, which share
one budget. Exact paths: `/v1/rfqs/user-id` and the per-id writes (cancel,
accept, confirm) only meet the shared limiter."""
PACE_MAX_WAIT_S = 15.0
"""How long a paced call may queue behind others in its group before it is
refused (`RateBudgetExceeded`) rather than sent late."""


# ---------------------------------------------------------------------------
# Market rules
# ---------------------------------------------------------------------------

@dataclass
class MarketRules:
    """What an order on one market must respect, from the public gateway."""

    slug: str
    tick: Decimal
    min_quantity: Decimal
    raw: dict[str, Any] = field(default_factory=dict)
    read_at: float = field(default_factory=time.monotonic)

    @property
    def precision(self) -> Precision:
        whole = self.min_quantity >= 1 and self.min_quantity % 1 == 0
        return Precision(
            tick=self.tick, min_quantity=self.min_quantity, quantity_step=self.min_quantity, whole_contracts=whole,
        )

    @classmethod
    def from_market(cls, market: dict[str, Any]) -> "MarketRules":
        return cls(
            slug=str(market.get("slug") or ""),
            tick=D(str(market.get("orderPriceMinTickSize") or "0.01")),
            min_quantity=D(str(market.get("minimumTradeQty") or "1")),
            raw=market,
        )


# ---------------------------------------------------------------------------
# Pure translation
# ---------------------------------------------------------------------------

def slug_of(market_id: str) -> str:
    """The slug behind a Synpath market id (`polymarket_us:<slug>`) or a bare
    slug. Another venue's id is refused before anything is signed."""
    try:
        return ids.native(VENUE, market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None


TIF_TO_WIRE = {
    TimeInForce.GTC: "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    TimeInForce.GTD: "TIME_IN_FORCE_GOOD_TILL_DATE",
    TimeInForce.IOC: "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
    TimeInForce.FOK: "TIME_IN_FORCE_FILL_OR_KILL",
}
TIF_FROM_WIRE = {wire: tif for tif, wire in TIF_TO_WIRE.items()} | {"TIME_IN_FORCE_DAY": TimeInForce.DAY}


def rfc3339(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def number(value: Decimal) -> int | float:
    """A quantity as the JSON number the venue wants: an integer when whole."""
    return int(value) if value == value.to_integral_value() else float(value)


def usd(value: Decimal) -> dict[str, str]:
    return {"value": str(value), "currency": "USD"}


def check_prices(price: Decimal, rules: MarketRules) -> Decimal:
    """Validate a YES price; the wire carries it as given."""
    validate_price(price, rules.precision)
    wire = price
    if not PRICE_FLOOR <= wire <= PRICE_CEILING:
        raise InvalidOrder(
            f"polymarket_us: the YES price would be {wire}, outside the exchange's [{PRICE_FLOOR}, {PRICE_CEILING}]"
        )
    return wire


def translate_order(request: OrderRequest, rules: MarketRules) -> dict[str, Any]:
    """An `OrderRequest` as the `POST /v1/orders` body."""
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"polymarket_us: {request.type.value} is held by the execution engine on this API; "
            f"submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder(
            "polymarket_us: 'day' is rewritten to 'gtd' by the engine; the venue's own DAY orders do not "
            "cancel at the session roll"
        )
    if request.price is None:
        raise InvalidOrder(
            "polymarket_us: a price is required -- a market order is sent as an immediate limit at the "
            "protection price you give"
        )
    slug = slug_of(request.market_id)
    wire_price = check_prices(D(request.price), rules)
    quantity = validate_quantity(D(request.quantity), rules.precision)
    if request.type == OrderType.MARKET:
        tif = TimeInForce.FOK if request.time_in_force == TimeInForce.FOK else TimeInForce.IOC
    else:
        tif = request.time_in_force
    if request.post_only and tif not in (TimeInForce.GTC, TimeInForce.GTD):
        raise InvalidOrder("polymarket_us: post-only orders must be gtc or gtd")
    body: dict[str, Any] = {
        "marketSlug": slug,
        "type": "ORDER_TYPE_LIMIT",
        "price": usd(wire_price),
        "quantity": number(quantity),
        "tif": TIF_TO_WIRE[tif],
        # Always the YES leg, as every price in this library is: `sell` is a
        # YES sell, which on this netting venue is the same order as a NO buy.
        "outcomeSide": "OUTCOME_SIDE_YES",
        "action": "ORDER_ACTION_BUY" if request.side == Side.BUY else "ORDER_ACTION_SELL",
        "manualOrderIndicator": (
            "MANUAL_ORDER_INDICATOR_MANUAL" if request.params.get("manual") else "MANUAL_ORDER_INDICATOR_AUTOMATIC"
        ),
    }
    if tif == TimeInForce.GTD:
        if request.expires_at is None:
            raise InvalidOrder("polymarket_us: a gtd order needs expires_at")
        body["goodTillTime"] = rfc3339(request.expires_at)
    if request.post_only:
        body["participateDontInitiate"] = True
    return body


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

STATE = {
    "ORDER_STATE_PENDING_NEW": OrderStatus.PENDING,
    "ORDER_STATE_PENDING_RISK": OrderStatus.PENDING,
    "ORDER_STATE_NEW": OrderStatus.OPEN,
    "ORDER_STATE_PARTIALLY_FILLED": OrderStatus.OPEN,
    "ORDER_STATE_PENDING_REPLACE": OrderStatus.PENDING_REPLACE,
    "ORDER_STATE_PENDING_CANCEL": OrderStatus.PENDING_CANCEL,
    "ORDER_STATE_FILLED": OrderStatus.CLOSED,
    "ORDER_STATE_CANCELED": OrderStatus.CANCELED,
    "ORDER_STATE_REPLACED": OrderStatus.CANCELED,
    "ORDER_STATE_REJECTED": OrderStatus.REJECTED,
    "ORDER_STATE_EXPIRED": OrderStatus.EXPIRED,
}
"""`REPLACED` is the superseded half of a cancel-replace; it will not trade
again, so it reads as cancelled with the native state kept in `info`."""

INTENT = {
    "ORDER_INTENT_BUY_LONG": ("yes", Side.BUY),
    "ORDER_INTENT_SELL_LONG": ("yes", Side.SELL),
    "ORDER_INTENT_BUY_SHORT": ("no", Side.BUY),
    "ORDER_INTENT_SELL_SHORT": ("no", Side.SELL),
}


def _amount(value: Any) -> Decimal | None:
    if isinstance(value, dict):
        value = value.get("value")
    return D(value) if value not in (None, "") else None


def outcome_and_side(raw: dict[str, Any]) -> tuple[str, Side]:
    """The outcome and action the venue holds the order in."""
    side, action = raw.get("outcomeSide"), raw.get("action")
    if side and action:
        return ("yes" if side == "OUTCOME_SIDE_YES" else "no"), (Side.BUY if action == "ORDER_ACTION_BUY" else Side.SELL)
    if raw.get("intent") in INTENT:
        return INTENT[raw["intent"]]
    return "yes", Side.BUY if raw.get("side") == "ORDER_SIDE_BUY" else Side.SELL


def yes_leg(outcome: str, side: Side) -> Side:
    """An order the venue holds as NO, seen from the YES leg: buying NO is
    selling YES and vice versa."""
    if outcome == "yes":
        return side
    return Side.SELL if side == Side.BUY else Side.BUY


def yes_price(price: Decimal, outcome: str) -> Decimal:
    return price if outcome == "yes" else complement(price)


def order_of(raw: dict[str, Any], *, account: Account | None = None) -> Order:
    """A venue order on the YES leg: an order the venue holds as a NO buy at
    0.30 reads as a sell at 0.70."""
    slug = str(raw.get("marketSlug") or (raw.get("marketMetadata") or {}).get("slug") or "")
    outcome, venue_side = outcome_and_side(raw)
    side = yes_leg(outcome, venue_side)
    price = _amount(raw.get("price"))
    average = _amount(raw.get("avgPx"))
    quantity = D(str(raw.get("quantity") or 0))
    filled = D(str(raw.get("cumQuantity") or 0))
    leaves = raw.get("leavesQuantity")
    status = STATE.get(str(raw.get("state") or ""), OrderStatus.PENDING)
    return Order(
        id=str(raw.get("id") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, slug),
        side=side,
        type=OrderType.MARKET if raw.get("type") == "ORDER_TYPE_MARKET" else OrderType.LIMIT,
        time_in_force=TIF_FROM_WIRE.get(str(raw.get("tif") or ""), TimeInForce.GTC),
        status=status,
        price=price,  # the venue already quotes the YES price
        quantity=quantity,
        filled=filled,
        remaining=D(str(leaves)) if leaves is not None else None,
        average_price=average if average is not None and filled > 0 else None,
        fee=_amount(raw.get("commissionNotionalTotalCollected")),
        fee_currency="USD",
        expires_at=parse_ts(raw.get("goodTillTime")),
        created_at=parse_ts(raw.get("createTime")),
        updated_at=parse_ts(raw.get("insertTime")),
        info=raw,
    )


def pending_order(order_id: str, request: OrderRequest, body: dict[str, Any], account: Account) -> Order:
    """The order just sent, before the engine has said anything about it."""
    return Order(
        id=order_id, client_order_id=request.client_order_id, venue=VENUE, account=account,
        market_id=ids.qualify(VENUE, body["marketSlug"]), side=request.side,
        type=request.type, time_in_force=TIF_FROM_WIRE[body["tif"]], status=OrderStatus.PENDING,
        price=D(request.price) if request.price is not None else None, quantity=D(request.quantity),
        post_only=bool(body.get("participateDontInitiate")), expires_at=request.expires_at,
        created_at=int(time.time() * 1000), book=request.book, trader=request.trader, tags=request.tags,
        info={"request": body},
    )


def position_of(slug: str, raw: dict[str, Any], *, account: Account | None = None) -> Position:
    """A netted position: long or short the market's YES side."""
    net = D(raw.get("netPositionDecimal") or raw.get("netPosition") or "0")
    cost = _amount(raw.get("cost"))
    contracts = abs(net)
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, slug),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=contracts,
        entry_price=(abs(cost) / contracts).quantize(Decimal("0.0001")) if cost is not None and contracts > 0 else None,
        unrealized_pnl=_amount(raw.get("cashValue")),
        realized_pnl=_amount(raw.get("realized")),
        resolved=bool(raw.get("expired")),
        timestamp=parse_ts(raw.get("updateTime")),
        info=raw,
    )


RESOLUTION_RESULT = {"POSITION_RESOLUTION_SIDE_LONG": "yes", "POSITION_RESOLUTION_SIDE_SHORT": "no"}


def settlement_of(raw: dict[str, Any], *, account: Account | None = None) -> Settlement:
    """A position resolution from the activity feed. Realized P&L is the change
    across the resolution; `result` follows the venue's resolution side."""
    before = raw.get("beforePosition") or {}
    after = raw.get("afterPosition") or {}
    net = D(before.get("netPositionDecimal") or before.get("netPosition") or "0")
    result = RESOLUTION_RESULT.get(str(raw.get("side") or ""))
    held = "yes" if net > 0 else "no" if net < 0 else None
    realized_before, realized_after = _amount(before.get("realized")), _amount(after.get("realized"))
    slug = str(raw.get("marketSlug") or "")
    return Settlement(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, slug),
        held=PositionSide.LONG if held == "yes" else PositionSide.SHORT if held == "no" else None,
        result=result,
        won=(held == result) if held and result else None,
        quantity=abs(net) if held else None,
        cost=_amount(before.get("cost")),
        payout=None,
        pnl=(realized_after - realized_before) if realized_after is not None and realized_before is not None else None,
        timestamp=parse_ts(raw.get("updateTime")),
        info=raw,
    )


def balance_of(raw: dict[str, Any], *, account: Account) -> Balance:
    rows = raw.get("balances") or []
    row = next((r for r in rows if str(r.get("currency") or "USD").upper() == "USD"), rows[0] if rows else {})
    total = D(str(row.get("currentBalance") or 0))
    buying_power = D(str(row["buyingPower"])) if row.get("buyingPower") is not None else None
    return Balance(
        venue=VENUE, account=account, currency="USD", total=total,
        available=buying_power if buying_power is not None else total,
        locked=D(str(row["openOrders"])) if row.get("openOrders") is not None else None,
        buying_power=buying_power, timestamp=parse_ts(row.get("lastUpdated")), info=row,
    )


def error_of(exc: ExchangeError) -> ExchangeError:
    body = exc.body if isinstance(exc.body, dict) else {}
    message = str(body.get("message") or body.get("error") or exc)
    text = message.lower()
    if isinstance(exc, AuthenticationError):
        return exc
    if "global rate limit exceeded" in text:
        # The venue's five-second latency stopgap, not a rate limit: safe to resend.
        return OrderRejected(message, reason="latency_stopgap", info=body, body=exc.body, status=exc.status)
    if "buying power" in text or "insufficient" in text:
        return InsufficientFunds(message, body=exc.body, status=exc.status)
    if ("closed" in text and "market" in text) or "halt" in text or "maintenance" in text:
        return MarketHalted(message, body=exc.body, status=exc.status)
    if isinstance(exc, MarketNotFound):
        return OrderNotFound(message, body=exc.body, status=exc.status)
    if isinstance(exc, BadRequest):
        return OrderRejected(message, reason=str(body.get("code") or "") or None, info=body, body=exc.body, status=exc.status)
    return exc


# ---------------------------------------------------------------------------
# Combos and RFQ (docs/rfq-combo.md)
# ---------------------------------------------------------------------------
#
# Sources: https://docs.polymarket.us/api-reference/combos/overview.md,
# /api-reference/rfqs/overview.md, /trader-guide/combos.md,
# /partners/orders/combos-takers.md and the OpenAPI schemas
# /api-reference/oapi-schemas/{combos,rfqs}-schema.json.
#
# A combo is an instrument with its own symbol (`caoc-...`, `crossd-...`);
# the symbol is a market slug like any other here, so its Synpath id is
# `polymarket_us:<symbol>` and its orders and positions use that slug.
#
# Sides are the requester's, on the YES (long) leg, and unambiguous:
# `buyPrice` is "the price offered for a requester BUY order", `sellPrice`
# "for a requester SELL order", and `acceptedSide` is the requester's side
# (`SIDE_BUY` takes `buyPrice` and `buyQtyDecimal`, the maker sells). Both
# prices are YES prices: a requester sell costs `1 - price` a contract.
# A price of "0" marks a side the maker declined.

MAX_COMBO_LEGS = 50
COMBO_PREFIXES = ("caoc-", "crossd-")
"""The documented combo symbol prefixes. The venue says to treat symbols as
opaque; this is only the fallback when an RFQ carries no `comboLegs`."""

SIDE_TO_WIRE = {Side.BUY: "SIDE_BUY", Side.SELL: "SIDE_SELL"}
SIDE_FROM_WIRE = {wire: side for side, wire in SIDE_TO_WIRE.items()}

RFQ_STATUS_TO_WIRE = {RfqStatus.OPEN: "RFQ_STATUS_OPEN", RfqStatus.CLOSED: "RFQ_STATUS_CLOSED"}
RFQ_STATUS_FROM_WIRE = {wire: status for status, wire in RFQ_STATUS_TO_WIRE.items()}

QUOTE_STATUS_FROM_WIRE = {
    "QUOTE_STATUS_ACTIVE": QuoteStatus.OPEN,
    "QUOTE_STATUS_ACCEPTED": QuoteStatus.ACCEPTED,
    "QUOTE_STATUS_CONFIRMED": QuoteStatus.CONFIRMED,
    "QUOTE_STATUS_EXECUTED": QuoteStatus.EXECUTED,
    # Withdrawn, replaced, or declined during the last look: the venue has
    # no expired or failed status, and none of these traded.
    "QUOTE_STATUS_DELETED": QuoteStatus.CANCELED,
}
QUOTE_STATUS_TO_WIRE = {status: wire for wire, status in QUOTE_STATUS_FROM_WIRE.items()}

COMBO_STATE = {"INSTRUMENT_STATE_OPEN": "open"}
"""Every other instrument state (pre-open, suspended, halted, closed,
expired, terminated...) reads as `closed`; the native state stays in `info`.
Settlement is read on the symbol, not from this state."""


def decimal_text(value: Decimal) -> str:
    """An exact decimal string, as the RFQ API wants prices and sizes: no
    exponent, no trailing zeros (`0.6150` -> `"0.615"`, `10` -> `"10"`)."""
    return format(D(value).normalize(), "f")


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, dict):
        value = value.get("value")
    return D(str(value)) if value not in (None, "") else None


def _positive_or_none(value: Any) -> Decimal | None:
    """A price or quantity, `None` for a declined side ("0") or an absent one."""
    number_ = _decimal(value)
    return number_ if number_ is not None and number_ > 0 else None


def is_combo_symbol(symbol: str) -> bool:
    return symbol.startswith(COMBO_PREFIXES)


def leg_to_wire(leg: ComboLeg) -> dict[str, str]:
    """`outcome=yes` buys the leg (`SIDE_BUY`); `no` sells it (`SIDE_SELL`)."""
    return {"symbol": slug_of(leg.market_id), "side": "SIDE_BUY" if leg.outcome == "yes" else "SIDE_SELL"}


def leg_of(raw: dict[str, Any]) -> ComboLeg:
    return ComboLeg(
        market_id=ids.qualify(VENUE, str(raw.get("symbol") or "")),
        outcome="yes" if raw.get("side") == "SIDE_BUY" else "no",
        info=raw,
    )


def combo_of(raw: dict[str, Any]) -> Combo:
    """A `v1ComboInstrument`."""
    state = str(raw.get("state") or "")
    return Combo(
        market_id=ids.qualify(VENUE, str(raw.get("id") or "")),
        venue=VENUE,
        legs=[leg_of(leg) for leg in raw.get("legs") or []],
        status=COMBO_STATE.get(state, "closed") if state else None,
        created_at=parse_ts(raw.get("createdTime")),
        info=raw,
    )


def rfq_of(raw: dict[str, Any], *, me: str | None = None, mine: bool | None = None, account: Account | None = None) -> Rfq:
    """A `v1RFQ`. `mine` is whether `rfqCreatorUserId` is `me`, this
    participant's RFQ user id, unless the caller already knows."""
    symbol = str(raw.get("symbol") or "")
    instrument = ids.qualify(VENUE, symbol)
    legs = raw.get("comboLegs") or []
    combo = None
    if len(legs) >= 2:
        combo = Combo(market_id=instrument, venue=VENUE, legs=[leg_of(leg) for leg in legs], info={"comboLegs": legs})
    on_combo = combo is not None or is_combo_symbol(symbol)
    if mine is None:
        mine = me is not None and raw.get("rfqCreatorUserId") == me
    status = RFQ_STATUS_FROM_WIRE.get(str(raw.get("status") or ""), RfqStatus.OPEN)
    return Rfq(
        id=str(raw.get("id") or ""),
        venue=VENUE,
        account=account if mine else None,
        market_id=None if on_combo else instrument,
        combo_id=instrument if on_combo else None,
        combo=combo,
        quantity=_positive_or_none(raw.get("qtyDecimal")),
        amount=_positive_or_none(raw.get("cashOrderQty")),
        rest_remainder=bool(raw.get("restRemainder")),
        status=status,
        mine=mine,
        created_at=parse_ts(raw.get("createdTime")),
        updated_at=parse_ts(raw.get("updatedTime")),
        info=raw,
    )


def quote_of(raw: dict[str, Any], *, me: str | None = None, mine: bool | None = None, account: Account | None = None) -> RfqQuote:
    """A `v1Quote` on the YES leg: `ask` is `buyPrice` (what the requester
    buys at), `bid` is `sellPrice` (what it sells at); a "0" side is `None`.
    `accepted_side` is the requester's side as the venue reports it."""
    if mine is None:
        mine = me is not None and raw.get("creatorRfqUserId") == me
    accepted = SIDE_FROM_WIRE.get(str(raw.get("acceptedSide") or ""))
    symbol = raw.get("symbol")
    quote = RfqQuote.from_venue(dict(
        id=str(raw.get("id") or ""),
        rfq_id=str(raw.get("rfqId") or ""),
        venue=VENUE,
        account=account if mine else None,
        market_id=ids.qualify(VENUE, str(symbol)) if symbol else None,
        bid=_positive_or_none(raw.get("sellPrice")),
        ask=_positive_or_none(raw.get("buyPrice")),
        bid_quantity=_positive_or_none(raw.get("sellQtyDecimal")),
        ask_quantity=_positive_or_none(raw.get("buyQtyDecimal")),
        status=QUOTE_STATUS_FROM_WIRE.get(str(raw.get("status") or ""), QuoteStatus.OPEN),
        accepted_side=accepted,
        confirm_by=parse_ts(raw.get("confirmationDeadline")),
        execute_by=parse_ts(raw.get("executionDeadline")),
        taker_order_id=raw.get("rfqCreatorOrderId") or None,
        maker_order_id=raw.get("creatorOrderId") or None,
        mine=mine,
        created_at=parse_ts(raw.get("createdTime")),
        updated_at=parse_ts(raw.get("updatedTime")),
        info=raw,
    ))
    if quote.crossed:
        log.warning("polymarket_us: quote %s on RFQ %s is crossed (bid %s above ask %s); kept and flagged",
                    quote.id, quote.rfq_id, quote.bid, quote.ask)
    return quote


def tolerant(rows: Any, build: Any, what: str) -> list[Any]:
    """Normalise a venue list row by row: a row the model refuses is logged
    and skipped, so one bad record does not break the whole list."""
    out = []
    for row in rows or []:
        try:
            out.append(build(row))
        except (ValueError, TypeError) as exc:
            log.warning("polymarket_us: skipped a %s the venue sent that does not validate (%s): %r",
                        what, exc, row)
    return out


def translate_rfq(
    instrument_id: str, *, quantity: Decimal | None, amount: Decimal | None, rest_remainder: bool,
) -> dict[str, Any]:
    """The `POST /v1/rfqs` body: `qtyDecimal` for contracts, `cashOrderQty`
    for money (the venue sizes each quoted side to it, taker fee included)."""
    if (quantity is None) == (amount is None):
        raise InvalidOrder("polymarket_us: size an RFQ by quantity (contracts) or amount (USD), exactly one")
    body: dict[str, Any] = {"symbol": slug_of(instrument_id), "restRemainder": bool(rest_remainder)}
    if quantity is not None:
        if D(quantity) <= 0:
            raise InvalidOrder(f"polymarket_us: quantity must be positive, got {quantity}")
        body["qtyDecimal"] = decimal_text(quantity)
    else:
        if D(amount) <= 0:
            raise InvalidOrder(f"polymarket_us: amount must be positive, got {amount}")
        body["cashOrderQty"] = decimal_text(amount)  # type: ignore[arg-type]
    return body


def translate_quote(quote: MakerQuote) -> dict[str, Any]:
    """The `POST /v1/rfqs/quotes` body. `buyPrice` is our ask (the requester
    buys, we sell), `sellPrice` our bid; "0" declines a side. The venue takes
    no quantity, expiry or client id: sizes come from the RFQ."""
    if quote.quantity is not None:
        raise InvalidOrder("polymarket_us: a quote is for the RFQ's full size; the venue takes no quantity")
    if quote.expires_at is not None:
        raise InvalidOrder("polymarket_us: a quote has no expiry on this venue; cancel it instead")
    body: dict[str, Any] = {
        "rfqId": quote.rfq_id,
        "buyPrice": decimal_text(quote.ask) if quote.ask is not None else "0",
        "sellPrice": decimal_text(quote.bid) if quote.bid is not None else "0",
        "restRemainder": bool(quote.rest_remainder),
    }
    if quote.post_only:
        body["postOnly"] = True
    return body


def accept_key(quote_id: str) -> str:
    """The engine's idempotency key for an accept (docs/rfq-combo.md §5)."""
    return f"rfq-accept:{VENUE}:{quote_id}"


def rfq_error_of(exc: ExchangeError) -> ExchangeError:
    """`error_of`, except that a 404 names an RFQ, quote or combo."""
    if isinstance(exc, MarketNotFound):
        return RfqNotFound(str(exc), body=exc.body, status=exc.status)
    return error_of(exc)



# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class PolymarketUSSigner:
    """Holds the Ed25519 key and signs `timestamp + METHOD + path`, the path
    without its query string, as the venue's SDK does."""

    def __init__(self, key_id: str, secret_key: str):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        raw = base64.b64decode(secret_key)
        if len(raw) not in (32, 64):
            raise InvalidOrder("polymarket_us: the secret key must be a base64 Ed25519 key (32 or 64 bytes)")
        self.key_id = key_id
        self._key = Ed25519PrivateKey.from_private_bytes(raw[:32])

    def headers(self, method: str, path: str, *, timestamp_ms: int | None = None) -> dict[str, str]:
        stamp = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1000))
        signature = self._key.sign(f"{stamp}{method.upper()}{path}".encode())
        return {
            "X-PM-Access-Key": self.key_id,
            "X-PM-Timestamp": stamp,
            "X-PM-Signature": base64.b64encode(signature).decode(),
        }


class PolymarketUSTrading(TradingExchange):
    """Polymarket US order entry on the retail API.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.polymarket_us import PolymarketUSTrading

    creds = require("polymarket_us", load_credentials())
    async with PolymarketUSTrading(creds) as us:
        print(await us.fetch_balance())
    ```
    """

    id = VENUE
    name = "Polymarket US"
    has: dict[str, Capability] = {
        "create_order": True,
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": False,
        # The activity feed's trades carry no order id and no side, so they
        # cannot be fills; `fetch_activities` returns them as the venue does.
        "fetch_my_trades": False,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        # The old key means the raw Kalshi RFQ methods; this venue answers
        # the typed ones below (docs/rfq-combo.md, P2).
        "rfq": False,
        "rfq_taker": True,
        "rfq_quoter": True,
        "combos": True,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: PolymarketUSCredentials,
        *,
        account_name: str = "default",
        api_url: str = API_URL,
        gateway_url: str = GATEWAY_URL,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        rfq_pace_per_second: float | None = RFQ_PACE_PER_SECOND,
    ):
        import httpx

        self.credentials = credentials
        self.signer = PolymarketUSSigner(credentials.key_id, credentials.secret_key)
        self.account = Account(venue=VENUE, name=account_name)
        # Twenty requests a second per key, shared by every endpoint: one lane.
        self.limiter = limiter or BudgetLimiter(read_per_second=20, write_per_second=20, burst_seconds=1)
        # One request a second on the RFQ lists and creates, per pace group,
        # ahead of the shared limiter: the engine's follower polls quotes and
        # must not trip the venue's 429s. `None` turns it off.
        self.pacers: dict[str, BudgetLimiter] = {} if not rfq_pace_per_second else {
            group: BudgetLimiter(read_per_second=rfq_pace_per_second, write_per_second=rfq_pace_per_second,
                                 burst_seconds=1, borrow_seconds=1, max_wait_s=PACE_MAX_WAIT_S)
            for group in set(PACED_ENDPOINTS.values())
        }
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.api = AsyncHttpClient(api_url, limiter=None, client=shared, venue=VENUE)
        self.gateway = AsyncHttpClient(gateway_url, limiter=None, client=shared, venue=VENUE)
        self._http = shared
        self._markets: dict[str, MarketRules] = {}
        self._rfq_user_id: str | None = None

    async def _call(
        self, method: str, path: str, *, params: Any = None, json: Any = None, priority: Priority = Priority.NORMAL,
        errors: Any = error_of,
    ) -> Any:
        group = PACED_ENDPOINTS.get((method.upper(), path))
        if group is not None and group in self.pacers:
            await self.pacers[group].acquire(cost=1, kind="write", priority=Priority.NORMAL)
        await self.limiter.acquire(cost=1, kind="write", priority=priority)
        headers = self.signer.headers(method, path)
        try:
            return await self.api.request(
                method, path, params=params or None, json=json, headers=headers,
            )
        except ExchangeError as exc:
            raise errors(exc) from None

    async def _rfq_call(self, method: str, path: str, **kwargs: Any) -> Any:
        return await self._call(method, path, errors=rfq_error_of, **kwargs)

    # -- market rules ---------------------------------------------------------

    async def market_rules(self, slug: str, *, refresh: bool = False) -> MarketRules:
        cached = self._markets.get(slug)
        if cached and not refresh and time.monotonic() - cached.read_at < MARKET_TTL_S:
            return cached
        try:
            raw = await self.gateway.get(f"/v1/market/slug/{slug}")
        except MarketNotFound:
            raise InvalidOrder(f"polymarket_us: no market {slug!r}") from None
        rules = MarketRules.from_market((raw or {}).get("market") or raw or {})
        self._markets[slug] = rules
        return rules

    def remember_market(self, rules: MarketRules) -> None:
        self._markets[rules.slug] = rules

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest, *, rules: MarketRules | None = None) -> Order:
        """Send one order. The result is `pending`: the venue answers with an
        id, and whether the order rests, fills or is rejected comes after."""
        slug = slug_of(request.market_id)
        body = translate_order(request, rules or await self.market_rules(slug))
        raw = await self._call("POST", "/v1/orders", json=body)
        executions = (raw or {}).get("executions") or []
        if executions and executions[-1].get("order"):
            order = order_of(executions[-1]["order"], account=request.account or self.account)
            return order.model_copy(update={"client_order_id": request.client_order_id})
        return pending_order(str((raw or {}).get("id") or ""), request, body, request.account or self.account)

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Twenty to a request. The gateway rejects a whole batch if any entry
        is malformed, so every entry is validated here first and the refused
        ones never join a batch."""
        prepared: list[dict[str, Any] | Exception] = []
        for request in requests:
            try:
                slug = slug_of(request.market_id)
                prepared.append(translate_order(request, await self.market_rules(slug)))
            except InvalidOrder as exc:
                prepared.append(exc)
        results: list[Order | Exception] = list(prepared)  # type: ignore[arg-type]
        sendable = [i for i, p in enumerate(prepared) if isinstance(p, dict)]
        for start in range(0, len(sendable), BATCH):
            chunk = sendable[start:start + BATCH]
            raw = await self._call("POST", "/v1/orders/batched", json={"orders": [prepared[i] for i in chunk]})
            ids = list((raw or {}).get("createdOrderIds") or [])
            for offset, index in enumerate(chunk):
                if offset < len(ids) and ids[offset]:
                    results[index] = pending_order(str(ids[offset]), requests[index], prepared[index], requests[index].account or self.account)  # type: ignore[arg-type]
                else:
                    results[index] = OrderRejected("polymarket_us: no order id returned for this entry", reason="no_id")
        return results

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Ask for a cancel and read the order back; it is usually
        `pending_cancel` or already `canceled` by then."""
        slug = slug_of(market_id) if market_id else slug_of((await self.fetch_order(order_id)).market_id)
        await self._call("POST", f"/v1/order/{order_id}/cancel", json={"marketSlug": slug}, priority=Priority.HIGH)
        return await self.fetch_order(order_id)

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel by id, twenty to a request. The venue echoes the ids rather
        than confirming them, so each comes back `pending_cancel`; an id not
        among the open orders comes back `OrderNotFound` without being sent."""
        open_orders = {o.id: o for o in await self.fetch_open_orders(market_id=market_id)}
        results: dict[str, Order | Exception] = {}
        entries = []
        for oid in order_ids:
            if oid in open_orders:
                entries.append({"orderId": oid, "marketSlug": slug_of(open_orders[oid].market_id)})
            else:
                results[oid] = OrderNotFound(f"polymarket_us: {oid} is not an open order")
        for start in range(0, len(entries), BATCH):
            chunk = entries[start:start + BATCH]
            await self._call("POST", "/v1/orders/batched/cancel", json={"orders": chunk}, priority=Priority.HIGH)
            for entry in chunk:
                results[entry["orderId"]] = open_orders[entry["orderId"]].model_copy(update={"status": OrderStatus.PENDING_CANCEL})
        return [results[oid] for oid in order_ids]

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        body = {"slugs": [slug_of(market_id)]} if market_id else {}
        raw = await self._call("POST", "/v1/orders/open/cancel", json=body, priority=Priority.HIGH)
        return len((raw or {}).get("canceledOrderIds") or [])

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """Modify price, quantity, time in force or expiry in place.

        The venue forwards a modify to the exchange as a cancel-replace, and
        does not say whether queue priority survives, so
        `queue_priority_preserved` is `None`. Prices are in the order's own
        outcome terms; the quantity is the order's new total.
        """
        current = current or await self.fetch_order(request.order_id)
        slug = slug_of(current.market_id)
        rules = await self.market_rules(slug)
        price = D(request.price) if request.price is not None else current.price
        if price is None:
            raise InvalidOrder("polymarket_us: the order has no price to keep")
        quantity = validate_quantity(D(request.quantity), rules.precision) if request.quantity is not None else current.quantity
        tif = request.time_in_force or current.time_in_force
        if tif == TimeInForce.DAY:
            raise InvalidOrder("polymarket_us: 'day' is rewritten to 'gtd' by the engine")
        body: dict[str, Any] = {
            "marketSlug": slug,
            "price": usd(check_prices(price, rules)),
            "quantity": number(quantity),
            "tif": TIF_TO_WIRE[tif],
        }
        expires = request.expires_at or current.expires_at
        if tif == TimeInForce.GTD:
            if expires is None:
                raise InvalidOrder("polymarket_us: a gtd order needs expires_at")
            body["goodTillTime"] = rfc3339(expires)
        await self._call("POST", f"/v1/order/{request.order_id}/modify", json=body)
        order = await self.fetch_order(request.order_id)
        return order.model_copy(update={"queue_priority_preserved": None, "info": {**order.info, "modify": body}})

    async def fetch_order(self, order_id: str) -> Order:
        raw = await self._call("GET", f"/v1/order/{order_id}")
        if not raw or not (raw.get("order") or {}).get("id"):
            raise OrderNotFound(f"polymarket_us: no order {order_id}")
        return order_of(raw["order"], account=self.account)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        raw = await self._call("GET", "/v1/orders/open", params={"slugs": slug_of(market_id)} if market_id else None)
        return [order_of(row, account=self.account) for row in (raw or {}).get("orders") or []]

    async def preview_order(self, request: OrderRequest) -> Order:
        """The venue's own validation and expected result, without placing anything."""
        slug = slug_of(request.market_id)
        body = translate_order(request, await self.market_rules(slug))
        raw = await self._call("POST", "/v1/order/preview", json={"request": body})
        return order_of((raw or {}).get("order") or {}, account=self.account)

    async def close_position(
        self, market_id: str, *, slippage_ticks: int | None = None, reference_price: Decimal | None = None,
    ) -> Order:
        """Sell everything held in a market at market, within an optional
        slippage band of `slippage_ticks` from `reference_price` (a YES price)."""
        slug = slug_of(market_id)
        body: dict[str, Any] = {"marketSlug": slug, "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC"}
        if slippage_ticks is not None:
            if reference_price is None:
                raise InvalidOrder("polymarket_us: a slippage band needs the reference price it is measured from")
            body["slippageTolerance"] = {"currentPrice": usd(D(reference_price)), "ticks": int(slippage_ticks)}
        raw = await self._call("POST", "/v1/order/close-position", json=body)
        executions = (raw or {}).get("executions") or []
        if executions and executions[-1].get("order"):
            return order_of(executions[-1]["order"], account=self.account)
        return Order(
            id=str((raw or {}).get("id") or ""), venue=VENUE, account=self.account,
            market_id=ids.qualify(VENUE, slug), side=Side.SELL, type=OrderType.MARKET,
            time_in_force=TimeInForce.IOC, status=OrderStatus.PENDING, quantity=Decimal("0"),
            info={"request": body, "close_position": True},
        )

    # -- account --------------------------------------------------------------

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        positions: list[Position] = []
        cursor: str | None = None
        while True:
            raw = await self._call("GET", "/v1/portfolio/positions", params={
                k: v for k, v in {"market": slug_of(market_id) if market_id else None, "limit": 100, "cursor": cursor}.items() if v is not None
            })
            for slug, row in ((raw or {}).get("positions") or {}).items():
                positions.append(position_of(slug, row, account=self.account))
            cursor = (raw or {}).get("nextCursor") or None
            if (raw or {}).get("eof") or not cursor:
                return [p for p in positions if p.contracts > 0]

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        return balance_of(await self._call("GET", "/v1/account/balances") or {}, account=account or self.account)

    async def fetch_activities(
        self, *, types: list[str] | None = None, market_id: str | None = None, limit: int | None = None,
        cursor: str | None = None, newest_first: bool = True,
    ) -> Page:
        """The venue's activity feed as it sends it: trades, resolutions,
        deposits and withdrawals, rebates."""
        params = {
            "types": types, "marketSlug": slug_of(market_id) if market_id else None, "limit": limit, "cursor": cursor,
            "sortOrder": "SORT_ORDER_DESCENDING" if newest_first else "SORT_ORDER_ASCENDING",
        }
        raw = await self._call("GET", "/v1/portfolio/activities", params={k: v for k, v in params.items() if v is not None})
        return Page(list((raw or {}).get("activities") or []), next_cursor=None if (raw or {}).get("eof") else (raw or {}).get("nextCursor"))

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        page = await self.fetch_activities(
            types=["ACTIVITY_TYPE_POSITION_RESOLUTION"], market_id=market_id, limit=limit, cursor=cursor,
        )
        rows = [settlement_of(a.get("positionResolution") or {}, account=self.account) for a in page]
        if since:
            rows = [r for r in rows if (r.timestamp or 0) >= since]
        return Page(rows, next_cursor=page.next_cursor)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The market's published taker theta and the venue's maker rebate at
        the YES `price` for `quantity` contracts. A negative maker fee is a rebate."""
        slug = slug_of(market_id)
        rules = await self.market_rules(slug)
        schedule = fee_schedule_of(rules.raw)
        if schedule is None:
            raise NotSupported(f"polymarket_us: market {slug} publishes no fee coefficient")
        wire = float(D(price))
        taker = schedule.estimate(wire, float(quantity), taker=True)
        maker = schedule.estimate(wire, float(quantity), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, slug), side=side, price=D(price), quantity=D(quantity),
            taker_fee=D(str(taker)) if taker is not None else None,
            maker_fee=D(str(maker)) if maker is not None else None,
            currency="USD", info={"schedule": schedule.model_dump()},
        )

    # -- combos ---------------------------------------------------------------

    async def create_combo(self, legs: list[ComboLeg]) -> Combo:
        """`POST /v1/combos`: the canonical combo for these legs. The same legs
        always give the same symbol, and returning an existing combo does not
        use the weekly quota of 1,000 new ones, so this is idempotent. Legs
        the venue cannot combine (incompatible, redundant, not tradable) are
        `InvalidOrder` with the venue's message."""
        if not 2 <= len(legs) <= MAX_COMBO_LEGS:
            raise InvalidOrder(f"polymarket_us: a combo has 2 to {MAX_COMBO_LEGS} legs, got {len(legs)}")
        wire = [leg_to_wire(leg) for leg in legs]
        if len({leg["symbol"] for leg in wire}) != len(wire):
            raise InvalidOrder("polymarket_us: a combo names each market once")
        try:
            raw = await self._rfq_call("POST", "/v1/combos", json={"legs": wire})
        except OrderRejected as exc:
            if exc.reason == "latency_stopgap":
                raise
            raise InvalidOrder(str(exc), body=exc.body, status=exc.status) from None
        found = (raw or {}).get("combo")
        if not found or not found.get("id"):
            raise ExchangeError("polymarket_us: create combo returned no combo", body=raw)
        return combo_of(found)

    async def fetch_combo(self, combo_id: str) -> Combo:
        """`GET /v1/combos?symbol=`, the exact symbol."""
        symbol = slug_of(combo_id)
        raw = await self._rfq_call("GET", "/v1/combos", params={"symbol": symbol})
        rows = (raw or {}).get("combos") or []
        if not rows:
            raise RfqNotFound(f"polymarket_us: no combo {symbol!r}")
        return combo_of(rows[0])

    async def lookup_combo(self, legs: list[ComboLeg]) -> Combo | None:
        raise NotSupported(
            "polymarket_us: no read-only lookup by legs; create_combo returns the existing combo for the same "
            "legs without using the weekly quota"
        )

    # -- RFQ, both sides ------------------------------------------------------

    async def fetch_rfq_user_id(self) -> str:
        """This participant's pseudonymous RFQ user id (`GET /v1/rfqs/user-id`),
        which marks its own RFQs and quotes. Read once and kept."""
        if self._rfq_user_id is None:
            raw = await self._rfq_call("GET", "/v1/rfqs/user-id")
            self._rfq_user_id = str((raw or {}).get("rfqUserId") or "") or None
        return self._rfq_user_id or ""

    async def _rfqs(self, params: dict[str, Any], *, mine: bool | None) -> Page[Rfq]:
        me = None if mine is not None else await self.fetch_rfq_user_id()
        raw = await self._rfq_call("GET", "/v1/rfqs", params={k: v for k, v in params.items() if v is not None})
        rows = tolerant((raw or {}).get("rfqs"), lambda row: rfq_of(row, me=me, mine=mine, account=self.account), "RFQ")
        return Page(rows, next_cursor=(raw or {}).get("cursor") or None)

    async def _quotes(self, params: dict[str, Any], *, mine: bool | None = None) -> list[RfqQuote]:
        me = None if mine is not None else await self.fetch_rfq_user_id()
        params = {k: v for k, v in params.items() if v is not None}
        quotes: list[RfqQuote] = []
        cursor: str | None = None
        while True:
            raw = await self._rfq_call("GET", "/v1/rfqs/quotes", params={**params, **({"cursor": cursor} if cursor else {})})
            quotes.extend(tolerant((raw or {}).get("quotes"),
                                   lambda row: quote_of(row, me=me, mine=mine, account=self.account), "quote"))
            cursor = (raw or {}).get("cursor") or None
            if not cursor:
                return quotes

    @staticmethod
    def _quote_status(status: QuoteStatus | str | None) -> str | None:
        if status is None:
            return None
        wire = QUOTE_STATUS_TO_WIRE.get(QuoteStatus(status))
        if wire is None:
            raise BadRequest(f"polymarket_us: quotes have no {QuoteStatus(status).value!r} status on this venue")
        return wire

    # -- RFQ, requester (taker) side ------------------------------------------

    async def create_rfq(
        self, instrument_id: str, *, quantity: Decimal | None = None, amount: Decimal | None = None,
        outcome: str | None = None, action: str | None = None, rest_remainder: bool = False,
        combo: Combo | None = None,
    ) -> Rfq:
        """`POST /v1/rfqs` on a combo or any open instrument. Two-sided: the
        venue takes no side, so `outcome`/`action` are refused. An `amount`
        is sent as `cashOrderQty` (USD, the combo taker fee included in it).
        Returns the RFQ as sent, `open`; the venue answers with its id only."""
        if outcome is not None or action is not None:
            raise InvalidOrder("polymarket_us: an RFQ is two-sided; pick the side when accepting a quote")
        body = translate_rfq(instrument_id, quantity=quantity, amount=amount, rest_remainder=rest_remainder)
        raw = await self._rfq_call("POST", "/v1/rfqs", json=body)
        rfq_id = str((raw or {}).get("rfqId") or "")
        if not rfq_id:
            raise ExchangeError("polymarket_us: create RFQ returned no id", body=raw)
        instrument = ids.qualify(VENUE, body["symbol"])
        on_combo = combo is not None or is_combo_symbol(body["symbol"])
        return Rfq(
            id=rfq_id, venue=VENUE, account=self.account,
            market_id=None if on_combo else instrument, combo_id=instrument if on_combo else None, combo=combo,
            quantity=D(quantity) if quantity is not None else None, amount=D(amount) if amount is not None else None,
            rest_remainder=rest_remainder, status=RfqStatus.OPEN, mine=True, created_at=int(time.time() * 1000),
            info={"request": body, "response": raw},
        )

    async def fetch_rfq(self, rfq_id: str) -> Rfq:
        page = await self._rfqs({"rfqId": rfq_id}, mine=None)
        if not page:
            raise RfqNotFound(f"polymarket_us: no RFQ {rfq_id}")
        return page[0]

    async def fetch_rfqs(
        self, *, status: RfqStatus | str | None = None, market_id: str | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """This participant's RFQs (`userFilter=USER_FILTER_SELF`)."""
        return await self._rfqs({
            "userFilter": "USER_FILTER_SELF", "status": RFQ_STATUS_TO_WIRE[RfqStatus(status)] if status else None,
            "symbol": slug_of(market_id) if market_id else None, "limit": limit, "cursor": cursor,
        }, mine=True)

    async def cancel_rfq(self, rfq_id: str) -> None:
        """`DELETE /v1/rfqs/{id}`. RFQs on this venue have no expiry, so an
        RFQ stays open until it is cancelled or a quote on it is accepted."""
        await self._rfq_call("DELETE", f"/v1/rfqs/{rfq_id}", priority=Priority.HIGH)

    async def fetch_quotes(self, rfq_id: str, *, status: QuoteStatus | str | None = None) -> list[RfqQuote]:
        """`GET /v1/rfqs/quotes?rfqId=`: every visible quote on our RFQ, or
        our own (with earlier replacements) on someone else's."""
        return await self._quotes({"rfqId": rfq_id, "status": self._quote_status(status), "limit": 100})

    async def fetch_quote(self, quote_id: str, *, rfq_id: str | None = None) -> RfqQuote:
        if not rfq_id:
            raise InvalidOrder("polymarket_us: a quote is read under its RFQ; give rfq_id")
        rows = [q for q in await self._quotes({"rfqId": rfq_id, "quoteId": quote_id}) if q.id == quote_id]
        if not rows:
            raise RfqNotFound(f"polymarket_us: no quote {quote_id} on RFQ {rfq_id}")
        return rows[0]

    async def accept_quote(self, accept: AcceptQuote) -> RfqQuote:
        """`PUT /v1/rfqs/{rfq}/quotes/{q}/accept {acceptedSide}`.

        The quote is read first. `acceptedSide` is the requester's side on
        the YES leg: buying YES (or selling NO) is `SIDE_BUY` at `buyPrice`,
        selling YES (or buying NO) is `SIDE_SELL` at `sellPrice`. Refused
        before anything is sent when that side was declined or is worse than
        `worst_price`, and when the quote is not active.

        A retry is safe: a quote already accepted on the same side is
        returned as read, without sending again, and the venue derives the
        requester's order id (`rfq1.`...) from the quote and side, so it
        cannot make a second order either. Returns at once with the quote
        `accepted`; `executed` later means the orders were submitted, not
        filled (`taker_order_id` is `rfqCreatorOrderId` then).
        """
        if not accept.rfq_id:
            raise InvalidOrder("polymarket_us: a quote is accepted under its RFQ; give rfq_id")
        if accept.quantity is not None or accept.amount is not None:
            raise InvalidOrder("polymarket_us: a quote is accepted for its full size; the venue takes no partial size")
        side = accept.side
        wire_side = SIDE_TO_WIRE[side]
        key = accept.client_order_id or accept_key(accept.quote_id)
        quote = await self.fetch_quote(accept.quote_id, rfq_id=accept.rfq_id)
        if quote.status in (QuoteStatus.ACCEPTED, QuoteStatus.CONFIRMED, QuoteStatus.EXECUTED):
            if quote.accepted_side == side:
                return quote.model_copy(update={"info": {**quote.info, "accept": {"acceptedSide": wire_side, "client_order_id": key, "resent": False}}})
            raise OrderRejected(
                f"polymarket_us: quote {quote.id} was already accepted on the {quote.accepted_side} side",
                reason="already_accepted", info=quote.info,
            )
        if quote.info.get("status") != "QUOTE_STATUS_ACTIVE":
            raise OrderRejected(
                f"polymarket_us: quote {quote.id} is {quote.info.get('status') or 'in no state'}, not active",
                reason="quote_not_active", info=quote.info,
            )
        price = quote.price_for(accept.outcome, accept.action)
        if price is None:
            raise InvalidOrder(f"polymarket_us: quote {quote.id} declines the {side.value} side")
        if accept.refuses(price):
            raise InvalidOrder(
                f"polymarket_us: quote {quote.id} {side.value}s YES at {price}, worse than the worst price "
                f"{accept.worst_price}"
            )
        body = {"acceptedSide": wire_side}
        raw = await self._rfq_call(
            "PUT", f"/v1/rfqs/{accept.rfq_id}/quotes/{accept.quote_id}/accept", json=body, priority=Priority.HIGH,
        )
        return quote.model_copy(update={
            "status": QuoteStatus.ACCEPTED, "accepted_side": side,
            "info": {**quote.info, "accept": {**body, "client_order_id": key, "response": raw, "resent": True}},
        })

    # -- RFQ, quoter (maker) side ---------------------------------------------

    async def fetch_incoming_rfqs(
        self, *, market_id: str | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """Open RFQs from other participants (ours are left out)."""
        page = await self._rfqs({
            "status": "RFQ_STATUS_OPEN", "symbol": slug_of(market_id) if market_id else None,
            "limit": limit, "cursor": cursor,
        }, mine=None)
        return Page([r for r in page if not r.mine], next_cursor=page.next_cursor)

    async def send_quote(self, quote: MakerQuote) -> RfqQuote:
        """`POST /v1/rfqs/quotes`, both sides at once: our `ask` is the
        venue's `buyPrice`, our `bid` its `sellPrice`, a missing side "0".
        Prices go as exact decimals; the venue checks them against the RFQ's
        `tickSize`. Returns the quote as sent, `open`, under the new id."""
        body = translate_quote(quote)
        raw = await self._rfq_call("POST", "/v1/rfqs/quotes", json=body)
        quote_id = str((raw or {}).get("quoteId") or "")
        if not quote_id:
            raise ExchangeError("polymarket_us: create quote returned no id", body=raw)
        return RfqQuote(
            id=quote_id, rfq_id=quote.rfq_id, venue=VENUE, account=quote.account or self.account,
            bid=quote.bid, ask=quote.ask, status=QuoteStatus.OPEN, mine=True, created_at=int(time.time() * 1000),
            info={"request": body, "response": raw, "client_order_id": quote.client_order_id},
        )

    async def replace_quote(self, quote_id: str, quote: MakerQuote) -> RfqQuote:
        """A new quote on the same RFQ. The venue replaces ours in place and
        deletes the old one (`quoteDeleted`); the result has a new id."""
        new = await self.send_quote(quote)
        return new.model_copy(update={"info": {**new.info, "replaces": quote_id}})

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None) -> None:
        """`DELETE /v1/rfqs/{rfq}/quotes/{q}`: withdraw a quote, or decline an
        accepted one during the last look."""
        if not rfq_id:
            raise InvalidOrder("polymarket_us: a quote is deleted under its RFQ; give rfq_id")
        await self._rfq_call("DELETE", f"/v1/rfqs/{rfq_id}/quotes/{quote_id}", priority=Priority.HIGH)

    async def confirm_quote(self, quote: RfqQuote) -> RfqQuote:
        """`PUT /v1/rfqs/{rfq}/quotes/{q}/confirm`, the maker's last look
        (3 s from `confirm_by`). Use the id the venue accepted
        (`quoteAccepted.quote.id`). The venue then submits the maker's order,
        then the requester's, at `execute_by`."""
        raw = await self._rfq_call(
            "PUT", f"/v1/rfqs/{quote.rfq_id}/quotes/{quote.id}/confirm", json={}, priority=Priority.HIGH,
        )
        return quote.model_copy(update={"status": QuoteStatus.CONFIRMED, "info": {**quote.info, "confirm": raw}})

    async def fetch_my_quotes(
        self, *, rfq_id: str | None = None, status: QuoteStatus | str | None = None,
    ) -> list[RfqQuote]:
        """Quotes this participant made: `userFilter=USER_FILTER_SELF`, or on
        one RFQ `rfqId` alone, which a maker reads as its own quotes there
        (replacements included). The venue takes one of the two filters."""
        scope = {"rfqId": rfq_id} if rfq_id else {"userFilter": "USER_FILTER_SELF"}
        return await self._quotes({**scope, "status": self._quote_status(status), "limit": 100}, mine=True)

    async def close(self) -> None:
        await self._http.aclose()
