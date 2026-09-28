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

**No client order id, no reduce-only.** The venue assigns order ids; a
`client_order_id` is kept on the returned `Order` for the caller's own
records and is not sent. Reduce-only is refused: the venue offers
close-position instead.
"""
from __future__ import annotations

import base64
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
from .errors import InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .money import D, complement, validate_amount, validate_price
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Precision, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "polymarket_us"
API_URL = "https://api.polymarket.us"
GATEWAY_URL = "https://gateway.polymarket.us"

BATCH = 20
PRICE_FLOOR = Decimal("0.01")
PRICE_CEILING = Decimal("0.99")
"""The exchange's absolute price limits. A price outside them still gets an
order id and is then rejected, so it is refused here first."""

MARKET_TTL_S = 300.0


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
            tick=self.tick, min_amount=self.min_quantity, amount_step=self.min_quantity, whole_contracts=whole,
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
    if request.reduce_only:
        raise InvalidOrder("polymarket_us: the venue has no reduce-only flag; use close_position")
    slug = slug_of(request.market_id)
    wire_price = check_prices(D(request.price), rules)
    quantity = validate_amount(D(request.amount), rules.precision)
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
    amount = D(str(raw.get("quantity") or 0))
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
        amount=amount,
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
        price=D(request.price) if request.price is not None else None, amount=D(request.amount),
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
        amount=abs(net) if held else None,
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
        # RFQ on this venue quotes combos, which synpath does not model yet.
        "rfq": False,
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
    ):
        import httpx

        self.credentials = credentials
        self.signer = PolymarketUSSigner(credentials.key_id, credentials.secret_key)
        self.account = Account(venue=VENUE, name=account_name)
        # Twenty requests a second per key, shared by every endpoint: one lane.
        self.limiter = limiter or BudgetLimiter(read_per_second=20, write_per_second=20, burst_seconds=1)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.api = AsyncHttpClient(api_url, limiter=None, client=shared, venue=VENUE)
        self.gateway = AsyncHttpClient(gateway_url, limiter=None, client=shared, venue=VENUE)
        self._http = shared
        self._markets: dict[str, MarketRules] = {}

    async def _call(
        self, method: str, path: str, *, params: Any = None, json: Any = None, priority: Priority = Priority.NORMAL,
    ) -> Any:
        await self.limiter.acquire(cost=1, kind="write", priority=priority)
        headers = self.signer.headers(method, path)
        try:
            return await self.api.request(
                method, path, params=params or None, json=json, headers=headers,
            )
        except ExchangeError as exc:
            raise error_of(exc) from None

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
        quantity = validate_amount(D(request.amount), rules.precision) if request.amount is not None else current.amount
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
            time_in_force=TimeInForce.IOC, status=OrderStatus.PENDING, amount=Decimal("0"),
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

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The market's published taker theta and the venue's maker rebate at
        the YES `price` for `amount` contracts. A negative maker fee is a rebate."""
        slug = slug_of(market_id)
        rules = await self.market_rules(slug)
        schedule = fee_schedule_of(rules.raw)
        if schedule is None:
            raise NotSupported(f"polymarket_us: market {slug} publishes no fee coefficient")
        wire = float(D(price))
        taker = schedule.estimate(wire, float(amount), taker=True)
        maker = schedule.estimate(wire, float(amount), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, slug), side=side, price=D(price), amount=D(amount),
            taker_fee=D(str(taker)) if taker is not None else None,
            maker_fee=D(str(maker)) if maker is not None else None,
            currency="USD", info={"schedule": schedule.model_dump()},
        )

    async def close(self) -> None:
        await self._http.aclose()
