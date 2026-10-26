"""Smarkets order entry: orders placed and cancelled, and the account's
orders, executions, positions and balance on the YES leg.

Built to Smarkets' trading API reference (docs.smarkets.com, the OpenAPI
document at `api.smarkets.com/v0/control/openapi/`); its records follow the
reference's examples. Every call carries the session token of an account
login (`SMARKETS_USERNAME`, `SMARKETS_PASSWORD`); see
`synpath.smarkets.SmarketsSession`. **Placing orders needs Smarkets' approved
API access** (applied for, with a one-off £150 fee): an ordinary account
reads its balance, orders and statement here, and is refused a new order
with `PermissionDenied`.

**An order buys or sells one contract's YES.** `buy` backs the contract
(takes its YES), `sell` lays it (takes its NO), and the price is the YES
price, as Smarkets' own `bids` (resting backs) are YES bids. Smarkets prices
in basis points of 100% (5000 = 50%), so there are no odds to invert.

**Quantities are pots.** An order's `quantity` is what the pot (back stake
plus lay liability) would be if it matched, in 1/10,000 of the account's
currency; a pot of 1 pays 1 to the winning side, so it is one contract. An
order for `n` contracts sends `n * 10,000`, rounded down. The minimum is a
back stake of £0.05 (or the currency equivalent), which Smarkets checks.

**Prices sit on an odds ladder.** Smarkets accepts only prices that are
decimal odds on the same bands as Betfair's (1.01 to 1000) turned into
basis points, plus the ends, 1 and 9999; every price in its public quotes
lies on it. A price within half a basis point of a tick is that tick; any
other is snapped onto the ladder on the cheaper side: a buy at the tick
below (it pays no more), a sell at the tick above (it receives no less).

**Order types.** A limit is `good_til_halted` by default (Smarkets' own
default): it rests through the market going in play and is cancelled when
the market halts (a goal, a red card). `params={"order_type":
"keep_in_play"}` (or `{"keep_in_play": True}`) keeps it through halts too;
`"good_til_cancelled"` cancels it when the market goes in play. Every
resting order is cancelled when a reduction factor is applied (a
withdrawn runner). A market order or an IOC limit is `immediate_or_cancel`
at the price given as the worst accepted; a FOK is the same with
`minimum_accepted_quantity` set to the whole order, so nothing matches
unless all of it can. GTD, DAY and post-only orders are not offered.

**No client order id; the label stands in.** Smarkets assigns the order id.
`client_order_id` (or `params["label"]`) is sent as the order's `label`, a
free string Smarkets keeps on the order and its statement rows, and is read
back as `client_order_id`. After a timeout, list the open orders and match
the label to find what was placed.

**Matched bets can be pending.** In play, a matched bet waits eight seconds
(longer after a goal or a red card) before it is confirmed and can be voided
in that time; the pending part is counted as filled and rides in `info`
(`quantity_filled_pending`).

**Positions** net per contract on the YES leg from the account's current
(unsettled) orders: backs matched are YES contracts, lays NO contracts.
Smarkets has no positions route.

**Fees** are commission on the account's net winnings per Smarkets market at
settlement (2% on the standard tier; the account's rate is `commission_type`
on `/v3/accounts/`).

**Rate limits.** 1200 requests per 60 s per account; placing and cancelling
one order 10 a second each; cancelling everything once a minute. A second
cancel-all inside the minute cancels the open orders one by one instead.

There is no account stream: Smarkets' socket feed is for approved market
makers and its trading API is HTTP only, so the engine polls the orders.
"""
from __future__ import annotations

import asyncio
import bisect
import time
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal
from typing import Any
from urllib.parse import parse_qs

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..betfair import CLASSIC_LADDER
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NetworkError, RateLimitExceeded
from ..smarkets import (
    API_URL, DEFAULT_COMMISSION, PRICE_SCALE, QUANTITY_SCALE, SmarketsSession, contract_native_id, error_type,
    fee_schedule_of, iso_ms, market_of, parse_contract_id, user_agent,
)
from ..types import Page
from .base import TradingExchange
from .credentials import SmarketsCredentials
from .errors import InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected, PermissionDenied
from .limiter import BudgetLimiter, Priority
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "smarkets"
ONE = Decimal("1")
SCALE = Decimal(PRICE_SCALE)
UNITS = Decimal(QUANTITY_SCALE)
PRICE_PLACES = Decimal("0.0000000001")

MAX_QUANTITY = 4_294_967_295
"""The largest `quantity` an order takes, in 1/10,000 of the currency."""

PER_PAGE = 100
"""Orders read per request when listing, Smarkets' maximum."""

ACTIVITY_PER_PAGE = 500
"""Statement rows read per request, Smarkets' maximum."""

MAX_PAGES = 50
"""Most pages one listing call reads."""

CANCEL_ALL_INTERVAL_S = 60.0
"""Smarkets allows one cancel-all request per 60 s."""

RESTING_TYPES = ("good_til_halted", "keep_in_play", "good_til_cancelled")
"""Smarkets' resting order types: cancelled when the market halts, kept
through halts, cancelled when the market goes in play."""

OPEN_STATES = ("created", "partial")
CURRENT_STATES = ("created", "partial", "filled")
"""Smarkets order states: unmatched, partly matched, fully matched, then
`settled` once the contract has its outcome."""

OUTCOMES = {"winner": "yes", "loser": "no", "deadheat": "dead_heat", "reduce": "void", "void": "void"}
"""A settled order's contract outcome as the YES result, as the read adapter
names them."""


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _ladder() -> tuple[int, ...]:
    ticks = {int((SCALE / odds).quantize(ONE, rounding=ROUND_HALF_EVEN)) for odds in CLASSIC_LADDER}
    return tuple(sorted(ticks | {1, PRICE_SCALE - 1}))


PRICE_LADDER: tuple[int, ...] = _ladder()
"""Every price Smarkets accepts, in basis points: the decimal odds 1.01 to
1000 in Betfair's bands, each as 10,000 / odds rounded to the nearest basis
point (half to even), with 1 and 9999 at the ends."""
_TICKS = frozenset(PRICE_LADDER)


def price_bps(side: Side, price: Any) -> int:
    """The ladder price in basis points for an order at this YES price: the
    tick it names (within half a basis point), else the next tick on the
    cheaper side, below for a buy and above for a sell."""
    p = D(price)
    if not 0 < p < 1:
        raise InvalidOrder(f"{VENUE}: a price is a YES probability strictly between 0 and 1, not {price}")
    raw = p * SCALE
    nearest = int(raw.quantize(ONE, rounding=ROUND_HALF_EVEN))
    if nearest in _TICKS:
        return nearest
    if side == Side.BUY:
        index = bisect.bisect_right(PRICE_LADDER, raw) - 1
        if index < 0:
            raise InvalidOrder(f"{VENUE}: {price} is below Smarkets' lowest price (0.0001)")
        return PRICE_LADDER[index]
    index = bisect.bisect_left(PRICE_LADDER, raw)
    if index >= len(PRICE_LADDER):
        raise InvalidOrder(f"{VENUE}: {price} is above Smarkets' highest price (0.9999)")
    return PRICE_LADDER[index]


def quantity_units(quantity: Any) -> int:
    """`quantity` contracts as Smarkets' pot, in 1/10,000 of the currency,
    rounded down."""
    units = int((D(quantity) * UNITS).to_integral_value(rounding=ROUND_DOWN))
    if units <= 0:
        raise InvalidOrder(f"{VENUE}: {quantity} contracts is less than Smarkets' smallest quantity (0.0001)")
    if units > MAX_QUANTITY:
        raise InvalidOrder(f"{VENUE}: {quantity} contracts is more than one order takes")
    return units


def yes_price(bps: Any) -> Decimal:
    """Basis points of 100% (possibly fractional) as the YES price."""
    return (D(bps) / SCALE).quantize(PRICE_PLACES).normalize()


def contracts(units: Any) -> Decimal:
    """A pot in 1/10,000 of the currency as contracts paying 1 each."""
    return D(units) / UNITS


def _units(row: dict[str, Any], name: str) -> Any:
    """A quantity in the account's currency where the row has one, else the
    GBP figure (the same thing on a GBP account)."""
    value = row.get(f"{name}_user_currency")
    return value if value is not None else row.get(name)


def _market(row: dict[str, Any]) -> str:
    return ids.qualify(VENUE, contract_native_id(row.get("market_id"), row.get("contract_id")))


def side_of(native: Any) -> Side:
    """`buy` backs the contract (YES); `sell` lays it (NO)."""
    return Side.SELL if str(native or "").lower() == "sell" else Side.BUY


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def filled_units(row: dict[str, Any]) -> Decimal:
    """What matched, in units: `quantity_filled`, plus the part still pending
    confirmation where Smarkets counts it apart (filled, pending and
    unfilled then add up to the order)."""
    filled, pending = D(_units(row, "quantity_filled")), D(_units(row, "quantity_filled_pending"))
    unfilled, total = D(_units(row, "quantity_unfilled")), D(_units(row, "quantity"))
    if pending and filled + pending + unfilled == total:
        filled += pending
    return filled


def order_of_row(row: dict[str, Any], *, account: Account | None = None) -> Order:
    """One order as `GET /v3/orders/` reports it.

    Smarkets has no cancelled state: an order in `created` or `partial` with
    nothing left unmatched was cancelled (or was an immediate order's
    unmatched rest), and a `settled` order that did not match in full had
    its rest cancelled before it settled."""
    quantity = contracts(_units(row, "quantity"))
    filled = contracts(filled_units(row))
    unfilled = contracts(_units(row, "quantity_unfilled"))
    state = str(row.get("state") or "")
    if state == "filled":
        status = OrderStatus.CLOSED
    elif state in OPEN_STATES or state == "settled":
        if state != "settled" and unfilled > 0:
            status = OrderStatus.OPEN
        else:
            status = OrderStatus.CLOSED if filled >= quantity else OrderStatus.CANCELED
    else:
        status = OrderStatus.PENDING
    native_type = str(row.get("type") or "")
    immediate = native_type in ("immediate_or_cancel", "over_the_counter", "over_the_counter_free")
    average = row.get("average_price_matched_precise") or row.get("average_price_matched")
    return Order(
        id=str(row.get("id") or ""),
        client_order_id=row.get("label") or None,
        venue=VENUE,
        account=account,
        market_id=_market(row),
        side=side_of(row.get("side")),
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.IOC if immediate else TimeInForce.GTC,
        status=status,
        price=yes_price(row["price"]) if row.get("price") else None,
        quantity=quantity,
        filled=filled,
        remaining=unfilled if status in (OrderStatus.OPEN, OrderStatus.PENDING) else Decimal("0"),
        average_price=yes_price(average) if filled and average and D(average) > 0 else None,
        created_at=iso_ms(row.get("created_datetime")),
        updated_at=iso_ms(row.get("last_modified_datetime")),
        info=row,
    )


def immediate(request: OrderRequest) -> bool:
    """Whether the order takes what is there and leaves nothing resting."""
    return request.type == OrderType.MARKET or request.time_in_force in (TimeInForce.IOC, TimeInForce.FOK)


def order_after_placing(request: OrderRequest, row: dict[str, Any], *, account: Account) -> Order:
    """The order from Smarkets' answer to `POST /v3/orders/`, with what the
    request carried that the answer does not. An immediate order's answer
    comes once it has matched what it could and been cancelled; any other
    once the exchange accepted it."""
    quantity = contracts(row.get("quantity"))
    executed = contracts(row.get("total_executed_quantity"))
    if executed >= quantity > 0:
        status, remaining = OrderStatus.CLOSED, Decimal("0")
    elif immediate(request):
        status, remaining = OrderStatus.CANCELED, Decimal("0")
    else:
        status = OrderStatus.OPEN
        left = row.get("available_quantity")
        remaining = contracts(left) if left is not None else quantity - executed
    average = row.get("executed_avg_price")
    now = int(time.time() * 1000)
    return Order(
        id=str(row.get("order_id") or ""),
        client_order_id=request.client_order_id,
        venue=VENUE,
        account=request.account or account,
        market_id=_market(row) if row.get("market_id") else ids.qualify(VENUE, ids.native(VENUE, request.market_id)),
        side=side_of(row.get("side")) if row.get("side") else request.side,
        type=request.type,
        time_in_force=request.time_in_force,
        status=status,
        price=yes_price(row["price"]) if row.get("price") else None,
        quantity=quantity,
        filled=executed,
        remaining=remaining,
        average_price=yes_price(average) if executed and average else None,
        created_at=now,
        updated_at=now,
        book=request.book,
        trader=request.trader,
        tags=request.tags,
        info=row,
    )


def fill_of_activity(row: dict[str, Any], *, account: Account | None = None) -> Fill:
    """One `order.execute` statement row as a fill: its price on the YES
    leg, its pot in contracts."""
    units = _units(row, "quantity")
    return Fill(
        id=f"{row.get('seq')}-{row.get('subseq')}",
        order_id=str(row.get("order_id") or ""),
        client_order_id=row.get("label") or None,
        venue=VENUE,
        account=account,
        market_id=_market(row),
        side=side_of(row.get("side")),
        price=yes_price(row.get("price")),
        quantity=abs(contracts(units)),
        fee=D(row["commission"]) if row.get("commission") is not None else None,
        liquidity=Liquidity.UNKNOWN,
        timestamp=iso_ms(row.get("timestamp")) or 0,
        info=row,
    )


def _matched(row: dict[str, Any]) -> tuple[Decimal, Decimal]:
    """(contracts matched, their average YES price) of one order."""
    filled = contracts(filled_units(row))
    average = row.get("average_price_matched_precise") or row.get("average_price_matched") or row.get("price")
    return filled, (D(average) / SCALE if average else Decimal("0"))


def positions_of_orders(rows: list[dict[str, Any]], *, account: Account | None = None) -> list[Position]:
    """Unsettled orders netted per contract on the YES leg: matched backs are
    YES contracts, matched lays NO contracts. `margin` is what the contract
    loses at worst: what was paid (a back's stake, a lay's liability) less
    what the losing side's bets return."""
    per: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("state") or "") == "settled":
            continue
        filled, price = _matched(row)
        if filled <= 0:
            continue
        entry = per.setdefault(_market(row), {"yes": Decimal("0"), "no": Decimal("0"), "cost": Decimal("0"),
                                              "traded": Decimal("0"), "orders": []})
        if side_of(row.get("side")) == Side.BUY:
            entry["yes"] += filled
            entry["cost"] += filled * price
        else:
            entry["no"] += filled
            entry["cost"] += filled * (ONE - price)
        entry["traded"] += filled * price
        entry["orders"].append(row)
    out = []
    for market_id, entry in per.items():
        yes, no, cost = entry["yes"], entry["no"], entry["cost"]
        net = yes - no
        worst = max(cost - yes, cost - no)
        out.append(Position(
            venue=VENUE,
            account=account,
            market_id=market_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net),
            inventory_yes=yes,
            inventory_no=no,
            # The average YES price of every matched bet, backs and lays.
            entry_price=(entry["traded"] / (yes + no)).quantize(PRICE_PLACES),
            margin=max(Decimal("0"), worst),
            info={"orders": entry["orders"]},
        ))
    return out


def settlements_of_orders(rows: list[dict[str, Any]], *, account: Account | None = None) -> list[Settlement]:
    """Settled orders, one settlement per contract and side the account
    matched. `payout` and `pnl` are before commission, which Smarkets takes
    on the net winnings of the whole Smarkets market."""
    groups: dict[tuple[str, Side], list[dict[str, Any]]] = {}
    for row in rows:
        if str(row.get("state") or "") != "settled":
            continue
        if contracts(filled_units(row)) <= 0:
            continue
        groups.setdefault((_market(row), side_of(row.get("side"))), []).append(row)
    out = []
    for (market_id, side), group in groups.items():
        quantity, cost = Decimal("0"), Decimal("0")
        for row in group:
            filled, price = _matched(row)
            quantity += filled
            cost += filled * (price if side == Side.BUY else ONE - price)
        outcomes = {str(row.get("outcome") or "") for row in group} - {""}
        result = OUTCOMES.get(outcomes.pop()) if len(outcomes) == 1 else None
        won: bool | None = None
        payout: Decimal | None = None
        if result in ("yes", "no"):
            won = (result == "yes") == (side == Side.BUY)
            payout = quantity if won else Decimal("0")
        elif result == "void":
            payout = cost
        stamps = [iso_ms(row.get("last_modified_datetime")) for row in group]
        out.append(Settlement(
            venue=VENUE,
            account=account,
            market_id=market_id,
            held=PositionSide.LONG if side == Side.BUY else PositionSide.SHORT,
            result=result,
            won=won,
            quantity=quantity,
            cost=cost,
            payout=payout,
            pnl=payout - cost if payout is not None else None,
            timestamp=max((s for s in stamps if s), default=None),
            info={"orders": group, "pnl": "before commission"},
        ))
    return out


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

def error_of(exc: ExchangeError) -> ExchangeError:
    """Smarkets' refusal of an order as a typed error, by its `error_type`."""
    code = error_type(exc.body) or ""
    text = f"{VENUE}: order refused: {code or exc}"
    if "INSUFFICIENT_FUNDS" in code:
        return InsufficientFunds(text, body=exc.body, status=exc.status)
    if any(word in code for word in ("MARKET_HALTED", "TRADING_SUSPENDED", "MARKET_NOT_OPEN", "CONTRACT_SETTLED",
                                     "MARKET_SETTLED")):
        return MarketHalted(text, body=exc.body, status=exc.status)
    if "INVALID_PRICE" in code or "INVALID_QUANTITY" in code or code in (
            "REQUEST_VALIDATION_ERROR", "ORDER_REJECTED_CONTRACT_NOT_FOUND", "ORDER_REJECTED_MARKET_NOT_FOUND"):
        return InvalidOrder(text)
    return OrderRejected(text, reason=code or None, info=exc.body if isinstance(exc.body, dict) else {},
                         body=exc.body, status=exc.status)


def _next(next_page: Any, key: str) -> str | None:
    """A paging value from Smarkets' `next_page` query string."""
    if not next_page:
        return None
    values = parse_qs(str(next_page).lstrip("?")).get(key)
    return values[-1] if values else None


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: Smarkets orders have no expiry; gtc, ioc or fok only")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.post_only:
        raise InvalidOrder(f"{VENUE}: Smarkets' API has no post-only order")


def venue_type(request: OrderRequest) -> str:
    """Smarkets' order type for a request; see the module docstring."""
    if immediate(request):
        return "immediate_or_cancel"
    chosen = request.params.get("order_type")
    if chosen is None:
        chosen = "keep_in_play" if request.params.get("keep_in_play") else "good_til_halted"
    if chosen not in RESTING_TYPES:
        raise InvalidOrder(f"{VENUE}: order_type is one of {', '.join(RESTING_TYPES)}, not {chosen!r}")
    return str(chosen)


def build_order(request: OrderRequest) -> dict[str, Any]:
    """One request as Smarkets' `POST /v3/orders/` body, before anything is sent."""
    check_request(request)
    market_id, contract_id = parse_contract_id(ids.native(VENUE, request.market_id))
    units = quantity_units(request.quantity)
    body: dict[str, Any] = {
        "market_id": market_id,
        "contract_id": contract_id,
        "side": "buy" if request.side == Side.BUY else "sell",
        "price": price_bps(request.side, request.price),
        "quantity": units,
        "type": venue_type(request),
    }
    if request.time_in_force == TimeInForce.FOK:
        body["minimum_accepted_quantity"] = units
    elif immediate(request) and request.params.get("minimum_quantity") is not None:
        body["minimum_accepted_quantity"] = min(units, quantity_units(request.params["minimum_quantity"]))
    label = request.params.get("label") or request.client_order_id
    if label:
        body["label"] = str(label)
    return body


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class SmarketsTrading(TradingExchange):
    """Smarkets order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.smarkets import SmarketsTrading

    async with SmarketsTrading(require("smarkets", load_credentials())) as smk:
        print(await smk.fetch_balance())
    ```
    """

    id = VENUE
    name = "Smarkets"
    has: dict[str, Capability] = {
        "create_order": True,
        # One request per order; not atomic.
        "create_orders": True,
        "cancel_order": True,
        # One request per order.
        "cancel_orders": True,
        # Everywhere or per Smarkets market, once a minute; per contract by
        # listing its open orders and cancelling each.
        "cancel_all_orders": True,
        # No amend route: cancel and replace.
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # Smarkets keeps current and recently settled orders here.
        "fetch_orders": "partial",
        # From the account statement's executions.
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        # Recently settled orders only.
        "fetch_settlements": "partial",
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        "rfq": False,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: SmarketsCredentials | None = None,
        *,
        session: SmarketsSession | None = None,
        session_token: str | None = None,
        account_name: str = "default",
        url: str = API_URL,
        commission: float | None = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        session_client: Any = None,
    ):
        """`credentials` log in on the first call; `session_token` reuses a
        login made elsewhere (an account with two-factor login needs that).
        `commission` overrides the rate fee estimates use, which is otherwise
        the account's own (`commission_type`)."""
        import httpx

        if session is None:
            if credentials is None and not session_token:
                from .errors import CredentialsMissing

                raise CredentialsMissing(f"{VENUE}: set SMARKETS_USERNAME and SMARKETS_PASSWORD, or pass session_token=")
            session = SmarketsSession(
                username=credentials.username if credentials else None,
                password=credentials.password if credentials else None,
                session_token=session_token, client=session_client, timeout=timeout, base_url=url,
            )
        self.credentials = credentials
        self.session = session
        self.account = Account(venue=VENUE, name=account_name)
        # 1200 requests a minute an account; placing and cancelling 10 a second each.
        self.limiter = limiter or BudgetLimiter(read_per_second=8, write_per_second=10)
        self.url = url
        self.commission = commission
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        shared.headers["User-Agent"] = user_agent()
        shared.headers["Accept"] = "application/json"
        self.http = AsyncHttpClient(url, limiter=None, client=shared, venue=VENUE)
        # Cancel-all is not retried on a 429: the open orders are cancelled
        # one by one instead.
        self._cancel_all_http = AsyncHttpClient(url, limiter=None, client=shared, venue=VENUE, attempts=1)
        self._last_cancel_all: float | None = None
        self._account_row: dict[str, Any] | None = None

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL, http: AsyncHttpClient | None = None,
    ) -> Any:
        """One call with the session token, paced; a 401 logs in again once."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = {k: v for k, v in (params or {}).items() if v is not None and v != []}
        for attempt in range(2):
            token = await asyncio.to_thread(self.session.token)
            try:
                return await (http or self.http).request(
                    method, path, params=query or None, json=body, headers={"Authorization": f"Session-Token {token}"},
                )
            except AuthenticationError as exc:
                if exc.status == 401 and attempt == 0:
                    self.session.invalidate()
                    continue
                if exc.status == 403:
                    code = error_type(exc.body)
                    if code is None:
                        raise PermissionDenied(f"{VENUE}: refused by Smarkets' Cloudflare front (403)",
                                               body=exc.body, status=403) from None
                    hint = ("; placing orders needs Smarkets' approved API access" if code == "FORBIDDEN"
                            and path.startswith("/v3/orders/") else "")
                    raise PermissionDenied(f"{VENUE}: refused: {code}{hint}", body=exc.body, status=403) from None
                raise
        raise AssertionError("unreachable")  # pragma: no cover

    async def account_info(self) -> dict[str, Any]:
        """The account (`GET /v3/accounts/`): its currency, balance, exposure
        and commission rate. The latest read is kept for the currency and the
        rate."""
        raw = await self._call("GET", "/v3/accounts/") or {}
        self._account_row = dict(raw.get("account") or {})
        return self._account_row

    async def currency(self) -> str:
        """The account's currency, read once: every quantity and amount is in it."""
        if self._account_row is None:
            await self.account_info()
        return str((self._account_row or {}).get("currency") or "GBP")

    async def commission_rate(self) -> float:
        """The commission rate fee estimates use: `commission` if given, else
        the account's `commission_type`, else the standard 2%."""
        if self.commission is not None:
            return float(self.commission)
        if self._account_row is None:
            await self.account_info()
        try:
            return float((self._account_row or {}).get("commission_type") or DEFAULT_COMMISSION)
        except ValueError:
            return DEFAULT_COMMISSION

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Place one order. An immediate one answers once it has matched what
        it could; a resting one once the exchange accepted it, so a fill
        straight after shows on `fetch_order`."""
        body = build_order(request)
        try:
            row = await self._call("POST", "/v3/orders/", body=body, kind="write")
        except BadRequest as exc:
            raise error_of(exc) from None
        if not isinstance(row, dict) or not row.get("order_id"):
            raise OrderRejected(f"{VENUE}: the order was answered without an id", info=row if isinstance(row, dict) else {})
        return order_after_placing(request, row, account=self.account)

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, one request each (Smarkets has no batch route), one
        result each in the order asked for. A refused order comes back as its
        exception."""
        out: list[Order | Exception] = []
        for request in requests:
            try:
                out.append(await self.create_order(request))
            except (ExchangeError, NetworkError) as exc:
                out.append(exc)
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back. One Smarkets refuses to cancel
        (already matched, too late) is read back as it stands; a cancel the
        exchange has not applied yet reads `pending_cancel`."""
        try:
            await self._call("DELETE", f"/v3/orders/{order_id}/", kind="write", priority=Priority.HIGH)
        except MarketNotFound as exc:
            raise OrderNotFound(f"{VENUE}: no order {order_id}", body=exc.body, status=exc.status) from None
        except BadRequest as exc:
            order = await self.fetch_order(order_id)
            if order.is_terminal:
                return order
            raise OrderRejected(f"{VENUE}: cancel of {order_id} refused: {error_type(exc.body) or exc}",
                                reason=error_type(exc.body) or "cancel_refused", body=exc.body) from None
        order = await self.fetch_order(order_id)
        if order.status == OrderStatus.OPEN:
            return order.model_copy(update={"status": OrderStatus.PENDING_CANCEL})
        return order

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel each order, one request each, one result each."""
        out: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                out.append(await self.cancel_order(order_id))
            except (ExchangeError, NetworkError) as exc:
                out.append(exc)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int | None:
        """Every open order, on a Smarkets market (`smarkets:<market id>`), on
        one contract, or on the account.

        The account or a Smarkets market is one request, which Smarkets
        acknowledges without a count (`None`). It allows one a minute: a
        second inside the minute, a refused one (429), and a contract (which
        cancel-all cannot name) cancel the open orders one by one, returning
        how many were cancelled."""
        native = ids.native(VENUE, market_id) if market_id else None
        whole_market = native is None or "-" not in native
        recent = self._last_cancel_all is not None and time.monotonic() - self._last_cancel_all < CANCEL_ALL_INTERVAL_S
        if whole_market and not recent:
            self._last_cancel_all = time.monotonic()
            try:
                await self._call("DELETE", "/v3/orders/", params={"market_id": market_of(native) if native else None},
                                 kind="write", priority=Priority.HIGH, http=self._cancel_all_http)
                return None
            except RateLimitExceeded:
                pass
        orders = await self.fetch_open_orders(market_id=market_id)
        results = await self.cancel_orders([o.id for o in orders])
        return sum(1 for r in results if isinstance(r, Order))

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id, in any state Smarkets still lists."""
        raw = await self._call("GET", "/v3/orders/", params={"id": [str(order_id)]}) or {}
        rows = [r for r in raw.get("orders") or [] if isinstance(r, dict) and str(r.get("id")) == str(order_id)]
        if rows:
            return order_of_row(rows[0], account=self.account)
        try:
            row = await self._call("GET", f"/v3/orders/{order_id}/")
        except MarketNotFound as exc:
            raise OrderNotFound(f"{VENUE}: no order {order_id}", body=exc.body, status=exc.status) from None
        if not isinstance(row, dict) or not row.get("id"):
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return order_of_row(row, account=self.account)

    def _scope(self, market_id: str | None) -> dict[str, Any]:
        """Smarkets' filter for a contract (`<market>-<contract>`) or a
        Smarkets market (a bare market id, the Synpath event)."""
        if not market_id:
            return {}
        native = ids.native(VENUE, market_id)
        if "-" in native:
            market, contract = parse_contract_id(native)
            return {"market_id": [market], "contract_id": contract}
        return {"market_id": [market_of(native)]}

    async def _orders(self, params: dict[str, Any], *, after: str | None = None,
                      pages: int = MAX_PAGES, limit: int = PER_PAGE) -> tuple[list[dict[str, Any]], str | None]:
        """Order rows by id from `after`, page by page; and the id to resume
        after, when more are left."""
        rows: list[dict[str, Any]] = []
        for _ in range(pages):
            raw = await self._call("GET", "/v3/orders/", params={
                **params, "limit": limit, "sort": "id", "pagination_last_id": after,
                "include_promotion_data": "false",
            }) or {}
            page = [r for r in raw.get("orders") or [] if isinstance(r, dict)]
            rows += page
            after = _next((raw.get("pagination") or {}).get("next_page"), "pagination_last_id")
            if not page or not after:
                return rows, None
        return rows, after

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting order, or every one on a contract or Smarkets market."""
        rows, _ = await self._orders({"state": list(OPEN_STATES), **self._scope(market_id)})
        orders = [order_of_row(row, account=self.account) for row in rows]
        return [o for o in orders if o.status == OrderStatus.OPEN]

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of the orders Smarkets lists (current and recently
        settled), oldest first: `open`, `closed` (fully matched) or
        `canceled` (ended with something unmatched). `since` keeps orders
        created since then."""
        native = {"open": list(OPEN_STATES), "closed": ["filled", "settled"], "canceled": list(OPEN_STATES) + ["settled"]}
        if status is not None and status not in native:
            raise BadRequest(f"{VENUE}: status is open, closed or canceled")
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        params: dict[str, Any] = {"state": native.get(status or ""), **self._scope(market_id)}
        if since is not None:
            from ..types import iso

            params["created_datetime_min"] = iso(since)
        rows, following = await self._orders(params, after=cursor, pages=1, limit=min(limit or PER_PAGE, PER_PAGE))
        orders = [order_of_row(row, account=self.account) for row in rows]
        wanted = {"open": OrderStatus.OPEN, "closed": OrderStatus.CLOSED, "canceled": OrderStatus.CANCELED}.get(status or "")
        if wanted is not None:
            orders = [o for o in orders if o.status == wanted]
        return Page(orders, next_cursor=following)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """The account's executions from its statement (`order.execute`
        rows), newest first."""
        params: dict[str, Any] = {"source": ["order.execute"], "sort": "-seq,-subseq",
                                  "limit": min(limit or ACTIVITY_PER_PAGE, ACTIVITY_PER_PAGE),
                                  "include_promotion_data": "false"}
        wanted_contract = None
        if market_id:
            native = ids.native(VENUE, market_id)
            params["market_id"] = [market_of(native)]
            if "-" in native:
                wanted_contract = parse_contract_id(native)[1]
        if order_id:
            params["order_id"] = [str(order_id)]
        if since is not None:
            from ..types import iso

            params["timestamp_min"] = iso(since)
        if cursor:
            seq, sep, subseq = cursor.partition(":")
            if not sep or not seq.isdigit() or not subseq.isdigit():
                raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
            params["pagination_last_seq"], params["pagination_last_subseq"] = seq, subseq
        raw = await self._call("GET", "/v3/accounts/activity/", params=params) or {}
        rows = [r for r in raw.get("account_activity") or [] if isinstance(r, dict)
                and r.get("source") == "order.execute" and r.get("contract_id")]
        if wanted_contract:
            rows = [r for r in rows if str(r.get("contract_id")) == wanted_contract]
        fills = [fill_of_activity(row, account=self.account) for row in rows]
        currency = await self.currency()
        fills = [f.model_copy(update={"fee_currency": currency}) if f.fee is not None else f for f in fills]
        next_page = (raw.get("pagination") or {}).get("next_page")
        seq = _next(next_page, "pagination_last_seq")
        subseq = _next(next_page, "pagination_last_subseq")
        last = raw.get("account_activity") or []
        if next_page and (seq is None or subseq is None) and last:
            seq, subseq = str(last[-1].get("seq")), str(last[-1].get("subseq"))
        return Page(fills, next_cursor=f"{seq}:{subseq}" if next_page and seq and subseq else None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """Cash (`balance`), what bets hold (`exposure`, `locked`), and what
        is free to bet (`available_balance`)."""
        row = await self.account_info()
        available = D(row.get("available_balance"))
        return Balance(
            venue=VENUE, account=account or self.account, currency=str(row.get("currency") or "GBP"),
            total=D(row.get("balance")), available=available, locked=abs(D(row.get("exposure"))),
            buying_power=available, timestamp=int(time.time() * 1000), info=row,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """The account's matched, unsettled bets netted per contract on the
        YES leg. `market_id` is a contract, `event_id` a Smarkets market."""
        rows, _ = await self._orders({"state": list(CURRENT_STATES), **self._scope(market_id or event_id)})
        positions = positions_of_orders(rows, account=self.account)
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            positions = [p for p in positions if p.market_id == wanted]
        return positions

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        """One page of recently settled orders, one settlement per contract
        and side. `since` keeps orders created since then."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        params: dict[str, Any] = {"state": ["settled"], **self._scope(market_id)}
        if since is not None:
            from ..types import iso

            params["created_datetime_min"] = iso(since)
        rows, following = await self._orders(params, after=cursor, pages=1)
        settlements = settlements_of_orders(rows, account=self.account)
        return Page(settlements[:limit] if limit else settlements, next_cursor=following)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The commission on `quantity` contracts at the YES `price` if the bet
        wins: a share of its profit. A `sell` lays the contract, which wins
        `price` a contract when it loses. Makers pay the same."""
        native = ids.native(VENUE, market_id)
        schedule = fee_schedule_of(market_of(native), await self.commission_rate())
        paid = D(price) if side == Side.BUY else ONE - D(price)
        fee = schedule.estimate(float(paid), float(quantity), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, native), side=side, price=D(price), quantity=D(quantity),
            taker_fee=D(fee) if fee is not None else None, maker_fee=D(fee) if fee is not None else None,
            currency=await self.currency(),
            info={"schedule": schedule.model_dump(), "charged": "at settlement, on net winnings in the Smarkets market"},
        )

    async def close(self) -> None:
        await self.http.close()
        self.session.close()


__all__ = [
    "SmarketsTrading", "PRICE_LADDER", "build_order", "price_bps", "quantity_units", "order_of_row",
    "order_after_placing", "fill_of_activity", "positions_of_orders", "settlements_of_orders",
]
