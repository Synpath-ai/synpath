"""Polymarket US order entry, on the exchange API for onboarded firms.

`api.{preprod,prod}.polymarketexchange.com`, reached with an Auth0
private-key JWT exchanged for a three-minute access token and a participant
id issued at onboarding. The retail API most accounts use is
`polymarket_us`; this is the one a firm gets after onboarding, with a
preprod environment, firm-wide rate limits and native stop orders.

What differs from the retail API, and shapes this adapter:

**Integers on the wire.** Prices and quantities are int64s scaled per
instrument (`priceScale`, `fractionalQtyScale` from reference data). The
scales are read once per symbol and cached: reference data is limited to six
requests a minute firm-wide.

**The YES leg only.** Orders carry a side and a YES price, nothing more.
Buying NO at `q` is sent as selling YES at `1 - q` and reads back that way,
as on Kalshi.

**Stops are held by the exchange.** `ORDER_TYPE_STOP` and
`ORDER_TYPE_STOP_LIMIT` are in the published order schema, so a stop-market
or stop-limit request is sent as one; a NO stop is the YES stop at `1 - s`
on the other side, which triggers on the same event. Trailing stops and the
rest remain the engine's.

**Reads are rationed.** Open orders are cheap; searching orders or
executions is twelve requests a minute firm-wide, and each has its own
budget here so a reconciliation loop cannot starve order entry.

**Entry is asynchronous.** An insert returns an order id; acceptance,
fills and rejection follow. Cancels and replaces return nothing. Results are
`pending`, `pending_cancel` or `pending_replace` unless read back.
"""
from __future__ import annotations

import asyncio
import time
import uuid
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
from .credentials import PolymarketUSExchangeCredentials
from .errors import (
    CredentialsMissing, DuplicateClientOrderId, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound,
    OrderRejected, PermissionDenied,
)
from .limiter import BudgetLimiter, Priority
from .money import D, validate_amount, validate_price
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType,
    Position, PositionSide, Precision, Side, TimeInForce,
)

VENUE = "polymarket_us"

ENVIRONMENTS = {
    "preprod": ("https://api.preprod.polymarketexchange.com", "pmx-preprod.us.auth0.com"),
    "prod": ("https://api.prod.polymarketexchange.com", "pmx-prod.us.auth0.com"),
}
GATEWAY_URL = "https://gateway.polymarket.us"

BATCH = 20
TOKEN_MARGIN_S = 30
"""Refresh the access token this long before the venue says it expires."""

NATIVE_ORDER_TYPES = frozenset({OrderType.LIMIT, OrderType.MARKET, OrderType.STOP_MARKET, OrderType.STOP_LIMIT})

TIF_TO_WIRE = {
    TimeInForce.GTC: "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    TimeInForce.GTD: "TIME_IN_FORCE_GOOD_TILL_TIME",
    TimeInForce.IOC: "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
    TimeInForce.FOK: "TIME_IN_FORCE_FILL_OR_KILL",
}
TIF_FROM_WIRE = {wire: tif for tif, wire in TIF_TO_WIRE.items()} | {"TIME_IN_FORCE_DAY": TimeInForce.DAY}

TYPE_TO_WIRE = {
    OrderType.LIMIT: "ORDER_TYPE_LIMIT",
    OrderType.MARKET: "ORDER_TYPE_LIMIT",
    OrderType.STOP_MARKET: "ORDER_TYPE_STOP",
    OrderType.STOP_LIMIT: "ORDER_TYPE_STOP_LIMIT",
}
TYPE_FROM_WIRE = {
    "ORDER_TYPE_LIMIT": OrderType.LIMIT,
    "ORDER_TYPE_MARKET_TO_LIMIT": OrderType.MARKET,
    "ORDER_TYPE_STOP": OrderType.STOP_MARKET,
    "ORDER_TYPE_STOP_LIMIT": OrderType.STOP_LIMIT,
}

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


# ---------------------------------------------------------------------------
# Instruments and scaling
# ---------------------------------------------------------------------------

@dataclass
class InstrumentScale:
    """How one symbol's integers map to prices and contracts."""

    symbol: str
    price_scale: int
    qty_scale: int
    tick: Decimal
    min_quantity: Decimal
    state: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def precision(self) -> Precision:
        whole = self.min_quantity >= 1 and self.min_quantity % 1 == 0
        return Precision(tick=self.tick, min_amount=self.min_quantity, amount_step=self.min_quantity, whole_contracts=whole)

    @classmethod
    def from_instrument(cls, raw: dict[str, Any]) -> "InstrumentScale":
        price_scale = int(raw.get("priceScale") or 1)
        qty_scale = int(raw.get("fractionalQtyScale") or 1) or 1
        tick = D(str(raw.get("tickSize"))) if raw.get("tickSize") else Decimal(1) / price_scale
        if tick >= 1:
            # A dollar tick of 1 or more cannot exist on a $1 contract, so a
            # tick that large is in scaled units.
            tick = tick / price_scale
        minimum = D(str(raw.get("minimumTradeQty") or qty_scale)) / qty_scale
        return cls(
            symbol=str(raw.get("symbol") or ""), price_scale=price_scale, qty_scale=qty_scale,
            tick=tick, min_quantity=minimum or Decimal(1), state=str(raw.get("state") or ""), raw=raw,
        )

    def price_to_wire(self, price: Decimal) -> str:
        scaled = price * self.price_scale
        if scaled != scaled.to_integral_value():
            raise InvalidOrder(f"polymarket_us: price {price} is finer than {self.symbol}'s price scale {self.price_scale}")
        return str(int(scaled))

    def qty_to_wire(self, quantity: Decimal) -> str:
        scaled = quantity * self.qty_scale
        if scaled != scaled.to_integral_value():
            raise InvalidOrder(f"polymarket_us: quantity {quantity} is finer than {self.symbol}'s quantity scale {self.qty_scale}")
        return str(int(scaled))

    def price_from_wire(self, value: Any) -> Decimal | None:
        if value in (None, "", "0", 0):
            return None
        return D(str(value)) / self.price_scale

    def qty_from_wire(self, value: Any) -> Decimal:
        return D(str(value or 0)) / self.qty_scale


# ---------------------------------------------------------------------------
# Pure translation
# ---------------------------------------------------------------------------

def symbol_of(market_id: str) -> str:
    """The exchange symbol (the market slug) behind a Synpath market id or a
    bare slug. Another venue's id is refused before anything is signed."""
    try:
        return ids.native(VENUE, market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None


def rfc3339(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def wire_side(side: Side) -> str:
    """Orders are already on the YES leg, which is what the exchange trades."""
    return "SIDE_BUY" if side == Side.BUY else "SIDE_SELL"


def translate_order(request: OrderRequest, scale: InstrumentScale, *, account: str) -> dict[str, Any]:
    """An `OrderRequest` as an `InsertOrderRequest`, on the YES leg."""
    if request.type not in NATIVE_ORDER_TYPES:
        raise InvalidOrder(
            f"polymarket_us: {request.type.value} is held by the execution engine; this venue holds "
            f"limit, stop-market and stop-limit orders"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder("polymarket_us: 'day' is rewritten to 'gtd' by the engine; the venue's DAY does not cancel at the roll")
    if request.reduce_only:
        raise InvalidOrder("polymarket_us: the exchange API has no reduce-only flag")
    symbol = symbol_of(request.market_id)
    precision = scale.precision
    needs_price = request.type in (OrderType.LIMIT, OrderType.MARKET, OrderType.STOP_LIMIT)
    if needs_price and request.price is None:
        raise InvalidOrder(
            "polymarket_us: a price is required -- a market order is sent as an immediate limit at the "
            "protection price you give, and a stop-limit needs its limit"
        )
    if request.type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT) and request.stop_price is None:
        raise InvalidOrder("polymarket_us: a stop order needs stop_price")
    price = validate_price(D(request.price), precision) if needs_price else None
    stop = validate_price(D(request.stop_price), precision) if request.stop_price is not None else None
    quantity = validate_amount(D(request.amount), precision)
    side, yes_price, yes_stop = wire_side(request.side), price, stop
    tif = request.time_in_force
    if request.type == OrderType.MARKET:
        tif = TimeInForce.FOK if tif == TimeInForce.FOK else TimeInForce.IOC
    if request.post_only and tif not in (TimeInForce.GTC, TimeInForce.GTD):
        raise InvalidOrder("polymarket_us: post-only orders must be gtc or gtd")
    body: dict[str, Any] = {
        "type": TYPE_TO_WIRE[request.type],
        "side": side,
        "orderQty": scale.qty_to_wire(quantity),
        "symbol": symbol,
        "timeInForce": TIF_TO_WIRE[tif],
        "clordId": request.client_order_id or str(uuid.uuid4()),
        "account": account,
        "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_MANUAL" if request.params.get("manual") else "MANUAL_ORDER_INDICATOR_AUTOMATED",
    }
    if yes_price is not None:
        body["price"] = scale.price_to_wire(yes_price)
    if yes_stop is not None:
        body["stopPrice"] = scale.price_to_wire(yes_stop)
    if tif == TimeInForce.GTD:
        if request.expires_at is None:
            raise InvalidOrder("polymarket_us: a gtd order needs expires_at")
        body["goodTillTime"] = rfc3339(request.expires_at)
    if request.post_only:
        body["participateDontInitiate"] = True
    for key in ("selfMatchPreventionInstruction", "selfMatchPreventionId", "orderCapacity", "minQty", "allOrNone"):
        if key in request.params:
            body[key] = request.params[key]
    return body


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

def order_of(raw: dict[str, Any], scale: InstrumentScale, *, account: Account | None = None) -> Order:
    """An exchange order, on the YES leg."""
    symbol = str(raw.get("symbol") or scale.symbol)
    amount = scale.qty_from_wire(raw.get("orderQty"))
    filled = scale.qty_from_wire(raw.get("cumQty"))
    leaves = raw.get("leavesQty")
    return Order(
        id=str(raw.get("id") or ""),
        client_order_id=raw.get("clordId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, symbol),
        side=Side.BUY if raw.get("side") == "SIDE_BUY" else Side.SELL,
        type=TYPE_FROM_WIRE.get(str(raw.get("type") or ""), OrderType.LIMIT),
        time_in_force=TIF_FROM_WIRE.get(str(raw.get("timeInForce") or ""), TimeInForce.GTC),
        status=STATE.get(str(raw.get("state") or ""), OrderStatus.PENDING),
        price=scale.price_from_wire(raw.get("price")),
        stop_price=scale.price_from_wire(raw.get("stopPrice")),
        amount=amount,
        filled=filled,
        remaining=scale.qty_from_wire(leaves) if leaves is not None else None,
        average_price=scale.price_from_wire(raw.get("avgPx")) if filled > 0 else None,
        post_only=bool(raw.get("participateDontInitiate")),
        expires_at=parse_ts(raw.get("goodTillTime")),
        created_at=parse_ts(raw.get("createTime")),
        updated_at=parse_ts(raw.get("lastTransactTime") or raw.get("insertTime")),
        info=raw,
    )


def scale_from_order(order: dict[str, Any], fallback: InstrumentScale | None = None) -> InstrumentScale | None:
    """The scales an order carries (copied from its instrument when it was
    entered), or `fallback` where it carries none."""
    price_scale = int(order.get("priceScale") or 0)
    if not price_scale:
        return fallback
    qty_scale = int(order.get("fractionalQuantityScale") or 0) or 1
    if fallback is not None and (fallback.price_scale, fallback.qty_scale) == (price_scale, qty_scale):
        return fallback
    return InstrumentScale(
        symbol=str(order.get("symbol") or (fallback.symbol if fallback else "")), price_scale=price_scale,
        qty_scale=qty_scale, tick=fallback.tick if fallback else Decimal(1) / price_scale,
        min_quantity=fallback.min_quantity if fallback else Decimal(1) / qty_scale,
    )


def commission_of(value: Any, scale: InstrumentScale) -> Decimal | None:
    """A commission field in dollars. Commissions are notional units: one
    dollar is `price_scale * fractional_quantity_scale` of them. Negative is
    a rebate."""
    if value in (None, ""):
        return None
    return D(str(value)) / (scale.price_scale * scale.qty_scale)


def fill_of(raw: dict[str, Any], scale: InstrumentScale, *, account: Account | None = None) -> Fill:
    order = raw.get("order") or {}
    scale = scale_from_order(order, scale) or scale
    symbol = str(order.get("symbol") or scale.symbol)
    return Fill(
        id=str(raw.get("tradeId") or raw.get("id") or ""),
        order_id=str(order.get("id") or ""),
        client_order_id=order.get("clordId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, symbol),
        side=Side.BUY if order.get("side") == "SIDE_BUY" else Side.SELL,
        price=scale.price_from_wire(raw.get("lastPx")) or Decimal("0"),
        amount=scale.qty_from_wire(raw.get("lastShares")),
        fee=commission_of(raw.get("commissionNotionalCollected"), scale),
        fee_currency="USD",
        liquidity=Liquidity.TAKER if raw.get("aggressor") else Liquidity.MAKER,
        timestamp=parse_ts(raw.get("transactTime")) or 0,
        info=raw,
    )


def position_of(raw: dict[str, Any], scale: InstrumentScale, *, account: Account | None = None) -> Position:
    """A netted position. Cost and realized P&L stay in `info`: the schema
    gives them as integers without saying which scale they use."""
    symbol = str(raw.get("symbol") or scale.symbol)
    net = scale.qty_from_wire(raw.get("netPosition"))
    return Position(
        venue=VENUE, account=account, market_id=ids.qualify(VENUE, symbol),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net), resolved=bool(raw.get("expired")),
        timestamp=parse_ts(raw.get("updateTime")), info=raw,
    )


GRPC_CODES = {3: "invalid_argument", 5: "not_found", 6: "already_exists", 7: "permission_denied", 8: "resource_exhausted", 9: "failed_precondition"}


def error_of(exc: ExchangeError) -> ExchangeError:
    body = exc.body if isinstance(exc.body, dict) else {}
    message = str(body.get("message") or exc)
    code = body.get("code")
    text = message.lower()
    if "global rate limit exceeded" in text:
        return OrderRejected(message, reason="latency_stopgap", info=body, body=exc.body, status=exc.status)
    if code == 7 or exc.status == 403:
        return PermissionDenied(message, body=exc.body, status=exc.status)
    if isinstance(exc, AuthenticationError):
        return exc
    if code == 6 or (exc.status == 409 and "clord" in text):
        return DuplicateClientOrderId(message)
    if "buying power" in text or "insufficient" in text:
        return InsufficientFunds(message, body=exc.body, status=exc.status)
    if any(word in text for word in ("market closed", "exchange closed", "instrument closed", "halted", "suspended", "not open")):
        return MarketHalted(message, body=exc.body, status=exc.status)
    if code == 5 or isinstance(exc, MarketNotFound):
        return OrderNotFound(message, body=exc.body, status=exc.status)
    if isinstance(exc, BadRequest) or code in GRPC_CODES:
        return OrderRejected(message, reason=GRPC_CODES.get(code) if isinstance(code, int) else None, info=body, body=exc.body, status=exc.status)
    return exc


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TokenSource:
    """Signs the client assertion and keeps a valid access token.

    One refresh at a time: concurrent callers wait for the same token rather
    than each asking Auth0 for their own.
    """

    def __init__(self, credentials: PolymarketUSExchangeCredentials, http: AsyncHttpClient, *, audience: str, domain: str):
        try:
            import jwt  # noqa: F401
            from cryptography.hazmat.primitives import serialization
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("Polymarket US exchange auth needs PyJWT: pip install synpath") from exc
        self.client_id = credentials.client_id
        self._key = serialization.load_pem_private_key(credentials.private_key_pem, password=None)
        self.http = http
        self.audience = audience
        self.domain = domain
        self._token: str | None = None
        self._expires = 0.0
        self._lock = asyncio.Lock()

    def assertion(self, *, now: int | None = None) -> str:
        import jwt

        stamp = int(now if now is not None else time.time())
        claims = {
            "iss": self.client_id, "sub": self.client_id, "aud": f"https://{self.domain}/oauth/token",
            "iat": stamp, "exp": stamp + 300, "jti": str(uuid.uuid4()),
        }
        return jwt.encode(claims, self._key, algorithm="RS256")

    async def token(self) -> str:
        if self._token and time.monotonic() < self._expires:
            return self._token
        async with self._lock:
            if self._token and time.monotonic() < self._expires:
                return self._token
            raw = await self.http.post(f"https://{self.domain}/oauth/token", json={
                "client_id": self.client_id,
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": self.assertion(),
                "audience": self.audience,
                "grant_type": "client_credentials",
            })
            if not raw or not raw.get("access_token"):
                raise CredentialsMissing("polymarket_us: the token endpoint returned no access token")
            self._token = str(raw["access_token"])
            self._expires = time.monotonic() + max(0, int(raw.get("expires_in") or 180) - TOKEN_MARGIN_S)
            return self._token

    def invalidate(self) -> None:
        self._token, self._expires = None, 0.0


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class PolymarketUSExchangeTrading(TradingExchange):
    """Polymarket US order entry on the exchange API.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.polymarket_us_exchange import PolymarketUSExchangeTrading

    creds = require("polymarket_us_exchange", load_credentials())
    async with PolymarketUSExchangeTrading(creds) as pmx:
        print(await pmx.whoami())
        print(await pmx.fetch_balance())
    ```
    """

    id = VENUE
    name = "Polymarket US (exchange API)"
    native_order_types = NATIVE_ORDER_TYPES
    """Order types the exchange holds itself, stops included."""
    has: dict[str, Capability] = {
        "create_order": True,
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        # No cancel-all endpoint: open orders are listed and cancelled
        # twenty to a request.
        "cancel_all_orders": True,
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": True,
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": False,
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
        credentials: PolymarketUSExchangeCredentials,
        *,
        account_name: str | None = None,
        base_url: str | None = None,
        auth_domain: str | None = None,
        gateway_url: str = GATEWAY_URL,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        env_url, env_domain = ENVIRONMENTS[credentials.env]
        self.credentials = credentials
        self.base_url = (base_url or env_url).rstrip("/")
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self._http = shared
        self.api = AsyncHttpClient(self.base_url, limiter=None, client=shared, venue=VENUE)
        self.gateway = AsyncHttpClient(gateway_url, limiter=None, client=shared, venue=VENUE)
        self.tokens = TokenSource(credentials, self.api, audience=self.base_url, domain=auth_domain or env_domain)
        self.trading_account = credentials.account
        self.account = Account(venue=VENUE, name=account_name or credentials.account or "default")
        # 100 requests a second per firm on a one-minute average; the query
        # endpoints below also have their own, much smaller budgets.
        self.limiter = limiter or BudgetLimiter(read_per_second=50, write_per_second=50)
        self.search_orders_budget = BudgetLimiter(read_per_second=12 / 60, write_per_second=12 / 60, burst_seconds=60, max_wait_s=30)
        self.search_executions_budget = BudgetLimiter(read_per_second=12 / 60, write_per_second=12 / 60, burst_seconds=60, max_wait_s=30)
        self.refdata_budget = BudgetLimiter(read_per_second=6 / 60, write_per_second=6 / 60, burst_seconds=60, max_wait_s=30)
        self._scales: dict[str, InstrumentScale] = {}
        self._gateway_markets: dict[str, dict[str, Any]] = {}

    # -- transport ------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: Any = None, json: Any = None, kind: str = "write",
        priority: Priority = Priority.NORMAL, budget: BudgetLimiter | None = None, account_scoped: bool = True,
    ) -> Any:
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        if budget is not None:
            await budget.acquire(cost=1, kind="read")
        for attempt in range(2):
            headers = {"Authorization": f"Bearer {await self.tokens.token()}"}
            if account_scoped:
                headers["x-participant-id"] = self.credentials.participant_id
            try:
                return await self.api.request(method, path, params=params or None, json=json, headers=headers)
            except AuthenticationError as exc:
                if exc.status == 401 and attempt == 0:
                    self.tokens.invalidate()
                    continue
                raise error_of(exc) from None
            except ExchangeError as exc:
                raise error_of(exc) from None
        raise AssertionError("unreachable")

    async def _account(self) -> str:
        if not self.trading_account:
            accounts = await self.list_accounts()
            if not accounts:
                raise PermissionDenied(f"polymarket_us: participant {self.credentials.participant_id} has no trading account")
            self.trading_account = accounts[0]
        return self.trading_account

    # -- identity -------------------------------------------------------------

    async def whoami(self) -> dict[str, Any]:
        return await self._call("GET", "/v1/whoami", kind="read")

    async def list_accounts(self) -> list[str]:
        raw = await self._call("GET", "/v1/accounts", kind="read")
        return [str(a) for a in (raw or {}).get("accounts") or []]

    # -- reference data -------------------------------------------------------

    async def load_instruments(self, symbols: list[str], *, strict: bool = True) -> dict[str, InstrumentScale]:
        """Scales for these symbols, fetched in one reference-data request and
        cached for the life of the adapter. With `strict`, a symbol reference
        data does not know is an `InvalidOrder`; without, it is left out."""
        missing = [s for s in dict.fromkeys(symbols) if s not in self._scales]
        if missing:
            raw = await self._call(
                "POST", "/v1/refdata/instruments", json={"symbols": missing, "pageSize": max(len(missing), 1)},
                kind="read", budget=self.refdata_budget, account_scoped=False,
            )
            for row in (raw or {}).get("instruments") or []:
                scale = InstrumentScale.from_instrument(row)
                self._scales[scale.symbol] = scale
        unknown = [s for s in symbols if s not in self._scales]
        if unknown and strict:
            raise InvalidOrder(f"polymarket_us: no instrument {unknown[0]!r} in reference data")
        return {s: self._scales[s] for s in symbols if s in self._scales}

    async def scale_of(self, symbol: str) -> InstrumentScale:
        return (await self.load_instruments([symbol]))[symbol]

    def cached_scale(self, symbol: str) -> InstrumentScale | None:
        """A symbol's scales if already read, without asking reference data."""
        return self._scales.get(symbol)

    def remember_instrument(self, scale: InstrumentScale) -> None:
        self._scales[scale.symbol] = scale

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Insert one order. Returns it `pending` with the exchange's id; the
        exchange accepts or rejects it asynchronously."""
        symbol = symbol_of(request.market_id)
        scale = await self.scale_of(symbol)
        body = translate_order(request, scale, account=await self._account())
        raw = await self._call("POST", "/v1/trading/orders", json=body)
        return self._pending(str((raw or {}).get("orderId") or ""), request, body, scale)

    def _pending(self, order_id: str, request: OrderRequest, body: dict[str, Any], scale: InstrumentScale) -> Order:
        order = order_of({**body, "id": order_id, "state": "ORDER_STATE_PENDING_NEW"}, scale, account=request.account or self.account)
        return order.model_copy(update={
            "created_at": int(time.time() * 1000), "book": request.book, "trader": request.trader,
            "tags": request.tags, "info": {"request": body},
        })

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        account = await self._account()
        symbols = []
        for request in requests:
            try:
                symbols.append(symbol_of(request.market_id))
            except InvalidOrder:
                pass
        await self.load_instruments(symbols, strict=False)
        prepared: list[tuple[dict[str, Any], InstrumentScale] | Exception] = []
        for request in requests:
            try:
                symbol = symbol_of(request.market_id)
                scale = self._scales[symbol]
                prepared.append((translate_order(request, scale, account=account), scale))
            except InvalidOrder as exc:
                prepared.append(exc)
            except KeyError:
                prepared.append(InvalidOrder(f"polymarket_us: no instrument for {request.market_id!r} in reference data"))
        results: list[Order | Exception] = [p if isinstance(p, Exception) else p for p in prepared]  # type: ignore[misc]
        sendable = [i for i, p in enumerate(prepared) if not isinstance(p, Exception)]
        for start in range(0, len(sendable), BATCH):
            chunk = sendable[start:start + BATCH]
            raw = await self._call("POST", "/v1/trading/orders/list", json={"requests": [prepared[i][0] for i in chunk]})  # type: ignore[index]
            answers = list((raw or {}).get("responses") or [])
            for offset, index in enumerate(chunk):
                body, scale = prepared[index]  # type: ignore[misc]
                order_id = (answers[offset] or {}).get("orderId") if offset < len(answers) else None
                results[index] = (
                    self._pending(str(order_id), requests[index], body, scale) if order_id
                    else OrderRejected("polymarket_us: no order id returned for this entry", reason="no_id")
                )
        return results

    async def cancel_order(self, order_id: str, *, market_id: str | None = None, current: Order | None = None) -> Order:
        """Request a cancel. Returns the order `pending_cancel`; pass
        `current` (or `market_id`) to spare the lookup of its symbol."""
        current = current or await self._find_order(order_id, symbol=symbol_of(market_id) if market_id else None)
        await self._call(
            "POST", "/v1/trading/orders/cancel", json={"orderId": order_id, "symbol": symbol_of(current.market_id)},
            priority=Priority.HIGH,
        )
        return current.model_copy(update={"status": OrderStatus.PENDING_CANCEL})

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        open_orders = {o.id: o for o in await self.fetch_open_orders(market_id=market_id)}
        results: dict[str, Order | Exception] = {}
        known = [oid for oid in order_ids if oid in open_orders]
        for oid in order_ids:
            if oid not in open_orders:
                results[oid] = OrderNotFound(f"polymarket_us: {oid} is not an open order")
        for start in range(0, len(known), BATCH):
            chunk = known[start:start + BATCH]
            await self._call(
                "POST", "/v1/trading/orders/cancel/list",
                json={"requests": [{"orderId": oid, "symbol": symbol_of(open_orders[oid].market_id)} for oid in chunk]},
                priority=Priority.HIGH,
            )
            for oid in chunk:
                results[oid] = open_orders[oid].model_copy(update={"status": OrderStatus.PENDING_CANCEL})
        return [results[oid] for oid in order_ids]

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order, or every one on a symbol: listed, then cancelled
        twenty to a request. Returns how many cancels were sent."""
        orders = await self.fetch_open_orders(market_id=market_id)
        results = await self.cancel_orders([o.id for o in orders], market_id=market_id)
        return sum(1 for r in results if not isinstance(r, Exception))

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """Cancel-replace price, quantity, time in force or expiry. Returns the
        order `pending_replace`; whether queue priority survives is not
        published, so `queue_priority_preserved` is `None`. Prices are YES
        prices, as the order reads back."""
        current = current or await self._find_order(request.order_id)
        scale = await self.scale_of(symbol_of(current.market_id))
        body: dict[str, Any] = {
            "orderId": request.order_id, "symbol": symbol_of(current.market_id),
            "clordId": request.client_order_id or str(uuid.uuid4()),
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATED",
        }
        price = D(request.price) if request.price is not None else current.price
        if price is not None:
            body["price"] = scale.price_to_wire(validate_price(price, scale.precision))
        if current.stop_price is not None:
            body["stopPrice"] = scale.price_to_wire(current.stop_price)
        quantity = validate_amount(D(request.amount), scale.precision) if request.amount is not None else current.amount
        body["orderQty"] = scale.qty_to_wire(quantity)
        tif = request.time_in_force or current.time_in_force
        if tif == TimeInForce.DAY:
            raise InvalidOrder("polymarket_us: 'day' is rewritten to 'gtd' by the engine")
        body["timeInForce"] = TIF_TO_WIRE[tif]
        expires = request.expires_at or current.expires_at
        if tif == TimeInForce.GTD:
            if expires is None:
                raise InvalidOrder("polymarket_us: a gtd order needs expires_at")
            body["goodTillTime"] = rfc3339(expires)
        await self._call("POST", "/v1/trading/orders/replace", json=body)
        return current.model_copy(update={
            "status": OrderStatus.PENDING_REPLACE, "queue_priority_preserved": None,
            "info": {**current.info, "replace": body},
        })

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        params: dict[str, Any] = {"accounts": await self._account()}
        if market_id:
            params["symbols"] = symbol_of(market_id)
        raw = await self._call("GET", "/v1/trading/orders/open", params=params, kind="read")
        rows = (raw or {}).get("orders") or []
        scales = await self.load_instruments([str(r.get("symbol")) for r in rows]) if rows else {}
        return [order_of(r, scales[str(r.get("symbol"))], account=self.account) for r in rows]

    async def _find_order(self, order_id: str, *, symbol: str | None = None) -> Order:
        for order in await self.fetch_open_orders(market_id=symbol):
            if order.id == order_id:
                return order
        return await self.fetch_order(order_id)

    async def fetch_order(self, order_id: str) -> Order:
        """By id: from the open orders when it is still working, otherwise
        from order search, which is rationed to twelve requests a minute."""
        for order in await self.fetch_open_orders():
            if order.id == order_id:
                return order
        page = await self.fetch_orders(order_id=order_id, limit=1)
        if not page:
            raise OrderNotFound(f"polymarket_us: no order {order_id}")
        return page[0]

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None, order_id: str | None = None,
    ) -> Page[Order]:
        """Order search. `status` is the venue's filter word without its
        prefix: `open`, `closed`, `filled`, `canceled`, `rejected`, `expired`."""
        body: dict[str, Any] = {"accounts": [await self._account()]}
        if status:
            body["orderStateFilter"] = f"ORDER_STATE_FILTER_{status.upper()}"
        for key, value in (("symbol", symbol_of(market_id) if market_id else None), ("orderId", order_id), ("pageToken", cursor), ("pageSize", limit)):
            if value:
                body[key] = value
        if since:
            body["startTime"] = rfc3339(since)
        raw = await self._call("POST", "/v1/report/orders/search", json=body, kind="read", budget=self.search_orders_budget)
        rows = (raw or {}).get("order") or (raw or {}).get("orders") or []
        scales = await self.load_instruments([str(r.get("symbol")) for r in rows]) if rows else {}
        return Page(
            [order_of(r, scales[str(r.get("symbol"))], account=self.account) for r in rows],
            next_cursor=(raw or {}).get("nextPageToken") or None,
        )

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        body: dict[str, Any] = {
            "accounts": [await self._account()],
            "types": ["EXECUTION_TYPE_FILL", "EXECUTION_TYPE_PARTIAL_FILL"],
            "newestFirst": True,
        }
        for key, value in (("symbol", symbol_of(market_id) if market_id else None), ("orderId", order_id), ("pageToken", cursor), ("pageSize", limit)):
            if value:
                body[key] = value
        if since:
            body["startTime"] = rfc3339(since)
        raw = await self._call("POST", "/v1/report/executions/search", json=body, kind="read", budget=self.search_executions_budget)
        rows = (raw or {}).get("executions") or []
        scales = await self.load_instruments([str((r.get("order") or {}).get("symbol")) for r in rows]) if rows else {}
        return Page(
            [fill_of(r, scales[str((r.get("order") or {}).get("symbol"))], account=self.account) for r in rows],
            next_cursor=None if (raw or {}).get("eof") else (raw or {}).get("nextPageToken") or None,
        )

    async def preview_order(self, request: OrderRequest) -> Order:
        symbol = symbol_of(request.market_id)
        scale = await self.scale_of(symbol)
        body = translate_order(request, scale, account=await self._account())
        raw = await self._call("POST", "/v1/trading/orders/preview", json={"request": body}, kind="read")
        return order_of((raw or {}).get("previewOrder") or {}, scale, account=self.account)

    # -- account --------------------------------------------------------------

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        params: dict[str, Any] = {"name": await self._account()}
        if market_id:
            params["symbol"] = symbol_of(market_id)
        raw = await self._call("GET", "/v1/positions", params=params, kind="read")
        rows = (raw or {}).get("positions") or []
        scales = await self.load_instruments([str(r.get("symbol")) for r in rows]) if rows else {}
        positions = [position_of(r, scales[str(r.get("symbol"))], account=self.account) for r in rows]
        return [p for p in positions if p.contracts > 0]

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        raw = await self._call("POST", "/v1/positions/balance", json={"name": await self._account(), "currency": "USD"}, kind="read") or {}
        total = D(raw.get("balance") or "0")
        buying_power = D(raw["buyingPower"]) if raw.get("buyingPower") not in (None, "") else None
        return Balance(
            venue=VENUE, account=account or self.account, currency="USD", total=total,
            available=buying_power if buying_power is not None else total,
            locked=D(raw["openOrders"]) if raw.get("openOrders") not in (None, "") else None,
            buying_power=buying_power, timestamp=parse_ts(raw.get("updateTime")), info=raw,
        )

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """From the public gateway's market (same symbols): the taker theta and
        the venue's maker rebate, at the YES price."""
        symbol = symbol_of(market_id)
        market = self._gateway_markets.get(symbol)
        if market is None:
            try:
                raw = await self.gateway.get(f"/v1/market/slug/{symbol}")
            except MarketNotFound:
                raise NotSupported(f"polymarket_us: {symbol} is not on the public gateway") from None
            market = self._gateway_markets[symbol] = (raw or {}).get("market") or raw or {}
        schedule = fee_schedule_of(market)
        if schedule is None:
            raise NotSupported(f"polymarket_us: market {symbol} publishes no fee coefficient")
        wire = float(D(price))
        taker = schedule.estimate(wire, float(amount), taker=True)
        maker = schedule.estimate(wire, float(amount), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, symbol), side=side, price=D(price), amount=D(amount),
            taker_fee=D(str(taker)) if taker is not None else None,
            maker_fee=D(str(maker)) if maker is not None else None,
            currency="USD", info={"schedule": schedule.model_dump()},
        )

    async def close(self) -> None:
        await self._http.aclose()
