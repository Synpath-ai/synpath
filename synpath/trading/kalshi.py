"""Kalshi order entry, on the V2 event-contract endpoints.

Two facts about the venue shape everything here.

**One leg on the wire.** Kalshi V2 quotes the YES leg only: `side=bid` is buy
YES, `side=ask` is sell YES, and the price is the YES price. A NO order is
the same order seen from the other side -- buying NO at *q* is selling YES at
`1 - q` -- so that is what is sent, and that is how the venue reports it
back. An order placed as "buy NO at 0.30" is therefore read back as "sell
YES at 0.70": the same order, in the venue's own words. `info` keeps the
venue's `outcome_side`, and the engine's journal keeps the request as it was
made.

**Limit orders only.** There is no market order type on V2. A market order
here is an immediate-or-cancel limit at the caller's protection price, and a
request without one is refused rather than sent at a price nobody chose.

Every request is signed: `timestamp + METHOD + path`, RSA-PSS with SHA-256
over the full request path (`/trade-api/v2/...`, no query string), sent in
three headers. The private key never leaves `KalshiSigner`.

The venue's budget is metered in tokens by account tier (`GET
/account/limits`); `fetch_limits` reads it and hands it to the budget
limiter so the adapter paces to what the account actually has.
"""
from __future__ import annotations

import base64
import time
import uuid
from decimal import Decimal
from typing import Any

from ..base import AsyncHttpClient, Capability
from .. import ids
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NotSupported
from ..kalshi import parse_ts, to_float
from ..types import FeeSchedule, Page
from .base import TradingExchange
from .credentials import KalshiCredentials
from .errors import (
    DuplicateClientOrderId, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected,
)
from .limiter import BudgetLimiter, Priority
from .money import D, kalshi_count, kalshi_dollars, validate_amount, validate_price
from .types import (
    Account, Balance, EditRequest, FeeEstimate, Fill, HeldBy, Liquidity, Order, OrderRequest,
    OrderStatus, OrderType, Position, PositionSide, Precision, Settlement, SettlementState, Side,
    TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "kalshi"

BASE_URLS = {
    "demo": "https://external-api.demo.kalshi.co/trade-api/v2",
    "prod": "https://external-api.kalshi.com/trade-api/v2",
}

TIF_TO_VENUE = {
    TimeInForce.GTC: "good_till_canceled",
    TimeInForce.IOC: "immediate_or_cancel",
    TimeInForce.FOK: "fill_or_kill",
    TimeInForce.GTD: "good_till_canceled",
}
"""`gtd` is `good_till_canceled` plus `expiration_time`. `day` is not here on
purpose: the engine rewrites it to `gtd` at the session end before an
adapter sees it, and an adapter that received one would have to invent a
session."""

DEFAULT_COST = 10
"""Tokens a call costs unless the venue's endpoint-cost table says
otherwise; one order is one default call, a batch is one per order."""

KNOWN_COSTS: dict[tuple[str, str], int] = {
    ("DELETE", "/trade-api/v2/portfolio/events/orders"): 2,
    ("DELETE", "/trade-api/v2/portfolio/events/orders/:order_id"): 2,
    ("DELETE", "/trade-api/v2/portfolio/events/orders/batched"): 2,
    ("GET", "/trade-api/v2/portfolio/orders/:order_id"): 2,
    ("POST", "/trade-api/v2/communications/quotes"): 2,
    ("DELETE", "/trade-api/v2/communications/rfqs/:rfq_id/quotes/:quote_id"): 2,
    ("GET", "/trade-api/v2/communications/rfqs/:rfq_id/quotes/:quote_id"): 2,
    ("PUT", "/trade-api/v2/communications/rfqs/:rfq_id/quotes/:quote_id/confirm"): 1,
}
"""The venue's published non-default costs as of 2026-09-17, used until
`fetch_limits` replaces them with the live table. Paths are the venue's
own patterns: `:name` matches one segment, `*name` the rest."""

DEFAULT_PRECISION = Precision(
    tick=Decimal("0.01"), min_amount=Decimal("0.01"), amount_step=Decimal("0.01"), whole_contracts=False,
)
"""What an order is checked against when the caller gives no instrument
spec: the coarse tick every market accepts, and the venue's two-decimal
contract count. A market with a finer ladder wants its own `Precision`."""


# ---------------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------------

class KalshiSigner:
    """Holds the private key and produces the three auth headers.

    The signing payload is `timestamp_ms + METHOD + path` with `path` the full
    request path including the `/trade-api/v2` prefix and excluding any query
    string, per the venue's own examples. RSA-PSS, SHA-256, salt length equal
    to the digest -- the combination the venue's reference client uses and
    that a `openssl dgst -sigopt rsa_pss_saltlen:digest` reproduces.
    """

    def __init__(self, key_id: str, private_key_pem: bytes):
        try:
            from cryptography.hazmat.primitives import serialization
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("Kalshi signing needs the `cryptography` package: pip install synpath") from exc
        self.key_id = key_id
        self._key = serialization.load_pem_private_key(private_key_pem, password=None)

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        payload = f"{timestamp_ms}{method.upper()}{path}".encode()
        signature = self._key.sign(
            payload,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def headers(self, method: str, path: str, *, timestamp_ms: int | None = None) -> dict[str, str]:
        stamp = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": self.sign(stamp, method, path),
            "KALSHI-ACCESS-TIMESTAMP": str(stamp),
        }


# ---------------------------------------------------------------------------
# Pure translation and normalizers
# ---------------------------------------------------------------------------

def ticker_of(market_id: str) -> str:
    """The ticker behind a Synpath market id (`kalshi:KXFOO-25`) or a bare
    ticker. Another venue's id is refused before anything is signed."""
    try:
        return ids.native(VENUE, market_id)
    except BadRequest as exc:
        raise InvalidOrder(str(exc)) from None


def translate_order(request: OrderRequest, *, precision: Precision | None = None) -> dict[str, Any]:
    """An `OrderRequest` as the V2 create body, on the YES leg.

    Validation happens here, before anything is signed: price on the tick and
    inside (0, 1), amount on the venue's two-decimal grid, an order type the
    venue holds, a time in force it understands, a protection price for a
    market order.
    """
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"kalshi: {request.type.value} is held by the execution engine, not the venue; "
            f"submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder("kalshi: 'day' is rewritten to 'gtd' by the engine; an adapter cannot pick a session end")
    if request.price is None:
        raise InvalidOrder(
            "kalshi: a price is required -- the venue has no market orders, so a market order is an "
            "immediate-or-cancel limit at the protection price you give"
        )
    ticker = ticker_of(request.market_id)
    spec = precision or DEFAULT_PRECISION
    price = validate_price(D(request.price), spec)
    amount = validate_amount(D(request.amount), spec)

    # The venue quotes the YES leg and so does this library: `buy` is a YES bid
    # at the price given, `sell` is a YES ask at the same price, which is what
    # the venue shows a NO buyer as.
    book_side, yes_price = ("bid" if request.side == Side.BUY else "ask"), price

    if request.type == OrderType.MARKET:
        tif = "fill_or_kill" if request.time_in_force == TimeInForce.FOK else "immediate_or_cancel"
    else:
        tif = TIF_TO_VENUE[request.time_in_force]

    params = dict(request.params)
    body: dict[str, Any] = {
        "ticker": ticker,
        "client_order_id": request.client_order_id or str(uuid.uuid4()),
        "side": book_side,
        "count": kalshi_count(amount),
        "price": kalshi_dollars(yes_price),
        "time_in_force": tif,
        "self_trade_prevention_type": params.pop("self_trade_prevention_type", "taker_at_cross"),
        "post_only": request.post_only,
        "reduce_only": request.reduce_only,
    }
    if request.time_in_force == TimeInForce.GTD and request.expires_at is not None:
        body["expiration_time"] = int(request.expires_at // 1000)
    if request.account and request.account.subaccount is not None:
        body["subaccount"] = int(request.account.subaccount)
    for key in ("cancel_order_on_pause", "order_group_id", "exchange_index"):
        if key in params:
            body[key] = params.pop(key)
    return body


def _status_of(raw: dict[str, Any], *, filled: Decimal, remaining: Decimal) -> OrderStatus:
    native = str(raw.get("status") or "").lower()
    if native == "resting":
        return OrderStatus.OPEN
    if native == "executed":
        return OrderStatus.CLOSED
    if native == "canceled":
        return OrderStatus.CANCELED
    # The create, amend and decrease responses carry no status word, only the
    # two counts; what they mean is unambiguous: nothing left to rest is done
    # if anything matched and cancelled if nothing did (an IOC that found no
    # liquidity, a decrease to zero).
    if remaining == 0:
        return OrderStatus.CLOSED if filled > 0 else OrderStatus.CANCELED
    return OrderStatus.OPEN


def _side_of(raw: dict[str, Any]) -> Side:
    book = str(raw.get("book_side") or raw.get("side") or "").lower()
    if book in ("bid", "ask"):
        return Side.BUY if book == "bid" else Side.SELL
    # Legacy rows carry action + (yes|no) side: buy yes / sell no are bids.
    action, side = str(raw.get("action") or "").lower(), str(raw.get("side") or "").lower()
    return Side.BUY if (action == "buy") == (side == "yes") else Side.SELL


def order_of(raw: dict[str, Any], *, account: Account | None = None) -> Order:
    """A venue order row as an `Order`, on the YES leg."""
    ticker = str(raw.get("ticker") or "")
    filled = D(raw.get("fill_count_fp") or raw.get("fill_count") or "0")
    remaining = D(raw.get("remaining_count_fp") or raw.get("remaining_count") or "0")
    initial = raw.get("initial_count_fp")
    amount = D(initial) if initial is not None else filled + remaining
    taker_cost = D(raw.get("taker_fill_cost_dollars") or "0")
    maker_cost = D(raw.get("maker_fill_cost_dollars") or "0")
    fee = D(raw.get("taker_fees_dollars") or "0") + D(raw.get("maker_fees_dollars") or "0")
    cost = taker_cost + maker_cost
    expires = parse_ts(raw.get("expiration_time"))
    return Order(
        id=str(raw.get("order_id") or ""),
        client_order_id=raw.get("client_order_id") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        side=_side_of(raw),
        type=OrderType.MARKET if str(raw.get("type") or "").lower() == "market" else OrderType.LIMIT,
        time_in_force=TimeInForce.GTD if expires else TimeInForce.GTC,
        status=_status_of(raw, filled=filled, remaining=remaining),
        held_by=HeldBy.VENUE,
        price=D(raw["yes_price_dollars"]) if raw.get("yes_price_dollars") is not None else None,
        amount=amount,
        filled=filled,
        remaining=remaining,
        average_price=(cost / filled).quantize(Decimal("0.0001")) if filled > 0 and cost > 0 else None,
        cost=cost if filled > 0 else None,
        fee=fee if filled > 0 else None,
        fee_currency="USD",
        expires_at=expires,
        created_at=parse_ts(raw.get("created_time")),
        updated_at=parse_ts(raw.get("last_update_time")),
        info=raw,
    )


def order_from_response(
    raw: dict[str, Any], *, body: dict[str, Any], account: Account | None = None,
    filled: Decimal | None = None,
) -> Order:
    """The create, amend or decrease response, which carries only the counts,
    joined with the request that produced it. `filled` stands in when the
    response omits `fill_count` (amend and decrease do)."""
    if raw.get("fill_count") not in (None, ""):
        filled = D(raw["fill_count"])
    elif filled is None:
        filled = Decimal("0")
    remaining = D(raw.get("remaining_count") or "0")
    average = raw.get("average_fill_price")
    ts_ms = raw.get("ts_ms")
    return Order(
        id=str(raw.get("order_id") or ""),
        client_order_id=raw.get("client_order_id") or body.get("client_order_id"),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, body["ticker"]),
        side=Side.BUY if body.get("side") == "bid" else Side.SELL,
        type=OrderType.LIMIT,
        time_in_force=(
            TimeInForce.GTD if body.get("expiration_time") else
            {"good_till_canceled": TimeInForce.GTC, "immediate_or_cancel": TimeInForce.IOC,
             "fill_or_kill": TimeInForce.FOK}.get(str(body.get("time_in_force")), TimeInForce.GTC)
        ),
        status=_status_of(raw, filled=filled, remaining=remaining),
        price=D(body["price"]) if body.get("price") is not None else None,
        amount=filled + remaining,
        filled=filled,
        remaining=remaining,
        average_price=D(average) if average not in (None, "") else None,
        fee=D(raw["average_fee_paid"]) * filled if raw.get("average_fee_paid") not in (None, "") and filled > 0 else None,
        fee_currency="USD",
        post_only=bool(body.get("post_only")),
        reduce_only=bool(body.get("reduce_only")),
        expires_at=int(body["expiration_time"]) * 1000 if body.get("expiration_time") else None,
        created_at=int(ts_ms) if ts_ms is not None else None,
        updated_at=int(ts_ms) if ts_ms is not None else None,
        info={"response": raw, "request": body},
    )


def fill_of(raw: dict[str, Any], *, account: Account | None = None) -> Fill:
    ticker = str(raw.get("ticker") or raw.get("market_ticker") or "")
    stamp = parse_ts(raw.get("created_time")) or parse_ts(raw.get("ts")) or 0
    return Fill(
        id=str(raw.get("fill_id") or raw.get("trade_id") or ""),
        order_id=str(raw.get("order_id") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        side=_side_of(raw),
        price=D(raw["yes_price_dollars"]),
        amount=D(raw.get("count_fp") or raw.get("count") or "0"),
        fee=D(raw["fee_cost"]) if raw.get("fee_cost") not in (None, "") else None,
        fee_currency="USD",
        liquidity=Liquidity.TAKER if raw.get("is_taker") else Liquidity.MAKER,
        settlement=SettlementState.CONFIRMED,
        timestamp=stamp,
        info=raw,
    )


def position_of(raw: dict[str, Any], *, account: Account | None = None) -> Position:
    """A market position row. `position_fp` is signed: positive is YES
    contracts (`long`), negative is NO (`short`). The venue nets, so there
    is no inventory to carry."""
    ticker = str(raw.get("ticker") or "")
    signed = D(raw.get("position_fp") or "0")
    contracts = abs(signed)
    exposure = D(raw.get("market_exposure_dollars") or "0")
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        side=PositionSide.FLAT if contracts == 0 else PositionSide.LONG if signed > 0 else PositionSide.SHORT,
        contracts=contracts,
        entry_price=(exposure / contracts).quantize(Decimal("0.0001")) if contracts > 0 else None,
        realized_pnl=D(raw["realized_pnl_dollars"]) if raw.get("realized_pnl_dollars") not in (None, "") else None,
        margin=exposure if contracts > 0 else None,
        timestamp=parse_ts(raw.get("last_updated_ts")),
        info=raw,
    )


def settlement_of(raw: dict[str, Any], *, account: Account | None = None) -> Settlement:
    ticker = str(raw.get("ticker") or "")
    result = str(raw.get("market_result") or "") or None
    yes_count = D(raw.get("yes_count_fp") or "0")
    no_count = D(raw.get("no_count_fp") or "0")
    cost = D(raw.get("yes_total_cost_dollars") or "0") + D(raw.get("no_total_cost_dollars") or "0")
    revenue = raw.get("revenue")
    payout = (D(revenue) / 100).quantize(Decimal("0.01")) if revenue is not None else None
    fee = D(raw["fee_cost"]) if raw.get("fee_cost") not in (None, "") else Decimal("0")
    held = "yes" if yes_count > no_count else "no" if no_count > yes_count else None
    won = (held == result) if held and result in ("yes", "no") else None
    return Settlement(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, ticker),
        held=PositionSide.LONG if held == "yes" else PositionSide.SHORT if held == "no" else None,
        result=result,
        won=won,
        amount=max(yes_count, no_count) if held else None,
        cost=cost,
        payout=payout,
        pnl=(payout - cost - fee) if payout is not None else None,
        timestamp=parse_ts(raw.get("settled_time")),
        info=raw,
    )


def balance_of(raw: dict[str, Any], *, account: Account) -> Balance:
    """`balance` is the venue's available cash; the portfolio's market value
    rides along in `info`. Kalshi reports no figure for cash reserved by
    resting orders, so `locked` is `None` rather than a guess."""
    available = D(raw["balance_dollars"]) if raw.get("balance_dollars") is not None else D(raw.get("balance") or 0) / 100
    return Balance(
        venue=VENUE,
        account=account,
        currency="USD",
        total=available,
        available=available,
        locked=None,
        buying_power=None,
        timestamp=parse_ts(raw.get("updated_ts")),
        info=raw,
    )


def map_error(exc: ExchangeError, *, path: str = "") -> ExchangeError:
    """The venue's error code as the typed error a caller branches on.

    `path` is the request path: the venue answers `not_found` for a missing
    order and a missing market alike, and only the path says which."""
    code = (exc.code or "").lower()
    message = str(exc)
    text = f"{code} {message}".lower()
    if isinstance(exc, AuthenticationError):
        return exc
    if "insufficient" in text and ("balance" in text or "fund" in text):
        return InsufficientFunds(message, body=exc.body, status=exc.status)
    missing = "not_found" in text or "not found" in text or "no such" in text or isinstance(exc, MarketNotFound)
    if missing and ("/orders" in path or "order" in text):
        return OrderNotFound(message, body=exc.body, status=exc.status)
    if "client_order_id" in text and ("duplicate" in text or "already" in text or "exists" in text):
        return DuplicateClientOrderId(message)
    if any(word in text for word in ("market_closed", "market is closed", "not open", "paused", "halted", "market_not_open")):
        return MarketHalted(message, body=exc.body, status=exc.status)
    if isinstance(exc, MarketNotFound):
        return exc
    if isinstance(exc, BadRequest):
        return OrderRejected(message, reason=code or None, info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body, status=exc.status)
    return exc


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class KalshiTrading(TradingExchange):
    """Kalshi order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.kalshi import KalshiTrading

    creds = require("kalshi", load_credentials())
    async with KalshiTrading(creds) as kalshi:
        balance = await kalshi.fetch_balance()
    ```
    """

    id = VENUE
    name = "Kalshi"
    has: dict[str, Capability] = {
        "create_order": True,
        "create_orders": True,
        "cancel_order": True,
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": True,
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": True,
        "fetch_fee_estimate": True,
        "rfq": True,
        # Kalshi nets YES against NO in one account; there are no token
        # inventories to split or merge.
        "split_merge": False,
        # The user WebSocket channels arrive with the WebSocket layer.
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: KalshiCredentials,
        *,
        account_name: str = "default",
        base_url: str | None = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        self.credentials = credentials
        self.env = credentials.env
        self.account = Account(venue=VENUE, name=account_name)
        self.base_url = (base_url or BASE_URLS[credentials.env]).rstrip("/")
        self._path_prefix = _path_of(self.base_url)
        self.signer = KalshiSigner(credentials.key_id, credentials.private_key_pem)
        # Basic tier until `fetch_limits` says otherwise: 200 read / 100 write
        # tokens per second, the smallest budget any account has.
        self.limiter = limiter or BudgetLimiter(read_per_second=200, write_per_second=100)
        self.http = AsyncHttpClient(self.base_url, limiter=None, timeout=timeout, client=client, venue=VENUE)
        self.default_cost = DEFAULT_COST
        self.endpoint_costs: dict[tuple[str, str], int] = dict(KNOWN_COSTS)

    # -- transport ------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: Any = None, json: Any = None,
        kind: str = "read", cost: float | None = None, priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One signed call, paced by the budget, errors mapped. `cost` is
        looked up in the venue's endpoint-cost table unless given."""
        full_path = f"{self._path_prefix}{path}"
        if cost is None:
            cost = self.cost_of(method, full_path)
        await self.limiter.acquire(cost=cost, kind=kind, priority=priority)  # type: ignore[arg-type]
        headers = self.signer.headers(method, full_path)
        try:
            return await self.http.request(method, path, params=_clean(params), json=json, headers=headers)
        except ExchangeError as exc:
            raise map_error(exc, path=path) from None

    def cost_of(self, method: str, full_path: str) -> int:
        """Tokens the venue charges for this call, from its cost table.

        The table's paths are patterns: `:order_id` matches one segment,
        `*endpoint` everything after it. A literal entry wins over a pattern
        so `/orders/batched` is not read as `/orders/:order_id`."""
        method = method.upper()
        literal = self.endpoint_costs.get((method, full_path))
        if literal is not None:
            return literal
        segments = full_path.split("/")
        for (table_method, pattern), cost in self.endpoint_costs.items():
            if table_method != method:
                continue
            parts = pattern.split("/")
            if _matches(parts, segments):
                return cost
        return self.default_cost

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest, *, precision: Precision | None = None) -> Order:
        """Place one order. See the module docstring for how a NO order and a
        market order reach the wire."""
        body = translate_order(request, precision=precision)
        raw = await self._call("POST", "/portfolio/events/orders", json=body, kind="write")
        order = order_from_response(raw, body=body, account=request.account or self.account)
        return order.model_copy(update={"book": request.book, "trader": request.trader, "tags": request.tags})

    async def create_orders(
        self, requests: list[OrderRequest], *, precision: Precision | None = None,
    ) -> list[Order | Exception]:
        """Many orders in one request. Each costs the venue what a single
        order costs, so the budget is drawn per order, not per call."""
        bodies: list[dict[str, Any] | Exception] = []
        for request in requests:
            try:
                bodies.append(translate_order(request, precision=precision))
            except InvalidOrder as exc:
                bodies.append(exc)
        sendable = [b for b in bodies if isinstance(b, dict)]
        results: list[Order | Exception] = list(bodies)  # type: ignore[arg-type]
        if not sendable:
            return results
        raw = await self._call(
            "POST", "/portfolio/events/orders/batched", json={"orders": sendable},
            kind="write", cost=self.default_cost * len(sendable),
        )
        answers = list((raw or {}).get("orders") or [])
        cursor = 0
        for index, body in enumerate(bodies):
            if not isinstance(body, dict):
                continue
            answer = answers[cursor] if cursor < len(answers) else {}
            cursor += 1
            error = answer.get("error")
            if error:
                results[index] = OrderRejected(str(error.get("message") or error), reason=str(error.get("code") or ""), info=error)
            else:
                results[index] = order_from_response(answer, body=body, account=requests[index].account or self.account)
        return results

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        raw = await self._call(
            "DELETE", f"/portfolio/events/orders/{order_id}",
            params={"market_ticker": ticker_of(market_id) if market_id else None}, kind="write", priority=Priority.HIGH,
        )
        return await self._after_cancel(order_id, raw)

    async def _after_cancel(self, order_id: str, raw: dict[str, Any]) -> Order:
        """The cancel response carries only `reduced_by`; the order itself is
        read back so the caller sees what filled before the cancel landed.

        The venue acknowledged the cancel, so the order *is* cancelled; the
        read-back can still say `resting` for a beat, and is corrected
        rather than trusted.
        """
        reduced = D(raw.get("reduced_by") or "0")
        try:
            order = await self.fetch_order(order_id)
        except OrderNotFound:
            return Order(
                id=order_id, venue=VENUE, account=self.account, market_id="",
                side=Side.BUY, type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                status=OrderStatus.CANCELED, amount=reduced, info=raw,
            )
        remaining = order.remaining if order.remaining is not None else order.amount - order.filled
        if order.status == OrderStatus.OPEN:
            remaining = max(Decimal("0"), remaining - reduced)
        status = OrderStatus.CLOSED if order.status == OrderStatus.CLOSED else OrderStatus.CANCELED
        return order.model_copy(update={
            "status": status, "remaining": remaining, "info": {**order.info, "cancel": raw},
        })

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        raw = await self._call(
            "DELETE", "/portfolio/events/orders/batched",
            json={"orders": [{"order_id": oid, "market_ticker": ticker_of(market_id)} if market_id else {"order_id": oid} for oid in order_ids]},
            kind="write", priority=Priority.HIGH,
        )
        answers = {str(a.get("order_id")): a for a in ((raw or {}).get("orders") or [])}
        results: list[Order | Exception] = []
        for order_id in order_ids:
            answer = answers.get(order_id, {})
            error = answer.get("error")
            if error:
                results.append(OrderRejected(str(error.get("message") or error), reason=str(error.get("code") or ""), info=error))
            else:
                results.append(await self._after_cancel(order_id, answer))
        return results

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int | None:
        """Every resting order in the account, or every one on a market.

        The venue's cancel-all answers 204 with no count and works
        asynchronously -- it may also cancel orders placed in the minute
        after the call -- so the account-wide form returns `None`. The
        per-market form is a batch cancel of that market's open orders and
        does return how many it cancelled.
        """
        if market_id:
            open_orders = await self.fetch_open_orders(market_id=market_id)
            if not open_orders:
                return 0
            results = await self.cancel_orders([o.id for o in open_orders], market_id=market_id)
            return sum(1 for r in results if not isinstance(r, Exception))
        await self._call("DELETE", "/portfolio/events/orders", kind="write", priority=Priority.HIGH)
        return None

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """Change a resting order in place.

        Only the amount coming down is a *decrease*, which keeps the order's
        place in the queue. Anything else -- a new price, or more contracts --
        is an *amend*, which the venue treats as a new order at the back of
        the queue. `queue_priority_preserved` says which one this was.
        """
        current = current or await self.fetch_order(request.order_id)
        if request.time_in_force is not None or request.expires_at is not None:
            raise InvalidOrder("kalshi: time in force cannot be edited; cancel and replace")
        price_changed = request.price is not None and D(request.price) != current.price
        new_amount = D(request.amount) if request.amount is not None else None
        as_sent = {
            **(current.info.get("request") or {}), "ticker": ticker_of(current.market_id),
            "side": "bid" if current.side == Side.BUY else "ask",
            "price": kalshi_dollars(current.price) if current.price is not None else None,
            "client_order_id": current.client_order_id,
        }
        if not price_changed and new_amount is not None and new_amount < current.amount:
            raw = await self._call(
                "POST", f"/portfolio/events/orders/{request.order_id}/decrease",
                json={"reduce_to": kalshi_count(new_amount - current.filled), "market_ticker": ticker_of(current.market_id)},
                kind="write", priority=Priority.HIGH,
            )
            order = self._edited(raw, body=as_sent, current=current)
            return order.model_copy(update={"queue_priority_preserved": True})
        if request.price is None and new_amount is None:
            return current
        price = D(request.price) if request.price is not None else current.price
        amount = new_amount if new_amount is not None else current.amount
        if price is None:
            raise InvalidOrder("kalshi: the order has no price to amend")
        body = {
            "ticker": ticker_of(current.market_id),
            "side": "bid" if current.side == Side.BUY else "ask",
            "price": kalshi_dollars(validate_price(price, DEFAULT_PRECISION)),
            "count": kalshi_count(validate_amount(amount, DEFAULT_PRECISION)),
            "client_order_id": current.client_order_id,
            "updated_client_order_id": request.client_order_id or str(uuid.uuid4()),
        }
        raw = await self._call(
            "POST", f"/portfolio/events/orders/{request.order_id}/amend", json=body,
            kind="write", priority=Priority.HIGH,
        )
        order = self._edited(raw, body={**body, "client_order_id": body["updated_client_order_id"]}, current=current)
        return order.model_copy(update={"queue_priority_preserved": False})

    def _edited(self, raw: dict[str, Any], *, body: dict[str, Any], current: Order) -> Order:
        """An amend or decrease answer as an `Order`. The venue answers either
        with the full order (`{"order": ...}`) or with the counts alone; the
        counts alone omit what has filled, which the order read before the
        edit supplies."""
        if isinstance(raw, dict) and isinstance(raw.get("order"), dict):
            return order_of(raw["order"], account=current.account)
        answer = dict(raw or {})
        if answer.get("remaining_count") in (None, "") and body.get("count") not in (None, ""):
            # The amend answer carries no counts at all; the total it accepted
            # is the one asked for, less what had already filled.
            answer["remaining_count"] = kalshi_count(D(body["count"]) - current.filled)
        order = order_from_response(answer, body=body, account=current.account, filled=current.filled)
        return order.model_copy(update={"id": order.id or current.id, "created_at": current.created_at})

    async def fetch_order(self, order_id: str, *, attempts: int = 3) -> Order:
        """One order by id.

        The venue's order store is a beat behind order entry: an order just
        placed can answer 404 for a few hundred milliseconds. A miss is
        retried briefly before it is reported, so read-after-write works.
        """
        import asyncio

        for attempt in range(attempts):
            try:
                raw = await self._call("GET", f"/portfolio/orders/{order_id}")
            except (MarketNotFound, OrderNotFound) as exc:
                if attempt + 1 < attempts:
                    await asyncio.sleep(0.25 * (attempt + 1))
                    continue
                raise OrderNotFound(f"kalshi: no order {order_id}", body=exc.body, status=exc.status) from None
            inner = raw.get("order") if isinstance(raw, dict) else None
            if inner:
                return order_of(inner, account=self.account)
        raise OrderNotFound(f"kalshi: no order {order_id}")

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        orders: list[Order] = []
        cursor: str | None = None
        while True:
            page = await self.fetch_orders(status="resting", market_id=market_id, limit=200, cursor=cursor)
            orders.extend(page)
            cursor = page.next_cursor
            if not cursor or not page:
                return orders

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """`status` is one of the venue's own words: resting, canceled, executed."""
        raw = await self._call("GET", "/portfolio/orders", params={
            "status": status, "ticker": ticker_of(market_id) if market_id else None,
            "min_ts": int(since / 1000) if since else None,
            "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("orders") or []
        return Page([order_of(r, account=self.account) for r in rows], next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        raw = await self._call("GET", "/portfolio/fills", params={
            "ticker": ticker_of(market_id) if market_id else None, "order_id": order_id,
            "min_ts": int(since / 1000) if since else None,
            "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("fills") or []
        return Page([fill_of(r, account=self.account) for r in rows], next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_queue_position(self, order_id: str) -> Decimal:
        raw = await self._call("GET", f"/portfolio/orders/{order_id}/queue_position")
        return D((raw or {}).get("queue_position_fp") or "0")

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        target = account or self.account
        raw = await self._call("GET", "/portfolio/balance", params={
            "subaccount": int(target.subaccount) if target.subaccount is not None else None,
        })
        return balance_of(raw, account=target)

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        positions: list[Position] = []
        cursor: str | None = None
        while True:
            raw = await self._call("GET", "/portfolio/positions", params={
                "ticker": ticker_of(market_id) if market_id else None,
                "event_ticker": ids.native(VENUE, event_id) if event_id else None, "count_filter": "position",
                "limit": 200, "cursor": cursor,
            })
            rows = (raw or {}).get("market_positions") or []
            positions.extend(position_of(r, account=self.account) for r in rows)
            cursor = (raw or {}).get("cursor") or None
            if not cursor or not rows:
                return [p for p in positions if p.contracts > 0]

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        raw = await self._call("GET", "/portfolio/settlements", params={
            "ticker": ticker_of(market_id) if market_id else None, "min_ts": int(since / 1000) if since else None,
            "limit": limit, "cursor": cursor,
        })
        rows = (raw or {}).get("settlements") or []
        return Page([settlement_of(r, account=self.account) for r in rows], next_cursor=(raw or {}).get("cursor") or None)

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The venue's published fee for the market's series, evaluated at
        `price` and `amount` -- the same formula the read API's
        `FeeSchedule.estimate` uses. The fee is symmetric in the YES price,
        so `side` does not change it."""
        ticker = ticker_of(market_id)
        series_id = ticker.split("-")[0]
        raw = await self._call("GET", f"/series/{series_id}")
        series = (raw or {}).get("series") or {}
        if not series.get("fee_type"):
            raise NotSupported(f"kalshi: series {series_id} publishes no fee schedule")
        schedule = FeeSchedule(
            venue=VENUE, scope="series", scope_id=series_id, fee_type=str(series["fee_type"]),
            multiplier=to_float(series.get("fee_multiplier")), rounding="up_to_cent", info=series,
        )
        taker = schedule.estimate(float(price), float(amount), taker=True)
        maker = schedule.estimate(float(price), float(amount), taker=False)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ticker), side=side, price=D(price), amount=D(amount),
            taker_fee=D(str(taker)) if taker is not None else None,
            maker_fee=D(str(maker)) if maker is not None else None,
            currency="USD",
            info={"schedule": schedule.model_dump()},
        )

    # -- budget ---------------------------------------------------------------

    async def fetch_limits(self) -> dict[str, Any]:
        """The account's live budget, adopted by the limiter.

        Reads `GET /account/limits` (refill rate and capacity per bucket and
        the usage tier) and `GET /account/endpoint_costs` (the endpoints that
        cost other than the default 10 tokens), and reconfigures the limiter
        to them. Call once at start-up; the tier changes with volume.
        """
        limits = await self._call("GET", "/account/limits")
        costs = await self._call("GET", "/account/endpoint_costs")
        read, write = (limits or {}).get("read") or {}, (limits or {}).get("write") or {}
        if read.get("refill_rate") and write.get("refill_rate"):
            self.limiter.configure(
                read_per_second=float(read["refill_rate"]), write_per_second=float(write["refill_rate"]),
            )
        rows = (costs or {}).get("endpoint_costs") or []
        if rows:
            self.endpoint_costs = {
                (str(row.get("method") or "").upper(), str(row.get("path") or "")): int(row.get("cost") or 0)
                for row in rows
            }
        if (costs or {}).get("default_cost"):
            self.default_cost = int(costs["default_cost"])
        return {"limits": limits, "endpoint_costs": costs}

    # -- order groups ---------------------------------------------------------

    async def create_order_group(self, contracts_limit: Decimal | int) -> str:
        """A venue-side kill switch: a rolling 15-second contract limit that,
        when breached, cancels every order in the group and blocks new ones
        until `reset_order_group`. Orders join a group through
        `params={"order_group_id": ...}` on the request."""
        raw = await self._call(
            "POST", "/portfolio/order_groups/create",
            json={"contracts_limit_fp": kalshi_count(D(contracts_limit))}, kind="write",
        )
        return str((raw or {}).get("order_group_id") or "")

    async def trigger_order_group(self, order_group_id: str) -> None:
        """Cancel every order in the group now and block new ones -- the kill switch, pulled."""
        await self._call("PUT", f"/portfolio/order_groups/{order_group_id}/trigger", json={}, kind="write", priority=Priority.HIGH)

    async def reset_order_group(self, order_group_id: str) -> None:
        await self._call("PUT", f"/portfolio/order_groups/{order_group_id}/reset", json={}, kind="write")

    async def update_order_group_limit(self, order_group_id: str, contracts_limit: Decimal | int) -> None:
        await self._call(
            "PUT", f"/portfolio/order_groups/{order_group_id}/limit",
            json={"contracts_limit_fp": kalshi_count(D(contracts_limit))}, kind="write",
        )

    async def fetch_order_group(self, order_group_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/portfolio/order_groups/{order_group_id}")

    async def delete_order_group(self, order_group_id: str) -> None:
        await self._call("DELETE", f"/portfolio/order_groups/{order_group_id}", kind="write", priority=Priority.HIGH)

    # -- RFQ ------------------------------------------------------------------

    async def create_rfq(
        self, market_id: str, contracts: Decimal | int, *, rest_remainder: bool = False,
        target_cost: Decimal | None = None,
    ) -> str:
        """Ask market makers for a two-sided quote on `contracts` of a market.
        `rest_remainder` leaves what a quote does not fill resting on the
        book. Returns the RFQ id; quotes arrive through `fetch_quotes`."""
        body: dict[str, Any] = {
            "market_ticker": ticker_of(market_id), "contracts_fp": kalshi_count(D(contracts)), "rest_remainder": rest_remainder,
        }
        if target_cost is not None:
            body["target_cost_dollars"] = kalshi_dollars(D(target_cost))
        raw = await self._call("POST", "/communications/rfqs", json=body, kind="write")
        return str((raw or {}).get("id") or "")

    async def fetch_rfq(self, rfq_id: str) -> dict[str, Any]:
        raw = await self._call("GET", f"/communications/rfqs/{rfq_id}")
        return dict((raw or {}).get("rfq") or {})

    async def fetch_quotes(self, rfq_id: str, *, status: str | None = "open") -> list[dict[str, Any]]:
        """Quotes answering one of this account's RFQs. The venue filters by
        the asking account, not by RFQ, so the RFQ is picked out here."""
        raw = await self._call("GET", "/communications/quotes", params={
            "rfq_user_filter": "self", "status": status, "limit": 500,
        })
        return [q for q in ((raw or {}).get("quotes") or []) if str(q.get("rfq_id")) == rfq_id]

    async def accept_quote(self, rfq_id: str, quote_id: str, side: str) -> None:
        """Take one side (`yes` or `no`) of a quote. The quoter then has a
        last look and confirms; the trade prints on their confirmation."""
        await self._call(
            "PUT", f"/communications/rfqs/{rfq_id}/quotes/{quote_id}/accept",
            json={"accepted_side": side}, kind="write",
        )

    async def delete_rfq(self, rfq_id: str) -> None:
        await self._call("DELETE", f"/communications/rfqs/{rfq_id}", kind="write", priority=Priority.HIGH)

    async def close(self) -> None:
        await self.http.close()


def _path_of(url: str) -> str:
    """`https://host/trade-api/v2` -> `/trade-api/v2`, the prefix the signature covers."""
    from urllib.parse import urlsplit

    return urlsplit(url).path.rstrip("/")


def _matches(pattern: list[str], segments: list[str]) -> bool:
    for index, part in enumerate(pattern):
        if part.startswith("*"):
            return index < len(segments)
        if index >= len(segments):
            return False
        if part.startswith(":"):
            continue
        if part != segments[index]:
            return False
    return len(pattern) == len(segments)


def _clean(params: Any) -> Any:
    if params is None:
        return None
    return {key: value for key, value in params.items() if value is not None}
