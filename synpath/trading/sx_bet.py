"""SX Bet order entry, and the venue's order and fill records on the YES leg.

Built to SX Bet's documentation and OpenAPI (OBv3, the exchange's third order
book): order hashing and signatures are checked against an independent
EIP-712 encoder.

**Two credentials.** Every request carries an SX Bet API key
(`x-sx-api-key`, self-service at sx.bet under Account); every order is also an
EIP-712 message signed by the wallet itself. The wallet is the order's maker;
the funds sit in a *proxy wallet* the venue derives from it, which must be
deployed (`deploy_proxy()`, once) and funded (sx.bet's deposit) before the
first order.

**An order bets one outcome.** `buy` bets outcome one (this library's YES) at
the price given; `sell` bets outcome two at one minus it. The venue counts the
stake in USDC and the price as the implied probability paid, so an order for
`n` contracts stakes `n * price` (or `n * (1 - price)`), rounded down to the
cent's millionth; prices are snapped onto the venue's ladder (0.125% today)
on the cheaper side. The smallest stake is 1 USDC.

**Limits rest; market orders take.** A limit is `GTC` (post-only on
request), or expires at `expires_at` with `gtd`. A market order, or an IOC
limit, is the venue's `IOC` at the price given as the worst accepted; `fok` is
the venue's `FOK`. Every order is placed with the venue's synchronous mode, so
what matched on arrival comes back in the answer. A resting order the venue
does not see filled before kickoff is swept when the game starts.

**Fees** are charged at settlement on winnings: 1% of a winning taker bet's
profit, nothing for a maker (the venue can set other rates per account).

**Positions** net per market on the YES leg: what the account wins if outcome
one comes in, less what it wins if outcome two does.

The *heartbeat* (`heartbeat(seconds)`) is a dead man's switch: unless renewed
in time, the venue cancels every open order on the account.
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import BadRequest, ExchangeError, MarketNotFound
from ..sx_bet import iso_ms
from ..types import Page
from . import sx_bet_signing as sig
from .base import TradingExchange
from .credentials import SxBetCredentials
from .errors import CredentialsMissing, InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .polymarket_signing import WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, SettlementState, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "sx_bet"
API_URL = "https://api.sx.bet"
TESTNET_API_URL = "https://api.toronto.sx.bet"
USDC = Decimal(1_000_000)
ODDS = Decimal(10) ** 20

DEFAULT_EXPIRY_S = 30 * 86400
"""How long a GTC order is signed to live: the venue needs an expiry, and a
game's orders are swept at kickoff long before this."""

WAIT_MS = 10_000
"""How long `create_order` lets the venue wait for the matching result."""

OUTCOME_STATUS = {
    "RESTED": OrderStatus.OPEN,
    "PARTIAL_FILL_RESTED": OrderStatus.OPEN,
    "FULLY_FILLED": OrderStatus.CLOSED,
    "PARTIAL_FILL_DONE": OrderStatus.CANCELED,
    "CANCELLED": OrderStatus.CANCELED,
    "TIMEOUT": OrderStatus.PENDING,
}
"""What the venue's synchronous answer says became of an order on arrival."""

INACTIVE_STATUS = {
    "FILLED": OrderStatus.CLOSED,
    "EXPIRED": OrderStatus.EXPIRED,
}
"""An inactive order's state by its `inactiveReason`; every other reason
(cancelled by the account, the venue, a heartbeat, kickoff) is a cancel."""


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def yes_price(odds: Any, outcome_one: bool) -> Decimal:
    """An outcome's 1e20-scaled odds as the YES price."""
    price = D(odds) / ODDS
    return price if outcome_one else Decimal("1") - price


def side_of(outcome_one: bool) -> Side:
    return Side.BUY if outcome_one else Side.SELL


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def order_of_row(row: dict[str, Any], *, account: Account | None = None) -> Order:
    """One order as the venue lists it (`GET /orders-v3`, the orders channel).
    Stakes become contracts at the order's own odds."""
    outcome_one = bool(row.get("isBettingOutcomeOne"))
    odds = int(D(row.get("percentageOdds")))
    total, remaining = int(D(row.get("totalBetSize"))), int(D(row.get("remainingSize")))
    amount = sig.contracts_of(total, odds)
    left = sig.contracts_of(remaining, odds)
    native = str(row.get("status") or "")
    reason = str(row.get("inactiveReason") or "")
    if native == "ACTIVE":
        status = OrderStatus.OPEN
    elif native == "PENDING":
        status = OrderStatus.PENDING
    else:
        status = INACTIVE_STATUS.get(reason, OrderStatus.CANCELED)
    filled = max(Decimal("0"), amount - left) if status != OrderStatus.CLOSED else amount
    return Order(
        id=str(row.get("id") or row.get("orderId") or ""),
        client_order_id=row.get("clientOrderId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketHash") or "").lower()),
        side=side_of(outcome_one),
        type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        status=status,
        price=yes_price(odds, outcome_one),
        amount=amount,
        filled=filled,
        remaining=left if status in (OrderStatus.OPEN, OrderStatus.PENDING) else Decimal("0"),
        expires_at=iso_ms(row.get("expiry")),
        created_at=iso_ms(row.get("createdAt")),
        updated_at=iso_ms(row.get("updatedAt")),
        info={k: v for k, v in row.items() if k != "orderSignature"},
    )


def fill_of_row(row: dict[str, Any], *, account: Account | None = None) -> Fill | None:
    """One fill (`GET /fills-v3`, the fills channel): the contracts are its
    return, one dollar each; the price its odds, on the YES leg."""
    if row.get("isParlay"):
        return None
    outcome_one = bool(row.get("isBettingOutcomeOne"))
    state = str(row.get("status") or "")
    return Fill(
        id=str(row.get("id") or row.get("matchId") or ""),
        order_id=str(row.get("orderId") or ""),
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketHash") or "").lower()),
        side=side_of(outcome_one),
        price=yes_price(row.get("fillOdds"), outcome_one),
        amount=D(row.get("returnAmount")) / USDC,
        fee=None,
        fee_currency="USDC",
        liquidity=Liquidity.MAKER if row.get("isMaker") else Liquidity.TAKER if row.get("isMaker") is False else Liquidity.UNKNOWN,
        settlement=SettlementState.FAILED if state == "FAILED" else
        SettlementState.MATCHED if state == "MATCHED" else SettlementState.CONFIRMED,
        timestamp=iso_ms(row.get("createdAt")) or 0,
        info=row,
    )


def position_of(row: dict[str, Any], *, account: Account | None = None) -> Position:
    """One market's bets netted on the YES leg: what the account makes if
    outcome one comes in, less what it makes if outcome two does."""
    best, worst = D(row.get("maxWin")) / USDC, D(row.get("maxLoss")) / USDC
    spread = best - worst
    net = spread if row.get("isOutcomeOneMaxWin") else -spread
    market = row.get("market") or {}
    settled = market.get("outcome") is not None or market.get("reportedDate") is not None
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketHash") or "").lower()),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        margin=D(row.get("totalStake")) / USDC,
        realized_pnl=D(row["pnl"]) / USDC if row.get("pnl") not in (None, "") else None,
        resolved=settled,
        final=settled,
        timestamp=iso_ms(row.get("updatedAt")),
        info=row,
    )


def reason_of(body: Any, fallback: str) -> str:
    if isinstance(body, dict):
        message = body.get("message")
        if isinstance(message, list):
            return "; ".join(str(m) for m in message)
        if message:
            return str(message)
    return fallback


def error_of(exc: ExchangeError) -> ExchangeError:
    """The venue's refusal of an order as a typed error, by its code."""
    text = reason_of(exc.body, str(exc))
    upper = text.upper()
    if "PROXY_NOT_DEPLOYED" in upper or "INVALID_PROXY" in upper:
        return CredentialsMissing(f"{VENUE}: {text} -- deploy the proxy wallet once with deploy_proxy(), then fund it on sx.bet")
    if "BALANCE" in upper or "INSUFFICIENT" in upper:
        return InsufficientFunds(f"{VENUE}: {text}", body=exc.body)
    if any(word in upper for word in ("ODDS", "SIZE", "EXPIRY", "LADDER", "INVALID")):
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=text.lower() or None,
                         info=exc.body if isinstance(exc.body, dict) else {}, body=exc.body)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def venue_time_in_force(request: OrderRequest) -> str:
    if request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC:
        return "IOC"
    if request.time_in_force == TimeInForce.FOK:
        return "FOK"
    return "GTC"


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder(f"{VENUE}: day orders are the engine's; gtc, gtd, ioc or fok")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent at the worst price you give")
    if request.reduce_only:
        raise InvalidOrder(f"{VENUE}: the venue nets every bet on a market; there is no reduce-only order")
    if request.post_only and venue_time_in_force(request) != "GTC":
        raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")


def build_signed_order(
    request: OrderRequest, *, signer: WalletSigner, meta: dict[str, Any], salt: int | None = None,
    now: float | None = None,
) -> tuple[dict[str, Any], str, int, int]:
    """`(body item, order id, odds, stake)` for one request, before anything is sent."""
    check_request(request)
    outcome_one = request.side == Side.BUY
    limits = meta.get("limits") or {}
    asset = meta.get("activeAsset") or {}
    odds, stake = sig.bet(
        outcome_one, D(request.price), D(request.amount),
        step_size=int(meta.get("oddsLadderStepSize") or 125),
        min_stake=int(limits.get("orderSizeMinimumBaseUnits") or 1_000_000),
    )
    clock = time.time() if now is None else now
    if request.time_in_force == TimeInForce.GTD and request.expires_at:
        expiry = int(request.expires_at // 1000)
        if expiry <= clock:
            raise InvalidOrder(f"{VENUE}: expires_at is in the past")
    else:
        expiry = int(clock) + DEFAULT_EXPIRY_S
    order = sig.build_order(
        market_hash=ids.native(VENUE, request.market_id).lower(), base_token=str(asset["baseToken"]),
        stake=stake, odds=odds, expiry=expiry, maker=signer.address, outcome_one=outcome_one,
        salt=salt if salt is not None else sig.new_salt(),
    )
    domain = meta["domain"]
    item = sig.wire_order(order, signature=sig.sign_order(signer, order, domain=domain),
                          time_in_force=venue_time_in_force(request))
    if request.post_only:
        item["postOnly"] = True
    if request.client_order_id:
        item["clientOrderId"] = request.client_order_id
    return item, sig.order_id(order, domain=domain), odds, stake


def order_after_placing(
    request: OrderRequest, result: dict[str, Any], *, order_id: str, odds: int, stake: int,
    account: Account,
) -> Order:
    """The order from the venue's synchronous answer: what matched on arrival
    and whether the rest is resting."""
    outcome = result.get("outcome") or {}
    state = str(outcome.get("state") or "TIMEOUT")
    status = OUTCOME_STATUS.get(state, OrderStatus.PENDING)
    amount = sig.contracts_of(stake, odds)
    blended = int(D(outcome.get("blendedOdds")))
    fill_stake = int(D(outcome.get("fillAmount")))
    filled = sig.contracts_of(fill_stake, blended) if blended else Decimal("0")
    outcome_one = request.side == Side.BUY
    if status == OrderStatus.CLOSED:
        filled = max(filled, amount) if filled else amount
    remaining = Decimal("0")
    if status == OrderStatus.OPEN:
        remaining = sig.contracts_of(int(D(outcome.get("remainingAmount"))), odds) if outcome.get("remainingAmount") else amount - filled
    elif status == OrderStatus.PENDING:
        remaining = amount
    return Order(
        id=order_id,
        client_order_id=request.client_order_id,
        venue=VENUE,
        account=request.account or account,
        market_id=ids.qualify(VENUE, ids.native(VENUE, request.market_id).lower()),
        side=request.side,
        type=request.type,
        time_in_force=request.time_in_force,
        status=status,
        price=yes_price(odds, outcome_one),
        amount=amount,
        filled=filled,
        remaining=max(Decimal("0"), remaining),
        average_price=yes_price(blended, outcome_one) if blended else None,
        post_only=request.post_only,
        created_at=int(time.time() * 1000),
        book=request.book,
        trader=request.trader,
        tags=request.tags,
        info={"result": result, "stake": stake, "odds": odds},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class SxBetTrading(TradingExchange):
    """SX Bet order entry.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.sx_bet import SxBetTrading

    async with SxBetTrading(require("sx_bet", load_credentials())) as sx:
        print(await sx.fetch_balance())
    ```
    """

    id = VENUE
    name = "SX Bet"
    has: dict[str, Capability] = {
        "create_order": True,
        # Up to ten a request, one maker; not atomic, one result each.
        "create_orders": True,
        "cancel_order": True,
        # Up to 100 a request.
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        # The venue lists only active orders; a closed one is read by id.
        "fetch_orders": False,
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
        credentials: SxBetCredentials,
        *,
        account_name: str = "default",
        url: str | None = None,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.testnet = bool(credentials.testnet)
        self.signer = WalletSigner(credentials.private_key)
        self.account = Account(venue=VENUE, name=account_name)
        self.limiter = limiter or BudgetLimiter(read_per_second=4, write_per_second=10)
        self.url = url or (TESTNET_API_URL if self.testnet else API_URL)
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(self.url, limiter=None, client=shared, venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            from ..sx_bet import SxBet

            catalog = SxBet(api_url=self.url)
        self.catalog = catalog
        """The read adapter: fee schedules."""
        self._meta: dict[str, Any] | None = None

    @property
    def address(self) -> str:
        """The wallet: the orders' maker, and the account the API key belongs to."""
        return self.signer.address

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        """One call with the API key, paced; the venue's `data` unwrapped."""
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        headers = {"x-sx-api-key": self.credentials.api_key}
        query = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            payload = await self.http.request(method, path, params=query or None, json=body, headers=headers)
        except MarketNotFound as exc:
            if path.startswith("/orders"):
                raise OrderNotFound(str(exc), body=exc.body, status=exc.status) from None
            raise
        if isinstance(payload, dict) and payload.get("status") == "failure":
            raise ExchangeError(f"{VENUE}: {path} failed: {reason_of(payload, 'failure')}", body=payload)
        return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload

    async def metadata(self, *, fresh: bool = False) -> dict[str, Any]:
        """The exchange's signing domain, token, odds ladder and limits. Read
        once and kept."""
        if self._meta is None or fresh:
            self._meta = await self._call("GET", "/metadata/obv3") or {}
        return self._meta

    async def proxy(self) -> dict[str, Any]:
        """The proxy wallet that holds this account's funds, and whether it is
        deployed yet."""
        return await self._call("GET", "/user/proxy") or {}

    async def deploy_proxy(self) -> dict[str, Any]:
        """Deploy the proxy wallet, once, before the first order. The venue
        does it on chain in a few seconds; `proxy()` says when it is done."""
        return await self._call("POST", "/user/deploy-proxy", kind="write") or {}

    async def heartbeat(self, timeout_s: int) -> dict[str, Any]:
        """Arm or renew the dead man's switch: unless this is called again
        within `timeout_s` seconds (at most 3600), the venue cancels every open
        order on the account. `0` disarms it."""
        if not 0 <= int(timeout_s) <= 3600:
            raise BadRequest(f"{VENUE}: the heartbeat timeout is 0 to 3600 seconds")
        return await self._call("POST", "/heartbeat/v3", body={"timeoutSeconds": int(timeout_s)},
                                kind="write", priority=Priority.HIGH) or {}

    # -- orders ---------------------------------------------------------------

    async def _place(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        try:
            raw = await self._call("POST", "/orders-v3", body={
                "orders": items, "waitForOutcome": True, "maxWaitTime": WAIT_MS,
            }, kind="write") or {}
        except (BadRequest, OrderNotFound) as exc:
            raise error_of(exc) from None
        return list(raw.get("orders") or [])

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign and place one order. The venue answers with what matched on
        arrival; an immediate order's rest is cancelled by the venue."""
        result = await self.create_orders([request])
        if isinstance(result[0], Exception):
            raise result[0]
        return result[0]

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, ten a request, one result each in the order asked for.
        A refused order comes back as its exception."""
        meta = await self.metadata()
        batch = int((meta.get("limits") or {}).get("maxCreateOrders") or 10)
        out: list[Order | Exception] = []
        for start in range(0, len(requests), batch):
            chunk = requests[start:start + batch]
            built: list[tuple[OrderRequest, dict[str, Any], str, int, int] | Exception] = []
            for request in chunk:
                try:
                    item, order_id, odds, stake = build_signed_order(request, signer=self.signer, meta=meta)
                    built.append((request, item, order_id, odds, stake))
                except InvalidOrder as exc:
                    built.append(exc)
            ready = [b for b in built if not isinstance(b, Exception)]
            results = await self._place([b[1] for b in ready]) if ready else []
            answers = iter(results)
            for entry in built:
                if isinstance(entry, Exception):
                    out.append(entry)
                    continue
                request, _, order_id, odds, stake = entry
                result = next(answers, {})
                if str(result.get("status") or "") != "SUBMITTED":
                    out.append(OrderRejected(f"{VENUE}: order {order_id} was not submitted: {result}",
                                             reason="failed", info=result))
                    continue
                out.append(order_after_placing(request, result, order_id=str(result.get("orderId") or order_id).lower(),
                                               odds=odds, stake=stake, account=self.account))
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back."""
        result = (await self.cancel_orders([order_id]))[0]
        if isinstance(result, Exception):
            raise result
        return result

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel up to 100 orders a request, then read each back. A cancel
        the venue could not confirm in time is read back as it stands."""
        meta = await self.metadata()
        batch = int((meta.get("limits") or {}).get("maxCancelOrders") or 100)
        refused: dict[str, str] = {}
        for start in range(0, len(order_ids), batch):
            raw = await self._call("DELETE", "/orders-v3", body={
                "orders": [{"orderId": o} for o in order_ids[start:start + batch]],
            }, kind="write", priority=Priority.HIGH) or {}
            for item in raw.get("notCancelled") or []:
                if isinstance(item, dict) and item.get("orderId"):
                    refused[str(item["orderId"]).lower()] = str(item.get("reason") or "refused")
        out: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                order = await self.fetch_order(order_id)
            except OrderNotFound as exc:
                out.append(exc)
                continue
            if order_id.lower() in refused and order.status == OrderStatus.OPEN:
                out.append(OrderRejected(f"{VENUE}: cancel of {order_id} refused: {refused[order_id.lower()]}",
                                         reason="cancel_refused"))
                continue
            out.append(order)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every open order on a market, or on the account. The venue submits
        the cancels and returns at once; returns how many it submitted."""
        if market_id:
            raw = await self._call("DELETE", "/orders-v3/market",
                                   params={"marketHash": ids.native(VENUE, market_id).lower()},
                                   kind="write", priority=Priority.HIGH) or {}
        else:
            raw = await self._call("DELETE", "/orders-v3/all", kind="write", priority=Priority.HIGH) or {}
        return len(raw.get("cancelledSubmitted") or [])

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id, in any state."""
        raw = await self._call("GET", f"/orders-v3/{order_id.lower()}")
        row = raw.get("order") if isinstance(raw, dict) and isinstance(raw.get("order"), dict) else raw
        if not isinstance(row, dict) or not row:
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return order_of_row(row, account=self.account)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting order, or every one on a market, page by page."""
        orders: list[Order] = []
        key = None
        for _ in range(50):
            raw = await self._call("GET", "/orders-v3", params={
                "marketHash": ids.native(VENUE, market_id).lower() if market_id else None,
                "perPage": 100, "nextKey": key,
            }) or {}
            orders += [order_of_row(row, account=self.account) for row in raw.get("orders") or []]
            key = raw.get("nextKey")
            if not key:
                break
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of this account's fills, newest first; parlays are left
        out. The venue filters by order but not by market, so a market is
        filtered here."""
        raw = await self._call("GET", "/fills-v3", params={
            "orderId": order_id.lower() if order_id else None,
            "startDate": _iso(since) if since else None,
            "perPage": min(limit or 50, 100), "nextKey": cursor,
        }) or {}
        fills = [f for f in (fill_of_row(row, account=self.account) for row in raw.get("fills") or []) if f]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id).lower())
            fills = [f for f in fills if f.market_id == wanted]
        return Page(fills, next_cursor=raw.get("nextKey") or None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The proxy wallet's USDC: what is free to bet, and what matched bets
        hold in escrow (`locked`). Resting orders lock nothing."""
        raw = await self._call("GET", "/user/balance-v3") or {}
        rows = raw.get("balances") if isinstance(raw, dict) else raw
        meta = await self.metadata()
        token = str((meta.get("activeAsset") or {}).get("baseToken") or "").lower()
        row = next((r for r in rows or [] if str(r.get("tokenAddress") or "").lower() == token), None) \
            or next(iter(rows or []), {})
        available = (D(row.get("availableAmount")) + D(row.get("pendingAvailableAmount"))) / USDC
        locked = (D(row.get("escrowedAmount")) + D(row.get("pendingEscrowAmount"))) / USDC
        return Balance(
            venue=VENUE, account=account or self.account, currency="USDC",
            total=available + locked, available=available, locked=locked, buying_power=available,
            timestamp=int(time.time() * 1000), info=row,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Open bets (matched or locked), netted per market on the YES leg."""
        positions: list[Position] = []
        key = None
        for _ in range(50):
            raw = await self._call("GET", "/positions-v3", params={
                "status": "MATCHED,LOCKED", "eventId": event_id, "perPage": 100, "nextKey": key,
            }) or {}
            positions += [position_of(row, account=self.account) for row in raw.get("positions") or []]
            key = raw.get("nextKey")
            if not key:
                break
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id).lower())
            positions = [p for p in positions if p.market_id == wanted]
        return positions

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """The taker fee on `amount` contracts at the YES `price` if the bet
        wins: a share of its profit. A `sell` bets outcome two at `1 - price`.
        Makers pay nothing."""
        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        paid = D(price) if side == Side.BUY else Decimal("1") - D(price)
        taker = schedule.estimate(float(paid), float(amount), taker=True)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id).lower()), side=side,
            price=D(price), amount=D(amount),
            taker_fee=D(taker) if taker is not None else None, maker_fee=Decimal("0"),
            currency="USDC", info={"schedule": schedule.model_dump(), "charged": "at settlement, if the bet wins"},
        )

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()


def _iso(ms: int) -> str:
    from ..types import iso

    return iso(ms) or ""


__all__ = ["SxBetTrading", "order_of_row", "fill_of_row", "position_of", "build_signed_order"]
