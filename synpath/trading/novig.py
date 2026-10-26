"""Novig order entry, and the venue's order, fill and position records on the YES leg.

Built to Novig's v3 documentation and OpenAPI document. Every request is
signed (NOVIG-V3, Ed25519 or P-256; see `novig_signing`), and the signer is
checked against the venue's thirty published vectors.

**Two keys.** A subaccount's *trading* key places and cancels orders on that
subaccount and reads its orders, fills, positions and balance; it is what
this adapter trades with (`NOVIG_KEY_ID`, `NOVIG_PRIVATE_KEY_PATH`). The
account's *management* key (made in the web app under Profile -> Settings ->
Novig API) opens, funds and labels subaccounts and issues their trading keys,
and never places an order; the subaccount methods here use it when it is
configured (`NOVIG_MANAGEMENT_KEY_ID`, `NOVIG_MANAGEMENT_PRIVATE_KEY_PATH`).

**An order buys one outcome.** `buy` buys the market's YES outcome (the one
Novig lists first) at the price given; `sell` buys the NO outcome at one minus
it. Nothing sells an outcome held: an order with `action=sell` is refused. Prices are snapped onto the venue's grid on the cheaper side (0.001 steps
near the ends, 0.005 in the middle), so an order never pays more than asked.
Sizes are dollar contracts in steps of 0.01: the venue counts 1-cent
contracts, 100 to this library's one.

**Placement is queued.** The venue answers a placement with the order id
before the order reaches the book; the order is `pending` until the private
stream (or a read) says it rests, filled, or was rejected. Limits are `GTC`,
or `GTT` with `gtd` until `expires_at`; post-only is `PO`. A market order, or
an IOC limit, is `IOC` at the price given as the worst accepted; `fok` is
`FOK`. A batch placement is all or nothing.

**Every resting order on an event is voided when it goes in play**, and again
each time play resumes after a stoppage.

**Fees** are charged to the taker per fill, `c * P * (1 - P)` per contract,
and only while the event is in play on a game market; a maker earns a share
of it as a credit. `fetch_my_trades` reports each fill's fee.

**Positions** are held per outcome, so a market's position carries both
inventories and nets them on the YES leg.

Production trades real money and requires identity verification and a
location check (Arizona, Michigan and Nevada are restricted); Paper is the
same API with play money.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from decimal import Decimal
from typing import Any

from .. import ids
from ..base import AsyncHttpClient, Capability
from ..errors import AuthenticationError, BadRequest, ExchangeError, MarketNotFound
from ..types import Page
from . import novig_signing as sig
from .base import TradingExchange
from .credentials import NovigCredentials
from .errors import (
    CredentialsMissing, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected, PermissionDenied,
)
from .limiter import BudgetLimiter, Priority
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "novig"
API_URLS = {"production": "https://api.novig.com", "paper": "https://api.paper.novig.com"}

BATCH = 256
"""Orders one batch placement or cancel takes."""

PAGE = 500
MAX_PAGES = 50

CLIENT_ID_NAMESPACE = uuid.UUID("6b1d3c5e-0f5a-4c47-9d0b-6e6f76696701")
"""A `client_order_id` that is not a UUID is sent as a UUID derived from it
under this namespace, so the same id always maps to the same `clientId`."""

STATUS = {
    "PENDING": OrderStatus.PENDING,
    "OPEN": OrderStatus.OPEN,
    "FILLED": OrderStatus.CLOSED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
}
"""The venue's five order states. A partly filled order stays `OPEN`."""

STATUS_FILTER = {
    OrderStatus.PENDING: "PENDING", OrderStatus.OPEN: "OPEN", OrderStatus.CLOSED: "FILLED",
    OrderStatus.CANCELED: "CANCELED", OrderStatus.REJECTED: "REJECTED",
}

TIME_IN_FORCE = {"GTC": TimeInForce.GTC, "GTT": TimeInForce.GTD, "IOC": TimeInForce.IOC, "FOK": TimeInForce.FOK,
                 "PO": TimeInForce.GTC}

ONE = Decimal("1")


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def yes_price(price: Any, on_yes: bool) -> Decimal:
    """An outcome's price as the YES price."""
    return D(price) if on_yes else ONE - D(price)


def side_of(on_yes: bool) -> Side:
    return Side.BUY if on_yes else Side.SELL


def client_id(client_order_id: str | None) -> str | None:
    """The venue's `clientId` (a UUID) for a client order id."""
    if not client_order_id:
        return None
    try:
        return str(uuid.UUID(client_order_id))
    except ValueError:
        return str(uuid.uuid5(CLIENT_ID_NAMESPACE, client_order_id))


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def order_of_row(row: dict[str, Any], *, yes_outcome: str, account: Account | None = None,
                 client_order_id: str | None = None) -> Order:
    """One order as the venue reports it: an order record (`GET /v3/orders`),
    or a resting order (`GET /v3/account/orders`, the stream's `open`), whose
    `qty` is what rests."""
    on_yes = str(row.get("outcomeId")) == yes_outcome
    native = str(row.get("status") or "OPEN")
    status = STATUS.get(native, OrderStatus.OPEN)
    quantity = sig.contracts_of(row.get("qty"))
    remaining = sig.contracts_of(row["remaining"]) if row.get("remaining") is not None else (
        quantity if status in (OrderStatus.OPEN, OrderStatus.PENDING) else Decimal("0"))
    if status == OrderStatus.CLOSED:
        remaining = Decimal("0")
    tif = str(row.get("tif") or "GTC")
    expires = row.get("expiresTs") or row.get("expiresAt")
    return Order(
        id=str(row.get("orderId") or ""),
        client_order_id=client_order_id or row.get("clientId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketId") or "")),
        side=side_of(on_yes),
        outcome="yes" if on_yes else "no",
        action="buy",
        type=OrderType.LIMIT,
        time_in_force=TIME_IN_FORCE.get(tif, TimeInForce.GTC),
        status=status,
        price=yes_price(row.get("price"), on_yes),
        quantity=quantity,
        filled=max(Decimal("0"), quantity - remaining),
        remaining=remaining,
        post_only=tif == "PO",
        expires_at=int(expires) if expires else None,
        created_at=row.get("createdTs"),
        info=row,
    )


def fill_of_row(row: dict[str, Any], *, yes_outcome: str, market_id: str | None = None,
                account: Account | None = None) -> Fill:
    """One fill: from `GET /v3/portfolio/fills`, whose price is
    `100 * cost / qty` and whose `fee` is present only on a charged fill, or
    from the stream's `fill` event, which carries the price and no fee."""
    on_yes = str(row.get("outcomeId")) == yes_outcome
    qty = int(row.get("qty") or 0)
    if row.get("price") not in (None, ""):
        paid = D(row["price"])
    else:
        paid = (D(row.get("cost")) * sig.QTY_PER_CONTRACT / qty) if qty else Decimal("0")
    fee = D(row["fee"]) if row.get("fee") not in (None, "") else (Decimal("0") if "cost" in row else None)
    taker = row.get("taker")
    return Fill(
        id=str(row.get("fillId") or ""),
        order_id=str(row.get("orderId") or ""),
        client_order_id=row.get("clientId") or None,
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, str(row.get("marketId") or market_id or "")),
        side=side_of(on_yes),
        price=yes_price(paid, on_yes),
        quantity=sig.contracts_of(qty),
        fee=fee,
        fee_currency="USD",
        liquidity=Liquidity.TAKER if taker is True else Liquidity.MAKER if taker is False else Liquidity.UNKNOWN,
        timestamp=int(row.get("ts") or time.time() * 1000),
        info=row,
    )


def position_of(market_id: str, rows: list[dict[str, Any]], *, yes_outcome: str,
                account: Account | None = None) -> Position:
    """A market's holdings of both outcomes, netted on the YES leg. `cost` is
    what was paid for each; the entry price is given when one side is held."""
    held = {"yes": Decimal("0"), "no": Decimal("0")}
    cost = {"yes": Decimal("0"), "no": Decimal("0")}
    for row in rows:
        key = "yes" if str(row.get("outcomeId")) == yes_outcome else "no"
        held[key] += sig.contracts_of(row.get("qty"))
        cost[key] += D(row.get("cost"))
    net = held["yes"] - held["no"]
    entry = None
    if held["yes"] and not held["no"]:
        entry = cost["yes"] / held["yes"]
    elif held["no"] and not held["yes"]:
        entry = ONE - cost["no"] / held["no"]
    return Position(
        venue=VENUE,
        account=account,
        market_id=ids.qualify(VENUE, market_id),
        side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
        contracts=abs(net),
        inventory_yes=held["yes"],
        inventory_no=held["no"],
        entry_price=entry.quantize(Decimal("0.00001")) if entry is not None else None,
        margin=cost["yes"] + cost["no"],
        info={"rows": rows},
    )


def error_of(exc: ExchangeError) -> ExchangeError:
    """The venue's refusal as a typed error, by its stable `code`."""
    body = exc.body if isinstance(exc.body, dict) else {}
    code = str(body.get("code") or "")
    message = str(body.get("message") or exc)
    text = f"{VENUE}: {message}" + (f" ({code})" if code else "")
    if code in ("INSUFFICIENT_BALANCE", "WAGER_LIMIT_EXCEEDED", "POSITION_LIMIT_EXCEEDED"):
        return InsufficientFunds(text, body=exc.body, status=exc.status)
    if code in ("INVALID_PRICE", "QTY_OUT_OF_RANGE", "ORDER_TOO_SMALL", "ORDER_TOO_LARGE", "TTL_REQUIRED",
                "TTL_NOT_ALLOWED", "TTL_OUT_OF_RANGE", "PRICE_BAND_VIOLATION", "INVALID_BODY", "INVALID_REQUEST"):
        return InvalidOrder(text, body=exc.body, status=exc.status)
    if code in ("MARKET_CLOSED", "MARKET_INACTIVE", "MARKET_LOCKED", "EVENT_LOCKED", "EVENT_NOT_TRADABLE",
                "LEAGUE_LOCKED", "COMPETITOR_LOCKED", "NOT_LIVE_TRADABLE", "PAUSED", "SYSTEM_LOCKED",
                "LIQUIDATION_ONLY"):
        return MarketHalted(text, body=exc.body, status=exc.status)
    if code == "ORDER_NOT_FOUND":
        return OrderNotFound(text, body=exc.body, status=exc.status)
    if code in ("OUTCOME_NOT_FOUND", "MARKET_NOT_FOUND", "EVENT_NOT_FOUND"):
        return MarketNotFound(text, body=exc.body, status=exc.status)
    if code == "SIGNATURE_REJECTED":
        return AuthenticationError(f"{text}: check the key id, the key file, NOVIG_ENV and the clock",
                                   body=exc.body, status=exc.status)
    if exc.status in (403, 423, 451) or code in ("SCOPE_INSUFFICIENT", "KYC_REQUIRED", "SELF_EXCLUDED",
                                                 "ACCOUNT_LOCKED", "ACCOUNT_EXCLUDED", "EMPLOYEE_ACCOUNT"):
        hint = ""
        if exc.status == 451 or code.startswith("GEOLOCATION") or "REGION" in code or code == "ANONYMIZED_NETWORK":
            hint = (": Production checks the request's location and the key holder's last device geolocation "
                    "(open the Novig app); Arizona, Michigan, Nevada and VPNs are refused")
        return PermissionDenied(text + hint, body=exc.body, status=exc.status)
    if code == "BATCH_REJECTED":
        return OrderRejected(text, reason="batch_rejected", info=body, body=exc.body, status=exc.status)
    return exc


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def venue_time_in_force(request: OrderRequest) -> str:
    if request.type == OrderType.MARKET or request.time_in_force == TimeInForce.IOC:
        return "IOC"
    if request.time_in_force == TimeInForce.FOK:
        return "FOK"
    if request.post_only:
        return "PO"
    if request.time_in_force == TimeInForce.GTD:
        return "GTT"
    return "GTC"


def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; "
            "submit it through the engine"
        )
    if request.time_in_force == TimeInForce.DAY:
        raise InvalidOrder(f"{VENUE}: day orders are the engine's; gtc, gtd, ioc or fok")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a market order is sent IOC at the worst price you give")
    if request.leg[1] == "sell":
        raise InvalidOrder(f"{VENUE}: every order buys an outcome; there is no selling one held -- buy the other "
                           f"outcome instead (outcome={'no' if request.leg[0] == 'yes' else 'yes'}, action=buy)")
    if request.post_only and (request.type == OrderType.MARKET
                              or request.time_in_force in (TimeInForce.IOC, TimeInForce.FOK)):
        raise InvalidOrder(f"{VENUE}: a post-only order cannot also take at once")
    if not Decimal("0") < D(request.price) < ONE:
        raise InvalidOrder(f"{VENUE}: price {request.price} is outside (0, 1)")


def build_order(
    request: OrderRequest, *, yes_outcome: str, no_outcome: str, now_ms: int | None = None,
) -> dict[str, Any]:
    """The placement body for one request, before anything is sent: the
    outcome bought, its price snapped down onto the grid, whole 1-cent
    contracts, and the time in force."""
    check_request(request)
    buying_yes = request.side == Side.BUY
    paid = D(request.price) if buying_yes else ONE - D(request.price)
    body: dict[str, Any] = {
        "outcomeId": yes_outcome if buying_yes else no_outcome,
        "price": sig.wire_price(sig.snap_down(paid)),
        "qty": sig.qty_of(D(request.quantity)),
        "tif": venue_time_in_force(request),
    }
    if request.time_in_force == TimeInForce.GTD:
        clock = int(time.time() * 1000) if now_ms is None else now_ms
        ttl = int(request.expires_at or 0) - clock
        if ttl <= 0:
            raise InvalidOrder(f"{VENUE}: expires_at is in the past")
        body["ttl"] = ttl
    identifier = client_id(request.client_order_id)
    if identifier:
        body["clientId"] = identifier
    return body


def order_after_placing(request: OrderRequest, answer: dict[str, Any], body: dict[str, Any], *,
                        account: Account) -> Order:
    """The order as placed: queued at the venue, not yet resting."""
    buying_yes = request.side == Side.BUY
    quantity = sig.contracts_of(body["qty"])
    return Order(
        id=str(answer.get("orderId") or ""),
        client_order_id=request.client_order_id,
        venue=VENUE,
        account=request.account or account,
        market_id=ids.qualify(VENUE, ids.native(VENUE, request.market_id)),
        side=request.side,
        outcome="yes" if buying_yes else "no",
        action="buy",
        type=request.type,
        time_in_force=request.time_in_force,
        status=OrderStatus.PENDING,
        price=yes_price(body["price"], buying_yes),
        quantity=quantity,
        filled=Decimal("0"),
        remaining=quantity,
        post_only=request.post_only,
        expires_at=request.expires_at if request.time_in_force == TimeInForce.GTD else None,
        created_at=int(time.time() * 1000),
        book=request.book,
        trader=request.trader,
        tags=request.tags,
        info={"body": body, "answer": answer},
    )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class NovigTrading(TradingExchange):
    """Novig order entry, on one subaccount.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.novig import NovigTrading

    async with NovigTrading(require("novig", load_credentials())) as nv:
        print(await nv.fetch_balance())
    ```
    """

    id = VENUE
    name = "Novig"
    has: dict[str, Capability] = {
        "create_order": True,
        # Up to 256 a request, all or nothing.
        "create_orders": True,
        "cancel_order": True,
        # Up to 256 a request; an unknown id does not stop the rest.
        "cancel_orders": True,
        "cancel_all_orders": True,
        "edit_order": False,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_orders": True,
        "fetch_my_trades": True,
        "fetch_positions": True,
        "fetch_balance": True,
        "fetch_settlements": False,
        "fetch_queue_position": False,
        "fetch_fee_estimate": True,
        # Novig's parlay RFQ is priced by liquidity providers: `synpath.trading.novig_rfq.NovigPricer`.
        "rfq": False,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: NovigCredentials,
        *,
        account_name: str = "default",
        url: str | None = None,
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
        management_client: Any = None,
    ):
        self.credentials = credentials
        self.env = credentials.env
        self.url = (url or API_URLS[credentials.env]).rstrip("/")
        self.signer = sig.NovigSigner(credentials.key_id, credentials.private_key_pem) \
            if credentials.key_id and credentials.private_key_pem else None
        """The subaccount's trading key; `None` with a management key alone."""
        self.management = sig.NovigSigner(credentials.management_key_id, credentials.management_private_key_pem) \
            if credentials.management_key_id and credentials.management_private_key_pem else None
        self.account = Account(venue=VENUE, name=account_name, subaccount=credentials.key_id)
        # The venue's throttles: place 8 Hz, cancel and read 16 Hz.
        self.limiter = limiter or BudgetLimiter(read_per_second=16, write_per_second=8)
        self.http = self._client(client, self.signer, timeout) if self.signer else None
        self.management_http = self._client(management_client, self.management, timeout) if self.management else None
        self._own_catalog = catalog is None
        if catalog is None:
            from ..novig import Novig

            catalog = Novig(env=credentials.env, api_url=self.url, credentials=credentials)
        self.catalog = catalog
        """The read adapter: which outcome of a market is YES, and fee schedules."""

    def _client(self, client: Any, signer: sig.NovigSigner, timeout: float) -> AsyncHttpClient:
        import httpx

        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                             headers={"Accept": "application/json"})
        shared.auth = sig.NovigAuth(signer)
        return AsyncHttpClient(self.url, limiter=None, client=shared, venue=VENUE)

    # -- plumbing -------------------------------------------------------------

    def _trading(self) -> AsyncHttpClient:
        if self.http is None:
            raise CredentialsMissing(
                f"{VENUE}: trading needs a subaccount's trading key; set NOVIG_KEY_ID and NOVIG_PRIVATE_KEY_PATH "
                "(open a subaccount with open_subaccount() and the management key to get one)"
            )
        return self.http

    def _managing(self) -> AsyncHttpClient:
        if self.management_http is None:
            raise CredentialsMissing(
                f"{VENUE}: subaccounts and funding need the management key (Profile -> Settings -> Novig API); "
                "set NOVIG_MANAGEMENT_KEY_ID and NOVIG_MANAGEMENT_PRIVATE_KEY_PATH"
            )
        return self.management_http

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL, cost: float = 1, management: bool = False,
    ) -> Any:
        """One signed call, paced; errors mapped by the venue's code."""
        http = self._managing() if management else self._trading()
        await self.limiter.acquire(cost=cost, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            return await http.request(method, path, params=query or None, json=body)
        except ExchangeError as exc:
            if exc.status == 404 and isinstance(exc.body, dict) and "notCanceled" in exc.body:
                return exc.body
            mapped = error_of(exc)
            if mapped is exc and exc.status == 404 and path.startswith("/v3/orders/"):
                raise OrderNotFound(f"{VENUE}: no order {path.rsplit('/', 1)[-1]}", body=exc.body,
                                    status=exc.status) from None
            raise mapped from None

    async def outcome_ids(self, market_id: str) -> tuple[str, str]:
        """(YES outcome, NO outcome) of a market, from the catalog; kept."""
        return await asyncio.to_thread(self.catalog.outcome_ids, ids.native(VENUE, market_id))

    async def _yes_of(self, market_ids: list[str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for market_id in dict.fromkeys(m for m in market_ids if m):
            out[market_id] = (await self.outcome_ids(market_id))[0]
        return out

    async def _orders(self, rows: list[dict[str, Any]]) -> list[Order]:
        yes = await self._yes_of([str(r.get("marketId") or "") for r in rows])
        return [order_of_row(r, yes_outcome=yes.get(str(r.get("marketId")), ""), account=self.account) for r in rows]

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Place one order. The venue queues it: the result is `pending`."""
        yes, no = await self.outcome_ids(request.market_id)
        body = build_order(request, yes_outcome=yes, no_outcome=no)
        answer = await self._call("POST", "/v3/orders", body=body, kind="write") or {}
        return order_after_placing(request, answer, body, account=self.account)

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, 256 a request, one result each in the order asked for.
        The venue places a batch all or nothing: when it refuses one order,
        every other order of that batch comes back refused too, unplaced."""
        out: list[Order | Exception] = []
        for start in range(0, len(requests), BATCH):
            chunk = requests[start:start + BATCH]
            built: list[tuple[OrderRequest, dict[str, Any]] | Exception] = []
            for request in chunk:
                try:
                    yes, no = await self.outcome_ids(request.market_id)
                    built.append((request, build_order(request, yes_outcome=yes, no_outcome=no)))
                except (InvalidOrder, MarketNotFound) as exc:
                    built.append(exc)
            ready = [b for b in built if not isinstance(b, Exception)]
            answers: list[Any] = []
            refused: ExchangeError | None = None
            if ready:
                try:
                    answers = list(await self._call("POST", "/v3/orders/batch",
                                                    body={"orders": [b[1] for b in ready]},
                                                    kind="write", cost=len(ready)) or [])
                except ExchangeError as exc:
                    refused = exc
            reasons: dict[int, str] = {}
            if refused is not None and isinstance(refused.body, dict):
                for item in refused.body.get("rejected") or []:
                    if isinstance(item, dict) and item.get("index") is not None:
                        reasons[int(item["index"])] = str(item.get("reason") or "refused")
            position = 0
            for entry in built:
                if isinstance(entry, Exception):
                    out.append(entry)
                    continue
                request, body = entry
                index, position = position, position + 1
                if refused is not None:
                    if index in reasons:
                        out.append(InvalidOrder(f"{VENUE}: {reasons[index]}", body=refused.body,
                                                status=refused.status))
                    else:
                        out.append(OrderRejected(f"{VENUE}: not placed: the batch was refused as a whole ({refused})",
                                                 reason="batch_rejected", body=refused.body, status=refused.status))
                    continue
                answer = answers[index] if index < len(answers) and isinstance(answers[index], dict) else {}
                out.append(order_after_placing(request, answer, body, account=self.account))
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order and read it back. The venue queues the cancel, so an
        order still resting when read is reported `pending_cancel`."""
        await self._call("DELETE", f"/v3/orders/{order_id}", kind="write", priority=Priority.HIGH)
        order = await self.fetch_order(order_id)
        if order.status == OrderStatus.OPEN:
            order = order.model_copy(update={"status": OrderStatus.PENDING_CANCEL})
        return order

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        """Cancel up to 256 orders a request, then read each back. An order the
        venue would not cancel (already filled, already cancelled, unknown)
        comes back as it stands, or as `OrderNotFound`."""
        refused: dict[str, str] = {}
        for start in range(0, len(order_ids), BATCH):
            chunk = order_ids[start:start + BATCH]
            raw = await self._call("DELETE", "/v3/orders/batch", body={"orderIds": chunk}, kind="write",
                                   priority=Priority.HIGH, cost=len(chunk)) or {}
            for item in raw.get("notCanceled") or []:
                if isinstance(item, dict) and item.get("orderId"):
                    refused[str(item["orderId"])] = str(item.get("reason") or "refused")
        out: list[Order | Exception] = []
        for order_id in order_ids:
            if refused.get(order_id) == "NOT_FOUND":
                out.append(OrderNotFound(f"{VENUE}: no order {order_id}"))
                continue
            try:
                order = await self.fetch_order(order_id)
            except OrderNotFound as exc:
                out.append(exc)
                continue
            if order.status == OrderStatus.OPEN and order_id not in refused:
                order = order.model_copy(update={"status": OrderStatus.PENDING_CANCEL})
            out.append(order)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        """Every resting order on the subaccount, or on one market. Returns how
        many cancels the venue queued."""
        params = {"market": ids.native(VENUE, market_id)} if market_id else None
        raw = await self._call("DELETE", "/v3/orders", params=params, kind="write", priority=Priority.HIGH) or {}
        return int(raw.get("canceled") or 0)

    async def fetch_order(self, order_id: str) -> Order:
        """One order by id, in any state. Right after placing, the venue can
        answer not-found until the order reaches the book."""
        row = await self._call("GET", f"/v3/orders/{order_id}")
        if not isinstance(row, dict) or not row.get("orderId"):
            raise OrderNotFound(f"{VENUE}: no order {order_id}")
        return (await self._orders([row]))[0]

    async def _paged(self, path: str, params: dict[str, Any], *, limit: int | None = None,
                     cursor: str | None = None, pages: int = MAX_PAGES) -> tuple[list[dict[str, Any]], str | None]:
        rows: list[dict[str, Any]] = []
        after = cursor
        for _ in range(pages):
            raw = await self._call("GET", path, params={**params, "limit": limit or PAGE, "after": after}) or {}
            rows += [r for r in raw.get("items") or [] if isinstance(r, dict)]
            after = raw.get("next") or None
            if not after or limit:
                break
        return rows, after

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting order, or every one on a market, page by page."""
        params = {"status": "OPEN", "market": ids.native(VENUE, market_id) if market_id else None}
        rows, _ = await self._paged("/v3/orders", params)
        return await self._orders(rows)

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of orders in one state (the venue lists one at a time,
        resting ones when none is given): open, pending, closed (filled),
        canceled or rejected. `since` filters on the creation time here."""
        wanted = OrderStatus(status) if status else OrderStatus.OPEN
        if wanted not in STATUS_FILTER:
            raise BadRequest(f"{VENUE}: orders list by one of {', '.join(s.value for s in STATUS_FILTER)}")
        params = {"status": STATUS_FILTER[wanted], "market": ids.native(VENUE, market_id) if market_id else None}
        rows, following = await self._paged("/v3/orders", params, limit=min(limit or PAGE, 5000), cursor=cursor)
        orders = await self._orders(rows)
        if since:
            orders = [o for o in orders if (o.created_at or 0) >= since]
        return Page(orders, next_cursor=following)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """One page of the subaccount's fills, newest first, each with the fee
        it paid (zero when it was not charged)."""
        params = {
            "market": ids.native(VENUE, market_id) if market_id else None, "order": order_id,
            "startsAfter": since - 1 if since else None,
        }
        rows, following = await self._paged("/v3/portfolio/fills", params, limit=min(limit or PAGE, 5000),
                                            cursor=cursor)
        yes = await self._yes_of([str(r.get("marketId") or "") for r in rows])
        fills = [fill_of_row(r, yes_outcome=yes.get(str(r.get("marketId")), ""), account=self.account) for r in rows]
        return Page(fills, next_cursor=following)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The subaccount's balance in USD. The venue reports one figure, so
        what resting orders hold is not split out (`locked` is `None`)."""
        key_id = self.credentials.key_id
        raw = await self._call("GET", f"/v3/account/subaccounts/{key_id}/balance") or {}
        total = D(raw.get("balance"))
        return Balance(
            venue=VENUE, account=account or self.account, currency="USD", total=total, available=total,
            locked=None, timestamp=int(time.time() * 1000), info=raw,
        )

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Open positions, one per market, both outcomes' holdings netted on
        the YES leg."""
        params = {"market": ids.native(VENUE, market_id) if market_id else None,
                  "event": ids.native(VENUE, event_id) if event_id else None}
        raw = await self._call("GET", "/v3/portfolio/positions", params=params) or []
        rows = [r for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []
        by_market: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_market.setdefault(str(row.get("marketId") or ""), []).append(row)
        yes = await self._yes_of(list(by_market))
        return [position_of(m, group, yes_outcome=yes.get(m, ""), account=self.account)
                for m, group in by_market.items()]

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The taker fee on `quantity` contracts at the YES `price`, and the
        maker's credit (negative), at the market's own coefficient. On a game
        market these apply while the event is in play; a fill matched
        pregame pays and earns nothing."""
        from ..novig import fee_estimate

        schedule = await asyncio.to_thread(self.catalog.fetch_fee_schedule, market_id)
        taker, maker = fee_estimate(schedule, D(price), D(quantity))
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), quantity=D(quantity), taker_fee=taker, maker_fee=maker, currency="USD",
            info={"schedule": schedule.model_dump()},
        )

    # -- keys, subaccounts, funding -------------------------------------------

    async def echo(self, body: Any = None, *, management: bool = False) -> Any:
        """`POST /v3/echo`: the body back, which proves the host, key, clock
        and signature are right."""
        return await self._call("POST", "/v3/echo", body=body if body is not None else {}, management=management)

    async def server_time(self) -> int:
        """The venue's clock, in Unix milliseconds."""
        raw = await self._call("GET", "/v3/time", management=self.http is None) or {}
        return int(raw.get("ts") or 0)

    async def limits(self) -> Any:
        """The key's throttle schedule, as the venue reports it."""
        return await self._call("GET", "/v3/limits", management=self.http is None)

    async def subaccounts(self) -> list[dict[str, Any]]:
        """Every live subaccount: its trading key id (its address), label and
        balance. Management key."""
        return list(await self._call("GET", "/v3/account/subaccounts", kind="read", management=True) or [])

    async def account_balance(self) -> Decimal:
        """The cash wallet that funds every subaccount. Management key."""
        raw = await self._call("GET", "/v3/account/balance", management=True) or {}
        return D(raw.get("balance"))

    async def open_subaccount(self, label: str, public_key_pem: str, *, algorithm: sig.Algorithm = "Ed25519",
                              expires_at: str | None = None) -> dict[str, Any]:
        """Open a subaccount with its one trading key, from the public half of
        a keypair you generated (`novig_signing.generate_key`). The answer's
        `keyId` is the new key's id and the subaccount's address. Management key."""
        body = {"label": label, "publicKey": public_key_pem, "algorithm": algorithm}
        if expires_at:
            body["expiresAt"] = expires_at
        return await self._call("POST", "/v3/account/subaccounts", body=body, kind="write", management=True) or {}

    async def label_subaccount(self, key_id: str, label: str) -> dict[str, Any]:
        return await self._call("PATCH", f"/v3/account/subaccounts/{key_id}", body={"label": label}, kind="write",
                                management=True) or {}

    async def issue_subaccount_key(self, key_id: str, name: str, public_key_pem: str, *,
                                   scope: str = "trading", algorithm: sig.Algorithm = "Ed25519") -> dict[str, Any]:
        """A replacement `trading` key (after the old one is revoked) or a
        `trading::read` key for a subaccount. Management key."""
        body = {"name": name, "publicKey": public_key_pem, "algorithm": algorithm, "scope": scope}
        return await self._call("POST", f"/v3/account/subaccounts/{key_id}/keys", body=body, kind="write",
                                management=True) or {}

    async def fund_subaccount(self, key_id: str, amount: Decimal, *, direction: str = "fund",
                              client_transfer_id: str | None = None) -> dict[str, Any]:
        """Move money between the cash wallet and a subaccount: `fund` into
        it, `defund` out. The venue accepts and applies it later; read
        `transfer()` for `Applied` or `Rejected`. `client_transfer_id` is the
        idempotency key (one is made when not given, and is in the answer):
        resend the same one to retry. Management key."""
        if direction not in ("fund", "defund"):
            raise BadRequest(f"{VENUE}: direction is fund or defund")
        value = D(amount)
        if value <= 0 or value != value.quantize(Decimal("0.00001")):
            raise BadRequest(f"{VENUE}: a transfer is a positive amount with at most five decimal places")
        body = {"direction": direction, "amount": str(value.quantize(Decimal("0.00001"))),
                "clientTransferId": client_transfer_id or str(uuid.uuid4())}
        return await self._call("POST", f"/v3/account/subaccounts/{key_id}/transfer", body=body, kind="write",
                                management=True) or {}

    async def transfer(self, key_id: str, transfer_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/v3/account/subaccounts/{key_id}/transfers/{transfer_id}",
                                management=True) or {}

    async def close(self) -> None:
        for http in (self.http, self.management_http):
            if http is not None:
                await http.close()
        if self._own_catalog:
            self.catalog.close()


__all__ = ["NovigTrading", "order_of_row", "fill_of_row", "position_of", "build_order", "error_of"]
