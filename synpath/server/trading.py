"""The trading surface over HTTP, and the event stream beside it.

The read app (`synpath.server.api`) keeps no accounts because reading a public
market needs none (`synpath serve` still puts it behind this app's token). This one places orders, so it needs to know who is asking
and what they may touch, and both are checked on every route.

What shapes it:

**Permissions are per subaccount, and nothing is implied.** A key with
`view` on `kalshi:desk-a` can read that account's orders and nothing else. A
key with `trade` may place and cancel there, but may not hand the permission
to anyone; `manage_members` does that, and by itself it cannot trade. Every
route names the permission it wants and the account it wants it on.

**Every handler is async, because the engine is.** The read app's handlers
are synchronous and Starlette runs them in threads; here the engine, the
journal and the venue adapters share one event loop, so blocking it would
stall order entry for everyone.

**Responses are the library's own types.** An order comes back as `Order`,
a position as `Position`. That is what makes the generated TypeScript client
typed rather than a wall of `object`, which is the reason this layer exists
at all.

**The event stream replays.** `/ws/events` starts by sending everything after
the cursor a client gives, then stays open for what happens next, so a
client that reconnects does not miss the fill that happened while it was
away. Events are the engine's own, filtered to the accounts the key may see.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from decimal import Decimal
from typing import Annotated, Any, Iterable, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, model_validator

from .. import ids
from .._deprecated import QUANTITY, rename_input
from ..bucket import (
    Bucket, BucketMember, BucketOrderReport, BucketPosition, bucket_id_of, is_bucket_id,
)
from ..engine.engine import Engine
from ..errors import (
    AuthenticationError, BadRequest, ExchangeError, NetworkError, RateLimitExceeded, RequestTimeout, SynpathError,
)
from ..engine.risk import RiskConfig
from ..trading.errors import (
    CancelUnconfirmed, InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected,
    RateBudgetExceeded, RiskRejected, SubmissionInDoubt, SubmissionPending,
)
from .errors import install_error_handlers
from ..trading.types import (
    Account, Balance, EditRequest, Fill, Order, OrderRequest, Position, Settlement, Side, TimeInForce,
)
from .models import ErrorBody, PageResponse
from .store import ALL_ACCOUNTS, ControlStore, Grant, Permission, Principal

log = logging.getLogger("synpath.server")

TRADING_STATUS: dict[type[Exception], int] = {
    SubmissionInDoubt: 202,
    SubmissionPending: 202,
    CancelUnconfirmed: 409,
    OrderNotFound: 404,
    InsufficientFunds: 400,
    OrderRejected: 400,
    InvalidOrder: 400,
    BadRequest: 400,
    MarketHalted: 409,
    RiskRejected: 409,
    RateBudgetExceeded: 429,
    RateLimitExceeded: 429,
    RequestTimeout: 504,
    NetworkError: 502,
    AuthenticationError: 502,
    ExchangeError: 502,
}
"""Library exception to status code on the trading routes, most specific
class first. A venue refusing our own credentials is 502: the caller's token
was fine, the server's venue key was not.

An order whose outcome is not known yet is 202, not a 5xx: the instruction
is recorded and the send may have happened, so it has not failed. The body
is the usual error envelope (`submission_in_doubt` for this send,
`submission_pending` for a retry while the first send is still being
reconciled), with `retryable: true` and the `client_order_id` to retry
under; the same id never sends twice. A cancel the venue neither performed
nor recognises is 409 `cancel_unconfirmed`: the order exists here, and its
last observed status stands."""


DEFAULT_ORDERS_PAGE = 100
MAX_ORDERS_PAGE = 1000


def orders_cursor(key: tuple[int, str, str]) -> str:
    """The paging key of a page's last order, `(created_ts, venue, id)`, as an opaque token."""
    return base64.urlsafe_b64encode(json.dumps(list(key)).encode()).decode().rstrip("=")


def orders_cursor_key(cursor: str | None) -> tuple[int, str, str] | None:
    if not cursor:
        return None
    try:
        created_ts, venue, order_id = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        return int(created_ts), str(venue), str(order_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="cursor is not one this server gave out") from None


def current_view(engine: Engine, order: Order) -> Order:
    """An engine-held parent this process runs is read from memory, which is
    what its journal row was last written from; anything else is the row."""
    parent = engine.orders.get(order.id) if order.held_by.value == "engine" else None
    return parent.as_order() if parent is not None else order


def _tag_venue(exc: Exception, venue: str | None) -> None:
    """Remember which venue answered, for the error body: the route knows
    where the order went even when the venue's own message does not say."""
    if venue and getattr(exc, "synpath_venue", None) is None:
        try:
            exc.synpath_venue = venue  # type: ignore[attr-defined]
        except AttributeError:
            pass


def trading_status_for(exc: Exception) -> int:
    for kind in type(exc).__mro__:
        if kind in TRADING_STATUS:
            return TRADING_STATUS[kind]
    return 500

RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorBody, "description": "The request was refused before anything was sent"},
    401: {"model": ErrorBody, "description": "No key, or a key that is not valid"},
    403: {"model": ErrorBody, "description": "The key may not do this on this account"},
    404: {"model": ErrorBody, "description": "No such order"},
    409: {"model": ErrorBody, "description": "Refused by a pre-trade risk rule"},
}


# ---------------------------------------------------------------------------
# Wire shapes the engine does not already have
# ---------------------------------------------------------------------------

class AccountView(BaseModel):
    """One subaccount a key may act on, and what it may do there."""

    account: str = Field(description="`venue:name`, or `*` for every account")
    venue: str | None = None
    permissions: list[str]


class WhoAmI(BaseModel):
    user_id: str
    name: str
    key_id: str
    accounts: list[AccountView]


class HaltRequest(BaseModel):
    reason: str = Field(description="Recorded in the journal and in the halt event")
    scope: str = Field(default="*", description="A venue id, or `*` for everything")
    policy: Literal["cancel", "hold", "rearm"] = "cancel"
    rearm_after_s: float | None = None


class HaltState(BaseModel):
    """Whether trading is halted, after a resume."""

    halted: bool = Field(description="True if a halt is still in force, such as one on another scope")
    halt_reason: str


class HaltResult(BaseModel):
    policy: str
    scope: str
    reason: str
    canceled: dict[str, Any] = Field(default_factory=dict)
    remaining: dict[str, Any] = Field(default_factory=dict)
    managed: int = 0


class PnlRow(BaseModel):
    key: str
    contracts: Decimal
    cost: Decimal
    realized: Decimal
    unrealized: Decimal | None = None
    fees: Decimal
    volume: Decimal
    positions: int
    marked: int


class PnlView(BaseModel):
    level: str
    rows: list[PnlRow]
    total: PnlRow


class BucketBody(BaseModel):
    """A bucket to create. The server assigns the id unless one is given."""

    book: str = Field(description="The strategy book the bucket belongs to, as on orders")
    name: str = Field(description="A label for people; not unique")
    members: list[BucketMember] = Field(description="Two or more Synpath market ids, each with `flip` and "
                                                    "`enabled` (default true; at least one enabled)")
    id: str | None = Field(default=None, description="A UUID made by the client (32 hex digits, or the dashed "
                                                     "form, kept as given in lower case), so a bucket defined "
                                                     "elsewhere keeps its id. Taken: 409")


class BucketUpdate(BaseModel):
    """A change to a bucket; fields left out are left alone. `members`
    replaces the whole list, each member's `enabled` included (switching a
    member off keeps it in the definition but out of routing and the merged
    book). `status` archives or restores it."""

    name: str | None = None
    book: str | None = None
    members: list[BucketMember] | None = None
    status: Literal["active", "archived"] | None = None


class BucketPlanBody(BaseModel):
    """What to preview: an order on the bucket, as `POST /orders` would take it."""

    side: Side = Field(description="`buy` takes the bucket's YES, `sell` its NO")
    quantity: Decimal = Field(gt=0, description="Contracts, in bucket terms")
    worst_price: Decimal = Field(gt=0, lt=1, description="The worst net price accepted, in bucket terms (an "
                                                          "order's `price`)")


class PlanLegView(BaseModel):
    """One leg of a plan: what one member venue would receive."""

    market_id: str
    venue: str
    flip: bool
    side: str = Field(description="The member's own side, after `flip`")
    price: Decimal = Field(description="The member's own YES price, after `flip` and tick rounding: what the "
                                       "venue would receive")
    quantity: Decimal
    bucket_price: Decimal = Field(description="The worst level this leg reaches, in bucket terms, before fees")
    net_price: Decimal = Field(description="Volume-weighted net price of the levels taken, in bucket terms, fee "
                                           "floor included")
    fee: Decimal = Field(description="The taker fee this leg is expected to pay, floor included")
    fee_floor: Decimal = Field(description="How much of `fee` is the venue's per-order floor")


class BucketPlanView(BaseModel):
    """`synpath.engine.router.plan` on the members' current books. Nothing was placed."""

    bucket_id: str
    side: str
    quantity: Decimal
    worst_price: Decimal
    legs: list[PlanLegView]
    allocated: Decimal = Field(description="Contracts the legs add up to; never more than `quantity`")
    unfilled: Decimal
    stop_reason: Literal["worst_price", "liquidity", "min_quantity"] | None = Field(
        description="Why the plan stops short of `quantity`; null when it fills it")
    expected_net_price: Decimal | None = Field(description="Volume-weighted across legs, in bucket terms, net of fees")
    fees: Decimal = Field(description="Every leg's expected taker fee")
    books: dict[str, Literal["stream", "rest", "missing"]] = Field(
        description="Per member, where its book came from: serve's live stream, a venue read for this request, "
                    "or nowhere")
    fees_unknown: list[str] = Field(description="Members whose taker fee the library cannot price; counted as free")
    computed_at: int = Field(description="Milliseconds since the epoch")


class EditBody(BaseModel):
    """A change to a resting order; the order is the one in the path. Fields
    left out are left alone."""

    price: Decimal | None = None
    quantity: Decimal | None = None
    time_in_force: TimeInForce | None = None
    expires_at: int | None = None
    client_order_id: str | None = Field(default=None, description="Idempotency key for the edit itself")

    @model_validator(mode="before")
    @classmethod
    def _renamed(cls, data: Any) -> Any:
        # TODO(0.9): remove with synpath._deprecated.
        return rename_input(data, QUANTITY)


class GrantBody(BaseModel):
    user_id: str
    account: str = Field(description="`venue:name`, or `*`")
    permission: Permission


class GrantView(BaseModel):
    id: str
    user_id: str
    account: str
    permission: str
    granted_ts: int


class KeyBody(BaseModel):
    user_id: str
    label: str | None = None


class IssuedKeyView(BaseModel):
    id: str
    user_id: str
    prefix: str
    secret: str = Field(description="Shown once. It is stored only as a hash.")
    label: str | None = None


class AuditRow(BaseModel):
    id: int
    ts: int
    actor: str | None = None
    request: str | None = None
    table_name: str
    action: str
    row_id: str | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None


class EngineStatus(BaseModel):
    owner: str
    running: bool
    halted: bool
    halt_reason: str = ""
    open_orders: int
    managed_orders: int
    positions: int
    risk_version: int | None = None
    journal_events: int


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

def get_engine(request: Request) -> Engine:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status_code=503, detail="this server has no engine attached")
    return engine


def get_store(request: Request) -> ControlStore:
    store = getattr(request.app.state, "control", None)
    if store is None:
        raise HTTPException(status_code=503, detail="this server has no account store attached")
    return store


async def principal_of(request: Request, store: ControlStore) -> Principal:
    header = request.headers.get("authorization") or ""
    secret = header[7:].strip() if header.lower().startswith("bearer ") else request.headers.get("x-api-key", "")
    if not secret:
        raise HTTPException(status_code=401, detail="no API key: send `Authorization: Bearer <key>`")
    principal = await store.principal(secret)
    if principal is None:
        raise HTTPException(status_code=401, detail="this key is not valid, or has been revoked")
    await store.acting_as(f"user:{principal.user_id}", f"{request.method} {request.url.path}")
    return principal


async def caller(request: Request, store: Annotated[ControlStore, Depends(get_store)]) -> Principal:
    return await principal_of(request, store)


async def socket_principal(socket: WebSocket, store: ControlStore, key: str | None) -> Principal | None:
    """Who an accepted WebSocket belongs to: the token from `?key=` (a browser
    cannot set headers on a socket) or from `Authorization: Bearer`; `None`
    without a valid one. The socket counterpart of `caller`."""
    header = socket.headers.get("authorization") or ""
    secret = key or (header[7:].strip() if header.lower().startswith("bearer ") else "")
    return await store.principal(secret) if secret else None


Caller = Annotated[Principal, Depends(caller)]
EngineDep = Annotated[Engine, Depends(get_engine)]
StoreDep = Annotated[ControlStore, Depends(get_store)]


def require(principal: Principal, permission: Permission, account: str | None = None) -> None:
    if not principal.may(permission, account):
        where = f" on {account}" if account else ""
        raise HTTPException(status_code=403, detail=f"this key may not {permission}{where}")


def order_venue(body: OrderRequest, engine: Engine) -> str | None:
    """Where an order goes: the named account's venue, else the venue its
    market id names (`kalshi:...`), else the only venue configured."""
    if body.account is not None:
        return body.account.venue
    venue, _ = ids.split(body.market_id)
    if venue is not None and venue in engine.adapters:
        return venue
    if len(engine.adapters) == 1:
        return next(iter(engine.adapters))
    return None


def account_key(request_account: Account | None, engine: Engine, venue: str | None = None) -> str:
    if request_account is not None:
        return request_account.key
    if venue and venue in engine.accounts:
        return engine.accounts[venue].key
    return f"{venue or 'unknown'}:default"


def visible(principal: Principal, account: str) -> bool:
    return principal.may("view", account) or principal.may("trade", account)


def bucket_accounts(bucket: Bucket, engine: Engine) -> list[str]:
    return [account_key(None, engine, venue) for venue in sorted(bucket.venues())]


def sees_bucket(principal: Principal, bucket: Bucket, engine: Engine) -> bool:
    """A bucket is visible to a caller who may see every account it trades on."""
    return all(visible(principal, account) for account in bucket_accounts(bucket, engine))


def client_bucket_id(value: str) -> str:
    """A client-made bucket id: any UUID, kept as given (lower case) so the
    client's references to `bucket:<id>` stay valid."""
    import uuid

    text = value[len("bucket:"):] if is_bucket_id(value) else value
    try:
        uuid.UUID(text)
    except ValueError:
        raise HTTPException(status_code=400, detail="a bucket id is a UUID: 32 hex digits or the dashed form") from None
    if len(text) not in (32, 36):
        raise HTTPException(status_code=400, detail="a bucket id is a UUID: 32 hex digits or the dashed form")
    return text.lower()


def checked_definition(principal: Principal, engine: Engine, bucket: Bucket) -> None:
    """The checks every stored definition passes: its shape, a venue this
    server trades for every member, and a caller allowed to trade each of
    them, since an order on the bucket will route there."""
    try:
        bucket.check()
    except BadRequest as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    missing = sorted(bucket.venues() - set(engine.adapters))
    if missing:
        raise HTTPException(status_code=400, detail=f"this server does not trade {', '.join(missing)}: "
                                                    "every member venue needs credentials here")
    for member_venue in sorted(bucket.venues()):
        require(principal, "trade", account_key(None, engine, member_venue))


def plan_view(bucket: Bucket, body: "BucketPlanBody", result: Any) -> "BucketPlanView":
    plan = result.plan
    legs = [PlanLegView(market_id=leg.market_id, venue=ids.venue_of(leg.market_id), flip=leg.flip,
                        side=leg.side.value, price=leg.price, quantity=leg.quantity, bucket_price=leg.bucket_price,
                        net_price=leg.net_price, fee=leg.fee, fee_floor=leg.fee_floor) for leg in plan.legs]
    return BucketPlanView(
        bucket_id=bucket.id, side=body.side.value, quantity=body.quantity, worst_price=body.worst_price, legs=legs,
        allocated=plan.allocated, unfilled=plan.unfilled, stop_reason=plan.reason or None,
        expected_net_price=plan.expected_net_price, fees=sum((leg.fee for leg in plan.legs), Decimal("0")),
        books=result.sources, fees_unknown=result.fees_unknown, computed_at=result.computed_at,
    )


def bucket_report(parent: Any) -> BucketOrderReport:
    report = {k: v for k, v in parent.report().items() if k in BucketOrderReport.model_fields}
    return BucketOrderReport.model_validate({**report, "stop_reason": report.get("stop_reason") or None,
                                             "status": parent.as_order().status.value})


# ---------------------------------------------------------------------------
# The routes
# ---------------------------------------------------------------------------

def create_trading_router() -> APIRouter:
    """Every route that needs an engine and a key."""
    router = APIRouter()

    # -- identity -------------------------------------------------------------

    @router.get("/me", tags=["accounts"], summary="Who this key is", responses=RESPONSES)
    async def me(principal: Caller) -> WhoAmI:
        by_account: dict[str, list[str]] = {}
        for grant in principal.grants:
            by_account.setdefault(grant.account, []).append(grant.permission)
        return WhoAmI(
            user_id=principal.user_id, name=principal.name, key_id=principal.key_id,
            accounts=[AccountView(account=key, venue=None if key == ALL_ACCOUNTS else key.split(":")[0],
                                  permissions=sorted(perms))
                      for key, perms in sorted(by_account.items())],
        )

    @router.get("/accounts", tags=["accounts"], summary="Accounts this engine trades", responses=RESPONSES)
    async def accounts(principal: Caller, engine: EngineDep) -> list[AccountView]:
        rows = []
        for venue, account in engine.accounts.items():
            if not visible(principal, account.key):
                continue
            rows.append(AccountView(account=account.key, venue=venue,
                                    permissions=sorted(p for p in ("view", "trade", "manage_credentials",
                                                                   "manage_members") if principal.may(p, account.key))))
        return rows

    @router.get("/status", tags=["accounts"], summary="What the engine is doing", responses=RESPONSES)
    async def status(principal: Caller, engine: EngineDep) -> EngineStatus:
        require(principal, "view")
        return EngineStatus(
            owner=engine.journal.owner, running=engine.running, halted=engine.risk.kill.engaged,
            halt_reason=engine.risk.kill.reason, open_orders=len(engine.open_orders()),
            managed_orders=len(engine.orders.live()), positions=len(engine.ledger.open_positions()),
            risk_version=engine.risk.config_version, journal_events=await engine.journal.last_seq(),
        )

    # -- orders ---------------------------------------------------------------

    @router.post("/orders", tags=["orders"], summary="Place an order", responses={
        **RESPONSES, 202: {"model": ErrorBody, "description": "Outcome not known yet (`submission_in_doubt` or "
                                                              "`submission_pending`): retry with the same "
                                                              "client_order_id, which never sends twice"}},
        status_code=201)
    async def create_order(principal: Caller, engine: EngineDep, body: OrderRequest) -> Order:
        if is_bucket_id(body.market_id):
            # A bucket order puts legs on every member venue: the caller must
            # be allowed to trade on each of them.
            row = await engine.journal.bucket(bucket_id_of(body.market_id))
            if row is None:
                raise HTTPException(status_code=404, detail=f"{body.market_id}: no such bucket")
            for member_venue in sorted(Bucket.model_validate(row).venues()):
                require(principal, "trade", account_key(None, engine, member_venue))
            venue = None
        else:
            venue = order_venue(body, engine)
            if venue is None:
                raise HTTPException(status_code=400, detail="more than one venue is configured and the market id "
                                                            "names none of them: use a Synpath id (venue:native) "
                                                            "or name an account")
            require(principal, "trade", account_key(body.account, engine, venue))
        try:
            return await engine.submit(body, venue=venue)
        except SynpathError as exc:
            _tag_venue(exc, venue)
            raise

    # -- buckets --------------------------------------------------------------

    @router.post("/buckets", tags=["buckets"], summary="Create a bucket", responses={
        **RESPONSES, 409: {"model": ErrorBody, "description": "A bucket with that id already exists"}},
        status_code=201)
    async def create_bucket(principal: Caller, engine: EngineDep, body: BucketBody) -> Bucket:
        bucket = Bucket(book=body.book, name=body.name, members=body.members)
        if body.id is not None:
            bucket.id = client_bucket_id(body.id)
        checked_definition(principal, engine, bucket)
        if body.id is not None and await engine.journal.bucket(bucket.id) is not None:
            raise HTTPException(status_code=409, detail=f"bucket {bucket.id} already exists")
        await engine.save_bucket(bucket)
        return Bucket.model_validate(await engine.journal.bucket(bucket.id))   # with its stored times

    async def _bucket(principal: Principal, engine: Engine, bucket_id: str) -> Bucket:
        """The bucket a path names, as `<id>` or as its market id `bucket:<id>`."""
        bucket_id = bucket_id_of(bucket_id) if is_bucket_id(bucket_id) else bucket_id
        row = await engine.journal.bucket(bucket_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"no bucket {bucket_id}")
        bucket = Bucket.model_validate(row)
        if not sees_bucket(principal, bucket, engine):
            raise HTTPException(status_code=403, detail="this key may not view every venue in that bucket")
        return bucket

    @router.get("/buckets", tags=["buckets"], summary="List buckets", responses=RESPONSES)
    async def list_buckets(
        principal: Caller,
        engine: EngineDep,
        book: Annotated[str | None, Query(description="Only this strategy book")] = None,
        status: Annotated[Literal["active", "archived", "all"], Query(description="Which buckets")] = "active",
    ) -> PageResponse[Bucket]:
        rows = await engine.journal.buckets(book=book, status=None if status == "all" else status)
        buckets = [b for b in (Bucket.model_validate(r) for r in rows) if sees_bucket(principal, b, engine)]
        return PageResponse[Bucket](data=buckets, next_cursor=None, count=len(buckets))

    @router.get("/buckets/{bucket_id}", tags=["buckets"], summary="Get a bucket", responses=RESPONSES)
    async def get_bucket(principal: Caller, engine: EngineDep, bucket_id: Annotated[str, Path()]) -> Bucket:
        return await _bucket(principal, engine, bucket_id)

    @router.delete("/buckets/{bucket_id}", tags=["buckets"], summary="Archive a bucket", responses=RESPONSES)
    async def archive_bucket(principal: Caller, engine: EngineDep, request: Request,
                             bucket_id: Annotated[str, Path()]) -> Bucket:
        bucket = await _bucket(principal, engine, bucket_id)
        for account in bucket_accounts(bucket, engine):
            require(principal, "trade", account)
        await engine.journal.archive_bucket(bucket.id)
        archived = Bucket.model_validate(await engine.journal.bucket(bucket.id))
        hub = getattr(request.app.state, "market_streams", None)
        if hub is not None:
            await hub.refresh_bucket(archived)
        return archived

    @router.patch("/buckets/{bucket_id}", tags=["buckets"], summary="Change a bucket", responses=RESPONSES)
    async def update_bucket(principal: Caller, engine: EngineDep, request: Request,
                            bucket_id: Annotated[str, Path()], body: BucketUpdate) -> Bucket:
        bucket = await _bucket(principal, engine, bucket_id)
        for account in bucket_accounts(bucket, engine):
            require(principal, "trade", account)
        changed = bucket.model_copy(update={
            **body.model_dump(exclude_none=True, exclude={"members"}),
            **({"members": body.members} if body.members is not None else {}),
            "updated_ms": None,
        })
        checked_definition(principal, engine, changed)
        await engine.save_bucket(changed)
        saved = Bucket.model_validate(await engine.journal.bucket(bucket.id))
        hub = getattr(request.app.state, "market_streams", None)
        if hub is not None:
            await hub.refresh_bucket(saved)
        return saved

    @router.post("/buckets/{bucket_id}/plan", tags=["buckets"], summary="Preview how an order would route",
                 responses=RESPONSES)
    async def plan_bucket_order(principal: Caller, engine: EngineDep, request: Request,
                                bucket_id: Annotated[str, Path()], body: BucketPlanBody) -> BucketPlanView:
        """The router's plan for a market order on the bucket, on the members'
        current books (serve's live streams where it holds them, else a venue
        read) and their published taker fees. Nothing is placed."""
        bucket = await _bucket(principal, engine, bucket_id)
        books = getattr(request.app.state, "bucket_books", None)
        if books is None:
            raise HTTPException(status_code=503, detail="this server has no market data attached for buckets")
        result = await books.plan(bucket, side=body.side, quantity=body.quantity, worst_price=body.worst_price)
        return plan_view(bucket, body, result)

    @router.get("/buckets/{bucket_id}/position", tags=["buckets"], summary="Position in a bucket",
                responses=RESPONSES)
    async def bucket_position(
        principal: Caller,
        engine: EngineDep,
        bucket_id: Annotated[str, Path()],
        book: Annotated[str | None, Query(description="Only this strategy book; default every book")] = None,
    ) -> BucketPosition:
        bucket = await _bucket(principal, engine, bucket_id)
        return BucketPosition.model_validate(await engine.bucket_position(bucket.id, book=book))

    @router.get("/buckets/{bucket_id}/orders", tags=["buckets"], summary="Orders on a bucket",
                responses=RESPONSES)
    async def bucket_orders(principal: Caller, engine: EngineDep,
                            bucket_id: Annotated[str, Path()]) -> PageResponse[BucketOrderReport]:
        bucket = await _bucket(principal, engine, bucket_id)
        reports = [bucket_report(p) for p in await engine.bucket_orders(bucket.id)]
        return PageResponse[BucketOrderReport](data=reports, next_cursor=None, count=len(reports))

    @router.get("/buckets/{bucket_id}/orders/{order_id}", tags=["buckets"], summary="One order on a bucket",
                responses=RESPONSES)
    async def bucket_order(principal: Caller, engine: EngineDep, bucket_id: Annotated[str, Path()],
                           order_id: Annotated[str, Path()]) -> BucketOrderReport:
        bucket = await _bucket(principal, engine, bucket_id)
        parent = await engine.bucket_order(order_id)
        if parent is None or parent.market_id != bucket.market_id:
            raise HTTPException(status_code=404, detail=f"no order {order_id} on bucket {bucket_id}")
        return bucket_report(parent)

    @router.get("/orders", tags=["orders"], summary="Orders: open ones, or every state", responses=RESPONSES)
    async def list_orders(
        principal: Caller,
        engine: EngineDep,
        venue: Annotated[str | None, Query(description="Only this venue")] = None,
        book: Annotated[str | None, Query(description="Only this strategy")] = None,
        scope: Annotated[Literal["open", "all"], Query(
            description="`open` (the default): resting venue orders and live engine-held parents "
                        "(`waiting`, `triggered`, `pending_cancel`). `all`: every state, finished ones too")] = "open",
        market_id: Annotated[str | None, Query(description="Only this market (`venue:native`)")] = None,
        since: Annotated[int | None, Query(description="Only orders created at or after this time, "
                                                       "milliseconds since the epoch")] = None,
        limit: Annotated[int | None, Query(ge=1, le=MAX_ORDERS_PAGE, description=(
            f"Rows per page, at most {MAX_ORDERS_PAGE}. Default: every open order for `open`, "
            f"{DEFAULT_ORDERS_PAGE} for `all`"))] = None,
        cursor: Annotated[str | None, Query(description="`next_cursor` from the previous page")] = None,
    ) -> PageResponse[Order]:
        """Newest first by creation. A child carries its parent's id in
        `parent_id`, so children group under their engine-held parent. A page
        may hold fewer rows than `limit` when some are on accounts this key may
        not view: follow `next_cursor` until it is null."""
        if limit is None and scope == "all":
            limit = DEFAULT_ORDERS_PAGE
        rows = await engine.journal.orders_page(
            venue=venue, book=book, market_id=market_id, since_ts=since, before=orders_cursor_key(cursor),
            open_only=scope == "open", limit=limit)
        orders = [current_view(engine, order) for order, _ in rows
                  if order.user_id in (None, engine.config.user_id)
                  and visible(principal, (order.account.key if order.account else f"{order.venue}:default"))]
        next_cursor = orders_cursor(rows[-1][1]) if limit is not None and len(rows) == limit else None
        return PageResponse[Order](data=orders, next_cursor=next_cursor, count=len(orders))

    @router.get("/orders/{order_id}", tags=["orders"], summary="One order", responses=RESPONSES)
    async def get_order(principal: Caller, engine: EngineDep, order_id: Annotated[str, Path()]) -> Order:
        parent = engine.orders.get(order_id)
        order = parent.as_order() if parent is not None else None
        if order is None:
            for venue in engine.adapters:
                order = await engine.journal.order(venue, order_id)
                if order is not None:
                    break
        if order is None:
            raise HTTPException(status_code=404, detail=f"no order {order_id}")
        account = order.account.key if order.account else f"{order.venue}:default"
        if not visible(principal, account):
            raise HTTPException(status_code=403, detail="this key may not view that account")
        return order

    @router.patch("/orders/{order_id}", tags=["orders"], summary="Amend an order", responses=RESPONSES)
    async def edit_order(principal: Caller, engine: EngineDep, order_id: Annotated[str, Path()],
                         body: EditBody) -> Order:
        order = await _known(engine, order_id)
        account = order.account.key if order.account else f"{order.venue}:default"
        require(principal, "trade", account)
        try:
            return await engine.edit(EditRequest(order_id=order_id, **body.model_dump(exclude_none=True)))
        except SynpathError as exc:
            _tag_venue(exc, order.venue)
            raise
        except RiskRejected as exc:
            raise HTTPException(status_code=409, detail=f"{exc.rule}: {exc}") from None

    @router.delete("/orders/{order_id}", tags=["orders"], summary="Cancel an order", responses=RESPONSES)
    async def cancel_order(principal: Caller, engine: EngineDep, order_id: Annotated[str, Path()]) -> Order:
        order = await _known(engine, order_id)
        account = order.account.key if order.account else f"{order.venue}:default"
        require(principal, "trade", account)
        try:
            return await engine.cancel(order_id)
        except SynpathError as exc:
            _tag_venue(exc, order.venue)
            raise

    async def _known(engine: Engine, order_id: str) -> Order:
        parent = engine.orders.get(order_id)
        if parent is not None:
            return parent.as_order()
        for venue in engine.adapters:
            order = await engine.journal.order(venue, order_id)
            if order is not None:
                return order
        raise HTTPException(status_code=404, detail=f"no order {order_id}")

    # -- fills, positions, balances ------------------------------------------

    @router.get("/fills", tags=["portfolio"], summary="Fills", responses=RESPONSES)
    async def fills(
        principal: Caller,
        engine: EngineDep,
        since: Annotated[int | None, Query(description="Milliseconds since the epoch")] = None,
        venue: str | None = None,
        book: str | None = None,
    ) -> PageResponse[Fill]:
        rows = [f for f in await engine.journal.fills(since_ts=since, venue=venue, book=book)
                if visible(principal, (f.account.key if f.account else f"{f.venue}:default"))]
        return PageResponse[Fill](data=rows, next_cursor=None, count=len(rows))

    @router.get("/positions", tags=["portfolio"], summary="Positions as the ledger has them", responses=RESPONSES)
    async def positions(principal: Caller, engine: EngineDep) -> PageResponse[Position]:
        rows = [p for p in engine.positions()
                if visible(principal, (p.account.key if p.account else f"{p.venue}:default"))]
        return PageResponse[Position](data=rows, next_cursor=None, count=len(rows))

    @router.get("/balances", tags=["portfolio"], summary="Balances, read from each venue", responses=RESPONSES)
    async def balances(principal: Caller, engine: EngineDep) -> PageResponse[Balance]:
        rows = []
        for venue, adapter in engine.adapters.items():
            account = engine.accounts.get(venue)
            if account is not None and not visible(principal, account.key):
                continue
            if not adapter.has.get("fetch_balance"):
                continue
            rows.append(await adapter.fetch_balance(account=account))
        return PageResponse[Balance](data=rows, next_cursor=None, count=len(rows))

    @router.get("/pnl", tags=["portfolio"], summary="Profit and loss, rolled up", responses=RESPONSES)
    async def pnl(
        principal: Caller,
        engine: EngineDep,
        level: Annotated[Literal["market", "book", "account"], Query()] = "book",
    ) -> PnlView:
        require(principal, "view")
        report = engine.pnl(level)
        rows = [PnlRow(key=key, contracts=row.contracts, cost=row.cost, realized=row.realized,
                       unrealized=row.unrealized, fees=row.fees, volume=row.volume, positions=row.positions,
                       marked=row.marked)
                for key, row in report["rows"].items()]
        total = report["total"]
        return PnlView(level=level, rows=rows,
                       total=PnlRow(key="*", contracts=total.contracts, cost=total.cost, realized=total.realized,
                                    unrealized=total.unrealized, fees=total.fees, volume=total.volume,
                                    positions=total.positions, marked=total.marked))


    # -- the order log ------------------------------------------------------

    @router.get("/order-log", tags=["orders"], summary="Every action on orders, newest first", responses=RESPONSES)
    async def get_order_log(
        principal: Caller, engine: EngineDep,
        order_id: Annotated[str | None, Query(description="One order and its family: its cancel, fills, children")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 200,
        before: Annotated[int | None, Query(description="The `next` of the previous page")] = None,
    ) -> dict[str, Any]:
        """The journal's order events as a log (`synpath.engine.order_log`):
        instructions saved, sends and their outcome, the venue's records,
        engine-held parents' states, cancels, fills and risk refusals, with
        repeats folded. Read only."""
        require(principal, "view")
        from ..engine.order_log import order_log
        rows, cursor = await order_log(engine, order_id=order_id, limit=limit, before=before)
        return {"data": rows, "next": cursor}

    @router.get("/journal", tags=["status"], summary="The engine's journal events, newest first", responses=RESPONSES)
    async def get_journal(
        principal: Caller, engine: EngineDep,
        kind: Annotated[str | None, Query(description="Only kinds starting with this, e.g. `reconcile.` or `engine.`")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 200,
        before: Annotated[int | None, Query(description="The `next` of the previous page")] = None,
    ) -> dict[str, Any]:
        """The append-only journal as it is, every kind (the order log reads
        the order kinds of it, folded). Each event: `seq`, `ts`, `kind`, `key`
        and its `payload`. Read only."""
        require(principal, "view")
        events = await engine.journal.events_page(prefixes=[kind] if kind else (), before=before, limit=limit)
        rows = [{"seq": e.seq, "ts": e.ts, "kind": e.kind, "key": e.key, "payload": e.payload} for e in events]
        return {"data": rows, "next": rows[-1]["seq"] if len(rows) == limit else None}

    # -- risk and the kill switch --------------------------------------------

    @router.get("/risk", tags=["risk"], summary="The rules in force", responses=RESPONSES)
    async def get_risk(principal: Caller, engine: EngineDep) -> RiskConfig:
        require(principal, "view")
        return engine.risk.config

    @router.put("/risk", tags=["risk"], summary="Replace the rules", responses=RESPONSES)
    async def put_risk(principal: Caller, engine: EngineDep, body: RiskConfig) -> RiskConfig:
        require(principal, "manage_credentials")
        await engine.set_risk(body, author=principal.user_id)
        return engine.risk.config

    @router.post("/halt", tags=["risk"], summary="Stop trading", responses=RESPONSES)
    async def halt(principal: Caller, engine: EngineDep, body: HaltRequest) -> HaltResult:
        require(principal, "trade", None if body.scope == "*" else None)
        result = await engine.halt(body.reason, scope=body.scope, policy=body.policy,
                                   rearm_after_s=body.rearm_after_s)
        return HaltResult(**{**result, "canceled": {k: str(v) for k, v in result.get("canceled", {}).items()},
                             "remaining": {k: str(v) for k, v in result.get("remaining", {}).items()}})

    @router.post("/resume", tags=["risk"], summary="Lift a halt", responses=RESPONSES)
    async def resume(principal: Caller, engine: EngineDep,
                     scope: Annotated[str | None, Query()] = None) -> HaltState:
        require(principal, "trade")
        await engine.resume(scope=scope)
        return HaltState(halted=engine.risk.kill.engaged, halt_reason=engine.risk.kill.reason)

    # -- members, keys and the audit log --------------------------------------

    @router.get("/grants", tags=["members"], summary="Grants for a user", responses=RESPONSES)
    async def list_grants(principal: Caller, store: StoreDep,
                          user_id: Annotated[str | None, Query()] = None) -> list[GrantView]:
        target = user_id or principal.user_id
        if target != principal.user_id:
            require(principal, "manage_members")
        rows = await store.grants(target)
        return [GrantView(id=g.id, user_id=g.user_id, account=g.account, permission=g.permission,
                          granted_ts=g.granted_ts) for g in rows]

    @router.post("/grants", tags=["members"], summary="Give a permission", responses=RESPONSES, status_code=201)
    async def add_grant(principal: Caller, store: StoreDep, body: GrantBody) -> GrantView:
        require(principal, "manage_members", body.account)
        grant = await store.grant(body.user_id, body.account, body.permission)
        return GrantView(id=grant.id, user_id=grant.user_id, account=grant.account, permission=grant.permission,
                         granted_ts=grant.granted_ts)

    @router.delete("/grants", tags=["members"], summary="Take a permission away", responses=RESPONSES)
    async def remove_grant(principal: Caller, store: StoreDep, body: GrantBody) -> dict[str, bool]:
        require(principal, "manage_members", body.account)
        return {"revoked": await store.revoke(body.user_id, body.account, body.permission)}

    @router.post("/keys", tags=["members"], summary="Issue an API key", responses=RESPONSES, status_code=201)
    async def issue_key(principal: Caller, store: StoreDep, body: KeyBody) -> IssuedKeyView:
        require(principal, "manage_credentials")
        issued = await store.issue_key(body.user_id, label=body.label)
        return IssuedKeyView(id=issued.id, user_id=issued.user_id, prefix=issued.prefix, secret=issued.secret,
                             label=issued.label)

    @router.delete("/keys/{key_id}", tags=["members"], summary="Revoke a key", responses=RESPONSES)
    async def revoke_key(principal: Caller, store: StoreDep, key_id: Annotated[str, Path()]) -> dict[str, bool]:
        require(principal, "manage_credentials")
        return {"revoked": await store.revoke_key(key_id)}

    @router.get("/audit", tags=["members"], summary="The audit log, append-only", responses=RESPONSES)
    async def audit(
        principal: Caller,
        store: StoreDep,
        since_id: Annotated[int, Query(description="Rows after this id")] = 0,
        limit: Annotated[int, Query(le=1000)] = 200,
        table: Annotated[str | None, Query()] = None,
    ) -> list[AuditRow]:
        require(principal, "manage_members")
        return [AuditRow(**row) for row in await store.audit(since_id=since_id, limit=limit, table=table)]

    return router


# ---------------------------------------------------------------------------
# The event stream
# ---------------------------------------------------------------------------

def add_event_socket(app: FastAPI) -> None:
    """`/ws/events`: replay from a cursor, then everything as it happens."""

    @app.websocket("/ws/events")
    async def events(socket: WebSocket, key: str | None = Query(default=None),
                     since: int = Query(default=0), kinds: str | None = Query(default=None)) -> None:
        store: ControlStore | None = getattr(socket.app.state, "control", None)
        engine: Engine | None = getattr(socket.app.state, "engine", None)
        # Accept first, then close with the reason: a close before accepting
        # becomes an HTTP 403 on the handshake, and a browser cannot read why.
        await socket.accept()
        if store is None or engine is None:
            await socket.close(code=1011, reason="this server has no engine attached")
            return
        principal = await socket_principal(socket, store, key)
        if principal is None or not principal.may("view"):
            await socket.close(code=4401, reason="an access token with view permission is required")
            return
        wanted = tuple(k for k in (kinds or "").split(",") if k) or None
        subscription = engine.bus.subscribe(*(wanted or ()))
        try:
            async for event in engine.bus.replay(since):
                await socket.send_text(json.dumps({
                    "seq": event.seq, "ts": event.ts, "kind": event.kind, "key": event.key, "payload": event.payload,
                }, default=str))
            while True:
                event = await subscription.queue.get()
                if wanted and not subscription.wants(event):
                    continue
                await socket.send_text(json.dumps({
                    "seq": event.seq, "ts": event.ts, "kind": event.kind, "key": event.key,
                    "payload": event.payload, "dropped": subscription.dropped,
                }, default=str))
        except WebSocketDisconnect:
            pass
        except Exception:  # pragma: no cover - a broken socket is not an engine problem
            log.debug("synpath.server: the event socket ended", exc_info=True)
        finally:
            subscription.close()


def create_trading_app(
    engine: Engine,
    store: ControlStore,
    *,
    title: str = "synpath trading",
    docs: bool = True,
) -> FastAPI:
    """An app with the trading routes and the event socket, and nothing else.

    ```python
    app = create_trading_app(engine, store)
    app.mount("/read", create_app())      # the public read surface, if wanted
    ```
    """
    app = FastAPI(
        title=title,
        version=__import__("synpath").__version__,
        description=__doc__,
        docs_url="/docs" if docs else None,
        openapi_url="/openapi.json" if docs else None,
    )
    app.state.engine = engine
    app.state.control = store
    app.include_router(create_trading_router())
    add_event_socket(app)

    install_error_handlers(app, trading_status_for)

    return app
