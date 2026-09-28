"""Engine-held orders: the machinery every synthetic type is built on.

A venue holds limit and market orders. Everything else in the order set --
stops, icebergs, brackets, TWAP, pegs -- is a decision this engine makes
over time, which means it must survive the process making it. So each one is
a `ManagedOrder`: a small state machine whose whole state is a dictionary,
written to the journal whenever it changes, restored on start.

The rules that follow from that:

**A parent is not an order at the venue.** It is journaled with
`held_by="engine"` and a status of `waiting` until its condition fires, then
`triggered` while its children work. Reconciliation knows to skip it, because
asking a venue about an order it was never told about would report it missing
every minute.

**Children are ordinary orders.** They go through `Engine.submit`, so the
risk rules, the journal and the ledger treat them exactly like anything else.
A parent that wants ten contracts in slices of one submits ten orders, each
of which can be refused on its own.

**Events arrive, the parent decides.** `on_book`, `on_trade`, `on_fill`,
`on_child`, `on_timer` and `on_halt` are the only ways in. A type that needs
the touch reads it from the book the engine keeps; one that needs a clock
gets `on_timer` about once a second, which is also what makes a restart
harmless: the timer does not care how long it has been away.

**Cancelling is not optional.** Cancelling a parent cancels its live
children first and only then marks itself done; a halt does the same. A
parent that cannot reach the venue stays `cancelling` and says so rather
than pretending.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Literal, Mapping

from ...trading.types import (
    Account, Fill, HeldBy, Order, OrderRequest, OrderStatus, OrderType, Side, TimeInForce,
)

log = logging.getLogger("synpath.engine.orders")

ZERO = Decimal("0")
ONE = Decimal("1")

State = Literal["waiting", "working", "cancelling", "done", "canceled", "rejected"]
LIVE_STATES: frozenset[str] = frozenset({"waiting", "working", "cancelling"})

STATUS_OF: dict[str, OrderStatus] = {
    "waiting": OrderStatus.WAITING,
    "working": OrderStatus.TRIGGERED,
    "cancelling": OrderStatus.PENDING_CANCEL,
    "done": OrderStatus.CLOSED,
    "canceled": OrderStatus.CANCELED,
    "rejected": OrderStatus.REJECTED,
}


def now_ms() -> int:
    return int(time.time() * 1000)


def D(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return default
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(slots=True)
class Child:
    """One venue order a parent put out, and what became of it.

    `filled`, `fee` and `status` are the venue's order record as it was last
    seen: the placement's answer, then every order-status update, then a
    direct read after a restart. Each is a whole snapshot and the latest
    overwrites. Fill events are never added on top; they are the ledger's."""

    order_id: str
    venue: str
    amount: Decimal
    price: Decimal | None = None
    filled: Decimal = ZERO
    fee: Decimal = ZERO
    status: str = "open"
    created_ms: int = field(default_factory=now_ms)

    @property
    def live(self) -> bool:
        return self.status in ("open", "pending")

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id, "venue": self.venue, "amount": str(self.amount),
            "price": str(self.price) if self.price is not None else None, "filled": str(self.filled),
            "fee": str(self.fee),
            "status": self.status, "created_ms": self.created_ms,
        }

    @classmethod
    def of(cls, row: Mapping[str, Any]) -> "Child":
        return cls(
            order_id=row["order_id"], venue=row["venue"], amount=D(row["amount"]), price=D(row.get("price")),
            filled=D(row.get("filled"), ZERO), fee=D(row.get("fee"), ZERO), status=row.get("status", "open"),
            created_ms=int(row.get("created_ms") or now_ms()),
        )


class Context:
    """What a managed order is allowed to do: submit, cancel, look, report.

    Deliberately narrow. A parent cannot reach the adapters, the journal or
    the risk rules directly, so every child it places is an ordinary order
    that the engine checks and records.
    """

    def __init__(self, engine: Any, parent: "ManagedOrder"):
        self.engine = engine
        self.parent = parent

    @property
    def now(self) -> float:
        return self.engine.clock()

    def book(self, market_id: str | None = None) -> Any | None:
        """The engine's local book for this instrument, if a stream feeds one
        and it is ready. A book that is not ready (no snapshot yet, or the
        stream lost confidence in it after a gap) reads as no book: its levels
        may be stale, and a stop must not fire on a price nobody is quoting."""
        book = self.engine.books.get(market_id or self.parent.market_id)
        if book is not None and getattr(book, "ready", True) is False:
            return None
        return book

    def touch(self, side: Side, market_id: str | None = None) -> Decimal | None:
        """The price a taker on `side` would pay: the ask to buy, the bid to sell."""
        book = self.book(market_id)
        if book is None:
            return None
        return book.best_ask if side == Side.BUY else book.best_bid

    def mid(self, market_id: str | None = None) -> Decimal | None:
        book = self.book(market_id)
        if book is None or book.best_bid is None or book.best_ask is None:
            return None
        return (book.best_bid + book.best_ask) / 2

    def last(self, market_id: str | None = None) -> Decimal | None:
        return self.engine.last_trade.get(market_id or self.parent.market_id)

    def mark(self, market_id: str | None = None) -> Decimal | None:
        instrument = market_id or self.parent.market_id
        return (self.engine.fair_values.get(self.parent.account_key, instrument)
                or self.mid(instrument) or self.last(instrument))

    def levels(self, side: Side, market_id: str | None = None) -> list[tuple[Decimal, Decimal]]:
        """The far side of the book, best first: what a taker would eat."""
        book = self.book(market_id)
        if book is None:
            return []
        bids, asks = book.levels()
        rows = asks if side == Side.BUY else bids
        return [(level.price, level.size) for level in rows]

    async def submit_child(self, request: OrderRequest, **kw: Any) -> Order:
        return await self.engine.submit_child(self.parent, request, **kw)

    async def cancel_child(self, order_id: str, venue: str | None = None) -> Order | None:
        return await self.engine.cancel_child(self.parent, order_id, venue=venue)

    async def publish(self, kind: str, payload: dict[str, Any] | None = None) -> None:
        await self.engine.bus.publish(kind, {"parent_id": self.parent.id, **(payload or {})}, key=self.parent.id)

    async def save(self) -> None:
        await self.engine.orders.save(self.parent)


class ManagedOrder:
    """One engine-held order. Subclasses implement the hooks they need."""

    kind: str = "managed"
    order_type: OrderType = OrderType.LIMIT

    def __init__(
        self,
        request: OrderRequest,
        *,
        id: str | None = None,
        venue: str,
        account: Account,
        state: State = "waiting",
    ):
        self.id = id or f"mo-{uuid.uuid4().hex[:16]}"
        self.request = request
        self.venue = venue
        self.account = account
        self.state: State = state
        self.children: list[Child] = []
        self.created_ms = now_ms()
        self.updated_ms = self.created_ms
        self.detail: str = ""
        self.params: dict[str, Any] = dict(request.params or {})
        self.owner_id: str | None = None
        """Set when this parent is a leg of another one (a bracket's stop)."""

    # -- identity -------------------------------------------------------------

    @property
    def market_id(self) -> str:
        return self.request.market_id

    @property
    def account_key(self) -> str:
        return self.account.key

    @property
    def side(self) -> Side:
        return self.request.side

    @property
    def amount(self) -> Decimal:
        return self.request.amount

    @property
    def filled(self) -> Decimal:
        """What the venues say has filled across the children. Derived, never
        accumulated, so a fill can only be counted once."""
        return sum((c.filled for c in self.children), ZERO)

    @property
    def remaining(self) -> Decimal:
        return max(ZERO, self.amount - self.filled)

    @property
    def live(self) -> bool:
        return self.state in LIVE_STATES

    @property
    def live_children(self) -> list[Child]:
        return [c for c in self.children if c.live]

    def child_of(self, order_id: str, venue: str | None = None) -> Child | None:
        """The child with this venue order id. Ids are the venue's own and only
        unique there, so a parent with legs on several venues must say which."""
        return next((c for c in self.children if c.order_id == order_id and (venue is None or c.venue == venue)), None)

    def markets(self) -> list[str]:
        """Which books this parent wants to hear about. Most want one; a
        bracket or an OCO across two markets says so by overriding this."""
        return [self.market_id]

    def venues(self) -> set[str]:
        """Every venue this parent has or may put a child on. A single-venue
        parent is its own venue; a parent across venues overrides this so a
        halt scoped to any one of them reaches it."""
        return {self.venue} | {c.venue for c in self.children}

    # -- hooks ----------------------------------------------------------------

    async def start(self, ctx: Context) -> None:
        """Called once, when the parent is accepted."""

    async def on_book(self, ctx: Context, market_id: str) -> None:
        """The book for one of this parent's instruments changed."""

    async def on_trade(self, ctx: Context, market_id: str, price: Decimal, amount: Decimal) -> None:
        """A public print."""

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        """One of this parent's children filled, wholly or in part."""

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        """A child's status changed (cancelled, rejected, expired, closed)."""

    async def on_timer(self, ctx: Context) -> None:
        """About once a second, whatever else happened."""

    async def on_halt(self, ctx: Context, reason: str) -> None:
        """Trading stopped. The default pulls the children and stands down."""
        await self.cancel(ctx, f"halted: {reason}")

    # -- lifecycle ------------------------------------------------------------

    async def cancel(self, ctx: Context, reason: str = "cancelled") -> None:
        self.detail = reason
        if not self.live:
            return
        self.state = "cancelling"
        await self.pull_children(ctx)
        if not self.live_children:
            await self.finish(ctx, "canceled", reason)
        else:
            await ctx.save()

    async def pull_children(self, ctx: Context) -> None:
        """Cancel every live child.

        A cancel the venue accepted counts, whatever status it echoes back:
        Kalshi's demo answers a cancel with the order still `resting`, and a
        parent that believed that would wait for ever to stand down. A cancel
        that fails is a different matter, and the child stays live so the
        parent keeps saying it is still cancelling.
        """
        for child in self.live_children:
            try:
                result = await ctx.cancel_child(child.order_id, child.venue)
                child.status = result.status.value if result is not None and result.is_terminal else "canceled"
            except Exception as exc:
                log.debug("synpath.engine.orders: cancelling %s failed: %s", child.order_id, exc)

    async def finish(self, ctx: Context, state: State, detail: str = "") -> None:
        self.state = state
        self.detail = detail or self.detail
        self.updated_ms = now_ms()
        await ctx.save()
        await ctx.publish(f"managed.{state}", {"kind": self.kind, "filled": str(self.filled), "detail": self.detail})

    def note_order(self, order: Order, child: Child) -> Fill | None:
        """Take the venue's latest record of a child. Returns the progress
        since the last record as a `Fill`-shaped step for the `on_fill`
        hook, or `None` if nothing more filled. Built from the order record
        alone: the venue's fill events go to the ledger, never here."""
        before, fee_before = child.filled, child.fee
        child.filled = order.filled or ZERO
        child.fee = order.fee or ZERO
        child.status = order.status.value
        self.updated_ms = now_ms()
        delta = child.filled - before
        if delta <= 0:
            return None
        return Fill(
            id=f"{order.venue}:{order.id}:{child.filled}", order_id=order.id, client_order_id=order.client_order_id,
            venue=order.venue, account=order.account, market_id=order.market_id, side=order.side,
            price=order.last_fill_price or order.average_price or order.price or ZERO, amount=delta,
            fee=max(ZERO, child.fee - fee_before), fee_currency=order.fee_currency,
            timestamp=order.updated_at or now_ms(),
        )

    async def complete_if_done(self, ctx: Context) -> bool:
        """Finish when the parent has what it asked for and nothing is live."""
        if self.remaining <= 0 and not self.live_children:
            await self.finish(ctx, "done", "filled")
            return True
        return False

    # -- children -------------------------------------------------------------

    def child_request(
        self,
        *,
        amount: Decimal,
        price: Decimal | None,
        type: OrderType = OrderType.LIMIT,
        time_in_force: TimeInForce | None = None,
        post_only: bool = False,
        expires_at: int | None = None,
        reduce_only: bool | None = None,
        market_id: str | None = None,
        side: Side | None = None,
        params: dict[str, Any] | None = None,
    ) -> OrderRequest:
        """A venue order in this parent's name, carrying its book and trader."""
        return OrderRequest(
            market_id=market_id or self.market_id, side=side or self.side, amount=amount, type=type,
            price=price, time_in_force=time_in_force or TimeInForce.GTC, post_only=post_only, expires_at=expires_at,
            # A parent that may only reduce a position must not open one
            # through its children: on Polymarket a reduce-only sell sells the
            # YES held instead of buying NO.
            reduce_only=self.request.reduce_only if reduce_only is None else reduce_only,
            account=self.account, book=self.request.book, trader=self.request.trader,
            tags={**self.request.tags, "parent": self.id}, params=params or {},
        )

    def track(self, order: Order, amount: Decimal, price: Decimal | None) -> Child:
        """Record a child, with the status the venue already gave it: an
        order that crossed on the way in comes back closed, and a parent that
        recorded it as resting would wait for a fill that has happened."""
        child = Child(order_id=order.id, venue=order.venue, amount=amount, price=price,
                      filled=order.filled, status=order.status.value)
        self.children.append(child)
        self.updated_ms = now_ms()
        return child

    # -- persistence ----------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Everything needed to rebuild this parent after a restart."""
        return {
            "id": self.id, "kind": self.kind, "state": self.state, "venue": self.venue,
            "account": self.account.model_dump(mode="json"), "request": self.request.model_dump(mode="json"),
            "filled": str(self.filled), "children": [c.to_dict() for c in self.children],
            "created_ms": self.created_ms, "updated_ms": self.updated_ms, "detail": self.detail,
            "owner_id": self.owner_id, "extra": self.extra(),
        }

    def extra(self) -> dict[str, Any]:
        """Type-specific state. Subclasses override."""
        return {}

    def load_extra(self, extra: dict[str, Any]) -> None:
        """Take type-specific state back. Subclasses override."""

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, Any]) -> "ManagedOrder":
        request = OrderRequest.model_validate(snapshot["request"])
        parent = cls(request, id=snapshot["id"], venue=snapshot["venue"],
                     account=Account.model_validate(snapshot["account"]), state=snapshot["state"])
        parent.children = [Child.of(row) for row in snapshot.get("children") or []]
        parent.created_ms = int(snapshot.get("created_ms") or now_ms())
        parent.updated_ms = int(snapshot.get("updated_ms") or parent.created_ms)
        parent.detail = snapshot.get("detail") or ""
        parent.owner_id = snapshot.get("owner_id")
        parent.load_extra(snapshot.get("extra") or {})
        return parent

    # -- how it looks from outside -------------------------------------------

    def as_order(self) -> Order:
        """The parent as an `Order`, so callers see one order, not a scheme."""
        average = None
        spent = sum((c.filled * (c.price or ZERO) for c in self.children), ZERO)
        if self.filled > 0 and spent > 0:
            average = spent / self.filled
        return Order(
            id=self.id, client_order_id=self.request.client_order_id, venue=self.venue, account=self.account,
            market_id=self.market_id, side=self.side, type=self.order_type,
            time_in_force=self.request.time_in_force, status=STATUS_OF[self.state], held_by=HeldBy.ENGINE,
            price=self.request.price, stop_price=self.request.stop_price, amount=self.amount, filled=self.filled,
            remaining=self.remaining, average_price=average, book=self.request.book, trader=self.request.trader,
            tags=dict(self.request.tags), created_at=self.created_ms, updated_at=self.updated_ms,
            info={"kind": self.kind, "state": self.state, "detail": self.detail,
                  "children": [c.order_id for c in self.children], "params": self.params},
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<{type(self).__name__} {self.id} {self.state} {self.filled}/{self.amount} {self.market_id}>"
