"""The supervisor: every engine-held order, and what reaches it.

One `ManagedOrders` per engine. It creates parents, hands them the events
they asked for, writes their state down whenever it changes, and brings them
back after a restart. Three details are worth stating, because they are what
makes an engine-held order safe rather than a background task:

**Every change is persisted before the next one.** A stop that has just
triggered is written as triggered before its child is sent, so a crash in
between leaves a parent whose child is in doubt, which the engine's own
recovery already knows how to resolve.

**Events are routed, not broadcast.** A parent hears about its own children
and about the instruments it named. A thousand resting parents on other
markets cost nothing when a book ticks.

**One bad parent does not stop the rest.** A hook that raises is logged, the
parent is marked rejected with the reason, and every other parent carries on.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Iterable, Mapping, Type

from ...trading.types import Fill, Order, OrderRequest, OrderType
from .base import Child, Context, ManagedOrder, State, now_ms

log = logging.getLogger("synpath.engine.orders")

REGISTRY: dict[str, Type[ManagedOrder]] = {}


def register(cls: Type[ManagedOrder]) -> Type[ManagedOrder]:
    """Make a managed order type known to `submit` and to recovery."""
    REGISTRY[cls.kind] = cls
    return cls


class ManagedOrders:
    """Every engine-held order this engine is running."""

    def __init__(self, engine: Any):
        self.engine = engine
        self.parents: dict[str, ManagedOrder] = {}
        self.by_child: dict[str, str] = {}
        """Child order id to parent id."""
        self.by_market: dict[str, set[str]] = {}

    # -- lookup ---------------------------------------------------------------

    def live(self) -> list[ManagedOrder]:
        return [p for p in self.parents.values() if p.live]

    def get(self, parent_id: str) -> ManagedOrder | None:
        return self.parents.get(parent_id)

    def parent_of(self, order_id: str, venue: str | None = None) -> ManagedOrder | None:
        parent_id = (self.by_child.get(f"{venue}:{order_id}") if venue else None) or self.by_child.get(order_id)
        return self.parents.get(parent_id) if parent_id else None

    def adopt_child(self, parent_id: str, order_id: str, venue: str | None = None) -> None:
        self.by_child[order_id] = parent_id
        if venue:
            self.by_child[f"{venue}:{order_id}"] = parent_id

    def context(self, parent: ManagedOrder) -> Context:
        return Context(self.engine, parent)

    def watching(self, market_id: str) -> list[ManagedOrder]:
        return [self.parents[pid] for pid in self.by_market.get(market_id, set())
                if pid in self.parents and self.parents[pid].live]

    # -- creating -------------------------------------------------------------

    def build(self, request: OrderRequest, *, venue: str, account: Any) -> ManagedOrder:
        # `managed_kind` lets one venue type map to an engine-held variant:
        # a market order the engine walks the book with, rather than the
        # adapter's single immediate limit.
        kind = str(request.params.get("managed_kind") or request.type.value)
        cls = REGISTRY.get(kind)
        if cls is None:
            raise NotImplementedError(
                f"{kind} is an engine-held order type with no implementation loaded; "
                f"known: {sorted(REGISTRY)}"
            )
        return cls(request, venue=venue, account=account)

    async def create(self, request: OrderRequest, *, venue: str, account: Any, owner: ManagedOrder | None = None) -> ManagedOrder:
        parent = self.build(request, venue=venue, account=account)
        parent.owner_id = owner.id if owner else None
        self.remember(parent)
        if owner is not None:
            self.by_child[parent.id] = owner.id
        await self.save(parent)
        await self.engine.bus.publish("managed.created", {
            "parent_id": parent.id, "kind": parent.kind, "market_id": parent.market_id,
            "side": parent.side.value, "amount": str(parent.amount), "book": request.book,
        }, key=parent.id)
        await self.run(parent, lambda ctx: parent.start(ctx))
        return parent

    def remember(self, parent: ManagedOrder) -> None:
        self.parents[parent.id] = parent
        for instrument in parent.markets():
            self.by_market.setdefault(instrument, set()).add(parent.id)
        for child in parent.children:
            self.adopt_child(parent.id, child.order_id, child.venue)

    def forget(self, parent: ManagedOrder) -> None:
        for instrument in parent.markets():
            self.by_market.get(instrument, set()).discard(parent.id)

    # -- routing --------------------------------------------------------------

    async def run(self, parent: ManagedOrder, call: Any) -> None:
        """Run one hook, catching what it raises so the others keep working."""
        ctx = self.context(parent)
        try:
            await call(ctx)
        except Exception as exc:
            log.exception("synpath.engine.orders: %s %s failed", parent.kind, parent.id)
            parent.detail = f"{type(exc).__name__}: {exc}"
            try:
                await parent.finish(ctx, "rejected", parent.detail)
            except Exception:  # pragma: no cover - the journal is the only thing left
                log.exception("synpath.engine.orders: could not record the failure of %s", parent.id)
        finally:
            if not parent.live:
                self.forget(parent)
            await self.save(parent)
            if not parent.live and parent.owner_id:
                await self.tell_owner(parent)

    async def tell_owner(self, leg: ManagedOrder) -> None:
        """A leg of a bracket or an OCO ended: its owner decides what that means."""
        owner = self.parents.get(leg.owner_id or "")
        if owner is None or not owner.live:
            return
        child = owner.child_of(leg.id)
        if child is None:
            return
        child.status = leg.state
        child.filled = leg.filled
        await self.run(owner, lambda ctx: owner.on_child(ctx, leg.as_order(), child))

    async def on_book(self, market_id: str) -> None:
        for parent in self.watching(market_id):
            await self.run(parent, lambda ctx, p=parent: p.on_book(ctx, market_id))

    async def on_trade(self, market_id: str, price: Decimal, amount: Decimal) -> None:
        for parent in self.watching(market_id):
            await self.run(parent, lambda ctx, p=parent: p.on_trade(ctx, market_id, price, amount))

    async def on_order(self, order: Order) -> None:
        """The venue's record of a child changed. This is the one channel a
        parent learns its children's fills from: the record is a snapshot,
        the latest overwrites, and the progress since the last one drives
        `on_fill`. Fill events are the ledger's and never reach a parent."""
        parent = self.parent_of(order.id, order.venue)
        if parent is None:
            return
        child = parent.child_of(order.id, order.venue)
        if child is None:
            return
        progress = parent.note_order(order, child)
        if progress is not None:
            await self.run(parent, lambda ctx: parent.on_fill(ctx, progress, child))
        await self.run(parent, lambda ctx: parent.on_child(ctx, order, child))

    async def on_timer(self) -> None:
        now_ms = int(self.engine.clock() * 1000)
        for parent in self.live():
            expires_at = parent.request.expires_at
            if expires_at is not None and now_ms >= expires_at:
                # The parent's own expiry: the whole order ends, every live
                # child pulled, whatever its type does on a timer.
                await self.cancel(parent.id, reason="expired")
                continue
            await self.run(parent, lambda ctx, p=parent: p.on_timer(ctx))

    async def on_halt(self, reason: str, *, scope: str = "*") -> int:
        halted = 0
        for parent in self.live():
            if scope != "*" and scope not in parent.venues():
                continue
            await self.run(parent, lambda ctx, p=parent: p.on_halt(ctx, reason))
            halted += 1
        return halted

    async def cancel(self, parent_id: str, *, reason: str = "cancelled by the caller") -> ManagedOrder:
        parent = self.parents.get(parent_id)
        if parent is None:
            raise KeyError(f"no engine-held order {parent_id!r}")
        await self.run(parent, lambda ctx: parent.cancel(ctx, reason))
        return parent

    # -- persistence ----------------------------------------------------------

    async def save(self, parent: ManagedOrder) -> None:
        for child in parent.children:
            self.by_child.setdefault(child.order_id, parent.id)
            self.by_child.setdefault(f"{child.venue}:{child.order_id}", parent.id)
        await self.engine.journal.save_managed(parent.snapshot())
        await self.engine.journal.upsert_order(parent.as_order(), event=f"managed.{parent.state}")
        self.engine._remember(parent.as_order())

    async def restore(self) -> int:
        """Bring back every parent that was still live, after a restart."""
        restored = 0
        for snapshot in await self.engine.journal.managed(states=("waiting", "working", "cancelling")):
            cls = REGISTRY.get(snapshot.get("kind", ""))
            if cls is None:
                log.warning("synpath.engine.orders: no implementation for %s; leaving %s alone",
                            snapshot.get("kind"), snapshot.get("id"))
                continue
            parent = cls.from_snapshot(snapshot)
            self.remember(parent)
            restored += 1
        if restored:
            await self.engine.bus.publish("managed.restored", {"count": restored})
        return restored
