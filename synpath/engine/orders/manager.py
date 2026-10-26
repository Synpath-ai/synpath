"""The supervisor: every engine-held order, and what reaches it.

One `ManagedOrderController` per engine. It creates parents, hands them the events
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

from ...errors import NetworkError
from ...trading.errors import SubmissionPending
from ..storage_types import IntentState
from ...trading.types import Fill, Order, OrderRequest, OrderType
from .base import Child, Context, ManagedOrder, State, now_ms

log = logging.getLogger("synpath.engine.orders")

REGISTRY: dict[str, Type[ManagedOrder]] = {}


def register(cls: Type[ManagedOrder]) -> Type[ManagedOrder]:
    """Make a managed order type known to `submit` and to recovery."""
    REGISTRY[cls.kind] = cls
    return cls


class ManagedOrderController:
    """Every engine-held order this engine is running."""

    def __init__(self, engine: Any):
        self.engine = engine
        self.parents: dict[str, ManagedOrder] = {}
        self.by_child: dict[str, str] = {}
        """Child order id to parent id."""
        self.by_market: dict[str, set[str]] = {}
        self._running_tasks: dict[str, Any] = {}

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

    def build(self, request: OrderRequest, *, venue: str | None, account: Any,
              execution_accounts: dict | None = None) -> ManagedOrder:
        """Construct the requested order implementation without saving or starting it."""
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
        return cls(request, venue=venue, account=account, execution_accounts=execution_accounts)

    async def create(self, request: OrderRequest, *, venue: str | None, account: Any,
                     execution_accounts: dict | None = None, owner: ManagedOrder | None = None) -> ManagedOrder:
        """Save and register an engine-held order, announce it, then start its behavior.

        `owner` is another managed order, not a user: for example, a bracket
        can own a stop order. The returned order may already have placed or
        filled venue children, or encountered a failure while starting.
        """
        # Choose the implementation (for example, RoutedLimit for a bucket).
        # A managed order can itself be a child of another managed order.
        parent = self.build(request, venue=venue, account=account, execution_accounts=execution_accounts)
        parent.owner_id = owner.id if owner else None

        # Persist the initial instruction before start() can send venue orders.
        await self.save(parent)

        # Register in-memory lookups so market and child updates reach this order.
        # The owner link also lets a nested managed order report back to its owner.
        self.remember(parent)
        if owner is not None:
            self.by_child[parent.id] = owner.id

        # Creation means the order exists, not that a venue has accepted a child.
        await self.engine.bus.publish("managed.created", {
            "parent_id": parent.id, "kind": parent.kind, "market_id": parent.market_id,
            "side": parent.side.value, "quantity": str(parent.quantity), "book": request.book,
        }, key=parent.id)
        # Await the type-specific start behavior through the controller's locking,
        # error handling and final save. A routed order plans and places children here.
        await self.run(parent, lambda ctx: parent.start(ctx))
        return parent

    def remember(self, parent: ManagedOrder) -> None:
        """Index an order by ID, watched markets and known children, in memory only."""
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
        """Run one order's behavior without overlapping another task for that order.

        The callback receives a Context for books, child submission and saving.
        Child notifications on the current task may call back into the same order.
        """
        task = asyncio.current_task()
        if self._running_tasks.get(parent.id) is task:
            await self._run_hook(parent, call)
            return
        async with self.engine._lock(f"managed:{parent.id}"):
            self._running_tasks[parent.id] = task
            try:
                await self._run_hook(parent, call)
            finally:
                self._running_tasks.pop(parent.id, None)

    async def _run_hook(self, parent: ManagedOrder, call: Any) -> None:
        """Run one hook, catching what it raises so the others keep working."""
        ctx = self.context(parent)
        try:
            if parent.execution_accounts:
                for venue, account in parent.execution_accounts.items():
                    self.engine.assert_execution_account(venue, account)
            await call(ctx)
        except SubmissionPending as exc:
            parent.detail = str(exc)
        except (NetworkError, asyncio.TimeoutError):
            parent.detail = "venue action is unresolved; waiting for reconciliation"
        except Exception as exc:
            log.exception("synpath.engine.orders: %s %s failed", parent.kind, parent.id)
            parent.detail = f"{type(exc).__name__}: {exc}"
            try:
                if await self._dispatch_is_uncertain(parent):
                    parent.detail += "; venue dispatch is unresolved; refusing to reject or resend"
                else:
                    await parent.finish(ctx, "rejected", parent.detail)
            except Exception:  # pragma: no cover - the journal is the only thing left
                log.exception("synpath.engine.orders: could not record the failure of %s", parent.id)
        finally:
            if not parent.live:
                self.forget(parent)
            await self.save(parent)
            if not parent.live and parent.owner_id:
                await self.tell_owner(parent)

    async def _dispatch_is_uncertain(self, parent: ManagedOrder) -> bool:
        """A decoder/host failure after sending cannot prove venue rejection."""
        pending = getattr(parent, "pending_child", None)
        if pending is None or not pending.client_order_id:
            return False
        intent = await self.engine.journal.intent(pending.client_order_id)
        return intent is not None and intent.state in (IntentState.SENDING, IntentState.SENT)

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
        await self.run(owner, lambda ctx: owner.on_child_order_updated(ctx, leg.as_order(), child))

    async def on_book(self, market_id: str) -> None:
        watching = self.watching(market_id)
        observations = {}
        for parent in watching:
            if parent.kind in ("stop_market", "stop_limit") and parent.state == "waiting":
                if parent.trigger_source in ("touch", "mid"):
                    observations[parent.id] = parent.watched(self.context(parent))
        for parent in watching:
            if parent.kind in ("stop_market", "stop_limit", "trailing_stop") and parent.state != "waiting":
                continue
            if parent.id in observations:
                price = observations[parent.id]
                if price is not None:
                    await self.run(parent, lambda ctx, p=parent, value=price: p.consider(ctx, value))
            else:
                await self.run(parent, lambda ctx, p=parent: p.on_book(ctx, market_id))

    async def on_trade(self, market_id: str, price: Decimal, quantity: Decimal) -> None:
        for parent in self.watching(market_id):
            if parent.kind in ("stop_market", "stop_limit", "trailing_stop") and parent.state != "waiting":
                continue
            await self.run(parent, lambda ctx, p=parent: p.on_trade(ctx, market_id, price, quantity))

    async def on_order(self, order: Order) -> None:
        """The venue's record of a child changed. This is the one channel a
        parent learns its children's fills from: the record is a snapshot,
        the latest overwrites, and the progress since the last one drives
        `on_fill`. Fill events are the ledger's and never reach a parent."""
        parent = self.parent_of(order.id, order.venue) or self.parents.get(order.parent_id or "")
        if parent is None:
            return
        async def apply(ctx):
            child = parent.child_of(order.id, order.venue)
            if child is None:
                return
            # First update the tracked child, even when no new quantity filled.
            progress = parent.update_child_from_venue_order(order, child)
            if progress is not None:
                await parent.on_fill(ctx, progress, child)
            # Then let the parent react to the updated child. A correct child
            # record alone does not advance the parent's lifecycle state.
            await parent.on_child_order_updated(ctx, order, child)
        await self.run(parent, apply)

    async def on_timer(self) -> None:
        for parent in self.live():
            async def tick(ctx, p=parent):
                expires_at = p.request.expires_at
                if expires_at is not None and int(self.engine.clock() * 1000) >= expires_at:
                    await p.expire(ctx)
                    if not p.live or p.state == "cancelling":
                        if p.live:
                            await p.on_timer(ctx)
                        return
                await p.on_timer(ctx)
            await self.run(parent, tick)

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
        """Save the restart snapshot and readable order record, and update local lookups."""
        for child in parent.children:
            self.by_child.setdefault(child.order_id, parent.id)
            self.by_child.setdefault(f"{child.venue}:{child.order_id}", parent.id)
        await self.engine.journal.save_managed(parent.snapshot(), order=parent.as_order())
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

    async def resume_restored(self) -> None:
        """Resume persisted stop actions only after intent reconciliation."""
        for parent in self.live():
            recover = getattr(parent, "recover", None)
            if recover is not None:
                await self.run(parent, lambda ctx, p=parent: p.recover(ctx))
