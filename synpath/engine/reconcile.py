"""Reconciliation: what the venue says, against what the engine believes.

The engine's journal is authoritative about its own decisions and about
nothing else. Someone can trade the same account from a phone, a fill can
arrive while the process is down, a venue can expire an order without
telling anyone. So the engine asks, on a timer and at startup, and names
every difference:

| Finding | Meaning |
|---|---|
| `orphan` | resting at the venue, unknown to the journal |
| `ghost` | open in the journal, gone at the venue |
| `drift` | known to both, different status, filled amount or price |
| `position` | the ledger and the venue disagree about a position |
| `fill` | a fill the venue has and the journal does not |
| `balance` | cash moved without a fill explaining it |

Findings are reported, not silently fixed. The policy decides what happens
to each: `report` only says so, `adopt` takes an orphan into the journal so
the engine manages it from now on, `cancel` pulls it. A ghost is always
closed in the journal, because the venue is the authority on whether an
order exists.

Balance reconciliation is the one that catches what nothing else does: a
deposit, a withdrawal, a settlement, or a fee charged outside any fill. It
compares the venue's cash against the cash the journal can explain, and
reports the difference rather than adjusting anything.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

from ..trading.base import TradingExchange
from ..trading.types import Balance, Fill, HeldBy, Order, OrderStatus, Position, PositionSide
from .engine import Engine

log = logging.getLogger("synpath.engine")

ZERO = Decimal("0")

OrphanPolicy = Literal["report", "adopt", "cancel"]
Finding = Literal["orphan", "ghost", "drift", "position", "fill", "balance"]


@dataclass(slots=True)
class Difference:
    kind: Finding
    venue: str
    key: str
    detail: dict[str, Any] = field(default_factory=dict)
    action: str = "reported"

    def as_event(self) -> dict[str, Any]:
        return {"kind": self.kind, "venue": self.venue, "key": self.key, "action": self.action, **self.detail}


@dataclass(slots=True)
class ReconcileReport:
    venue: str
    orphans: list[Difference] = field(default_factory=list)
    ghosts: list[Difference] = field(default_factory=list)
    drifts: list[Difference] = field(default_factory=list)
    positions: list[Difference] = field(default_factory=list)
    fills: list[Difference] = field(default_factory=list)
    balances: list[Difference] = field(default_factory=list)
    checked_orders: int = 0
    checked_positions: int = 0
    new_fills: int = 0

    @property
    def differences(self) -> list[Difference]:
        return [*self.orphans, *self.ghosts, *self.drifts, *self.positions, *self.fills, *self.balances]

    @property
    def clean(self) -> bool:
        return not self.differences

    def summary(self) -> dict[str, Any]:
        return {
            "venue": self.venue, "clean": self.clean, "orders_checked": self.checked_orders,
            "positions_checked": self.checked_positions, "new_fills": self.new_fills,
            "orphans": len(self.orphans), "ghosts": len(self.ghosts), "drifts": len(self.drifts),
            "position_differences": len(self.positions), "balance_differences": len(self.balances),
        }


class Reconciler:
    """Compares one engine against the venues it trades."""

    def __init__(
        self,
        engine: Engine,
        *,
        orphan_policy: OrphanPolicy = "report",
        position_tolerance: Decimal = ZERO,
        balance_tolerance: Decimal = Decimal("0.01"),
    ):
        self.engine = engine
        self.orphan_policy = orphan_policy
        self.position_tolerance = position_tolerance
        self.balance_tolerance = balance_tolerance

    async def run(self, venue: str | None = None) -> list[ReconcileReport]:
        """Reconcile one venue or all of them."""
        names = [venue] if venue else list(self.engine.adapters)
        reports = []
        for name in names:
            report = await self.venue(name, self.engine.adapters[name])
            reports.append(report)
        return reports

    async def venue(self, venue: str, adapter: TradingExchange) -> ReconcileReport:
        report = ReconcileReport(venue=venue)
        await self._orders(venue, adapter, report)
        await self._fills(venue, adapter, report)
        await self._positions(venue, adapter, report)
        await self._balance(venue, adapter, report)
        await self.engine.bus.publish("reconcile.done", report.summary(), key=venue)
        for difference in report.differences:
            await self.engine.bus.publish(f"reconcile.{difference.kind}", difference.as_event(), key=f"{venue}:{difference.key}")
        return report

    # -- orders ---------------------------------------------------------------

    async def _orders(self, venue: str, adapter: TradingExchange, report: ReconcileReport) -> None:
        live = {o.id: o for o in await adapter.fetch_open_orders()}
        # Engine-held parents are not orders at the venue: asking about one
        # would report it missing every minute.
        mine = {o.id: o for o in await self.engine.journal.open_orders(venue=venue) if o.held_by != HeldBy.ENGINE}
        report.checked_orders = len(live) + len(mine)

        for order_id, order in live.items():
            known = mine.get(order_id)
            if known is None:
                # An order this engine never wrote down. Adopting it means
                # managing it; cancelling it assumes nobody else should be
                # trading this account. Reporting is the only safe default.
                difference = Difference("orphan", venue, order_id, {
                    "market_id": order.market_id, "side": order.side.value,
                    "amount": str(order.amount), "price": str(order.price) if order.price else None,
                    "client_order_id": order.client_order_id,
                })
                if self.orphan_policy == "adopt":
                    await self.engine.on_order(order)
                    difference.action = "adopted"
                elif self.orphan_policy == "cancel":
                    try:
                        await adapter.cancel_order(order_id, market_id=order.market_id)
                        difference.action = "canceled"
                    except Exception as exc:
                        difference.action = f"cancel failed: {type(exc).__name__}"
                report.orphans.append(difference)
                continue
            if _drifted(known, order):
                await self.engine.on_order(order)
                report.drifts.append(Difference("drift", venue, order_id, {
                    "journal_status": known.status.value, "venue_status": order.status.value,
                    "journal_filled": str(known.filled), "venue_filled": str(order.filled),
                    "journal_price": str(known.price) if known.price else None,
                    "venue_price": str(order.price) if order.price else None,
                }, action="updated"))

        for order_id, order in mine.items():
            if order_id in live:
                continue
            # Open here, gone there. The venue is the authority: close it, but
            # read it first, because it may have filled rather than vanished.
            final = None
            try:
                final = await adapter.fetch_order(order_id)
            except Exception:
                final = None
            closed = final or order.model_copy(update={"status": OrderStatus.CANCELED})
            await self.engine.on_order(closed)
            report.ghosts.append(Difference("ghost", venue, order_id, {
                "market_id": order.market_id, "journal_status": order.status.value,
                "final_status": closed.status.value, "filled": str(closed.filled),
            }, action="closed"))

    # -- fills ----------------------------------------------------------------

    async def _fills(self, venue: str, adapter: TradingExchange, report: ReconcileReport) -> None:
        if not adapter.has.get("fetch_my_trades"):
            return
        since = int(await self.engine.journal.cursor(f"reconcile_fills:{venue}", 0) or 0)
        try:
            fills = await adapter.fetch_my_trades(since=since or None, limit=500)
        except Exception as exc:
            log.warning("synpath.engine: reading fills from %s failed: %s", venue, exc)
            return
        newest = since
        for fill in fills:
            newest = max(newest, fill.timestamp or 0)
            if await self.engine.journal.has_fill(venue, fill.id):
                continue
            await self.engine.on_fill(fill)
            report.new_fills += 1
            report.fills.append(Difference("fill", venue, fill.id, {
                "order_id": fill.order_id, "market_id": fill.market_id,
                "amount": str(fill.amount), "price": str(fill.price),
            }, action="booked"))
        if newest > since:
            await self.engine.journal.set_cursor(f"reconcile_fills:{venue}", newest)

    # -- positions ------------------------------------------------------------

    async def _positions(self, venue: str, adapter: TradingExchange, report: ReconcileReport) -> None:
        if not adapter.has.get("fetch_positions"):
            return
        try:
            live = await adapter.fetch_positions()
        except Exception as exc:
            log.warning("synpath.engine: reading positions from %s failed: %s", venue, exc)
            return
        rows = self.engine.ledger.merged(live)
        report.checked_positions = len(rows)
        for row in rows:
            if row["venue"] != venue:
                continue
            difference = row["difference"]
            if abs(difference) > self.position_tolerance:
                report.positions.append(Difference("position", venue, row["market_id"], {
                    "account": row["account"], "engine": str(row["engine"]),
                    "venue_contracts": str(row["venue_contracts"]), "difference": str(difference),
                    "books": {k: str(v) for k, v in row["books"].items()},
                }))

    # -- balance --------------------------------------------------------------

    async def _balance(self, venue: str, adapter: TradingExchange, report: ReconcileReport) -> None:
        if not adapter.has.get("fetch_balance"):
            return
        try:
            balance = await adapter.fetch_balance()
        except Exception as exc:
            log.warning("synpath.engine: reading the balance from %s failed: %s", venue, exc)
            return
        key = f"balance:{venue}"
        previous = await self.engine.journal.cursor(key)
        await self.engine.journal.set_cursor(key, str(balance.total))
        if previous is None:
            return
        moved = balance.total - Decimal(previous)
        if moved == ZERO:
            return
        explained = await self._explained_cash(venue)
        unexplained = moved - explained
        if abs(unexplained) > self.balance_tolerance:
            report.balances.append(Difference("balance", venue, balance.currency, {
                "moved": str(moved), "explained_by_fills": str(explained), "unexplained": str(unexplained),
                "total": str(balance.total), "available": str(balance.available),
                "note": "a deposit, a withdrawal, a settlement or a fee charged outside a fill",
            }))

    async def _explained_cash(self, venue: str) -> Decimal:
        """Cash the fills since the last check account for."""
        key = f"balance_cursor:{venue}"
        since = int(await self.engine.journal.cursor(key, 0) or 0)
        newest = since
        total = ZERO
        for fill in await self.engine.journal.fills(since_ts=since, venue=venue):
            newest = max(newest, fill.timestamp or 0)
            signed = -1 if fill.side.value == "buy" else 1
            total += Decimal(signed) * fill.price * fill.amount - (fill.fee or ZERO)
        if newest > since:
            await self.engine.journal.set_cursor(key, newest)
        return total


def _drifted(mine: Order, theirs: Order) -> bool:
    return (
        mine.status != theirs.status
        or mine.filled != theirs.filled
        or (mine.price or ZERO) != (theirs.price or ZERO)
        or (mine.remaining or ZERO) != (theirs.remaining or ZERO)
    )
