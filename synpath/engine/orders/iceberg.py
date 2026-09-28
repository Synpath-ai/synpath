"""Iceberg: show a slice, keep the rest to yourself.

A large resting order tells everyone what you are doing, and on a book with
a few hundred contracts at the touch it moves the price before it fills. An
iceberg shows `display` contracts at a time and replaces the slice when it
is consumed.

Two details decide whether it is worth anything:

**The reload delay.** Replacing a filled slice instantly makes the iceberg
obvious: the size at that price never falls. A short, jittered pause makes it
look like separate participants, and `reload_delay_s` sets it (zero is
allowed, and honest, for callers who only want the smaller footprint).

**Queue priority is lost on every reload.** A new slice goes to the back of
the queue at its price, which is the real cost of hiding size. The parent
records how many reloads it has done, so a caller can see what patience was
spent.

The price may follow the market or stay put: with `follow=True` the next
slice is priced at the touch when it is placed, otherwise at the price the
parent was given.
"""
from __future__ import annotations

import random
from decimal import Decimal
from typing import Any

from ...trading.types import Fill, Order, OrderRequest, OrderType, Side, TimeInForce
from .base import ZERO, Child, Context, D, ManagedOrder
from .manager import register


@register
class Iceberg(ManagedOrder):
    """`params`: `display`, `reload_delay_s`, `jitter_s`, `follow`."""

    kind = "iceberg"
    order_type = OrderType.ICEBERG

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.display = D(self.params.get("display"))
        if self.display is None or self.display <= 0:
            raise ValueError("an iceberg needs params display: how many contracts to show at a time")
        if self.request.price is None:
            raise ValueError("an iceberg needs a price; a hidden market order is just a market order")
        self.reload_delay_s = float(self.params.get("reload_delay_s") or 0)
        self.jitter_s = float(self.params.get("jitter_s") or 0)
        self.follow = bool(self.params.get("follow"))
        self.reloads = 0
        self.next_slice_at: float = 0.0

    def slice_size(self) -> Decimal:
        return min(self.display, self.remaining - self.working())

    def working(self) -> Decimal:
        """Contracts currently resting in slices."""
        return sum((c.amount - c.filled for c in self.live_children), ZERO)

    def slice_price(self, ctx: Context) -> Decimal | None:
        if not self.follow:
            return D(self.request.price)
        touch = ctx.book() and (ctx.book().best_bid if self.side == Side.BUY else ctx.book().best_ask)
        return touch or D(self.request.price)

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        await self.place(ctx)

    async def place(self, ctx: Context) -> None:
        size = self.slice_size()
        if size <= 0:
            return
        price = self.slice_price(ctx)
        request = self.child_request(amount=size, price=price, type=OrderType.LIMIT,
                                     time_in_force=self.request.time_in_force or TimeInForce.GTC,
                                     post_only=self.request.post_only, expires_at=self.request.expires_at)
        order = await ctx.submit_child(request)
        self.track(order, size, price)
        await ctx.publish("managed.slice", {"amount": str(size), "price": str(price), "reloads": self.reloads})
        await ctx.save()

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        if await self.complete_if_done(ctx):
            return
        if child.filled >= child.amount:
            self.schedule_reload(ctx)
        await ctx.save()

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if order.is_terminal and self.state == "working" and self.remaining > 0:
            self.schedule_reload(ctx)
        await self.complete_if_done(ctx)

    def schedule_reload(self, ctx: Context) -> None:
        delay = self.reload_delay_s + (random.uniform(0, self.jitter_s) if self.jitter_s else 0)
        self.next_slice_at = ctx.now + delay

    async def on_timer(self, ctx: Context) -> None:
        if self.state != "working" or self.remaining <= 0:
            return
        if self.working() > 0 or ctx.now < self.next_slice_at:
            return
        self.reloads += 1
        await self.place(ctx)

    def extra(self) -> dict[str, Any]:
        return {
            "display": str(self.display), "reload_delay_s": self.reload_delay_s, "jitter_s": self.jitter_s,
            "follow": self.follow, "reloads": self.reloads, "next_slice_at": self.next_slice_at,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.display = D(extra.get("display"), self.display)
        self.reload_delay_s = float(extra.get("reload_delay_s") or 0)
        self.jitter_s = float(extra.get("jitter_s") or 0)
        self.follow = bool(extra.get("follow"))
        self.reloads = int(extra.get("reloads") or 0)
        self.next_slice_at = float(extra.get("next_slice_at") or 0)
