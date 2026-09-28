"""TWAP: the same order, spread over time.

Buying two hundred contracts at once on a book with forty at the touch pays
for the privilege. A TWAP cuts the order into slices and works them across a
window, so the average price is closer to the market's own average than to
the depth of one moment.

How it decides what to do, each slice:

* **The schedule is by clock, not by fill.** Slices are due at even
  intervals across the window. A slice that misses its turn is not skipped;
  the next one carries what is behind, so the order still finishes on time.
* **Patient or aggressive is a choice.** `style="limit"` posts at the near
  touch and lets the market come; `style="taker"` crosses with an
  immediate-or-cancel at the far touch. A limit slice that is still resting
  when the next one is due is pulled first, so the order cannot end up with
  five stale slices in the book.
* **The end is respected.** With `finish="complete"` the remainder is taken
  at the end of the window; with `finish="stop"` whatever is unfilled is
  abandoned, which is what a caller who was only ever price-sensitive wants.

A restart resumes the schedule from the clock, because the window's start and
end are persisted; the engine does not need to remember how many timers it
had running.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ...trading.types import Fill, Order, OrderRequest, OrderType, Side, TimeInForce
from .base import ZERO, Child, Context, D, ManagedOrder
from .manager import register


@register
class TWAP(ManagedOrder):
    """`params`: `window_s`, `slices`, `style`, `limit`, `finish`."""

    kind = "twap"
    order_type = OrderType.TWAP

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.window_s = float(self.params.get("window_s") or 0)
        self.slices = int(self.params.get("slices") or 0)
        if self.window_s <= 0 or self.slices <= 0:
            raise ValueError("a TWAP needs params window_s and slices, both above zero")
        self.style = str(self.params.get("style") or "limit")
        if self.style not in ("limit", "taker"):
            raise ValueError("a TWAP's style is 'limit' or 'taker'")
        self.limit = D(self.params.get("limit")) or D(self.request.price)
        """The worst price any slice may pay. `None` means the market's."""
        self.at_end = str(self.params.get("finish") or "complete")
        """What the end of the window does with anything unfilled."""
        self.started_at: float = 0.0
        self.sent_slices = 0

    # -- the schedule ---------------------------------------------------------

    @property
    def interval(self) -> float:
        return self.window_s / self.slices

    def due_by(self, now: float) -> int:
        """How many slices should have gone out by now."""
        if self.started_at <= 0:
            return 0
        elapsed = max(0.0, now - self.started_at)
        return min(self.slices, int(elapsed // self.interval) + 1)

    def slice_size(self) -> Decimal:
        per_slice = self.amount / self.slices
        return min(per_slice, self.remaining)

    def ends_at(self) -> float:
        return self.started_at + self.window_s

    # -- running --------------------------------------------------------------

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        self.started_at = ctx.now
        await self.work(ctx)

    async def on_timer(self, ctx: Context) -> None:
        if self.state == "working":
            await self.work(ctx)

    async def work(self, ctx: Context) -> None:
        if self.remaining <= 0:
            await self.complete_if_done(ctx)
            return
        now = ctx.now
        if now >= self.ends_at():
            await self.close_out(ctx)
            return
        due = self.due_by(now)
        if self.sent_slices >= due:
            return
        # A limit slice still resting when the next is due has had its turn.
        for child in self.live_children:
            await ctx.cancel_child(child.order_id)
            child.status = "canceled"
        behind = due - self.sent_slices
        size = min(self.remaining, self.slice_size() * behind)
        if size <= 0:
            return
        await self.send(ctx, size)
        self.sent_slices = due
        await ctx.save()

    async def send(self, ctx: Context, size: Decimal) -> None:
        if self.style == "taker":
            price = ctx.touch(self.side) or self.limit
            if self.limit is not None and price is not None:
                price = min(price, self.limit) if self.side == Side.BUY else max(price, self.limit)
            if price is None:
                return
            request = self.child_request(amount=size, price=price, type=OrderType.MARKET,
                                         time_in_force=TimeInForce.IOC)
        else:
            near = ctx.book()
            price = None
            if near is not None:
                price = near.best_bid if self.side == Side.BUY else near.best_ask
            price = price or self.limit
            if price is None:
                return
            if self.limit is not None:
                price = min(price, self.limit) if self.side == Side.BUY else max(price, self.limit)
            request = self.child_request(amount=size, price=price, type=OrderType.LIMIT,
                                         time_in_force=TimeInForce.GTC, post_only=self.request.post_only)
        order = await ctx.submit_child(request)
        self.track(order, size, D(request.price))
        await ctx.publish("managed.slice", {"amount": str(size), "price": str(request.price),
                                            "slice": self.sent_slices + 1, "of": self.slices, "style": self.style})

    async def close_out(self, ctx: Context) -> None:
        """The window is over."""
        for child in self.live_children:
            await ctx.cancel_child(child.order_id)
            child.status = "canceled"
        if self.remaining > 0 and self.at_end == "complete":
            price = ctx.touch(self.side) or self.limit
            if price is not None:
                if self.limit is not None:
                    price = min(price, self.limit) if self.side == Side.BUY else max(price, self.limit)
                request = self.child_request(amount=self.remaining, price=price, type=OrderType.MARKET,
                                             time_in_force=TimeInForce.IOC)
                order = await ctx.submit_child(request)
                self.track(order, request.amount, price)
                await ctx.publish("managed.close_out", {"amount": str(request.amount), "price": str(price)})
                await ctx.save()
                return
        await self.finish_window(ctx)

    async def finish_window(self, ctx: Context) -> None:
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
        else:
            await self.finish(ctx, "done" if self.filled > 0 else "canceled",
                              f"the window ended with {self.remaining} unfilled")

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        await self.complete_if_done(ctx)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if self.state != "working" or not order.is_terminal:
            return
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
        elif ctx.now >= self.ends_at() and not self.live_children:
            await self.finish_window(ctx)

    def extra(self) -> dict[str, Any]:
        return {
            "window_s": self.window_s, "slices": self.slices, "style": self.style, "finish": self.at_end,
            "limit": str(self.limit) if self.limit is not None else None,
            "started_at": self.started_at, "sent_slices": self.sent_slices,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.window_s = float(extra.get("window_s") or self.window_s)
        self.slices = int(extra.get("slices") or self.slices)
        self.style = extra.get("style", self.style)
        self.at_end = extra.get("finish", self.at_end)
        self.limit = D(extra.get("limit"))
        self.started_at = float(extra.get("started_at") or 0)
        self.sent_slices = int(extra.get("sent_slices") or 0)
