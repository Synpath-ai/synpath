"""Peg: stay at the touch without chasing it.

A pegged order follows a reference price -- the near touch, the far touch or
the mid -- and re-prices when the reference moves. On a thin prediction
market that is how a maker stays at the front of the queue without watching
a screen, and also how a naive implementation burns its rate limit and its
queue position in a minute.

Three restraints, all of them the point:

**A minimum stay.** `min_stay_s` is how long an order must sit before it may
be moved at all. Without it, a flickering touch re-prices the order every few
hundred milliseconds, and each move goes to the back of the queue.

**A level cap.** `level_cap` counts how many times the order may be moved
before the peg gives up and stays where it is. A market that runs away from
you is not one to follow to the end of the book.

**Bounds.** `min_price` and `max_price` are the range within which the peg
may operate. Outside them it waits rather than trades, because a peg with no
bound is an instruction to pay anything.

`offset` sits the order behind the reference: a buy peg with offset 0.01 at a
0.41 bid rests at 0.40. Re-pricing is cancel and replace, which loses queue
priority on every venue here, so the parent counts the moves and reports
them.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ...trading.types import Fill, Order, OrderRequest, OrderType, Side, TimeInForce
from .base import ZERO, Child, Context, D, ManagedOrder
from .manager import register

REFERENCES = ("near", "far", "mid")


@register
class Peg(ManagedOrder):
    """`params`: `reference`, `offset`, `min_stay_s`, `level_cap`, `min_price`, `max_price`."""

    kind = "peg"
    order_type = OrderType.PEG

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.reference = str(self.params.get("reference") or "near")
        if self.reference not in REFERENCES:
            raise ValueError(f"a peg's reference is one of {REFERENCES}")
        self.offset = D(self.params.get("offset"), ZERO)
        self.min_stay_s = float(self.params.get("min_stay_s") or 0)
        self.level_cap = int(self.params.get("level_cap") or 0)
        self.min_price = D(self.params.get("min_price"))
        self.max_price = D(self.params.get("max_price"))
        self.moves = 0
        self.placed_at: float = 0.0
        self.resting_price: Decimal | None = None

    # -- where the peg wants to be -------------------------------------------

    def target(self, ctx: Context) -> Decimal | None:
        book = ctx.book()
        if book is None:
            return None
        if self.reference == "mid":
            reference = ctx.mid()
        elif self.reference == "near":
            reference = book.best_bid if self.side == Side.BUY else book.best_ask
        else:
            reference = book.best_ask if self.side == Side.BUY else book.best_bid
        if reference is None:
            return None
        price = reference - self.offset if self.side == Side.BUY else reference + self.offset
        # The bounds cap the chase rather than stopping it: a buy peg whose
        # reference runs past its maximum rests at the maximum, where it may
        # still be hit, instead of leaving the market entirely.
        if self.min_price is not None:
            price = max(price, self.min_price)
        if self.max_price is not None:
            price = min(price, self.max_price)
        return price

    def may_move(self, ctx: Context) -> bool:
        if self.level_cap and self.moves >= self.level_cap:
            return False
        return ctx.now - self.placed_at >= self.min_stay_s

    # -- running --------------------------------------------------------------

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        await self.reprice(ctx, first=True)

    async def on_book(self, ctx: Context, market_id: str) -> None:
        if self.state != "working":
            return
        target = self.target(ctx)
        if target is None or target == self.resting_price:
            return
        if not self.live_children:
            await self.reprice(ctx)
            return
        if self.may_move(ctx):
            await self.reprice(ctx)

    async def on_timer(self, ctx: Context) -> None:
        if self.state == "working" and not self.live_children and self.remaining > 0:
            await self.reprice(ctx)

    async def reprice(self, ctx: Context, *, first: bool = False) -> None:
        target = self.target(ctx)
        if target is None:
            if self.live_children:
                for child in self.live_children:
                    await ctx.cancel_child(child.order_id)
                    child.status = "canceled"
                await ctx.publish("managed.peg_paused", {"reason": "the reference is outside the bounds"})
                await ctx.save()
            return
        for child in self.live_children:
            await ctx.cancel_child(child.order_id)
            child.status = "canceled"
        size = self.remaining
        if size <= 0:
            await self.complete_if_done(ctx)
            return
        request = self.child_request(amount=size, price=target, type=OrderType.LIMIT,
                                     time_in_force=TimeInForce.GTC, post_only=self.request.post_only)
        order = await ctx.submit_child(request)
        self.track(order, size, target)
        self.resting_price = target
        self.placed_at = ctx.now
        if not first:
            self.moves += 1
        await ctx.publish("managed.pegged", {"price": str(target), "moves": self.moves,
                                             "capped": bool(self.level_cap and self.moves >= self.level_cap)})
        await ctx.save()

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        await self.complete_if_done(ctx)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if self.state != "working" or not order.is_terminal:
            return
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")

    def extra(self) -> dict[str, Any]:
        return {
            "reference": self.reference, "offset": str(self.offset), "min_stay_s": self.min_stay_s,
            "level_cap": self.level_cap, "moves": self.moves, "placed_at": self.placed_at,
            "min_price": str(self.min_price) if self.min_price is not None else None,
            "max_price": str(self.max_price) if self.max_price is not None else None,
            "resting_price": str(self.resting_price) if self.resting_price is not None else None,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.reference = extra.get("reference", self.reference)
        self.offset = D(extra.get("offset"), ZERO)
        self.min_stay_s = float(extra.get("min_stay_s") or 0)
        self.level_cap = int(extra.get("level_cap") or 0)
        self.moves = int(extra.get("moves") or 0)
        self.placed_at = float(extra.get("placed_at") or 0)
        self.min_price = D(extra.get("min_price"))
        self.max_price = D(extra.get("max_price"))
        self.resting_price = D(extra.get("resting_price"))
