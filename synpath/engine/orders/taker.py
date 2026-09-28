"""Taking liquidity: the market order these venues do not have, and a patient one.

**Market.** None of the three venues holds a market order: Kalshi's V2 has
only limits, and the other two turn one into an immediate limit. Sending a
single limit at the far touch fills what is there and cancels the rest, which
is not what "market" means either. So the engine walks the book: an
immediate-or-cancel clip at each level in turn, stopping at the caller's
protection price or when `max_slippage` from the first touch is spent. What
it cannot buy within that range it reports unfilled, because the alternative
is paying any price at all.

**Smart taker** is the same walk with patience: clips of `clip` contracts,
`interval_s` apart, up to `limit`. It exists because taking two hundred
contracts in one clip pays for depth that would have refilled in thirty
seconds. Between clips it does nothing, which is the point; if the market
improves it takes the better price, and if it runs away it stops at the
limit.

Both count what the book actually offered, so a caller can tell a fill that
cost slippage from one that did not.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ...trading.types import Fill, Order, OrderRequest, OrderType, Side, TimeInForce
from .base import ZERO, Child, Context, D, ManagedOrder
from .manager import register


class _Taker(ManagedOrder):
    """Shared: a price bound, a clip, and one live child at a time."""

    default_slippage = Decimal("0.05")

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.limit = D(self.params.get("limit")) or D(self.request.price)
        self.max_slippage = D(self.params.get("max_slippage"), self.default_slippage)
        self.first_touch: Decimal | None = None
        self.clips = 0
        self.next_clip_at: float = 0.0
        self.paid: Decimal = ZERO

    def bound(self, ctx: Context) -> Decimal | None:
        """The worst price this order may pay, from the limit or the slippage."""
        if self.limit is not None:
            return self.limit
        if self.first_touch is None:
            self.first_touch = ctx.touch(self.side)
        if self.first_touch is None:
            return None
        return (self.first_touch + self.max_slippage) if self.side == Side.BUY else (self.first_touch - self.max_slippage)

    def affordable(self, ctx: Context) -> tuple[Decimal, Decimal] | None:
        """How much of the far side is inside the bound, and at what price."""
        bound = self.bound(ctx)
        if bound is None:
            return None
        available = ZERO
        worst: Decimal | None = None
        for price, size in ctx.levels(self.side):
            if (self.side == Side.BUY and price > bound) or (self.side == Side.SELL and price < bound):
                break
            available += size
            worst = price
        if available <= 0 or worst is None:
            return None
        return available, bound

    async def take(self, ctx: Context, size: Decimal) -> bool:
        """One immediate-or-cancel clip. `False` when there is nothing to take."""
        offered = self.affordable(ctx)
        if offered is None:
            return False
        available, bound = offered
        amount = min(size, available, self.remaining)
        if amount <= 0:
            return False
        request = self.child_request(amount=amount, price=bound, type=OrderType.MARKET,
                                     time_in_force=TimeInForce.IOC)
        order = await ctx.submit_child(request)
        self.track(order, amount, bound)
        self.clips += 1
        await ctx.publish("managed.clip", {"amount": str(amount), "bound": str(bound), "clip": self.clips})
        await ctx.save()
        return True

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        self.paid += fill.price * fill.amount
        await self.complete_if_done(ctx)

    def extra(self) -> dict[str, Any]:
        return {
            "limit": str(self.limit) if self.limit is not None else None,
            "max_slippage": str(self.max_slippage),
            "first_touch": str(self.first_touch) if self.first_touch is not None else None,
            "clips": self.clips, "next_clip_at": self.next_clip_at, "paid": str(self.paid),
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.limit = D(extra.get("limit"))
        self.max_slippage = D(extra.get("max_slippage"), self.default_slippage)
        self.first_touch = D(extra.get("first_touch"))
        self.clips = int(extra.get("clips") or 0)
        self.next_clip_at = float(extra.get("next_clip_at") or 0)
        self.paid = D(extra.get("paid"), ZERO)


@register
class MarketOrder(_Taker):
    """A market order the engine holds: walk the book inside the bound, now."""

    kind = "market_engine"
    order_type = OrderType.MARKET

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        if not await self.take(ctx, self.remaining):
            await self.finish(ctx, "canceled", "nothing on the book inside the price bound")

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if self.state != "working" or not order.is_terminal:
            return
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
            return
        # The clip took what was there; try the next level, once.
        if not await self.take(ctx, self.remaining):
            await self.finish(ctx, "done" if self.filled else "canceled",
                              f"stopped with {self.remaining} unfilled: the book ran out inside the bound")


@register
class SmartTaker(_Taker):
    """`params`: `clip`, `interval_s`, `limit`, `max_slippage`, `expires_s`."""

    kind = "smart_taker"
    order_type = OrderType.SMART_TAKER

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.clip = D(self.params.get("clip")) or self.amount
        self.interval_s = float(self.params.get("interval_s") or 1.0)
        self.expires_s = float(self.params.get("expires_s") or 0)
        self.started_at: float = 0.0

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        self.started_at = ctx.now
        await self.clip_now(ctx)

    async def clip_now(self, ctx: Context) -> None:
        if await self.take(ctx, self.clip):
            self.next_clip_at = ctx.now + self.interval_s
        else:
            # Nothing inside the bound: wait and look again.
            self.next_clip_at = ctx.now + self.interval_s

    async def on_timer(self, ctx: Context) -> None:
        if self.state != "working":
            return
        if self.expires_s and ctx.now - self.started_at >= self.expires_s:
            await self.finish(ctx, "done" if self.filled else "canceled",
                              f"expired with {self.remaining} unfilled")
            return
        if self.live_children or ctx.now < self.next_clip_at or self.remaining <= 0:
            return
        await self.clip_now(ctx)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if self.state == "working" and self.remaining <= 0 and not self.live_children:
            await self.finish(ctx, "done", "filled")

    def extra(self) -> dict[str, Any]:
        return super().extra() | {
            "clip": str(self.clip), "interval_s": self.interval_s, "expires_s": self.expires_s,
            "started_at": self.started_at,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        super().load_extra(extra)
        self.clip = D(extra.get("clip"), self.amount)
        self.interval_s = float(extra.get("interval_s") or 1.0)
        self.expires_s = float(extra.get("expires_s") or 0)
        self.started_at = float(extra.get("started_at") or 0)
