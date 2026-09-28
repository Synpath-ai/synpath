"""Stops: stop-market, stop-limit, and trailing.

None of the three venues holds a stop on a prediction market, so the engine
watches the price and sends a child when the level is reached. What "the
price" means is the whole question, and the answer here is the touch on the
side that would fill you: a buy stop watches the best ask, a sell stop the
best bid.

Why not the last trade or the mid, which most venues use? A prediction
market's book is thin and its tape is slow. A market that last traded four
hours ago would leave a last-trade stop asleep through a move; a mid sitting
between a 0.30 bid and a 0.70 ask is a number nobody can trade at. The touch
is the price at which the triggered order could actually execute, so it is
the price the trigger should use. `trigger_source` takes `last` or `mid` for
callers who disagree, and says so in the journal either way.

**Trailing** ratchets: a sell stop follows the highest bid seen, a buy stop
the lowest ask, never the other way. The distance is `trail` in price units
or `trail_percent` of the watched price, and the current stop is persisted,
so a restart resumes from the level the market actually reached rather than
from where it started.

**Triggering is one-way.** Once fired, the child is out; a price that comes
back does not un-trigger it. A stop-market child is an immediate-or-cancel
limit at a protection price (the touch plus `max_slippage`, or the caller's
own), because no venue here accepts a market order without one.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ...trading.types import Fill, Order, OrderRequest, OrderType, Side, TimeInForce
from .base import ZERO, Child, Context, D, ManagedOrder
from .manager import register

TRIGGER_SOURCES = ("touch", "last", "mid")


class _Stop(ManagedOrder):
    """Shared machinery: watch a price, fire once, then work the child."""

    default_slippage = Decimal("0.05")

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stop_price: Decimal = D(self.request.stop_price)
        self.trigger_source: str = str(self.params.get("trigger_source") or "touch")
        if self.trigger_source not in TRIGGER_SOURCES:
            raise ValueError(f"trigger_source must be one of {TRIGGER_SOURCES}, not {self.trigger_source!r}")
        self.triggered_at: int | None = None
        self.trigger_price: Decimal | None = None

    # -- the watched price ----------------------------------------------------

    def watched(self, ctx: Context) -> Decimal | None:
        if self.trigger_source == "touch":
            return ctx.touch(self.side)
        if self.trigger_source == "mid":
            return ctx.mid()
        return ctx.last()

    def reached(self, price: Decimal) -> bool:
        """A buy stop fires when the price rises to it, a sell stop when it falls."""
        return price >= self.stop_price if self.side == Side.BUY else price <= self.stop_price

    async def on_book(self, ctx: Context, market_id: str) -> None:
        if self.trigger_source in ("touch", "mid"):
            await self.consider(ctx)

    async def on_trade(self, ctx: Context, market_id: str, price: Decimal, amount: Decimal) -> None:
        if self.trigger_source == "last":
            await self.consider(ctx, price)

    async def consider(self, ctx: Context, price: Decimal | None = None) -> None:
        if self.state != "waiting":
            return
        watched = price if price is not None else self.watched(ctx)
        if watched is None:
            return
        await self.adjust(ctx, watched)
        if self.reached(watched):
            await self.trigger(ctx, watched)

    async def adjust(self, ctx: Context, watched: Decimal) -> None:
        """Trailing overrides this; a fixed stop does not move."""

    # -- firing ---------------------------------------------------------------

    async def trigger(self, ctx: Context, watched: Decimal) -> None:
        self.state = "working"
        self.triggered_at = int(ctx.now * 1000)
        self.trigger_price = watched
        await ctx.publish("managed.triggered", {
            "kind": self.kind, "stop_price": str(self.stop_price), "trigger_price": str(watched),
            "trigger_source": self.trigger_source,
        })
        await self.send_child(ctx, watched)

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        raise NotImplementedError

    def protection(self, ctx: Context, watched: Decimal) -> Decimal | None:
        """The worst price the triggered order may pay."""
        given = D(self.params.get("protection"))
        if given is not None:
            return given
        slippage = D(self.params.get("max_slippage"), self.default_slippage)
        touch = ctx.touch(self.side)
        if touch is None:
            # No book to price against. A last-trade trigger can fire on a
            # market nobody is quoting, and an order sent then would take
            # whatever appears.
            return None
        return touch + slippage if self.side == Side.BUY else touch - slippage

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        await self.complete_if_done(ctx)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if not order.is_terminal or self.state != "working":
            return
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
        elif not self.live_children:
            # The child ended without finishing the job: say so rather than
            # quietly leaving a stop that has already fired.
            await self.finish(ctx, "done", f"child {order.status.value} with {self.remaining} unfilled")

    # -- persistence ----------------------------------------------------------

    def extra(self) -> dict[str, Any]:
        return {
            "stop_price": str(self.stop_price), "trigger_source": self.trigger_source,
            "triggered_at": self.triggered_at,
            "trigger_price": str(self.trigger_price) if self.trigger_price is not None else None,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.stop_price = D(extra.get("stop_price"), self.stop_price)
        self.trigger_source = extra.get("trigger_source", self.trigger_source)
        self.triggered_at = extra.get("triggered_at")
        self.trigger_price = D(extra.get("trigger_price"))


@register
class StopMarket(_Stop):
    """Fires an immediate-or-cancel child at the protection price."""

    kind = "stop_market"
    order_type = OrderType.STOP_MARKET

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        price = self.protection(ctx, watched)
        if price is None:
            await self.finish(ctx, "rejected", "no book and no protection price: refusing to send at any price")
            return
        request = self.child_request(amount=self.remaining, price=price, type=OrderType.MARKET,
                                     time_in_force=TimeInForce.IOC)
        order = await ctx.submit_child(request)
        self.track(order, request.amount, price)
        await ctx.save()


@register
class StopLimit(_Stop):
    """Fires a limit child at the price the caller named."""

    kind = "stop_limit"
    order_type = OrderType.STOP_LIMIT

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        price = D(self.request.price)
        request = self.child_request(amount=self.remaining, price=price, type=OrderType.LIMIT,
                                     time_in_force=self.request.time_in_force or TimeInForce.GTC,
                                     expires_at=self.request.expires_at)
        order = await ctx.submit_child(request)
        self.track(order, request.amount, price)
        await ctx.save()


@register
class TrailingStop(_Stop):
    """A stop that follows the market one way and never the other."""

    kind = "trailing_stop"
    order_type = OrderType.TRAILING_STOP

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.trail = D(self.params.get("trail"))
        self.trail_percent = D(self.params.get("trail_percent"))
        if self.trail is None and self.trail_percent is None:
            raise ValueError("a trailing stop needs params trail or trail_percent")
        self.extreme: Decimal | None = None
        """The best price seen: the highest for a sell stop, the lowest for a buy."""

    def distance(self, watched: Decimal) -> Decimal:
        if self.trail is not None:
            return self.trail
        return watched * self.trail_percent / Decimal("100")

    async def adjust(self, ctx: Context, watched: Decimal) -> None:
        improved = (
            self.extreme is None
            or (self.side == Side.SELL and watched > self.extreme)
            or (self.side == Side.BUY and watched < self.extreme)
        )
        if not improved:
            return
        self.extreme = watched
        distance = self.distance(watched)
        moved = watched - distance if self.side == Side.SELL else watched + distance
        # The stop only ever tightens towards the market.
        if self.stop_price is None or (moved > self.stop_price if self.side == Side.SELL else moved < self.stop_price):
            self.stop_price = moved
            await ctx.publish("managed.trailed", {"stop_price": str(moved), "watched": str(watched)})
            await ctx.save()

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        price = D(self.request.price) or self.protection(ctx, watched)
        kind = OrderType.LIMIT if self.request.price is not None else OrderType.MARKET
        request = self.child_request(amount=self.remaining, price=price, type=kind,
                                     time_in_force=TimeInForce.IOC if kind == OrderType.MARKET else TimeInForce.GTC)
        order = await ctx.submit_child(request)
        self.track(order, request.amount, price)
        await ctx.save()

    def extra(self) -> dict[str, Any]:
        return super().extra() | {
            "trail": str(self.trail) if self.trail is not None else None,
            "trail_percent": str(self.trail_percent) if self.trail_percent is not None else None,
            "extreme": str(self.extreme) if self.extreme is not None else None,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        super().load_extra(extra)
        self.trail = D(extra.get("trail"))
        self.trail_percent = D(extra.get("trail_percent"))
        self.extreme = D(extra.get("extreme"))
