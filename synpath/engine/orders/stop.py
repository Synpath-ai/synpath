"""Stops: stop-market, stop-limit, and trailing.

None of the venues holds a stop on a prediction market, so the engine holds
it: the parent is `waiting` and nothing is sent until the watched price
reaches `stop_price` -- a buy stop when it rises to it, a sell stop when it
falls to it. Then it is `triggered`, once and for good, and a child goes to
the venue for the remaining quantity, carrying the parent's id.

**What is watched** is `trigger_source`, a field of the order. `touch`, the
default, is the side that would fill you: the best ask for a buy stop, the
best bid for a sell stop. A prediction market's book is thin and its tape is
slow: a market that last traded four hours ago would leave a last-trade
stop asleep through a move, and a mid between a 0.30 bid and a 0.70 ask is
a number nobody can trade at. The touch is where the triggered order could
actually execute. `last` (the last public trade) and `mid` are there for
callers who disagree, and the journal records which was used.

**Stop market** (`type="stop_market"`). `time_in_force` is how long the stop
stays armed: `gtc` (the default), `gtd` until `expires_at`, or `day` until
the engine's session end; `ioc` and `fok` are refused. A stop whose time
runs out before it triggers ends `expired`. The child is a `market` order,
`ioc`. No venue here takes a market order without a worst price, so the
engine sets the legal venue price bound for the side, using the cached
market precision when supplied and the venue grid otherwise. The caller
never supplies a slippage cap. With
no book to price against at the moment of triggering, the stop ends
`rejected` rather than send an order that would take whatever appears. A
child that fills ends the stop `closed`; an IOC remainder that does not
fill ends it with the unfilled quantity recorded.

**Stop limit** (`type="stop_limit"`). The child is a `limit` at `price`
(at or below `stop_price` for a sell, at or above it for a buy) with the
order's own `time_in_force` (`gtc`, `gtd`, `ioc`, `fok`) and `expires_at`,
and it may rest. A `gtd` stop that expires before it triggers, or with its
child still resting, ends `expired`; a cancel ends it `canceled` before or
after the trigger; a child the venue refuses ends it `rejected`.

**Trailing** ratchets: a sell stop follows the highest bid seen, a buy stop
the lowest ask, never the other way. The distance is `trail` in price units
or `trail_percent` of the watched price, and the current stop is persisted,
so a restart resumes from the level the market actually reached rather than
from where it started. With a `price` it sends a limit; without one, a
market child with its existing five-cent touch protection.

**Statuses**, as the parent `Order` reports them: `waiting`, `triggered`,
then `closed`, `canceled`, `rejected` or `expired`.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from ...trading.instruments import VENUE_RULES
from ...trading.money import round_to
from ...trading.types import Fill, Order, OrderRequest, Precision, OrderStatus, OrderType, Side, TimeInForce, TriggerSource
from ..storage_types import IntentState
from .base import Child, Context, D, ManagedOrder
from .manager import register

TRIGGER_SOURCES = tuple(source.value for source in TriggerSource)

TRAILING_PROTECTION = Decimal("0.05")
"""Legacy trailing-stop protection; stop-market uses legal venue bounds."""


class _Stop(ManagedOrder):
    """Shared machinery: watch a price, fire once, then work the child."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stop_price: Decimal = D(self.request.stop_price)
        self.trigger_source: str = TriggerSource(self.request.trigger_source).value
        if self.trigger_source not in TRIGGER_SOURCES:
            raise ValueError(f"trigger_source must be one of {TRIGGER_SOURCES}, not {self.trigger_source!r}")
        self.triggered_at: int | None = None
        self.trigger_price: Decimal | None = None
        self.pending_child: OrderRequest | None = None
        self.pending_precision: Precision | None = None
        self.pending_end: str | None = None

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

    async def on_trade(self, ctx: Context, market_id: str, price: Decimal, quantity: Decimal) -> None:
        if self.trigger_source == "last":
            timestamp = ctx.last_trade_timestamp(market_id)
            if timestamp is not None and timestamp < self.created_ms:
                return
            await self.consider(ctx, price)

    async def consider(self, ctx: Context, price: Decimal | None = None) -> None:
        if self.state != "waiting":
            return
        if self.request.expires_at is not None and int(ctx.now * 1000) >= self.request.expires_at:
            await self.expire(ctx)
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
        await self.send_child(ctx, watched)

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        raise NotImplementedError

    def child_price_bound(self, ctx: Context) -> Decimal | None:
        """Stop market takes all available prices on the instrument's legal
        grid. Trailing stops retain their existing touch protection policy.
        No ready, uncrossed executable book means no market dispatch."""
        touch = ctx.touch(self.side)
        if touch is None:
            return None
        precision = ctx.registered_precision() or Precision(
            tick=VENUE_RULES.get(self.venue, {}).get("default_tick") or Decimal("0.01"))
        tick = precision.tick
        if self.kind == "stop_market":
            self.pending_precision = precision
            if self.side == Side.SELL:
                return tick
            ceiling = round_to(precision.face_value, tick, "down")
            return ceiling - tick if ceiling == precision.face_value else ceiling
        if self.side == Side.BUY:
            return min(round_to(touch + TRAILING_PROTECTION, tick, "down"), 1 - tick)
        return max(round_to(touch - TRAILING_PROTECTION, tick, "up"), tick)

    async def submit_planned_child(self, ctx: Context, request: OrderRequest) -> None:
        """Persist one stable dispatch identity with the trigger before sending."""
        identity = f"{self.request.user_id}:{self.account_key}:{self.id}:stop-child"
        request = request.model_copy(update={"client_order_id": f"sp-{uuid.uuid5(uuid.NAMESPACE_URL, identity).hex[:20]}"})
        self.pending_child = request
        await ctx.plan_child(request)
        await ctx.save()
        await ctx.publish("managed.triggered", {"kind": self.kind, "trigger_source": self.trigger_source,
                                               "stop_price": str(self.stop_price), "trigger_price": str(self.trigger_price),
                                               "child_client_order_id": request.client_order_id})
        if not ctx.defer_pending_action():
            await self.resume_pending(ctx)

    async def resume_pending(self, ctx: Context, *, allow_send: bool = True) -> None:
        request = self.pending_child
        if request is None:
            return
        if request.type == OrderType.LIMIT and request.expires_at is not None and int(ctx.now * 1000) >= request.expires_at:
            self.pending_end = "expired"
            self.state = "cancelling"
            self.detail = "expired before pending dispatch completed"
            allow_send = False
            await ctx.save()
        order, intent = await ctx.observe_pending_child(request.client_order_id)
        if order is None and intent is not None and intent.state == IntentState.PLANNED:
            if not allow_send:
                intent = await ctx.settle_unsent_child(request.client_order_id)
            else:
                order = await ctx.submit_child(request, precision=self.pending_precision)
        if order is not None:
            if self.child_of(order.id, order.venue) is None:
                self.track(order, request.quantity, request.price)
            self.pending_child = None
            self.pending_precision = None
            await ctx.save()
            if self.pending_end:
                await self.cancel(ctx, self.detail, end=self.pending_end)
            else:
                await self.on_child(ctx, order, self.child_of(order.id, order.venue))
            return
        if intent is not None and intent.state in (IntentState.REJECTED, IntentState.FAILED, IntentState.SETTLED):
            self.pending_child = None
            self.pending_precision = None
            await self.finish(ctx, self.pending_end or "rejected", intent.detail or "child was not accepted")
        else:
            self.detail = "child submission is unresolved; refusing to repeat the venue send"
            await ctx.save()

    async def recover(self, ctx: Context) -> None:
        expires_at = self.request.expires_at
        expired = expires_at is not None and int(ctx.now * 1000) >= expires_at
        if expired and (self.state == "waiting" or self.kind != "stop_market"):
            await self.expire(ctx)
        if self.live:
            await self.on_timer(ctx)

    async def on_timer(self, ctx: Context) -> None:
        if (self.pending_child is not None or self.state == "cancelling") and ctx.defer_pending_action():
            return
        if self.pending_child is not None:
            await self.resume_pending(ctx, allow_send=self.state == "working" and self.pending_end is None)
        elif self.state == "cancelling":
            await self.cancel(ctx, self.detail, end=self.pending_end or "canceled")

    async def cancel(self, ctx: Context, reason: str = "cancelled", *, end: str = "canceled") -> None:
        if not self.live:
            return
        self.pending_end = end
        self.detail = reason
        if self.pending_child is not None:
            self.state = "cancelling"
            await ctx.save()
            await self.resume_pending(ctx, allow_send=False)
            return
        await super().cancel(ctx, reason, end=end)
        if self.remaining <= 0 and not self.live_children:
            await self.finish(ctx, "done", "filled before cancellation completed")

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        await self.complete_if_done(ctx)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if not order.is_terminal or self.state != "working":
            return
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
        elif not self.live_children:
            await self.child_ended(ctx, order)

    async def child_ended(self, ctx: Context, order: Order) -> None:
        """The child ended without finishing the job: say so rather than
        quietly leaving a stop that has already fired."""
        unfilled = f"child {order.status.value} with {self.remaining} unfilled"
        if order.status == OrderStatus.REJECTED and self.filled <= 0:
            await self.finish(ctx, "rejected", unfilled)
        elif order.status == OrderStatus.EXPIRED:
            await self.finish(ctx, "expired", unfilled)
        else:
            # An immediate-or-cancel child's remainder: the stop is over, and
            # what did not fill is on the record.
            await self.finish(ctx, "done", unfilled)

    async def expire(self, ctx: Context) -> None:
        """`expires_at` passed. A stop still waiting ends `expired` without
        having sent anything."""
        if self.state == "waiting":
            await self.finish(ctx, "expired", "expired before it triggered")
            return
        await super().expire(ctx)

    # -- persistence ----------------------------------------------------------

    def extra(self) -> dict[str, Any]:
        return {
            "stop_price": str(self.stop_price), "trigger_source": self.trigger_source,
            "pending_child": self.pending_child.model_dump(mode="json") if self.pending_child else None,
            "pending_precision": self.pending_precision.model_dump(mode="json") if self.pending_precision else None,
            "pending_end": self.pending_end,
            "triggered_at": self.triggered_at,
            "trigger_price": str(self.trigger_price) if self.trigger_price is not None else None,
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.stop_price = D(extra.get("stop_price"), self.stop_price)
        self.trigger_source = extra.get("trigger_source", self.trigger_source)
        self.triggered_at = extra.get("triggered_at")
        self.trigger_price = D(extra.get("trigger_price"))
        self.pending_child = OrderRequest.model_validate(extra["pending_child"]) if extra.get("pending_child") else None
        self.pending_precision = Precision.model_validate(extra["pending_precision"]) if extra.get("pending_precision") else None
        self.pending_end = extra.get("pending_end")


@register
class StopMarket(_Stop):
    """Fires one IOC child within the legal venue price bounds."""

    kind = "stop_market"
    order_type = OrderType.STOP_MARKET

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        price = self.child_price_bound(ctx)
        if price is None:
            await self.finish(ctx, "rejected", "no book to price the market order against: refusing to send at any price")
            return
        request = self.child_request(quantity=self.remaining, price=price, type=OrderType.MARKET,
                                     time_in_force=TimeInForce.IOC)
        await self.submit_planned_child(ctx, request)

    async def expire(self, ctx: Context) -> None:
        """`time_in_force` is how long the stop stays armed. Once triggered,
        its immediate-or-cancel child ends on its own."""
        if self.state == "waiting":
            await super().expire(ctx)


@register
class StopLimit(_Stop):
    """Fires a limit child at the price the caller named."""

    kind = "stop_limit"
    order_type = OrderType.STOP_LIMIT

    async def send_child(self, ctx: Context, watched: Decimal) -> None:
        price = D(self.request.price)
        request = self.child_request(quantity=self.remaining, price=price, type=OrderType.LIMIT,
                                     time_in_force=self.request.time_in_force or TimeInForce.GTC,
                                     expires_at=self.request.expires_at, post_only=self.request.post_only)
        await self.submit_planned_child(ctx, request)


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
        price = D(self.request.price) or self.child_price_bound(ctx)
        if price is None:
            await self.finish(ctx, "rejected", "no book to price the market order against: refusing to send at any price")
            return
        kind = OrderType.LIMIT if self.request.price is not None else OrderType.MARKET
        request = self.child_request(quantity=self.remaining, price=price, type=kind,
                                     time_in_force=TimeInForce.IOC if kind == OrderType.MARKET else TimeInForce.GTC)
        await self.submit_planned_child(ctx, request)

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
