"""One cancels the other, and the bracket built on it.

**OCO** holds two orders where only one should happen: take profit at 0.70
or stop out at 0.40, buy this market or that one. Each leg is a real order
in its own right -- a limit at the venue, or another engine-held order like
a stop -- and the parent's job is to keep them in step.

The linkage is by size, not by existence, because a partial fill is the
common case on a thin book. When one leg fills two of five contracts, the
other is replaced at three: leaving it at five would let the pair fill seven.
When one leg is done, the other is pulled.

**Bracket** is an entry with that pair attached: buy ten at 0.42, and when it
fills, protect it with a take-profit and a stop-loss sized to what actually
filled. The protection goes out as the entry fills, contract by contract, so
a position is never unprotected while the entry works. An entry that is
cancelled with nothing filled takes the bracket with it.

Both are written to the journal like any other engine-held order, so a
restart finds the pair and its sizes rather than two loose orders nobody
links any more.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from ...trading.types import Fill, Order, OrderRequest, OrderType, Side, TimeInForce
from .base import ZERO, Child, Context, D, ManagedOrder
from .manager import register

VENUE_TYPES = (OrderType.LIMIT, OrderType.MARKET)


def leg_request(spec: Mapping[str, Any], *, parent: ManagedOrder, amount: Decimal | None = None) -> OrderRequest:
    """One leg's request, inheriting what the parent already knows."""
    body: dict[str, Any] = {
        "market_id": spec.get("market_id", parent.market_id),
        "side": spec.get("side", parent.side.value),
        "amount": str(amount if amount is not None else D(spec.get("amount"), parent.amount)),
        "type": spec.get("type", "limit"),
        "account": parent.account.model_dump(mode="json"),
        "book": parent.request.book,
        "trader": parent.request.trader,
        "tags": {**parent.request.tags, "parent": parent.id},
    }
    for key in ("price", "stop_price", "time_in_force", "expires_at", "post_only", "reduce_only", "params"):
        if spec.get(key) is not None:
            body[key] = spec[key]
    if "reduce_only" not in body and parent.request.reduce_only:
        body["reduce_only"] = True
    return OrderRequest.model_validate(body)


class _Linked(ManagedOrder):
    """Shared machinery for parents whose legs must be resized together."""

    async def place_leg(self, ctx: Context, spec: Mapping[str, Any], amount: Decimal, tag: str) -> Child:
        request = leg_request(spec, parent=self, amount=amount)
        order = await ctx.submit_child(request)
        child = self.track(order, amount, D(request.price))
        self.leg_of[child.order_id] = tag
        self.spec_of[tag] = dict(spec)
        await ctx.publish("managed.leg", {"leg": tag, "order_id": order.id, "amount": str(amount),
                                          "type": str(spec.get("type", "limit"))})
        return child

    async def resize_leg(self, ctx: Context, tag: str, amount: Decimal) -> None:
        """Cancel and re-place a leg at a new size; below one contract, pull it."""
        for child in list(self.live_children):
            if self.leg_of.get(child.order_id) != tag:
                continue
            await ctx.cancel_child(child.order_id)
            child.status = "canceled"
        if amount <= 0:
            return
        spec = self.spec_of.get(tag)
        if spec:
            await self.place_leg(ctx, spec, amount, tag)

    def leg_children(self, tag: str) -> list[Child]:
        return [c for c in self.children if self.leg_of.get(c.order_id) == tag]

    def extra(self) -> dict[str, Any]:
        return {"leg_of": dict(self.leg_of), "spec_of": {k: dict(v) for k, v in self.spec_of.items()}}

    def load_extra(self, extra: dict[str, Any]) -> None:
        self.leg_of = dict(extra.get("leg_of") or {})
        self.spec_of = {k: dict(v) for k, v in (extra.get("spec_of") or {}).items()}


@register
class OCO(_Linked):
    """`params`: `legs`, a list of two order specifications."""

    kind = "oco"
    order_type = OrderType.OCO

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.leg_of: dict[str, str] = {}
        self.spec_of: dict[str, dict[str, Any]] = {}
        legs = self.params.get("legs") or []
        if len(legs) != 2:
            raise ValueError("an OCO needs params legs: exactly two order specifications")
        self.legs = [dict(leg) for leg in legs]

    def markets(self) -> list[str]:
        return list({self.market_id, *(str(leg.get("market_id") or self.market_id) for leg in self.legs)})

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        for index, spec in enumerate(self.legs):
            await self.place_leg(ctx, spec, self.remaining, f"leg{index}")
        await ctx.save()

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        tag = self.leg_of.get(child.order_id, "")
        other = "leg1" if tag == "leg0" else "leg0"
        if self.remaining <= 0:
            await self.cancel_other(ctx, other, "the other leg filled")
            await self.finish(ctx, "done", "filled")
            return
        # Partly filled: the other leg must not be able to fill the same size.
        await self.resize_leg(ctx, other, self.remaining)
        await ctx.save()

    async def cancel_other(self, ctx: Context, tag: str, reason: str) -> None:
        for child in self.leg_children(tag):
            if child.live:
                await ctx.cancel_child(child.order_id)
                child.status = "canceled"
        await ctx.publish("managed.leg_canceled", {"leg": tag, "reason": reason})

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if not order.is_terminal or self.state != "working":
            return
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
            return
        if not self.live_children:
            await self.finish(ctx, "canceled", f"both legs ended with {self.remaining} unfilled")

    def extra(self) -> dict[str, Any]:
        return super().extra() | {"legs": self.legs}

    def load_extra(self, extra: dict[str, Any]) -> None:
        super().load_extra(extra)
        self.legs = [dict(leg) for leg in extra.get("legs") or self.params.get("legs") or []]


@register
class Bracket(_Linked):
    """`params`: `entry` (optional), `take_profit`, `stop_loss`.

    Without an entry the protection is placed at once, for a position that
    already exists.
    """

    kind = "bracket"
    order_type = OrderType.BRACKET

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.leg_of: dict[str, str] = {}
        self.spec_of: dict[str, dict[str, Any]] = {}
        self.entry = dict(self.params["entry"]) if self.params.get("entry") else None
        self.take_profit = dict(self.params.get("take_profit") or {})
        self.stop_loss = dict(self.params.get("stop_loss") or {})
        if not self.take_profit and not self.stop_loss:
            raise ValueError("a bracket needs params take_profit, stop_loss, or both")
        self.entered: Decimal = ZERO
        """Contracts the entry has filled, which is what the protection covers."""
        self.protected: Decimal = ZERO

    def markets(self) -> list[str]:
        return [self.market_id]

    def exit_side(self) -> str:
        """Protection closes the position the entry opens."""
        return (Side.SELL if self.side == Side.BUY else Side.BUY).value

    async def start(self, ctx: Context) -> None:
        self.state = "working"
        if self.entry is not None:
            await self.place_leg(ctx, {**self.entry, "side": self.side.value}, self.amount, "entry")
        else:
            self.entered = self.amount
            await self.protect(ctx, self.amount)
        await ctx.save()

    async def protect(self, ctx: Context, amount: Decimal) -> None:
        """Put the take-profit and the stop-loss out for `amount` contracts."""
        if amount <= 0:
            return
        side = self.exit_side()
        if self.take_profit:
            await self.place_leg(ctx, {**self.take_profit, "side": side}, amount, "take_profit")
        if self.stop_loss:
            spec = {"type": "stop_market", **self.stop_loss, "side": side}
            await self.place_leg(ctx, spec, amount, "stop_loss")
        self.protected += amount
        await ctx.publish("managed.protected", {"amount": str(amount), "protected": str(self.protected)})

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        tag = self.leg_of.get(child.order_id, "")
        if tag == "entry":
            self.entered += fill.amount
            # Protect what has actually been bought, as it is bought.
            await self.protect(ctx, fill.amount)
            await ctx.save()
            return
        # An exit filled: the other side of the protection must shrink.
        other = "stop_loss" if tag == "take_profit" else "take_profit"
        remaining = max(ZERO, self.protected - self.exit_filled())
        await self.resize_leg(ctx, other, remaining)
        if remaining <= 0 and self.entry_done():
            await self.finish(ctx, "done", "the position was closed")
        await ctx.save()

    def exit_filled(self) -> Decimal:
        return sum((c.filled for c in self.children if self.leg_of.get(c.order_id) in ("take_profit", "stop_loss")), ZERO)

    def entry_done(self) -> bool:
        if self.entry is None:
            return True
        entry_children = self.leg_children("entry")
        return bool(entry_children) and not any(c.live for c in entry_children)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        tag = self.leg_of.get(child.order_id, "")
        if self.state != "working" or not order.is_terminal:
            return
        if tag == "entry" and self.entered <= 0:
            await self.finish(ctx, "canceled", "the entry ended without filling")
            return
        if tag in ("take_profit", "stop_loss") and self.exit_filled() >= self.protected > 0:
            await self.finish(ctx, "done", "the position was closed")

    async def cancel(self, ctx: Context, reason: str = "cancelled") -> None:
        await super().cancel(ctx, reason)

    def extra(self) -> dict[str, Any]:
        return super().extra() | {
            "entry": self.entry, "take_profit": self.take_profit, "stop_loss": self.stop_loss,
            "entered": str(self.entered), "protected": str(self.protected),
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        super().load_extra(extra)
        self.entry = dict(extra["entry"]) if extra.get("entry") else None
        self.take_profit = dict(extra.get("take_profit") or {})
        self.stop_loss = dict(extra.get("stop_loss") or {})
        self.entered = D(extra.get("entered"), ZERO)
        self.protected = D(extra.get("protected"), ZERO)
