"""A market order on a bucket: legs on every member venue, re-planned as they fill.

The bucket is the instrument, not a strategy: this parent is what a market
order becomes when its market id is `bucket:<id>`. Its price is the worst
price the caller accepts, in bucket terms (called `limit` inside). It asks the router
where the size should sit given the members' books, puts one leg per venue,
and every time a leg fills, a book moves or a leg is pulled, asks again for
the remainder. Legs that the new plan keeps at the same price and size are
left alone, so they hold their place in the queue; only what changed is
cancelled and re-placed, and never before `min_stay_s` has passed.

What it never does: put out legs whose sizes sum to more than is left, send
anything without a price, or keep going past the caller's limit. It stops,
and says why, when the worst price is reached with nothing resting, when what
is left is below every venue's minimum, or when `max_rounds` or `max_age_s`
is reached. A stop with a partial fill finishes `canceled`, because `done`
means filled.

What each leg has filled comes from one place, the venue's order record:
the placement's answer, then every order-status update, then a direct read
after a restart. Fill events never reach this parent; they are the ledger's.

Fees come from each venue adapter's estimate, one call per distinct level
price, cached for the parent's life. Precision is the venue's published
rules unless `params["precision"]` overrides it per market.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ...bucket import Bucket, bucket_id_of
from ...trading.types import Fill, Order, OrderRequest, OrderType, Precision, Side, TimeInForce
from .. import router
from .base import ZERO, Child, Context, D, ManagedOrder, now_ms
from .manager import register

FEE_REFERENCE_TRIES = 4
"""Larger reference orders asked for while a venue's floor hides its curve:
100, then 10,000, a million and 100 million contracts."""

FEE_REFERENCE = Decimal("100")
"""Contracts in the order a level's fee is quoted for, then divided back to
one contract. A one-contract quote overstates the rate wherever a venue
charges per order: Kalshi rounds each order's fee up to the cent, and
Opinion charges at least 0.25 USDT an order, which would read as 25c a
contract and keep the router off the venue entirely. The floor itself is
kept per member and applied to whole legs by the plan."""


@dataclass
class LegState:
    """One resting leg, by market. The `Child` holds what the venue said;
    this holds what the plan meant."""
    order_id: str
    venue: str
    side: str
    price: Decimal
    """The member's own YES price, as sent."""
    bucket_price: Decimal
    amount: Decimal
    placed_at: float
    round: int

    def to_dict(self) -> dict[str, Any]:
        return {"order_id": self.order_id, "venue": self.venue, "side": self.side, "price": str(self.price),
                "bucket_price": str(self.bucket_price), "amount": str(self.amount),
                "placed_at": self.placed_at, "round": self.round}

    @classmethod
    def of(cls, row: dict[str, Any]) -> "LegState":
        return cls(order_id=row["order_id"], venue=row.get("venue") or "", side=row["side"], price=D(row["price"]),
                   bucket_price=D(row["bucket_price"]),
                   amount=D(row["amount"]), placed_at=float(row.get("placed_at") or 0), round=int(row.get("round") or 0))


@register
class RoutedLimit(ManagedOrder):
    """`params`: `bucket` (its definition; the engine fills this in from the
    journal), `min_stay_s`, `max_rounds`, `max_age_s`, `precision`."""

    kind = "routed_limit"
    order_type = OrderType.MARKET

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        definition = self.params.get("bucket")
        if not definition:
            raise ValueError("a routed limit needs its bucket definition in params['bucket']")
        self.bucket = Bucket.model_validate(definition)
        if self.request.price is None:
            raise ValueError("a bucket order needs a price: the worst price the caller accepts")
        self.limit: Decimal = D(self.request.price)
        self.min_stay_s = float(self.params.get("min_stay_s") or 0)
        self.max_rounds = int(self.params.get("max_rounds") or 0)
        self.max_age_s = float(self.params.get("max_age_s") or 0)
        self.precision: dict[str, Precision] = {}
        for market_id in self.bucket.market_ids():
            override = (self.params.get("precision") or {}).get(market_id)
            self.precision[market_id] = Precision.model_validate(override) if override else router.default_precision(market_id)
        self.legs: dict[str, LegState] = {}
        self.rounds = 0
        self.started_at = 0.0
        self.stop_reason = ""
        self.fees_paid = ZERO
        self.fee_cache: dict[str, Decimal] = {}
        self.fee_floors: dict[str, Decimal] = {}
        """Per member, the least one taker order pays, as its venue reported."""
        """`market_id@price` -> taker fee per contract."""
        self.fee_unknown: set[str] = set()
        self._replanning = False
        self._replan_again = False
        self._resynced = False
        """False until this parent has asked the venues about its legs after
        a restart. A fresh parent sets it in `start`."""

    # -- identity: the bucket, and every member ---------------------------------

    @property
    def market_id(self) -> str:
        return self.request.market_id

    def markets(self) -> list[str]:
        return self.bucket.market_ids()

    def venues(self) -> set[str]:
        return self.bucket.venues()

    # -- what is filled, by the venue's order record ----------------------------

    @staticmethod
    def key(child: Child) -> str:
        return f"{child.venue}:{child.order_id}"

    def known(self, child: Child) -> Decimal:
        """Filled on this child as the venue's order record last said."""
        return child.filled

    @property
    def known_filled(self) -> Decimal:
        return self.filled

    @property
    def known_remaining(self) -> Decimal:
        return self.remaining

    def budget(self) -> Decimal:
        """Size new legs may add: what is left after every live leg's whole
        size is reserved and every closed leg's fill is counted."""
        reserved = sum((c.amount if c.live else self.known(c) for c in self.children), ZERO)
        return max(ZERO, self.amount - reserved)

    # -- running --------------------------------------------------------------

    async def start(self, ctx: Context) -> None:
        self._resynced = True
        self.state = "working"
        self.started_at = ctx.now
        await self.replan(ctx, first=True)

    async def resync(self, ctx: Context) -> None:
        """After a restart: the venues are the authority on what each leg
        did while this process was away. Ask every venue for every child,
        take in any child the journal links to this parent that the
        snapshot did not carry (an intent that was in doubt when the
        process died), then plan the remainder. Nothing is placed before
        this has run."""
        self._resynced = True
        for child in list(self.children):
            adapter = ctx.engine.adapters.get(child.venue)
            if adapter is None:
                continue
            try:
                order = await adapter.fetch_order(child.order_id)
            except Exception as exc:
                await ctx.publish("managed.resync_failed", {"order_id": child.order_id, "venue": child.venue,
                                                            "error": f"{type(exc).__name__}: {exc}"})
                continue
            progress = self.note_order(order, child)      # the record read back overwrites the snapshot's
            if progress is not None:
                self.fees_paid += progress.fee or ZERO
        known = {self.key(c) for c in self.children}
        for order in await ctx.engine.journal.orders_for_parent(self.id):
            if f"{order.venue}:{order.id}" in known:
                continue
            member = self.bucket.member(order.market_id)
            _, bucket_price = member.to_bucket(order.side, order.price or ZERO)
            child = self.track(order, order.amount, bucket_price)
            child.fee = order.fee or ZERO
            ctx.engine.orders.adopt_child(self.id, order.id, order.venue)
            if child.live and order.market_id not in self.legs:
                self.legs[order.market_id] = LegState(order_id=order.id, venue=order.venue, side=order.side.value,
                                                      price=order.price or ZERO, bucket_price=bucket_price,
                                                      amount=order.amount, placed_at=ctx.now, round=self.rounds)
        self._forget_dead_legs()
        await ctx.publish("managed.resynced", {"children": len(self.children), "filled": str(self.known_filled)})
        if self.state == "working":
            if self.remaining <= 0:
                await self.pull_children(ctx)
                await self.finish(ctx, "done", "filled")
            else:
                await self.replan(ctx)

    async def _ready(self, ctx: Context) -> bool:
        if not self._resynced:
            await self.resync(ctx)
        return self.state == "working"

    async def on_book(self, ctx: Context, market_id: str) -> None:
        if await self._ready(ctx):
            await self.replan(ctx)

    async def on_fill(self, ctx: Context, fill: Fill, child: Child) -> None:
        self.fees_paid += fill.fee or ZERO
        if not await self._ready(ctx):
            return
        if self.remaining <= 0:
            await self.pull_children(ctx)
            await self.finish(ctx, "done", "filled")
            return
        await self.replan(ctx)

    async def on_child(self, ctx: Context, order: Order, child: Child) -> None:
        if not await self._ready(ctx) or not order.is_terminal:
            return
        for market_id, leg in list(self.legs.items()):
            if leg.order_id == order.id and leg.venue == order.venue:
                self.legs.pop(market_id, None)
        if self.remaining <= 0:
            await self.complete_if_done(ctx)
        else:
            await self.replan(ctx)

    async def on_timer(self, ctx: Context) -> None:
        if not await self._ready(ctx):
            return
        if self.max_age_s and ctx.now - self.started_at >= self.max_age_s:
            await self.stop(ctx, "max_age")
            return
        if not self.live_children and self.remaining > 0:
            await self.replan(ctx)

    async def stop(self, ctx: Context, reason: str) -> None:
        """Stand down with what filled. `done` only when it all did."""
        self.stop_reason = reason
        await self.pull_children(ctx)
        if self.remaining <= 0:
            await self.finish(ctx, "done", "filled")
        elif self.live_children:
            self.state = "cancelling"
            await ctx.save()
        else:
            await self.finish(ctx, "canceled", f"stopped: {reason}")

    # -- the plan and the legs ------------------------------------------------

    def may_move(self, ctx: Context, leg: LegState) -> bool:
        return ctx.now - leg.placed_at >= self.min_stay_s

    async def replan(self, ctx: Context, *, first: bool = False) -> None:
        """Re-entrant safe: a cancel inside a plan can hand the engine a
        terminal child, whose `on_child` asks for a plan of its own. That
        request is noted and honoured once this plan is through."""
        if self._replanning:
            self._replan_again = True
            return
        self._replanning = True
        try:
            await self._replan(ctx, first=first)
            while self._replan_again and self.state == "working":
                self._replan_again = False
                await self._replan(ctx)
        finally:
            self._replanning = False
            self._replan_again = False

    async def _replan(self, ctx: Context, *, first: bool = False) -> None:
        if self.known_remaining <= 0:
            # Everything is filled by the venue's word; the fills the stream
            # has not delivered yet finish it through `on_fill`.
            await self.complete_if_done(ctx)
            return
        self._forget_dead_legs()
        pulled = await self._trim(ctx)
        books = {}
        for member in self.bucket.members:
            book = ctx.book(member.market_id)
            if book is not None:
                books[member.market_id] = router.view(book, member)
        if not books:
            # No book for any member yet (a restart before the streams are
            # back): nothing to plan against, and no reason to pull what
            # rests beyond what the trim took. Wait for a book.
            if not first and pulled:
                self.rounds += 1
            await ctx.publish("managed.routed", {"round": self.rounds, "placed": 0, "pulled": pulled,
                                                 "resting": len(self.legs), "remaining": str(self.known_remaining),
                                                 "reason": "no_books"})
            await ctx.save()
            return
        await self._price_fees(ctx, books)
        plan = router.plan(self.bucket, books, self.precision, self._fee,
                           side=self.side, amount=self.known_remaining, limit=self.limit, floors=self.fee_floors)
        wanted = {leg.market_id: leg for leg in plan.legs}

        # Keep a leg that is still at a price the plan wants, as long as what
        # is kept does not add up to more than is left; pull one whose price
        # is wrong, unless it has not sat for min_stay_s yet. Shrinking a leg
        # is a cancel and a re-place at the back of the queue, so a leg is
        # not pulled merely because the plan would size it smaller today. A
        # kept leg is not topped up in the same round.
        kept_resting = ZERO
        for market_id, leg in list(self.legs.items()):
            child = self.child_of(leg.order_id, leg.venue)
            if child is None or not child.live:
                self.legs.pop(market_id, None)
                continue
            want = wanted.get(market_id)
            resting = child.amount - self.known(child)
            if market_id not in books:
                # No view of this member's book: nothing says the leg is wrong.
                kept_resting += resting
                wanted.pop(market_id, None)
                continue
            if (want is not None and want.price == leg.price and want.side == leg.side
                    and kept_resting + resting <= self.known_remaining):
                kept_resting += resting
                wanted.pop(market_id)
                continue
            if not self.may_move(ctx, leg):
                wanted.pop(market_id, None)
                continue
            self.legs.pop(market_id, None)
            await ctx.cancel_child(child.order_id, child.venue)
            child.status = "canceled"
            pulled += 1

        # Never put out more than is left once every live leg is reserved.
        budget = self.budget()
        placed = 0
        for market_id, want in wanted.items():
            size = min(want.amount, budget)
            spec = self.precision[market_id]
            if size <= 0 or size < spec.min_amount:
                continue
            if spec.min_notional is not None and want.notional() * size / want.amount < spec.min_notional:
                continue
            request = self.child_request(market_id=market_id, side=want.side, price=want.price, amount=size,
                                         type=OrderType.LIMIT, time_in_force=TimeInForce.GTC)
            order = await ctx.submit_child(request)
            child = self.track(order, size, want.bucket_price)   # the child's price in bucket terms
            child.fee = order.fee or ZERO
            self.legs[market_id] = LegState(order_id=order.id, venue=order.venue, side=want.side.value, price=want.price,
                                            bucket_price=want.bucket_price, amount=size, placed_at=ctx.now,
                                            round=self.rounds)
            budget -= size
            placed += 1
        if not first and (placed or pulled):
            # A round is a re-allocation that changed something. The re-plans
            # a cancel's own echo triggers change nothing and do not count.
            self.rounds += 1
        await ctx.publish("managed.routed", {
            "round": self.rounds, "placed": placed, "pulled": pulled, "resting": len(self.legs),
            "remaining": str(self.known_remaining),
            "unfilled": str(plan.unfilled), "reason": plan.reason,
            "expected_net_price": str(plan.expected_net_price) if plan.expected_net_price is not None else None,
        })
        if not self.live_children and self.known_remaining > 0 and plan.reason:
            await self.stop(ctx, plan.reason)
            return
        if self.max_rounds and self.rounds >= self.max_rounds and self.known_remaining > 0:
            await self.stop(ctx, "max_rounds")
            return
        await ctx.save()

    async def _trim(self, ctx: Context) -> int:
        """Pull legs, newest first, until what rests no longer exceeds what
        is left. The ordinary case never needs it; a child adopted after a
        restart (an intent whose answer was lost) can put the total over."""
        by_key = {f"{leg.venue}:{leg.order_id}": (market_id, leg) for market_id, leg in self.legs.items()}
        live = [(by_key.get(self.key(child)), child) for child in self.live_children]
        excess = sum((child.amount - self.known(child) for _, child in live), ZERO) - self.known_remaining
        pulled = 0

        def newest_first(item: Any) -> tuple[int, int, float]:
            found, _ = item
            if found is None:            # a child no plan of this parent placed: first to go
                return (1, 0, 0.0)
            _, leg = found
            return (0, leg.round, leg.placed_at)

        for found, child in sorted(live, key=newest_first, reverse=True):
            if excess <= 0:
                break
            excess -= child.amount - self.known(child)
            if found is not None:
                self.legs.pop(found[0], None)
            await ctx.cancel_child(child.order_id, child.venue)
            child.status = "canceled"
            pulled += 1
        if pulled:
            await ctx.publish("managed.trimmed", {"pulled": pulled, "remaining": str(self.known_remaining)})
        return pulled

    def _forget_dead_legs(self) -> None:
        for market_id, leg in list(self.legs.items()):
            child = self.child_of(leg.order_id, leg.venue)
            if child is None or not child.live:
                self.legs.pop(market_id, None)

    # -- fees -----------------------------------------------------------------

    def _fee(self, market_id: str, price: Decimal, contracts: Decimal) -> Decimal:
        return self.fee_cache.get(f"{market_id}@{price}", ZERO) * contracts

    async def _price_fees(self, ctx: Context, books: dict[str, router.BookView]) -> None:
        """One estimate per distinct level price, from the member's own venue."""
        for member in self.bucket.members:
            seen = books.get(member.market_id)
            if seen is None:
                continue
            rows = seen.asks if self.side == Side.BUY else seen.bids
            for price, _ in rows:
                key = f"{member.market_id}@{price}"
                if key in self.fee_cache:
                    continue
                self.fee_cache[key] = await self._estimate(ctx, member.market_id, member.to_member(self.side, price))

    async def _estimate(self, ctx: Context, market_id: str, member_order: tuple[Side, Decimal]) -> Decimal:
        venue = market_id.split(":", 1)[0]
        adapter = ctx.engine.adapters.get(venue)
        has = getattr(adapter, "has", {}) or {}
        if adapter is None or not has.get("fetch_fee_estimate"):
            if market_id not in self.fee_unknown:
                self.fee_unknown.add(market_id)
                await ctx.publish("managed.fee_unknown", {"market_id": market_id})
            return ZERO
        side, price = member_order
        reference = FEE_REFERENCE
        fee = None
        for _ in range(FEE_REFERENCE_TRIES):
            try:
                estimate = await adapter.fetch_fee_estimate(market_id, side, price, reference)
            except Exception as exc:   # a fee the venue would not quote is not a reason to stop
                await ctx.publish("managed.fee_unknown", {"market_id": market_id, "error": str(exc)})
                return ZERO
            fee = getattr(estimate, "taker_fee", None)
            floor = getattr(estimate, "min_fee", None)
            if floor is not None:
                self.fee_floors[market_id] = D(floor, ZERO)
            if fee is None or floor is None or D(fee, ZERO) > D(floor, ZERO):
                break
            # The floor is all this quote shows: ask for a larger order until
            # the curve under it does.
            reference *= 100
        return D(fee, ZERO) / reference if fee is not None else ZERO

    # -- reporting ------------------------------------------------------------

    def average_price(self) -> Decimal | None:
        filled = self.known_filled
        if filled <= 0:
            return None
        spent = sum((self.known(c) * (c.price or ZERO) for c in self.children), ZERO)
        return spent / filled

    def as_order(self) -> Order:
        """As the base class, but filled and the average by the venue's word,
        in bucket terms."""
        order = super().as_order()
        filled = self.known_filled
        return order.model_copy(update={"filled": filled, "remaining": max(ZERO, self.amount - filled),
                                        "average_price": self.average_price()})

    def report(self) -> dict[str, Any]:
        """What filled, where, at what: bucket-terms prices, per market."""
        per_market: dict[str, dict[str, Any]] = {}
        for child in self.children:
            slot = per_market.setdefault(child.venue, {"filled": ZERO, "cost": ZERO})
            slot["filled"] += self.known(child)
            slot["cost"] += self.known(child) * (child.price or ZERO)
        breakdown = []
        for venue, slot in per_market.items():
            breakdown.append({"venue": venue, "filled": str(slot["filled"]),
                              "average_price": str(slot["cost"] / slot["filled"]) if slot["filled"] > 0 else None})
        return {
            "order_id": self.id, "bucket_id": self.bucket.id, "side": self.side.value, "amount": str(self.amount), "worst_price": str(self.limit),
            "filled": str(self.known_filled), "unfilled": str(self.known_remaining),
            "average_price": str(self.average_price()) if self.average_price() is not None else None,
            "fees_paid": str(self.fees_paid), "rounds": self.rounds, "state": self.state,
            "stop_reason": self.stop_reason, "per_venue": breakdown, "detail": self.detail or None,
        }

    # -- persistence ----------------------------------------------------------

    def extra(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket.model_dump(mode="json"), "limit": str(self.limit),
            "min_stay_s": self.min_stay_s, "max_rounds": self.max_rounds, "max_age_s": self.max_age_s,
            "precision": {k: v.model_dump(mode="json") for k, v in self.precision.items()},
            "legs": {k: v.to_dict() for k, v in self.legs.items()},
            "rounds": self.rounds, "started_at": self.started_at, "stop_reason": self.stop_reason,
            "fees_paid": str(self.fees_paid), "fee_cache": {k: str(v) for k, v in self.fee_cache.items()},
            "fee_floors": {k: str(v) for k, v in self.fee_floors.items()},
        }

    def load_extra(self, extra: dict[str, Any]) -> None:
        if extra.get("bucket"):
            self.bucket = Bucket.model_validate(extra["bucket"])
        self.limit = D(extra.get("limit"), self.limit)
        self.min_stay_s = float(extra.get("min_stay_s") or 0)
        self.max_rounds = int(extra.get("max_rounds") or 0)
        self.max_age_s = float(extra.get("max_age_s") or 0)
        for market_id, row in (extra.get("precision") or {}).items():
            self.precision[market_id] = Precision.model_validate(row)
        self.legs = {k: LegState.of(v) for k, v in (extra.get("legs") or {}).items()}
        self.rounds = int(extra.get("rounds") or 0)
        self.started_at = float(extra.get("started_at") or 0)
        self.stop_reason = extra.get("stop_reason") or ""
        self.fees_paid = D(extra.get("fees_paid"), ZERO)
        self.fee_cache = {k: D(v) for k, v in (extra.get("fee_cache") or {}).items()}
        self.fee_floors = {k: D(v) for k, v in (extra.get("fee_floors") or {}).items()}


__all__ = ["RoutedLimit", "LegState", "bucket_id_of"]
