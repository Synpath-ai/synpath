"""A bucket's merged book and its router preview, computed in `synpath serve`.

Two things a screen needs before it routes an order on a bucket, both made
with the library's own router (`synpath.engine.router`), so what a client
shows is what an order on the bucket would see:

* **The merged book** (`bucket:<id>` on `/ws/market`): every member's live
  book in bucket terms (`router.view`, a flipped member read through its NO
  side) laid side by side by `router.merge`, each level net of its venue's
  taker fee and labelled with the member it came from. The members' books are
  the hub's shared venue streams; a bucket subscription opens no connection
  of its own. A member switched off (`enabled=False` on the definition) is
  not subscribed, adds no levels and does not hold `ready` back.
* **The plan** (`POST /trading/buckets/{id}/plan`): `router.plan` for a side,
  a quantity and a worst price, on the same books, the same fees and the
  venues' published precision, over the enabled members only. Nothing is placed.

Fees are each member's published fee schedule (`fetch_fee_schedule` through
the read app's registry, the formula the Kalshi adapter's own estimate uses),
read once per member and kept for `FEE_SCHEDULE_TTL_S`. A level's fee is
quoted for `FEE_REFERENCE` contracts and divided back to one, and a venue's
per-order floor is kept apart for the plan, exactly as an order on the bucket
prices its levels (`synpath.engine.orders.routed`). A member whose fee the
library cannot price counts as free and is named in `fees_unknown`.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from .. import ids
from ..bucket import Bucket, BucketMember
from ..engine import router
from ..engine.orders.routed import FEE_REFERENCE, FEE_REFERENCE_TRIES
from ..trading.types import Side
from ..ws.base import now_ms

log = logging.getLogger("synpath.server")

ZERO = Decimal("0")

FEE_SCHEDULE_TTL_S = 600.0
"""How long a member's fee schedule is trusted before it is read again."""

FEE_SCHEDULE_RETRY_S = 60.0
"""How long a member whose schedule could not be read counts as free before
it is asked again."""

BookSource = str
"""Where a member's book came from: `stream` (serve's live venue stream),
`rest` (read from the venue for this request) or `missing`."""


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


# ---------------------------------------------------------------------------
# Fees
# ---------------------------------------------------------------------------

@dataclass
class _Schedule:
    schedule: Any | None
    """The member's `FeeSchedule`, or `None` where it could not be read."""
    read_at: float


class BucketFees:
    """Each member's taker fee per contract, from its published fee schedule,
    and its per-order floor. `prepare` reads what is missing (blocking venue
    reads, run in a thread); everything after that is a cache lookup, so a
    book can be priced on every update without a venue call."""

    def __init__(self, registry: Any, *, ttl_s: float = FEE_SCHEDULE_TTL_S, retry_s: float = FEE_SCHEDULE_RETRY_S,
                 clock: Callable[[], float] = time.monotonic):
        self.registry = registry
        self.ttl_s, self.retry_s, self.clock = ttl_s, retry_s, clock
        self.schedules: dict[str, _Schedule] = {}
        self.per_contract_cache: dict[tuple[str, Decimal], Decimal | None] = {}

    async def prepare(self, bucket: Bucket) -> None:
        """Read every member's schedule that is missing or stale."""
        for member in bucket.members:
            if not self._fresh(member.market_id):
                self._store(member.market_id, await asyncio.to_thread(self._read_schedule, member.market_id))

    def _fresh(self, market_id: str) -> bool:
        held = self.schedules.get(market_id)
        if held is None:
            return False
        lifetime = self.ttl_s if held.schedule is not None else self.retry_s
        return self.clock() - held.read_at < lifetime

    def _read_schedule(self, market_id: str) -> Any | None:
        try:
            return self.registry.get(ids.venue_of(market_id)).fetch_fee_schedule(market_id)
        except Exception as exc:   # a fee the venue will not publish is not a reason to refuse the book
            log.info("synpath.server: no fee schedule for %s: %s", market_id, exc)
            return None

    def _store(self, market_id: str, schedule: Any | None) -> None:
        self.schedules[market_id] = _Schedule(schedule, self.clock())
        self.per_contract_cache = {key: value for key, value in self.per_contract_cache.items()
                                   if key[0] != market_id}

    def per_contract(self, market_id: str, member_yes_price: Decimal) -> Decimal | None:
        """The taker fee for one contract at the member's own YES price, or
        `None` where the library cannot price it. Quoted for `FEE_REFERENCE`
        contracts and divided back; while the venue's floor is all a quote
        shows, a larger reference is asked for, as the routed order does."""
        key = (market_id, member_yes_price)
        if key in self.per_contract_cache:
            return self.per_contract_cache[key]
        held = self.schedules.get(market_id)
        schedule = held.schedule if held is not None else None
        fee: Decimal | None = None
        if schedule is not None:
            reference = FEE_REFERENCE
            floor = getattr(schedule, "min_fee", None)
            for _ in range(FEE_REFERENCE_TRIES):
                estimate = schedule.estimate(float(member_yes_price), float(reference), taker=True)
                if estimate is None:
                    break
                fee = Decimal(str(estimate)) / reference
                if floor is None or Decimal(str(estimate)) > Decimal(str(floor)):
                    break
                reference *= 100
        self.per_contract_cache[key] = fee
        return fee

    def fee_function(self, bucket: Bucket, side: Side) -> router.FeeFn:
        """`fee(market_id, bucket_price, contracts)` for the router, for
        orders on `side` of the bucket: each level is priced at the member's
        own YES price, after `flip`."""
        def fee(market_id: str, bucket_price: Decimal, contracts: Decimal) -> Decimal:
            _, member_price = bucket.member(market_id).to_member(side, bucket_price)
            return (self.per_contract(market_id, member_price) or ZERO) * contracts
        return fee

    def floors(self, bucket: Bucket) -> dict[str, Decimal]:
        """Per member, the least one taker order pays, where the venue has a floor."""
        out = {}
        for member in bucket.members:
            held = self.schedules.get(member.market_id)
            floor = getattr(held.schedule, "min_fee", None) if held is not None and held.schedule is not None else None
            if floor is not None:
                out[member.market_id] = Decimal(str(floor))
        return out

    def unknown(self, bucket: Bucket) -> list[str]:
        """Members whose fee the library cannot price: no schedule, or a
        formula it does not know."""
        out = []
        for member in bucket.members:
            held = self.schedules.get(member.market_id)
            if held is None or held.schedule is None or held.schedule.estimate(0.5, 1.0) is None:
                out.append(member.market_id)
        return out


# ---------------------------------------------------------------------------
# Books
# ---------------------------------------------------------------------------

def usable(book: Any) -> bool:
    """A member book the router may read: snapshotted, still trusted, and
    not crossed. The rule an engine-held order applies to its own books."""
    if book is None or getattr(book, "ready", True) is False:
        return False
    # A stream's `LocalBook` gives prices; a read `OrderBook` gives levels.
    best_bid = getattr(book.best_bid, "price", book.best_bid)
    best_ask = getattr(book.best_ask, "price", book.best_ask)
    return best_bid is None or best_ask is None or best_bid <= best_ask


@dataclass
class MemberState:
    """What a bucket subscription knows about one member beyond its book."""

    market_id: str
    error: str | None = None
    """Why serve cannot stream this member (`credentials_required`, …), if it cannot."""


def merged_levels(bucket: Bucket, views: dict[str, router.BookView], fees: BucketFees,
                  ) -> tuple[list[router.Level], list[router.Level]]:
    """`(asks, bids)` in bucket terms, each net of its venue's taker fee for
    the side it would be taken on: asks are bought, bids are sold."""
    asks, _ = router.merge(bucket, views, fees.fee_function(bucket, Side.BUY))
    _, bids = router.merge(bucket, views, fees.fee_function(bucket, Side.SELL))
    return asks, bids


def level_rows(levels: list[router.Level]) -> list[dict[str, Any]]:
    return [{"price": str(level.price), "net_price": str(level.net_price), "size": str(level.size),
             "market_id": level.market_id, "venue": ids.venue_of(level.market_id)} for level in levels]


def bucket_book_message(bucket: Bucket, books: dict[str, Any], fees: BucketFees, *, depth: int | None,
                        member_states: dict[str, MemberState] | None = None) -> dict[str, Any]:
    """The `bucket_book` frame: the merged book as it stands, ready only while
    every enabled member's book is usable. A bucket missing a member is not
    the bucket, so it is sent with no levels rather than as a partial book.
    A member switched off (`enabled=False`) is listed, with `enabled: false`
    and no book, and neither adds levels nor holds `ready` back."""
    member_states = member_states or {}
    members = []
    views: dict[str, router.BookView] = {}
    for member in bucket.members:
        if not member.enabled:
            members.append({
                "market_id": member.market_id, "venue": ids.venue_of(member.market_id), "flip": member.flip,
                "enabled": False, "ready": False, "sequence": None, "timestamp": None, "error": None,
            })
            continue
        book = books.get(member.market_id)
        ready = usable(book)
        if ready:
            seen = router.view(book, member)
            views[member.market_id] = seen if depth is None else router.BookView(
                seen.market_id, seen.asks[:depth], seen.bids[:depth])
        state = member_states.get(member.market_id)
        members.append({
            "market_id": member.market_id, "venue": ids.venue_of(member.market_id), "flip": member.flip,
            "enabled": True, "ready": ready, "sequence": getattr(book, "sequence", None) if book is not None else None,
            "timestamp": getattr(book, "timestamp", None) if book is not None else None,
            "error": state.error if state is not None else None,
        })
    enabled = bucket.enabled_members()
    routable = bucket.model_copy(update={"members": enabled})
    ready = bool(enabled) and len(views) == len(enabled)
    asks, bids = merged_levels(routable, views, fees) if ready else ([], [])
    timestamps = [m["timestamp"] for m in members if m["timestamp"] is not None]
    return {
        "type": "bucket_book", "market_id": bucket.market_id, "bucket_id": bucket.id, "name": bucket.name,
        "side": "yes", "ready": ready,
        "asks": level_rows(asks), "bids": level_rows(bids),
        "best_ask": _text(min((l.price for l in asks), default=None)),
        "best_bid": _text(max((l.price for l in bids), default=None)),
        "best_ask_net": _text(asks[0].net_price) if asks else None,
        "best_bid_net": _text(bids[0].net_price) if bids else None,
        "members": members,
        "fees_unknown": fees.unknown(routable),
        "fee_floors": {market_id: str(floor) for market_id, floor in fees.floors(routable).items()},
        "timestamp": max(timestamps) if timestamps else None,
        "sent_at": now_ms(),
    }


def bucket_trade_message(bucket: Bucket, member: BucketMember, trade: dict[str, Any]) -> dict[str, Any]:
    """A member's public trade in bucket terms: a flipped member's YES trade
    at p is a bucket trade at 1 - p, its taker on the other side."""
    side = trade.get("taker_side")
    price = Decimal(trade["price"])
    if side in ("buy", "sell"):
        bucket_side, bucket_price = member.to_bucket(Side(side), price)
        side = bucket_side.value
    else:
        _, bucket_price = member.to_bucket(Side.BUY, price)
    return {**trade, "market_id": bucket.market_id, "member_market_id": member.market_id, "flip": member.flip,
            "price": str(bucket_price), "taker_side": side}


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------

@dataclass
class PlanResult:
    plan: router.Plan
    sources: dict[str, BookSource]
    fees_unknown: list[str]
    computed_at: int = field(default_factory=now_ms)


class BucketBooks:
    """The bucket side of serve: loads definitions from the engine's journal,
    reads members' books from the `/ws/market` hub (or the venue, where no
    stream holds one), and prices them with `BucketFees`."""

    def __init__(self, engine: Any, registry: Any, hub: Any | None = None, *, fees: BucketFees | None = None):
        self.engine = engine
        self.registry = registry
        self.hub = hub
        self.fees = fees or BucketFees(registry)

    async def load(self, bucket_id: str) -> Bucket | None:
        row = await self.engine.journal.bucket(bucket_id)
        return Bucket.model_validate(row) if row is not None else None

    def visible_to(self, principal: Any, bucket: Bucket) -> bool:
        """Whether a caller may stream this bucket: the rule every bucket route uses."""
        from .trading import sees_bucket

        return sees_bucket(principal, bucket, self.engine)

    def live_book(self, market_id: str) -> Any | None:
        """The hub's maintained book for a member, if a stream holds a usable one."""
        if self.hub is None:
            return None
        book = self.hub.member_book(market_id)
        return book if usable(book) else None

    async def current_books(self, bucket: Bucket) -> tuple[dict[str, Any], dict[str, BookSource]]:
        """Every member's book now: the live one where serve streams it, else
        one read from the venue for this request."""
        books: dict[str, Any] = {}
        sources: dict[str, BookSource] = {}
        for member in bucket.members:
            book = self.live_book(member.market_id)
            source = "stream"
            if book is None:
                book = await asyncio.to_thread(self._read_book, member.market_id)
                source = "rest" if book is not None else "missing"
            sources[member.market_id] = source
            if book is not None:
                books[member.market_id] = book
        return books, sources

    def _read_book(self, market_id: str) -> Any | None:
        try:
            return self.registry.get(ids.venue_of(market_id)).fetch_order_book(market_id, side="yes")
        except Exception as exc:
            log.info("synpath.server: no book for %s: %s", market_id, exc)
            return None

    async def plan(self, bucket: Bucket, *, side: Side, quantity: Decimal, worst_price: Decimal) -> PlanResult:
        """`router.plan` on the members' current books, as an order on the
        bucket would make it now, on its enabled members only. Nothing is placed."""
        bucket = bucket.routable()
        await self.fees.prepare(bucket)
        books, sources = await self.current_books(bucket)
        precision = {member.market_id: router.default_precision(member.market_id) for member in bucket.members}
        plan = router.plan(bucket, books, precision, self.fees.fee_function(bucket, side), side=side,
                           quantity=quantity, limit=worst_price, floors=self.fees.floors(bucket))
        return PlanResult(plan=plan, sources=sources, fees_unknown=self.fees.unknown(bucket))


__all__ = ["BucketBooks", "BucketFees", "MemberState", "PlanResult", "bucket_book_message", "bucket_trade_message",
           "merged_levels", "usable"]
