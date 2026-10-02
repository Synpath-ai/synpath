"""The router: one order on a bucket, as legs on its member venues.

Pure functions over data the caller already holds. `merge` lays the members'
books side by side in bucket terms; `plan` walks the merged book from the
best net price outwards and says how much to put where. Nothing here talks
to a venue or makes an order; the parent that owns the bucket order does
that with the plan, and re-plans as fills and books move.

Three rules the plan keeps, because they are the difference between a
router and a footgun:

* **The legs never sum to more than was asked.** Oversubscribing to fill
  faster is how a bucket buys twice.
* **Prices are net of the taker fee**, per level, because on Kalshi the fee
  is a curve in price and a 0.41 with fee can cost more than a 0.42 without.
* **A leg below its venue's minimum is dropped and its size goes to the next
  best venue**, rather than rounded up into more than the caller wanted. The
  minimum is in contracts, and where the venue has one (Opinion), in the
  value of the token actually bought.

A venue that charges a fee floor per order (Opinion: 0.25 USDT) makes a
small leg cost more per contract than its levels say. Each leg's fee is
raised to its floor after the walk; a leg that then breaks the limit is
dropped, and one whose size fills as fully for less elsewhere is moved.

With the Rust core installed, reading a Rust book into bucket terms, the
merge of the side being planned and the walk itself run there
(`synpath._core.router_levels`); sizing the legs, the floors and the
re-walks stay here, so every division rounds as before. A plan Rust cannot
carry exactly in Python's `Decimal` terms is made here from the start.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Callable, Literal, Mapping

from .. import _native
from ..bucket import Bucket, BucketMember
from ..trading.instruments import VENUE_RULES
from ..trading.types import Precision, Side
from ..types import OrderBook

ZERO = Decimal("0")
FeeFn = Callable[[str, Decimal, Decimal], Decimal]
"""`fee(market_id, price, contracts)` -> the taker fee for that many contracts
at that price, in the venue's currency. The plan asks per level."""

Reason = Literal["", "worst_price", "liquidity", "min_amount"]

FeeFloors = Mapping[str, Decimal]
"""Per market id, the least one taker order pays. Absent means no floor."""

PriceSize = tuple[Decimal, Decimal]


@dataclass(frozen=True)
class BookView:
    """One member's book in bucket terms: `asks` is what one bucket YES costs,
    `bids` what it fetches, both best first, as Decimals."""
    market_id: str
    asks: tuple[PriceSize, ...]
    bids: tuple[PriceSize, ...]


def view(book: Any, member: BucketMember, *, face_value: Decimal = Decimal("1")) -> BookView:
    """A member's book as the bucket sees it.

    Takes an `OrderBook` (either side) or anything with `levels()` giving
    `(bids, asks)` of price/size pairs (`synpath.ws.LocalBook`, the paper
    venue's book), which is always the YES side. A flipped member's YES book
    is turned into its NO book here: NO asks are `face - YES bids`, NO bids
    `face - YES asks`. An `OrderBook` already on the NO side is taken as is."""
    def pairs(rows: Any) -> tuple[PriceSize, ...]:
        out = []
        for row in rows:
            if isinstance(row, (tuple, list)):
                price, size = Decimal(str(row[0])), Decimal(str(row[1]))
            else:
                price, size = Decimal(str(row.price)), Decimal(str(row.size))
            if size > 0:
                out.append((price, size))
        return tuple(out)

    if _native.core is not None and face_value == 1:
        fast = _native.core.router_view(book, member.book_side() == "no")
        if fast is not None:
            return BookView(member.market_id, fast[0], fast[1])
    side_in = getattr(book, "side", "yes")
    if callable(getattr(book, "levels", None)):
        bids, asks = book.levels()
    else:
        bids, asks = book.bids, book.asks
    bids, asks = pairs(bids), pairs(asks)
    wanted = member.book_side()
    if side_in == wanted:
        return BookView(member.market_id, asks, bids)
    if side_in == "yes" and wanted == "no":
        return BookView(
            member.market_id,
            asks=tuple((face_value - p, s) for p, s in bids),
            bids=tuple((face_value - p, s) for p, s in asks),
        )
    raise ValueError(f"{member.market_id}: book is the {side_in} side, bucket needs {wanted}")


def default_precision(market_id: str) -> Precision:
    """The venue's published rules, for a member whose market has not been read."""
    venue = market_id.split(":", 1)[0]
    rules = VENUE_RULES.get(venue, VENUE_RULES["polymarket"])
    return Precision(tick=rules["default_tick"], min_amount=rules["min_amount"],
                     amount_step=rules["amount_step"], whole_contracts=rules["whole"],
                     min_notional=rules.get("min_notional"))


@dataclass(frozen=True)
class Level:
    market_id: str
    price: Decimal
    """In bucket terms: what one contract of the bucket's YES costs (asks) or fetches (bids)."""
    net_price: Decimal
    """`price` plus the taker fee per contract on a buy, minus it on a sell."""
    size: Decimal


@dataclass
class Leg:
    market_id: str
    flip: bool
    side: Side
    """The member's own side, after `flip`."""
    price: Decimal
    """The member's own YES price, after `flip` and tick rounding. What the venue receives."""
    amount: Decimal
    bucket_price: Decimal
    """The worst bucket-terms price this leg reaches, before fees."""
    net_price: Decimal
    """Volume-weighted net price of the levels this leg takes, in bucket terms,
    with the venue's fee floor included where it binds."""
    fee: Decimal = ZERO
    """The taker fee this leg is expected to pay, floor included."""
    fee_floor: Decimal = ZERO
    """How much of `fee` is the floor over what the levels' fees add up to."""

    def notional(self) -> Decimal:
        """What the token this leg buys costs: YES at `price` on a buy, NO at
        `1 - price` on a sell (a sell buys NO on a token venue)."""
        return self.amount * (self.price if self.side == Side.BUY else Decimal("1") - self.price)


@dataclass
class Plan:
    side: Side
    amount: Decimal
    limit: Decimal
    legs: list[Leg] = field(default_factory=list)
    unfilled: Decimal = ZERO
    reason: Reason = ""

    @property
    def allocated(self) -> Decimal:
        return sum((leg.amount for leg in self.legs), ZERO)

    @property
    def expected_net_price(self) -> Decimal | None:
        if not self.legs:
            return None
        return sum((leg.net_price * leg.amount for leg in self.legs), ZERO) / self.allocated


def merge(bucket: Bucket, books: Mapping[str, Any], fee: FeeFn) -> tuple[list[Level], list[Level]]:
    """The members' books as one, in bucket terms: `(asks, bids)`, asks
    cheapest-net first, bids richest-net first. Books go through `view`, so a
    flipped member may hand in either side."""
    asks: list[Level] = []
    bids: list[Level] = []
    for member in bucket.members:
        book = books.get(member.market_id)
        if book is None:
            continue
        seen = book if isinstance(book, BookView) else view(book, member)
        for price, size in seen.asks:
            asks.append(Level(member.market_id, price, price + _per_contract(fee, member.market_id, price), size))
        for price, size in seen.bids:
            bids.append(Level(member.market_id, price, price - _per_contract(fee, member.market_id, price), size))
    asks.sort(key=lambda l: (l.net_price, l.price))
    bids.sort(key=lambda l: (-l.net_price, -l.price))
    return asks, bids


def _per_contract(fee: FeeFn, market_id: str, price: Decimal) -> Decimal:
    charged = fee(market_id, price, Decimal("1"))
    return charged if charged > 0 else ZERO


class _Inexact(Exception):
    """The Rust walk met a number it could not carry exactly; plan in Python."""


Taken = dict[str, dict[str, Decimal]]
Walk = Callable[[set[str]], tuple[Taken, Decimal, Reason]]


def plan(
    bucket: Bucket,
    books: Mapping[str, Any],
    precision: Mapping[str, Precision],
    fee: FeeFn,
    *,
    side: Side,
    amount: Decimal,
    limit: Decimal,
    floors: FeeFloors | None = None,
) -> Plan:
    """How to take `amount` of the bucket at no worse than `limit` net, given
    what the books show now.

    Walks the merged book best-net first and stops at the limit. A leg under
    its venue's minimum (contracts, or value where the venue has one) is
    dropped and the walk repeated without that venue, so its size goes to
    the next best. Then the fee floors: a leg whose floor takes it past the
    limit is dropped the same way, and a floored leg is also dropped when the
    walk without it fills as much for less."""
    floors = floors or {}
    if _native.core is not None:
        walk = _native_walk(bucket, books, fee, side, amount, limit)
        if walk is not None:
            try:
                return _plan(bucket, precision, side, amount, limit, floors, walk)
            except _Inexact:
                pass
    asks, bids = merge(bucket, books, fee)
    levels = asks if side == Side.BUY else bids
    return _plan(bucket, precision, side, amount, limit, floors,
                 lambda excluded: _walk_levels(levels, side, amount, limit, excluded))


def _native_walk(
    bucket: Bucket, books: Mapping[str, Any], fee: FeeFn, side: Side, amount: Decimal, limit: Decimal,
) -> Walk | None:
    """The walk on the Rust core: the side being planned merged there once,
    then walked once per call. `None` to plan in Python."""
    rows = []
    for member in bucket.members:
        book = books.get(member.market_id)
        if book is None:
            continue
        seen = book if isinstance(book, BookView) else view(book, member)
        rows.append((member.market_id, seen.asks if side == Side.BUY else seen.bids))
    buy = side == Side.BUY
    levels = _native.core.router_levels(rows, fee, buy)
    if levels is None:
        return None

    def walk(excluded: set[str]) -> tuple[Taken, Decimal, Reason]:
        done = levels.walk(buy, amount, limit, sorted(excluded))
        if done is None:
            raise _Inexact
        slots, remaining, reason = done
        taken = {m: {"amount": a, "cost": c, "fee": f, "worst": w} for m, a, c, f, w in slots}
        return taken, remaining, reason

    return walk


def _plan(
    bucket: Bucket, precision: Mapping[str, Precision], side: Side, amount: Decimal, limit: Decimal,
    floors: FeeFloors, walk: Walk,
) -> Plan:
    def settle(excluded: set[str]) -> tuple[list[Leg], Decimal, Reason, set[str]]:
        excluded = set(excluded)
        for _ in range(len(bucket.members) + 1):
            taken, remaining, reason = walk(excluded)
            legs, remaining = _legs(bucket, taken, remaining, precision, side, floors)
            bad = [leg for leg in legs if _too_small(leg, precision[leg.market_id]) or _past_limit(leg, side, limit)]
            if not bad:
                return legs, remaining, reason, excluded
            excluded.update(leg.market_id for leg in bad)
        return [], amount, "min_amount", excluded

    best = settle(set())
    for _ in range(len(bucket.members)):
        moved = False
        for leg in sorted((l for l in best[0] if l.fee_floor > 0), key=lambda l: -l.fee_floor):
            other = settle(best[3] | {leg.market_id})
            if _better(other, best, side):
                best, moved = other, True
                break
        if not moved:
            break

    legs, remaining, reason, _ = best
    out = Plan(side=side, amount=amount, limit=limit, legs=legs, unfilled=remaining, reason=reason)
    if remaining > 0 and reason == "":
        out.reason = "min_amount"
    return out


def _too_small(leg: Leg, spec: Precision) -> bool:
    if leg.amount < spec.min_amount:
        return True
    return spec.min_notional is not None and leg.notional() < spec.min_notional


def _past_limit(leg: Leg, side: Side, limit: Decimal) -> bool:
    """Only a fee floor can take a leg past the limit: every level it took
    was inside it."""
    if leg.fee_floor <= 0:
        return False
    return leg.net_price > limit if side == Side.BUY else leg.net_price < limit


def _better(a: tuple, b: tuple, side: Side) -> bool:
    """Whether plan `a` fills at least as much as `b` for less (a buy) or for
    more (a sell), fees and floors included."""
    legs_a, remaining_a = a[0], a[1]
    legs_b, remaining_b = b[0], b[1]
    if remaining_a > remaining_b:
        return False
    if remaining_a < remaining_b:
        return True
    total_a = sum((l.net_price * l.amount for l in legs_a), ZERO)
    total_b = sum((l.net_price * l.amount for l in legs_b), ZERO)
    return total_a < total_b if side == Side.BUY else total_a > total_b


def _walk_levels(
    levels: list[Level], side: Side, amount: Decimal, limit: Decimal, excluded: set[str],
) -> tuple[Taken, Decimal, Reason]:
    """What each member's levels give, best first, up to `amount` and no
    worse than `limit` net: per member the contracts, their net cost, their
    fees and the worst price reached, in the order members were first taken."""
    taken: Taken = {}
    remaining = amount
    reason: Reason = "liquidity"
    for lvl in levels:
        if remaining <= 0:
            reason = ""
            break
        if lvl.market_id in excluded:
            continue
        if (side == Side.BUY and lvl.net_price > limit) or (side == Side.SELL and lvl.net_price < limit):
            reason = "worst_price"
            break
        take = min(lvl.size, remaining)
        slot = taken.setdefault(lvl.market_id, {"amount": ZERO, "cost": ZERO, "fee": ZERO, "worst": lvl.price})
        slot["amount"] += take
        slot["cost"] += lvl.net_price * take
        slot["fee"] += abs(lvl.net_price - lvl.price) * take
        slot["worst"] = max(slot["worst"], lvl.price) if side == Side.BUY else min(slot["worst"], lvl.price)
        remaining -= take
    else:
        if remaining <= 0:
            reason = ""
    return taken, remaining, reason


def _legs(
    bucket: Bucket, taken: Taken, remaining: Decimal, precision: Mapping[str, Precision], side: Side,
    floors: FeeFloors,
) -> tuple[list[Leg], Decimal]:
    """The walk's takings as legs: rounded down to each venue's step, priced
    on its tick toward the side that cannot worsen the limit, the fee floor
    added where it binds. What rounding gives back is unfilled again."""
    legs: list[Leg] = []
    for market_id, slot in taken.items():
        member = bucket.member(market_id)
        spec = precision[market_id]
        qty = _round_amount(slot["amount"], spec)
        if qty <= 0:
            remaining += slot["amount"]
            continue
        remaining += slot["amount"] - qty
        member_side, member_price = member.to_member(side, slot["worst"])
        member_price = _round_price(member_price, spec.tick, member_side)
        share = qty / slot["amount"]
        curve = slot["fee"] * share
        floor = floors.get(market_id)
        extra = max(ZERO, floor - curve) if floor is not None and curve > 0 else ZERO
        net = slot["cost"] / slot["amount"] + (extra / qty if side == Side.BUY else -extra / qty)
        legs.append(Leg(
            market_id=market_id, flip=member.flip, side=member_side, price=member_price, amount=qty,
            bucket_price=slot["worst"], net_price=net, fee=curve + extra, fee_floor=extra,
        ))
    return legs, remaining


def _round_amount(amount: Decimal, spec: Precision) -> Decimal:
    """Down, never up: a leg may fall short of what the level offered but must
    not exceed it."""
    if spec.whole_contracts:
        return Decimal(int(amount))
    if spec.amount_step:
        return (amount / spec.amount_step).to_integral_value(rounding=ROUND_FLOOR) * spec.amount_step
    return amount


def _round_price(price: Decimal, tick: Decimal, side: Side) -> Decimal:
    """Onto the venue's tick, toward the side that cannot worsen the limit:
    a buy rounds down, a sell rounds up."""
    rounding = ROUND_FLOOR if side == Side.BUY else ROUND_CEILING
    return (price / tick).to_integral_value(rounding=rounding) * tick


def member_from_plan(bucket: Bucket, leg: Leg) -> BucketMember:
    return bucket.member(leg.market_id)
