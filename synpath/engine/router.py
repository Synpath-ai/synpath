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
  best venue**, rather than rounded up into more than the caller wanted.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Callable, Literal, Mapping

from ..bucket import Bucket, BucketMember
from ..trading.instruments import VENUE_RULES
from ..trading.types import Precision, Side
from ..types import OrderBook

ZERO = Decimal("0")
FeeFn = Callable[[str, Decimal, Decimal], Decimal]
"""`fee(market_id, price, contracts)` -> the taker fee for that many contracts
at that price, in the venue's currency. The plan asks per level."""

Reason = Literal["", "worst_price", "liquidity", "min_amount"]

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
                     amount_step=rules["amount_step"], whole_contracts=rules["whole"])


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
    """Volume-weighted net price of the levels this leg takes, in bucket terms."""


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


def plan(
    bucket: Bucket,
    books: Mapping[str, Any],
    precision: Mapping[str, Precision],
    fee: FeeFn,
    *,
    side: Side,
    amount: Decimal,
    limit: Decimal,
) -> Plan:
    """How to take `amount` of the bucket at no worse than `limit` net, given
    what the books show now. Walks the merged book best-net first, stops at
    the limit, then drops any leg under its venue's minimum and re-walks with
    that venue excluded so its size goes to the next best."""
    asks, bids = merge(bucket, books, fee)
    levels = asks if side == Side.BUY else bids
    excluded: set[str] = set()
    out = Plan(side=side, amount=amount, limit=limit)
    for _ in range(len(bucket.members) + 1):
        legs, remaining, reason = _walk(bucket, levels, precision, side, amount, limit, excluded)
        short = [leg for leg in legs if leg.amount < precision[leg.market_id].min_amount]
        if not short:
            out.legs, out.unfilled, out.reason = legs, remaining, reason
            if remaining > 0 and reason == "" :
                out.reason = "min_amount"
            return out
        excluded.update(leg.market_id for leg in short)
    out.legs, out.unfilled, out.reason = [], amount, "min_amount"
    return out


def _walk(
    bucket: Bucket,
    levels: list[Level],
    precision: Mapping[str, Precision],
    side: Side,
    amount: Decimal,
    limit: Decimal,
    excluded: set[str],
) -> tuple[list[Leg], Decimal, Reason]:
    taken: dict[str, dict[str, Decimal]] = {}
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
        slot = taken.setdefault(lvl.market_id, {"amount": ZERO, "cost": ZERO, "worst": lvl.price})
        slot["amount"] += take
        slot["cost"] += lvl.net_price * take
        slot["worst"] = max(slot["worst"], lvl.price) if side == Side.BUY else min(slot["worst"], lvl.price)
        remaining -= take
    else:
        if remaining <= 0:
            reason = ""
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
        legs.append(Leg(
            market_id=market_id, flip=member.flip, side=member_side, price=member_price, amount=qty,
            bucket_price=slot["worst"], net_price=slot["cost"] / slot["amount"],
        ))
    return legs, remaining, reason


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
