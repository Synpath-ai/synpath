"""Buckets: one tradable thing made of several venues' listings.

A bucket sits where a single market does. You place an order on it the way
you place one on `kalshi:KXFOO-25`; the engine's router turns that into legs
on the member markets. It is the user's own definition, a list of member
markets and, for each, whether that member's YES is the bucket's YES or the
opposite (`flip`). Nothing here judges whether the members really are the
same question; that is the caller's call, or a matching service's, later.

Ids are UUIDs made locally, so a bucket created offline stays unique when
its definition later moves to a hosted account. Ownership is the `book`, the
same cross-venue strategy label orders and P&L already roll up by.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Literal

from pydantic import Field

from . import ids
from .errors import BadRequest
from .trading.types import Side
from .types import BookSide, _Base

ONE = Decimal("1")
BUCKET_PREFIX = "bucket:"


class BucketMember(_Base):
    market_id: str
    """A Synpath id, `venue:native`."""
    flip: bool = False
    """True when this member's YES is the opposite of the bucket's proposition."""

    def book_side(self) -> BookSide:
        """Which of the member's two books is the bucket's YES book. Reading
        the NO book of a flipped member gives prices already in bucket terms,
        so nothing downstream converts."""
        return "no" if self.flip else "yes"

    def to_member(self, side: Side, price: Decimal) -> tuple[Side, Decimal]:
        """A bucket-side order as the member venue must receive it. Prices in
        this library are always the YES price and `sell` is the NO side, so
        buying a flipped member's NO at p is `sell` at 1 - p."""
        if not self.flip:
            return side, price
        return (Side.SELL if side == Side.BUY else Side.BUY), ONE - price

    def to_bucket(self, side: Side, price: Decimal) -> tuple[Side, Decimal]:
        """A member fill back in bucket terms. The same map; it is its own inverse."""
        return self.to_member(side, price)


class Bucket(_Base):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    book: str
    name: str
    members: list[BucketMember]
    status: Literal["active", "archived"] = "active"
    created_ms: int | None = None
    updated_ms: int | None = None

    @property
    def market_id(self) -> str:
        """What an order names to target this bucket: `bucket:<id>`."""
        return BUCKET_PREFIX + self.id

    def market_ids(self) -> list[str]:
        return [m.market_id for m in self.members]

    def venues(self) -> set[str]:
        return {ids.venue_of(m.market_id) for m in self.members}

    def member(self, market_id: str) -> BucketMember:
        for m in self.members:
            if m.market_id == market_id:
                return m
        raise KeyError(market_id)

    def check(self) -> None:
        """The shape checks a definition must pass before it is stored or
        traded: well-formed ids on known venues, no duplicates, at least two
        members. Whether the members mean the same thing is not checked."""
        if len(self.members) < 2:
            raise BadRequest("a bucket needs at least two members")
        seen: set[str] = set()
        for m in self.members:
            ids.venue_of(m.market_id)   # raises BadRequest on a bare or unknown id
            if m.market_id in seen:
                raise BadRequest(f"{m.market_id} is listed twice")
            seen.add(m.market_id)


class BucketVenueFill(_Base):
    venue: str
    filled: Decimal
    average_price: Decimal | None = Field(description="In bucket terms")


class BucketOrderReport(_Base):
    """What an order on a bucket has done, in bucket terms, and where."""

    order_id: str
    bucket_id: str
    status: str = Field(description="As on the order: `open`, `closed`, `canceled`, ...")
    side: str
    amount: Decimal
    worst_price: Decimal = Field(description="The worst price the order accepts, in bucket terms")
    filled: Decimal
    unfilled: Decimal
    average_price: Decimal | None = Field(description="Weighted across every venue, in bucket terms")
    fees_paid: Decimal
    rounds: int = Field(description="Re-allocations that moved a leg")
    stop_reason: str | None = Field(description="`worst_price` (nothing left at or better than it), `liquidity`, `min_amount`, `max_rounds`, `max_age`, "
                                                "or null while working or once filled")
    per_venue: list[BucketVenueFill]
    detail: str | None = Field(default=None, description="Why the order was rejected or stopped, in words; "
                                                        "null while it works normally")


class BucketMemberPosition(_Base):
    market_id: str
    venue: str
    book: str
    account: str
    flip: bool
    contracts: Decimal = Field(description="On the member's own YES leg")
    bucket_contracts: Decimal = Field(description="The same, in bucket terms: negated when `flip`")
    entry_price: Decimal | None
    mark: Decimal | None


class BucketPosition(_Base):
    bucket_id: str
    name: str
    book: str
    contracts: Decimal = Field(description="Net across members, in bucket terms; negative is short")
    side: Literal["long", "short", "flat"]
    entry_price: Decimal | None
    realized: Decimal
    fees: Decimal
    members: list[BucketMemberPosition]


def is_bucket_id(market_id: str) -> bool:
    return market_id.startswith(BUCKET_PREFIX)


def bucket_id_of(market_id: str) -> str:
    if not is_bucket_id(market_id):
        raise BadRequest(f"{market_id!r} is not a bucket id")
    return market_id[len(BUCKET_PREFIX):]
