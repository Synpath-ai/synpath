"""Fair values: the number the engine marks a position at.

A mark is a decision, not a fact, so it is stored rather than computed on
the fly: per account and instrument, with where it came from and when. The
ledger marks unrealized profit with it, and later the synthetic order types
peg to it and the router compares venues with it.

Three sources, in the order the engine prefers them:

* one the caller set, because a desk's own model outranks anything here;
* the midpoint of a book the engine already has, which is free and honest
  while the book is tight;
* the last trade, when a side of the book is empty and a midpoint would be
  invented.

A mark that nothing has refreshed goes stale. `stale_after_s` decides when,
and a stale mark is returned as `None` rather than used quietly, so a
position shows its cost instead of a profit computed from yesterday's price.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

from .journal import Journal

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True, slots=True)
class FairValue:
    account_key: str
    market_id: str
    value: Decimal
    source: str
    ts: int

    def stale(self, *, after_s: float, now_ms: int | None = None) -> bool:
        stamp = now_ms if now_ms is not None else int(time.time() * 1000)
        return (stamp - self.ts) / 1000 > after_s


class FairValues:
    """Marks per account and instrument, kept in memory and in the journal."""

    def __init__(self, journal: Journal | None = None, *, stale_after_s: float = 300.0, face_value: Decimal = ONE):
        self.journal = journal
        self.stale_after_s = stale_after_s
        self.face_value = face_value
        self._values: dict[tuple[str, str], FairValue] = {}

    async def load(self) -> None:
        """Read what the journal has, so a restart does not start blind."""
        if self.journal is None:
            return
        stamp = int(time.time() * 1000)
        for (account_key, market_id), value in (await self.journal.fair_values()).items():
            self._values[(account_key, market_id)] = FairValue(account_key, market_id, value, "journal", stamp)

    async def set(self, account_key: str, market_id: str, value: Decimal, *, source: str = "manual", persist: bool = True) -> FairValue:
        mark = FairValue(account_key, market_id, Decimal(value), source, int(time.time() * 1000))
        self._values[(account_key, market_id)] = mark
        if persist and self.journal is not None:
            await self.journal.set_fair_value(account_key, market_id, mark.value, source=source)
        return mark

    def get(self, account_key: str, market_id: str) -> Decimal | None:
        """The mark, or `None` if there is none or it has gone stale."""
        mark = self._values.get((account_key, market_id))
        if mark is None or mark.stale(after_s=self.stale_after_s):
            return None
        return mark.value

    def entry(self, account_key: str, market_id: str) -> FairValue | None:
        return self._values.get((account_key, market_id))

    def marks(self) -> dict[tuple[str, str], Decimal]:
        """Every live mark, in the shape `Ledger.rollup` wants."""
        stamp = int(time.time() * 1000)
        return {
            key: mark.value for key, mark in self._values.items()
            if not mark.stale(after_s=self.stale_after_s, now_ms=stamp)
        }

    async def from_book(
        self,
        account_key: str,
        market_id: str,
        *,
        bid: Decimal | None = None,
        ask: Decimal | None = None,
        last: Decimal | None = None,
        persist: bool = False,
    ) -> FairValue | None:
        """Mark from a book: the midpoint where both sides exist, otherwise the
        last trade, otherwise the one side there is. `None` when there is
        nothing to mark with."""
        if bid is not None and ask is not None and ask >= bid:
            return await self.set(account_key, market_id, (bid + ask) / 2, source="mid", persist=persist)
        if last is not None:
            return await self.set(account_key, market_id, last, source="last", persist=persist)
        one_side = bid if bid is not None else ask
        if one_side is not None:
            return await self.set(account_key, market_id, one_side, source="touch", persist=persist)
        return None

    async def from_books(self, account_key: str, books: Iterable[tuple[str, Any]], *, persist: bool = False) -> int:
        """Mark a batch of `(market_id, LocalBook)` from the streaming layer."""
        marked = 0
        for market_id, book in books:
            bid, ask = getattr(book, "best_bid", None), getattr(book, "best_ask", None)
            if await self.from_book(account_key, market_id, bid=bid, ask=ask, persist=persist):
                marked += 1
        return marked
