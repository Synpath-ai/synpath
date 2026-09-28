"""End of day: close the books, say what happened, start the next one.

Prediction markets trade almost around the clock, so "end of day" is a
choice rather than a bell. It is whatever hour the operator names, and what
it does is bookkeeping, not trading:

1. read the settlements each venue reports and book them, so a resolved
   market stops counting as an open position;
2. write the day's report -- realized profit, fees, volume, fills, and the
   open positions carried into tomorrow, per strategy and per account;
3. roll the risk day, which is what makes the daily loss limit mean
   "today" rather than "since the engine started";
4. journal the report so the next morning's question, "what did we make
   yesterday", is answered by reading rather than recomputing.

Nothing here cancels or trades. A desk that wants orders pulled at the close
says so with the kill switch's `cancel` policy, which is a decision, not a
side effect of a report.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from ..trading.types import Settlement
from .engine import Engine

ZERO = Decimal("0")


@dataclass(slots=True)
class DailyReport:
    """One day, closed."""

    date: str
    started_ms: int
    ended_ms: int
    realized: Decimal = ZERO
    fees: Decimal = ZERO
    volume: Decimal = ZERO
    fills: int = 0
    settlements: int = 0
    settled_realized: Decimal = ZERO
    by_book: dict[str, dict[str, str]] = field(default_factory=dict)
    by_account: dict[str, dict[str, str]] = field(default_factory=dict)
    open_positions: list[dict[str, Any]] = field(default_factory=list)
    unrealized: Decimal | None = None
    marked: int = 0
    unmarked: int = 0

    def summary(self) -> dict[str, Any]:
        return {
            "date": self.date, "realized": str(self.realized), "fees": str(self.fees), "volume": str(self.volume),
            "fills": self.fills, "settlements": self.settlements, "settled_realized": str(self.settled_realized),
            "unrealized": str(self.unrealized) if self.unrealized is not None else None,
            "open_positions": len(self.open_positions), "marked": self.marked, "unmarked": self.unmarked,
            "by_book": self.by_book, "by_account": self.by_account,
        }


class EndOfDay:
    """Runs the close, on demand or on a timer."""

    def __init__(self, engine: Engine, *, hour_utc: int = 0, minute_utc: int = 5):
        self.engine = engine
        self.hour_utc = hour_utc
        self.minute_utc = minute_utc

    async def run(self, *, date: str | None = None, book_settlements: bool = True) -> DailyReport:
        engine = self.engine
        started = int(engine.clock() * 1000)
        day = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        report = DailyReport(date=day, started_ms=started, ended_ms=started)

        if book_settlements:
            report.settlements, report.settled_realized = await self._settle()

        since = engine.risk.day_start_ms
        for fill in await engine.journal.fills(since_ts=since):
            report.fills += 1
            report.volume += fill.amount
            report.fees += fill.fee or ZERO

        marks = engine.fair_values.marks()
        for level, target in (("book", report.by_book), ("account", report.by_account)):
            for key, row in engine.ledger.rollup(level, marks).items():  # type: ignore[arg-type]
                target[key] = {
                    "realized": str(row.realized), "unrealized": str(row.unrealized) if row.unrealized is not None else "",
                    "fees": str(row.fees), "volume": str(row.volume), "open_positions": str(row.positions),
                }
        total = engine.ledger.total(marks)
        report.realized = total.realized
        report.unrealized = total.unrealized
        for state in engine.ledger.open_positions():
            mark = marks.get((state.account_key, state.market_id))
            report.marked += 1 if mark is not None else 0
            report.unmarked += 0 if mark is not None else 1
            report.open_positions.append({
                "account": state.account_key, "book": state.book, "market_id": state.market_id,
                "contracts": str(state.contracts), "average_cost": str(state.average_cost),
                "mark": str(mark) if mark is not None else None,
            })

        report.ended_ms = int(engine.clock() * 1000)
        await engine.bus.publish("eod.report", report.summary(), key=day)
        engine.risk.roll_day(realized_now=total.realized)
        await engine.journal.set_cursor("eod:last", day)
        return report

    async def _settle(self) -> tuple[int, Decimal]:
        """Book every settlement the venues report since the last close."""
        booked, realized = 0, ZERO
        for venue, adapter in self.engine.adapters.items():
            if not adapter.has.get("fetch_settlements"):
                continue
            since = int(await self.engine.journal.cursor(f"settlements:{venue}", 0) or 0)
            try:
                settlements = await adapter.fetch_settlements(since=since or None, limit=500)
            except Exception:
                continue
            newest = since
            for settlement in settlements:
                newest = max(newest, settlement.timestamp or 0)
                realized += self.engine.ledger.apply_settlement(settlement)
                await self.engine.bus.publish("settlement.booked", settlement.model_dump(mode="json"),
                                              key=f"{venue}:{settlement.market_id}")
                booked += 1
            if newest > since:
                await self.engine.journal.set_cursor(f"settlements:{venue}", newest)
        return booked, realized

    def seconds_until_next(self, *, now: float | None = None) -> float:
        """How long until the next close, for a caller scheduling it."""
        stamp = datetime.fromtimestamp(now if now is not None else self.engine.clock(), tz=timezone.utc)
        target = stamp.replace(hour=self.hour_utc, minute=self.minute_utc, second=0, microsecond=0)
        if target <= stamp:
            target += timedelta(days=1)
        return (target - stamp).total_seconds()

    async def loop(self) -> None:
        """Run the close every day at the configured hour."""
        import asyncio

        while self.engine.running:
            await asyncio.sleep(self.seconds_until_next())
            try:
                await self.run()
            except Exception:
                import logging

                logging.getLogger("synpath.engine").exception("synpath.engine: the daily close failed")
