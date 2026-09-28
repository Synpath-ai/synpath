"""A venue's request budget, read and write, with a fast lane and a hard edge.

The read API's `RateLimiter` is one bucket of requests per second. Trading
needs three things it does not have:

  * **Two buckets.** Kalshi meters reads and writes separately, in tokens
    per second by tier (Basic: 200 read / 100 write; an order costs 10, a
    batch cancel 2 per item). A price loop must not starve an order.
  * **A fast lane.** A cancel or a kill switch may not queue behind new
    orders. A high-priority call draws from a small borrow bucket and goes
    at once; the normal callers already waiting are pushed back by exactly
    what it took, so the venue still sees one budget.
  * **A hard edge.** A stop that fires late is worse than one that fails
    loudly. A call whose wait would exceed its deadline raises
    `RateBudgetExceeded` before anything is sent, instead of being delayed
    and looking like it ran on time.

`configure` replaces the rates from the venue's own report (`GET
/account/limits` on Kalshi), so a strategy built on a Premier account does
not silently throttle on Basic.

How the ordering works without a queue: a normal call *reserves* its tokens
immediately, driving the bucket negative, and sleeps for exactly the refill
its own draw needs. Callers therefore go in reservation order. A fast-lane
draw deducts from the same bucket and adds its cost to a running `shift`;
each sleeping normal caller, on waking, sleeps again for whatever shift
accrued since it reserved. Nothing is ever dispatched out of order and the
sum of what goes out never exceeds the refill.
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Literal

from .errors import RateBudgetExceeded

Kind = Literal["read", "write"]


class Priority(IntEnum):
    HIGH = 0
    """Cancels, kill switch, the replace half of an edit."""
    NORMAL = 1


@dataclass
class _Bucket:
    rate: float
    capacity: float
    tokens: float
    updated: float

    def refill(self, now: float) -> None:
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now


@dataclass
class _Lane:
    main: _Bucket
    """The venue's budget, as reserved by every caller."""
    fast: _Bucket
    """What a high-priority call may borrow ahead of the queue."""
    shift: float = 0.0
    """Seconds of delay injected ahead of sleeping normal callers."""


@dataclass(frozen=True)
class Reservation:
    kind: str
    cost: float
    wait: float
    shift_then: float


class BudgetLimiter:
    def __init__(
        self,
        *,
        read_per_second: float,
        write_per_second: float,
        burst_seconds: float = 2.0,
        borrow_seconds: float = 1.0,
        max_wait_s: float = 5.0,
    ):
        self.burst_seconds = burst_seconds
        self.borrow_seconds = borrow_seconds
        self.max_wait_s = max_wait_s
        self._lock = threading.Lock()
        now = time.monotonic()
        self._lanes: dict[str, _Lane] = {
            "read": self._lane(read_per_second, now),
            "write": self._lane(write_per_second, now),
        }

    def _lane(self, rate: float, now: float) -> _Lane:
        return _Lane(
            main=_Bucket(rate, rate * self.burst_seconds, rate * self.burst_seconds, now),
            fast=_Bucket(rate, rate * self.borrow_seconds, rate * self.borrow_seconds, now),
        )

    def configure(self, *, read_per_second: float | None = None, write_per_second: float | None = None) -> None:
        """Adopt the venue's reported budget. Unspent tokens are kept, capped
        at the new capacity."""
        with self._lock:
            now = time.monotonic()
            for kind, rate in (("read", read_per_second), ("write", write_per_second)):
                if rate is None:
                    continue
                lane = self._lanes[kind]
                for bucket, seconds in ((lane.main, self.burst_seconds), (lane.fast, self.borrow_seconds)):
                    bucket.refill(now)
                    bucket.rate = rate
                    bucket.capacity = rate * seconds
                    bucket.tokens = min(bucket.tokens, bucket.capacity)

    def reserve(self, cost: float, kind: Kind, priority: Priority = Priority.NORMAL) -> Reservation:
        """Take `cost` tokens now and say how long to wait before using them."""
        with self._lock:
            lane = self._lanes[kind]
            now = time.monotonic()
            lane.main.refill(now)
            lane.fast.refill(now)
            if priority == Priority.HIGH and lane.fast.tokens >= cost:
                lane.fast.tokens -= cost
                lane.main.tokens -= cost
                lane.shift += cost / lane.main.rate
                return Reservation(kind, cost, 0.0, lane.shift)
            lane.main.tokens -= cost
            deficit = -lane.main.tokens
            wait = deficit / lane.main.rate if deficit > 0 else 0.0
            return Reservation(kind, cost, wait, lane.shift)

    def release(self, reservation: Reservation) -> None:
        """Give back a reservation that will not be used."""
        with self._lock:
            bucket = self._lanes[reservation.kind].main
            bucket.tokens = min(bucket.capacity, bucket.tokens + reservation.cost)

    def shift_since(self, reservation: Reservation) -> float:
        with self._lock:
            return self._lanes[reservation.kind].shift - reservation.shift_then

    async def acquire(
        self, cost: float = 10.0, kind: Kind = "write", priority: Priority = Priority.NORMAL,
        max_wait_s: float | None = None,
    ) -> None:
        """Wait for budget, or refuse if the wait would exceed the deadline."""
        deadline = self.max_wait_s if max_wait_s is None else max_wait_s
        reservation = self.reserve(cost, kind, priority)
        if reservation.wait > deadline:
            self.release(reservation)
            raise RateBudgetExceeded(
                f"{kind} budget: {reservation.wait:.2f}s of queue ahead, more than the "
                f"{deadline:.2f}s this call allows; nothing was sent",
                wait_s=reservation.wait,
            )
        wait = reservation.wait
        while wait > 0:
            await asyncio.sleep(wait)
            # A fast-lane draw since the reservation pushed the queue back by
            # its cost; sleep that much more, then check again.
            wait = self.shift_since(reservation)
            reservation = Reservation(kind, cost, 0.0, reservation.shift_then + wait)

    def snapshot(self) -> dict[str, dict[str, float]]:
        with self._lock:
            now = time.monotonic()
            out = {}
            for kind, lane in self._lanes.items():
                lane.main.refill(now)
                out[kind] = {"rate": lane.main.rate, "capacity": lane.main.capacity, "tokens": lane.main.tokens}
            return out
