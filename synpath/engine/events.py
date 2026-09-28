"""The engine's event bus: one stream, many readers, replayable.

Everything the engine does becomes an event: an order accepted, a fill
booked, a risk rule refusing, a halt, a reconciliation finding an order
nobody asked for. Strategies, the server and the operator all read the same
stream, so what a user sees in a dashboard is what the journal recorded.

Two properties matter more than speed:

**A slow reader does not stall the engine.** Each subscriber gets its own
bounded queue. A reader that stops draining loses its oldest events and is
told how many, rather than blocking order entry behind it.

**The stream survives a restart.** Every event carries the journal sequence
number it was written under, so a reader that saved its cursor asks for
`replay(since)` and continues without a gap.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Iterable

from .journal import Journal

log = logging.getLogger("synpath.engine")


@dataclass(frozen=True, slots=True)
class EngineEvent:
    """One thing that happened. `kind` is a dotted name (`order.accepted`,
    `risk.rejected`, `reconcile.orphan`), `key` the thing it happened to."""

    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
    key: str | None = None
    ts: int = field(default_factory=lambda: int(time.time() * 1000))
    seq: int | None = None
    """The journal sequence number, once it has one."""

    def __str__(self) -> str:
        return f"{self.kind}({self.key})" if self.key else self.kind


class Subscription:
    """One reader's view of the stream."""

    def __init__(self, bus: "EventBus", *, kinds: tuple[str, ...] | None = None, maxsize: int = 10_000):
        self.bus = bus
        self.kinds = kinds
        self.queue: asyncio.Queue[EngineEvent] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def wants(self, event: EngineEvent) -> bool:
        return not self.kinds or any(event.kind == k or event.kind.startswith(k + ".") for k in self.kinds)

    def offer(self, event: EngineEvent) -> None:
        if not self.wants(event):
            return
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop the oldest: a stalled dashboard must not hold up trading.
            try:
                self.queue.get_nowait()
                self.dropped += 1
                self.queue.put_nowait(event)
            except asyncio.QueueEmpty:  # pragma: no cover - raced with the reader
                pass

    async def __aiter__(self) -> AsyncIterator[EngineEvent]:
        while True:
            yield await self.queue.get()

    def close(self) -> None:
        self.bus.unsubscribe(self)


class EventBus:
    """Publish once, persist once, fan out to every reader."""

    def __init__(self, journal: Journal | None = None, *, persist: bool = True):
        self.journal = journal
        self.persist = persist and journal is not None
        self.subscribers: list[Subscription] = []
        self.hooks: list[Callable[[EngineEvent], Any]] = []
        self.published = 0

    def subscribe(self, *kinds: str, maxsize: int = 10_000) -> Subscription:
        """A reader of the whole stream, or of `kinds` and their children
        (`subscribe("order")` also gets `order.accepted`)."""
        sub = Subscription(self, kinds=kinds or None, maxsize=maxsize)
        self.subscribers.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self.subscribers:
            self.subscribers.remove(sub)

    def on(self, hook: Callable[[EngineEvent], Any]) -> Callable[[EngineEvent], Any]:
        """Call `hook` for every event. A hook that raises is logged, not fatal."""
        self.hooks.append(hook)
        return hook

    async def publish(self, kind: str, payload: dict[str, Any] | None = None, *, key: str | None = None, persist: bool | None = None) -> EngineEvent:
        event = EngineEvent(kind=kind, payload=payload or {}, key=key)
        if (self.persist if persist is None else persist) and self.journal is not None:
            seq = await self.journal.append(kind, payload or {}, key=key)
            event = EngineEvent(kind=kind, payload=event.payload, key=key, ts=event.ts, seq=seq)
        self.emit(event)
        return event

    def emit(self, event: EngineEvent) -> None:
        """Fan out an event that is already written (or deliberately not)."""
        self.published += 1
        for sub in list(self.subscribers):
            sub.offer(event)
        for hook in list(self.hooks):
            try:
                result = hook(event)
                if asyncio.iscoroutine(result):
                    asyncio.get_running_loop().create_task(_guard(result))
            except Exception:
                log.exception("synpath.engine: event hook failed on %s", event.kind)

    async def replay(self, since: int = 0, *, kinds: Iterable[str] | None = None) -> AsyncIterator[EngineEvent]:
        """Events already in the journal, oldest first, for a reader catching up."""
        if self.journal is None:
            return
        async for row in self.journal.replay(since, kinds=kinds):
            yield EngineEvent(kind=row.kind, payload=row.payload, key=row.key, ts=row.ts, seq=row.seq)


async def _guard(coro: Any) -> None:
    try:
        await coro
    except Exception:
        log.exception("synpath.engine: async event hook failed")
