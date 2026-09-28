"""Alerts: the few events a person needs to see now.

The event stream carries everything; almost none of it is worth waking
somebody for. This module is the filter, and it is deliberately small: a
handful of conditions, each with a severity, each stated as a sentence an
operator can act on without opening the journal.

What is worth an alert, and why:

* **the lease was taken** — another engine is trading this account, or this
  one lost the right to; either way orders may be duplicated;
* **an intent stayed in doubt** — an order may exist at a venue that this
  engine does not manage;
* **reconciliation found a difference** — an orphan, a ghost, or a position
  the venue and the ledger disagree about;
* **the kill switch engaged**, and by whom;
* **the daily loss is approaching its limit** — at the warning fraction,
  while there is still time to decide;
* **a stream went quiet or reconnected with reconciliation required**;
* **a venue rejected orders repeatedly** — the count in a window, not each
  one, because a rejection storm is one problem, not fifty.

Delivery is a callback. A file, a log line, a webhook, a chat message: the
engine does not care, and none of them are built in, because an alerting
path that cannot be tested is worse than none.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Awaitable, Callable, Literal

from .events import EngineEvent, EventBus

log = logging.getLogger("synpath.engine.alerts")

Severity = Literal["info", "warning", "critical"]
Sink = Callable[["Alert"], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class Alert:
    severity: Severity
    kind: str
    message: str
    detail: dict[str, Any] = field(default_factory=dict)
    ts: int = field(default_factory=lambda: int(time.time() * 1000))

    def __str__(self) -> str:
        return f"[{self.severity}] {self.message}"


@dataclass
class AlertRules:
    """When to speak up."""

    reject_burst: int = 10
    """Venue rejections within the window before one alert is raised."""
    reject_window_s: float = 60.0
    daily_loss_warning: Decimal = Decimal("0.8")
    """Fraction of the daily loss limit that warrants a warning."""
    repeat_after_s: float = 300.0
    """The same alert is not repeated inside this window."""
    quiet_kinds: tuple[str, ...] = ()
    """Alert kinds to suppress entirely."""


class Alerts:
    """Watches the event bus and calls the sinks when something matters."""

    def __init__(self, bus: EventBus, *, rules: AlertRules | None = None, sinks: list[Sink] | None = None):
        self.bus = bus
        self.rules = rules or AlertRules()
        self.sinks: list[Sink] = list(sinks or [])
        self.raised: list[Alert] = []
        self._last: dict[str, float] = {}
        self._rejects: deque[float] = deque(maxlen=512)
        bus.on(self._on_event)

    def sink(self, sink: Sink) -> Sink:
        self.sinks.append(sink)
        return sink

    # -- the rules ------------------------------------------------------------

    def _on_event(self, event: EngineEvent) -> None:
        rules = self.rules
        kind, payload = event.kind, event.payload
        if kind == "engine.lease_lost":
            self.raise_alert("critical", "lease", "another engine took the journal lease; this one stopped trading", payload)
        elif kind == "intent.unresolved":
            self.raise_alert("critical", "in_doubt",
                             f"an order sent to {payload.get('venue')} could not be resolved: "
                             f"{payload.get('reason', 'unknown')}; it may be live and unmanaged", payload)
        elif kind == "intent.swept":
            self.raise_alert("info", "swept", f"an unanswered order was swept: {payload.get('client_order_id')}", payload)
        elif kind == "engine.halted":
            self.raise_alert("critical", "halt", f"trading halted ({payload.get('policy')}): {payload.get('reason')}", payload)
        elif kind == "reconcile.orphan":
            self.raise_alert("warning", "orphan",
                             f"{payload.get('venue')} has a resting order this engine did not place "
                             f"({payload.get('key')} on {payload.get('market_id')})", payload)
        elif kind == "reconcile.position":
            self.raise_alert("warning", "position",
                             f"{payload.get('venue')} and the ledger disagree on {payload.get('key')}: "
                             f"engine {payload.get('engine')}, venue {payload.get('venue_contracts')}", payload)
        elif kind == "reconcile.balance":
            self.raise_alert("warning", "balance",
                             f"{payload.get('venue')} cash moved {payload.get('moved')} with "
                             f"{payload.get('unexplained')} no fill explains", payload)
        elif kind == "order.rejected":
            now = time.time()
            self._rejects.append(now)
            recent = [t for t in self._rejects if t > now - rules.reject_window_s]
            if len(recent) >= rules.reject_burst:
                self.raise_alert("warning", "rejects",
                                 f"{len(recent)} orders rejected in the last {int(rules.reject_window_s)}s", payload)
        elif kind == "stream.gap" or (kind == "stream.status" and payload.get("state") == "gap"):
            self.raise_alert("warning", "stream", f"a stream reported a gap: {payload.get('detail', '')}", payload)

    def check_daily_loss(self, realized_today: Decimal, limit: Decimal | None) -> Alert | None:
        """Called by the engine's day loop; warns before the limit stops trading."""
        if limit is None or limit == 0:
            return None
        loss = -realized_today
        if loss <= 0:
            return None
        fraction = loss / limit
        if fraction >= 1:
            return self.raise_alert("critical", "daily_loss", f"today's loss {loss} has reached the {limit} limit",
                                    {"loss": str(loss), "limit": str(limit)})
        if fraction >= self.rules.daily_loss_warning:
            return self.raise_alert("warning", "daily_loss",
                                    f"today's loss {loss} is {round(float(fraction) * 100)}% of the {limit} limit",
                                    {"loss": str(loss), "limit": str(limit)})
        return None

    # -- raising --------------------------------------------------------------

    def raise_alert(self, severity: Severity, kind: str, message: str, detail: dict[str, Any] | None = None) -> Alert | None:
        if kind in self.rules.quiet_kinds:
            return None
        now = time.time()
        last = self._last.get(f"{kind}:{severity}")
        if last is not None and now - last < self.rules.repeat_after_s:
            return None
        self._last[f"{kind}:{severity}"] = now
        alert = Alert(severity=severity, kind=kind, message=message, detail=detail or {})
        self.raised.append(alert)
        log.log(logging.CRITICAL if severity == "critical" else logging.WARNING if severity == "warning" else logging.INFO,
                "synpath.engine: %s", alert)
        for sink in list(self.sinks):
            try:
                result = sink(alert)
                if asyncio.iscoroutine(result):
                    asyncio.get_running_loop().create_task(_guard(result))
            except Exception:
                log.exception("synpath.engine: an alert sink failed")
        return alert


async def _guard(coro: Any) -> None:
    try:
        await coro
    except Exception:
        log.exception("synpath.engine: an async alert sink failed")
