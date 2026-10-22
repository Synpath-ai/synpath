"""Database-independent journal records and execution ownership errors."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


class LeaseLost(RuntimeError):
    """Another engine holds the journal, or this one's lease expired."""


class IntentState:
    PLANNED = "planned"
    """Written, risk passed, not yet sent."""
    SENDING = "sending"
    """Handed to the venue; the answer has not come back."""
    SENT = "sent"
    SETTLED = "settled"
    """The order reached a terminal status."""
    REJECTED = "rejected"
    FAILED = "failed"
    """The venue refused it, or the call failed in a way that proves nothing left."""
    LOST = "lost"
    """In doubt for longer than the sweep allows and not found at the venue."""


@dataclass(frozen=True, slots=True)
class JournalEvent:
    seq: int
    ts: int
    kind: str
    key: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class Intent:
    client_order_id: str
    state: str
    operation: str
    venue: str
    account_key: str
    market_id: str | None
    order_id: str | None
    target_order_id: str | None
    book: str | None
    trader: str | None
    attempts: int
    request: dict[str, Any]
    detail: str | None
    created_ts: int
    updated_ts: int

    @property
    def age_ms(self) -> int:
        return int(time.time() * 1000) - self.updated_ts


@dataclass(slots=True)
class Lease:
    name: str
    owner: str
    holder: str
    acquired_ts: int
    expires_ts: int
    ttl_ms: int = 30_000
    renew_task: Any = field(default=None, repr=False)
