"""Day orders: an expiry the venue will honour after this process is gone.

None of the three venues has a Day order, because none of them has a trading
day: they run nearly around the clock. An engine that held Day orders itself
would cancel them when it stopped, which is the opposite of what a Day order
promises -- the one thing it must do is expire even if nobody is watching.

So Day is not an engine-held type at all. It is rewritten before the order
reaches an adapter: `time_in_force="day"` becomes `gtd` with `expires_at` at
the end of the configured session, in the configured timezone. The venue then
holds the expiry, and the journal keeps the original `day` alongside the
computed timestamp, so what the caller asked for is still visible.

The session is the operator's: `session_timezone` (the machine's zone by
default) and `session_end` (23:59:59). A desk in New York and one in London
mean different things by "today", and the engine should not guess.
"""
from __future__ import annotations

from datetime import datetime, time as clock_time, timedelta
from typing import Any

from ...trading.types import OrderRequest, TimeInForce

try:  # Python 3.9+ carries the zone database on most systems
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - a build without zoneinfo
    ZoneInfo = None  # type: ignore[assignment]


def parse_session_end(text: str) -> clock_time:
    parts = [int(p) for p in str(text).split(":")]
    while len(parts) < 3:
        parts.append(0)
    return clock_time(*parts[:3])


def zone(name: str | None):
    """The timezone to read `session_end` in; the machine's if none is named."""
    if not name:
        return datetime.now().astimezone().tzinfo
    if ZoneInfo is None:  # pragma: no cover - depends on the platform
        raise RuntimeError(f"this build has no zoneinfo, so the session timezone {name!r} cannot be used")
    return ZoneInfo(name)


def session_expiry(now_s: float, *, timezone_name: str | None = None, session_end: str = "23:59:59") -> int:
    """When the current session ends, in milliseconds since the epoch.

    A Day order placed after the session end belongs to the next session, not
    to a moment in the past.
    """
    tz = zone(timezone_name)
    end = parse_session_end(session_end)
    local = datetime.fromtimestamp(now_s, tz=tz)
    target = local.replace(hour=end.hour, minute=end.minute, second=end.second, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    return int(target.timestamp() * 1000)


def as_gtd(request: OrderRequest, *, now_s: float, timezone_name: str | None = None,
           session_end: str = "23:59:59") -> OrderRequest:
    """A Day order as the venue will hold it. Anything else is returned as is."""
    if request.time_in_force != TimeInForce.DAY:
        return request
    expires_at = request.expires_at or session_expiry(now_s, timezone_name=timezone_name, session_end=session_end)
    return request.model_copy(update={
        "time_in_force": TimeInForce.GTD,
        "expires_at": expires_at,
        "params": {**request.params, "requested_time_in_force": "day"},
    })
