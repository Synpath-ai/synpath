"""Error hierarchy, named after ccxt's so the reflexes transfer.

Two branches under `SynpathError`, and the split is the one a caller actually
acts on:

  * `NetworkError`  — the request never got a verdict from the venue. Retrying
    the identical call is reasonable.
  * `ExchangeError` — the venue answered and said no. Retrying unchanged will
    fail again; the caller has to change the request.

`RateLimitExceeded` sits under `NetworkError` for exactly this reason, which
surprises people until they notice that waiting and retrying is the correct
response to it.
"""
from __future__ import annotations

from typing import Any


class SynpathError(Exception):
    """Base for everything this library raises on purpose."""


class NetworkError(SynpathError):
    """No verdict from the venue: timeout, DNS, connection reset, 5xx."""


class ExchangeNotAvailable(NetworkError):
    """The venue is up but refusing service — maintenance, 502/503.

    `body` is the venue's parsed payload when it sent one: some venues say
    *why* they refuse (cancel-only mode, a scheduled pause) in a 503.
    """

    def __init__(self, message: str, *, body: Any = None, status: int | None = None):
        super().__init__(message)
        self.body = body
        self.status = status


class RequestTimeout(NetworkError):
    """The venue did not answer inside the timeout."""


class RateLimitExceeded(NetworkError):
    """Too many requests. Retry after backing off."""

    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after
        """Seconds to wait, when the venue said. Kalshi does not, so usually None."""


class ExchangeError(SynpathError):
    """The venue answered and rejected the request.

    `body` is the venue's parsed error payload when it sent JSON, and
    `status` the HTTP status, so a caller (or a trading adapter) can branch
    on the venue's own error code rather than on message text.
    """

    def __init__(self, message: str, *, body: Any = None, status: int | None = None):
        super().__init__(message)
        self.body = body
        self.status = status

    @property
    def code(self) -> str | None:
        """The venue's error code, where its payload carries one."""
        if isinstance(self.body, dict):
            inner = self.body.get("error") if isinstance(self.body.get("error"), dict) else self.body
            code = inner.get("code") if isinstance(inner, dict) else None
            return str(code) if code is not None else None
        return None


class BadRequest(ExchangeError):
    """Malformed or invalid parameters (4xx that is not auth or not-found)."""


class BadSymbol(BadRequest):
    """A market, event or instrument id this venue does not recognise."""


class MarketNotFound(BadSymbol):
    """The id is well-formed but the venue has no such market."""


class NotSupported(SynpathError):
    """This venue does not offer the capability.

    Raised rather than returning empty, so a missing feature is never mistaken
    for an absence of data. Check `exchange.has` to branch before calling.
    """


class AuthenticationError(ExchangeError):
    """Credentials missing, malformed, or rejected."""
