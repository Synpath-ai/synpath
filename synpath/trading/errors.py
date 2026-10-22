"""Trading errors, under the same two branches as the read API's.

`ExchangeError` still means "the venue answered and said no" and
`NetworkError` still means "no verdict". Two things are new. `RiskRejected`
is neither: the request never reached a venue because this library's own
pre-trade checks refused it, and retrying unchanged will be refused again.
`RateBudgetExceeded` is a refusal from the local budget limiter -- the queue
ahead of the request was already too long for it to be honoured in time --
which is deliberately not `RateLimitExceeded`: nothing was sent, and waiting
longer is the wrong answer for an order that had a deadline.

The engine adds three that say an outcome is not yet known, rather than
that something failed: `SubmissionInDoubt` (this send got no verdict),
`SubmissionPending` (a retry of a send still being reconciled) and
`CancelUnconfirmed` (the venue neither cancelled the order nor knows it).
"""
from __future__ import annotations

from typing import Any

from ..errors import AuthenticationError, BadRequest, ExchangeError, NetworkError, SynpathError


class InvalidOrder(BadRequest):
    """The order cannot be placed as written: off-tick price, size below the
    minimum, a market order without a protection price, a sell with no
    inventory on a venue that does not net. Caught before signing."""


class DuplicateClientOrderId(InvalidOrder):
    """A `client_order_id` this account has already used.

    Raised rather than resubmitted: the id is the idempotency key, and a
    second order under the same id is exactly the duplicate it exists to
    prevent.
    """


class InsufficientFunds(ExchangeError):
    """The venue refused for lack of balance or buying power."""


class OrderNotFound(ExchangeError):
    """No order under that id at the venue -- already gone, or never there."""


class RfqNotFound(OrderNotFound):
    """No RFQ, quote or combo under that id at the venue, or none visible to
    this account."""


class OrderRejected(ExchangeError):
    """The venue took the order and rejected it: post-only would have crossed,
    the market is closed, self-trade prevention fired. `info` says which."""

    def __init__(
        self, message: str, *, reason: str | None = None, info: dict | None = None,
        body: Any = None, status: int | None = None,
    ):
        super().__init__(message, body=body, status=status)
        self.reason = reason
        self.info = info or {}


class MarketHalted(ExchangeError):
    """The market is listed but not accepting orders right now."""


class PermissionDenied(AuthenticationError):
    """Credentials are valid but not allowed to do this: a read-only key, a
    subaccount the key was not granted, a venue the account has not enabled."""


class CredentialsMissing(AuthenticationError):
    """No credentials configured for this venue.

    Says which environment variables to set. Distinct from `PermissionDenied`
    on purpose: one is a setup gap, the other is a decision someone made.
    """


class RiskRejected(SynpathError):
    """Refused by this library's pre-trade checks before anything was sent.

    `rule` names the check that fired. Not an `ExchangeError`: no venue was
    involved, and retrying unchanged will be refused again.
    """

    def __init__(self, message: str, *, rule: str):
        super().__init__(message)
        self.rule = rule


class RateBudgetExceeded(SynpathError):
    """The local budget limiter could not honour the request in time.

    Nothing was sent. The queue ahead of the request was already longer than
    its deadline allowed, and quietly delaying it would have let a stop fire
    late while looking like it fired on time.
    """

    def __init__(self, message: str, *, wait_s: float):
        super().__init__(message)
        self.wait_s = wait_s


class SubmissionPending(NetworkError):
    """An earlier send has no definitive answer. Retrying must not resend it.

    Raised to a retry of a `client_order_id` whose first send is still being
    reconciled; the same retry later returns the order or its refusal."""

    def __init__(self, message: str, *, client_order_id: str | None = None):
        super().__init__(message)
        self.client_order_id = client_order_id


class SubmissionInDoubt(NetworkError):
    """This send reached the venue, or may have, and no answer came back.

    The intent stays `sending` and is reconciled with the venue by its client
    order id; retrying with the same `client_order_id` returns the order once
    it is found and never sends a second one."""

    def __init__(self, message: str, *, client_order_id: str | None = None):
        super().__init__(message)
        self.client_order_id = client_order_id


class CancelUnconfirmed(OrderNotFound):
    """The venue would not cancel the order and has no record of it either.

    Not found is not cancelled: the order may have filled or expired. Its last
    observed status stands until the venue reports otherwise."""
