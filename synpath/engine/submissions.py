"""Retry identity and unresolved submission errors, independent of storage."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..errors import NetworkError
from ..trading.types import OrderRequest


class SubmissionPending(NetworkError):
    """An earlier send has no definitive answer. Retrying must not resend it."""


def request_identity(request: OrderRequest) -> dict[str, Any]:
    """Compare instructions rather than the spelling of their decimal values."""
    body = request.model_dump(mode="json")
    for name in ("quantity", "price", "stop_price"):
        if body[name] is not None:
            body[name] = format(Decimal(body[name]).normalize(), "f")
    return body
