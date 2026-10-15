"""Retry identity, independent of storage. The unresolved-action errors live
with the other trading errors and are re-exported here."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..trading.errors import CancelUnconfirmed, SubmissionInDoubt, SubmissionPending
from ..trading.types import OrderRequest

__all__ = ["CancelUnconfirmed", "SubmissionInDoubt", "SubmissionPending", "request_identity"]


def request_identity(request: OrderRequest) -> dict[str, Any]:
    """Compare instructions rather than the spelling of their decimal values."""
    body = request.model_dump(mode="json")
    for name in ("quantity", "price", "stop_price"):
        if body[name] is not None:
            body[name] = format(Decimal(body[name]).normalize(), "f")
    return body
