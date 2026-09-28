"""Wire shapes the HTTP layer adds on top of the library's types.

Everything else on the wire is a library type serialized as-is. These three
exist because HTTP needs to say things an in-process call does not: where the
next page starts, what went wrong, and what a venue can do.
"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResponse(BaseModel, Generic[T]):
    """A page of results plus where the next one starts.

    Lists are enveloped and single resources are not. The envelope earns its
    place only when there is a cursor to carry; wrapping a single market in
    `{"data": ...}` would be ceremony that every generated client has to unwrap
    for nothing.
    """

    data: list[T]
    next_cursor: str | None = Field(
        default=None,
        description=(
            "Pass as `cursor` to fetch the next page. `null` means this venue "
            "returned no continuation — for a search result that is normal, "
            "since search is not paged."
        ),
    )
    count: int = Field(description="Rows in `data`, for convenience.")


class ErrorDetail(BaseModel):
    code: str = Field(description="Machine-readable, snake_case: `insufficient_funds`, `risk_rejected`, "
                                  "`unauthorized`, `validation_error`, ...")
    message: str = Field(description="For people. Do not branch on it; branch on `code`.")
    details: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Always `venue` (the venue that answered, or null) and `retryable` (true when the request never got a "
            "verdict: timeout, rate limit, venue down). A risk refusal adds `rule`; a malformed request adds "
            "`errors`, one entry per field; a rate limit may add `retry_after`."
        ),
    )


class ErrorBody(BaseModel):
    """What every non-2xx response carries, on every route of both apps:
    `{"error": {"code", "message", "details"}}`."""

    error: ErrorDetail


class VenueInfo(BaseModel):
    """What one venue is and what it can do.

    A client should read `has` before calling rather than discovering a gap
    through a 501. The values mirror the library's `Exchange.has` exactly:
    `true`, `false` or `"partial"`.
    """

    id: str
    name: str
    book_model: str = Field(
        description=(
            "`shared_complement` when both sides of a market read one book "
            "(Kalshi), `native_per_outcome` when each side owns its own "
            "(Polymarket)."
        )
    )
    has: dict[str, object]
