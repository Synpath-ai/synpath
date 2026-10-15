"""Synpath's hosted services through `synpath serve`: matching and tick history.

The HTTP form of `synpath.match_market`, `synpath.match_event` and
`synpath.history` (`fetch_order_book_at`, `fetch_order_book_range`,
`fetch_trades_range`), plus the hosted arbitrage scan the matching service
publishes. A client of serve therefore needs no Synpath API key and opens no
connection to api.synpath.dev or api2.synpath.dev itself: serve makes the
call, with the key the library finds (`SYNPATH_API_KEY`, else the key
`synpath login` / `synpath keys create` saved), exactly as an in-process
caller's would be found.

These routes are added by `synpath serve`, not by the plain read app
(`create_app`), which stays a single-venue surface with no cross-venue logic.

Hosted failures get their own error codes, so a client can tell "serve
refused my token" (401) from "Synpath's service refused serve's key":

| Hosted answer | Serve answers |
|---|---|
| 401 / 403 (no or bad Synpath API key) | 502 `hosted_unauthorized` |
| 404 from the service (no such listing) | 404 `market_not_found` |
| 404 from the host in front (not deployed) | 502 `hosted_unavailable`, retryable |
| 413 (result too large to return whole) | 413 `result_too_large` |
| 429 | 429 `rate_limit_exceeded`, retryable |
| timeout | 504 `hosted_timeout`, retryable |
| unreachable, 5xx | 502 `hosted_unavailable`, retryable |
"""
from __future__ import annotations

from functools import wraps
from typing import Annotated, Any, Callable, Literal, TypeVar

from fastapi import FastAPI, HTTPException, Query, Request

from .. import history, matching
from ..errors import (
    AuthenticationError, BadRequest, ExchangeError, MarketNotFound, NetworkError, RateLimitExceeded, RequestTimeout,
    SynpathError,
)
from ..hosted import LABEL, call, hosted_client
from ..types import EventMatch, MarketMatch, OrderBookAtResponse, OrderBookRangeResponse, TradesRangeResponse
from .errors import http_error
from .models import ErrorBody

R = TypeVar("R")

HOSTED_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorBody, "description": "Invalid parameters, such as an id that is not a Synpath id."},
    404: {"model": ErrorBody, "description": "The hosted service has no such listing."},
    413: {"model": ErrorBody, "description": "`result_too_large`: narrow the range or raise `limit`."},
    429: {"model": ErrorBody, "description": "The hosted service's rate limit; retry after backing off."},
    502: {"model": ErrorBody, "description": "`hosted_unauthorized` (serve's Synpath API key is missing or refused) "
                                             "or `hosted_unavailable` (the service is down or unreachable)."},
    504: {"model": ErrorBody, "description": "`hosted_timeout`: the service did not answer in time."},
}


def hosted_failure(exc: SynpathError) -> HTTPException:
    """A hosted service's failure as serve's answer (see the table above)."""
    message = str(exc)
    if isinstance(exc, ExchangeError) and exc.status == 413:
        return http_error(413, "result_too_large", message, venue=LABEL, retryable=False)
    if isinstance(exc, AuthenticationError):
        return http_error(502, "hosted_unauthorized", message, venue=LABEL, retryable=False)
    if isinstance(exc, MarketNotFound):
        return http_error(404, "market_not_found", message, venue=LABEL, retryable=False)
    if isinstance(exc, BadRequest):
        return http_error(400, "bad_request", message, venue=LABEL, retryable=False)
    if isinstance(exc, RateLimitExceeded):
        return http_error(429, "rate_limit_exceeded", message, venue=LABEL, retryable=True)
    if isinstance(exc, RequestTimeout):
        return http_error(504, "hosted_timeout", message, venue=LABEL, retryable=True)
    if isinstance(exc, NetworkError):
        return http_error(502, "hosted_unavailable", message, venue=LABEL, retryable=True)
    return http_error(502, "hosted_error", message, venue=LABEL, retryable=False)


def answered_by_hosted(handler: Callable[..., R]) -> Callable[..., R]:
    """Run a route that calls a hosted service, turning its failures into serve's errors."""
    @wraps(handler)
    def run(*args: Any, **kwargs: Any) -> R:
        try:
            return handler(*args, **kwargs)
        except SynpathError as exc:
            raise hosted_failure(exc) from None
    return run


def hosted_http_of(request: Request) -> Any:
    """The `httpx.Client` hosted calls go through, when the host injected one
    (tests, or a host pooling connections); otherwise each call opens and
    closes its own, as the library does."""
    return getattr(request.app.state, "hosted_http", None)


def check_window(start_ms: int, end_ms: int) -> None:
    if end_ms <= start_ms:
        raise http_error(400, "validation_error", "end_ms: must be after start_ms", field="end_ms")


MarketIdQuery = Annotated[str, Query(min_length=3, max_length=300, description=(
    "A Synpath id (`kalshi:KXFOO-25`, `polymarket:2252244`)."))]
SideQuery = Annotated[Literal["yes", "no"] | None, Query(description="Which side the book is priced for.")]


def add_hosted_routes(app: FastAPI, protected: list[Any]) -> None:
    """Add `/matching/...` and `/history/...` to serve's root app."""

    @app.get("/matching/market", dependencies=protected, tags=["matching"], responses=HOSTED_RESPONSES,
             summary="The same market on another venue")
    @answered_by_hosted
    def match_market(request: Request, id: MarketIdQuery,
                     venue: Annotated[str | None, Query(description=(
                         "Which venue to look on; default the anchor's usual counterpart."))] = None) -> MarketMatch:
        """`synpath.match_market`. `matched: null` means the anchor is a real listing with nothing on the other
        venue; an anchor the service does not know is a 404."""
        return matching.match_market(id, venue=venue, client=hosted_http_of(request))

    @app.get("/matching/event", dependencies=protected, tags=["matching"], responses=HOSTED_RESPONSES,
             summary="The same event on the other venues")
    @answered_by_hosted
    def match_event(request: Request, id: MarketIdQuery) -> EventMatch:
        """`synpath.match_event`: per venue, its native event ids on the same event, or `null`."""
        return matching.match_event(id, client=hosted_http_of(request))

    @app.get("/matching/arbitrage", dependencies=protected, tags=["matching"], responses=HOSTED_RESPONSES,
             summary="The hosted cross-venue scan")
    @answered_by_hosted
    def arbitrage(request: Request) -> dict[str, Any]:
        """The matching service's `GET /arbitrage`, passed through as it answers: `computed_at`, and `results`,
        each an outcome priced as YES on one venue and NO on another (best asks, net of taker fees):
        `outcome_id`, `event_title`, `outcome_label`, `close_at`, `yes` and `no` (`venue`, `native_market_id`,
        `title`, `side`, `price`, `fee`, `url`), `gross_spread`, `fees`, `net_profit`."""
        injected = hosted_http_of(request)
        http = hosted_client(base_url=None, env=matching.BASE_URL_ENV, default=matching.DEFAULT_URL,
                             http_client=injected)
        return call(http, "GET", "/arbitrage", api_key=None, owned=injected is None, use_stored=True)

    @app.get("/history/order-book/at", dependencies=protected, tags=["history"], responses=HOSTED_RESPONSES,
             summary="The book as it stood at a time")
    @answered_by_hosted
    def order_book_at(request: Request, market_id: MarketIdQuery,
                      as_of_ms: Annotated[int, Query(ge=0, description="Recorder receive time, Unix ms.")],
                      side: SideQuery = None,
                      depth: Annotated[int | None, Query(ge=1, le=1000)] = None) -> OrderBookAtResponse:
        """`synpath.fetch_order_book_at`. `book: null` with `absence_reason` where there is no coverage."""
        return history.fetch_order_book_at(market_id, as_of_ms, side=side, depth=depth,
                                           client=hosted_http_of(request))

    @app.get("/history/order-book/range", dependencies=protected, tags=["history"], responses=HOSTED_RESPONSES,
             summary="Every change to a book over a window")
    @answered_by_hosted
    def order_book_range(request: Request, market_id: MarketIdQuery,
                         start_ms: Annotated[int, Query(ge=0)], end_ms: Annotated[int, Query(ge=0)],
                         side: SideQuery = None,
                         limit: Annotated[int | None, Query(ge=1)] = None) -> OrderBookRangeResponse:
        """`synpath.fetch_order_book_range`: segments, each from a full book; uncovered time is its own segment.
        A window too large to return whole is a 413 `result_too_large`."""
        check_window(start_ms, end_ms)
        return history.fetch_order_book_range(market_id, start_ms, end_ms, side=side, limit=limit,
                                              client=hosted_http_of(request))

    @app.get("/history/trades/range", dependencies=protected, tags=["history"], responses=HOSTED_RESPONSES,
             summary="Trades over a window")
    @answered_by_hosted
    def trades_range(request: Request, market_id: MarketIdQuery,
                     start_ms: Annotated[int, Query(ge=0)], end_ms: Annotated[int, Query(ge=0)],
                     limit: Annotated[int | None, Query(ge=1)] = None) -> TradesRangeResponse:
        """`synpath.fetch_trades_range`, with recording coverage; the metadata names the time basis."""
        check_window(start_ms, end_ms)
        return history.fetch_trades_range(market_id, start_ms, end_ms, limit=limit, client=hosted_http_of(request))


__all__ = ["add_hosted_routes", "hosted_failure"]
