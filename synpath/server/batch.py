"""Batch reads and search across venues: many markets or books in one call, and
one query against several venues' catalogs.

These are the read app's cross-venue *fan-out* routes, the HTTP form of
`synpath.Client.fetch_markets_by_ids`, `Client.fetch_order_books` and
`Client.fetch_markets` / `fetch_events` without a venue. Nothing here
compares or matches listings; each venue answers for its own ids, through
the same registry adapters as every other route, so the rate limiter and
connection pool stay shared.

One rule for every route here: **a failure is reported where it happened,
never for the whole call.** Ids are grouped by venue and each venue is asked
once (Polymarket, Kalshi and the others batch natively); a venue that fails
fails only its own ids, a malformed id fails only itself, and an id the venue
no longer serves comes back as `market_not_found`. Every id asked for is in
exactly one of `data` and `errors`. Only a request that is wrong as a whole
(too many ids, a cursor with several venues) is refused outright.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Any, Callable, Generic, Literal, TypeVar

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel, Field

import synpath
from synpath import Event, Market, OrderBook, SynpathError
from synpath import ids as synpath_ids
from synpath.errors import MarketNotFound, NetworkError, NotSupported

from .errors import code_of, http_error
from .models import ErrorDetail, PageResponse

MAX_BATCH_IDS = 100
"""Ids one batch call may name. The terminal asks for at most 60 at a time; a
caller with more splits them, so one request never holds a venue's whole rate
budget."""

MAX_SEARCH_LIMIT = 100

T = TypeVar("T")


class BatchMarkets(BaseModel):
    """`GET /markets`: the markets found, in the order asked for, and why the rest are not there."""

    data: list[Market] = Field(description="Markets found, in the order their ids were given (duplicates once).")
    errors: dict[str, ErrorDetail] = Field(
        default_factory=dict,
        description="Synpath id -> why it is not in `data`: `bad_market_id`, `market_not_found` (the venue no "
                    "longer serves it), or the venue's own failure (`details.venue`, `details.retryable`).",
    )


class BatchBooks(BaseModel):
    """`GET /books`: one side's book per market, keyed by Synpath id."""

    data: dict[str, OrderBook] = Field(description="Synpath id -> its book, priced for `side`.")
    errors: dict[str, ErrorDetail] = Field(default_factory=dict, description="Synpath id -> why it has no book.")


class VenueSearch(BaseModel, Generic[T]):
    """`GET /search/...`: one page per venue, and the venues that failed."""

    results: dict[str, PageResponse[T]] = Field(
        description="Venue -> its page, most relevant first as the venue ranks it. `next_cursor` continues that "
                    "venue's search when exactly one venue was asked.")
    errors: dict[str, ErrorDetail] = Field(default_factory=dict, description="Venue -> why it has no page.")


# ---------------------------------------------------------------------------
# Grouping ids and turning failures into per-id errors
# ---------------------------------------------------------------------------

def parse_id_list(raw: str) -> list[str]:
    """`"a, b,a"` -> `["a", "b"]`: trimmed, empty entries dropped, duplicates once, order kept."""
    wanted = list(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))
    if not wanted:
        raise http_error(400, "validation_error", "ids: name at least one Synpath id", field="ids")
    if len(wanted) > MAX_BATCH_IDS:
        raise http_error(400, "too_many_ids", f"ids: at most {MAX_BATCH_IDS} per call; got {len(wanted)}",
                         field="ids", limit=MAX_BATCH_IDS)
    return wanted


def group_by_venue(market_ids: list[str]) -> tuple[dict[str, list[str]], dict[str, ErrorDetail]]:
    """Valid ids by venue, and an error for every id no venue can answer."""
    by_venue: dict[str, list[str]] = {}
    errors: dict[str, ErrorDetail] = {}
    for market_id in market_ids:
        venue, native_id = synpath_ids.split(market_id)
        if venue is None or not native_id:
            errors[market_id] = detail("bad_market_id", f"{market_id!r} is not a Synpath id: expected "
                                       f"'venue:native_id' with venue one of {', '.join(synpath_ids.VENUES)}")
        elif venue not in synpath.exchanges:
            errors[market_id] = detail("unknown_venue", f"unknown venue {venue!r}", venue=venue)
        else:
            by_venue.setdefault(venue, []).append(market_id)
    return by_venue, errors


def detail(code: str, message: str, *, venue: str | None = None, retryable: bool = False) -> ErrorDetail:
    return ErrorDetail(code=code, message=message, details={"venue": venue, "retryable": retryable})


def error_of(exc: Exception, venue: str) -> ErrorDetail:
    """A library exception as the same error a single-venue route would answer with."""
    if isinstance(exc, SynpathError):
        return detail(code_of(exc), str(exc), venue=getattr(exc, "synpath_venue", None) or venue,
                      retryable=isinstance(exc, NetworkError))
    return detail("internal_error", f"{venue}: the request failed", venue=venue)


def per_venue(work: dict[str, Any], ask: Callable[[str, Any], Any]) -> dict[str, Any | Exception]:
    """`ask(venue, item)` for every venue at once, each answer or the exception it raised.

    Venues share nothing, so they are asked in parallel: a search over eight
    venues takes as long as the slowest, not the sum. Each venue's own rate
    limiter still paces its requests."""
    if not work:
        return {}

    def one(venue: str) -> Any:
        try:
            return ask(venue, work[venue])
        except Exception as exc:  # reported per venue, never for the whole call
            return exc

    with ThreadPoolExecutor(max_workers=min(len(work), 8), thread_name_prefix="synpath-batch") as pool:
        return dict(zip(work, pool.map(one, work)))


def registry_of(request: Request) -> Any:
    from .api import DEFAULT_REGISTRY
    return getattr(request.app.state, "registry", None) or DEFAULT_REGISTRY


# ---------------------------------------------------------------------------
# The routes
# ---------------------------------------------------------------------------

IdsQuery = Annotated[str, Query(max_length=20_000, description=(
    f"Comma-separated Synpath ids (`kalshi:KXFOO-25,polymarket:2252244`), at most {MAX_BATCH_IDS}. "
    "Duplicates are answered once."))]

SearchQuery = Annotated[str, Query(min_length=1, max_length=200, description="The text to search for.")]
VenuesQuery = Annotated[list[str] | None, Query(alias="venue", description=(
    "Repeat to name several venues; default every venue."))]
LimitQuery = Annotated[int, Query(ge=1, le=MAX_SEARCH_LIMIT, description="Per venue.")]
CursorQuery = Annotated[str | None, Query(description="`next_cursor` from a one-venue search.")]
StatusQuery = Annotated[str, Query(pattern="^(open|closed|settled|all)$")]



def add_batch_routes(app: FastAPI, protected: list[Any], responses: dict[int | str, dict[str, Any]]) -> None:
    """Add `GET /markets`, `GET /books`, `GET /search/markets` and `GET /search/events` to the read app."""

    @app.get("/markets", dependencies=protected, tags=["batch"], summary="Many markets by id, across venues",
             responses=responses)
    def batch_markets(request: Request, ids: IdsQuery) -> BatchMarkets:
        """`fetch_markets_by_ids`, one request per venue. This is the quote path: each market carries its
        `yes.quote` and `no.quote` (bid, ask, last) and `stats`, as `fetch_markets_by_ids` returns them."""
        wanted = parse_id_list(ids)
        by_venue, errors = group_by_venue(wanted)
        registry = registry_of(request)
        answers = per_venue(by_venue, lambda venue, batch: registry.get(venue).fetch_markets_by_ids(batch))
        found: dict[str, Market] = {}
        for venue, answer in answers.items():
            if isinstance(answer, Exception):
                errors.update({market_id: error_of(answer, venue) for market_id in by_venue[venue]})
                continue
            found.update({market.id: market for market in answer})
            for market_id in by_venue[venue]:
                if synpath_ids.qualify(venue, synpath_ids.split(market_id)[1]) not in found:
                    errors[market_id] = detail("market_not_found", f"{venue}: no market {market_id}", venue=venue)
        data = [found[market_id] for market_id in wanted if market_id in found]
        return BatchMarkets(data=data, errors=errors)

    @app.get("/books", dependencies=protected, tags=["batch"], summary="Many order books by id, across venues",
             responses=responses)
    def batch_books(
        request: Request,
        ids: IdsQuery,
        side: Annotated[Literal["yes", "no"], Query(description="Which side every book is priced for.")] = "yes",
        depth: Annotated[int | None, Query(ge=1, le=1000)] = None,
    ) -> BatchBooks:
        """`fetch_order_books`, one call per venue (Polymarket answers a batch in one request; venues
        without a batch endpoint read one book per market against their shared rate budget)."""
        wanted = parse_id_list(ids)
        by_venue, errors = group_by_venue(wanted)
        registry = registry_of(request)

        def books_of(venue: str, batch: list[str]) -> dict[str, OrderBook]:
            exchange = registry.get(venue)
            try:
                return exchange.fetch_order_books(batch, side=side, depth=depth)
            except NotSupported:
                return {market_id: exchange.fetch_order_book(market_id, side=side, depth=depth) for market_id in batch}

        answers = per_venue(by_venue, books_of)
        data: dict[str, OrderBook] = {}
        for venue, answer in answers.items():
            if isinstance(answer, Exception):
                errors.update({market_id: error_of(answer, venue) for market_id in by_venue[venue]})
                continue
            for market_id in by_venue[venue]:
                book = answer.get(synpath_ids.qualify(venue, synpath_ids.split(market_id)[1]))
                if book is None:
                    errors[market_id] = detail("market_not_found", f"{venue}: no book for {market_id}", venue=venue)
                else:
                    data[market_id] = book
        return BatchBooks(data=data, errors=errors)

    def search(request: Request, kind: Literal["markets", "events"], query: str, venues: list[str] | None,
               limit: int, cursor: str | None, status: str) -> dict[str, Any]:
        """One query against each venue's own search (`fetch_markets(query=...)` / `fetch_events(query=...)`)."""
        chosen = list(dict.fromkeys(venues or synpath.exchanges))
        unknown = [venue for venue in chosen if venue not in synpath.exchanges]
        if unknown:
            raise http_error(404, "unknown_venue", f"unknown venue {unknown[0]!r}; available: "
                             f"{', '.join(sorted(synpath.exchanges))}", field="venue")
        if cursor is not None and len(chosen) != 1:
            raise http_error(400, "validation_error", "cursor: a cursor belongs to one venue; name exactly one venue",
                             field="cursor")
        registry = registry_of(request)

        def page_of(venue: str, _: Any) -> Any:
            exchange = registry.get(venue)
            fetch = exchange.fetch_markets if kind == "markets" else exchange.fetch_events
            return fetch(query=query, limit=limit, cursor=cursor, status=status)

        results: dict[str, Any] = {}
        errors: dict[str, ErrorDetail] = {}
        for venue, answer in per_venue({venue: None for venue in chosen}, page_of).items():
            if isinstance(answer, Exception):
                errors[venue] = error_of(answer, venue)
            else:
                results[venue] = {"data": list(answer), "next_cursor": getattr(answer, "next_cursor", None),
                                  "count": len(answer)}
        return {"results": results, "errors": errors}

    @app.get("/search/markets", dependencies=protected, tags=["batch"], summary="Search markets on several venues",
             responses=responses)
    def search_markets(request: Request, query: SearchQuery, venues: VenuesQuery = None, limit: LimitQuery = 20,
                       cursor: CursorQuery = None, status: StatusQuery = "open") -> VenueSearch[Market]:
        """Each venue's own search, asked in parallel: Kalshi's and Polymarket US's search endpoints,
        Polymarket's server-side text filter, and so on (`fetch_markets(query=...)`)."""
        return VenueSearch[Market].model_validate(search(request, "markets", query, venues, limit, cursor, status))

    @app.get("/search/events", dependencies=protected, tags=["batch"], summary="Search events on several venues",
             responses=responses)
    def search_events(request: Request, query: SearchQuery, venues: VenuesQuery = None, limit: LimitQuery = 20,
                      cursor: CursorQuery = None, status: StatusQuery = "open") -> VenueSearch[Event]:
        """Events with their markets nested, from each venue's own search (`fetch_events(query=...)`)."""
        return VenueSearch[Event].model_validate(search(request, "events", query, venues, limit, cursor, status))


__all__ = ["BatchBooks", "BatchMarkets", "MAX_BATCH_IDS", "VenueSearch", "add_batch_routes"]
