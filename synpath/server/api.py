"""The HTTP surface over the library. No authentication, on purpose.

This app is the *inner* app. It knows how to turn a request into a library
call and a library exception into a status code, and nothing else — no API
keys, no quotas, no metering, no cross-venue logic. A hosted deployment mounts
it under its own middleware and owns all of that:

```python
from fastapi import FastAPI, Depends
from synpath.server import create_app, VenueRegistry

outer = FastAPI()
outer.state.registry = VenueRegistry()          # optional; a default is used otherwise
outer.include_router(create_app(docs=False).router, dependencies=[Depends(my_auth)])
```

Note the registry on the host app. A mounted route is served by the host, so
`request.app` is the host's app and that is where the registry is looked for.

Routes are written out one by one rather than dispatched dynamically from a
method name. Dynamic dispatch is less code and produces an OpenAPI document
that says every response is `object`, which makes generated clients untyped —
and a typed TypeScript client generated from this schema is the whole reason
the server layer exists.

Handlers are `def`, not `async def`. The library is synchronous, so Starlette
runs them in a worker thread; writing them `async` would block the event loop
on every venue call.
"""
from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager
from typing import Literal, Annotated, Any, Iterator

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

import synpath
from synpath import (
    BadRequest,
    Candle,
    Event,
    Exchange,
    ExchangeError,
    ExchangeNotAvailable,
    FeeSchedule,
    Market,
    MarketNotFound,
    NetworkError,
    NotSupported,
    OrderBook,
    RateLimitExceeded,
    RequestTimeout,
    Series,
    SynpathError,
    Trade,
)

from ..base import timeframe_seconds
from .errors import http_error, install_error_handlers
from .models import ErrorBody, PageResponse, VenueInfo

log = logging.getLogger("synpath.server")

STATUS_FOR: dict[type[Exception], int] = {
    MarketNotFound: 404,
    BadRequest: 400,
    NotSupported: 501,
    RateLimitExceeded: 429,
    RequestTimeout: 504,
    ExchangeNotAvailable: 502,
    NetworkError: 502,
    ExchangeError: 502,
}
"""Library exception to status code.

The two that are worth explaining: `NotSupported` is 501 rather than 404,
because the venue has no such capability at all and no id would have worked;
and a venue failing us is 502, not 500, because this service is fine and is
reporting on an upstream that is not.
"""


def _status_for(exc: Exception) -> int:
    for kind in type(exc).__mro__:
        if kind in STATUS_FOR:
            return STATUS_FOR[kind]
    return 500


def _venue_of(exc: Exception) -> str | None:
    text = str(exc)
    head = text.split(":", 1)[0].strip().lower()
    return head if head in synpath.exchanges else None


class VenueRegistry:
    """One adapter instance per venue, reused across requests.

    Reused rather than constructed per request because the rate limiter and
    the HTTP connection pool both have to be shared: a venue counts requests
    per account and IP, so a fresh client per request would hand every request
    a full token bucket and sail straight past the limit.

    Nothing request-scoped is kept on an adapter — paging cursors ride on the
    returned `Page` — so sharing one across threads is safe.
    """

    def __init__(self, exchanges: dict[str, Exchange] | None = None):
        self._exchanges: dict[str, Exchange] = exchanges or {}
        # Handlers are sync `def`, so Starlette runs them in a thread pool and
        # two requests really do race here. Without this, both would build an
        # adapter, one would be dropped still holding an open connection pool,
        # and for that moment two rate limiters would each think they owned the
        # venue's whole budget.
        self._lock = threading.Lock()

    def get(self, venue: str) -> Exchange:
        if venue not in synpath.exchanges:
            raise http_error(
                404, "unknown_venue",
                f"unknown venue {venue!r}; available: {', '.join(sorted(synpath.exchanges))}",
            )
        existing = self._exchanges.get(venue)
        if existing is not None:
            return existing
        with self._lock:
            # Checked again under the lock: another thread may have built it
            # while this one waited.
            if venue not in self._exchanges:
                self._exchanges[venue] = synpath.exchange(venue)
            return self._exchanges[venue]

    def close(self) -> None:
        with self._lock:
            for exchange in self._exchanges.values():
                exchange.close()
            self._exchanges.clear()


DEFAULT_REGISTRY = VenueRegistry()
"""Used when a request arrives on an app that carries no registry of its own —
which is what happens when the router is mounted on somebody else's app."""


def get_venue(
    request: Request,
    venue: Annotated[str, Path(description="Venue id, e.g. `kalshi`.")],
) -> Exchange:
    """Resolve the adapter for this request.

    The registry is looked up on the app handling the request, so a host that
    mounts this router and wants its own registry sets it on its own app:

    ```python
    outer.state.registry = VenueRegistry()
    outer.include_router(create_app().router)
    ```

    Without that, requests fall back to the process-wide default, which is the
    right answer for the standalone case and harmless for the mounted one.
    """
    registry = getattr(request.app.state, "registry", None) or DEFAULT_REGISTRY
    return registry.get(venue)


Venue = Annotated[Exchange, Depends(get_venue)]
"""Module level on purpose: `from __future__ import annotations` turns every
annotation into a string, and FastAPI resolves those against module globals.
Defined inside the factory it would be invisible and the app would fail to
build its schema."""

RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorBody, "description": "Invalid parameters."},
    404: {"model": ErrorBody, "description": "No such venue or market."},
    429: {"model": ErrorBody, "description": "Venue rate limit; retry after backing off."},
    501: {"model": ErrorBody, "description": "This venue does not offer the capability."},
    502: {"model": ErrorBody, "description": "The venue failed or rejected the request."},
    504: {"model": ErrorBody, "description": "The venue did not answer in time."},
}


def create_app(
    *,
    registry: VenueRegistry | None = None,
    title: str = "synpath",
    docs: bool = True,
) -> FastAPI:
    """Build the app. Pass `registry` to inject stub adapters in tests."""
    registry = registry or DEFAULT_REGISTRY

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        # Closes the registry this app was built with, not the one belonging to
        # whichever app runs the lifespan: `include_router` merges lifespan
        # contexts, so a host mounting this router runs this function with its
        # own app object, which has no registry of ours on it.
        registry.close()

    app = FastAPI(
        lifespan=lifespan,
        title=title,
        version=synpath.__version__,
        summary="One API for prediction markets.",
        description=(
            "Read-only market data for Kalshi and Polymarket behind one "
            "interface.\n\n"
            "Prices are labelled by source: `bid`, `ask`, `mid` and `last` are "
            "separate fields, `mid` is null unless both sides are quoted, and a "
            "null price means the venue published none rather than zero. "
            "Candles carry `price_source`; volume figures carry `volume_unit`, "
            "because Kalshi counts contracts and Polymarket counts collateral.\n\n"
            "Call `GET /venues` first: `has` says which capabilities a venue "
            "offers, and an unsupported one answers 501."
        ),
        docs_url="/docs" if docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if docs else None,
    )
    app.state.registry = registry

    # -- errors -------------------------------------------------------------

    install_error_handlers(app, _status_for)

    # -- discovery ----------------------------------------------------------

    @app.get("/", tags=["meta"], summary="What this server is")
    def index(request: Request) -> dict[str, Any]:
        """A landing answer for someone who opens the address in a browser."""
        links = {"docs": "/docs", "openapi": "/openapi.json", "health": "/health", "venues": "/venues"}
        trading = getattr(request.app.state, "trading_path", None)
        if trading:
            links["trading"] = trading
        return {"name": "synpath", "version": synpath.__version__, "links": links}

    @app.get("/health", tags=["meta"], summary="Liveness")
    def health() -> dict[str, str]:
        """Answers without touching a venue, so it stays useful when one is down."""
        return {"status": "ok", "version": synpath.__version__}

    @app.get("/venues", tags=["meta"], summary="List venues and capabilities")
    def list_venues() -> list[VenueInfo]:
        """Read this before anything else. `has` tells you which calls a venue
        answers and which it will refuse with a 501."""
        return [
            VenueInfo(
                id=cls.id, name=cls.name, book_model=cls.book_model, has=dict(cls.has),
            )
            for cls in synpath.exchanges.values()
        ]

    @app.get("/venues/{venue}", tags=["meta"], summary="One venue", responses=RESPONSES)
    def get_venue_info(exchange: Venue) -> VenueInfo:
        return VenueInfo(
            id=exchange.id, name=exchange.name,
            book_model=exchange.book_model, has=dict(exchange.has),
        )

    # -- catalog ------------------------------------------------------------

    @app.get(
        "/venues/{venue}/markets", tags=["catalog"],
        summary="List markets", responses=RESPONSES,
    )
    def list_markets(
        exchange: Venue,
        query: Annotated[str | None, Query(description=(
            "Text filter. Server-side on Polymarket; on Kalshi the page is "
            "filtered locally, so a page may come back short while "
            "`next_cursor` still points at more."
        ))] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
        cursor: Annotated[str | None, Query(description=(
            "Opaque cursor from a previous `next_cursor`. There is no `offset`: "
            "the catalog changes between calls and offset paging silently drops "
            "or repeats rows when it does."
        ))] = None,
        status: Annotated[str, Query(pattern="^(open|closed|settled|all)$")] = "open",
        sort: Annotated[str | None, Query(description=(
            "`volume`, `liquidity` or `newest`. Any other value is refused with 400, and a venue that cannot "
            "sort answers 501 rather than returning unsorted rows."
        ))] = None,
    ) -> PageResponse[Market]:
        page = exchange.fetch_markets(
            query=query, limit=limit, cursor=cursor, status=status, sort=sort,
        )
        return PageResponse[Market](
            data=list(page), next_cursor=page.next_cursor, count=len(page),
        )

    @app.get(
        "/venues/{venue}/markets/{market_id}", tags=["catalog"],
        summary="One market", responses=RESPONSES,
    )
    def get_market(
        exchange: Venue,
        market_id: Annotated[str, Path(description=(
            "Venue market id: a Kalshi ticker, or a Polymarket numeric id. "
            "No path converter here on purpose — a greedy one would swallow "
            "the `/trades` and `/fee` suffixes below."
        ))],
    ) -> Market:
        return exchange.fetch_market(market_id)

    @app.get(
        "/venues/{venue}/events", tags=["catalog"],
        summary="List events with markets nested", responses=RESPONSES,
    )
    def list_events(
        exchange: Venue,
        query: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
        cursor: Annotated[str | None, Query()] = None,
        status: Annotated[str, Query(pattern="^(open|closed|settled|all)$")] = "open",
    ) -> PageResponse[Event]:
        page = exchange.fetch_events(
            query=query, limit=limit, cursor=cursor, status=status,
        )
        return PageResponse[Event](
            data=list(page), next_cursor=page.next_cursor, count=len(page),
        )

    # -- market data --------------------------------------------------------

    @app.get(
        "/venues/{venue}/markets/{market_id}/book", tags=["market data"],
        summary="Order book for one side of a market", responses=RESPONSES,
    )
    def get_order_book(
        exchange: Venue,
        market_id: Annotated[str, Path(description=(
            "A Synpath id (`kalshi:KXFOO-25`, `polymarket:2252244`) or the "
            "venue's own id for this venue."
        ))],
        side: Annotated[Literal["yes", "no"], Query(description=(
            "Which side the book is priced for. `no` is what NO costs."
        ))] = "yes",
        depth: Annotated[int | None, Query(ge=1, le=1000)] = None,
    ) -> OrderBook:
        """Bids descend, asks ascend, best first on both sides.

        On Kalshi and Polymarket US one book serves both sides, so the NO
        view is the YES book reflected through the market's face value and
        the response says so with `derived: true`. On Polymarket the NO book
        is a real book of its own.
        """
        return exchange.fetch_order_book(market_id, side=side, depth=depth)

    @app.get(
        "/venues/{venue}/markets/{market_id}/trades", tags=["market data"],
        summary="Recent trades", responses=RESPONSES,
    )
    def list_trades(
        exchange: Venue,
        market_id: str,
        since: Annotated[int | None, Query(description="Milliseconds since epoch, inclusive.")] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
        cursor: Annotated[str | None, Query()] = None,
    ) -> PageResponse[Trade]:
        """Oldest first, in the YES price. `side` is what the taker did on the
        YES leg: `buy` took YES, `sell` took NO."""
        page = exchange.fetch_trades(market_id, since=since, limit=limit, cursor=cursor)
        return PageResponse[Trade](
            data=list(page), next_cursor=page.next_cursor, count=len(page),
        )

    @app.get(
        "/venues/{venue}/markets/{market_id}/candles", tags=["market data"],
        summary="OHLCV candles, in the YES price", responses=RESPONSES,
    )
    def list_candles(
        exchange: Venue,
        market_id: str,
        timeframe: Annotated[str, Query(description=(
            "Kalshi accepts `1m`, `1h`, `1d` only and refuses anything else "
            "rather than rounding to a period you did not ask for."
        ))] = "1h",
        since: Annotated[int | None, Query(description=(
            "Milliseconds since epoch. With it, bars are read forward from here and the "
            "first `limit` returned; without it, the newest `limit` bars."
        ))] = None,
        until: Annotated[int | None, Query(description="Milliseconds since epoch.")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        cursor: Annotated[str | None, Query(description=(
            "`next_cursor` from the previous page of a read with `since`; continues after its last bar."
        ))] = None,
    ) -> PageResponse[Candle]:
        """Read `price_source` on every bar before using it.

        `trade` means executions. `bid_ask_mid` means nothing traded in that
        period and the bar is the book's midpoint. `sampled_mid` means the
        venue publishes no candles at all and these were bucketed from price
        samples — those carry `volume: null`, because null is not zero.

        A long history is paged forward: pass `since`, then each
        `next_cursor` until it is null. Every page costs the venue only the
        requests that page needs.
        """
        if cursor is not None:
            if not cursor.isdigit():
                raise http_error(400, "validation_error", "cursor: not a cursor from this endpoint", field="cursor")
            since = int(cursor)
        seconds = timeframe_seconds(timeframe)
        candles = exchange.fetch_ohlcv(
            market_id, timeframe=timeframe, since=since, until=until, limit=limit,
        )
        more = since is not None and len(candles) == limit
        next_cursor = str(candles[-1].timestamp + seconds * 1000) if more else None
        return PageResponse[Candle](data=list(candles), next_cursor=next_cursor, count=len(candles))

    # -- reference ----------------------------------------------------------

    @app.get(
        "/venues/{venue}/markets/{market_id}/fee", tags=["reference"],
        summary="Fee schedule, before trading", responses=RESPONSES,
    )
    def get_fee_schedule(exchange: Venue, market_id: str) -> FeeSchedule:
        """What trading this market costs. Kalshi keys fees on the series above
        the market, so this resolves that for you."""
        return exchange.fetch_fee_schedule(market_id)

    @app.get(
        "/venues/{venue}/series/{series_id}", tags=["reference"],
        summary="One series", responses=RESPONSES,
    )
    def get_series(exchange: Venue, series_id: str) -> Series:
        """Kalshi only. Polymarket has no series tier and answers 501."""
        return exchange.fetch_series(series_id)

    return app


def venues_dependency(app: FastAPI) -> Iterator[VenueRegistry]:
    """The registry an app is using, for a host that wants to reach into it."""
    yield app.state.registry
