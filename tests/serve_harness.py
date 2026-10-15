"""`synpath serve`, whole, in this process: mocked venues, fake streams, a real HTTP surface.

`ServeHarness` builds `serve.build()` with

* a **mocked Kalshi**: the real `KalshiTrading` adapter and the real `Kalshi`
  read adapter, both on `httpx.MockTransport` (`MockKalshi`), so orders and
  market-data routes run the library's own request and parsing code; other
  venues' read adapters (or stand-ins) can be added with `exchanges=`;
* other venues' **trading adapters** (`trading=`, a `PaperVenue` say), so a
  bucket across venues can be defined;
* a **mocked hosted API** (`MockHosted`) behind the matching and history
  routes;
* **fake venue streams** (`FakeVenueStream`) for the engine's feeds and for
  `/ws/market`, which the test drives by pushing the library's own events;
* a temporary journal (SQLite, or PostgreSQL when `SYNPATH_TEST_POSTGRES_DSN`
  names a disposable database), control database and home directory.

Everything runs on one event loop in a background thread (an anyio blocking
portal), and a Starlette `TestClient` is pointed at that same loop, so plain
synchronous tests drive REST and WebSockets while the engine, the feeds and
the hub run where they would in production. Nothing touches the network.

```python
def test_something(serve_harness):
    with serve_harness() as harness:
        harness.client.get("/venues", headers=harness.auth)
        with harness.client.websocket_connect(f"/ws/market?key={harness.token}&market_id=polymarket:1") as ws:
            harness.push("polymarket", harness.stream("polymarket").snapshot("polymarket:1", bids=[("0.4", "10")]))
```
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from decimal import Decimal as D
from pathlib import Path
from typing import Any, Callable, Iterable

import httpx
import pytest
from anyio.from_thread import start_blocking_portal

import synpath
from synpath.engine.feeds import VenueStreams
from synpath.server import serve
from synpath.server.api import VenueRegistry
from synpath.trading.credentials import KalshiCredentials
from synpath.trading.kalshi import KalshiTrading
from synpath.ws.base import BookEvent, BookLevel, LocalBook, QuoteEvent, StreamStatusEvent, TradeEvent

POSTGRES_DSN_VARIABLE = "SYNPATH_TEST_POSTGRES_DSN"
KALSHI_TRADE_API = "https://external-api.kalshi.com/trade-api/v2"
PUBLIC_STREAM_VENUES = ("polymarket", "hyperliquid", "limitless")
"""Venues whose market stream needs no credentials; the fake factory opens these by default."""
VENUES_WITHOUT_TRADE_STREAMS = ("limitless", "predict_fun")


# ---------------------------------------------------------------------------
# A mocked Kalshi
# ---------------------------------------------------------------------------

def kalshi_credentials() -> KalshiCredentials:
    """A throwaway RSA key: requests are signed for real, and go nowhere."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    return KalshiCredentials(key_id="harness-key", private_key_pem=pem, env="demo")


class MockKalshi:
    """Kalshi's REST API, enough for the read routes and an empty account.
    Every request is recorded; anything unexpected answers 404."""

    def __init__(self, *, book: dict[str, list[list[str]]] | None = None):
        self.requests: list[httpx.Request] = []
        self.book = book or {"yes_dollars": [["0.40", "10"]], "no_dollars": [["0.55", "7"]]}

    @staticmethod
    def market(ticker: str) -> dict[str, Any]:
        return {"ticker": ticker, "event_ticker": "KXHARNESS", "status": "active", "title": "Harness market",
                "price_level_structure": "linear_cent", "yes_bid_dollars": "0.40", "yes_ask_dollars": "0.45"}

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path.removeprefix("/trade-api/v2")
        parts = path.strip("/").split("/")
        if request.method == "GET" and parts[:1] == ["markets"] and len(parts) == 3 and parts[2] == "orderbook":
            return httpx.Response(200, json={"orderbook_fp": self.book})
        if request.method == "GET" and parts[:1] == ["markets"] and len(parts) == 2:
            return httpx.Response(200, json={"market": self.market(parts[1])})
        if request.method == "GET" and path in ("/markets", "/historical/markets") and request.url.params.get("tickers"):
            # The batch read: only `KXHARNESS*` tickers exist, and only on the live endpoint.
            tickers = request.url.params["tickers"].split(",")
            found = [self.market(t) for t in tickers if t.startswith("KXHARNESS")] if path == "/markets" else []
            return httpx.Response(200, json={"markets": found, "cursor": ""})
        if request.method == "GET" and parts[:1] == ["series"]:
            return httpx.Response(200, json={"series": {"ticker": parts[-1], "fee_type": "quadratic",
                                                        "fee_multiplier": 1}})
        if request.method == "GET" and path == "/portfolio/orders":
            return httpx.Response(200, json={"orders": [], "cursor": ""})
        if request.method == "GET" and path == "/portfolio/balance":
            return httpx.Response(200, json={"balance": 1_000_000, "portfolio_value": 0})
        if request.method == "GET" and path in ("/portfolio/positions", "/portfolio/fills", "/portfolio/settlements"):
            key = {"/portfolio/positions": "market_positions", "/portfolio/fills": "fills",
                   "/portfolio/settlements": "settlements"}[path]
            return httpx.Response(200, json={key: [], "event_positions": [], "cursor": ""})
        return httpx.Response(404, json={"error": {"code": "not_found", "message": f"harness: {request.method} {path}"}})


class MockHosted:
    """Synpath's hosted services (matching at api.synpath.dev, history at
    api2.synpath.dev), answering from `routes`: `(method, path)` -> a JSON body,
    or `(status, body)`. Every request is recorded; an unknown route answers
    404 with no Synpath error body, as the host in front of an undeployed
    service does."""

    def __init__(self, routes: dict[tuple[str, str], Any] | None = None):
        self.requests: list[httpx.Request] = []
        self.routes: dict[tuple[str, str], Any] = dict(routes or {})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.routes.get((request.method, request.url.path))
        if answer is None:
            return httpx.Response(404, text="not found")
        status, body = answer if isinstance(answer, tuple) else (200, answer)
        return httpx.Response(status, json=body)


# ---------------------------------------------------------------------------
# A fake venue stream
# ---------------------------------------------------------------------------

class FakeVenueStream:
    """Stands in for a library market stream (`KalshiStream`,
    `PolymarketMarketStream`, …): the same `watch_*`, `unwatch`, `book()`,
    `has` and async iteration, with `LocalBook`s the test fills through
    `snapshot()`/`delta()`. Events reach subscribers only when pushed."""

    def __init__(self, venue: str, *, private: bool = False, unwatch: bool = True, trades: bool = True):
        self.venue, self.name = venue, f"fake-{venue}"
        self.has = {"watch_order_book": True, "watch_ticker": True, "watch_trades": trades,
                    "watch_orders": private, "watch_my_trades": private}
        if not unwatch:
            self.unwatch = None  # Kalshi and Polymarket US cannot unwatch
        self.books: dict[str, LocalBook] = {}
        self.watched: list[tuple[str, tuple[str, ...]]] = []
        self.unwatched: list[str] = []
        self.subscribed: list[str] = []
        self.queue: asyncio.Queue = asyncio.Queue()
        self.started = self.closed = False

    # -- the stream interface -------------------------------------------------

    def start(self) -> "FakeVenueStream":
        self.started = True
        return self

    async def close(self) -> None:
        self.closed = True
        self.queue.put_nowait(None)

    async def watch_order_book(self, market_ids: list[str]) -> None:
        self.watched.append(("book", tuple(market_ids)))
        for market_id in market_ids:
            self.books.setdefault(market_id, LocalBook())

    async def watch_trades(self, market_ids: list[str]) -> None:
        self.watched.append(("trades", tuple(market_ids)))

    async def watch_ticker(self, market_ids: list[str]) -> None:
        self.watched.append(("quotes", tuple(market_ids)))

    async def unwatch(self, market_ids: list[str]) -> None:  # type: ignore[no-redef]
        self.unwatched += market_ids
        for market_id in market_ids:
            self.books.pop(market_id, None)

    async def watch_orders(self, market_ids: list[str] | None = None) -> None:
        self.subscribed.append("orders")

    async def watch_my_trades(self, market_ids: list[str] | None = None) -> None:
        self.subscribed.append("fills")

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(market_id)
        return book.mirrored() if book is not None and side == "no" else book

    def __aiter__(self) -> "FakeVenueStream":
        return self

    async def __anext__(self) -> Any:
        event = await self.queue.get()
        if event is None:
            raise StopAsyncIteration
        return event

    # -- what a test pushes -------------------------------------------------

    def push(self, event: Any) -> None:
        self.queue.put_nowait(event)

    def snapshot(self, market_id: str, *, bids: Iterable[tuple[str, str]] = (),
                 asks: Iterable[tuple[str, str]] = ()) -> BookEvent:
        """Replace the book, as a venue snapshot does, and return its event."""
        bids, asks = [(D(p), D(s)) for p, s in bids], [(D(p), D(s)) for p, s in asks]
        book = self.books.setdefault(market_id, LocalBook())
        book.replace(bids, asks)
        return BookEvent(venue=self.venue, market_id=market_id, kind="snapshot",
                         bids=tuple(BookLevel(p, s) for p, s in bids), asks=tuple(BookLevel(p, s) for p, s in asks),
                         best_bid=book.best_bid, best_ask=book.best_ask)

    def delta(self, market_id: str, side: str, price: str, size: str) -> BookEvent:
        book = self.books[market_id]
        book.set(side, D(price), D(size))
        level = (BookLevel(D(price), D(size)),)
        return BookEvent(venue=self.venue, market_id=market_id, kind="delta",
                         bids=level if side == "bid" else (), asks=level if side == "ask" else (),
                         best_bid=book.best_bid, best_ask=book.best_ask)

    def trade(self, market_id: str, price: str, quantity: str) -> TradeEvent:
        return TradeEvent(venue=self.venue, market_id=market_id, id=f"t-{uuid.uuid4().hex[:6]}",
                          price=D(price), quantity=D(quantity), timestamp=int(time.time() * 1000))

    def quote(self, market_id: str, bid: str, ask: str, side: str = "yes") -> QuoteEvent:
        return QuoteEvent(venue=self.venue, market_id=market_id, side=side, bid=D(bid), ask=D(ask))

    def drop(self) -> StreamStatusEvent:
        """The venue socket dropped: every book stops being trusted, as
        `Stream.on_disconnect` does."""
        for book in self.books.values():
            book.invalidate()
        return StreamStatusEvent(venue=self.venue, stream=self.name, state="disconnected", detail="harness drop")

    def reconnected(self) -> StreamStatusEvent:
        return StreamStatusEvent(venue=self.venue, stream=self.name, state="connected", detail="reconnected")


# ---------------------------------------------------------------------------
# The harness
# ---------------------------------------------------------------------------

class ServeHarness:
    """A built `synpath serve` stack on a background event loop. Use as a
    context manager; `client`, `token` and `auth` are ready inside it."""

    def __init__(self, tmp_path: Path, *, storage: str = "sqlite", postgres_dsn: str | None = None,
                 stream_venues: Iterable[str] = PUBLIC_STREAM_VENUES, engine_stream: bool = True,
                 config: dict[str, Any] | None = None, kalshi: MockKalshi | None = None,
                 exchanges: dict[str, Any] | None = None, hosted: "MockHosted | None" = None,
                 trading: dict[str, Any] | None = None):
        self.tmp_path = tmp_path
        self.storage = storage
        self.postgres_dsn = postgres_dsn
        self.postgres_schema = "synpath_harness_" + uuid.uuid4().hex if storage == "postgresql" else None
        self.stream_venues = set(stream_venues)
        self.kalshi = kalshi or MockKalshi()
        self.exchanges = dict(exchanges or {})
        """Read adapters (or stand-ins) for venues other than the mocked Kalshi."""
        self.hosted = hosted or MockHosted()
        self.trading = dict(trading or {})
        """Trading adapters for venues other than the mocked Kalshi (a `PaperVenue`, say)."""
        self.engine_stream = FakeVenueStream("kalshi", private=True, unwatch=False) if engine_stream else None
        self.opened: dict[str, list[FakeVenueStream]] = {}
        """Market streams the hub opened itself, by venue, oldest first."""
        self.config = {"risk": {"price_collar": None, "duplicate_window_ms": 0, "closing_soon_s": None},
                       "book_interval_s": 0, **(config or {})}
        self.portal: Any = None
        self._portal_context: Any = None
        self.stack: Any = None
        self.client: Any = None
        self._loops: list[Any] = []

    # -- what tests use -------------------------------------------------------

    @property
    def token(self) -> str:
        return self.stack.owner_key

    @property
    def auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def call(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Run `fn(*args)` (sync or async) on the server's event loop."""
        return self.portal.call(fn, *args)

    def stream(self, venue: str, index: int = -1) -> FakeVenueStream:
        """The fake stream the server reads `venue` from: the engine's for
        Kalshi, otherwise the latest one the hub opened."""
        if venue == "kalshi" and self.engine_stream is not None:
            return self.engine_stream
        return self.opened[venue][index]

    def push(self, venue: str, event: Any) -> None:
        self.call(self.stream(venue).push, event)

    def wait_until(self, condition: Callable[[], bool], timeout: float = 3.0) -> None:
        """Poll `condition` on the server's loop until true."""
        deadline = time.monotonic() + timeout
        while not self.call(condition):
            if time.monotonic() > deadline:
                raise AssertionError("the server did not get there in time")
            time.sleep(0.01)

    def remote_client(self) -> synpath.Client:
        """`synpath.Client(server=...)` against this stack, in process."""
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.stack.app))
        return synpath.Client(server="http://synpath", access_token=self.token, server_http=http)

    # -- building and tearing down --------------------------------------------

    def __enter__(self) -> "ServeHarness":
        from fastapi.testclient import TestClient

        self._portal_context = start_blocking_portal("asyncio")
        self.portal = self._portal_context.__enter__()
        try:
            self.stack = self.call(self._build)
            self._loops = [self.portal.start_task_soon(lambda c=coro: c) for coro in self._feed_loops()]
            self.client = TestClient(self.stack.app, base_url="http://synpath")
            self.client.portal = self.portal   # requests and sockets run on the server's loop
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            for future in self._loops:
                future.cancel()
            if self.stack is not None:
                self.call(serve.shutdown, self.stack)
            if self.postgres_schema is not None:
                self.call(self._drop_postgres_schema)
        finally:
            self._portal_context.__exit__(None, None, None)

    async def _build(self) -> Any:
        credentials = kalshi_credentials()
        trading_http = httpx.AsyncClient(transport=httpx.MockTransport(self.kalshi.handle))
        adapters = {"kalshi": KalshiTrading(credentials, base_url=KALSHI_TRADE_API, client=trading_http),
                    **self.trading}
        read_http = httpx.Client(transport=httpx.MockTransport(self.kalshi.handle))
        registry = VenueRegistry({"kalshi": synpath.exchange("kalshi", base_url=KALSHI_TRADE_API,
                                                             client=read_http, limiter=None), **self.exchanges})
        stream_map = {"kalshi": VenueStreams(market=self.engine_stream, private=self.engine_stream)} \
            if self.engine_stream is not None else {}
        return await serve.build(
            config={**self.config, **self._storage_config()},
            journal=str(self.tmp_path / "serve.db"), control=str(self.tmp_path / "control.db"),
            adapters=adapters, stream_map=stream_map, registry=registry,
            market_stream_factory=self._open_fake_stream, home_dir=str(self.tmp_path / "home"),
            hosted_http=httpx.Client(transport=httpx.MockTransport(self.hosted.handle)),
        )

    def _storage_config(self) -> dict[str, Any]:
        if self.storage == "sqlite":
            return {}
        return {"storage_backend": "postgresql", "journal": self.postgres_dsn, "storage_schema": self.postgres_schema}

    def _open_fake_stream(self, venue: str) -> FakeVenueStream | None:
        if venue not in self.stream_venues:
            return None
        # Like the real ones, the Limitless and predict.fun streams carry no trades.
        stream = FakeVenueStream(venue, trades=venue not in VENUES_WITHOUT_TRADE_STREAMS)
        self.opened.setdefault(venue, []).append(stream)
        return stream

    def _feed_loops(self) -> list[Any]:
        """The feeds' loops (pumps and subscription sync), not the whole of
        `serve.background`: the poll, reconcile and status loops would only
        add mocked venue traffic no test here reads."""
        if self.stack.feeds is None:
            return []
        self.stack.feeds.sync_interval_s = 0.05
        return list(self.stack.feeds.background().values())

    async def _drop_postgres_schema(self) -> None:
        import psycopg
        from psycopg import sql

        async with await psycopg.AsyncConnection.connect(self.postgres_dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(self.postgres_schema)))


STORAGE_BACKENDS = ["sqlite", "postgresql"]


@pytest.fixture(params=STORAGE_BACKENDS)
def serve_harness(request: Any, tmp_path: Path) -> Callable[..., ServeHarness]:
    """A factory for `ServeHarness` on each storage backend. PostgreSQL runs
    only when `SYNPATH_TEST_POSTGRES_DSN` names a disposable database, as the
    other PostgreSQL tests do; otherwise that case is skipped."""
    storage = request.param
    dsn = os.environ.get(POSTGRES_DSN_VARIABLE)
    if storage == "postgresql":
        if not dsn:
            pytest.skip(f"set {POSTGRES_DSN_VARIABLE} to a disposable PostgreSQL database")
        pytest.importorskip("psycopg")

    def make(**kwargs: Any) -> ServeHarness:
        return ServeHarness(tmp_path, storage=storage, postgres_dsn=dsn, **kwargs)

    return make


def receive_until(socket: Any, kind: str, *, limit: int = 20, **fields: Any) -> dict[str, Any]:
    """The next message of `type == kind` (and matching `fields`), skipping others."""
    for _ in range(limit):
        message = socket.receive_json()
        if message.get("type") == kind and all(message.get(k) == v for k, v in fields.items()):
            return message
    raise AssertionError(f"no {kind} message {fields or ''} in {limit} frames")


__all__ = ["FakeVenueStream", "MockHosted", "MockKalshi", "ServeHarness", "serve_harness", "receive_until"]
