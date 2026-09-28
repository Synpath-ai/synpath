"""The HTTP layer, driven against stub adapters.

Stubs rather than recorded HTTP, because what is under test here is the
translation — request to library call, library exception to status code, page
to envelope — and not the venue. The adapters have their own tests.
"""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="server extra not installed")

from typing import Annotated  # noqa: E402

from fastapi import Depends, FastAPI, Header  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import synpath  # noqa: E402
from synpath import (  # noqa: E402
    BadRequest, Exchange, ExchangeNotAvailable, MarketNotFound, NotSupported,
    Page, RateLimitExceeded, RequestTimeout,
)
from synpath.server.api import VenueRegistry, create_app  # noqa: E402
from synpath.server.errors import code_of  # noqa: E402
from synpath.types import Candle, Market, OrderBook, OrderLevel, Outcome, Quote, Trade  # noqa: E402


def make_market(market_id: str = "KXTEST-1") -> Market:
    return Market(
        id=market_id, venue="stub", venue_market_id=market_id, title="Will it?",
        status="open", active=True,
        yes=Outcome(label="Yes", quote=Quote(bid=0.4, ask=0.42, mid=0.41)),
        no=Outcome(label="No", quote=Quote(bid=0.58, ask=0.6, mid=0.59)),
    )


class StubExchange(Exchange):
    """An adapter that answers from memory, or raises whatever a test sets."""

    id = "stub"
    name = "Stub"
    has = {
        "fetch_markets": True, "fetch_events": True, "fetch_order_book": True,
        "fetch_trades": True, "fetch_ohlcv": True, "fetch_series": False,
        "fetch_fee_schedule": False,
    }

    def __init__(self):
        self.raises: Exception | None = None
        self.calls: list[tuple[str, dict]] = []
        self.next_cursor: str | None = "CURSOR2"

    def _check(self, name: str, **kwargs):
        self.calls.append((name, kwargs))
        if self.raises:
            raise self.raises

    def fetch_markets(self, *, query=None, limit=None, cursor=None, status="open", sort=None):
        self._check("fetch_markets", query=query, limit=limit, cursor=cursor, status=status, sort=sort)
        return Page([make_market()], next_cursor=self.next_cursor)

    def fetch_events(self, *, query=None, limit=None, cursor=None, status="open"):
        self._check("fetch_events", query=query, limit=limit, cursor=cursor, status=status)
        return Page([], next_cursor=None)

    def fetch_market(self, market_id: str):
        self._check("fetch_market", market_id=market_id)
        return make_market(market_id)

    def fetch_order_book(self, market_id: str, *, side="yes", depth=None):
        self._check("fetch_order_book", market_id=market_id, side=side, depth=depth)
        return OrderBook(
            market_id=market_id, side=side, venue="stub",
            bids=[OrderLevel(price=0.4, size=10)],
            asks=[OrderLevel(price=0.42, size=5)],
        )

    def fetch_trades(self, market_id, *, since=None, limit=None, cursor=None):
        self._check("fetch_trades", market_id=market_id, since=since, limit=limit, cursor=cursor)
        return Page(
            [Trade(id="t1", market_id=market_id, timestamp=1, datetime="1970-01-01T00:00:00Z",
                   price=0.4, amount=3)],
            next_cursor="100",
        )

    BARS = [1_700_000_000_000 + n * 3_600_000 for n in range(5)]
    """Five hourly bars the stub's market has."""

    def fetch_ohlcv(self, market_id, *, timeframe="1h", since=None, until=None, limit=None):
        self._check("fetch_ohlcv", market_id=market_id, timeframe=timeframe, since=since, until=until, limit=limit)
        stamps = [t for t in self.BARS if since is None or t >= since]
        stamps = (stamps[:limit] if since is not None else stamps[-limit:]) if limit else stamps
        return [Candle(timestamp=t, datetime="x", close=0.5) for t in stamps]


@pytest.fixture
def stub():
    return StubExchange()


@pytest.fixture
def client(stub, monkeypatch):
    monkeypatch.setitem(synpath.exchanges, "stub", StubExchange)
    app = create_app(registry=VenueRegistry({"stub": stub}))
    with TestClient(app) as client:
        yield client


class TestDiscovery:
    def test_health_needs_no_venue(self, client):
        body = client.get("/health").json()
        assert body["status"] == "ok"
        assert body["version"] == synpath.__version__

    def test_venues_expose_capabilities(self, client):
        rows = {row["id"]: row for row in client.get("/venues").json()}
        assert rows["kalshi"]["has"]["fetch_ohlcv"] is True
        assert rows["polymarket"]["has"]["fetch_ohlcv"] is True
        assert rows["polymarket_us"]["has"]["fetch_ohlcv"] == "partial"
        assert rows["polymarket"]["has"]["fetch_series"] is False

    def test_book_model_is_advertised(self, client):
        rows = {row["id"]: row for row in client.get("/venues").json()}
        assert rows["kalshi"]["book_model"] == "shared_complement"
        assert rows["polymarket"]["book_model"] == "native_per_outcome"

    def test_unknown_venue_lists_the_known_ones(self, client):
        response = client.get("/venues/betfair/markets")
        assert response.status_code == 404
        error = response.json()["error"]
        assert error["code"] == "unknown_venue" and "kalshi" in error["message"]


class TestPassthrough:
    """Query parameters have to reach the library unchanged."""

    def test_market_query_params(self, client, stub):
        client.get("/venues/stub/markets", params={
            "query": "fed", "limit": 7, "cursor": "ABC", "status": "closed", "sort": "volume",
        })
        name, kwargs = stub.calls[-1]
        assert name == "fetch_markets"
        assert kwargs == {"query": "fed", "limit": 7, "cursor": "ABC", "status": "closed", "sort": "volume"}

    def test_book_depth(self, client, stub):
        client.get("/venues/stub/markets/KXTEST-1/book", params={"depth": 3})
        assert stub.calls[-1][1]["depth"] == 3

    def test_trades_since_is_milliseconds(self, client, stub):
        client.get("/venues/stub/markets/KXTEST-1/trades", params={"since": 1789471790532})
        assert stub.calls[-1][1]["since"] == 1789471790532

    def test_market_id_with_colon_survives_the_url(self, client, stub):
        client.get("/venues/stub/markets/stub:KXTEST-1/book", params={"side": "no"})
        assert stub.calls[-1][1]["market_id"] == "stub:KXTEST-1" and stub.calls[-1][1]["side"] == "no"


class TestEnvelope:
    def test_list_carries_cursor_and_count(self, client):
        body = client.get("/venues/stub/markets").json()
        assert body["count"] == len(body["data"]) == 1
        assert body["next_cursor"] == "CURSOR2"

    def test_exhausted_page_has_null_cursor(self, client):
        body = client.get("/venues/stub/events").json()
        assert body["next_cursor"] is None
        assert body["data"] == []

    def test_single_resource_is_not_enveloped(self, client):
        body = client.get("/venues/stub/markets/KXTEST-1").json()
        assert body["id"] == "KXTEST-1"
        assert "data" not in body

    def test_nulls_survive_serialisation(self, client):
        """A missing price has to stay null over the wire, not become 0."""
        body = client.get("/venues/stub/markets/KXTEST-1").json()
        quote = body["yes"]["quote"]
        assert quote["last"] is None
        assert quote["bid"] == 0.4

    def test_response_matches_the_library_type(self, client):
        body = client.get("/venues/stub/markets/KXTEST-1").json()
        assert Market.model_validate(body) == make_market()


class TestErrorMapping:
    @pytest.mark.parametrize("error, status, retryable", [
        (MarketNotFound("stub: gone"), 404, False),
        (BadRequest("stub: bad"), 400, False),
        (NotSupported("stub: nope"), 501, False),
        (RateLimitExceeded("stub: slow down"), 429, True),
        (RequestTimeout("stub: timeout"), 504, True),
        (ExchangeNotAvailable("stub: down"), 502, True),
    ])
    def test_status_codes(self, client, stub, error, status, retryable):
        stub.raises = error
        response = client.get("/venues/stub/markets")
        assert response.status_code == status
        error_body = response.json()["error"]
        assert set(error_body) == {"code", "message", "details"}
        assert error_body["code"] == code_of(error)
        assert error_body["details"]["retryable"] is retryable

    def test_venue_is_named_in_the_body(self, client, stub):
        stub.raises = MarketNotFound("kalshi: no market NOPE")
        assert client.get("/venues/stub/markets").json()["error"]["details"]["venue"] == "kalshi"

    def test_retry_after_becomes_a_header(self, client, stub):
        stub.raises = RateLimitExceeded("stub: slow down", retry_after=2)
        response = client.get("/venues/stub/markets")
        assert response.headers["retry-after"] == "2"
        assert response.json()["error"]["details"]["retry_after"] == 2

    def test_unsupported_capability_is_501_not_404(self, client):
        """404 would say the id was wrong. 501 says no id would have worked."""
        response = client.get("/venues/stub/series/ABC")
        assert response.status_code == 501
        assert response.json()["error"]["code"] == "not_supported"

    def test_invalid_limit_is_rejected(self, client):
        refused = client.get("/venues/stub/markets", params={"limit": 0})
        assert refused.status_code == 422
        error = refused.json()["error"]
        assert error["code"] == "validation_error" and error["details"]["errors"][0]["field"] == "query.limit"
        assert client.get("/venues/stub/markets", params={"limit": 9999}).status_code == 422


class TestSchema:
    """The OpenAPI document is the contract generated clients are built from."""

    @pytest.fixture
    def spec(self, client):
        return client.get("/openapi.json").json()

    def test_every_route_is_documented(self, spec):
        for path in (
            "/venues", "/venues/{venue}/markets", "/venues/{venue}/markets/{market_id}",
            "/venues/{venue}/events", "/venues/{venue}/markets/{market_id}/book",
            "/venues/{venue}/markets/{market_id}/trades",
            "/venues/{venue}/markets/{market_id}/candles",
            "/venues/{venue}/markets/{market_id}/fee", "/venues/{venue}/series/{series_id}",
        ):
            assert path in spec["paths"], path

    def test_responses_are_typed_not_object(self, spec):
        """Dynamic dispatch would make every response `object` and every
        generated client untyped. These have real schemas."""
        for name in ("Market", "OrderBook", "Trade", "Candle", "Quote", "FeeSchedule"):
            assert name in spec["components"]["schemas"], name

    def test_market_response_references_the_market_schema(self, spec):
        content = spec["paths"]["/venues/{venue}/markets/{market_id}"]["get"]
        schema = content["responses"]["200"]["content"]["application/json"]["schema"]
        assert schema["$ref"].endswith("/Market")

    def test_errors_are_documented_per_route(self, spec):
        responses = spec["paths"]["/venues/{venue}/markets"]["get"]["responses"]
        for status in ("400", "404", "429", "501", "502", "504"):
            assert status in responses, status
        error = responses["404"]["content"]["application/json"]["schema"]
        assert error["$ref"].endswith("/ErrorBody")

    def test_quote_fields_are_nullable_in_the_schema(self, spec):
        """A generated client must know a price can be absent."""
        quote = spec["components"]["schemas"]["Quote"]["properties"]["bid"]
        assert "null" in str(quote)

    def test_paths_carry_no_auth_scheme(self, spec):
        """Auth belongs to whatever mounts this, never to the core app."""
        assert "securitySchemes" not in spec.get("components", {})
        assert "security" not in spec


class TestMounting:
    def test_router_mounts_under_a_host_app_with_auth(self, stub, monkeypatch):
        """The shape a hosted deployment actually uses."""
        monkeypatch.setitem(synpath.exchanges, "stub", StubExchange)
        seen: list[str] = []

        def require_key(x_api_key: Annotated[str, Header()] = ""):
            seen.append(x_api_key)
            if x_api_key != "secret":
                from fastapi import HTTPException

                raise HTTPException(status_code=401, detail="bad key")

        registry = VenueRegistry({"stub": stub})
        inner = create_app(registry=registry, docs=False)
        outer = FastAPI()
        # A mounted route is served by the host app, so the registry has to
        # live there for the host's own adapters to be the ones used.
        outer.state.registry = registry
        outer.include_router(inner.router, dependencies=[Depends(require_key)])

        with TestClient(outer) as client:
            assert client.get("/venues/stub/markets").status_code == 401
            response = client.get("/venues/stub/markets", headers={"x-api-key": "secret"})
            assert response.status_code == 200
            assert response.json()["count"] == 1
        assert seen
        # The injected adapter did the work — not a fresh one built by a
        # fallback registry, which would look identical in the response.
        assert [name for name, _ in stub.calls] == ["fetch_markets"]

    def test_docs_can_be_switched_off(self, stub, monkeypatch):
        monkeypatch.setitem(synpath.exchanges, "stub", StubExchange)
        app = create_app(registry=VenueRegistry({"stub": stub}), docs=False)
        with TestClient(app) as client:
            assert client.get("/openapi.json").status_code == 404


class TestRegistry:
    def test_adapters_are_reused_across_requests(self, client, stub):
        """A fresh adapter per request would hand every request a full rate
        limit budget and blow straight past the venue's ceiling."""
        registry = VenueRegistry()
        first = registry.get("kalshi")
        assert registry.get("kalshi") is first
        registry.close()

    def test_close_empties_the_registry(self):
        registry = VenueRegistry()
        registry.get("kalshi")
        registry.close()
        assert registry._exchanges == {}


class TestRouting:
    """Regression: `market_id` once used a greedy path converter, so
    `/markets/ABC/trades` resolved to the single-market route with an id of
    `ABC/trades` — a 404 on a request that should have listed trades."""

    @pytest.mark.parametrize("path, endpoint", [
        ("/venues/kalshi/markets/KXFOO-25", "get_market"),
        ("/venues/kalshi/markets/KXFOO-25/trades", "list_trades"),
        ("/venues/kalshi/markets/KXFOO-25/fee", "get_fee_schedule"),
        ("/venues/kalshi/markets/KXFOO-25/book", "get_order_book"),
        ("/venues/kalshi/markets/KXFOO-25/candles", "list_candles"),
    ])
    def test_suffixes_are_not_swallowed(self, client, path, endpoint):
        app = client.app
        for route in app.routes:
            match, scope = route.matches(
                {"type": "http", "method": "GET", "path": path, "path_params": {}}
            )
            if match.value == 2:
                assert route.endpoint.__name__ == endpoint
                assert "/" not in scope["path_params"].get("market_id", "")
                return
        pytest.fail(f"no route matched {path}")


class TestLifespan:
    """Regression: `include_router` merges lifespan contexts, so a mounted
    router ran this app's shutdown against the *host's* app object. Reading the
    registry off that app raised AttributeError and closed nothing."""

    def test_mounted_shutdown_closes_its_own_registry(self, stub, monkeypatch):
        monkeypatch.setitem(synpath.exchanges, "stub", StubExchange)
        closed: list[str] = []

        class TracedRegistry(VenueRegistry):
            def close(self):
                closed.append("closed")
                super().close()

        registry = TracedRegistry({"stub": stub})
        inner = create_app(registry=registry, docs=False)
        outer = FastAPI()
        outer.state.registry = registry
        outer.include_router(inner.router)

        with TestClient(outer) as client:
            assert client.get("/health").status_code == 200
        assert closed == ["closed"]


class TestRegistryConcurrency:
    """Regression: lazy construction was a check-then-set with no lock. Two
    cold requests both built an adapter, one was dropped still holding an open
    connection pool, and for that moment two rate limiters each thought they
    owned the venue's whole budget."""

    def test_concurrent_first_requests_share_one_adapter(self):
        from concurrent.futures import ThreadPoolExecutor

        registry = VenueRegistry()
        try:
            with ThreadPoolExecutor(max_workers=16) as pool:
                adapters = list(pool.map(lambda _: registry.get("kalshi"), range(64)))
            assert len({id(adapter) for adapter in adapters}) == 1
        finally:
            registry.close()

    def test_close_is_safe_while_others_read(self):
        registry = VenueRegistry()
        registry.get("kalshi")
        registry.close()
        assert registry._exchanges == {}


class TestCandlePaging:
    """A long history is paged forward from `since`, a page per call, rather
    than fetched whole and cut down to the newest `limit` bars."""

    def test_pages_forward_until_the_cursor_is_null(self, client, stub):
        seen, params = [], {"timeframe": "1h", "since": StubExchange.BARS[0], "limit": 2}
        while True:
            body = client.get("/venues/stub/markets/KXTEST-1/candles", params=params).json()
            seen += [bar["timestamp"] for bar in body["data"]]
            if body["next_cursor"] is None:
                break
            params = {"timeframe": "1h", "limit": 2, "cursor": body["next_cursor"]}
        assert seen == StubExchange.BARS

    def test_the_cursor_is_the_next_since(self, client, stub):
        body = client.get("/venues/stub/markets/KXTEST-1/candles",
                          params={"since": StubExchange.BARS[0], "limit": 2}).json()
        client.get("/venues/stub/markets/KXTEST-1/candles", params={"cursor": body["next_cursor"], "limit": 2})
        assert stub.calls[-1][1]["since"] == StubExchange.BARS[2]

    def test_without_since_there_is_no_cursor(self, client):
        body = client.get("/venues/stub/markets/KXTEST-1/candles", params={"limit": 2}).json()
        assert [bar["timestamp"] for bar in body["data"]] == StubExchange.BARS[-2:]
        assert body["next_cursor"] is None

    def test_a_foreign_cursor_is_refused(self, client):
        response = client.get("/venues/stub/markets/KXTEST-1/candles", params={"cursor": "abc"})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "validation_error"
