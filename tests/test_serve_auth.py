"""`synpath serve` requires its access token on every route but `/`, `/health` and the docs."""
from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from serve_harness import serve_harness  # noqa: F401  (the fixture)

MARKET_ROUTES = [
    "/venues",
    "/venues/kalshi",
    "/venues/kalshi/markets",
    "/venues/kalshi/markets/KXHARNESS-1",
    "/venues/kalshi/events",
    "/venues/kalshi/markets/KXHARNESS-1/book",
    "/venues/kalshi/markets/KXHARNESS-1/trades",
    "/venues/kalshi/markets/KXHARNESS-1/candles",
    "/venues/kalshi/markets/KXHARNESS-1/fee",
    "/venues/kalshi/series/KXHARNESS",
    "/venues/nowhere/markets",
]


def test_every_market_data_route_refuses_a_caller_without_a_token(serve_harness):
    with serve_harness() as harness:
        for route in MARKET_ROUTES:
            response = harness.client.get(route)
            assert response.status_code == 401, route
            assert response.json()["error"]["code"] == "unauthorized"
        assert harness.client.get("/venues", headers={"Authorization": "Bearer sk_not_a_key"}).status_code == 401
        assert not harness.kalshi.requests, "an unauthenticated call never reaches the venue"


def test_with_the_token_market_data_is_served(serve_harness):
    with serve_harness() as harness:
        assert harness.client.get("/venues", headers=harness.auth).status_code == 200
        book = harness.client.get("/venues/kalshi/markets/KXHARNESS-1/book", headers=harness.auth)
        assert book.status_code == 200, book.text
        assert book.json()["bids"][0]["price"] == 0.4
        assert harness.client.get("/venues/kalshi/markets/KXHARNESS-1/book",
                                  headers={"X-API-Key": harness.token}).status_code == 200
        assert harness.client.get("/venues/nowhere/markets", headers=harness.auth).status_code == 404


def test_what_this_server_is_and_whether_it_is_up_stay_open(serve_harness):
    with serve_harness() as harness:
        assert harness.client.get("/health").status_code == 200
        index = harness.client.get("/")
        assert index.status_code == 200
        assert index.json()["links"]["market_stream"] == "/ws/market"
        assert harness.client.get("/openapi.json").status_code == 200


def test_trading_still_requires_the_token(serve_harness):
    with serve_harness() as harness:
        assert harness.client.get("/trading/orders").status_code == 401
        assert harness.client.get("/trading/orders", headers=harness.auth).status_code == 200


@pytest.mark.parametrize("credential", ["", "key=sk_not_a_key"])
def test_the_market_socket_closes_4401_without_a_valid_token(serve_harness, credential):
    with serve_harness() as harness:
        with harness.client.websocket_connect(f"/ws/market?market_id=polymarket:1&{credential}") as socket:
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_json()
        assert closed.value.code == 4401
        assert "polymarket" not in harness.opened, "nothing was subscribed upstream"


def test_the_market_socket_takes_the_token_as_a_header_or_a_query(serve_harness):
    with serve_harness() as harness:
        with harness.client.websocket_connect("/ws/market?market_id=polymarket:1", headers=harness.auth) as socket:
            assert socket.receive_json()["type"] == "subscribed"
        with harness.client.websocket_connect(f"/ws/market?market_id=polymarket:1&key={harness.token}") as socket:
            assert socket.receive_json()["type"] == "subscribed"


def test_the_python_client_still_trades_through_the_server(serve_harness):
    with serve_harness() as harness:
        client = harness.remote_client()
        try:
            assert harness.call(client.fetch_open_orders) == []
        finally:
            harness.call(client.close)
