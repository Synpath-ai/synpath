"""Bounded stop-limit lifecycle through the real Kalshi adapter, mocked HTTP."""
from __future__ import annotations

import json
from decimal import Decimal as D

import httpx
import pytest

from examples.verify_kalshi_stop_limit import MARKET, TICKER, OneOrderGuard, verify
from synpath.errors import BadRequest
from synpath.trading.kalshi import KalshiTrading
from test_kalshi_trading import creds, pem, rsa_key

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class MockKalshi:
    def __init__(self, *, lose_response=False, delayed_reads=0):
        self.orders = {}
        self.creates = []
        self.cancels = []
        self.lose_response = lose_response
        self.delayed_reads = delayed_reads

    def handle(self, request):
        path = request.url.path.removeprefix("/trade-api/v2")
        if request.method == "GET" and path == f"/markets/{TICKER}":
            return httpx.Response(200, json={"market": {"status": "active", "price_level_structure": "linear_cent"}})
        if request.method == "GET" and path == "/series/KXFEDDECISION":
            return httpx.Response(200, json={"series": {"fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1}})
        if request.method == "GET" and path == f"/markets/{TICKER}/orderbook":
            return httpx.Response(200, json={"orderbook_fp": {"yes_dollars": [["0.53", "20"]], "no_dollars": [["0.44", "20"]]}})
        if request.method == "POST" and path == "/portfolio/events/orders":
            body = json.loads(request.content)
            self.creates.append(body)
            self.orders["test-child"] = {
                "order_id": "test-child", "client_order_id": body["client_order_id"], "ticker": TICKER,
                "book_side": body["side"], "type": "limit", "status": "resting", "initial_count_fp": body["count"],
                "fill_count_fp": "0", "remaining_count_fp": body["count"], "yes_price_dollars": body["price"],
            }
            if self.lose_response:
                raise httpx.ReadError("accepted but response lost", request=request)
            return httpx.Response(201, json={"order_id": "test-child", "client_order_id": body["client_order_id"],
                                           "fill_count": "0", "remaining_count": body["count"]})
        if request.method == "DELETE" and path == "/portfolio/events/orders/test-child":
            self.cancels.append("test-child")
            self.orders["test-child"].update(status="canceled", remaining_count_fp="0")
            return httpx.Response(200, json={"order_id": "test-child", "reduced_by": "1"})
        if request.method == "GET" and path == "/portfolio/orders/test-child":
            if self.delayed_reads:
                self.delayed_reads -= 1
                return httpx.Response(404, json={"error": {"code": "not_found", "message": "read projection not ready"}})
            return httpx.Response(200, json={"order": self.orders["test-child"]})
        if request.method == "GET" and path == "/portfolio/orders":
            return httpx.Response(200, json={"orders": list(self.orders.values()), "cursor": ""})
        raise AssertionError(f"unexpected mock call: {request.method} {path}")


@pytest.mark.parametrize("delayed_reads", [0, 4])
async def test_stop_limit_adapter_arms_restores_triggers_and_cleans_only_its_child(creds, tmp_path, delayed_reads):
    venue, guard = MockKalshi(delayed_reads=delayed_reads), OneOrderGuard()
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check]}) as client:
        async with KalshiTrading(creds, base_url="https://external-api.kalshi.com/trade-api/v2", client=client) as adapter:
            report = await verify(adapter, guard, tmp_path)
    assert report["waiting_restart_verified"] and report["terminal_restart_verified"]
    assert report["trigger_and_child_verified"] and report["cleanup_verified"]
    assert report["filled"] == "0" and report["parent_status"] == "canceled"
    assert len(venue.creates) == 1 and venue.cancels == ["test-child"]
    sent = venue.creates[0]
    assert sent["price"] == "0.0100" and sent["count"] == "1.00" and sent["side"] == "bid"
    assert sent["post_only"] is True and sent["expiration_time"] > 0
    assert sent["time_in_force"] == "good_till_canceled"


async def test_response_lost_still_finds_and_cancels_only_the_accepted_child(creds, tmp_path):
    venue, guard = MockKalshi(lose_response=True), OneOrderGuard()
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check]}) as client:
        async with KalshiTrading(creds, base_url="https://external-api.kalshi.com/trade-api/v2", client=client) as adapter:
            with pytest.raises(RuntimeError, match="trigger did not create"):
                await verify(adapter, guard, tmp_path)
    report = json.loads((tmp_path / "result.json").read_text())
    assert report["cleanup_verified"] and report["venue_status"] == "canceled"
    assert len(venue.creates) == 1 and venue.cancels == ["test-child"]


@pytest.mark.parametrize("change", [{"count": "2"}, {"price": "0.02"}, {"side": "ask"},
                                    {"post_only": False}, {"reduce_only": True}, {"subaccount": 1}])
async def test_wire_guard_refuses_any_broader_order(change):
    guard = OneOrderGuard()
    guard.enabled = True
    body = dict(ticker=TICKER, side="bid", count="1", price="0.01", post_only=True,
                time_in_force="good_till_canceled", expiration_time=int(guard.created_at + 85), client_order_id="tiny-test")
    body.update(change)
    with pytest.raises(ValueError, match="unexpected size"):
        await guard.check(httpx.Request("POST", "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders", json=body))
    assert guard.sent is None


async def test_wire_guard_refuses_repeat_submission_and_unrelated_cancel():
    guard = OneOrderGuard()
    guard.enabled = True
    request = httpx.Request("POST", "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders",
                            json=dict(ticker=TICKER, side="bid", count="1", price="0.01", post_only=True,
                                      time_in_force="good_till_canceled",
                                      expiration_time=int(guard.created_at + 85), client_order_id="tiny-test"))
    await guard.check(request)
    with pytest.raises(ValueError, match="single-order authorization"):
        await guard.check(request)
    with pytest.raises(ValueError, match="single-order authorization"):
        await guard.check(httpx.Request("DELETE", "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders/someone-elses"))


@pytest.mark.parametrize("tif", ["ioc", "fok"])
def test_a_stop_limit_cannot_be_immediate_at_all(tif):
    """One time in force for the whole order: a stop that must live until it
    triggers cannot be immediate-or-cancel, post-only or not."""
    from pydantic import ValidationError
    from synpath.trading.types import OrderRequest
    for post_only in (True, False):
        with pytest.raises(ValidationError, match="how long the whole order lives"):
            OrderRequest(market_id=MARKET, side="buy", quantity="1", type="stop_limit",
                         stop_price="0.01", price="0.01", post_only=post_only, time_in_force=tif)
