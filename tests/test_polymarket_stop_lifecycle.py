"""Synthetic stops through the real Polymarket adapter over mocked HTTP.

Only public test signing keys are used. No wallet or live venue is contacted.
"""
import json
from decimal import Decimal as D
from types import SimpleNamespace

import httpx
import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.trading.types import OrderRequest, OrderStatus
from test_polymarket_trading import venue, adapter, MARKET, TOKEN, NO_TOKEN, DOC_ORDER

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def engine_for(poly, path):
    return Engine({"polymarket": poly}, EngineConfig(journal_path=str(path), user_id="mock-poly-owner"),
                  risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))


@pytest.mark.parametrize("side,leg,token_id,wire_side,maker,taker", [
    ("buy", None, TOKEN, "BUY", "2400000", "5000000"),                     # Buy YES
    ("buy", ("no", "sell"), NO_TOKEN, "SELL", "5000000", "2600000"),        # Sell NO held
    ("sell", None, NO_TOKEN, "BUY", "3100000", "5000000"),                 # Buy NO
    ("sell", ("yes", "sell"), TOKEN, "SELL", "5000000", "1900000"),         # Sell YES held
])
async def test_waiting_restart_limit_child_direction_and_targeted_cancel(venue, tmp_path, side, leg,
                                                                       token_id, wire_side, maker, taker):
    venue.on("POST", "/order", body={"success": True, "orderID": "0xchild", "status": "live"})
    venue.on("DELETE", "/order", body={"canceled": ["0xchild"], "not_canceled": {}})
    venue.on("GET", "/data/order/0xchild", body={**DOC_ORDER, "id": "0xchild", "asset_id": token_id,
                                                "side": wire_side, "original_size": "5", "status": "CANCELED"})
    poly = adapter(venue)
    path = tmp_path / "orders.db"
    engine = engine_for(poly, path)
    request = OrderRequest(market_id=MARKET, side=side, quantity="5", type="stop_limit",
                           stop_price="0.45" if side == "buy" else "0.40",
                           price="0.48" if side == "buy" else "0.38", post_only=True,
                           **({"outcome": leg[0], "action": leg[1]} if leg else {}), client_order_id="stop-a")
    try:
        await engine.start()
        engine.set_book(MARKET, SimpleNamespace(ready=True, best_bid=D("0.44"), best_ask=D("0.44")))
        parent = await engine.submit(request)
        await engine.on_book({"market_id": MARKET})
        assert parent.status == OrderStatus.WAITING and venue.sent("POST", "/order") == []
        await engine.stop()
        engine = engine_for(poly, path)
        await engine.start()
        assert (await engine.submit(request)).id == parent.id
        engine.set_book(MARKET, SimpleNamespace(ready=True, best_bid=D("0.39"), best_ask=D("0.46")))
        await engine.on_book({"market_id": MARKET})
        assert engine.managed_order_controller.get(parent.id).as_order().status == OrderStatus.TRIGGERED
        sent = venue.sent("POST", "/order")
        assert len(sent) == 1
        body = json.loads(sent[0].content)
        assert body["orderType"] == "GTC" and body["postOnly"] is True
        assert body["order"]["tokenId"] == token_id and body["order"]["side"] == wire_side
        assert body["order"]["makerAmount"] == maker and body["order"]["takerAmount"] == taker
        assert (await engine.cancel(parent.id)).status == OrderStatus.CANCELED
        assert len(venue.sent("DELETE", "/order")) == 1
        assert venue.sent("DELETE", "/cancel-all") == []
        await engine.stop()
        engine = engine_for(poly, path)
        await engine.start()
        replay = await engine.submit(request)
        assert replay.id == parent.id and replay.status == OrderStatus.CANCELED
        assert len(venue.sent("POST", "/order")) == 1
    finally:
        await engine.stop()
        await poly.close()


@pytest.mark.parametrize("side,token_id,wire_price", [("buy", TOKEN, "0.99"), ("sell", NO_TOKEN, "0.99")])
async def test_stop_market_child_uses_fak_and_remains_pending_until_fill_readback(venue, tmp_path, side, token_id, wire_price):
    venue.on("POST", "/order", body={"success": True, "orderID": "0xchild", "status": "matched"})
    venue.on("GET", "/data/order/0xchild", body={**DOC_ORDER, "id": "0xchild", "asset_id": token_id,
                                                "side": "BUY", "original_size": "5", "size_matched": "5",
                                                "price": wire_price, "order_type": "FAK", "status": "MATCHED"})
    poly = adapter(venue)
    engine = engine_for(poly, tmp_path / "orders.db")
    try:
        await engine.start()
        parent = await engine.submit(OrderRequest(market_id=MARKET, side=side, quantity="5", type="stop_market",
                                                 stop_price="0.40", client_order_id="market-a"))
        engine.set_book(MARKET, SimpleNamespace(ready=True, best_bid=D("0.39"), best_ask=D("0.41")))
        await engine.on_book({"market_id": MARKET})
        managed = engine.managed_order_controller.get(parent.id)
        assert managed.as_order().status == OrderStatus.TRIGGERED and managed.filled == 0
        sent = json.loads(venue.sent("POST", "/order")[0].content)
        assert sent["orderType"] == "FAK" and sent["order"]["tokenId"] == token_id
        assert sent["order"]["makerAmount"] == "4950000" and sent["order"]["takerAmount"] == "5000000"
        await engine.on_order(await poly.fetch_order("0xchild"))
        assert managed.as_order().status == OrderStatus.CLOSED and managed.filled == 5 and managed.remaining == 0
    finally:
        await engine.stop()
        await poly.close()


async def test_response_lost_never_resends_but_needs_signed_identity_recovery(venue, tmp_path):
    poly = adapter(venue)
    original = venue.handler
    def lose_response(request):
        if request.method == "POST" and request.url.path == "/order":
            venue.requests.append(request)
            raise httpx.ReadError("accepted; response lost", request=request)
        return original(request)
    poly.clob._client = httpx.AsyncClient(transport=httpx.MockTransport(lose_response))
    venue.on("GET", "/data/orders", body={"data": [{**DOC_ORDER, "id": "0xunlinked", "original_size": "5"}], "next_cursor": "LTE="})
    path = tmp_path / "orders.db"
    engine = engine_for(poly, path)
    req = OrderRequest(market_id=MARKET, side="buy", quantity="5", type="stop_limit",
                       stop_price="0.40", price="0.42", client_order_id="uncertain-a")
    try:
        await engine.start()
        parent = await engine.submit(req)
        engine.set_book(MARKET, SimpleNamespace(ready=True, best_bid=D("0.39"), best_ask=D("0.41")))
        await engine.on_book({"market_id": MARKET})
        assert engine.managed_order_controller.get(parent.id).pending_child is not None
        await engine.stop()
        engine = engine_for(poly, path)
        recovery = await engine.start()
        assert recovery.unresolved == 1
        restored = engine.managed_order_controller.get(parent.id)
        assert restored.pending_child is not None and restored.children == []
        assert (await engine.submit(req)).id == parent.id
        assert len(venue.sent("POST", "/order")) == 1
    finally:
        await engine.stop()
        await poly.close()
