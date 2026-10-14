"""Natural-crossing and filled stop verification, using mocked HTTP only."""
import json
from decimal import Decimal as D

import httpx
import pytest

from examples.verify_kalshi_stop_fill import FillGuard, book_for, verify
from synpath.trading.kalshi import KalshiTrading
from test_kalshi_trading import creds, pem, rsa_key

pytestmark = pytest.mark.anyio
TICKER = "KXBTC15M-TEST-00"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FillVenue:
    def __init__(self, *, crossing=True, lose_response=False, rejected=False):
        self.crossing, self.lose_response = crossing, lose_response
        self.rejected = rejected
        self.books = 0
        self.orders = {}
        self.posts = []
        self.position = D("0")

    def handle(self, request):
        path = request.url.path.removeprefix("/trade-api/v2")
        if path == f"/markets/{TICKER}/orderbook":
            self.books += 1
            ask = "0.42" if self.crossing and self.books > 1 else "0.40"
            return httpx.Response(200, json={"orderbook_fp": {"yes_dollars": [["0.39", "10"]],
                                                            "no_dollars": [[str(1-D(ask)), "10"]]}})
        if request.method == "POST" and path == "/portfolio/events/orders":
            body = json.loads(request.content)
            self.posts.append(body)
            if self.rejected:
                return httpx.Response(400, json={"error": {"code": "insufficient_balance", "message": "insufficient shard balance"}})
            entry = body["side"] == "bid"
            oid = "entry" if entry else "exit"
            price = D("0.42") if entry else D("0.39")
            self.position += D("1") if entry else -D(body["count"])
            self.orders[oid] = {"order_id": oid, "client_order_id": body["client_order_id"], "ticker": TICKER,
                                "book_side": body["side"], "status": "executed", "initial_count_fp": "1",
                                "fill_count_fp": "1", "remaining_count_fp": "0", "yes_price_dollars": body["price"],
                                "outcome_side": "yes" if entry else "no",
                                "taker_fill_cost_dollars": str(price if entry else 1 - price), "taker_fees_dollars": "0.02"}
            if entry and self.lose_response:
                raise httpx.ReadError("accepted; response lost", request=request)
            return httpx.Response(201, json={"order_id": oid, "client_order_id": body["client_order_id"],
                                            "fill_count": "1", "remaining_count": "0", "average_fill_price": str(price)})
        if request.method == "GET" and path.startswith("/portfolio/orders/"):
            return httpx.Response(200, json={"order": self.orders[path.split("/")[-1]]})
        if request.method == "GET" and path == "/portfolio/orders":
            return httpx.Response(200, json={"orders": list(self.orders.values()), "cursor": ""})
        if request.method == "GET" and path == "/portfolio/positions":
            assert self.position == 0
            return httpx.Response(200, json={"market_positions": []})
        if request.method == "GET" and path == "/portfolio/fills":
            return httpx.Response(200, json={"fills": [{"fill_id": "fill-1", "order_id": "entry", "ticker": TICKER,
                                                       "book_side": "bid", "count_fp": "1", "yes_price_dollars": "0.42",
                                                       "fee_cost": "0.02", "is_taker": True}]})
        raise AssertionError(f"unexpected operation {request.method} {path}")


@pytest.mark.parametrize("lose_response", [False, True])
async def test_real_price_crossing_fills_once_then_closes_only_test_quantity(creds, tmp_path, lose_response):
    venue, guard = FillVenue(lose_response=lose_response), FillGuard(TICKER, D("0.44"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle), event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with KalshiTrading(creds, base_url="https://external-api.kalshi.com/trade-api/v2", client=client) as adapter:
            book = await book_for(adapter, TICKER)
            report = await verify(adapter, guard, tmp_path, {"ticker": TICKER}, book, D("0.41"), D("0.44"), wait_s=2, poll_s=0)
    assert report["outcome"] == "filled and closed" and report["terminal_restart_verified"]
    assert report["initial_ask"] == "0.40" and report["observed_trigger_ask"] == "0.42"
    assert report["entry_filled"] == "1" and report["entry_average_price"] == "0.4200"
    assert report["exit_filled"] == "1" and report["remaining_position_contracts"] == "0"
    assert report["exit_average_price"] == "0.3900"
    assert len(venue.posts) == 2 and venue.posts[1]["reduce_only"] is True
    assert venue.posts[1]["count"] == "1.00" and venue.position == 0


async def test_timeout_cancels_local_parent_without_a_venue_order(creds, tmp_path):
    venue, guard = FillVenue(crossing=False), FillGuard(TICKER, D("0.44"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle), event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with KalshiTrading(creds, base_url="https://external-api.kalshi.com/trade-api/v2", client=client) as adapter:
            report = await verify(adapter, guard, tmp_path, {"ticker": TICKER}, await book_for(adapter, TICKER),
                                  D("0.41"), D("0.44"), wait_s=0.5, poll_s=0.01)
    assert report["creates_attempted"] == 0 and venue.posts == []


@pytest.mark.parametrize("change", [{"count": "2"}, {"price": "0.45"}, {"side": "ask"}, {"reduce_only": True}])
async def test_fill_guard_refuses_broader_entry(change):
    guard = FillGuard(TICKER, D("0.44"))
    guard.stage = "entry"
    body = dict(ticker=TICKER, side="bid", price="0.44", count="1", time_in_force="immediate_or_cancel",
                post_only=False, reduce_only=False, client_order_id="entry")
    body.update(change)
    with pytest.raises(ValueError):
        await guard.check(httpx.Request("POST", "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders", json=body))
    assert not guard.sent


async def test_fill_guard_forbids_extra_entry_and_oversized_exit():
    guard = FillGuard(TICKER, D("0.44"))
    guard.stage = "entry"
    body = dict(ticker=TICKER, side="bid", price="0.44", count="1", time_in_force="immediate_or_cancel",
                post_only=False, reduce_only=False, client_order_id="entry")
    request = httpx.Request("POST", "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders", json=body)
    await guard.check(request)
    with pytest.raises(ValueError):
        await guard.check(request)
    guard.stage, guard.exit_quantity = "exit", D("0.50")
    body.update(side="ask", price="0.001", reduce_only=True, client_order_id="exit")
    with pytest.raises(ValueError):
        await guard.check(httpx.Request("POST", str(request.url), json=body))


async def test_definitive_shard_refusal_is_not_reported_as_lost_acceptance(creds, tmp_path):
    venue, guard = FillVenue(rejected=True), FillGuard(TICKER, D("0.44"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with KalshiTrading(creds, base_url="https://external-api.kalshi.com/trade-api/v2", client=client) as adapter:
            report = await verify(adapter, guard, tmp_path, {"ticker": TICKER}, await book_for(adapter, TICKER),
                                  D("0.41"), D("0.44"), wait_s=2, poll_s=0)
    assert report["outcome"] == "venue rejected the entry; no fill" and report["cleanup_verified"]
    assert len(venue.posts) == 1 and venue.orders == {} and venue.position == 0


async def test_price_wait_renews_lease_before_sending(creds, tmp_path, monkeypatch):
    import asyncio
    import examples.verify_kalshi_stop_fill as check
    venue, guard = FillVenue(), FillGuard(TICKER, D("0.44"))
    original_engine = check.engine_for
    def brief_lease(adapter, path):
        engine = original_engine(adapter, path)
        engine.config.lease_ttl_ms = 150
        engine.config.lease_renew_s = 0.02
        return engine
    original_book = check.book_for
    async def slow_book(adapter, ticker):
        await asyncio.sleep(0.25)
        return await original_book(adapter, ticker)
    monkeypatch.setattr(check, "engine_for", brief_lease)
    monkeypatch.setattr(check, "book_for", slow_book)
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with KalshiTrading(creds, base_url="https://external-api.kalshi.com/trade-api/v2", client=client) as adapter:
            report = await check.verify(adapter, guard, tmp_path, {"ticker": TICKER}, await original_book(adapter, TICKER),
                                        D("0.41"), D("0.44"), wait_s=2, poll_s=0)
    assert report["outcome"] == "filled and closed" and len(venue.posts) == 2
    assert venue.posts[1]["price"] == "0.0100", "exit price is legal on a cent-grid market"


def test_kalshi_file_credentials_override_process_environment(pem, tmp_path, monkeypatch):
    from examples.verify_kalshi_stop_limit import credentials_from
    (tmp_path / "key.pem").write_bytes(pem)
    (tmp_path / ".env").write_text("KALSHI_KEY_ID=file-key\nKALSHI_PRIVATE_KEY_PATH=key.pem\nKALSHI_ENV=prod\n")
    monkeypatch.setenv("KALSHI_KEY_ID", "system-key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "/missing/system.pem")
    monkeypatch.setenv("KALSHI_ENV", "demo")
    credential = credentials_from(tmp_path)
    assert credential.key_id == "file-key" and credential.env == "prod"
    assert credential.private_key_pem == pem
