"""US asynchronous stop-market fills and native close, over mocked HTTP."""
import json
from decimal import Decimal as D

import httpx
import pytest

from examples.verify_polymarket_us_stop_fill import Guard, book_for, verify, QUANTITY
from synpath.trading.polymarket_us import PolymarketUSTrading, MarketRules
from synpath.trading.credentials import PolymarketUSCredentials
from test_polymarket_us_trading import ed_key, ed_secret

pytestmark = pytest.mark.anyio
SLUG = "btc-test-15m"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Venue:
    def __init__(self):
        self.posts = []
        self.books = 0
        self.position = D("0")
        self.orders = {}

    def handle(self, request):
        path = request.url.path
        if path == f"/v1/markets/{SLUG}/book":
            self.books += 1
            ask = "0.40" if self.books == 1 else "0.42"
            return httpx.Response(200, json={"marketData": {"bids": [{"px": {"value": "0.39"}, "qty": "10"}],
                                                           "offers": [{"px": {"value": ask}, "qty": "10"}]}})
        if request.method == "POST" and path in ("/v1/orders", "/v1/order/close-position"):
            self.posts.append((path, json.loads(request.content)))
            entry = path == "/v1/orders"
            oid = "entry" if entry else "close"
            self.position = QUANTITY if entry else D("0")
            self.orders[oid] = {"id": oid, "marketSlug": SLUG, "outcomeSide": "OUTCOME_SIDE_YES",
                                "action": "ORDER_ACTION_BUY" if entry else "ORDER_ACTION_SELL",
                                "quantity": str(QUANTITY), "cumQuantity": str(QUANTITY), "leavesQuantity": 0,
                                "state": "ORDER_STATE_FILLED", "type": "ORDER_TYPE_LIMIT",
                                "price": {"value": "0.99" if entry else "0.39"},
                                "avgPx": {"value": "0.42" if entry else "0.39"},
                                "commissionNotionalTotalCollected": {"value": "0.0002"}}
            return httpx.Response(200, json={"id": oid})  # receipt, not a fill confirmation
        if request.method == "GET" and path.startswith("/v1/order/"):
            return httpx.Response(200, json={"order": self.orders[path.split("/")[-1]]})
        if path == "/v1/portfolio/positions":
            return httpx.Response(200, json={"positions": {SLUG: {"netPositionDecimal": str(self.position)}} if self.position else {}, "eof": True})
        raise AssertionError(f"unexpected call {request.method} {path}")


async def test_fractional_stop_market_waits_crosses_fills_and_closes_only_new_position(ed_secret, tmp_path):
    venue, guard = Venue(), Guard(SLUG, tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with PolymarketUSTrading(PolymarketUSCredentials("mock-key", ed_secret), client=client) as adapter:
            rules = MarketRules(SLUG, D("0.01"), QUANTITY)
            adapter.remember_market(rules)
            report = await verify(adapter, guard, tmp_path, rules, await book_for(adapter, SLUG), D("0.41"), wait_s=2, poll_s=0)
    assert report["outcome"] == "filled and closed" and report["remaining_position_contracts"] == "0"
    assert report["initial_ask"] == "0.40" and report["observed_trigger_ask"] == "0.42"
    assert report["entry_filled"] == "0.01" and report["entry_average_price"] == "0.42"
    assert report["parent_status"] == "closed" and report["exit_filled"] == "0.01"
    assert [p for p, _ in venue.posts] == ["/v1/orders", "/v1/order/close-position"]
    assert venue.posts[0][1]["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
    assert venue.posts[0][1]["price"]["value"] == "0.99"
    assert json.loads((tmp_path / "receipts.json").read_text()) == {"entry": "entry", "close": "close"}


@pytest.mark.parametrize("side,bound", [("buy", "0.99"), ("sell", "0.01")])
async def test_subcent_market_grid_still_respects_us_absolute_bounds(ed_secret, tmp_path, side, bound):
    from synpath.engine import Engine, EngineConfig, RiskConfig
    from synpath.trading.types import OrderRequest
    from types import SimpleNamespace
    venue = Venue()
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle)) as client:
        async with PolymarketUSTrading(PolymarketUSCredentials("mock-key", ed_secret), client=client) as adapter:
            adapter.remember_market(MarketRules(SLUG, D("0.001"), QUANTITY))
            engine = Engine({"polymarket_us": adapter}, EngineConfig(journal_path=str(tmp_path / "engine.db")),
                            risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))
            await engine.start()
            try:
                await engine.submit(OrderRequest(market_id=f"polymarket_us:{SLUG}", side=side, quantity=QUANTITY,
                                                 type="stop_market", stop_price="0.40"))
                engine.set_book(f"polymarket_us:{SLUG}", SimpleNamespace(ready=True, best_bid=D("0.39"), best_ask=D("0.41")))
                await engine.on_book({"market_id": f"polymarket_us:{SLUG}"})
                assert D(venue.posts[0][1]["price"]["value"]) == D(bound)
            finally:
                await engine.stop()


@pytest.mark.parametrize("change", [{"quantity": 1}, {"price": {"value": "0.999", "currency": "USD"}},
                                    {"action": "ORDER_ACTION_SELL"}, {"tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL"}])
async def test_guard_rejects_larger_wrong_side_or_resting_entries(tmp_path, change):
    guard = Guard(SLUG, tmp_path)
    guard.stage = "entry"
    body = dict(marketSlug=SLUG, quantity=0.01, price={"value": "0.99", "currency": "USD"}, type="ORDER_TYPE_LIMIT",
                outcomeSide="OUTCOME_SIDE_YES", action="ORDER_ACTION_BUY", tif="TIME_IN_FORCE_IMMEDIATE_OR_CANCEL")
    body.update(change)
    with pytest.raises(ValueError):
        await guard.check(httpx.Request("POST", "https://api.polymarket.us/v1/orders", json=body))
    assert not guard.sent


async def test_price_wait_keeps_the_execution_lease_alive(ed_secret, tmp_path, monkeypatch):
    import asyncio
    import examples.verify_polymarket_us_stop_fill as check
    from synpath.engine import EngineConfig, Engine, RiskConfig
    venue, guard = Venue(), Guard(SLUG, tmp_path)
    original = check.engine_for
    def brief_lease(adapter, directory):
        engine = original(adapter, directory)
        engine.config.lease_ttl_ms = 150
        engine.config.lease_renew_s = 0.02
        return engine
    original_book = check.book_for
    async def delayed_book(adapter, slug):
        await asyncio.sleep(0.25)  # exceeds the lease without its renewal task
        return await original_book(adapter, slug)
    monkeypatch.setattr(check, "engine_for", brief_lease)
    monkeypatch.setattr(check, "book_for", delayed_book)
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with PolymarketUSTrading(PolymarketUSCredentials("mock-key", ed_secret), client=client) as adapter:
            rules = MarketRules(SLUG, D("0.01"), QUANTITY)
            adapter.remember_market(rules)
            initial = await original_book(adapter, SLUG)
            report = await check.verify(adapter, guard, tmp_path, rules, initial, D("0.41"), wait_s=2, poll_s=0)
    assert report["outcome"] == "filled and closed" and len(venue.posts) == 2


async def test_receipt_hook_reads_a_streamed_response_before_decoding(tmp_path):
    class StreamingBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"id":"streamed-entry"}'
    guard = Guard(SLUG, tmp_path)
    response = httpx.Response(200, stream=StreamingBody(),
                              request=httpx.Request("POST", "https://api.polymarket.us/v1/orders"))
    assert not response.is_stream_consumed
    await guard.observe(response)
    assert response.is_stream_consumed and guard.ids["entry"] == "streamed-entry"
    assert json.loads((tmp_path / "receipts.json").read_text())["entry"] == "streamed-entry"


def test_us_file_credentials_override_process_environment(ed_secret, tmp_path, monkeypatch):
    from examples.verify_polymarket_us_stop_fill import credentials_from
    (tmp_path / ".env").write_text(f"POLYMARKET_US_KEY_ID=file-key\nPOLYMARKET_US_SECRET_KEY={ed_secret}\n")
    monkeypatch.setenv("POLYMARKET_US_KEY_ID", "system-key")
    monkeypatch.setenv("POLYMARKET_US_SECRET_KEY", "invalid-system-secret")
    credential = credentials_from(tmp_path)
    assert credential.key_id == "file-key"


async def test_already_eligible_stop_uses_next_real_observation(ed_secret, tmp_path):
    venue, guard = Venue(), Guard(SLUG, tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(venue.handle),
                                event_hooks={"request": [guard.check], "response": [guard.observe]}) as client:
        async with PolymarketUSTrading(PolymarketUSCredentials("mock-key", ed_secret), client=client) as adapter:
            rules = MarketRules(SLUG, D("0.01"), QUANTITY)
            adapter.remember_market(rules)
            initial = await book_for(adapter, SLUG)
            assert venue.posts == []
            report = await verify(adapter, guard, tmp_path, rules, initial, D("0.01"), initially_eligible=True, wait_s=2, poll_s=0)
    assert report["outcome"] == "filled and closed" and report["trigger_mode"] == "initially eligible"
    assert len(venue.posts) == 2 and report["remaining_position_contracts"] == "0"
