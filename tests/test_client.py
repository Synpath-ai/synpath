"""`synpath.Client`: one object, routed by the venue in each id."""
from __future__ import annotations

from decimal import Decimal

import pytest

import synpath
from synpath import BadRequest, Client, OrderRequest, Side
from synpath.kalshi import normalize_market as kalshi_market
from synpath.polymarket import normalize_market as poly_market
from synpath.trading.errors import CredentialsMissing

pytestmark = pytest.mark.anyio
D = Decimal


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeRead:
    def __init__(self, venue, market):
        self.id, self.market, self.calls = venue, market, []

    def fetch_market(self, market_id):
        self.calls.append(("fetch_market", market_id))
        return self.market

    def fetch_markets_by_ids(self, market_ids):
        self.calls.append(("fetch_markets_by_ids", list(market_ids)))
        return [self.market]

    def fetch_order_book(self, market_id, *, side="yes", depth=None):
        self.calls.append(("fetch_order_book", market_id, side))
        return {"market_id": market_id, "side": side}

    def fetch_order_books(self, market_ids, *, side="yes", depth=None):
        return {m: {"side": side} for m in market_ids}

    def close(self):
        self.calls.append(("close",))


class FakeTrading:
    def __init__(self, venue):
        self.id, self.calls = venue, []

    async def create_order(self, request):
        self.calls.append(("create_order", request.market_id))
        return request

    async def create_orders(self, requests):
        self.calls.append(("create_orders", [r.market_id for r in requests]))
        return list(requests)

    async def cancel_order(self, order_id, *, market_id=None):
        self.calls.append(("cancel_order", order_id, market_id))
        return order_id

    async def cancel_all_orders(self, *, market_id=None):
        self.calls.append(("cancel_all_orders", market_id))
        return 2

    async def fetch_positions(self, *, market_id=None, event_id=None):
        return [(self.id, market_id)]

    async def fetch_open_orders(self, *, market_id=None):
        return [(self.id, market_id)]

    async def close(self):
        self.calls.append(("close",))


@pytest.fixture
def client(kalshi_market_payload, kalshi_event, poly_market_payload):
    k = kalshi_market(kalshi_market_payload, kalshi_event)
    p = poly_market(poly_market_payload)
    reads = {"kalshi": FakeRead("kalshi", k), "polymarket": FakeRead("polymarket", p)}
    trading = {"kalshi": FakeTrading("kalshi"), "polymarket": FakeTrading("polymarket")}
    return Client(exchanges=reads, trading=trading), reads, trading, k, p


@pytest.fixture
def kalshi_market_payload(kalshi_market):
    return kalshi_market


@pytest.fixture
def poly_market_payload(poly_market):
    return poly_market


class TestReads:
    def test_a_read_goes_to_the_venue_in_the_id(self, client):
        c, reads, _, k, p = client
        assert c.fetch_market(k.id) is k and reads["kalshi"].calls[-1] == ("fetch_market", k.id)
        assert c.fetch_order_book(p.id, side="no")["side"] == "no"
        assert reads["polymarket"].calls[-1] == ("fetch_order_book", p.id, "no")

    def test_a_bare_native_id_cannot_be_routed(self, client):
        c, *_ = client
        with pytest.raises(BadRequest, match="not a Synpath id"):
            c.fetch_market("KXFOO-25")

    def test_batches_split_by_venue_and_keep_the_order(self, client):
        c, reads, _, k, p = client
        got = c.fetch_markets_by_ids([p.id, k.id])
        assert [m.id for m in got] == [p.id, k.id]
        assert reads["kalshi"].calls[-1] == ("fetch_markets_by_ids", [k.id])
        books = c.fetch_order_books([k.id, p.id], side="no")
        assert set(books) == {k.id, p.id}

    def test_unknown_venue_prefix_is_refused(self, client):
        c, *_ = client
        with pytest.raises(BadRequest):
            c.fetch_market("nasdaq:AAPL")


class TestTrading:
    async def test_an_order_goes_to_the_venue_in_its_market_id(self, client):
        c, _, trading, k, p = client
        request = OrderRequest(market_id=p.id, side="sell", amount=5, price="0.30")
        assert await c.create_order(request) is request
        assert trading["polymarket"].calls == [("create_order", p.id)] and trading["kalshi"].calls == []

    async def test_a_batch_fans_out_and_comes_back_in_order(self, client):
        c, _, trading, k, p = client
        requests = [OrderRequest(market_id=m, side="buy", amount=5, price="0.30") for m in (p.id, k.id, p.id)]
        results = await c.create_orders(requests)
        assert results == requests
        assert trading["polymarket"].calls[-1] == ("create_orders", [p.id, p.id])

    async def test_cancel_needs_somewhere_to_go(self, client):
        c, _, trading, k, _ = client
        await c.cancel_order("o1", market_id=k.id)
        assert trading["kalshi"].calls[-1] == ("cancel_order", "o1", k.id)
        await c.cancel_order("o2", venue="kalshi")
        assert trading["kalshi"].calls[-1] == ("cancel_order", "o2", None)
        with pytest.raises(BadRequest, match="market_id= or venue="):
            await c.cancel_order("o3")
        with pytest.raises(BadRequest, match="belongs to kalshi"):
            await c.cancel_order("o4", market_id=k.id, venue="polymarket")

    async def test_no_scope_means_every_connected_venue(self, client):
        c, _, trading, *_ = client
        assert await c.cancel_all_orders() == 4
        assert sorted(v for v, _ in await c.fetch_positions()) == ["kalshi", "polymarket"]
        assert sorted(v for v, _ in await c.fetch_open_orders()) == ["kalshi", "polymarket"]

    async def test_missing_credentials_name_the_variables(self):
        c = Client({})
        with pytest.raises(CredentialsMissing, match="KALSHI_KEY_ID"):
            await c.create_order(OrderRequest(market_id="kalshi:KXFOO-25", side="buy", amount=1, price="0.5"))

    async def test_close_closes_everything_it_built(self, client):
        c, reads, trading, *_ = client
        async with c:
            pass
        assert reads["kalshi"].calls[-1] == ("close",) and trading["polymarket"].calls[-1] == ("close",)


class TestExports:
    def test_client_is_a_top_level_name(self):
        assert synpath.Client is Client and "Client" in synpath.__all__

    def test_read_adapters_are_built_on_first_use(self):
        c = Client()
        assert isinstance(c.exchange("kalshi"), synpath.Kalshi)
        assert c.exchange("kalshi") is c.exchange("kalshi")
