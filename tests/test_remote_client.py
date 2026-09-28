"""`synpath.Client(server=...)`: the same method names, through your own synpath serve."""
from __future__ import annotations

from decimal import Decimal as D
from pathlib import Path

import httpx
import pytest

import synpath
from synpath.bucket import Bucket, BucketMember
from synpath.engine.feeds import VenueStreams
from synpath.engine.paper import PaperVenue
from synpath.errors import AuthenticationError, BadRequest, NotSupported
from synpath.server import serve
from synpath.trading.errors import OrderNotFound, RiskRejected
from synpath.trading.types import Account, EditRequest, OrderRequest, OrderStatus, OrderType, Side

from test_feeds import FakeStream

pytestmark = pytest.mark.anyio

K, P = "kalshi:KX-A", "polymarket:123"
SERVER = "http://127.0.0.1:8000"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SYNPATH_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("SYNPATH_ACCESS_TOKEN", raising=False)
    kalshi = PaperVenue(venue="kalshi", cash=D("10000"), account=Account(venue="kalshi", name="paper"))
    poly = PaperVenue(venue="polymarket", cash=D("10000"), account=Account(venue="polymarket", name="paper"))
    kalshi.set_book(K, bids=[(D("0.30"), D("100"))], asks=[(D("0.60"), D("100"))])
    fake = FakeStream("kalshi", "kalshi", private=True)
    built = await serve.build(
        config={"risk": {"price_collar": None, "duplicate_window_ms": 0, "closing_soon_s": None,
                         "max_order_contracts": "50"}},
        journal=str(tmp_path / "serve.db"), control=str(tmp_path / "control.db"),
        adapters={"kalshi": kalshi, "polymarket": poly},
        stream_map={"kalshi": VenueStreams(market=fake, private=fake)},
        port=8000, home_dir=str(tmp_path / "home"),
    )
    for venue in (kalshi, poly):                 # the paper venues stand in for the venues' private streams
        venue.subscribe(built.engine.on_fill)
        venue.subscribe_orders(built.engine.on_order)
    built.kalshi, built.poly = kalshi, poly   # type: ignore[attr-defined]
    yield built
    await serve.shutdown(built)


def client_for(stack, **kw) -> synpath.Client:
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.app))
    return synpath.Client(server=SERVER, server_http=http, **kw)


def order(**kw) -> OrderRequest:
    base = dict(market_id=K, side=Side.BUY, amount=D("5"), type=OrderType.LIMIT, price=D("0.35"), book="alpha")
    return OrderRequest(**{**base, **kw})


class TestOrders:
    async def test_a_plain_order_reaches_the_venue_the_market_id_names(self, stack):
        async with client_for(stack) as client:
            placed = await client.create_order(order())
            assert placed.venue == "kalshi" and placed.status == OrderStatus.OPEN
            assert [o.amount for o in stack.kalshi.orders.values()] == [D("5")]
            assert repr(client).endswith(f"server={SERVER}>")

    async def test_an_engine_held_order_is_reachable_from_python(self, stack):
        async with client_for(stack) as client:
            stop = await client.create_order(order(side=Side.SELL, type=OrderType.STOP_MARKET, stop_price=D("0.25"),
                                                   price=None, params={"max_slippage": "0.05"}))
            assert stop.held_by.value == "engine" and stop.status == OrderStatus.WAITING
            assert stack.kalshi.orders == {}, "held by the engine, not at the venue"

    async def test_list_get_edit_and_cancel_round_trip(self, stack):
        async with client_for(stack) as client:
            placed = await client.create_order(order())
            assert [o.id for o in await client.fetch_open_orders()] == [placed.id]
            assert [o.id for o in await client.fetch_open_orders(market_id=P)] == []
            assert (await client.fetch_order(placed.id)).id == placed.id
            edited = await client.edit_order(EditRequest(order_id=placed.id, price=D("0.36")))
            assert edited.price == D("0.36")
            cancelled = await client.cancel_order(placed.id)
            assert cancelled.status == OrderStatus.CANCELED
            assert await client.fetch_open_orders() == []

    async def test_cancel_all_and_create_orders(self, stack):
        async with client_for(stack) as client:
            results = await client.create_orders([order(), order(price=D("0.34")), order(amount=D("500"))])
            assert [type(r).__name__ for r in results] == ["Order", "Order", "RiskRejected"]
            assert await client.cancel_all_orders() == 2
            assert await client.fetch_open_orders() == []

    async def test_an_order_on_a_bucket(self, stack):
        bucket = await stack.engine.save_bucket(Bucket(book="alpha", name="b", members=[
            BucketMember(market_id=K), BucketMember(market_id=P)]))
        async with client_for(stack) as client:
            placed = await client.create_order(order(market_id=bucket.market_id, type=OrderType.MARKET, price=D("0.45")))
            assert placed.held_by.value == "engine" and placed.market_id == bucket.market_id


class TestPortfolio:
    async def test_positions_fills_and_balances(self, stack):
        async with client_for(stack) as client:
            await client.create_order(order(price=D("0.60")))       # crosses the paper ask
            await stack.kalshi.deliver()
            positions = await client.fetch_positions()
            assert [(p.market_id, p.contracts) for p in positions] == [(K, D("5"))]
            assert await client.fetch_positions(market_id=P) == []
            fills = await client.fetch_my_trades(venue="kalshi", limit=10)
            assert len(fills) == 1 and fills[0].amount == D("5")
            balance = await client.fetch_balance("kalshi")
            assert balance.venue == "kalshi"
            with pytest.raises(NotSupported):
                await client.fetch_settlements(venue="kalshi")


class TestKeys:
    async def test_a_loopback_server_needs_no_key_from_the_caller(self, stack):
        async with client_for(stack) as client:            # no access_token: found in the registry
            assert client._remote.key == stack.owner_key
            assert (await client.create_order(order())).status == OrderStatus.OPEN

    async def test_the_environment_and_an_explicit_key_win_over_the_registry(self, stack, monkeypatch):
        monkeypatch.setenv("SYNPATH_ACCESS_TOKEN", "from-env")
        async with client_for(stack) as client:
            assert client._remote.key == "from-env"
        async with client_for(stack, access_token="explicit") as client:
            assert client._remote.key == "explicit"

    async def test_a_network_address_without_a_key_says_what_to_do(self, stack):
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.app))
        async with synpath.Client(server="http://10.0.0.5:8000", server_http=http) as client:
            with pytest.raises(AuthenticationError, match="SYNPATH_ACCESS_TOKEN"):
                await client.create_order(order())

    async def test_a_key_without_the_permission_and_a_bad_key(self, stack):
        viewer = await stack.store.create_user("viewer")
        await stack.store.acting_as("test", "setup")
        await stack.store.grant(viewer.id, "kalshi:paper", "view")
        key = await stack.store.issue_key(viewer.id, label="view only")
        async with client_for(stack, access_token=key.secret) as client:
            with pytest.raises(AuthenticationError):
                await client.create_order(order())
            assert await client.fetch_open_orders() == []
        async with client_for(stack, access_token="wrong") as client:
            with pytest.raises(AuthenticationError):
                await client.fetch_open_orders()


class TestErrors:
    async def test_server_answers_map_to_the_library_s_errors(self, stack):
        async with client_for(stack) as client:
            with pytest.raises(RiskRejected) as risk:
                await client.create_order(order(amount=D("500")))
            assert risk.value.rule == "max_order_contracts"
            with pytest.raises(OrderNotFound):
                await client.fetch_order("nope")
            with pytest.raises(BadRequest):
                await client.create_order(order(market_id="KX-A"))

    async def test_market_data_stays_direct(self, stack):
        class Stub:
            closed = False

            def close(self):
                self.closed = True

        stub = Stub()
        client = client_for(stack, exchanges={"kalshi": stub})
        assert client.exchange("kalshi") is stub, "reads never go through the server"
        await client.close()
        assert stub.closed


class TestBuckets:
    async def test_create_bucket_then_order_on_it(self, stack):
        async with client_for(stack) as client:
            bucket = await client.create_bucket(book="alpha", name="same question", members=[
                BucketMember(market_id=K), BucketMember(market_id=P, flip=True)])
            assert isinstance(bucket, Bucket) and bucket.market_id.startswith("bucket:")
            assert await stack.engine.journal.bucket(bucket.id) is not None
            placed = await client.create_order(order(market_id=bucket.market_id, type=OrderType.MARKET, price=D("0.45")))
            assert placed.held_by.value == "engine" and placed.market_id == bucket.market_id

    async def test_a_bad_definition_is_a_bad_request(self, stack):
        async with client_for(stack) as client:
            with pytest.raises(BadRequest):
                await client.create_bucket(book="alpha", name="one", members=[BucketMember(market_id=K)])

    async def test_without_a_server_there_is_nowhere_to_keep_it(self):
        client = synpath.Client()
        with pytest.raises(BadRequest, match="server="):
            await client.create_bucket(book="alpha", name="x", members=[
                BucketMember(market_id=K), BucketMember(market_id=P)])


class TestBucketReads:
    async def test_the_whole_bucket_lifecycle_from_python(self, stack):
        async with client_for(stack) as client:
            bucket = await client.create_bucket(book="alpha", name="same question", members=[
                BucketMember(market_id=K), BucketMember(market_id=P, flip=True)])
            assert [b.id for b in await client.fetch_buckets()] == [bucket.id]
            assert (await client.fetch_bucket(bucket.market_id)).id == bucket.id      # either id form
            placed = await client.create_order(order(market_id=bucket.market_id, type=OrderType.MARKET, price=D("0.45")))
            reports = await client.fetch_bucket_orders(bucket.id)
            assert [r.order_id for r in reports] == [placed.id]
            one = await client.fetch_bucket_order(bucket.id, placed.id)
            assert isinstance(one, synpath.BucketOrderReport) and one.amount == D("5")
            position = await client.fetch_bucket_position(bucket.id)
            assert position.side == "flat"
            await client.cancel_order(placed.id)
            assert (await client.fetch_bucket_order(bucket.id, placed.id)).status == "canceled"
            archived = await client.archive_bucket(bucket.id)
            assert archived.status == "archived" and await client.fetch_buckets() == []
            with pytest.raises(OrderNotFound):
                await client.fetch_bucket("nope")

    async def test_every_bucket_call_needs_a_server(self):
        client = synpath.Client()
        for call in (client.fetch_buckets(), client.fetch_bucket("x"), client.archive_bucket("x"),
                     client.fetch_bucket_position("x"), client.fetch_bucket_orders("x"),
                     client.fetch_bucket_order("x", "y")):
            with pytest.raises(BadRequest, match="server="):
                await call
