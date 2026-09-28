"""`synpath serve` and the `synpath` command: the whole self-hosted stack in one process."""
from __future__ import annotations

try:
    import tomllib
except ImportError:  # Python 3.10
    import tomli as tomllib
from decimal import Decimal as D
from pathlib import Path

import httpx
import pytest

from synpath.__main__ import main as synpath_main
from synpath.engine.feeds import VenueStreams
from synpath.engine.paper import PaperVenue
from synpath.server import serve
from synpath.trading.types import Account

from test_feeds import FakeStream

pytestmark = pytest.mark.anyio

K = "kalshi:KX-A"


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def stack_for(tmp_path: Path, *, streams: bool = True, port: int = 8000):
    kalshi = PaperVenue(venue="kalshi", cash=D("10000"), account=Account(venue="kalshi", name="paper"))
    fake = FakeStream("kalshi", "kalshi", private=True)
    stack = await serve.build(
        config={"risk": {"price_collar": None, "duplicate_window_ms": 0, "closing_soon_s": None}},
        journal=str(tmp_path / "serve.db"), control=str(tmp_path / "control.db"),
        adapters={"kalshi": kalshi}, stream_map={"kalshi": VenueStreams(market=fake, private=fake)}, streams=streams,
        port=port, home_dir=str(tmp_path / "home"),
    )
    return stack, kalshi, fake


class TestZeroConfig:
    def test_with_no_venues_named_it_uses_every_venue_with_credentials(self):
        assert serve.venues_config({}, {"kalshi": object(), "polymarket": None, "polymarket_us": object()}) == {
            "venues": {"kalshi": {}, "polymarket_us": {}}}
        assert serve.venues_config({}, {})["venues"] == {}
        named = {"venues": {"paper": {"mirrors": "kalshi"}}}
        assert serve.venues_config(named, {"kalshi": object()}) is named

    def test_the_command_is_declared_in_the_package(self):
        project = tomllib.loads((Path(__file__).parent.parent / "pyproject.toml").read_text())
        assert project["project"]["scripts"]["synpath"] == "synpath.__main__:main"


class TestStack:
    async def test_an_order_placed_over_http_reaches_the_venue(self, tmp_path):
        stack, kalshi, fake = await stack_for(tmp_path)
        try:
            assert stack.owner_key, "a new control database gets an owner access token"
            assert fake.started and fake.subscribed == ["orders", "fills"]
            transport = httpx.ASGITransport(app=stack.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://synpath") as client:
                placed = await client.post(
                    "/trading/orders", headers={"Authorization": f"Bearer {stack.owner_key}"},
                    json={"market_id": K, "side": "buy", "amount": "5", "type": "limit", "price": "0.35", "book": "alpha"})
                assert placed.status_code == 201, placed.text
                assert placed.json()["venue"] == "kalshi"
                assert (await client.post("/trading/orders", json={"market_id": K})).status_code == 401
            assert [o.amount for o in kalshi.orders.values()] == [D("5")]
        finally:
            await serve.shutdown(stack)

    async def test_the_address_itself_says_what_the_server_is(self, tmp_path):
        stack, _, _ = await stack_for(tmp_path)
        try:
            transport = httpx.ASGITransport(app=stack.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://synpath") as client:
                index = await client.get("/")
            assert index.status_code == 200
            assert index.json()["name"] == "synpath" and index.json()["links"]["trading"] == "/trading"
        finally:
            await serve.shutdown(stack)

    async def test_the_owner_key_is_made_once(self, tmp_path):
        first, *_ = await stack_for(tmp_path)
        await serve.shutdown(first)
        second, *_ = await stack_for(tmp_path)
        try:
            assert second.owner_key is None
        finally:
            await serve.shutdown(second)

    async def test_every_loop_the_stack_needs_is_scheduled(self, tmp_path):
        stack, *_ = await stack_for(tmp_path)
        loops = serve.background(stack)
        try:
            assert {"lease", "sweep", "poll", "managed", "feeds-sync", "feed-kalshi-kalshi",
                    "reconcile", "control", "eod", "status"} <= set(loops)
        finally:
            for coro in loops.values():
                coro.close()
            await serve.shutdown(stack)

    async def test_streams_can_be_turned_off(self, tmp_path):
        stack, kalshi, fake = await stack_for(tmp_path, streams=False)
        try:
            assert stack.feeds is None and not fake.started
            assert "managed" in (loops := serve.background(stack))
        finally:
            for coro in loops.values():
                coro.close()
            await serve.shutdown(stack)

    async def test_shutdown_cancels_what_rests_and_releases_the_lease(self, tmp_path):
        stack, kalshi, fake = await stack_for(tmp_path)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=stack.app), base_url="http://s") as client:
            await client.post("/trading/orders", headers={"Authorization": f"Bearer {stack.owner_key}"},
                              json={"market_id": K, "side": "buy", "amount": "5", "type": "limit", "price": "0.35"})
        await serve.shutdown(stack)
        assert all(o.is_terminal for o in kalshi.orders.values()), "the default halt policy cancels"
        assert fake.closed
        again, *_ = await stack_for(tmp_path)          # the lease is free: a new process can start
        await serve.shutdown(again)


class TestLocalKey:
    async def test_the_key_is_left_for_this_machine_and_kept_across_starts(self, tmp_path):
        from synpath.server import local

        home = str(tmp_path / "home")
        first, *_ = await stack_for(tmp_path)
        try:
            path = local.registry_path(home)
            assert path.exists() and oct(path.stat().st_mode & 0o777) == "0o600"
            assert first.local_key == first.owner_key
            assert local.lookup("http://127.0.0.1:8000", home_dir=home) == first.owner_key
            assert local.lookup("http://localhost:8000/", home_dir=home) == first.owner_key
            assert local.lookup("http://127.0.0.1:9000", home_dir=home) is None, "another port is another server"
            assert local.lookup("http://10.0.0.5:8000", home_dir=home) is None, "never for a network address"
        finally:
            await serve.shutdown(first)
        second, *_ = await stack_for(tmp_path)
        try:
            assert second.owner_key is None and second.local_key == first.owner_key, "the record survives a restart"
        finally:
            await serve.shutdown(second)

    async def test_a_different_control_database_on_the_same_port_does_not_inherit_the_key(self, tmp_path):
        from synpath.server import local

        first, *_ = await stack_for(tmp_path)
        await serve.shutdown(first)
        other = tmp_path / "other"
        other.mkdir()
        (other / "home").symlink_to(tmp_path / "home")
        second, *_ = await stack_for(other)
        try:
            assert second.owner_key and second.local_key == second.owner_key
            assert local.lookup("http://127.0.0.1:8000", home_dir=str(tmp_path / "home")) == second.owner_key
        finally:
            await serve.shutdown(second)


class TestCommand:
    def test_help_and_unknown_commands(self, capsys):
        assert synpath_main([]) == 0 and "synpath serve" in capsys.readouterr().out
        assert synpath_main(["nope"]) == 2

    @pytest.mark.parametrize("argv", [["serve", "--help"], ["doctor", "--help"], ["status", "--help"],
                                      ["schema", "--help"], ["engine", "--help"]])
    def test_every_command_reaches_its_module(self, argv):
        with pytest.raises(SystemExit) as exit_:
            synpath_main(argv)
        assert exit_.value.code == 0
