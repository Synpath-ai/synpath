"""Engine-held orders against Kalshi's demo environment.

Marked `demo`: these place and cancel real orders. Each test drives one
synthetic type end to end -- the parent is journaled, its children are real
Kalshi orders, and the venue's own answers come back through the adapter --
then leaves the account flat.

```bash
pytest -m demo tests/test_engine_orders_demo.py
```

The book is read from the venue and handed to the engine, because a stop
that has no book to watch is not a stop. Orders are one contract, priced far
from the touch so they rest rather than fill, except where the test is
explicitly about taking.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.trading.credentials import load_credentials
from synpath.trading.kalshi import KalshiTrading
from synpath.trading.types import Account, OrderRequest, OrderStatus, OrderType, Side, TimeInForce

pytestmark = [pytest.mark.demo, pytest.mark.anyio]
ACCOUNT = Account(venue="kalshi", name="demo")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def kalshi():
    credentials = load_credentials(dotenv=".env").get("kalshi")
    if credentials is None:
        pytest.skip("no Kalshi credentials configured")
    if getattr(credentials, "env", "demo") != "demo":
        pytest.skip("these tests only run against the demo environment")
    adapter = KalshiTrading(credentials)
    await adapter.cancel_all_orders()
    # The demo reflects a cancel about half a second later; start flat.
    for _ in range(12):
        if not await adapter.fetch_open_orders():
            break
        await asyncio.sleep(0.5)
    yield adapter
    try:
        await adapter.cancel_all_orders()
    finally:
        await adapter.close()


@pytest.fixture
async def market(kalshi):
    """An open market quoted on both sides, with room below the bid.

    The room matters: a stop needs a level under the touch to wait at, and
    much of the demo trades at a one-cent bid.
    """
    cursor, fallback = None, None
    for _ in range(6):
        page = await kalshi._call("GET", "/markets", params={"status": "open", "limit": 200, "cursor": cursor})
        for row in page["markets"]:
            bid, ask = row.get("yes_bid_dollars"), row.get("yes_ask_dollars")
            if not (bid and ask and D("0") < D(bid) < D("0.5") < D(ask) < D("1")):
                continue
            if D(bid) >= D("0.05"):
                return row["ticker"]
            fallback = fallback or row["ticker"]
        cursor = page.get("cursor")
        if not cursor:
            break
    if fallback:
        return fallback
    pytest.skip("no suitable open demo market")


@pytest.fixture
async def engine(tmp_path: Path, kalshi):
    engine = Engine(
        {"kalshi": kalshi},
        EngineConfig(journal_path=str(tmp_path / "orders.db"), managed_tick_s=0.05, in_doubt_timeout_s=0),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_orders_per_minute=None, closing_soon_s=None,
                        max_order_contracts=D("5")),
        accounts={"kalshi": ACCOUNT},
    )
    await engine.start()
    timer = asyncio.get_running_loop().create_task(engine._managed_loop())
    yield engine
    timer.cancel()
    await engine.stop()


async def show_book(engine: Engine, kalshi, ticker: str) -> tuple[D | None, D | None]:
    """Read the venue's book and hand it to the engine, as a stream would.

    Kalshi quotes both legs as bids (`orderbook_fp` has `yes_dollars` and
    `no_dollars`); a NO bid at 0.97 is a YES offer at 0.03.
    """
    from synpath.ws.base import LocalBook

    raw = await kalshi._call("GET", f"/markets/{ticker}/orderbook", params={"depth": 5})
    levels = raw.get("orderbook_fp") or raw.get("orderbook") or {}
    yes = [(D(str(p)), D(str(s))) for p, s in (levels.get("yes_dollars") or [])]
    no = [(D(str(p)), D(str(s))) for p, s in (levels.get("no_dollars") or [])]
    local = LocalBook()
    local.replace(yes, [(D("1") - p, s) for p, s in no])
    engine.set_book(f"{ticker}:yes", local)
    return local.best_bid, local.best_ask


async def wait_for(check, *, timeout: float = 8.0, step: float = 0.4) -> bool:
    """Poll a condition. The demo's order store lags its own answers."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = check()
        if asyncio.iscoroutine(result):
            result = await result
        if result or loop.time() >= deadline:
            return bool(result)
        await asyncio.sleep(step)


async def no_orders_left(kalshi, market) -> bool:
    return not [o for o in await kalshi.fetch_open_orders() if o.market_id == market]


class TestOnDemo:
    async def test_an_iceberg_shows_only_its_slice(self, engine, kalshi, market):
        parent = await engine.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("4"), price=D("0.02"),
            type=OrderType.ICEBERG, params={"display": "1", "reload_delay_s": 0}, book="demo-iceberg",
        ))
        assert parent.status == OrderStatus.TRIGGERED and parent.held_by.value == "engine"

        managed = engine.orders.get(parent.id)
        mine = {c.order_id for c in managed.children}
        assert len(mine) == 1, "one slice at a time"

        async def the_slice_is_resting() -> bool:
            resting = {o.id: o for o in await kalshi.fetch_open_orders() if o.market_id == market}
            ours = [resting[i] for i in mine if i in resting]
            return len(ours) == 1 and ours[0].amount == D("1")

        # The demo lists a new order a few hundred milliseconds after taking it.
        assert await wait_for(the_slice_is_resting), "three of the four contracts are not at the venue"

        canceled = await engine.cancel(parent.id)
        assert canceled.status == OrderStatus.CANCELED
        assert await wait_for(lambda: len(
            [o for o in engine.open_orders(venue="kalshi") if o.market_id == market and o.held_by.value == "venue"]
        ) == 0)

    async def test_a_stop_watches_the_real_book_and_does_not_fire_early(self, engine, kalshi, market):
        bid, ask = await show_book(engine, kalshi, market)
        # A sell stop watches the bid; a demo book often quotes only one side.
        assert bid is not None
        if bid <= D("0.02"):
            pytest.skip(f"the bid is {bid}: there is no room below it for a stop")
        parent = await engine.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.SELL, amount=D("1"), type=OrderType.STOP_MARKET,
            stop_price=(bid / 2).quantize(D("0.01")), params={"protection": "0.01"}, book="demo-stop",
        ))
        await engine.on_book({"instrument_id": f"{market}:yes"})
        managed = engine.orders.get(parent.id)
        assert managed.state == "waiting", "the stop is far from the touch"
        assert [o for o in await kalshi.fetch_open_orders() if o.market_id == market] == []

        # Move the watched price under the stop: the trigger is the engine's.
        from synpath.ws.base import LocalBook

        moved = LocalBook()
        moved.replace([(D("0.01"), D("10"))], [(D("0.99"), D("10"))])
        engine.set_book(f"{market}:yes", moved)
        await engine.on_book({"instrument_id": f"{market}:yes"})
        assert managed.state in ("working", "done", "rejected")
        assert managed.triggered_at is not None

    async def test_a_twap_sends_its_slices_to_the_venue(self, engine, kalshi, market):
        parent = await engine.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("2"), price=D("0.02"), type=OrderType.TWAP,
            params={"window_s": 4, "slices": 2, "style": "limit", "limit": "0.02"}, book="demo-twap",
        ))
        managed = engine.orders.get(parent.id)
        assert managed.sent_slices == 1
        assert await wait_for(lambda: len(managed.children) >= 2, timeout=10)

        async def every_slice_is_real() -> bool:
            for child in managed.children:
                try:
                    await kalshi.fetch_order(child.order_id)
                except Exception:
                    return False
            return True

        assert await wait_for(every_slice_is_real), "every slice is a real order at the venue"
        await engine.cancel(parent.id)

    async def test_a_parent_survives_a_restart_against_the_venue(self, tmp_path, kalshi, market):
        path = str(tmp_path / "restart.db")
        first = Engine({"kalshi": kalshi}, EngineConfig(journal_path=path, managed_tick_s=0.05),
                       risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
                       accounts={"kalshi": ACCOUNT})
        await first.start()
        parent = await first.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("2"), price=D("0.02"), type=OrderType.ICEBERG,
            params={"display": "1"}, book="demo-restart",
        ))
        child_ids = [c.order_id for c in first.orders.get(parent.id).children]
        await first.journal.close()

        second = Engine({"kalshi": kalshi}, EngineConfig(journal_path=path, managed_tick_s=0.05),
                        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
                        accounts={"kalshi": ACCOUNT})
        recovery = await second.start()
        try:
            assert recovery.managed == 1
            revived = second.orders.get(parent.id)
            assert revived is not None and revived.state == "working"
            assert [c.order_id for c in revived.children] == child_ids, "the same child, not a new one"
            async def child_is_live() -> bool:
                live = [o.id for o in await kalshi.fetch_open_orders() if o.market_id == market]
                return live.count(child_ids[0]) == 1

            assert await wait_for(child_is_live), "the child kept resting across the restart"
            await second.cancel(parent.id)
            assert await wait_for(lambda: no_orders_left(kalshi, market))
        finally:
            await second.stop()

    async def test_a_halt_stands_parents_and_children_down(self, engine, kalshi, market):
        await engine.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("2"), price=D("0.02"), type=OrderType.ICEBERG,
            params={"display": "1"}, book="demo-halt",
        ))
        result = await engine.halt("demo drill", policy="cancel")
        assert result["managed"] >= 1
        assert all(not p.live for p in engine.orders.parents.values())
        assert await wait_for(lambda: no_orders_left(kalshi, market))

    async def test_a_day_order_carries_a_venue_expiry(self, engine, kalshi, market):
        order = await engine.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"), price=D("0.02"),
            time_in_force=TimeInForce.DAY, book="demo-day",
        ))
        assert order.time_in_force == TimeInForce.GTD and order.expires_at
        live = await kalshi.fetch_order(order.id)
        assert live.expires_at is not None, "the venue holds the expiry, not the engine"
        assert abs(live.expires_at - order.expires_at) < 120_000
        await engine.cancel(order.id)
