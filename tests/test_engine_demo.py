"""The engine against Kalshi's demo environment.

Marked `demo`, deselected by default: these place and cancel real orders on
the demo exchange with the credentials in the environment.

```bash
pytest -m demo tests/test_engine_demo.py
```

What they prove that the offline tests cannot: that the crash-and-restart
path finds a real order at a real venue by its client order id, that
reconciliation agrees with what Kalshi reports, and that a halt cancels
through the venue's own cancel-all rather than one order at a time.

Orders are placed far from the touch at one contract, so they rest and are
cancelled rather than filling.
"""
from __future__ import annotations

import asyncio
import os
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.eod import EndOfDay
from synpath.engine.journal import IntentState
from synpath.engine.reconcile import Reconciler
from synpath.trading.credentials import load_credentials
from synpath.trading.errors import RiskRejected
from synpath.trading.kalshi import KalshiTrading
from synpath.trading.types import Account, OrderRequest, OrderStatus, Side

pytestmark = [pytest.mark.demo, pytest.mark.anyio]


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
    yield adapter
    await adapter.close()


@pytest.fixture
async def market(kalshi):
    """An open market with a two-sided book, so a resting order is realistic.
    Demo pages are mostly one-sided, so this walks a few of them."""
    cursor = None
    for _ in range(5):
        page = await kalshi._call("GET", "/markets", params={"status": "open", "limit": 200, "cursor": cursor})
        for row in page["markets"]:
            bid, ask = row.get("yes_bid_dollars"), row.get("yes_ask_dollars")
            if bid and ask and D("0") < D(bid) < D("0.5") < D(ask) < D("1"):
                return row["ticker"]
        cursor = page.get("cursor")
        if not cursor:
            break
    pytest.skip("no suitable open demo market")


async def no_open_orders(kalshi, *, timeout_s: float = 6.0) -> bool:
    """Kalshi's demo store lags its own cancels by a moment, so read it back
    until it agrees rather than once."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if not await kalshi.fetch_open_orders():
            return True
        await asyncio.sleep(0.5)
    return not await kalshi.fetch_open_orders()


async def engine_for(path: Path, kalshi) -> Engine:
    engine = Engine(
        {"kalshi": kalshi},
        EngineConfig(journal_path=str(path), in_doubt_timeout_s=0, require_lease=True),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_order_contracts=D("5"),
                        closing_soon_s=None, max_orders_per_minute=None),
        accounts={"kalshi": Account(venue="kalshi", name="demo")},
    )
    await engine.start()
    return engine


class TestEngineOnDemo:
    async def test_submit_cancel_and_the_journal_agree_with_kalshi(self, tmp_path, kalshi, market):
        engine = await engine_for(tmp_path / "demo.db", kalshi)
        try:
            order = await engine.submit(OrderRequest(
                instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"), price=D("0.02"),
                book="demo-check", trader="tests",
            ))
            assert order.client_order_id and order.book == "demo-check"
            live = await kalshi.fetch_order(order.id)
            assert live.client_order_id == order.client_order_id
            assert live.status in (OrderStatus.OPEN, OrderStatus.PENDING)

            report = (await Reconciler(engine).run("kalshi"))[0]
            mine = [d for d in report.orphans if d.key == order.id]
            assert not mine, "an order this engine placed must not read as an orphan"

            canceled = await engine.cancel(order.id)
            assert canceled.status in (OrderStatus.CANCELED, OrderStatus.CLOSED)
            intent = await engine.journal.intent(order.client_order_id)
            assert intent.state == IntentState.SENT and intent.order_id == order.id
        finally:
            await kalshi.cancel_all_orders()
            await engine.stop()

    async def test_a_restart_adopts_a_real_order_by_its_client_id(self, tmp_path, kalshi, market):
        """The crash path against a real venue: the journal knows the client
        order id, the venue knows the order, and the restart matches them."""
        path = tmp_path / "demo.db"
        engine = await engine_for(path, kalshi)
        order = await engine.submit(OrderRequest(
            instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"), price=D("0.02"), book="demo-restart",
        ))
        # Rewind the journal to the moment before the answer came back, which
        # is what a process killed mid-submit leaves behind.
        await engine.journal.mark_intent(order.client_order_id, IntentState.SENDING)
        await engine.journal.close()
        # Kalshi's demo lists a new order a few hundred milliseconds after it
        # accepts it; a restart takes longer than that, and the engine refuses
        # to call an order lost until it has.
        await asyncio.sleep(1.5)

        restarted = await engine_for(path, kalshi)
        try:
            intent = await restarted.journal.intent(order.client_order_id)
            assert intent.state == IntentState.SENT and intent.order_id == order.id
            assert order.id in [o.id for o in restarted.open_orders()]
            open_at_venue = [o.id for o in await kalshi.fetch_open_orders()]
            assert open_at_venue.count(order.id) == 1, "no second order was sent"
        finally:
            await kalshi.cancel_all_orders()
            await restarted.stop()

    async def test_a_halt_cancels_through_the_venue(self, tmp_path, kalshi, market):
        engine = await engine_for(tmp_path / "demo.db", kalshi)
        try:
            for price in ("0.02", "0.03"):
                await engine.submit(OrderRequest(instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"),
                                                 price=D(price), book="demo-halt"))
            result = await engine.halt("demo drill", policy="cancel")
            # Kalshi answers its account-wide cancel with 204 and no count, so
            # the proof is the empty book, not the number.
            assert "kalshi" in result["canceled"] and not str(result["canceled"]["kalshi"]).startswith("failed")
            assert await no_open_orders(kalshi), "the halt left orders resting at the venue"
            with pytest.raises(RiskRejected):
                await engine.submit(OrderRequest(instrument_id=f"{market}:yes", side=Side.BUY, amount=D("1"),
                                                 price=D("0.02"), book="demo-halt"))
        finally:
            await kalshi.cancel_all_orders()
            await engine.stop()

    async def test_the_ledger_reconciles_with_kalshi_after_settlement(self, tmp_path, kalshi):
        """Fills, then settlements, then agreement.

        A fresh journal reads the account's fills and books them, which leaves
        the ledger holding contracts in markets that have since resolved and
        that Kalshi therefore no longer lists as positions. Booking the
        settlements is what closes them, and afterwards every difference the
        reconciler reports belongs to a market that is still open.
        """
        engine = await engine_for(tmp_path / "demo.db", kalshi)
        try:
            await Reconciler(engine).run("kalshi")
            booked = await EndOfDay(engine).run()
            settled = {s.market_id for s in await kalshi.fetch_settlements(limit=500)}
            positions = await kalshi.fetch_positions()
            venue_side = {p.instrument_id: p for p in positions}
            for row in engine.ledger.merged(positions):
                if row["difference"] == D("0"):
                    continue
                assert row["market_id"] not in settled, (
                    f"{row['market_id']} settled, so the ledger should be flat: {row}"
                )
                assert row["instrument_id"] in venue_side or row["engine"] != D("0")
            assert booked.date and booked.fills >= 0
        finally:
            await engine.stop()
