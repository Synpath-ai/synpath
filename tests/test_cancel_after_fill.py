"""Cancelling an order the venue already finished keeps the venue's verdict.

Kalshi answers a cancel of an order that is no longer resting with 404
`not_found`. That is not a confirmation of cancellation: the order may have
filled. These tests drive the real `KalshiTrading` adapter over a mocked
Kalshi (`serve_harness.MockKalshi`) on both storage backends, for a plain
limit order, a stop-limit whose resting child filled, and a stop-market whose
immediate child filled, before and after a restart.
"""
from __future__ import annotations

import os
import uuid
from decimal import Decimal as D
from types import SimpleNamespace

import httpx
import pytest

from serve_harness import KALSHI_TRADE_API, MockKalshi, kalshi_credentials
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.storage_types import IntentState
from synpath.trading.errors import OrderNotFound
from synpath.trading.kalshi import KalshiTrading
from synpath.trading.types import OrderRequest, OrderStatus

pytestmark = pytest.mark.anyio

MARKET = "kalshi:KXHARNESS-1"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=["sqlite", "postgresql"])
async def engine_config(request, tmp_path):
    """An `EngineConfig` factory on each backend; PostgreSQL only with a disposable DSN."""
    if request.param == "sqlite":
        yield lambda: EngineConfig(journal_path=str(tmp_path / "engine.db"))
        return
    dsn = os.environ.get("SYNPATH_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set SYNPATH_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
    psycopg = pytest.importorskip("psycopg")
    schema = "synpath_cancel_" + uuid.uuid4().hex
    try:
        yield lambda: EngineConfig(journal_path=dsn, storage_backend="postgresql", storage_schema=schema)
    finally:
        from psycopg import sql
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


async def started(venue: MockKalshi, config: EngineConfig, credentials) -> Engine:
    http = httpx.AsyncClient(transport=httpx.MockTransport(venue.handle))
    adapter = KalshiTrading(credentials, base_url=KALSHI_TRADE_API, client=http)
    engine = Engine({"kalshi": adapter}, config,
                    risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))
    await engine.start()
    return engine


@pytest.fixture
def credentials():
    return kalshi_credentials()


def limit(**changes) -> OrderRequest:
    base = dict(market_id=MARKET, side="buy", quantity="2", price="0.40", client_order_id="limit-1")
    return OrderRequest(**{**base, **changes})


async def cancel_intent(engine: Engine, order_id: str):
    return await engine.journal.intent(f"cancel:kalshi:{order_id}")


async def feed(engine: Engine, bid: str, ask: str) -> None:
    engine.set_book(MARKET, SimpleNamespace(ready=True, best_bid=D(bid), best_ask=D(ask)))
    await engine.on_book({"market_id": MARKET})


# -- a plain limit order ------------------------------------------------------

async def test_cancel_after_the_venue_filled_a_resting_limit_reports_it_closed(engine_config, credentials):
    venue = MockKalshi()
    engine = await started(venue, engine_config(), credentials)
    try:
        placed = await engine.submit(limit())
        assert placed.status == OrderStatus.OPEN
        venue.fill(placed.id)   # filled at the venue; the engine has not heard yet

        result = await engine.cancel(placed.id)

        assert result.status == OrderStatus.CLOSED and result.filled == D("2") and result.remaining == 0
        stored = await engine.journal.order("kalshi", placed.id)
        assert stored.status == OrderStatus.CLOSED and stored.filled == D("2")
        assert stored.client_order_id == "limit-1", "the engine's own fields survive the read-back"
        assert (await cancel_intent(engine, placed.id)).state == IntentState.SETTLED
        assert engine.open_orders() == []
        assert venue.cancels == [placed.id]
    finally:
        await engine.stop()


async def test_cancel_of_an_order_the_engine_already_knows_filled_sends_nothing(engine_config, credentials):
    venue = MockKalshi(fill_on_create=True)
    engine = await started(venue, engine_config(), credentials)
    try:
        placed = await engine.submit(limit())
        assert placed.status == OrderStatus.CLOSED

        result = await engine.cancel(placed.id)

        assert result.status == OrderStatus.CLOSED and result.filled == D("2")
        assert (await engine.journal.order("kalshi", placed.id)).status == OrderStatus.CLOSED
        assert venue.cancels == [], "a finished order is not cancelled at the venue"
    finally:
        await engine.stop()


async def test_cancel_after_the_venue_cancelled_a_partly_filled_order_keeps_the_fill(engine_config, credentials):
    venue = MockKalshi()
    engine = await started(venue, engine_config(), credentials)
    try:
        placed = await engine.submit(limit())
        venue.orders[placed.id].update(status="canceled", fill_count_fp="1", remaining_count_fp="0",
                                       taker_fill_cost_dollars="0.40")

        result = await engine.cancel(placed.id)

        assert result.status == OrderStatus.CANCELED and result.filled == D("1")
        assert (await engine.journal.order("kalshi", placed.id)).filled == D("1")
    finally:
        await engine.stop()


async def test_a_venue_with_no_record_of_the_order_does_not_confirm_a_cancel(engine_config, credentials):
    venue = MockKalshi()
    engine = await started(venue, engine_config(), credentials)
    try:
        placed = await engine.submit(limit())
        venue.forgotten.add(placed.id)

        with pytest.raises(OrderNotFound, match="unconfirmed"):
            await engine.cancel(placed.id)

        stored = await engine.journal.order("kalshi", placed.id)
        assert stored.status == OrderStatus.OPEN, "the last venue observation stands"
        assert (await cancel_intent(engine, placed.id)).state == IntentState.FAILED
    finally:
        await engine.stop()


# -- engine-held parents --------------------------------------------------------

async def test_cancel_after_a_stop_limit_child_filled_closes_the_parent(engine_config, credentials):
    venue = MockKalshi()
    engine = await started(venue, engine_config(), credentials)
    try:
        parent = await engine.submit(OrderRequest(market_id=MARKET, side="buy", quantity="2", type="stop_limit",
                                                  stop_price="0.45", price="0.46", client_order_id="stop-limit-1"))
        await feed(engine, "0.44", "0.46")
        child_id = engine.managed_order_controller.get(parent.id).children[0].order_id
        assert venue.orders[child_id]["status"] == "resting"
        venue.fill(child_id)   # the resting child fills; no stream tells the engine

        result = await engine.cancel(parent.id)

        assert result.status == OrderStatus.CLOSED and result.filled == D("2")
        child = await engine.journal.order("kalshi", child_id)
        assert child.status == OrderStatus.CLOSED and child.filled == D("2")
        stored = await engine.journal.managed_one(parent.id)
        assert stored[1]["state"] == "done"
    finally:
        await engine.stop()


async def test_a_filled_stop_market_stays_closed_when_cancelled_before_and_after_restart(engine_config, credentials):
    venue = MockKalshi()
    config = engine_config()
    engine = await started(venue, config, credentials)
    try:
        parent = await engine.submit(OrderRequest(market_id=MARKET, side="sell", quantity="2", type="stop_market",
                                                  stop_price="0.40", client_order_id="stop-market-1"))
        await feed(engine, "0.39", "0.41")
        assert len(venue.creates) == 1 and venue.creates[0]["time_in_force"] == "immediate_or_cancel"
        assert (await engine.cancel(parent.id)).status == OrderStatus.CLOSED
    finally:
        await engine.stop()

    restarted = await started(venue, config, credentials)
    try:
        assert restarted.managed_order_controller.get(parent.id) is None, "a finished parent is not restored"
        result = await restarted.cancel(parent.id)
        assert result.status == OrderStatus.CLOSED and result.filled == D("2")
        stored = await restarted.journal.order("kalshi", parent.id)
        assert stored.status == OrderStatus.CLOSED
        assert venue.cancels == [], "neither the parent id nor the finished child goes to the venue"
    finally:
        await restarted.stop()
