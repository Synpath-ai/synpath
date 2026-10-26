"""Post-write rollback and executable legacy routed-parent upgrade contracts."""
import asyncio
import copy
from decimal import Decimal as D

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.storage import create_journal
from synpath.trading.types import Order, OrderRequest, Side
from test_owner_migrations import configuration, config  # noqa: F401
from test_postgres_storage import database  # noqa: F401
from test_routed_limit import build, request, ACCOUNT_K, ACCOUNT_P, K
from test_routed_recovery import resting_legs

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize("operation", ["save", "plan"])
@pytest.mark.parametrize("interruption", ["failure", "cancellation"])
async def test_parent_transaction_rolls_back_after_all_component_writes(config, tmp_path, monkeypatch, operation, interruption):
    source, _, _, _, bucket = await build(tmp_path)
    try:
        original = request(bucket, client_order_id="post-write-atomic", user_id=source.config.user_id)
        parent = await source.submit(original)
        snapshot = source.managed_order_controller.get(parent.id).snapshot()
        effective = OrderRequest.stored(snapshot["request"])
        child = OrderRequest(market_id=K, side="buy", quantity="1", price="0.40", account=ACCOUNT_K,
                             user_id=parent.user_id, client_order_id="planned-child", tags={"parent": parent.id})
        store = create_journal(**config, user_id=parent.user_id)
        async with store:
            await store.record_submission(original, effective, venue="__managed__", account=None)
            baseline_seq = await store.last_seq()
            entered = asyncio.Event()
            helper_name = "_write_order" if operation == "save" else "_write_intent"
            helper = getattr(store, helper_name)

            async def interrupt_after_write(*args, **kwargs):
                await helper(*args, **kwargs)
                entered.set()
                if interruption == "failure":
                    raise RuntimeError("failure after all component writes")
                await asyncio.Event().wait()

            monkeypatch.setattr(store, helper_name, interrupt_after_write)
            action = (store.save_managed(snapshot, order=parent) if operation == "save" else
                      store.plan_managed_child(snapshot, child, venue="kalshi", account=ACCOUNT_K, parent_order=parent))
            task = asyncio.create_task(action)
            await asyncio.wait_for(entered.wait(), 5)
            if interruption == "cancellation":
                task.cancel()
            with pytest.raises(asyncio.CancelledError if interruption == "cancellation" else RuntimeError):
                await task
            monkeypatch.setattr(store, helper_name, helper)
            assert await store.managed_one(parent.id) is None
            assert await store.order(None, parent.id) is None
            assert await store.routed_account_keys("kalshi") == []
            assert await store.intent("planned-child") is None
            assert await store.last_seq() == baseline_seq
            reservation = await store.submission(original.client_order_id)
            assert reservation["result_id"] is None and reservation["state"] == "pending"
        async with store:
            assert await store.managed_one(parent.id) is None
            assert await store.order(None, parent.id) is None
            assert (await store.submission(original.client_order_id))["result_id"] is None
            await store.save_managed(snapshot, order=parent)
            assert (await store.submission(original.client_order_id))["result_id"] == parent.id
            assert (await store.order(None, parent.id)).execution_accounts == parent.execution_accounts
    finally:
        await source.stop()


async def test_migrated_working_parent_restores_and_cancels_original_children(config, tmp_path):
    source, kalshi, poly, clock, _, original = await resting_legs(tmp_path)
    try:
        kalshi.on_trade(K, price=D("0.41"), quantity=D("10"), taker_side=Side.SELL)
        await kalshi.deliver()
        managed = source.managed_order_controller.get(original.id)
        assert managed.filled == D("10") and managed.state == "working"
        snapshot = copy.deepcopy(managed.snapshot())
        projection = managed.as_order().model_dump(mode="json")
        children = await source.journal.orders_for_parent(original.id)
        snapshot.update(venue="kalshi", account=ACCOUNT_K.model_dump(mode="json"))
        snapshot.pop("execution_accounts")
        snapshot["request"]["account"] = ACCOUNT_K.model_dump(mode="json")
        snapshot["request"]["params"].pop("execution_accounts")
        projection.update(venue="kalshi", account=ACCOUNT_K.model_dump(mode="json"))
        projection.pop("execution_accounts")
        async with create_journal(**config, user_id=original.user_id) as store:
            await store.save_managed(snapshot)
            await store.upsert_order(Order.model_validate(projection))
            for child in children:
                await store.upsert_order(child)
            await store._db.execute("DROP TABLE parent_execution_accounts")
            await store._db.execute("UPDATE alembic_version SET version_num='0005_drop_fair_values'")
            await store._db.execute("UPDATE meta SET value='10' WHERE key='schema_version'")
    finally:
        await source.stop()

    journal = create_journal(**config, user_id=original.user_id)
    restored = Engine({"kalshi": kalshi, "polymarket": poly},
                      EngineConfig(user_id=original.user_id, require_lease=True), journal=journal,
                      accounts={"kalshi": ACCOUNT_K, "polymarket": ACCOUNT_P}, clock=clock,
                      risk=RiskConfig(price_collar=None, closing_soon_s=None))
    for paper in (kalshi, poly):
        paper.listeners.clear()
        paper.order_listeners.clear()
        paper.subscribe(restored.on_fill)
        paper.subscribe_orders(restored.on_order)
    await restored.start()
    try:
        parent = restored.managed_order_controller.get(original.id)
        assert parent is not None and parent.live
        assert parent.execution_accounts == {"kalshi": ACCOUNT_K, "polymarket": ACCOUNT_P}
        assert parent.filled == D("10")
        assert {(c.venue, c.order_id) for c in parent.children} == {(c.venue, c.id) for c in children}
        result = await restored.cancel(original.id)
        assert result.status.value == "canceled" and result.filled == D("10")
        assert not [order for paper in (kalshi, poly) for order in paper.orders.values() if not order.is_terminal]
        assert len(kalshi.orders) == len(poly.orders) == 1
        for child in await journal.orders_for_parent(original.id):
            assert child.account == {"kalshi": ACCOUNT_K, "polymarket": ACCOUNT_P}[child.venue]
        assert (await journal.order(None, original.id)).filled == D("10")
        assert await journal.order("kalshi", original.id) is None
    finally:
        await restored.stop()
