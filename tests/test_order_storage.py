"""Shared storage behavior: SQLite is the first backend under this contract."""
from __future__ import annotations

import json
import sqlite3
from decimal import Decimal as D

import pytest

from synpath.errors import BadRequest
from synpath.engine import Engine, EngineConfig, Journal, RiskConfig
from synpath.engine.paper import PaperVenue
from synpath.engine.storage import JournalStore, create_journal
from synpath.trading.types import Account, Order, OrderRequest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def journal(tmp_path):
    async with create_journal(path=str(tmp_path / "journal.db"), user_id="owner-a") as store:
        yield store


def request(kind="limit", **changes):
    fields = dict(market_id="kalshi:KX-A", side="sell", quantity="10", type=kind,
                  user_id="owner-a", client_order_id="request-a", price="0.38")
    if kind.startswith("stop"):
        fields["stop_price"] = "0.40"
    return OrderRequest(**{**fields, **changes})


@pytest.mark.parametrize("kind", ["market", "limit", "stop_market", "stop_limit"])
async def test_each_type_has_its_own_table_with_required_owner(journal, kind):
    req = request(kind)
    account = Account(venue="kalshi", name="paper")
    await journal.record_intent(req, client_order_id="request-a", venue="kalshi", account=account)
    row = await journal.typed_order(kind, "kalshi", "request-a")
    assert row["user_id"] == "owner-a" and row["request"]["quantity"] == "10"
    assert row["state"] == "planned" and row["payload_version"] == 1
    with sqlite3.connect(journal.path) as connection:
        columns = connection.execute(f"PRAGMA table_info({kind}_orders)").fetchall()
    assert next(column for column in columns if column[1] == "user_id")[3] == 1


async def test_instruction_is_persisted_before_send_and_survives_venue_update(journal):
    req = request()
    account = Account(venue="kalshi", name="paper")
    await journal.record_intent(req, client_order_id="request-a", venue="kalshi", account=account)
    order = Order(id="venue-a", client_order_id="request-a", user_id="owner-a", venue="kalshi", account=account,
                  market_id=req.market_id, side=req.side, quantity=D("10"), price=D("0.39"),
                  type="limit", status="closed", time_in_force="gtc", filled=D("4"))
    await journal.upsert_order(order)
    row = await journal.typed_order("limit", "kalshi", "request-a")
    assert row["request"]["price"] == "0.38" and row["price"] == "0.38"
    assert row["snapshot"]["price"] == "0.39" and row["filled"] == "4"
    assert row["venue_order_id"] == "venue-a" and row["state"] == "closed"


async def test_type_record_and_dispatch_intent_rollback_together(journal, monkeypatch):
    original = journal._order_records.write_request

    async def fail_after_type_write(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("interrupted transaction")

    monkeypatch.setattr(journal._order_records, "write_request", fail_after_type_write)
    before = await journal.last_seq()
    with pytest.raises(RuntimeError, match="interrupted"):
        await journal.record_intent(request(), client_order_id="request-a", venue="kalshi", account=Account(venue="kalshi"))
    assert await journal.intent("request-a") is None
    assert await journal.typed_order("limit", "kalshi", "request-a") is None
    assert await journal.last_seq() == before


async def test_old_execution_records_keep_unknown_ownership_and_no_invented_request(tmp_path):
    path = str(tmp_path / "old.db")
    async with Journal(path, user_id="new-owner") as store:
        order = Order(id="old", venue="kalshi", market_id="kalshi:KX-A", side="buy", type="limit",
                      quantity=D("1"), price=None, time_in_force="gtc", status="open")
        await store.upsert_order(order)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM limit_orders")
        connection.execute("DELETE FROM meta WHERE key='order_records_version'")
    for _ in range(2):
        async with Journal(path, user_id="new-owner") as store:
            row = await store.typed_order("limit", "kalshi", "old")
            assert row["user_id"] == "legacy:unattributed" and row["request"] is None
            assert row["price"] is None


async def test_configured_owner_survives_parent_restart_and_is_inherited_by_child(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNPATH_USER_ID", "environment-owner")
    config = EngineConfig(journal_path=str(tmp_path / "engine.db"))
    venue = PaperVenue(venue="kalshi", cash=D("10000"))
    risk = RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None)
    engine = Engine({"kalshi": venue}, config, risk=risk)
    await engine.start()
    parent = await engine.submit(request("stop_market", user_id=None, client_order_id=None))
    assert parent.user_id == "environment-owner" and parent.client_order_id
    before = await engine.journal.typed_order("stop_market", "kalshi", parent.id)
    assert before["state"] == "waiting" and before["user_id"] == "environment-owner"
    await engine.stop()
    restarted = Engine({"kalshi": venue}, config, risk=risk)
    try:
        await restarted.start()
        restored = restarted.orders.get(parent.id)
        assert restored.request.user_id == "environment-owner" and restored.state == "waiting"
        assert restored.child_request(quantity=D("1"), price=D("0.3")).user_id == "environment-owner"
    finally:
        await restarted.stop()


async def test_storage_contract_rejects_unknown_backend_and_owner_spoofing(tmp_path):
    store = create_journal(path=str(tmp_path / "contract.db"))
    assert isinstance(store, JournalStore)
    with pytest.raises(ValueError, match="not available"):
        create_journal(backend="unknown")
    with pytest.raises(ValueError, match="empty"):
        create_journal(user_id=" ")
    venue = PaperVenue(venue="kalshi")
    async with Engine({"kalshi": venue}, EngineConfig(journal_path=str(tmp_path / "engine.db"), user_id="owner-a")) as engine:
        before = await engine.journal.last_seq()
        with pytest.raises(BadRequest, match="assigned"):
            await engine.submit(request(user_id="owner-b"))
        assert await engine.journal.last_seq() == before


async def test_upgrade_restores_unanswered_intent_as_owned_type_instruction(tmp_path):
    path = str(tmp_path / "pending.db")
    async with Journal(path, user_id="owner-a") as store:
        await store.record_intent(request(), client_order_id="request-a", venue="kalshi", account=Account(venue="kalshi"))
        await store.mark_intent("request-a", "sending")
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM limit_orders")
        connection.execute("DELETE FROM meta WHERE key='order_records_version'")
    async with Journal(path, user_id="owner-a") as store:
        row = await store.typed_order("limit", "kalshi", "request-a")
        assert row["state"] == "sending" and row["user_id"] == "owner-a"
        assert row["request"]["price"] == "0.38" and row["venue_order_id"] is None


async def test_failed_migration_closes_the_connection(tmp_path, monkeypatch):
    async def fail(self):
        raise RuntimeError("migration failed")

    monkeypatch.setattr(Journal, "_upgrade_order_records", fail)
    store = Journal(str(tmp_path / "failed.db"))
    with pytest.raises(RuntimeError, match="migration failed"):
        await store.open()
    assert not store.is_open


async def test_managed_type_record_preserves_subaccount_identity(journal):
    account = Account(venue="kalshi", name="desk", subaccount="17")
    snapshot = dict(id="parent", kind="stop_limit", venue="kalshi", state="waiting",
                    account=account.model_dump(mode="json"), request=request("stop_limit").model_dump(mode="json"),
                    filled="2", children=[])
    await journal.save_managed(snapshot)
    row = await journal.typed_order("stop_limit", "kalshi", "parent")
    assert row["account_key"] == "kalshi:desk:17" and row["filled"] == "2" and row["remaining"] == "8"


async def test_stop_recovery_uses_type_snapshot_even_if_legacy_projection_is_stale(journal):
    account = Account(venue="kalshi")
    snapshot = dict(id="parent", kind="stop_market", venue="kalshi", state="working",
                    account=account.model_dump(mode="json"), request=request("stop_market").model_dump(mode="json"),
                    filled="0", children=[])
    await journal.save_managed(snapshot)
    stale = {**snapshot, "state": "waiting"}
    with sqlite3.connect(journal.path) as connection:
        connection.execute("UPDATE managed_orders SET payload=?, state='waiting' WHERE id='parent'", (json.dumps(stale),))
    assert (await journal.managed(states=("working",)))[0]["state"] == "working"
    assert (await journal.managed_one("parent"))[1]["state"] == "working"
    assert (await journal.managed_on("kalshi:KX-A"))[0][1]["state"] == "working"


async def test_legacy_stop_snapshot_does_not_inherit_the_new_workers_identity(tmp_path):
    path = str(tmp_path / "legacy-stop.db")
    snapshot = dict(id="legacy-parent", kind="stop_market", venue="kalshi", state="waiting",
                    account=Account(venue="kalshi").model_dump(mode="json"),
                    request=request("stop_market", user_id=None).model_dump(mode="json"), filled="0", children=[])
    async with Journal(path) as store:
        await store.save_managed(snapshot)
    with pytest.raises(ValueError, match="stored execution owner"):
        async with Journal(path, user_id="new-worker"):
            pass
    with sqlite3.connect(path) as connection:
        stored = json.loads(connection.execute("SELECT snapshot FROM stop_market_orders WHERE id='legacy-parent'").fetchone()[0])
    assert stored["request"]["user_id"] is None


async def test_canceled_bucket_write_rolls_back_and_allows_next_write(journal, monkeypatch):
    import asyncio
    entered = asyncio.Event()
    original = journal._db.execute
    async def pause_member(query, args):
        await original(query, args)
        entered.set()
        await asyncio.Event().wait()
    def execute(query, args=None):
        if query.startswith("INSERT INTO bucket_members"):
            return pause_member(query, args)
        return original(query, args)
    monkeypatch.setattr(journal._db, "execute", execute)
    task = asyncio.create_task(journal.save_bucket({"id": "cancel-bucket", "book": "a", "name": "test",
                                                   "members": [{"market_id": "kalshi:KX-A"}]}))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await journal.bucket("cancel-bucket") is None
    assert await asyncio.wait_for(journal.append("after-cancellation"), 2) > 0
