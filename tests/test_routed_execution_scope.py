"""Cross-venue identity through real engine, HTTP, restart and both stores.

All adapters are PaperVenue; these tests never use exchange credentials.
"""
from __future__ import annotations

import json
from decimal import Decimal as D

import pytest
from fastapi.testclient import TestClient

from synpath.bucket import BucketMember
from synpath.engine import RiskConfig
from synpath.engine.storage import create_journal
from synpath.engine.storage_types import IntentState
from synpath.errors import BadRequest
from synpath.server.order_access import may_view_event
from synpath.server.store import ControlStore
from synpath.server.trading import create_trading_app
from synpath.trading.errors import RiskRejected, SubmissionPending
from synpath.trading.types import Account, Order, OrderRequest, OrderType, Side

from test_routed_limit import build, book, request, K, P, ACCOUNT_K, ACCOUNT_P
from test_routed_recovery import resting_legs, new_engine
from test_owner_migrations import configuration, config  # noqa: F401
from test_postgres_storage import database  # noqa: F401

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def scope_engine(tmp_path):
    engine, kalshi, poly, clock, bucket = await build(tmp_path)
    book(kalshi, engine, K, asks=[(D("0.44"), D("100"))])
    book(poly, engine, P, asks=[(D("0.40"), D("100"))])
    yield engine, kalshi, poly, clock, bucket
    await engine.stop()


async def test_reordering_members_preserves_owner_scope_and_nonfirst_allocation(scope_engine):
    engine, kalshi, poly, _, bucket = scope_engine
    first = await engine.submit(request(bucket, quantity=D("10"), client_order_id="original"))
    reverse = bucket.model_copy(update={"members": list(reversed(bucket.members))})
    await engine.save_bucket(reverse)
    second = await engine.submit(request(reverse, quantity=D("10"), client_order_id="reversed"))
    assert first.venue is second.venue is None and first.account is second.account is None
    assert first.execution_accounts == second.execution_accounts == {"kalshi": ACCOUNT_K, "polymarket": ACCOUNT_P}
    assert first.user_id == second.user_id == engine.config.user_id
    assert not kalshi.orders and [o.quantity for o in poly.orders.values()] == [D("10"), D("10")]
    assert all(o.venue == "polymarket" and o.account == ACCOUNT_P for o in poly.orders.values())
    for venue in ("kalshi", "polymarket"):
        rows = await engine.journal.orders_page(venue=venue)
        assert sum(o.id == first.id for o, _ in rows) == 1
    assert (await engine.journal.order(None, first.id)).execution_accounts == first.execution_accounts


async def test_flipped_child_uses_member_account_and_native_side(tmp_path):
    engine, kalshi, poly, _, bucket = await build(tmp_path, flip_p=True)
    try:
        book(poly, engine, P, bids=[(D("0.70"), D("100"))])
        book(kalshi, engine, K, asks=[(D("0.40"), D("100"))])
        parent = await engine.submit(request(bucket, quantity=D("10"), outcome="yes", action="buy"))
        child = next(iter(poly.orders.values()))
        assert parent.venue is None and not kalshi.orders
        assert (child.account, child.side, child.price, child.outcome) == (ACCOUNT_P, Side.SELL, D("0.70"), "no")
    finally:
        await engine.stop()


@pytest.mark.parametrize("rule", ["capacity", "halt", "restricted"])
async def test_nonfirst_destination_risk_refuses_before_dispatch(scope_engine, rule):
    engine, kalshi, poly, _, bucket = scope_engine
    if rule == "capacity":
        # A 50/50 split fits but the agreed full-capacity policy refuses 100.
        engine.order_risk_checks.configure(RiskConfig(max_position_contracts=D("60"), price_collar=None))
    elif rule == "halt":
        engine.order_risk_checks.kill.engage(reason="test", scope="polymarket", policy="hold")
    else:
        engine.order_risk_checks.configure(RiskConfig(restricted=[P], price_collar=None))
    with pytest.raises(RiskRejected):
        await engine.submit(request(bucket, quantity=D("100")))
    assert not kalshi.orders and not poly.orders


async def test_capture_is_used_once_across_authorization_and_concurrent_bucket_edit(scope_engine, monkeypatch):
    engine, kalshi, poly, _, bucket = scope_engine
    original_read = engine.journal.bucket
    reads = []

    async def read_then_edit(bucket_id):
        row = await original_read(bucket_id)
        reads.append(bucket_id)
        await engine.save_bucket(bucket.model_copy(update={"members": [
            BucketMember(market_id=K), BucketMember(market_id="polymarket_us:new")]}))
        return row

    monkeypatch.setattr(engine.journal, "bucket", read_then_edit)
    authorized = []
    parent = await engine.submit(request(bucket), authorize_execution_accounts=lambda accounts: authorized.append(set(accounts)))
    assert reads == [bucket.id] and authorized == [{"kalshi", "polymarket"}]
    assert set(parent.execution_accounts) == {"kalshi", "polymarket"}
    assert poly.orders and not kalshi.orders


async def test_same_client_id_retry_keeps_original_snapshot_after_bucket_edit(scope_engine):
    engine, _, poly, _, bucket = scope_engine
    instruction = request(bucket, client_order_id="retry")
    first = await engine.submit(instruction)
    await engine.save_bucket(bucket.model_copy(update={"members": [
        BucketMember(market_id=K), BucketMember(market_id=P, enabled=False)]}))
    again = await engine.submit(instruction)
    assert first.id == again.id and first.execution_accounts == again.execution_accounts
    assert len(poly.orders) == 1
    with pytest.raises(BadRequest, match="different instruction"):
        await engine.submit(instruction.model_copy(update={"quantity": D("1")}))


async def test_singular_parent_account_and_nullable_venue_order_are_rejected(scope_engine):
    engine, _, _, _, bucket = scope_engine
    with pytest.raises(BadRequest, match="singular account"):
        await engine.submit(request(bucket, account=ACCOUNT_K))
    with pytest.raises(ValueError, match="captured execution accounts"):
        Order(id="bad", venue=None, market_id=K, side="buy", type="limit", time_in_force="gtc", status="open", quantity="1")


async def test_legacy_client_id_without_verifiable_reservation_cannot_create_another_parent(scope_engine):
    engine, _, poly, _, bucket = scope_engine
    instruction = request(bucket, client_order_id="legacy-client")
    parent = await engine.submit(instruction)
    await engine.journal._db.execute("DELETE FROM submissions WHERE client_order_id=?", (instruction.client_order_id,))
    with pytest.raises(BadRequest, match="older bucket order"):
        await engine.submit(instruction)
    assert len(poly.orders) == 1 and (await engine._known(parent.id, None)).id == parent.id


@pytest.mark.parametrize("supplied_scope", [{"kalshi": ACCOUNT_K.model_dump(mode="json")}, {"kalshi": "malformed"}])
async def test_caller_scope_params_never_authorize_the_full_reserved_request(scope_engine, tmp_path, supplied_scope):
    engine, _, _, _, bucket = scope_engine
    parent = await engine.submit(request(bucket, params={"execution_accounts": supplied_scope}))
    store = await ControlStore(str(tmp_path / "audit-control.db")).open()
    try:
        owner, _ = await store.bootstrap("owner")
        viewer = await store.create_user("partial")
        await store.acting_as(f"user:{owner.id}", "test")
        await store.grant(viewer.id, ACCOUNT_K.key, "view")
        key = await store.issue_key(viewer.id)
        principal = await store.principal(key.secret)
        events = [event async for event in engine.journal.replay(0)]
        reserved = next(event for event in events if event.kind == "submission.reserved")
        assert set(reserved.payload["effective_request"]["params"]["execution_accounts"]) == {"kalshi", "polymarket"}
        assert not await may_view_event(principal, engine, reserved)
        with TestClient(create_trading_app(engine, store)) as client:
            response = client.get("/journal", headers={"Authorization": f"Bearer {key.secret}"})
            assert response.status_code == 200 and parent.id not in response.text
            assert "effective_request" not in response.text
    finally:
        await store.close()


async def test_changed_adapter_identity_blocks_bulk_reads_and_halt(scope_engine, monkeypatch):
    engine, _, poly, _, bucket = scope_engine
    engine.books.clear()
    await engine.submit(request(bucket))
    # Keep the parent active but change only the actual adapter's account.
    poly.account = Account(venue="polymarket", name="replacement")
    calls = []

    async def wrong_account(*args, **kwargs):
        calls.append("wrong-account I/O")
        raise AssertionError("must not contact replacement account")

    for name in ("fetch_open_orders", "fetch_my_trades", "fetch_positions", "fetch_balance", "cancel_all_orders"):
        monkeypatch.setattr(poly, name, wrong_account)
    with pytest.raises(SubmissionPending):
        await engine.poll("polymarket")
    from synpath.engine.reconcile import Reconciler
    with pytest.raises(SubmissionPending):
        await Reconciler(engine).run("polymarket")
    result = await engine.halt("test", scope="polymarket")
    assert "failed" in result["canceled"]["polymarket"] and calls == []


async def test_parent_detail_reports_logs_and_legacy_events_use_saved_scope(scope_engine, tmp_path):
    engine, _, _, _, bucket = scope_engine
    parent = await engine.submit(request(bucket))
    store = await ControlStore(str(tmp_path / "control.db")).open()
    try:
        owner, owner_key = await store.bootstrap("owner")
        partial = await store.create_user("partial")
        await store.acting_as(f"user:{owner.id}", "test grants")
        await store.grant(partial.id, ACCOUNT_K.key, "view")
        await store.grant(partial.id, ACCOUNT_K.key, "trade")
        key = await store.issue_key(partial.id, label="one venue")
        partial_principal = await store.principal(key.secret)
        auth = {"Authorization": f"Bearer {key.secret}"}
        full = {"Authorization": f"Bearer {owner_key.secret}"}
        with TestClient(create_trading_app(engine, store)) as client:
            assert client.get(f"/orders/{parent.id}", headers=auth).status_code == 403
            assert client.delete(f"/orders/{parent.id}", headers=auth).status_code == 403
            assert client.patch(f"/orders/{parent.id}", headers=auth, json={"price": "0.43"}).status_code == 403
            assert client.get(f"/order-log?order_id={parent.id}", headers=auth).status_code == 403
            for path in ("/journal", "/order-log", "/orders?scope=all"):
                assert parent.id not in json.dumps(client.get(path, headers=auth).json())
            assert client.get(f"/orders/{parent.id}", headers=full).json()["execution_accounts"]["polymarket"]
            # The saved scope still governs history after editing the current definition.
            await engine.save_bucket(bucket.model_copy(update={"members": [
                BucketMember(market_id=K), BucketMember(market_id=P, enabled=False)]}))
            reports = client.get(f"/buckets/{bucket.id}/orders", headers=auth)
            assert parent.id not in reports.text
            assert client.get(f"/buckets/{bucket.id}/orders/{parent.id}", headers=auth).status_code == 403
            await store.grant(partial.id, ACCOUNT_P.key, "view")
            await engine.save_bucket(bucket.model_copy(update={"members": [
                BucketMember(market_id=K), BucketMember(market_id="polymarket_us:later")]}))
            # Access to the original history cannot depend on newly added accounts.
            assert client.get(f"/buckets/{bucket.id}/orders/{parent.id}", headers=auth).status_code == 200
            assert parent.id in client.get(f"/buckets/{bucket.id}/orders", headers=auth).text
        from synpath.engine.storage_types import JournalEvent
        principal = partial_principal
        legacy = JournalEvent(1, 1, "managed.working", parent.id, {"id": parent.id, "venue": "kalshi", "account": ACCOUNT_K.model_dump()})
        assert not await may_view_event(principal, engine, legacy)
    finally:
        await store.close()


async def test_changed_account_blocks_recovery_reads_cancels_and_false_lost(tmp_path, monkeypatch):
    engine, kalshi, poly, clock, path, parent = await resting_legs(tmp_path)
    managed = engine.managed_order_controller.get(parent.id)
    child = managed.children[1]
    child_order = await engine.journal.order(child.venue, child.order_id)
    pending = OrderRequest(market_id=P, side=Side.BUY, quantity=D("1"), price=D("0.42"),
                           account=ACCOUNT_P, tags={"parent": parent.id}, client_order_id="uncertain")
    await engine.journal.record_intent(pending, client_order_id="uncertain", venue="polymarket", account=ACCOUNT_P)
    await engine.journal.mark_intent("uncertain", IntentState.SENDING)
    await engine.journal.close()
    calls = []

    async def wrong_account(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("must not contact the replacement account")

    for name in ("fetch_order", "fetch_open_orders", "fetch_orders", "cancel_order"):
        monkeypatch.setattr(poly, name, wrong_account)
    restarted = new_engine(path, kalshi, poly, clock)
    restarted.accounts["polymarket"] = Account(venue="polymarket", name="replacement")
    await restarted.start()
    try:
        await restarted.managed_order_controller.on_timer()
        with pytest.raises(SubmissionPending, match="differs"):
            await restarted.cancel(child.order_id, venue=child.venue)
        assert calls == []
        assert (await restarted.journal.intent("uncertain")).state == IntentState.SENDING
        assert (await restarted.journal.order(child.venue, child.order_id)).status == child_order.status
        assert restarted.managed_order_controller.get(parent.id).live
    finally:
        await restarted.stop()


@pytest.mark.parametrize("failure_boundary", ["membership", "projection", "event"])
async def test_atomic_parent_snapshot_scope_projection_rollback_on_both_stores(config, tmp_path, monkeypatch, failure_boundary):
    engine, _, _, _, bucket = await build(tmp_path)
    try:
        parent_order = await engine.submit(request(bucket, client_order_id="atomic"))
        snapshot = engine.managed_order_controller.get(parent_order.id).snapshot()
        async with create_journal(**config, user_id=engine.config.user_id) as store:
            execute = store._db.execute

            def fail(query, params=()):
                match = {"membership": "INSERT INTO parent_execution_accounts", "projection": "INSERT INTO orders(",
                         "event": "INSERT INTO events("}[failure_boundary]
                if query.startswith(match):
                    raise RuntimeError("injected write failure")
                return execute(query, params)

            monkeypatch.setattr(store._db, "execute", fail)
            with pytest.raises(RuntimeError, match="injected"):
                await store.save_managed(snapshot, order=parent_order)
            monkeypatch.setattr(store._db, "execute", execute)
            assert await store.managed_one(parent_order.id) is None
            assert await store.order(None, parent_order.id) is None
            assert await store.routed_account_keys("kalshi") == []
            assert await store.last_seq() == 0
            await store.save_managed(snapshot, order=parent_order)
            assert (await store.order(None, parent_order.id)).execution_accounts == parent_order.execution_accounts
            assert (await store.managed_one(parent_order.id))[1]["execution_accounts"]
    finally:
        await engine.stop()


@pytest.mark.parametrize("historical_evidence", ["complete", "missing", "explicit", "conflict"])
async def test_legacy_nominal_parent_migration_preserves_or_refuses_accounts(config, tmp_path, historical_evidence):
    engine, _, _, _, bucket = await build(tmp_path)
    try:
        parent_order = await engine.submit(request(bucket))
        snapshot = engine.managed_order_controller.get(parent_order.id).snapshot()
        # Make an actual pre-PRE-6 snapshot and projection, keeping its original IDs.
        snapshot.update(venue="kalshi", account=ACCOUNT_K.model_dump(mode="json"))
        snapshot.pop("execution_accounts")
        snapshot["request"]["account"] = ACCOUNT_K.model_dump(mode="json")
        snapshot["request"]["params"].pop("execution_accounts")
        legacy_projection = parent_order.model_dump(mode="json")
        legacy_projection.update(venue="kalshi", account=ACCOUNT_K.model_dump(mode="json"))
        legacy_projection.pop("execution_accounts")
        async with create_journal(**config, user_id=engine.config.user_id) as store:
            await store.save_managed(snapshot)
            await store.upsert_order(Order.model_validate(legacy_projection))
            if historical_evidence in ("complete", "conflict"):
                child = Order(id="historical-child", venue="polymarket", account=ACCOUNT_P, market_id=P,
                              side="buy", type="limit", time_in_force="gtc", status="open", quantity="1",
                              tags={"parent": parent_order.id}, user_id=engine.config.user_id)
                await store.upsert_order(child)
            if historical_evidence in ("explicit", "conflict"):
                account = ACCOUNT_P if historical_evidence == "explicit" else Account(venue="polymarket", name="conflict")
                await store._db.execute("INSERT INTO meta(key,value) VALUES('legacy_execution_accounts',?)",
                                        (json.dumps({parent_order.id: {"polymarket": account.model_dump(mode="json")}}),))
            await store._db.execute("DROP TABLE parent_execution_accounts")
            await store._db.execute("UPDATE alembic_version SET version_num='0005_drop_fair_values'")
            await store._db.execute("UPDATE meta SET value='10' WHERE key='schema_version'")
        reopened = create_journal(**config, user_id=engine.config.user_id)
        if historical_evidence in ("missing", "conflict"):
            with pytest.raises(ValueError, match="historical execution accounts|conflicting historical"):
                await reopened.open()
            assert not reopened.is_open
            # Retry using a reviewed explicit mapping for the missing venue.
            if historical_evidence == "missing":
                from synpath.engine.database import connect_sqlite, connect_postgres
                connection = await (connect_sqlite(config["path"]) if config["backend"] == "sqlite"
                                    else connect_postgres(config["path"]))
                if config["backend"] == "postgresql":
                    await connection.execute(f'SET search_path TO "{config["schema"]}", pg_catalog')
                async with connection.execute("SELECT value FROM meta WHERE key='schema_version'") as cursor:
                    assert (await cursor.fetchone())["value"] == "10"
                await connection.execute("INSERT INTO meta(key,value) VALUES('legacy_execution_accounts',?)",
                    (json.dumps({parent_order.id: {"polymarket": ACCOUNT_P.model_dump(mode="json")}}),))
                await connection.close()
            else:
                return
        async with reopened:
            migrated = await reopened.order(None, parent_order.id)
            assert migrated.id == parent_order.id and migrated.venue is None and migrated.account is None
            assert migrated.execution_accounts == {"kalshi": ACCOUNT_K, "polymarket": ACCOUNT_P}
            assert (await reopened.managed_one(parent_order.id))[1]["legacy_execution_identity"]["venue"] == "kalshi"
            assert await reopened.order("kalshi", parent_order.id) is None
            assert sum(order.id == parent_order.id for order, _ in await reopened.orders_page()) == 1
        async with reopened:
            assert (await reopened.order(None, parent_order.id)).execution_accounts == migrated.execution_accounts
    finally:
        await engine.stop()
