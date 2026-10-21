"""Ownership and Alembic upgrade contracts on both real database backends."""
import importlib
import json
import sqlite3

import pytest
from synpath.engine.storage import create_journal
from synpath.engine.database import connect_sqlite, connect_postgres
from synpath.trading.types import Account
from test_order_storage import request
from test_postgres_storage import database

pytestmark = pytest.mark.anyio

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture(params=["sqlite", "postgresql"])
def configuration(request, tmp_path):
    if request.param == "sqlite":
        return dict(backend="sqlite", path=str(tmp_path / "journal.db"))
    return dict(backend="postgresql")

@pytest.fixture
def config(configuration, request):
    if configuration["backend"] == "postgresql":
        dsn, schema = request.getfixturevalue("database")
        configuration.update(path=dsn, schema=schema)
    return configuration

async def test_singleton_and_all_instruction_foreign_keys(config):
    async with create_journal(**config, user_id="owner-a") as store:
        async with store._db.execute("SELECT user_id FROM users") as cursor:
            assert [row["user_id"] for row in await cursor.fetchall()] == ["owner-a"]
        assert await store.schema_version() == 10
        async with store._db.execute("SELECT version_num FROM alembic_version") as cursor:
            assert (await cursor.fetchone())["version_num"] == "0005_drop_fair_values"
        for kind in ("market", "limit", "stop_market", "stop_limit"):
            req = request(kind, client_order_id="key-" + kind)
            await store.record_intent(req, client_order_id=req.client_order_id, venue="kalshi", account=Account(venue="kalshi"))
            with pytest.raises(Exception) as error:
                await store._db.execute(f"UPDATE {kind}_orders SET user_id='unknown'")
            assert "foreign key" in str(error.value).lower()
        with pytest.raises(Exception) as error:
            await store._db.execute("INSERT INTO users(user_id) VALUES('second')")
        assert "unique" in str(error.value).lower() or "duplicate" in str(error.value).lower()
    with pytest.raises(ValueError, match="stored execution owner"):
        async with create_journal(**config, user_id="owner-b"):
            pass
    async with create_journal(**config, user_id="owner-a") as store:
        assert (await store.intent("key-stop_market")).state == "planned"


async def legacy_connection(config):
    if config["backend"] == "sqlite":
        connection = await connect_sqlite(config["path"])
    else:
        connection = await connect_postgres(config["path"])
        await connection.execute(f'CREATE SCHEMA "{config["schema"]}"')
        await connection.execute(f'SET search_path TO "{config["schema"]}", pg_catalog')
    source = importlib.import_module("synpath.engine.migrations.versions.0001_journal_v6").SCHEMA
    if config["backend"] == "postgresql":
        import re
        source = re.sub(r"\bINTEGER\b", "BIGINT", source.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY"))
    await connection.executescript(source)
    for key, value in (("schema_version", "6"), ("order_records_version", "1"), ("submissions_version", "1")):
        await connection.execute("INSERT INTO meta(key,value) VALUES(?,?)", (key, value))
    return connection

@pytest.mark.parametrize("owner,has_instruction", [("owner-a", True), ("legacy:unattributed", False), ("other", True)])
async def test_baseline_adoption_preserves_or_refuses_without_relabeling(config, owner, has_instruction):
    connection = await legacy_connection(config)
    body = request(user_id=owner).model_dump(mode="json")
    await connection.execute("""INSERT INTO limit_orders(venue,id,user_id,account_key,market_id,side,quantity,state,time_in_force,price,request,snapshot,created_ts,updated_ts)
                              VALUES('kalshi','old',?,'kalshi:default','kalshi:KX-A','sell','10','planned','gtc','0.38',?, ?,1,1)""",
                             (owner, json.dumps(body) if has_instruction else None, json.dumps(body)))
    await connection.close()
    if owner == "other":
        store = create_journal(**config, user_id="owner-a")
        with pytest.raises(ValueError, match="ownership conflicts"):
            await store.open()
        assert not store.is_open
        connection = await (connect_sqlite(config["path"]) if config["backend"] == "sqlite" else connect_postgres(config["path"]))
        if config["backend"] == "postgresql":
            await connection.execute(f'SET search_path TO "{config["schema"]}", pg_catalog')
        async with connection.execute("SELECT user_id FROM limit_orders WHERE id='old'") as cursor:
            assert (await cursor.fetchone())["user_id"] == "other"
        async with connection.execute("SELECT value FROM meta WHERE key='schema_version'") as cursor:
            assert (await cursor.fetchone())["value"] == "6"
        await connection.close()
        return
    for _ in range(2):
        async with create_journal(**config, user_id="owner-a") as store:
            row = await store.typed_order("limit", "kalshi", "old")
            assert row["user_id"] == owner
            assert row["request"] == (body if has_instruction else None)
            assert await store.schema_version() == 10


async def test_buckets_stored_before_enabled_existed_load_with_every_member_on(config):
    connection = await legacy_connection(config)
    await connection.execute("INSERT INTO buckets(id,book,name,status,created_ts,updated_ts) VALUES('b','a','old','active',1,1)")
    for position, (market_id, flip) in enumerate((("kalshi:KX-A", 0), ("polymarket:1", 1))):
        await connection.execute("INSERT INTO bucket_members(bucket_id,market_id,flip,position) VALUES(?,?,?,?)",
                                 ("b", market_id, flip, position))
    await connection.close()
    async with create_journal(**config, user_id="owner-a") as store:
        assert await store.schema_version() == 10
        members = (await store.bucket("b"))["members"]
        assert [(m["market_id"], m["flip"], m["enabled"]) for m in members] == [
            ("kalshi:KX-A", False, True), ("polymarket:1", True, True)]
        await store.save_bucket({**(await store.bucket("b")), "members": [
            {"market_id": "kalshi:KX-A"}, {"market_id": "polymarket:1", "flip": True, "enabled": False}]})
        assert [m["enabled"] for m in (await store.bucket("b"))["members"]] == [True, False]


async def test_orders_from_before_0004_get_a_creation_time_and_page_newest_first(config):
    connection = await legacy_connection(config)
    for order_id, created, updated in (("dated", 5, 6), ("undated", None, 9), ("older", 2, 3)):
        payload = {"id": order_id, "venue": "kalshi", "market_id": "kalshi:KX-A", "side": "buy", "type": "limit", "time_in_force": "gtc",
                   "status": "closed", "quantity": "1", "filled": "1"}
        await connection.execute(
            "INSERT INTO orders(venue,id,account_key,market_id,side,status,terminal,quantity,filled,payload,"
            "created_ts,updated_ts) VALUES('kalshi',?,'kalshi:default','kalshi:KX-A','buy','closed',1,'1','1',?,?,?)",
            (order_id, json.dumps(payload), created, updated))
    await connection.close()
    async with create_journal(**config, user_id="owner-a") as store:
        assert await store.schema_version() == 10
        rows = await store.orders_page(limit=2)
        assert [(order.id, key[0]) for order, key in rows] == [("undated", 9), ("dated", 5)]
        rest = await store.orders_page(limit=2, before=rows[-1][1])
        assert [order.id for order, _ in rest] == ["older"]
        assert [order.id for order, _ in await store.orders_page(since_ts=5)] == ["undated", "dated"]
        if config["backend"] == "sqlite":
            query = "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='orders'"
        else:
            query = f"SELECT indexname AS name FROM pg_indexes WHERE schemaname='{config['schema']}' AND tablename='orders'"
        async with store._db.execute(query) as cursor:
            names = {row["name"] for row in await cursor.fetchall()}
        assert {"orders_recent", "orders_market_recent"} <= names


async def test_injected_journal_and_engine_must_share_owner(config):
    from synpath.engine import Engine, EngineConfig
    async with create_journal(**config, user_id="owner-a") as journal:
        connection = journal._db
        assert await journal.open() is journal and journal._db is connection
        with pytest.raises(ValueError, match="same user_id"):
            Engine({}, EngineConfig(user_id="owner-b"), journal=journal)


async def test_canceled_migration_rolls_back_then_reopens(config, monkeypatch):
    import asyncio
    import synpath.engine.migration as migration
    original = migration.upgrade
    entered = asyncio.Event()
    async def pause(database, user_id):
        await original(database, user_id)
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(migration, "upgrade", pause)
    store = create_journal(**config, user_id="owner-a")
    opening = asyncio.create_task(store.open())
    await asyncio.wait_for(entered.wait(), 5)
    opening.cancel()
    with pytest.raises(asyncio.CancelledError):
        await opening
    assert not store.is_open
    monkeypatch.setattr(migration, "upgrade", original)
    async with create_journal(**config, user_id="owner-a") as restarted:
        assert await restarted.schema_version() == 10
        async with restarted._db.execute("SELECT user_id FROM users") as cursor:
            assert (await cursor.fetchone())["user_id"] == "owner-a"


async def test_cancellation_stays_cancellation_when_driver_operation_fails():
    import asyncio
    from synpath.engine.database import finish_database_operation
    entered, release = asyncio.Event(), asyncio.Event()
    async def fails():
        entered.set()
        await release.wait()
        raise RuntimeError("driver failed while caller was canceled")
    task = asyncio.create_task(finish_database_operation(fails()))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_repeated_cancellation_keeps_begin_rollback_locked():
    import asyncio
    connection = await connect_sqlite(":memory:")
    begin_entered, begin_release = asyncio.Event(), asyncio.Event()
    rollback_entered, rollback_release = asyncio.Event(), asyncio.Event()
    driver = connection.connection
    class DelayedConnection:
        def __getattr__(self, name):
            return getattr(driver, name)
        async def exec_driver_sql(self, query, *args):
            if query.startswith("BEGIN"):
                begin_entered.set()
                await begin_release.wait()
            elif query == "ROLLBACK":
                rollback_entered.set()
                await rollback_release.wait()
            return await driver.exec_driver_sql(query, *args)
    connection.connection = DelayedConnection()
    await connection.execute("CREATE TABLE observed(value INTEGER)")
    async def begin():
        await connection.execute("BEGIN IMMEDIATE")
    opening = asyncio.create_task(begin())
    write = None
    try:
        await begin_entered.wait()
        opening.cancel()
        begin_release.set()
        await rollback_entered.wait()
        opening.cancel()
        async def insert():
            await connection.execute("INSERT INTO observed VALUES(1)")
        write = asyncio.create_task(insert())
        await asyncio.sleep(0.01)
        assert connection.lock.locked() and not write.done()
        rollback_release.set()
        with pytest.raises(asyncio.CancelledError):
            await opening
        await write
        async with connection.execute("SELECT value FROM observed") as cursor:
            assert (await cursor.fetchone())["value"] == 1
    finally:
        begin_release.set()
        rollback_release.set()
        await asyncio.gather(opening, *([write] if write else []), return_exceptions=True)
        await connection.close()
