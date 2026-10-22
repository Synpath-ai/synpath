"""Real PostgreSQL contracts, opt-in with SYNPATH_TEST_POSTGRES_DSN.

The DSN must point at a disposable test database. Each test owns a unique
schema; no public schema, exchange credentials or real venue is used.
Portable SQLite scenarios are reused so both backends face the same claims.
"""
from __future__ import annotations

import asyncio
import inspect
import os
import subprocess
import sys
import uuid
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.postgres import PostgresJournal
from synpath.engine.storage import JournalStore, create_journal
from synpath.engine.storage_types import LeaseLost
from synpath.errors import BadRequest
from synpath.trading.types import Account
import test_order_storage as storage_scenarios
import test_stop_lifecycle as lifecycle_scenarios
import test_engine_orders as order_scenarios

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def database():
    dsn = os.environ.get("SYNPATH_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set SYNPATH_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
    psycopg = pytest.importorskip("psycopg")
    schema = "synpath_test_" + uuid.uuid4().hex
    try:
        yield dsn, schema
    finally:
        from psycopg import sql
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as connection:
            await connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
async def journal(database):
    dsn, schema = database
    async with PostgresJournal(dsn, schema=schema, user_id="owner-a") as store:
        yield store


PORTABLE_STORAGE = [
    storage_scenarios.test_instruction_is_persisted_before_send_and_survives_venue_update,
    storage_scenarios.test_type_record_and_dispatch_intent_rollback_together,
    storage_scenarios.test_managed_type_record_preserves_subaccount_identity,
    storage_scenarios.test_canceled_bucket_write_rolls_back_and_allows_next_write,
]


@pytest.mark.parametrize("scenario", PORTABLE_STORAGE, ids=lambda fn: fn.__name__)
async def test_shared_storage_contract(scenario, journal, monkeypatch):
    arguments = {"journal": journal, "monkeypatch": monkeypatch}
    await scenario(**{name: arguments[name] for name in inspect.signature(scenario).parameters})


@pytest.mark.parametrize("kind", ["market", "limit", "stop_market", "stop_limit"])
async def test_type_schema_enforces_owner_and_roundtrips_instruction(journal, kind):
    request = storage_scenarios.request(kind)
    await journal.record_intent(request, client_order_id=request.client_order_id, venue="kalshi", account=Account(venue="kalshi"))
    record = await journal.typed_order(kind, "kalshi", request.client_order_id)
    assert record["user_id"] == "owner-a" and record["request"]["quantity"] == "10"
    assert record["payload_version"] == 1 and record["state"] == "planned"
    async with journal._db.execute("SELECT is_nullable FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name='user_id'", (journal.schema, kind + "_orders")) as cursor:
        assert (await cursor.fetchone())["is_nullable"] == "NO"
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation):
        await journal._db.execute(f"UPDATE {kind}_orders SET user_id=' '")
    assert (await journal.typed_order(kind, "kalshi", request.client_order_id))["user_id"] == "owner-a"


# These scenarios use the same scripted venue, requests and assertions as
# SQLite, substituting only EngineConfig's persistence selection.
PORTABLE_LIFECYCLE = [
    getattr(lifecycle_scenarios, name) for name in [
        "test_retry_same_stop_returns_original_parent_even_after_restart",
        "test_concurrent_retries_create_one_parent_and_conflicts_do_not_send",
        "test_ordinary_retry_and_lost_response_never_send_twice",
        "test_an_unresolved_retry_stays_pending_without_another_send",
        "test_trigger_and_child_plan_are_committed_before_adapter_call",
        "test_lost_child_response_stays_triggered_and_is_adopted_after_restart",
        "test_cancel_and_trigger_race_leaves_no_working_child",
        "test_cancel_acknowledgement_with_nonterminal_result_stays_pending",
        "test_order_history_pages_are_exhausted_before_adoption",
        "test_cancel_a_planned_child_never_dispatches_it",
        "test_expired_planned_stop_limit_does_not_send_after_restart",
        "test_cancellation_of_uncertain_child_survives_restart",
        "test_a_fill_that_wins_the_cancel_race_closes_the_parent",
        "test_risk_refusal_is_durable_on_retry",
        "test_sell_stop_market_notional_limit_counts_the_no_price",
        "test_unexpected_decoder_error_after_send_preserves_uncertain_stop",
    ]
]


@pytest.fixture
def postgres_engine_config(database, monkeypatch):
    dsn, schema = database
    def configured(**kwargs):
        return EngineConfig(**{**kwargs, "journal_path": dsn, "storage_backend": "postgresql", "storage_schema": schema})
    monkeypatch.setattr(lifecycle_scenarios, "EngineConfig", configured)
    return configured


@pytest.mark.parametrize("scenario", PORTABLE_LIFECYCLE, ids=lambda fn: fn.__name__)
async def test_shared_stop_lifecycle(scenario, postgres_engine_config, tmp_path):
    await scenario(tmp_path)


@pytest.fixture
async def setup(postgres_engine_config):
    from synpath.engine.paper import PaperVenue, quadratic_fee
    clock = order_scenarios.Clock()
    paper = PaperVenue(venue="kalshi", fees=quadratic_fee(), cash=D("10000"), clock=clock)
    engine = Engine({"kalshi": paper}, postgres_engine_config(require_lease=True, managed_tick_s=0.01),
                    risk=RiskConfig(price_collar=None, duplicate_window_ms=0, max_orders_per_minute=None,
                                    closing_soon_s=None, max_open_orders=None),
                    accounts={"kalshi": order_scenarios.ACCOUNT}, clock=clock)
    paper.subscribe(engine.on_fill)
    paper.subscribe_orders(engine.on_order)
    await engine.start()
    try:
        yield engine, paper, clock
    finally:
        await engine.stop()


PAPER_SCENARIOS = [getattr(order_scenarios.TestStops(), name) for name in vars(order_scenarios.TestStops)
                   if name.startswith("test_") and name != "test_the_old_stop_params_are_refused"] + [
    lifecycle_scenarios.test_day_retry_keeps_the_original_expiry,
    lifecycle_scenarios.test_expiry_wins_when_price_arrives_before_the_timer,
    lifecycle_scenarios.test_stop_market_uses_deep_available_liquidity_and_reports_execution_average,
    lifecycle_scenarios.test_registered_market_precision_controls_the_persisted_price_bound,
    lifecycle_scenarios.test_crossed_book_and_replayed_trade_do_not_trigger,
]


@pytest.mark.parametrize("scenario", PAPER_SCENARIOS, ids=lambda fn: fn.__name__)
async def test_shared_paper_execution(scenario, setup):
    await scenario(setup)


async def test_atomic_child_plan_rolls_back(setup, monkeypatch):
    await lifecycle_scenarios.test_atomic_plan_rolls_back_both_parent_and_child(setup, monkeypatch)


@pytest.mark.parametrize("boundary", ["before_send", "after_acceptance", "after_response"])
async def test_hard_crash_recovery(database, tmp_path, boundary):
    dsn, schema = database
    venue_path = tmp_path / "durable-fake.json"
    script = f"DSN={dsn!r}\nSCHEMA={schema!r}\nVENUE={str(venue_path)!r}\nBOUNDARY={boundary!r}\n" + '''
import asyncio, os
import test_stop_lifecycle as scenarios
from synpath.engine import EngineConfig
original_config = EngineConfig
def configured(**kwargs):
    return original_config(**{**kwargs, 'journal_path': DSN, 'storage_backend': 'postgresql', 'storage_schema': SCHEMA})
scenarios.EngineConfig = configured
async def crash():
    engine, venue = await scenarios.scripted_engine('unused', scenarios.DurableFakeVenue(VENUE))
    await engine.submit(scenarios.stop_request())
    if BOUNDARY == 'before_send':
        original = engine.journal.mark_intent
        async def exit_before_send(*args, **kwargs):
            if args[1] == 'sending': os._exit(77)
            return await original(*args, **kwargs)
        engine.journal.mark_intent = exit_before_send
    elif BOUNDARY == 'after_acceptance':
        original = venue.create_order
        async def exit_after_acceptance(request):
            await original(request)
            os._exit(77)
        venue.create_order = exit_after_acceptance
    else:
        original = engine.journal.upsert_order
        async def exit_after_response(order, **kwargs):
            if order.held_by.value == 'venue': os._exit(77)
            return await original(order, **kwargs)
        engine.journal.upsert_order = exit_after_response
    await scenarios.cross(engine)
asyncio.run(crash())
'''
    environment = {**os.environ, "PYTHONPATH": str(Path(__file__).parent) + os.pathsep + os.environ.get("PYTHONPATH", "")}
    completed = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script], env=environment,
                                       timeout=20, capture_output=True, text=True)
    assert completed.returncode == 77, completed.stderr
    async with PostgresJournal(dsn, schema=schema) as store:
        await store._db.execute("UPDATE leases SET expires_ts=0")
    venue = lifecycle_scenarios.DurableFakeVenue(venue_path)
    engine, _ = await lifecycle_scenarios.scripted_engine(dsn, venue, storage_backend="postgresql", storage_schema=schema)
    try:
        parent = engine.managed_order_controller.live()[0]
        assert len(parent.children) == 1 and parent.pending_child is None
        for _ in range(2):
            await engine.managed_order_controller.on_timer()
        assert len(venue.orders) == 1
        assert len(venue.sent) == (1 if boundary == "before_send" else 0)
    finally:
        await engine.stop()


async def test_concurrent_initialization_and_lease_takeover(database):
    dsn, schema = database
    first, second = PostgresJournal(dsn, schema=schema), PostgresJournal(dsn, schema=schema)
    await asyncio.gather(first.open(), second.open())
    try:
        results = await asyncio.gather(first.acquire_lease(), second.acquire_lease(), return_exceptions=True)
        assert sum(isinstance(result, LeaseLost) for result in results) == 1
        winner, loser = (first, second) if first.lease else (second, first)
        await winner._db.execute("UPDATE leases SET expires_ts=0")
        assert not await winner.renew_lease(), "an expired owner cannot revive itself"
        await loser.acquire_lease()
        await winner.release_lease()
        assert await loser.holds_lease() and not await winner.holds_lease()
    finally:
        await first.close()
        await second.close()


async def test_server_clock_and_no_dispatch_after_lease_loss(database, monkeypatch):
    engine, venue = await lifecycle_scenarios.scripted_engine(database[0], storage_backend="postgresql", storage_schema=database[1])
    try:
        import synpath.engine.journal as module
        monkeypatch.setattr(module, "now_ms", lambda: 1)
        assert await engine.journal.renew_lease() and await engine.journal.holds_lease()
        assert engine.journal.lease.expires_ts > 1_000_000_000_000
        await engine.journal._db.execute("UPDATE leases SET owner='replacement'")
        with pytest.raises(LeaseLost):
            await engine.submit(lifecycle_scenarios.stop_request())
        assert venue.sent == []
    finally:
        await engine.stop()


async def test_task_cancellation_rolls_back_and_unblocks_reader(journal, monkeypatch):
    original = journal._order_records.write_request
    entered = asyncio.Event()
    async def pause(*args, **kwargs):
        await original(*args, **kwargs)
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(journal._order_records, "write_request", pause)
    writer = asyncio.create_task(journal.record_intent(storage_scenarios.request(), client_order_id="request-a", venue="kalshi", account=Account(venue="kalshi")))
    await asyncio.wait_for(entered.wait(), 2)
    reader = asyncio.create_task(journal.typed_order("limit", "kalshi", "request-a"))
    await asyncio.sleep(0.01)
    assert not reader.done(), "same-session readers must not see uncommitted instruction state"
    writer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await writer
    assert await asyncio.wait_for(reader, 2) is None
    assert await journal.intent("request-a") is None
    assert await journal.append("healthy") > 0


async def test_reservations_are_atomic_across_connections(database):
    first = PostgresJournal(database[0], schema=database[1], user_id="owner-a")
    second = PostgresJournal(database[0], schema=database[1], user_id="owner-a")
    await asyncio.gather(first.open(), second.open())
    req = storage_scenarios.request()
    try:
        await asyncio.gather(*(store.record_submission(req, req, venue="kalshi", account=Account(venue="kalshi")) for store in (first, second)))
        assert len([event async for event in first.replay(kinds=["submission.reserved"])]) == 1
        conflict = req.model_copy(update={"quantity": D("11")})
        with pytest.raises(BadRequest, match="different durable"):
            await second.record_submission(conflict, conflict, venue="kalshi", account=Account(venue="kalshi"))
        assert (await first.submission(req.client_order_id))["effective_request"]["quantity"] == "10"
    finally:
        await first.close()
        await second.close()


@pytest.mark.parametrize("version_key", ["schema_version", "order_records_version", "submissions_version"])
async def test_reopen_preserves_data_and_rejects_unknown_version(database, version_key):
    dsn, schema = database
    async with PostgresJournal(dsn, schema=schema) as store:
        seq = await store.append("preserved", {"value": 1})
        version = await store.save_config("risk", {"max": "10"})
        await store.set_cursor("cursor", "page-2")
        await store.save_bucket({"id": "b", "book": "a", "name": "bucket", "members": [
            {"market_id": "kalshi:KX-A", "flip": True}, {"market_id": "polymarket:1", "enabled": False}]})
    async with PostgresJournal(dsn, schema=schema) as store:
        assert await store.last_seq() >= seq
        assert await store.config("risk") == (version, {"max": "10"})
        assert await store.cursor("cursor") == "page-2"
        assert (await store.bucket("b"))["members"] == [{"market_id": "kalshi:KX-A", "flip": True, "enabled": True},
                                                        {"market_id": "polymarket:1", "flip": False, "enabled": False}]
        assert await store.archive_bucket("b") and not await store.archive_bucket("b")
        await store._db.execute("UPDATE meta SET value='999' WHERE key=?", (version_key,))
    invalid = PostgresJournal(dsn, schema=schema)
    with pytest.raises(ValueError, match="unsupported PostgreSQL journal"):
        await invalid.open()
    assert not invalid.is_open


def test_factory_requires_explicit_dsn_and_hides_credentials():
    with pytest.raises(ValueError, match="explicit connection"):
        create_journal(backend="postgresql")
    store = create_journal(backend="postgresql", path="postgresql://user:secret@localhost/db", schema="orders")
    assert isinstance(store, JournalStore) and store.path == "postgresql:orders"
    for schema in ('unsafe; DROP TABLE orders', "a" * 64, "pg_catalog", "information_schema"):
        with pytest.raises(ValueError, match="schema name"):
            PostgresJournal("postgresql://localhost/db", schema=schema)


# Keep adapter-specific payload, asynchronous receipt and response-loss
# assertions intact while running their real engine against PostgreSQL.
import functools
import synpath.engine as engine_module
import test_polymarket_stop_lifecycle as poly_scenarios
import test_polymarket_us_stop_lifecycle as us_scenarios
import test_kalshi_stop_verification as kalshi_scenarios
import examples.verify_kalshi_stop_limit as kalshi_example
import examples.verify_polymarket_us_stop_fill as us_example
from test_polymarket_trading import venue
from test_polymarket_us_trading import ed_key, ed_secret
from test_kalshi_trading import creds, pem, rsa_key


@pytest.fixture
def postgres_adapter_config(postgres_engine_config, monkeypatch):
    for module in (engine_module, poly_scenarios, kalshi_example, us_example):
        monkeypatch.setattr(module, "EngineConfig", postgres_engine_config)


def postgres_adapter_scenario(scenario):
    @pytest.mark.usefixtures("postgres_adapter_config")
    @functools.wraps(scenario)
    async def run(*args, **kwargs):
        await scenario(*args, **kwargs)
    return run


test_pg_kalshi_stop_limit = postgres_adapter_scenario(kalshi_scenarios.test_stop_limit_adapter_arms_restores_triggers_and_cleans_only_its_child)
test_pg_kalshi_lost_response = postgres_adapter_scenario(kalshi_scenarios.test_response_lost_still_finds_and_cancels_only_the_accepted_child)
test_pg_polymarket_stop_limit = postgres_adapter_scenario(poly_scenarios.test_waiting_restart_limit_child_direction_and_targeted_cancel)
test_pg_polymarket_stop_market = postgres_adapter_scenario(poly_scenarios.test_stop_market_child_uses_fak_and_remains_pending_until_fill_readback)
test_pg_polymarket_lost_response = postgres_adapter_scenario(poly_scenarios.test_response_lost_never_resends_but_needs_signed_identity_recovery)
test_pg_us_stop_market = postgres_adapter_scenario(us_scenarios.test_fractional_stop_market_waits_crosses_fills_and_closes_only_new_position)
test_pg_us_bounds = postgres_adapter_scenario(us_scenarios.test_subcent_market_grid_still_respects_us_absolute_bounds)


async def test_failed_schema_initialization_rolls_back_then_retries(database, monkeypatch):
    import psycopg
    store = PostgresJournal(database[0], schema=database[1])
    original = store._apply_schema
    async def fail():
        await original()
        raise RuntimeError("schema checkpoint interrupted")
    monkeypatch.setattr(store, "_apply_schema", fail)
    with pytest.raises(RuntimeError, match="interrupted"):
        await store.open()
    assert not store.is_open
    async with await psycopg.AsyncConnection.connect(database[0], autocommit=True) as connection:
        cursor = await connection.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name=%s", (database[1],))
        assert await cursor.fetchone() is None
    monkeypatch.setattr(store, "_apply_schema", original)
    async with store:
        assert await store.schema_version() == 10


async def test_commit_failure_rolls_back_before_next_writer(journal, monkeypatch):
    original = journal._db._execute
    failed = False
    async def fail_commit(query, *args, **kwargs):
        nonlocal failed
        if query == "COMMIT" and not failed:
            failed = True
            raise RuntimeError("commit refused")
        return await original(query, *args, **kwargs)
    monkeypatch.setattr(journal._db, "_execute", fail_commit)
    with pytest.raises(RuntimeError, match="commit refused"):
        await journal.record_intent(storage_scenarios.request(), client_order_id="request-a", venue="kalshi", account=Account(venue="kalshi"))
    assert await journal.intent("request-a") is None
    assert await journal.typed_order("limit", "kalshi", "request-a") is None
    assert await asyncio.wait_for(journal.append("next-writer"), 2) > 0


async def test_audit_failure_rolls_back_submission_before_dispatch(database):
    import psycopg
    engine, venue = await lifecycle_scenarios.scripted_engine(database[0], storage_backend="postgresql", storage_schema=database[1])
    try:
        await engine.journal._db.execute("""CREATE FUNCTION refuse_audit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.kind = 'submission.reserved' THEN RAISE EXCEPTION 'simulated audit failure'; END IF;
            RETURN NEW; END $$""")
        await engine.journal._db.execute("CREATE TRIGGER refuse_audit BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION refuse_audit()")
        with pytest.raises(psycopg.errors.RaiseException, match="simulated audit failure"):
            await engine.submit(lifecycle_scenarios.stop_request())
        assert await engine.journal.submission("protect-a") is None
        assert engine.managed_order_controller.parents == {} and venue.sent == []
    finally:
        await engine.stop()


async def test_sidecar_selects_postgres_without_recording_connection_secret(database, tmp_path):
    import json
    from synpath.server.serve import build
    stack = await build(config={"storage_backend": "postgresql", "storage_schema": database[1], "journal": database[0]},
                        control=str(tmp_path / "control.db"), home_dir=str(tmp_path / "home"),
                        adapters={}, streams=False)
    try:
        assert isinstance(stack.engine.journal, PostgresJournal)
        registry = json.loads((tmp_path / "home" / "servers.json").read_text())
        assert registry["127.0.0.1:8000"]["journal"] == "postgresql:" + database[1]
        assert database[0] not in json.dumps(registry)
    finally:
        await stack.engine.stop()
        await stack.store.close()


async def test_missing_driver_is_explicit_and_does_not_fall_back(monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg", None)
    store = PostgresJournal("postgresql://localhost/test")
    with pytest.raises(ImportError, match="synpath\\[postgres\\]"):
        await store.open()
    assert not store.is_open


import test_kalshi_stop_fill as kalshi_fill_scenarios
import examples.verify_kalshi_stop_fill as kalshi_fill_example

@pytest.fixture
def postgres_kalshi_fill_config(postgres_engine_config, monkeypatch):
    monkeypatch.setattr(kalshi_fill_example, "EngineConfig", postgres_engine_config)

@pytest.mark.usefixtures("postgres_kalshi_fill_config")
@functools.wraps(kalshi_fill_scenarios.test_real_price_crossing_fills_once_then_closes_only_test_quantity)
async def test_pg_kalshi_stop_fill(*args, **kwargs):
    await kalshi_fill_scenarios.test_real_price_crossing_fills_once_then_closes_only_test_quantity(*args, **kwargs)


test_pg_us_already_eligible = postgres_adapter_scenario(us_scenarios.test_already_eligible_stop_uses_next_real_observation)

@pytest.mark.usefixtures("postgres_kalshi_fill_config")
@functools.wraps(kalshi_fill_scenarios.test_price_wait_renews_lease_before_sending)
async def test_pg_kalshi_fill_lease(*args, **kwargs):
    await kalshi_fill_scenarios.test_price_wait_renews_lease_before_sending(*args, **kwargs)
