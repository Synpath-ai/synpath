"""Stop transactions, retries, races and recovery using fake venues only.

No test constructs a credentialed adapter. Crash tests use a file-backed
fake venue, so acceptance survives an abruptly killed worker.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.submissions import SubmissionPending
from synpath.errors import BadRequest, NetworkError
from synpath.trading.types import OrderRequest, OrderStatus, Precision
from synpath.trading.instruments import MarketSpec
from synpath.types import Page

from test_engine import ScriptedVenue
from test_engine_orders import setup, book, feed_book, request

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def scripted_engine(path, venue=None, **config):
    venue = venue or ScriptedVenue()
    engine = Engine({"kalshi": venue}, EngineConfig(journal_path=str(path), **config),
                    risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))
    await engine.start()
    engine.set_book("kalshi:KX-A", SimpleNamespace(ready=True, best_bid=D("0.39"), best_ask=D("0.41")))
    return engine, venue


def stop_request(**changes):
    base = dict(market_id="kalshi:KX-A", side="sell", quantity="10", type="stop_market",
                stop_price="0.40", client_order_id="protect-a")
    return OrderRequest(**{**base, **changes})


async def test_retry_same_stop_returns_original_parent_even_after_restart(tmp_path):
    path = tmp_path / "engine.db"
    engine, venue = await scripted_engine(path)
    original = await engine.submit(stop_request())
    repeated = await engine.submit(stop_request(quantity="10.000", stop_price="0.400"))
    assert repeated.id == original.id and len(engine.orders.parents) == 1
    await engine.stop()
    restarted, _ = await scripted_engine(path, venue)
    try:
        assert (await restarted.submit(stop_request())).id == original.id
        assert venue.sent == []
    finally:
        await restarted.stop()


async def test_concurrent_retries_create_one_parent_and_conflicts_do_not_send(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    try:
        parents = await asyncio.gather(*(engine.submit(stop_request()) for _ in range(12)))
        assert len({parent.id for parent in parents}) == 1
        with pytest.raises(BadRequest, match="different instruction"):
            await engine.submit(stop_request(quantity="11"))
        with pytest.raises(BadRequest, match="different instruction"):
            await engine.submit(stop_request(type="stop_limit", price="0.38"))
        assert venue.sent == []
    finally:
        await engine.stop()


async def test_ordinary_retry_and_lost_response_never_send_twice(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    req = OrderRequest(market_id="kalshi:KX-A", side="buy", quantity="1", price="0.40", client_order_id="limit-a")
    try:
        first = await engine.submit(req)
        assert (await engine.submit(req)).id == first.id
        assert len(venue.sent) == 1
        venue.fail_next = "arrived"
        uncertain = req.model_copy(update={"client_order_id": "limit-b"})
        with pytest.raises(NetworkError):
            await engine.submit(uncertain)
        recovered = await engine.submit(uncertain)
        assert recovered.client_order_id == "limit-b" and len(venue.sent) == 2
    finally:
        await engine.stop()


async def test_an_unresolved_retry_stays_pending_without_another_send(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db", lost_after_s=0)
    req = OrderRequest(market_id="kalshi:KX-A", side="buy", quantity="1", price="0.40", client_order_id="lost-a")
    try:
        venue.fail_next = "lost"
        with pytest.raises(NetworkError):
            await engine.submit(req)
        for _ in range(3):
            with pytest.raises(SubmissionPending):
                await engine.submit(req)
        assert venue.sent == ["lost-a"]
    finally:
        await engine.stop()


async def test_day_retry_keeps_the_original_expiry(setup):
    engine, paper, clock = setup
    req = request(side="sell", type="stop_market", stop_price="0.40", time_in_force="day", client_order_id="day-a")
    first = await engine.submit(req)
    expiry = engine.orders.get(first.id).request.expires_at
    clock.tick(86400)
    repeated = await engine.submit(req)
    assert repeated.id == first.id and repeated.expires_at == expiry
    await engine.orders.on_timer()
    assert engine.orders.get(first.id).state == "expired" and paper.fills == []


async def test_expiry_wins_when_price_arrives_before_the_timer(setup):
    engine, paper, clock = setup
    expires = int((clock() + 1) * 1000)
    parent = await engine.submit(request(side="sell", type="stop_market", stop_price="0.40", time_in_force="gtd", expires_at=expires))
    clock.tick(1)
    book(paper, engine, bids=[(D("0.39"), D("100"))], asks=[(D("0.41"), D("100"))])
    await feed_book(engine, paper=paper)
    restored = engine.orders.get(parent.id)
    assert restored.state == "expired" and restored.children == []


async def test_trigger_and_child_plan_are_committed_before_adapter_call(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    parent = await engine.submit(stop_request())
    original = venue.create_order
    async def inspect(request):
        snapshot = (await engine.journal.managed_one(parent.id))[1]
        intent = await engine.journal.intent(request.client_order_id)
        assert snapshot["state"] == "working" and snapshot["extra"]["pending_child"]["client_order_id"] == request.client_order_id
        assert intent.state == "sending" and request.tags["parent"] == parent.id
        return await original(request)
    venue.create_order = inspect
    try:
        await engine.on_book({"market_id": "kalshi:KX-A"})
        assert len(venue.sent) == 1
    finally:
        await engine.stop()


async def test_atomic_plan_rolls_back_both_parent_and_child(setup, monkeypatch):
    engine, paper, clock = setup
    parent = await engine.submit(request(side="sell", type="stop_market", stop_price="0.40"))
    managed = engine.orders.get(parent.id)
    snapshot = {**managed.snapshot(), "state": "working"}
    child = managed.child_request(quantity=D("10"), price=D("0.01"), type="market", time_in_force="ioc")
    child = child.model_copy(update={"client_order_id": "child-a"})
    original = engine.journal._write_intent
    async def fail(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("interrupt transaction")
    monkeypatch.setattr(engine.journal, "_write_intent", fail)
    with pytest.raises(RuntimeError):
        await engine.journal.plan_managed_child(snapshot, child, venue=managed.venue, account=managed.account)
    assert (await engine.journal.managed_one(parent.id))[1]["state"] == "waiting"
    assert await engine.journal.intent("child-a") is None
    assert await engine.journal.typed_order("market", "kalshi", "child-a") is None
    assert paper.fills == []


async def test_lost_child_response_stays_triggered_and_is_adopted_after_restart(tmp_path):
    path = tmp_path / "engine.db"
    engine, venue = await scripted_engine(path)
    parent = await engine.submit(stop_request())
    venue.fail_next = "arrived"
    await engine.on_book({"market_id": "kalshi:KX-A"})
    assert engine.orders.get(parent.id).state == "working" and len(venue.sent) == 1
    await engine.stop()
    restarted, _ = await scripted_engine(path, venue)
    try:
        restored = restarted.orders.get(parent.id)
        assert len(restored.children) == 1 and restored.pending_child is None
        for _ in range(3):
            await restarted.orders.on_timer()
            await restarted.on_book({"market_id": "kalshi:KX-A"})
        assert len(venue.sent) == 1
        assert restored.children[0].order_id == "V1"
    finally:
        await restarted.stop()


async def test_stop_market_uses_deep_available_liquidity_and_reports_execution_average(setup):
    engine, paper, clock = setup
    parent = await engine.submit(request(side="sell", type="stop_market", stop_price="0.40"))
    book(paper, engine, bids=[(D("0.39"), D("4")), (D("0.20"), D("6"))], asks=[(D("0.41"), D("10"))])
    await feed_book(engine, paper=paper)
    result = engine.orders.get(parent.id).as_order()
    assert result.status == OrderStatus.CLOSED and result.filled == D("10")
    assert result.average_price == D("0.276"), "the fill average is not the 0.01 child limit"
    child = (await engine.journal.orders_for_parent(parent.id))[0]
    assert child.price == D("0.01") and child.parent_id == parent.id


async def test_registered_market_precision_controls_the_persisted_price_bound(setup):
    engine, paper, clock = setup
    specification = MarketSpec(market_id="kalshi:KX-A", venue="kalshi", precision=Precision(tick=D("0.001")), tradable=True, status="open")
    engine.market_master.put([specification])
    parent = await engine.submit(request(type="stop_market", stop_price="0.50"))
    book(paper, engine, bids=[(D("0.49"), D("10"))], asks=[(D("0.51"), D("10"))])
    await feed_book(engine, paper=paper)
    child = (await engine.journal.orders_for_parent(parent.id))[0]
    assert child.price == D("0.999")


async def test_cancel_and_trigger_race_leaves_no_working_child(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    parent = await engine.submit(stop_request(type="stop_limit", price="0.38"))
    try:
        await asyncio.wait_for(asyncio.gather(engine.on_book({"market_id": "kalshi:KX-A"}), engine.cancel(parent.id)), 2)
        assert engine.orders.get(parent.id).state == "canceled"
        assert await venue.fetch_open_orders() == []
        assert len(venue.sent) <= 1
    finally:
        await engine.stop()


async def test_cancel_acknowledgement_with_nonterminal_result_stays_pending(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    parent = await engine.submit(stop_request(type="stop_limit", price="0.38"))
    await engine.on_book({"market_id": "kalshi:KX-A"})
    original = venue.cancel_order
    async def acknowledge(order_id, **kwargs):
        return venue.orders[order_id]
    venue.cancel_order = acknowledge
    try:
        result = await engine.cancel(parent.id)
        assert result.status == OrderStatus.PENDING_CANCEL
        venue.cancel_order = original
        await engine.orders.on_timer()
        assert engine.orders.get(parent.id).state == "canceled"
    finally:
        await engine.stop()


async def test_order_history_pages_are_exhausted_before_adoption(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    parent = await engine.submit(stop_request())
    venue.fail_next = "arrived"
    await engine.on_book({"market_id": "kalshi:KX-A"})
    child = venue.orders["V1"].model_copy(update={"status": OrderStatus.CLOSED, "filled": D("10"), "average_price": D("0.39")})
    venue.orders["V1"] = child
    cursors = []
    async def history(**kwargs):
        cursors.append(kwargs.get("cursor"))
        return Page([], next_cursor="next") if kwargs.get("cursor") is None else Page([child])
    venue.fetch_orders = history
    try:
        await engine.orders.on_timer()
        assert cursors == [None, "next"]
        assert engine.orders.get(parent.id).state == "done" and len(venue.sent) == 1
    finally:
        await engine.stop()


async def test_no_child_dispatch_after_lease_loss(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    try:
        with sqlite3.connect(engine.config.journal_path) as connection:
            connection.execute("UPDATE leases SET owner='replacement'")
        from synpath.engine import LeaseLost
        with pytest.raises(LeaseLost):
            await engine.submit(stop_request())
        assert venue.sent == []
    finally:
        await engine.stop()


class DurableFakeVenue(ScriptedVenue):
    """Acceptance is durable outside the worker process, like a real venue."""
    def __init__(self, path):
        super().__init__()
        self.path = Path(path)
        if self.path.exists():
            from synpath.trading.types import Order
            self.orders = {row["id"]: Order.model_validate(row) for row in json.loads(self.path.read_text())}
    async def create_order(self, request):
        order = await super().create_order(request)
        with self.path.open("w") as file:
            json.dump([value.model_dump(mode="json") for value in self.orders.values()], file)
            file.flush()
            os.fsync(file.fileno())
        return order


@pytest.mark.parametrize("boundary", ["before_send", "after_acceptance", "after_response"])
async def test_hard_crash_restores_one_child_at_each_network_boundary(tmp_path, boundary):
    journal = tmp_path / "worker.db"
    venue_path = tmp_path / "fake-venue.json"
    script = '''
import asyncio, os
from test_stop_lifecycle import scripted_engine, stop_request, DurableFakeVenue
async def crash():
    engine, venue = await scripted_engine(JOURNAL, DurableFakeVenue(VENUE))
    await engine.submit(stop_request())
    if BOUNDARY == 'before_send':
        original = engine.journal.mark_intent
        async def stop_before_mark(*args, **kwargs):
            if args[1] == 'sending':
                os._exit(77)
            return await original(*args, **kwargs)
        engine.journal.mark_intent = stop_before_mark
    elif BOUNDARY == 'after_acceptance':
        original = venue.create_order
        async def accept_then_stop(request):
            await original(request)
            os._exit(77)
        venue.create_order = accept_then_stop
    else:
        original = engine.journal.upsert_order
        async def stop_before_record(order, **kwargs):
            if order.held_by.value == 'venue':
                os._exit(77)
            return await original(order, **kwargs)
        engine.journal.upsert_order = stop_before_record
    await engine.on_book({'market_id': 'kalshi:KX-A'})
asyncio.run(crash())
'''
    script = f'JOURNAL={str(journal)!r}\nVENUE={str(venue_path)!r}\nBOUNDARY={boundary!r}\n' + script
    environment = dict(os.environ)
    environment['PYTHONPATH'] = str(Path(__file__).parent) + os.pathsep + environment.get('PYTHONPATH', '')
    completed = subprocess.run([sys.executable, '-c', script], env=environment, timeout=15, capture_output=True, text=True)
    assert completed.returncode == 77, completed.stderr
    with sqlite3.connect(journal) as connection:
        connection.execute("UPDATE leases SET expires_ts=0")
    venue = DurableFakeVenue(venue_path)
    engine, _ = await scripted_engine(journal, venue)
    try:
        parent = engine.orders.live()[0]
        assert len(parent.children) == 1 and parent.pending_child is None
        assert len(venue.orders) == 1
        for _ in range(2):
            await engine.orders.on_timer()
        assert len(venue.orders) == 1
        assert len(venue.sent) == (1 if boundary == 'before_send' else 0)
    finally:
        await engine.stop()


async def test_legacy_checkpoint_client_key_is_reserved_during_migration(tmp_path):
    path = tmp_path / "old.db"
    engine, venue = await scripted_engine(path)
    original = await engine.submit(stop_request())
    await engine.stop()
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM submissions")
        connection.execute("DELETE FROM meta WHERE key='submissions_version'")
    restarted, _ = await scripted_engine(path, venue)
    try:
        assert (await restarted.submit(stop_request())).id == original.id
        assert len(restarted.orders.parents) == 1 and venue.sent == []
    finally:
        await restarted.stop()


async def test_cancel_a_planned_child_never_dispatches_it(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    parent = await engine.submit(stop_request())
    managed = engine.orders.get(parent.id)
    managed.state = "working"
    child = managed.child_request(quantity=D("10"), price=D("0.01"), type="market", time_in_force="ioc")
    child = child.model_copy(update={"client_order_id": "planned-child"})
    managed.pending_child = child
    await engine.journal.plan_managed_child(managed.snapshot(), child, venue=managed.venue, account=managed.account)
    try:
        result = await engine.cancel(parent.id)
        assert result.status == OrderStatus.CANCELED and venue.sent == []
        assert (await engine.journal.intent(child.client_order_id)).state == "settled"
        from synpath.trading.errors import OrderRejected
        with pytest.raises(OrderRejected):
            await engine.submit(child)
        assert venue.sent == []
    finally:
        await engine.stop()


async def test_expired_planned_stop_limit_does_not_send_after_restart(tmp_path):
    path = tmp_path / "engine.db"
    engine, venue = await scripted_engine(path)
    parent = await engine.submit(stop_request(type="stop_limit", price="0.38", time_in_force="gtd", expires_at=int((engine.clock() + 60) * 1000)))
    managed = engine.orders.get(parent.id)
    managed.state = "working"
    managed.pending_child = managed.child_request(quantity=D("10"), price=D("0.38"), type="limit", time_in_force="gtd", expires_at=managed.request.expires_at).model_copy(update={"client_order_id": "planned-expiry"})
    await engine.journal.plan_managed_child(managed.snapshot(), managed.pending_child, venue=managed.venue, account=managed.account)
    await engine.stop()
    config = EngineConfig(journal_path=str(path))
    restarted = Engine({"kalshi": venue}, config, clock=lambda: managed.request.expires_at / 1000 + 1)
    try:
        await restarted.start()
        assert restarted.orders.get(parent.id).state == "expired"
        assert venue.sent == []
    finally:
        await restarted.stop()


async def test_cancellation_of_uncertain_child_survives_restart(tmp_path):
    path = tmp_path / "engine.db"
    engine, venue = await scripted_engine(path)
    parent = await engine.submit(stop_request(type="stop_limit", price="0.38"))
    venue.fail_next = "arrived"
    await engine.on_book({"market_id": "kalshi:KX-A"})
    original = venue.fetch_open_orders
    async def unavailable(**kwargs):
        raise NetworkError("fake venue unavailable")
    venue.fetch_open_orders = unavailable
    canceled = await engine.cancel(parent.id)
    assert canceled.status == OrderStatus.PENDING_CANCEL
    await engine.stop()
    venue.fetch_open_orders = original
    restarted, _ = await scripted_engine(path, venue)
    try:
        assert restarted.orders.get(parent.id).state == "canceled"
        assert await venue.fetch_open_orders() == [] and len(venue.sent) == 1
    finally:
        await restarted.stop()


async def test_a_fill_that_wins_the_cancel_race_closes_the_parent(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    parent = await engine.submit(stop_request(type="stop_limit", price="0.38"))
    await engine.on_book({"market_id": "kalshi:KX-A"})
    async def already_filled(order_id, **kwargs):
        done = venue.orders[order_id].model_copy(update={"filled": D("10"), "status": OrderStatus.CLOSED, "average_price": D("0.39")})
        venue.orders[order_id] = done
        return done
    venue.cancel_order = already_filled
    try:
        result = await engine.cancel(parent.id)
        assert result.status == OrderStatus.CLOSED and result.filled == D("10")
    finally:
        await engine.stop()


async def test_crossed_book_and_replayed_trade_do_not_trigger(setup):
    engine, paper, clock = setup
    parent = await engine.submit(request(side="sell", type="stop_market", stop_price="0.40"))
    engine.set_book("kalshi:KX-A", SimpleNamespace(ready=True, best_bid=D("0.39"), best_ask=D("0.30")))
    await engine.on_book({"market_id": "kalshi:KX-A"})
    assert engine.orders.get(parent.id).state == "waiting"
    last = await engine.submit(request(side="sell", type="stop_market", stop_price="0.40", trigger_source="last"))
    await engine.on_trade({"market_id": "kalshi:KX-A", "price": "0.39", "quantity": "1", "timestamp": engine.started_ts - 1000})
    assert engine.orders.get(last.id).state == "waiting" and paper.fills == []


async def test_risk_refusal_is_durable_on_retry(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    from synpath.trading.errors import RiskRejected
    try:
        await engine.set_risk(RiskConfig(max_order_contracts=D("5")))
        for _ in range(2):
            with pytest.raises(RiskRejected) as error:
                await engine.submit(stop_request())
            assert error.value.rule == "max_order_contracts"
        assert venue.sent == []
    finally:
        await engine.stop()


async def test_submission_reservation_and_audit_rollback_before_any_dispatch(tmp_path):
    engine, venue = await scripted_engine(tmp_path / "engine.db")
    with sqlite3.connect(engine.config.journal_path) as connection:
        connection.executescript("""
            CREATE TRIGGER fail_reservation_audit BEFORE INSERT ON events
            WHEN NEW.kind = 'submission.reserved'
            BEGIN SELECT RAISE(ABORT, 'simulated audit failure'); END;
        """)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="simulated audit failure"):
            await engine.submit(stop_request())
        assert await engine.journal.submission('protect-a') is None
        assert engine.orders.parents == {} and venue.sent == []
    finally:
        await engine.stop()


@pytest.mark.parametrize('action', ['dispatch', 'cancel', 'expire'])
async def test_slow_venue_does_not_block_another_markets_stop_observation(tmp_path, action):
    engine, venue = await scripted_engine(tmp_path / 'engine.db')
    clock = [1_800_000_000.0]
    engine.clock = lambda: clock[0]
    first = await engine.submit(stop_request(client_order_id='slow-a'))
    fields = dict(market_id='kalshi:KX-B', client_order_id='other-b')
    if action == 'expire':
        fields.update(type='stop_limit', price='0.38', time_in_force='gtd', expires_at=int((clock[0]+1)*1000))
    second = await engine.submit(stop_request(**fields))
    engine.set_book('kalshi:KX-B', SimpleNamespace(ready=True, best_bid=D('0.39'), best_ask=D('0.41')))
    entered = asyncio.Event()
    release = asyncio.Event()
    original = venue.create_order
    async def slow(request):
        if request.tags['parent'] == first.id:
            entered.set()
            await release.wait()
        return await original(request)
    venue.create_order = slow
    dispatcher = asyncio.create_task(engine._dispatch_loop())
    await asyncio.sleep(0)
    try:
        await asyncio.wait_for(engine.on_book({'market_id': 'kalshi:KX-A'}), 1)
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(engine.on_book({'market_id': 'kalshi:KX-B'}), 1)
        managed = engine.orders.get(second.id)
        assert managed.state == 'working' and managed.pending_child is not None
        if action == 'cancel':
            await engine.cancel(second.id)
        elif action == 'expire':
            clock[0] += 2
        release.set()
        for _ in range(100):
            expected = 'canceled' if action == 'cancel' else 'expired' if action == 'expire' else 'working'
            if engine.orders.get(first.id).children and managed.pending_child is None and managed.state == expected:
                break
            await asyncio.sleep(0.005)
        assert managed.pending_child is None
        assert managed.state == expected
        assert len(venue.sent) == (2 if action == 'dispatch' else 1)
    finally:
        release.set()
        dispatcher.cancel()
        await asyncio.gather(dispatcher, return_exceptions=True)
        await engine.stop()


async def test_sell_stop_market_notional_limit_counts_the_no_price(tmp_path):
    engine, venue = await scripted_engine(tmp_path / 'engine.db')
    parent = await engine.submit(stop_request())
    try:
        await engine.set_risk(RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None,
                                        max_order_notional=D('5')))
        await engine.on_book({'market_id': 'kalshi:KX-A'})
        assert engine.orders.get(parent.id).state == 'rejected'
        assert '9.90' in engine.orders.get(parent.id).detail
        assert venue.sent == [], 'buying NO at a 0.01 YES limit can cost 0.99 per contract'
    finally:
        await engine.stop()


async def test_unexpected_decoder_error_after_send_preserves_uncertain_stop(tmp_path):
    path = tmp_path / "orders.db"
    engine, venue = await scripted_engine(path)
    parent = await engine.submit(stop_request())
    original = venue.create_order
    async def accepted_then_decoder_failed(request):
        await original(request)
        raise RuntimeError("response decoder failed after acceptance")
    venue.create_order = accepted_then_decoder_failed
    try:
        await engine.on_book({"market_id": "kalshi:KX-A"})
        managed = engine.orders.get(parent.id)
        assert managed.state == "working" and managed.pending_child is not None
        assert "unresolved" in managed.detail and len(venue.sent) == 1
    finally:
        await engine.stop()
    venue.create_order = original
    engine, _ = await scripted_engine(path, venue)
    try:
        managed = engine.orders.get(parent.id)
        assert len(managed.children) == 1 and managed.pending_child is None
        assert len(venue.sent) == 1
    finally:
        await engine.stop()
