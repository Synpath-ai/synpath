"""Routed account isolation over sockets and indexed membership pagination."""
from __future__ import annotations

import base64
import json

import pytest
from fastapi.testclient import TestClient

from synpath.engine.orders.routed import RoutedLimit
from synpath.engine.storage import create_journal
from synpath.server.store import ControlStore
from synpath.server.trading import create_trading_app
from synpath.trading.types import Account, Order
from test_owner_migrations import configuration, config  # noqa: F401
from test_postgres_storage import database  # noqa: F401
from test_routed_limit import build, request, ACCOUNT_K, ACCOUNT_P, K, P

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize("full_scope", [False, True], ids=["partial", "full"])
async def test_socket_replay_and_live_filter_parents_but_keep_concrete_children(tmp_path, full_scope):
    engine, _, _, _, bucket = await build(tmp_path)
    store = await ControlStore(str(tmp_path / "socket-control.db")).open()
    try:
        parent = await engine.submit(request(bucket))
        owner, _ = await store.bootstrap("owner")
        viewer = await store.create_user("viewer")
        await store.acting_as(f"user:{owner.id}", "socket account grants")
        await store.grant(viewer.id, ACCOUNT_K.key, "view")
        if full_scope:
            await store.grant(viewer.id, ACCOUNT_P.key, "view")
        key = await store.issue_key(viewer.id)
        children = [Order(id=f"probe-{venue}", venue=venue, account=account,
                          market_id=market, side="buy", type="limit", status="open",
                          time_in_force="gtc", quantity="1", price="0.4", parent_id=parent.id)
                    for venue, account, market in [("kalshi", ACCOUNT_K, K), ("polymarket", ACCOUNT_P, P)]]

        async def publish_phase(phase):
            await engine.bus.publish("managed.security_probe", {**parent.model_dump(mode="json"), "probe": phase}, key=parent.id)
            for child in children:
                await engine.bus.publish("order.security_probe", {**child.model_dump(mode="json"), "probe": phase}, key=f"{child.venue}:{child.id}")
            await engine.bus.publish("verification.barrier", {"probe": phase})

        since = await engine.journal.last_seq()
        await publish_phase("replay")
        with TestClient(create_trading_app(engine, store)) as client:
            kinds = "managed.security_probe,order.security_probe,verification.barrier"
            with client.websocket_connect(f"/ws/events?key={key.secret}&since={since}&kinds={kinds}") as socket:
                for phase in ("replay", "live"):
                    if phase == "live":
                        client.portal.call(publish_phase, phase)
                    received = []
                    while True:
                        event = json.loads(socket.receive_text())
                        assert event["payload"]["probe"] == phase
                        if event["kind"] == "verification.barrier":
                            break
                        received.append(event)
                    expected = {"probe-kalshi", "probe-polymarket", parent.id} if full_scope else {"probe-kalshi"}
                    assert {event["payload"]["id"] for event in received} == expected
                    assert len(received) == len(expected)
                    assert all(event["seq"] > since for event in received)
    finally:
        await store.close()
        await engine.stop()


async def test_membership_filters_page_live_and_finished_parents_with_indexed_queries(config, tmp_path, monkeypatch):
    engine, _, _, _, bucket = await build(tmp_path)
    try:
        accepted = await engine.submit(request(bucket))
        original = engine.managed_order_controller.get(accepted.id).snapshot()
        async with create_journal(**config, user_id=engine.config.user_id) as store:
            parents = []
            for index in range(8):
                snapshot = {**original, "id": f"membership-{index}", "created_ms": 1000,
                            "state": "working" if index % 2 == 0 else "done"}
                snapshot["request"] = {**original["request"], "client_order_id": f"membership-client-{index}"}
                parent = RoutedLimit.from_snapshot(snapshot)
                await store.save_managed(snapshot, order=parent.as_order())
                parents.append(parent.as_order())
            concrete = []
            for venue, account, market in [("kalshi", ACCOUNT_K, K), ("polymarket", ACCOUNT_P, P)]:
                for terminal in (False, True):
                    order = Order(user_id=engine.config.user_id, id=f"concrete-{terminal}", venue=venue,
                                  account=account, market_id=market, side="buy", type="limit", time_in_force="gtc",
                                  status="closed" if terminal else "open", quantity="1", price="0.4", created_at=1000)
                    await store.upsert_order(order)
                    concrete.append(order)
            outsider = Order(user_id=engine.config.user_id, id="outside", venue="other", account=Account(venue="other"),
                             market_id="other:market", side="buy", type="limit", time_in_force="gtc", status="open", quantity="1", created_at=1000)
            await store.upsert_order(outsider)
            captured = []
            execute = store._db.execute

            def record_query(query, params=()):
                if query.startswith("SELECT payload") and "parent_execution_accounts" in query:
                    captured.append((query, params))
                return execute(query, params)

            monkeypatch.setattr(store._db, "execute", record_query)
            for venue in ("kalshi", "polymarket"):
                for open_only in (False, True):
                    rows, cursor = [], None
                    while True:
                        page = await store.orders_page(venue=venue, open_only=open_only, before=cursor, limit=3)
                        if not page:
                            break
                        rows.extend(page)
                        cursor = page[-1][1]
                    expected = [order for order in parents + concrete if (order.execution_accounts or order.venue == venue)
                                and (not open_only or not order.is_terminal)]
                    assert {(order.venue, order.id) for order, _ in rows} == {(order.venue, order.id) for order in expected}
                    assert len(rows) == len(expected)
                    assert [key for _, key in rows] == sorted([key for _, key in rows], reverse=True)
                    assert sum(bool(order.execution_accounts) for order, _ in rows) == (4 if open_only else 8)
                account = ACCOUNT_K if venue == "kalshi" else ACCOUNT_P
                opened = await store.open_orders(account_key=account.key)
                assert {(order.venue, order.id) for order in opened} == {
                    (order.venue, order.id) for order in parents + concrete
                    if not order.is_terminal and (order.execution_accounts or order.account == account)}
            assert await store.open_orders(account_key="kalshi:unrelated") == []
            assert await store.orders_page(venue="unrelated", limit=3) == []
            monkeypatch.setattr(store._db, "execute", execute)
            # Ask each backend to explain the actual production membership queries.
            # Tiny PostgreSQL fixtures prefer sequential scans; disabling that option
            # verifies index eligibility rather than claiming a production cost estimate.
            if config["backend"] == "postgresql":
                await store._db.execute("SET enable_seqscan=off")
            for name, (query, params) in {"venue-page": next(item for item in captured if "created_ts" in item[0]),
                                          "account-open": next(item for item in captured if "scope.account_key" in item[0])}.items():
                prefix = "EXPLAIN QUERY PLAN " if config["backend"] == "sqlite" else "EXPLAIN "
                async with store._db.execute(prefix + query, params) as cursor:
                    plan = "\n".join(str(dict(row)) for row in await cursor.fetchall())
                assert "parent_execution" in plan
                assert "INDEX" in plan.upper(), plan
                assert "orders" in plan
                assert any("parent_execution" in line and "INDEX" in line.upper() for line in plan.splitlines()), plan
                print(f"{config['backend']} {name}: {plan}")
    finally:
        await engine.stop()


async def test_http_membership_pagination_rejects_old_cursor(tmp_path):
    engine, _, _, _, bucket = await build(tmp_path)
    store = await ControlStore(str(tmp_path / "cursor-control.db")).open()
    try:
        first = await engine.submit(request(bucket, client_order_id="page-first"))
        await engine.cancel(first.id)
        second = await engine.submit(request(bucket, client_order_id="page-second"))
        _, key = await store.bootstrap("owner")
        auth = {"Authorization": f"Bearer {key.secret}"}
        with TestClient(create_trading_app(engine, store)) as client:
            for venue in ("kalshi", "polymarket"):
                seen, cursor = [], None
                while True:
                    params = {"venue": venue, "scope": "all", "limit": 1}
                    if cursor:
                        params["cursor"] = cursor
                    response = client.get("/orders", params=params, headers=auth)
                    assert response.status_code == 200
                    page = response.json()
                    seen.extend(order["id"] for order in page["data"])
                    cursor = page["next_cursor"]
                    if not cursor:
                        break
                assert set(seen) == {first.id, second.id} and len(seen) == 2
            old = base64.urlsafe_b64encode(json.dumps([1000, "kalshi", first.id]).encode()).decode().rstrip("=")
            assert client.get("/orders", params={"scope": "all", "cursor": old}, headers=auth).status_code == 400
    finally:
        await store.close()
        await engine.stop()
