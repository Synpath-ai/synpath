"""The trading server: keys, grants, the audit log, and the event stream.

Everything runs against a real engine with a paper venue and a real control
store on a temporary file, through FastAPI's test client, so the tests check
what a caller over HTTP would actually get: the status code, the body, and
the permission decision behind it.

The plan's exit criteria for this step are the first two classes here: a
viewer key cannot place an order, and an audit row cannot be altered through
any route. The rest covers what the routes have to get right to be worth
generating a client from -- typed responses, replayable events, and a
document with no bare `object` in it.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.paper import PaperVenue, quadratic_fee
from synpath.server.store import ControlStore
from synpath.server.trading import create_trading_app
from synpath.trading.types import Account, OrderRequest, OrderType, Side

pytestmark = pytest.mark.anyio
ACCOUNT = Account(venue="kalshi", name="desk-a")
OTHER = Account(venue="kalshi", name="desk-b")
SYM = "kalshi:KX-A"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def server(tmp_path: Path):
    """An engine, a store, an app, and three keys with different powers."""
    from fastapi.testclient import TestClient

    paper = PaperVenue(venue="kalshi", fees=quadratic_fee(), cash=D("10000"), account=ACCOUNT)
    paper.set_book(SYM, bids=[(D("0.40"), D("100"))], asks=[(D("0.60"), D("100"))])
    engine = Engine(
        {"kalshi": paper},
        EngineConfig(journal_path=str(tmp_path / "engine.db"), require_lease=True),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
        accounts={"kalshi": ACCOUNT},
    )
    await engine.start()
    store = await ControlStore(str(tmp_path / "control.db")).open()
    owner, owner_key = await store.bootstrap("owner")

    viewer = await store.create_user("viewer")
    await store.acting_as(f"user:{owner.id}", "setup")
    await store.grant(viewer.id, ACCOUNT.key, "view")
    viewer_key = await store.issue_key(viewer.id, label="read only")

    trader = await store.create_user("trader")
    await store.grant(trader.id, ACCOUNT.key, "trade")
    await store.grant(trader.id, ACCOUNT.key, "view")
    trader_key = await store.issue_key(trader.id, label="desk-a only")

    app = create_trading_app(engine, store)
    with TestClient(app) as client:
        yield {
            "client": client, "engine": engine, "paper": paper, "store": store,
            "owner": owner, "owner_key": owner_key.secret,
            "viewer": viewer, "viewer_key": viewer_key.secret,
            "trader": trader, "trader_key": trader_key.secret,
        }
    await store.close()
    await engine.stop()


def auth(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def order_body(**kw) -> dict:
    body = {
        "market_id": SYM, "side": "buy", "amount": "5", "type": "limit", "price": "0.35",
        "book": "alpha", "account": ACCOUNT.model_dump(mode="json"),
    }
    return {**body, **kw}


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------

class TestPermissions:
    async def test_a_viewer_key_cannot_place_an_order(self, server):
        client, engine = server["client"], server["engine"]
        response = client.post("/orders", json=order_body(), headers=auth(server["viewer_key"]))
        assert response.status_code == 403
        error = response.json()["error"]
        assert error["code"] == "forbidden" and "may not trade" in error["message"]
        assert engine.open_orders() == [], "nothing reached the venue"

        allowed = client.post("/orders", json=order_body(), headers=auth(server["trader_key"]))
        assert allowed.status_code == 201
        assert allowed.json()["book"] == "alpha" and allowed.json()["status"] == "open"

    async def test_a_viewer_can_read_what_it_may_see(self, server):
        client = server["client"]
        client.post("/orders", json=order_body(), headers=auth(server["trader_key"]))
        response = client.get("/orders", headers=auth(server["viewer_key"]))
        assert response.status_code == 200 and response.json()["count"] == 1
        assert client.get("/positions", headers=auth(server["viewer_key"])).status_code == 200

    async def test_a_key_sees_only_the_accounts_it_was_granted(self, server):
        client, store, engine = server["client"], server["store"], server["engine"]
        # An order on another desk: the viewer's grant does not reach it.
        other = await engine.submit(OrderRequest(
            market_id=SYM, side=Side.BUY, amount=D("1"), price=D("0.30"), account=OTHER, book="beta",
        ), venue="kalshi")
        response = client.get("/orders", headers=auth(server["viewer_key"]))
        ids = [o["id"] for o in response.json()["data"]]
        assert other.id not in ids
        assert client.get(f"/orders/{other.id}", headers=auth(server["viewer_key"])).status_code == 403
        assert client.get(f"/orders/{other.id}", headers=auth(server["owner_key"])).status_code == 200

    async def test_trading_does_not_imply_granting(self, server):
        client = server["client"]
        body = {"user_id": server["viewer"].id, "account": ACCOUNT.key, "permission": "trade"}
        refused = client.post("/grants", json=body, headers=auth(server["trader_key"]))
        assert refused.status_code == 403 and "manage_members" in refused.json()["error"]["message"]
        allowed = client.post("/grants", json=body, headers=auth(server["owner_key"]))
        assert allowed.status_code == 201

    async def test_no_key_and_a_revoked_key_are_both_refused(self, server):
        client, store = server["client"], server["store"]
        assert client.get("/orders").status_code == 401
        assert client.get("/orders", headers=auth("sk_not_a_key")).status_code == 401
        keys = await store.keys(server["viewer"].id)
        assert client.delete(f"/keys/{keys[0]['id']}", headers=auth(server["owner_key"])).status_code == 200
        assert client.get("/orders", headers=auth(server["viewer_key"])).status_code == 401

    async def test_me_lists_what_this_key_may_do(self, server):
        client = server["client"]
        body = client.get("/me", headers=auth(server["trader_key"])).json()
        assert body["name"] == "trader"
        assert body["accounts"] == [{"account": ACCOUNT.key, "venue": "kalshi", "permissions": ["trade", "view"]}]


# ---------------------------------------------------------------------------
# The audit log
# ---------------------------------------------------------------------------

class TestAudit:
    async def test_every_grant_change_is_recorded_with_its_actor(self, server):
        client = server["client"]
        body = {"user_id": server["viewer"].id, "account": ACCOUNT.key, "permission": "trade"}
        client.post("/grants", json=body, headers=auth(server["owner_key"]))
        client.request("DELETE", "/grants", json=body, headers=auth(server["owner_key"]))
        rows = client.get("/audit", params={"table": "grants"}, headers=auth(server["owner_key"])).json()
        actions = [r["action"] for r in rows]
        assert actions[-2:] == ["insert", "delete"]
        assert rows[-1]["actor"] == f"user:{server['owner'].id}"
        assert rows[-1]["request"] == "DELETE /grants"
        assert rows[-1]["before"] == {"user_id": server["viewer"].id, "account": ACCOUNT.key, "permission": "trade"}

    async def test_an_audit_row_cannot_be_altered_through_any_route(self, server):
        client, store = server["client"], server["store"]
        rows = client.get("/audit", headers=auth(server["owner_key"])).json()
        assert rows, "there is something to try to change"
        # There is no route that writes the audit table: every verb is refused.
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            response = client.request(method, "/audit", headers=auth(server["owner_key"]),
                                      json={"actor": "someone"})
            assert response.status_code in (404, 405), f"{method} /audit must not exist"
        target = rows[0]["id"]
        for method in ("PUT", "PATCH", "DELETE"):
            response = client.request(method, f"/audit/{target}", headers=auth(server["owner_key"]), json={})
            assert response.status_code in (404, 405)
        # And the database refuses even without a route.
        with pytest.raises(Exception, match="append-only"):
            await store._db.execute("UPDATE audit SET actor='someone else' WHERE id=?", (target,))
        with pytest.raises(Exception, match="append-only"):
            await store._db.execute("DELETE FROM audit WHERE id=?", (target,))
        after = client.get("/audit", headers=auth(server["owner_key"])).json()
        assert after[0] == rows[0], "the row is exactly as it was"

    async def test_reading_the_audit_needs_manage_members(self, server):
        client = server["client"]
        assert client.get("/audit", headers=auth(server["trader_key"])).status_code == 403
        assert client.get("/audit", headers=auth(server["owner_key"])).status_code == 200

    async def test_issuing_a_key_shows_the_secret_once_and_stores_a_hash(self, server):
        client, store = server["client"], server["store"]
        body = client.post("/keys", json={"user_id": server["viewer"].id, "label": "second"},
                           headers=auth(server["owner_key"])).json()
        assert body["secret"].startswith("sk_") and body["prefix"] == body["secret"][:12]
        stored = await store.keys(server["viewer"].id)
        assert all(body["secret"] not in json.dumps(row) for row in stored)
        assert (await store.principal(body["secret"])).user_id == server["viewer"].id


# ---------------------------------------------------------------------------
# Trading routes
# ---------------------------------------------------------------------------

class TestTradingRoutes:
    async def test_place_amend_cancel(self, server):
        client = server["client"]
        created = client.post("/orders", json=order_body(), headers=auth(server["trader_key"])).json()
        order_id = created["id"]

        amended = client.patch(f"/orders/{order_id}", json={"amount": "3"}, headers=auth(server["trader_key"]))
        assert amended.status_code == 200, amended.text
        assert amended.json()["amount"] == "3", "the order is the one in the path"
        again = client.patch(f"/orders/{order_id}", json={"order_id": "ignored", "amount": "4"},
                             headers=auth(server["trader_key"]))
        assert again.status_code == 200 and again.json()["amount"] == "4", "an id in the body is ignored"

        fetched = client.get(f"/orders/{order_id}", headers=auth(server["viewer_key"]))
        assert fetched.status_code == 200 and fetched.json()["id"] == order_id

        canceled = client.delete(f"/orders/{order_id}", headers=auth(server["trader_key"]))
        assert canceled.status_code == 200 and canceled.json()["status"] == "canceled"
        assert client.get("/orders", headers=auth(server["trader_key"])).json()["count"] == 0

    async def test_a_risk_rule_answers_409_with_its_name(self, server):
        client, engine = server["client"], server["engine"]
        engine.risk.config = engine.risk.config.model_copy(update={"max_order_contracts": D("1")})
        response = client.post("/orders", json=order_body(amount="5"), headers=auth(server["trader_key"]))
        error = response.json()["error"]
        assert response.status_code == 409 and error["code"] == "risk_rejected"
        assert error["details"]["rule"] == "max_order_contracts"

    async def test_an_engine_held_order_is_one_order_over_http(self, server):
        client = server["client"]
        created = client.post("/orders", json=order_body(
            type="iceberg", amount="10", price="0.35", params={"display": "2"},
        ), headers=auth(server["trader_key"]))
        assert created.status_code == 201
        body = created.json()
        assert body["type"] == "iceberg" and body["held_by"] == "engine" and body["status"] == "triggered"
        assert body["info"]["kind"] == "iceberg"
        listed = client.get("/orders", headers=auth(server["trader_key"])).json()["data"]
        assert body["id"] in [o["id"] for o in listed]
        assert client.delete(f"/orders/{body['id']}", headers=auth(server["trader_key"])).status_code == 200

    async def test_portfolio_routes(self, server):
        client, engine, paper = server["client"], server["engine"], server["paper"]
        client.post("/orders", json=order_body(price="0.60", amount="4"), headers=auth(server["trader_key"]))
        for fill in await paper.deliver() or []:
            pass
        assert client.get("/positions", headers=auth(server["trader_key"])).status_code == 200
        balances = client.get("/balances", headers=auth(server["trader_key"])).json()
        assert balances["count"] == 1 and balances["data"][0]["currency"] == "USD"
        pnl = client.get("/pnl", params={"level": "book"}, headers=auth(server["trader_key"])).json()
        assert pnl["level"] == "book" and "total" in pnl

    async def test_fair_values_round_trip(self, server):
        client = server["client"]
        put = client.put("/fair-values", json={"account": ACCOUNT.key, "market_id": SYM, "value": "0.51"},
                         headers=auth(server["trader_key"]))
        assert put.status_code == 200 and put.json()["value"] == "0.51"
        listed = client.get("/fair-values", headers=auth(server["viewer_key"])).json()
        assert listed == [{"account": ACCOUNT.key, "market_id": SYM, "value": "0.51"}]

    async def test_halt_blocks_trading_and_resume_lifts_it(self, server):
        client = server["client"]
        halted = client.post("/halt", json={"reason": "drill", "policy": "cancel"},
                             headers=auth(server["trader_key"]))
        assert halted.status_code == 200 and halted.json()["policy"] == "cancel"
        refused = client.post("/orders", json=order_body(), headers=auth(server["trader_key"]))
        assert refused.status_code == 409 and refused.json()["error"]["details"]["rule"] == "kill_switch"
        assert client.post("/resume", headers=auth(server["trader_key"])).status_code == 200
        assert client.post("/orders", json=order_body(), headers=auth(server["trader_key"])).status_code == 201

    async def test_risk_configuration_is_read_and_replaced(self, server):
        client = server["client"]
        current = client.get("/risk", headers=auth(server["viewer_key"]))
        assert current.status_code == 200 and "max_open_orders" in current.json()
        changed = {**current.json(), "max_order_contracts": "7"}
        assert client.put("/risk", json=changed, headers=auth(server["trader_key"])).status_code == 403
        response = client.put("/risk", json=changed, headers=auth(server["owner_key"]))
        assert response.status_code == 200
        assert client.get("/risk", headers=auth(server["owner_key"])).json()["max_order_contracts"] == "7"


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

class TestEvents:
    async def test_the_socket_replays_from_a_cursor_then_follows(self, server):
        client = server["client"]
        client.post("/orders", json=order_body(), headers=auth(server["trader_key"]))
        with client.websocket_connect(f"/ws/events?key={server['viewer_key']}&since=0") as socket:
            replayed = [json.loads(socket.receive_text()) for _ in range(3)]
            kinds = [e["kind"] for e in replayed]
            # The journal's first events: the lease, then the engine starting.
            assert kinds[0] == "lease.acquired" and "engine.started" in kinds
            assert all(e["seq"] for e in replayed)
            # Something new: the socket keeps going.
            client.post("/orders", json=order_body(price="0.34"), headers=auth(server["trader_key"]))
            kinds = []
            for _ in range(40):
                kinds.append(json.loads(socket.receive_text())["kind"])
                if "intent.sent" in kinds:
                    break
            assert "intent.sent" in kinds

    async def test_the_socket_needs_a_key_with_view(self, server):
        from starlette.websockets import WebSocketDisconnect

        client = server["client"]
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/events") as socket:
                socket.receive_text()


# ---------------------------------------------------------------------------
# The document a client is generated from
# ---------------------------------------------------------------------------

class TestOpenAPI:
    async def test_every_route_is_typed(self, server):
        client = server["client"]
        document = client.get("/openapi.json").json()
        assert document["info"]["title"] == "synpath trading"
        untyped = []
        for path, methods in document["paths"].items():
            for method, operation in methods.items():
                ok = operation.get("responses", {}).get("200") or operation.get("responses", {}).get("201")
                if not ok:
                    continue
                schema = (ok.get("content") or {}).get("application/json", {}).get("schema", {})
                if not schema or schema == {"type": "object"}:
                    untyped.append(f"{method.upper()} {path}")
        assert untyped == [], "a generated client would see these as untyped"

    async def test_the_order_schema_is_the_library_type(self, server):
        client = server["client"]
        document = client.get("/openapi.json").json()
        order = document["components"]["schemas"]["Order"]
        assert {"id", "venue", "market_id", "side", "status", "amount"} <= set(order["properties"])
        create = document["paths"]["/orders"]["post"]
        assert create["responses"]["201"]["content"]["application/json"]["schema"]["$ref"].endswith("/Order")


# ---------------------------------------------------------------------------
# Routing an order by its market id, and orders on a bucket
# ---------------------------------------------------------------------------

POLY = Account(venue="polymarket", name="paper")


@pytest.fixture
async def two_venues(tmp_path: Path):
    """Two venues, an owner access token that may do anything, a trader allowed Kalshi only."""
    from fastapi.testclient import TestClient

    from synpath.bucket import Bucket, BucketMember

    kalshi = PaperVenue(venue="kalshi", cash=D("10000"), account=ACCOUNT)
    poly = PaperVenue(venue="polymarket", cash=D("10000"), account=POLY)
    engine = Engine(
        {"kalshi": kalshi, "polymarket": poly},
        EngineConfig(journal_path=str(tmp_path / "engine.db"), require_lease=True),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
        accounts={"kalshi": ACCOUNT, "polymarket": POLY},
    )
    await engine.start()
    bucket = await engine.save_bucket(Bucket(book="alpha", name="b", members=[
        BucketMember(market_id=SYM), BucketMember(market_id="polymarket:123")]))
    store = await ControlStore(str(tmp_path / "control.db")).open()
    owner, owner_key = await store.bootstrap("owner")
    trader = await store.create_user("trader")
    await store.acting_as(f"user:{owner.id}", "setup")
    await store.grant(trader.id, ACCOUNT.key, "trade")
    trader_key = await store.issue_key(trader.id, label="kalshi only")
    with TestClient(create_trading_app(engine, store)) as client:
        yield {"client": client, "engine": engine, "bucket": bucket,
               "owner_key": owner_key.secret, "trader_key": trader_key.secret}
    await store.close()
    await engine.stop()


def plain(**kw) -> dict:
    return {"side": "buy", "amount": "5", "type": "limit", "price": "0.35", "book": "alpha", **kw}


class TestRouting:
    async def test_the_market_id_alone_routes_the_order(self, two_venues):
        client, key = two_venues["client"], two_venues["owner_key"]
        placed = client.post("/orders", json=plain(market_id="polymarket:123"), headers=auth(key))
        assert placed.status_code == 201, placed.text
        assert placed.json()["venue"] == "polymarket" and placed.json()["account"]["venue"] == "polymarket"

    async def test_a_bare_id_with_several_venues_says_what_to_do(self, two_venues):
        client, key = two_venues["client"], two_venues["owner_key"]
        refused = client.post("/orders", json=plain(market_id="KX-A"), headers=auth(key))
        assert refused.status_code == 400 and "venue:native" in refused.text

    async def test_permission_follows_the_routed_venue(self, two_venues):
        client, key = two_venues["client"], two_venues["trader_key"]
        assert client.post("/orders", json=plain(market_id=SYM), headers=auth(key)).status_code == 201
        assert client.post("/orders", json=plain(market_id="polymarket:123"), headers=auth(key)).status_code == 403


class TestBucketOrders:
    async def test_an_order_on_a_bucket_is_held_by_the_engine(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["owner_key"], two_venues["bucket"]
        placed = client.post("/orders", json=plain(market_id=bucket.market_id, type="market", price="0.45"), headers=auth(key))
        assert placed.status_code == 201, placed.text
        assert placed.json()["held_by"] == "engine" and placed.json()["market_id"] == bucket.market_id

    async def test_the_caller_must_be_allowed_on_every_member_venue(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["trader_key"], two_venues["bucket"]
        refused = client.post("/orders", json=plain(market_id=bucket.market_id, type="market", price="0.45"), headers=auth(key))
        assert refused.status_code == 403

    async def test_an_unknown_bucket_is_a_404_and_a_limit_order_on_one_a_400(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["owner_key"], two_venues["bucket"]
        assert client.post("/orders", json=plain(market_id="bucket:nope", type="market"),
                           headers=auth(key)).status_code == 404
        limit = client.post("/orders", json=plain(market_id=bucket.market_id), headers=auth(key))
        assert limit.status_code == 400 and "market order" in limit.json()["error"]["message"]
        no_price = client.post("/orders", json={**plain(market_id=bucket.market_id, type="market"), "price": None},
                               headers=auth(key))
        assert no_price.status_code == 400 and "worst price" in no_price.json()["error"]["message"]


class TestCreateBucket:
    def body(self, **kw) -> dict:
        return {"book": "alpha", "name": "same question",
                "members": [{"market_id": SYM}, {"market_id": "polymarket:123", "flip": True}], **kw}

    async def test_a_bucket_is_stored_and_tradable_by_its_market_id(self, two_venues):
        client, key, engine = two_venues["client"], two_venues["owner_key"], two_venues["engine"]
        made = client.post("/buckets", json=self.body(), headers=auth(key))
        assert made.status_code == 201, made.text
        bucket = made.json()
        assert len(bucket["id"]) == 32 and bucket["status"] == "active"
        assert [m["flip"] for m in bucket["members"]] == [False, True]
        stored = await engine.journal.bucket(bucket["id"])
        assert stored is not None and stored["name"] == "same question"
        placed = client.post("/orders", json=plain(market_id=f"bucket:{bucket['id']}", type="market", price="0.45"), headers=auth(key))
        assert placed.status_code == 201, placed.text

    async def test_the_server_assigns_the_id(self, two_venues):
        client, key = two_venues["client"], two_venues["owner_key"]
        first = client.post("/buckets", json=self.body(), headers=auth(key)).json()
        second = client.post("/buckets", json=self.body(), headers=auth(key)).json()
        assert first["id"] != second["id"]

    async def test_the_creator_must_be_allowed_to_trade_every_member_venue(self, two_venues):
        client, key = two_venues["client"], two_venues["trader_key"]
        assert client.post("/buckets", json=self.body(), headers=auth(key)).status_code == 403
        assert client.post("/buckets", json=self.body()).status_code == 401

    async def test_shape_errors_and_unconfigured_venues_are_400(self, two_venues):
        client, key = two_venues["client"], two_venues["owner_key"]
        one = client.post("/buckets", json=self.body(members=[{"market_id": SYM}]), headers=auth(key))
        assert one.status_code == 400 and "two members" in one.text
        twice = client.post("/buckets", json=self.body(members=[{"market_id": SYM}, {"market_id": SYM}]),
                            headers=auth(key))
        assert twice.status_code == 400 and "twice" in twice.text
        us = client.post("/buckets", json=self.body(members=[{"market_id": SYM}, {"market_id": "polymarket_us:abc"}]),
                         headers=auth(key))
        assert us.status_code == 400 and "polymarket_us" in us.text


class TestBucketRoutes:
    async def test_list_get_and_archive(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["owner_key"], two_venues["bucket"]
        listed = client.get("/buckets", headers=auth(key)).json()
        assert [b["id"] for b in listed["data"]] == [bucket.id] and listed["count"] == 1
        assert client.get(f"/buckets/{bucket.id}", headers=auth(key)).json()["name"] == "b"
        assert client.get(f"/buckets/{bucket.market_id}", headers=auth(key)).json()["id"] == bucket.id
        archived = client.delete(f"/buckets/{bucket.id}", headers=auth(key))
        assert archived.status_code == 200 and archived.json()["status"] == "archived"
        assert client.get("/buckets", headers=auth(key)).json()["count"] == 0
        assert client.get("/buckets?status=archived", headers=auth(key)).json()["count"] == 1
        assert client.get("/buckets?status=all", headers=auth(key)).json()["count"] == 1
        refused = client.post("/orders", json=plain(market_id=bucket.market_id, type="market", price="0.45"), headers=auth(key))
        assert refused.status_code == 400 and "archived" in refused.text

    async def test_a_bucket_is_hidden_from_a_caller_who_cannot_see_every_venue(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["trader_key"], two_venues["bucket"]
        assert client.get("/buckets", headers=auth(key)).json()["count"] == 0
        assert client.get(f"/buckets/{bucket.id}", headers=auth(key)).status_code == 403
        assert client.delete(f"/buckets/{bucket.id}", headers=auth(key)).status_code == 403

    async def test_unknown_ids_are_404(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["owner_key"], two_venues["bucket"]
        assert client.get("/buckets/nope", headers=auth(key)).status_code == 404
        assert client.get("/buckets/nope/position", headers=auth(key)).status_code == 404
        assert client.get(f"/buckets/{bucket.id}/orders/nope", headers=auth(key)).status_code == 404

    async def test_orders_on_a_bucket_and_their_report(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["owner_key"], two_venues["bucket"]
        placed = client.post("/orders", json=plain(market_id=bucket.market_id, type="market", price="0.45"), headers=auth(key)).json()
        client.post("/orders", json=plain(market_id=SYM), headers=auth(key))          # not on the bucket
        orders = client.get(f"/buckets/{bucket.id}/orders", headers=auth(key)).json()
        assert [o["order_id"] for o in orders["data"]] == [placed["id"]]
        one = client.get(f"/buckets/{bucket.id}/orders/{placed['id']}", headers=auth(key))
        assert one.status_code == 200, one.text
        report = one.json()
        assert report["bucket_id"] == bucket.id and report["amount"] == "5" and report["worst_price"] == "0.45"
        assert report["filled"] == "0" and report["stop_reason"] is None and report["detail"] is None
        canceled = client.delete(f"/orders/{placed['id']}", headers=auth(key))
        assert canceled.status_code == 200 and canceled.json()["status"] == "canceled"
        after = client.get(f"/buckets/{bucket.id}/orders/{placed['id']}", headers=auth(key)).json()
        assert after["status"] == "canceled"

    async def test_position_is_flat_before_any_fill(self, two_venues):
        client, key, bucket = two_venues["client"], two_venues["owner_key"], two_venues["bucket"]
        position = client.get(f"/buckets/{bucket.id}/position", headers=auth(key))
        assert position.status_code == 200, position.text
        assert position.json()["side"] == "flat" and position.json()["contracts"] == "0"



class TestErrorsAndAnswers:
    async def test_a_venue_refusal_is_a_400_with_the_venues_reason(self, server, monkeypatch):
        from synpath.trading.errors import InsufficientFunds

        async def refuse(request):
            raise InsufficientFunds("AsyncHttpClient POST /portfolio/events/orders: insufficient balance")
        monkeypatch.setattr(server["paper"], "create_order", refuse)
        refused = server["client"].post("/orders", json=order_body(), headers=auth(server["trader_key"]))
        assert refused.status_code == 400, refused.text
        error = refused.json()["error"]
        assert error["code"] == "insufficient_funds" and "insufficient balance" in error["message"]
        assert error["details"]["venue"] == "kalshi" and error["details"]["retryable"] is False
        after = server["client"].get("/orders", headers=auth(server["trader_key"]))
        assert after.status_code == 200, "the server keeps answering"

    async def test_an_unreachable_venue_is_a_retryable_502(self, server, monkeypatch):
        from synpath.errors import ExchangeNotAvailable

        async def down(request):
            raise ExchangeNotAvailable("kalshi: 503 from the venue")
        monkeypatch.setattr(server["paper"], "create_order", down)
        failed = server["client"].post("/orders", json=order_body(), headers=auth(server["trader_key"]))
        assert failed.status_code == 502 and failed.json()["error"]["details"]["retryable"] is True

    async def test_accounts_lists_a_configured_venue_without_a_named_account(self, tmp_path):
        from fastapi.testclient import TestClient

        engine = Engine({"kalshi": PaperVenue(venue="kalshi", cash=D("100"))},
                        EngineConfig(journal_path=str(tmp_path / "e.db"), require_lease=True))
        await engine.start()
        store = await ControlStore(str(tmp_path / "c.db")).open()
        _, key = await store.bootstrap("owner")
        try:
            with TestClient(create_trading_app(engine, store)) as client:
                rows = client.get("/accounts", headers=auth(key.secret)).json()
            assert [r["account"] for r in rows] == ["kalshi:default"]
        finally:
            await store.close()
            await engine.stop()

    async def test_put_risk_answers_with_the_rules_now_in_force(self, server):
        client, key = server["client"], server["owner_key"]
        rules = client.get("/risk", headers=auth(key)).json()
        changed = client.put("/risk", json={**rules, "max_order_contracts": "7"}, headers=auth(key))
        assert changed.status_code == 200 and changed.json()["max_order_contracts"] == "7"

    async def test_resume_answers_with_the_halt_state(self, server):
        client, key = server["client"], server["owner_key"]
        client.post("/halt", json={"reason": "t", "policy": "hold"}, headers=auth(key))
        resumed = client.post("/resume", headers=auth(key))
        assert resumed.status_code == 200 and resumed.json() == {"halted": False, "halt_reason": ""}


class TestBucketReportDetail:
    async def test_a_rejected_bucket_order_says_why(self, two_venues, monkeypatch):
        from synpath.trading.errors import InsufficientFunds

        client, key, bucket, engine = (two_venues["client"], two_venues["owner_key"], two_venues["bucket"],
                                       two_venues["engine"])
        for venue in engine.adapters.values():
            venue.set_book(SYM, asks=[(D("0.40"), D("10"))])
            venue.set_book("polymarket:123", asks=[(D("0.41"), D("10"))])
        engine.set_book(SYM, engine.adapters["kalshi"].books[SYM])
        engine.set_book("polymarket:123", engine.adapters["polymarket"].books["polymarket:123"])

        async def refuse(request):
            raise InsufficientFunds("kalshi: insufficient balance")
        monkeypatch.setattr(engine.adapters["kalshi"], "create_order", refuse)
        placed = client.post("/orders", json=plain(market_id=bucket.market_id, type="market", price="0.45"), headers=auth(key)).json()
        report = client.get(f"/buckets/{bucket.id}/orders/{placed['id']}", headers=auth(key)).json()
        assert report["status"] == "rejected" and "insufficient balance" in report["detail"]



class TestOneErrorShape:
    async def test_no_token_malformed_body_and_unknown_order_share_one_shape(self, server):
        client = server["client"]
        for response, status, code in (
            (client.get("/me"), 401, "unauthorized"),
            (client.post("/orders", json={"side": "buy"}, headers=auth(server["trader_key"])), 422, "validation_error"),
            (client.get("/orders/nope", headers=auth(server["trader_key"])), 404, "not_found"),
        ):
            assert response.status_code == status, response.text
            error = response.json()["error"]
            assert set(error) == {"code", "message", "details"} and error["code"] == code
            assert set(error["details"]) >= {"venue", "retryable"}

    async def test_the_events_socket_closes_with_4401_without_a_token(self, server):
        from starlette.websockets import WebSocketDisconnect

        with pytest.raises(WebSocketDisconnect) as closed:
            with server["client"].websocket_connect("/ws/events?since=0") as socket:
                socket.receive_json()
        assert closed.value.code == 4401
