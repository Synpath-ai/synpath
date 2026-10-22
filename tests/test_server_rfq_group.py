"""The cross-venue RFQ group routes under /trading, offline (docs/rfq-combo.md, "Cross-venue RFQ").

A real engine on three paper RFQ venues (the venues and the mocked matching
service of `test_engine_rfq_group`), a real control store, and FastAPI's
test client. Covered: create, list, get, accept and cancel; each grant
refusing (view for reads, trade on every venue the group asks for the
rest, a venue the key may not trade skipped as `forbidden`); 202 by default
and 200 when a waited-for accept is final; the double-accept 409; `400
no_venue` with the reasons; the event socket; the halt; the OpenAPI document.
"""
from __future__ import annotations

import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.server.store import ControlStore
from synpath.server.trading import create_trading_app
from synpath.trading.types import Account

from test_engine_rfq_group import K_A, K_B, matching, venues

pytestmark = pytest.mark.anyio

ACCOUNTS = {name: Account(venue=name, name="desk-a") for name in ("kalshi", "polymarket", "polymarket_us")}


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def no_saved_key(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNPATH_API_KEY", raising=False)
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(tmp_path / "no-credentials.json"))


@pytest.fixture
async def server(tmp_path: Path):
    from fastapi.testclient import TestClient

    adapters = venues()
    for name, venue in adapters.items():
        venue.account = ACCOUNTS[name]
    engine = Engine(
        adapters,
        EngineConfig(journal_path=str(tmp_path / "engine.db"), require_lease=True, in_doubt_timeout_s=0,
                     rfq_poll_interval_s=0.01, rfq_incoming_interval_s=None),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None),
        accounts=ACCOUNTS, matcher=matching(),
    )
    await engine.start()
    store = await ControlStore(str(tmp_path / "control.db")).open()
    owner, _ = await store.bootstrap("owner")
    await store.acting_as(f"user:{owner.id}", "setup")

    async def user(name: str, grants: dict[str, tuple[str, ...]]) -> str:
        made = await store.create_user(name)
        for venue, permissions in grants.items():
            for permission in permissions:
                await store.grant(made.id, ACCOUNTS[venue].key, permission)
        return (await store.issue_key(made.id)).secret

    everything = ("trade", "view")
    keys = {
        "trader": await user("trader", {v: everything for v in ACCOUNTS}),
        "viewer": await user("viewer", {v: ("view",) for v in ACCOUNTS}),
        "partial": await user("partial", {"kalshi": everything, "polymarket": everything}),
        "outsider": await user("outsider", {"kalshi": everything}),
    }
    with TestClient(create_trading_app(engine, store)) as client:
        yield {"client": client, "engine": engine, "adapters": adapters, **keys}
    await store.close()
    await engine.stop()


def auth(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def group_body(**kw) -> dict:
    return {"instrument_id": K_A, "quantity": "10", "cross_venue": True, "book": "alpha", **kw}


def created(server, key: str = "trader", **kw) -> dict:
    response = server["client"].post("/rfq-groups", json=group_body(**kw), headers=auth(server[key]))
    assert response.status_code == 201, response.text
    return response.json()


class TestCreateAndRead:
    async def test_create_list_and_get(self, server):
        client = server["client"]
        group = created(server)
        assert group["status"] == "open" and {m["venue"] for m in group["members"]} == set(ACCOUNTS)
        assert [q["venue"] for q in group["quotes"]] == ["kalshi", "polymarket", "polymarket_us"]
        best = group["quotes"][0]
        assert best["rank"] == 1 and best["raw_price"] == "0.43" and best["fee"] == "0.18"
        assert best["effective_price"] == "0.448"
        listed = client.get("/rfq-groups", headers=auth(server["viewer"]))
        assert listed.status_code == 200 and [g["id"] for g in listed.json()["data"]] == [group["id"]]
        one = client.get(f"/rfq-groups/{group['id']}?refresh=true", headers=auth(server["viewer"]))
        assert one.status_code == 200 and one.json()["id"] == group["id"]
        assert client.get("/rfq-groups/nope", headers=auth(server["viewer"])).status_code == 404

    async def test_a_venue_the_key_may_not_trade_is_skipped(self, server):
        group = created(server, "partial")
        assert {m["venue"] for m in group["members"]} == {"kalshi", "polymarket"}
        assert [(s["venue"], s["code"]) for s in group["skipped"]] == [("polymarket_us", "forbidden")]
        client = server["client"]
        named = client.post("/rfq-groups", json=group_body(venues=["polymarket_us"]), headers=auth(server["partial"]))
        assert named.status_code == 403
        assert client.post("/rfq-groups", json=group_body(), headers=auth(server["viewer"])).status_code == 403
        # A key that cannot see every venue of a group sees neither it nor its list row.
        assert client.get(f"/rfq-groups/{group['id']}", headers=auth(server["outsider"])).status_code == 403
        assert client.get("/rfq-groups", headers=auth(server["outsider"])).json()["data"] == []

    async def test_no_venue_left_is_400_with_the_reasons(self, server):
        response = server["client"].post("/rfq-groups", json={
            "legs": [{"market_id": K_A, "outcome": "yes"}, {"market_id": K_B, "outcome": "no"}],
            "quantity": "10", "venues": ["polymarket_us"]}, headers=auth(server["trader"]))
        assert response.status_code == 400, response.text
        body = response.json()
        assert body["error"]["code"] == "no_venue"
        assert [s["code"] for s in body["error"]["details"]["skipped"] if s["venue"] == "polymarket_us"] == ["missing_leg"]

    async def test_a_body_with_two_sizes_is_422(self, server):
        response = server["client"].post("/rfq-groups", json=group_body(amount="5"), headers=auth(server["trader"]))
        assert response.status_code == 422


class TestAcceptAndCancel:
    async def test_accept_is_202_then_200_with_wait_and_a_second_is_409(self, server):
        client = server["client"]
        group = created(server)
        best = group["quotes"][0]
        assert client.post(f"/rfq-groups/{group['id']}/accept", json={"quote_id": best["quote_id"], "venue": best["venue"]},
                           headers=auth(server["viewer"])).status_code == 403
        assert client.post(f"/rfq-groups/{group['id']}/accept", json={"quote_id": best["quote_id"], "venue": best["venue"]},
                           headers=auth(server["partial"])).status_code == 403, "it would cancel polymarket_us"
        done = client.post(f"/rfq-groups/{group['id']}/accept",
                           json={"quote_id": best["quote_id"], "venue": best["venue"], "worst_price": "0.45", "wait": True},
                           headers=auth(server["trader"]))
        assert done.status_code == 200, done.text
        body = done.json()
        assert body["final"] and body["quote"]["status"] == "executed" and body["intent_state"] == "sent"
        assert body["group"]["status"] == "executed" and body["order_id"]
        assert {m["venue"]: m["status"] for m in body["group"]["members"]} == {
            "kalshi": "executed", "polymarket": "lapsing", "polymarket_us": "canceled"}
        again = client.post(f"/rfq-groups/{group['id']}/accept", json={"quote_id": group["quotes"][1]["quote_id"], "venue": group["quotes"][1]["venue"]},
                            headers=auth(server["trader"]))
        assert again.status_code == 409 and again.json()["error"]["details"]["rule"] == "rfq_group_accepted"

    async def test_accept_without_wait_is_202(self, server):
        for venue in server["adapters"].values():
            venue.auto_advance = False
        group = created(server)
        sent = server["client"].post(f"/rfq-groups/{group['id']}/accept",
                                     json={"quote_id": group["quotes"][0]["quote_id"], "venue": group["quotes"][0]["venue"]}, headers=auth(server["trader"]))
        assert sent.status_code == 202, sent.text
        assert sent.json()["group"]["status"] == "accepted" and not sent.json()["final"]
        refused = server["client"].delete(f"/rfq-groups/{group['id']}", headers=auth(server["trader"]))
        assert refused.status_code == 409 and refused.json()["error"]["details"]["rule"] == "rfq_group_accept_in_flight"

    async def test_accept_names_the_venue_when_quote_ids_collide(self, server):
        client = server["client"]
        group = created(server)
        ids_by_venue = {q["venue"]: q["quote_id"] for q in group["quotes"]}
        assert len(set(ids_by_venue.values())) == 1, "the paper venues return the same quote id"
        quote_id = ids_by_venue["polymarket_us"]
        missing = client.post(f"/rfq-groups/{group['id']}/accept", json={"quote_id": quote_id},
                              headers=auth(server["trader"]))
        assert missing.status_code == 400, missing.text
        error = missing.json()["error"]
        assert error["code"] == "venue_required" and "name the quote's venue" in error["message"]
        assert error["details"]["venues"] == ["kalshi", "polymarket", "polymarket_us"]
        outside = client.post(f"/rfq-groups/{group['id']}/accept", json={"quote_id": quote_id, "venue": "limitless"},
                              headers=auth(server["trader"]))
        assert outside.status_code == 400 and outside.json()["error"]["code"] == "venue_not_in_group"
        done = client.post(f"/rfq-groups/{group['id']}/accept",
                           json={"quote_id": quote_id, "venue": "polymarket_us", "wait": True},
                           headers=auth(server["trader"]))
        assert done.status_code == 200, done.text
        assert done.json()["venue"] == "polymarket_us" and done.json()["group"]["accepted_venue"] == "polymarket_us"
        assert server["adapters"]["kalshi"].calls.count("accept_quote") == 0

    async def test_cancel_all(self, server):
        client = server["client"]
        group = created(server)
        assert client.delete(f"/rfq-groups/{group['id']}", headers=auth(server["viewer"])).status_code == 403
        canceled = client.delete(f"/rfq-groups/{group['id']}", headers=auth(server["trader"]))
        assert canceled.status_code == 200 and canceled.json()["status"] == "canceled"
        assert {m["status"] for m in canceled.json()["members"]} == {"canceled", "lapsing"}
        accept = client.post(f"/rfq-groups/{group['id']}/accept", json={"quote_id": group["quotes"][0]["quote_id"], "venue": group["quotes"][0]["venue"]},
                             headers=auth(server["trader"]))
        assert accept.status_code == 400

    async def test_halt_cancels_open_groups(self, server):
        client = server["client"]
        group = created(server)
        halted = client.post("/halt", json={"reason": "stop", "policy": "cancel"}, headers=auth(server["trader"]))
        assert halted.status_code == 200, halted.text
        assert halted.json()["rfq"]["groups"][group["id"]]["status"] == "canceled"
        after = client.get(f"/rfq-groups/{group['id']}", headers=auth(server["viewer"])).json()
        assert after["status"] == "canceled"


class Catalogue:
    """The server's read adapter for one venue: YES quotes by market id."""

    def __init__(self, quotes: dict[str, tuple[float, float]]):
        self.quotes = quotes

    def fetch_markets_by_ids(self, market_ids):
        from synpath.types import Market, Outcome, Quote

        return [Market(id=m, venue=m.partition(":")[0], venue_market_id=m.partition(":")[2], title=m, status="open",
                       yes=Outcome(label="Yes", quote=Quote(bid=self.quotes[m][0], ask=self.quotes[m][1])),
                       no=Outcome(label="No")) for m in market_ids if m in self.quotes]


class Registry:
    def __init__(self, venues: dict[str, Catalogue]):
        self.venues = venues

    def get(self, venue: str):
        if venue not in self.venues:
            raise KeyError(venue)
        return self.venues[venue]


LEGS = [{"market_id": K_A, "outcome": "yes"}, {"market_id": K_B, "outcome": "no"}]


class TestPreview:
    async def test_offered_venues_with_leg_prices_and_skipped_ones(self, server):
        client = server["client"]
        client.app.state.registry = Registry({
            "kalshi": Catalogue({K_A: (0.58, 0.60), K_B: (0.30, 0.32)}),
            "polymarket": Catalogue({"polymarket:111": (0.57, 0.59)}),
        })
        answer = client.post("/rfq-groups/preview", json={"legs": LEGS}, headers=auth(server["trader"]))
        assert answer.status_code == 200, answer.text
        body = answer.json()
        offered = {v["venue"]: v for v in body["offered"]}
        assert set(offered) == {"kalshi", "polymarket"}
        # Kalshi: YES A at its ask 0.60, NO B at 1 - its bid 0.30.
        assert offered["kalshi"]["leg_prices"] == ["0.6000", "0.7000"] and offered["kalshi"]["combined_price"] == "0.420000"
        assert offered["kalshi"]["collection_id"] == "KXMVE-1"
        # Polymarket has no quote for 222: no estimate.
        assert offered["polymarket"]["leg_prices"] == ["0.5900", None] and offered["polymarket"]["combined_price"] is None
        assert [(s["venue"], s["code"]) for s in body["skipped"]] == [("polymarket_us", "missing_leg")]
        assert client.get("/rfq-groups", headers=auth(server["trader"])).json()["data"] == []

    async def test_a_venue_the_key_may_not_trade_is_forbidden_and_bad_bodies_are_422(self, server):
        client = server["client"]
        answer = client.post("/rfq-groups/preview", json={"legs": LEGS}, headers=auth(server["partial"])).json()
        assert ("polymarket_us", "forbidden") in [(s["venue"], s["code"]) for s in answer["skipped"]]
        assert all(v["leg_prices"] is not None for v in answer["offered"])     # no registry: every price unknown
        assert client.post("/rfq-groups/preview", json={}, headers=auth(server["trader"])).status_code == 422
        both = {"legs": LEGS, "quantity": "1", "amount": "1"}
        assert client.post("/rfq-groups/preview", json=both, headers=auth(server["trader"])).status_code == 422
        unknown = client.post("/rfq-groups/preview", json={"legs": LEGS, "venues": ["nowhere"]},
                              headers=auth(server["trader"]))
        assert unknown.status_code == 404


class TestBoard:
    async def test_a_listed_venues_board(self, server):
        response = server["client"].get("/combos/board?venue=polymarket", headers=auth(server["viewer"]))
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["venue"] == "polymarket" and body["count"] == 2 and body["complete"]
        assert [e["kind"] for e in body["events"]] == ["question", "question"]
        assert body["events"][0]["yes"]["market_id"] in ("polymarket:111", "polymarket:222")

    async def test_kalshi_without_a_catalogue_is_empty_and_a_venue_without_combos_is_400(self, server):
        client = server["client"]
        kalshi = client.get("/combos/board?venue=kalshi", headers=auth(server["viewer"])).json()
        assert kalshi["events"] == [] and not kalshi["complete"]
        assert client.get("/combos/board?venue=nowhere", headers=auth(server["viewer"])).status_code in (400, 404)


class TestEventsAndSchema:
    async def test_the_event_socket_carries_group_events(self, server):
        client = server["client"]
        group = created(server)
        with client.websocket_connect(f"/ws/events?key={server['viewer']}&since=0&kinds=rfq_group") as socket:
            seen = []
            for _ in range(50):
                seen.append(json.loads(socket.receive_text()))
                if seen[-1]["kind"] == "rfq_group.updated":
                    break
        kinds = [e["kind"] for e in seen]
        assert kinds[0] == "rfq_group.created" and "rfq_group.quote" in kinds
        assert all(k.startswith("rfq_group.") for k in kinds)
        assert all(e["payload"]["group_id"] == group["id"] for e in seen)

    async def test_openapi(self, server):
        document = server["client"].get("/openapi.json").json()
        paths = document["paths"]
        assert {"get", "post"} <= set(paths["/rfq-groups"])
        assert {"get", "delete"} <= set(paths["/rfq-groups/{group_id}"])
        accept = paths["/rfq-groups/{group_id}/accept"]["post"]["responses"]
        for code in ("200", "202"):
            assert accept[code]["content"]["application/json"]["schema"]["$ref"].endswith("/GroupAcceptResult")
        assert {"403", "404", "409"} <= set(accept)
        made = paths["/rfq-groups"]["post"]["responses"]["201"]
        assert made["content"]["application/json"]["schema"]["$ref"].endswith("/RfqGroup")
        schemas = document["components"]["schemas"]
        assert {"RfqGroup", "GroupMember", "GroupQuote", "SkippedVenue", "RfqGroupBody", "GroupAcceptBody",
                "GroupPreview", "PreviewVenue", "RfqGroupPreviewBody", "ComboBoard", "BoardEvent"} <= set(schemas)
        assert "post" in paths["/rfq-groups/preview"] and "get" in paths["/combos/board"]
