"""The RFQ and combo routes under /trading, offline (P5 of docs/rfq-combo.md).

A real engine on `PaperRfqVenue` (the paper RFQ venue of P3/P4) beside a plain
paper venue without RFQ, a real control store, and FastAPI's test client.
Covered: every route; each grant refusing; 202 by default and 200 only when a
waited-for quote is final; in-doubt sends answering 202 with the intent key;
risk refusals carrying `rule`; the event socket carrying `rfq.*`, `quote.*`
and `combo.saved`; `POST /halt` cancelling our RFQs and quotes under the
`cancel` policy; and the OpenAPI document.
"""
from __future__ import annotations

import json
from decimal import Decimal as D
from pathlib import Path

import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.paper import PaperRfqVenue, PaperVenue
from synpath.errors import NetworkError
from synpath.server.store import ControlStore
from synpath.server.trading import create_trading_app
from synpath.trading.types import Account, QuoteStatus, RfqStatus

pytestmark = pytest.mark.anyio

VENUE = "polymarket_us"
ACCOUNT = Account(venue=VENUE, name="desk-a")
KALSHI = Account(venue="kalshi", name="desk-a")
MARKET = "polymarket_us:tec-nba-a"
LEG_B = "polymarket_us:tec-nba-b"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def server(tmp_path: Path):
    """An engine with an RFQ venue and a plain one, and keys of different powers."""
    from fastapi.testclient import TestClient

    rfq = PaperRfqVenue(venue=VENUE, account=ACCOUNT, cash=D("10000"))
    plain = PaperVenue(venue="kalshi", account=KALSHI, cash=D("10000"))
    engine = Engine(
        {VENUE: rfq, "kalshi": plain},
        EngineConfig(journal_path=str(tmp_path / "engine.db"), require_lease=True, in_doubt_timeout_s=0,
                     rfq_poll_interval_s=0.01, rfq_incoming_interval_s=None),
        risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None, max_order_contracts=D("500")),
        accounts={VENUE: ACCOUNT, "kalshi": KALSHI},
    )
    await engine.start()
    store = await ControlStore(str(tmp_path / "control.db")).open()
    owner, owner_key = await store.bootstrap("owner")
    await store.acting_as(f"user:{owner.id}", "setup")

    viewer = await store.create_user("viewer")
    await store.grant(viewer.id, ACCOUNT.key, "view")
    viewer_key = await store.issue_key(viewer.id)

    trader = await store.create_user("trader")
    await store.grant(trader.id, ACCOUNT.key, "trade")
    await store.grant(trader.id, ACCOUNT.key, "view")
    trader_key = await store.issue_key(trader.id)

    other = await store.create_user("other")         # trades kalshi only
    await store.grant(other.id, KALSHI.key, "trade")
    await store.grant(other.id, KALSHI.key, "view")
    other_key = await store.issue_key(other.id)

    with TestClient(create_trading_app(engine, store)) as client:
        yield {"client": client, "engine": engine, "paper": rfq, "store": store,
               "owner": owner_key.secret, "viewer": viewer_key.secret, "trader": trader_key.secret,
               "other": other_key.secret}
    await store.close()
    await engine.stop()


def auth(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def rfq_body(**kw) -> dict:
    return {"market_id": MARKET, "quantity": "10", "book": "alpha", "trader": "yuki", **kw}


def accept_body(quote: dict, **kw) -> dict:
    return {"rfq_id": quote["rfq_id"], "outcome": "yes", "action": "buy", "worst_price": "0.50", "book": "alpha", **kw}


def maker_body(rfq_id: str, **kw) -> dict:
    return {"rfq_id": rfq_id, "bid": "0.40", "ask": "0.50", "book": "mm", **kw}


def legs() -> list[dict]:
    return [{"market_id": MARKET, "outcome": "yes"}, {"market_id": LEG_B, "outcome": "no"}]


def new_rfq(server, **kw) -> tuple[dict, dict]:
    """An RFQ of ours and the paper maker's quote on it."""
    client = server["client"]
    created = client.post("/rfqs", json=rfq_body(**kw), headers=auth(server["trader"]))
    assert created.status_code == 201, created.text
    rfq = created.json()
    quotes = client.get(f"/rfqs/{rfq['id']}/quotes", headers=auth(server["viewer"])).json()["data"]
    return rfq, quotes[0]


def incoming(server, **kw):
    return server["paper"].add_incoming_rfq(MARKET, quantity=D(kw.pop("quantity", "10")), **kw)


# ---------------------------------------------------------------------------
# Taker routes
# ---------------------------------------------------------------------------

class TestRfqs:
    async def test_create_list_get_quotes_and_cancel(self, server):
        client = server["client"]
        rfq, quote = new_rfq(server)
        assert rfq["venue"] == VENUE and rfq["market_id"] == MARKET and rfq["quantity"] == "10"
        assert rfq["book"] == "alpha" and rfq["trader"] == "yuki" and rfq["status"] == "open"
        assert quote["ask"] == "0.45" and quote["bid"] == "0.40" and quote["status"] == "open"

        listed = client.get("/rfqs", headers=auth(server["viewer"])).json()
        assert listed["count"] == 1 and listed["data"][0]["id"] == rfq["id"] and listed["data"][0]["book"] == "alpha"
        held = client.get("/rfqs?source=engine", headers=auth(server["viewer"])).json()
        assert [r["id"] for r in held["data"]] == [rfq["id"]]
        assert client.get("/rfqs?status=closed", headers=auth(server["viewer"])).json()["count"] == 0
        assert client.get(f"/rfqs?venue={VENUE}&market_id={MARKET}", headers=auth(server["viewer"])).json()["count"] == 1
        assert client.get("/rfqs?market_id=polymarket_us:other", headers=auth(server["viewer"])).json()["count"] == 0

        one = client.get(f"/rfqs/{rfq['id']}", headers=auth(server["viewer"]))
        assert one.status_code == 200 and one.json()["id"] == rfq["id"] and one.json()["book"] == "alpha"

        canceled = client.delete(f"/rfqs/{rfq['id']}", headers=auth(server["trader"]))
        assert canceled.status_code == 200
        assert canceled.json() == {"id": rfq["id"], "venue": VENUE, "kind": "rfq", "status": "canceled"}
        assert client.get(f"/rfqs/{rfq['id']}", headers=auth(server["viewer"])).json()["status"] == "closed"
        assert client.get("/rfqs?source=engine", headers=auth(server["viewer"])).json()["count"] == 0
        quotes = client.get(f"/rfqs/{rfq['id']}/quotes", headers=auth(server["viewer"])).json()["data"]
        assert quotes[0]["status"] == "canceled"
        assert client.get(f"/rfqs/{rfq['id']}/quotes?status=open", headers=auth(server["viewer"])).json()["count"] == 0

    async def test_by_amount_on_a_combo(self, server):
        client = server["client"]
        made = client.post("/combos", json={"legs": legs()}, headers=auth(server["trader"]))
        assert made.status_code == 200, made.text
        combo = made.json()
        again = client.post("/combos", json={"legs": legs()}, headers=auth(server["trader"])).json()
        assert again["market_id"] == combo["market_id"], "the same legs give the same combo"
        got = client.get(f"/combos/{combo['market_id']}", headers=auth(server["viewer"]))
        assert got.status_code == 200 and [l["market_id"] for l in got.json()["legs"]] == [MARKET, LEG_B]

        created = client.post("/rfqs", json={"market_id": combo["market_id"], "amount": "20"},
                              headers=auth(server["trader"]))
        assert created.status_code == 201, created.text
        rfq = created.json()
        assert rfq["combo_id"] == combo["market_id"] and rfq["market_id"] is None and rfq["amount"] == "20"
        assert rfq["combo"]["legs"][1]["outcome"] == "no"

    async def test_bodies_and_ids_refused(self, server):
        client = server["client"]
        both = client.post("/rfqs", json=rfq_body(amount="5"), headers=auth(server["trader"]))
        assert both.status_code == 422 and both.json()["error"]["code"] == "validation_error"
        assert client.get("/rfqs/nope", headers=auth(server["viewer"])).status_code == 404
        unknown = client.get("/rfqs/x?venue=nowhere", headers=auth(server["owner"]))
        assert unknown.status_code == 404 and unknown.json()["error"]["code"] == "unknown_venue"
        # A venue without RFQs refuses before anything is sent.
        no_rfq = client.post("/rfqs", json=rfq_body(market_id="kalshi:KX-A"), headers=auth(server["owner"]))
        assert no_rfq.status_code == 400 and "rfq_taker" in no_rfq.json()["error"]["message"]
        assert client.get(f"/combos/{VENUE}:combo-nope", headers=auth(server["viewer"])).status_code == 404

    async def test_a_venue_without_a_call_is_501(self, server):
        lookup = server["client"].post("/combos/lookup", json={"legs": legs()}, headers=auth(server["viewer"]))
        assert lookup.status_code == 501 and lookup.json()["error"]["code"] == "not_supported"


class TestAccept:
    async def test_accept_answers_202_at_once(self, server):
        client, engine = server["client"], server["engine"]
        _, quote = new_rfq(server)
        response = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote), headers=auth(server["trader"]))
        assert response.status_code == 202, response.text
        body = response.json()
        assert body["quote"]["status"] == "accepted" and body["final"] is False
        assert body["client_order_id"] == f"rfq-accept:{VENUE}:{quote['id']}" and body["intent_state"] == "sending"
        assert body["venue"] == VENUE and body["order_id"] is None
        # A retry never sends twice.
        again = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote), headers=auth(server["trader"]))
        assert again.status_code == 202 and server["paper"].calls.count("accept_quote") == 1
        assert engine.rfq.following

    async def test_wait_answers_200_when_the_quote_is_final(self, server):
        client, engine, paper = server["client"], server["engine"], server["paper"]
        _, quote = new_rfq(server)
        paper.auto_advance = True                       # the venue moves the quote each time it is read
        response = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote, wait=True, timeout_s=5),
                               headers=auth(server["trader"]))
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["final"] is True and body["quote"]["status"] == "executed"
        assert body["intent_state"] == "sent" and body["order_id"]
        positions = client.get("/positions", headers=auth(server["viewer"])).json()["data"]
        assert [(p["market_id"], D(p["contracts"])) for p in positions] == [(MARKET, D("10"))]

    async def test_wait_that_times_out_is_still_202(self, server):
        _, quote = new_rfq(server)
        response = server["client"].post(f"/quotes/{quote['id']}/accept",
                                         json=accept_body(quote, wait=True, timeout_s=0.05),
                                         headers=auth(server["trader"]))
        assert response.status_code == 202 and response.json()["final"] is False

    async def test_worst_price_refusal_carries_the_rule(self, server):
        client, paper = server["client"], server["paper"]
        _, quote = new_rfq(server)
        response = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote, worst_price="0.44"),
                               headers=auth(server["trader"]))
        assert response.status_code == 409
        error = response.json()["error"]
        assert error["code"] == "risk_rejected" and error["details"]["rule"] == "worst_price"
        assert "accept_quote" not in paper.calls, "nothing was sent"

    async def test_size_limit_refusal_on_create(self, server):
        response = server["client"].post("/rfqs", json=rfq_body(quantity="1000"), headers=auth(server["trader"]))
        assert response.status_code == 409
        assert response.json()["error"]["details"]["rule"] == "max_order_contracts"
        assert "create_rfq" not in server["paper"].calls

    async def test_no_answer_is_202_with_the_intent_key(self, server, monkeypatch):
        client, paper = server["client"], server["paper"]
        _, quote = new_rfq(server)

        async def lost(accept):
            raise NetworkError("connection reset")
        monkeypatch.setattr(paper, "accept_quote", lost)
        response = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote), headers=auth(server["trader"]))
        assert response.status_code == 202
        error = response.json()["error"]
        assert error["code"] == "submission_in_doubt" and error["details"]["retryable"] is True
        assert error["details"]["client_order_id"] == f"rfq-accept:{VENUE}:{quote['id']}"

    async def test_a_halt_refuses_the_accept(self, server):
        client = server["client"]
        _, quote = new_rfq(server)
        assert client.post("/halt", json={"reason": "test", "policy": "hold"}, headers=auth(server["trader"])).status_code == 200
        response = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote), headers=auth(server["trader"]))
        assert response.status_code == 409 and response.json()["error"]["details"]["rule"] == "kill_switch"


# ---------------------------------------------------------------------------
# Quoter routes
# ---------------------------------------------------------------------------

class TestQuoter:
    async def test_incoming_send_list_replace_cancel(self, server):
        client = server["client"]
        theirs = incoming(server)
        rows = client.get("/rfqs/incoming", headers=auth(server["viewer"])).json()
        assert [r["id"] for r in rows["data"]] == [theirs.id] and rows["data"][0]["mine"] is False
        assert client.get("/rfqs/incoming?source=engine", headers=auth(server["viewer"])).status_code == 200

        sent = client.post("/quotes", json=maker_body(theirs.id, expires_at=4_102_444_800_000),
                           headers=auth(server["trader"]))
        assert sent.status_code == 201, sent.text
        mine = sent.json()
        assert mine["mine"] is True and mine["bid"] == "0.40" and mine["ask"] == "0.50"

        listed = client.get("/quotes", headers=auth(server["viewer"])).json()
        assert [q["id"] for q in listed["data"]] == [mine["id"]]
        assert listed["data"][0]["expires_at"] == 4_102_444_800_000
        assert client.get(f"/quotes?rfq_id={theirs.id}&status=open", headers=auth(server["viewer"])).json()["count"] == 1

        replaced = client.put(f"/quotes/{mine['id']}", json=maker_body(theirs.id, bid="0.41", ask="0.49"),
                              headers=auth(server["trader"]))
        assert replaced.status_code == 200, replaced.text
        new = replaced.json()
        assert new["id"] != mine["id"] and new["bid"] == "0.41"

        gone = client.delete(f"/quotes/{new['id']}", headers=auth(server["trader"]))
        assert gone.status_code == 200 and gone.json()["kind"] == "quote"
        assert client.get("/quotes?status=open", headers=auth(server["viewer"])).json()["count"] == 0

    async def test_confirm_202_then_200_with_wait(self, server):
        client, paper = server["client"], server["paper"]
        theirs = incoming(server)
        mine = client.post("/quotes", json=maker_body(theirs.id), headers=auth(server["trader"])).json()
        paper.requester_accepts(mine["id"])
        confirmed = client.post(f"/quotes/{mine['id']}/confirm", json={}, headers=auth(server["trader"]))
        assert confirmed.status_code == 202, confirmed.text
        body = confirmed.json()
        assert body["quote"]["status"] == "confirmed" and body["client_order_id"] == f"rfq-confirm:{VENUE}:{mine['id']}"

        other = client.post("/quotes", json=maker_body(incoming(server).id), headers=auth(server["trader"])).json()
        paper.requester_accepts(other["id"])
        paper.auto_advance = True
        waited = client.post(f"/quotes/{other['id']}/confirm", json={"wait": True, "timeout_s": 5},
                             headers=auth(server["trader"]))
        assert waited.status_code == 200, waited.text
        assert waited.json()["quote"]["status"] == "executed" and waited.json()["intent_state"] == "sent"

    async def test_confirm_needs_no_body(self, server):
        client, paper = server["client"], server["paper"]
        mine = client.post("/quotes", json=maker_body(incoming(server).id), headers=auth(server["trader"])).json()
        paper.requester_accepts(mine["id"])
        assert client.post(f"/quotes/{mine['id']}/confirm", headers=auth(server["trader"])).status_code == 202

    async def test_send_refused_over_the_size_limit(self, server):
        theirs = incoming(server, quantity="1000")
        response = server["client"].post("/quotes", json=maker_body(theirs.id), headers=auth(server["trader"]))
        assert response.status_code == 409 and response.json()["error"]["details"]["rule"] == "max_order_contracts"


# ---------------------------------------------------------------------------
# Grants
# ---------------------------------------------------------------------------

class TestGrants:
    async def test_a_viewer_may_read_but_not_write(self, server):
        client, paper = server["client"], server["paper"]
        rfq, quote = new_rfq(server)
        theirs = incoming(server)
        mine = client.post("/quotes", json=maker_body(theirs.id), headers=auth(server["trader"])).json()
        viewer = auth(server["viewer"])
        writes = [
            ("post", "/rfqs", rfq_body()),
            ("delete", f"/rfqs/{rfq['id']}", None),
            ("post", f"/quotes/{quote['id']}/accept", accept_body(quote)),
            ("post", "/quotes", maker_body(theirs.id)),
            ("put", f"/quotes/{mine['id']}", maker_body(theirs.id, bid="0.41")),
            ("delete", f"/quotes/{mine['id']}", None),
            ("post", f"/quotes/{mine['id']}/confirm", {}),
            ("post", "/combos", {"legs": legs()}),
        ]
        calls_before = list(paper.calls)
        for method, path, body in writes:
            response = client.request(method.upper(), path, json=body, headers=viewer)
            assert response.status_code == 403, (method, path, response.text)
            assert response.json()["error"]["code"] == "forbidden"
            assert "may not trade" in response.json()["error"]["message"]
        assert paper.calls == calls_before, "a refused write reaches no venue"
        reads = ["/rfqs", f"/rfqs/{rfq['id']}", f"/rfqs/{rfq['id']}/quotes", "/rfqs/incoming", "/quotes"]
        for path in reads:
            assert client.get(path, headers=viewer).status_code == 200, path

    async def test_a_key_for_another_account_sees_nothing_here(self, server):
        client = server["client"]
        rfq, quote = new_rfq(server)
        other = auth(server["other"])
        assert client.get("/rfqs", headers=other).json()["count"] == 0
        assert client.get("/quotes", headers=other).json()["count"] == 0
        assert client.get(f"/rfqs/{rfq['id']}", headers=other).status_code == 403
        assert client.get(f"/rfqs/{rfq['id']}/quotes", headers=other).status_code == 403
        assert client.get(f"/rfqs?venue={VENUE}", headers=other).status_code == 403
        refused = client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote), headers=other)
        assert refused.status_code == 403 and ACCOUNT.key in refused.json()["error"]["message"]
        assert client.post("/rfqs", json=rfq_body(), headers=other).status_code == 403

    async def test_no_key_is_401(self, server):
        assert server["client"].get("/rfqs").status_code == 401


# ---------------------------------------------------------------------------
# Events and the halt
# ---------------------------------------------------------------------------

class TestEvents:
    async def test_the_socket_carries_rfq_quote_and_combo_events(self, server):
        client = server["client"]
        with client.websocket_connect(f"/ws/events?key={server['viewer']}&since=0&kinds=rfq,quote,combo") as socket:
            client.post("/combos", json={"legs": legs()}, headers=auth(server["trader"]))
            _, quote = new_rfq(server)
            client.post(f"/quotes/{quote['id']}/accept", json=accept_body(quote), headers=auth(server["trader"]))
            seen: list[dict] = []
            for _ in range(50):
                seen.append(json.loads(socket.receive_text()))
                if any(e["kind"] == "quote.accepted" for e in seen):
                    break
        kinds = [e["kind"] for e in seen]
        assert {"combo.saved", "rfq.created", "quote.accepted"} <= set(kinds), kinds
        assert all(k.startswith(("rfq.", "quote.", "combo.")) for k in kinds), "the kinds filter holds on replay too"
        accepted = next(e for e in seen if e["kind"] == "quote.accepted")
        assert accepted["key"] == f"{VENUE}:quote:{quote['id']}" and accepted["payload"]["status"] == "accepted"

    async def test_a_pushed_incoming_rfq_reaches_the_socket_at_once(self, server):
        """`rfq.incoming` goes from `engine.rfq.on_incoming` straight to the
        socket's queue: no poll, no tick in between."""
        import time

        client, engine, paper = server["client"], server["engine"], server["paper"]
        rfq = paper.add_incoming_rfq(MARKET, quantity=D("10"))
        before = len(engine.bus.subscribers)
        with client.websocket_connect(f"/ws/events?key={server['viewer']}&kinds=rfq.incoming") as socket:
            for _ in range(200):
                if len(engine.bus.subscribers) > before:
                    break
                time.sleep(0.005)
            start = time.perf_counter()
            client.portal.call(engine.rfq.on_incoming, VENUE, rfq)
            event = json.loads(socket.receive_text())
            elapsed_ms = (time.perf_counter() - start) * 1000
        assert event["kind"] == "rfq.incoming" and event["payload"]["id"] == rfq.id
        assert elapsed_ms < 400, elapsed_ms

    async def test_replay_carries_quote_events(self, server):
        client = server["client"]
        _, quote = new_rfq(server)
        await server["engine"].rfq.tick()             # the follower sees the maker's quote: quote.received
        with client.websocket_connect(f"/ws/events?key={server['viewer']}&since=0&kinds=quote") as socket:
            event = json.loads(socket.receive_text())
        assert event["kind"] == "quote.received" and event["payload"]["id"] == quote["id"]


class TestHalt:
    async def test_halt_cancels_our_rfqs_and_quotes(self, server):
        client, paper = server["client"], server["paper"]
        rfq, _ = new_rfq(server)
        mine = client.post("/quotes", json=maker_body(incoming(server).id), headers=auth(server["trader"])).json()
        halted = client.post("/halt", json={"reason": "stop", "policy": "cancel"}, headers=auth(server["trader"]))
        assert halted.status_code == 200, halted.text
        result = halted.json()["rfq"]
        assert result["rfqs"][VENUE] == 1 and result["quotes"][VENUE] == 1
        assert paper.rfqs[rfq["id"]].status == RfqStatus.CLOSED
        assert paper.quotes[mine["id"]].status == QuoteStatus.CANCELED
        assert client.get("/rfqs?source=engine", headers=auth(server["viewer"])).json()["count"] == 0
        assert client.get("/quotes?status=open", headers=auth(server["viewer"])).json()["count"] == 0
        refused = client.post("/rfqs", json=rfq_body(), headers=auth(server["trader"]))
        assert refused.status_code == 409 and refused.json()["error"]["details"]["rule"] == "kill_switch"


# ---------------------------------------------------------------------------
# The OpenAPI document
# ---------------------------------------------------------------------------

class TestOpenAPI:
    ROUTES = {
        "/rfqs": {"get", "post"}, "/rfqs/incoming": {"get"}, "/rfqs/{rfq_id}": {"get", "delete"},
        "/rfqs/{rfq_id}/quotes": {"get"}, "/quotes": {"get", "post"}, "/quotes/{quote_id}": {"put", "delete"},
        "/quotes/{quote_id}/accept": {"post"}, "/quotes/{quote_id}/confirm": {"post"}, "/combos": {"post"},
        "/combos/lookup": {"post"}, "/combos/{market_id}": {"get"},
    }

    async def test_the_routes_and_models_are_in_the_document(self, server):
        document = server["client"].get("/openapi.json").json()
        for path, methods in self.ROUTES.items():
            assert methods <= set(document["paths"].get(path, {})), path
        schemas = document["components"]["schemas"]
        assert {"RfqView", "RfqQuote", "QuoteResult", "Combo", "ComboLeg", "RfqBody", "AcceptBody",
                "MakerQuoteBody", "Rfq", "Canceled"} <= set(schemas)
        accept = document["paths"]["/quotes/{quote_id}/accept"]["post"]["responses"]
        for code in ("200", "202"):
            assert accept[code]["content"]["application/json"]["schema"]["$ref"].endswith("/QuoteResult")
        assert {"409", "403", "404", "501"} <= set(accept)
        created = document["paths"]["/rfqs"]["post"]["responses"]["201"]
        assert created["content"]["application/json"]["schema"]["$ref"].endswith("/RfqView")
        assert "rfq" in document["components"]["schemas"]["HaltResult"]["properties"]
