"""Market orders through a whole `synpath serve`, on the mocked Kalshi, on SQLite and PostgreSQL.

The reference pattern for an order type (lifecycle doc, instruction table,
save-before-send, offline tests on both stores) applied to `market`: Kalshi
has no market order type, so a market order is one immediate-or-cancel limit
at the worst price the caller gives (the terminal gives Kalshi's legal bound,
0.99 for a YES bid and 0.01 for a YES ask). The instruction is in
`market_orders` before the venue is called; a retry under the same
`client_order_id` never sends twice; an unanswered send is in doubt and is
adopted, not resent. See `synpath/engine/orders/lifecycle/market.md`.
"""
from __future__ import annotations

import json
from decimal import Decimal as D

from serve_harness import MockKalshi, serve_harness  # noqa: F401  (the fixture)

K = "kalshi:KXHARNESS-1"


def market(**changes) -> dict:
    return {"market_id": K, "side": "buy", "quantity": "2", "type": "market", "price": "0.99",
            "time_in_force": "ioc", **changes}


def place(harness, body: dict):
    return harness.client.post("/trading/orders", headers=harness.auth, json=body)


def market_rows(harness) -> list[dict]:
    """`market_orders` as stored, on whichever backend the harness runs."""
    journal = harness.stack.engine.journal

    async def read() -> list[dict]:
        async with journal._db.execute(
                "SELECT client_order_id, side, price, state, time_in_force, request, snapshot "
                "FROM market_orders ORDER BY created_ts") as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    return harness.call(read)


class TestMarketOrders:
    def test_a_market_order_is_saved_then_sent_once_as_ioc_at_its_worst_price_and_closes(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = place(harness, market(client_order_id="mkt-1", tags={"terminal_outcome": "yes"}))
            assert placed.status_code == 201, placed.text
            order = placed.json()
            assert (order["type"], order["status"], D(order["filled"])) == ("market", "closed", D(2))
            assert order["tags"]["terminal_outcome"] == "yes"
            [sent] = kalshi.creates
            assert (sent["side"], D(sent["price"]), sent["time_in_force"]) == ("bid", D("0.99"), "immediate_or_cancel")
            [row] = market_rows(harness)
            assert (row["client_order_id"], row["side"], D(row["price"])) == ("mkt-1", "buy", D("0.99"))
            assert json.loads(row["request"])["price"] == "0.99", "the instruction is kept as it was made"
            assert row["state"] == "closed"
            listed = harness.client.get("/trading/orders", headers=harness.auth, params={"scope": "all"}).json()
            assert [(o["id"], o["type"], o["tags"].get("terminal_outcome")) for o in listed["data"]] == \
                [(order["id"], "market", "yes")]

    def test_a_sell_yes_at_the_lowest_legal_price_keeps_its_outcome_and_action(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = place(harness, market(side="sell", outcome="yes", action="sell", price="0.01",
                                           client_order_id="mkt-s"))
            assert placed.status_code == 201, placed.text
            assert (placed.json()["outcome"], placed.json()["action"]) == ("yes", "sell")
            [sent] = kalshi.creates
            assert (sent["side"], D(sent["price"])) == ("ask", D("0.01")) and "reduce_only" not in sent
            [row] = market_rows(harness)
            assert row["side"] == "sell" and json.loads(row["request"])["action"] == "sell"
            refused = place(harness, market(side="sell", price="0.01", reduce_only=True, client_order_id="mkt-r"))
            assert refused.status_code in (400, 422) and "reduce_only is removed" in refused.text

    def test_the_same_client_order_id_twice_sends_once(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            first = place(harness, market(client_order_id="mkt-2")).json()
            again = place(harness, market(client_order_id="mkt-2")).json()
            assert first["id"] == again["id"]
            assert len(kalshi.creates) == 1
            assert len(market_rows(harness)) == 1

    def test_a_lost_response_is_in_doubt_and_the_retry_adopts_without_resending(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            doubt = place(harness, market(client_order_id="mkt-3"))
            assert doubt.status_code == 202, doubt.text
            assert doubt.json()["error"]["code"] == "submission_in_doubt"
            [row] = market_rows(harness)
            assert row["client_order_id"] == "mkt-3", "saved before the send"
            kalshi.lose_create_response = False
            adopted = place(harness, market(client_order_id="mkt-3"))
            assert adopted.status_code == 201, adopted.text
            assert adopted.json()["status"] == "closed"
            assert len(kalshi.creates) == 1, "checked with Kalshi, never sent twice"

    def test_another_instruction_under_the_same_id_is_refused(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            place(harness, market(client_order_id="mkt-4"))
            refused = place(harness, market(client_order_id="mkt-4", quantity="3"))
            assert refused.status_code in (400, 409, 422), refused.text
            assert len(kalshi.creates) == 1

    def test_kalshi_refuses_a_market_order_without_a_worst_price_and_sends_nothing(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            refused = place(harness, market(price=None, client_order_id="mkt-5"))
            assert 400 <= refused.status_code < 500, refused.text
            assert kalshi.creates == []
