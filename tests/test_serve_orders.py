"""`/trading/orders` through a whole `synpath serve`, on the mocked Kalshi.

What a client of the server (the terminal, `synpath.Client(server=...)`)
relies on: an order whose outcome is not known yet is a 202 with its own
error code and the `client_order_id` to retry under, and the retry never
sends twice; a cancel never relabels a fill; and the order list shows every
state, engine-held parents included, newest first.
"""
from __future__ import annotations

from decimal import Decimal as D

import pytest

from serve_harness import MockKalshi, receive_until, serve_harness  # noqa: F401  (the fixture)
from synpath.trading.errors import CancelUnconfirmed, SubmissionInDoubt, SubmissionPending
from synpath.trading.types import OrderRequest, OrderStatus

K = "kalshi:KXHARNESS-1"


def limit(**changes) -> dict:
    return {"market_id": K, "side": "buy", "quantity": "2", "type": "limit", "price": "0.40",
            "time_in_force": "gtc", **changes}


def place(harness, body: dict):
    return harness.client.post("/trading/orders", headers=harness.auth, json=body)


class TestInDoubt:
    def test_a_lost_response_is_202_submission_in_doubt_and_the_retry_adopts_without_resending(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            first = place(harness, limit(client_order_id="ticket-1"))
            assert first.status_code == 202, first.text
            error = first.json()["error"]
            assert error["code"] == "submission_in_doubt"
            assert error["details"]["retryable"] is True
            assert error["details"]["client_order_id"] == "ticket-1"
            assert error["details"]["venue"] == "kalshi"

            kalshi.lose_create_response = False
            again = place(harness, limit(client_order_id="ticket-1"))
            assert again.status_code == 201, again.text
            assert again.json()["client_order_id"] == "ticket-1" and again.json()["status"] == "open"
            assert len(kalshi.creates) == 1, "the retry checked the venue and never sent again"

    def test_a_retry_while_the_venue_has_no_record_yet_is_202_submission_pending(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            assert place(harness, limit(client_order_id="ticket-2")).status_code == 202
            kalshi.orders.clear()   # the venue never took it, as far as it will say yet

            again = place(harness, limit(client_order_id="ticket-2"))
            assert again.status_code == 202, again.text
            error = again.json()["error"]
            assert error["code"] == "submission_pending"
            assert error["details"]["retryable"] is True and error["details"]["client_order_id"] == "ticket-2"
            assert len(kalshi.creates) == 1

    def test_an_order_sent_without_an_id_is_in_doubt_under_the_id_the_engine_gave_it(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            first = place(harness, limit())
            assigned = first.json()["error"]["details"]["client_order_id"]
            assert first.status_code == 202 and assigned == kalshi.creates[0]["client_order_id"]

            kalshi.lose_create_response = False
            again = place(harness, limit(client_order_id=assigned))
            assert again.status_code == 201 and again.json()["id"] == "kx-1"
            assert len(kalshi.creates) == 1

    def test_a_different_instruction_under_the_same_id_is_refused(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            assert place(harness, limit(client_order_id="ticket-3")).status_code == 202
            other = place(harness, limit(client_order_id="ticket-3", quantity="3"))
            assert other.status_code == 400 and len(kalshi.creates) == 1

    def test_the_python_client_raises_the_in_doubt_types_with_the_id(self, serve_harness):
        kalshi = MockKalshi(lose_create_response=True)
        with serve_harness(kalshi=kalshi) as harness:
            client = harness.remote_client()
            request = OrderRequest(market_id=K, side="buy", quantity="2", price="0.40", client_order_id="ticket-4")
            try:
                with pytest.raises(SubmissionInDoubt) as in_doubt:
                    harness.call(client.create_order, request)
                assert in_doubt.value.client_order_id == "ticket-4"

                saved = dict(kalshi.orders)
                kalshi.orders.clear()
                with pytest.raises(SubmissionPending) as pending:
                    harness.call(client.create_order, request)
                assert pending.value.client_order_id == "ticket-4"

                kalshi.orders.update(saved)
                kalshi.lose_create_response = False
                order = harness.call(client.create_order, request)
                assert order.client_order_id == "ticket-4" and len(kalshi.creates) == 1
            finally:
                harness.call(client.close)


class TestCancel:
    def test_cancelling_an_order_the_venue_filled_returns_it_closed(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = place(harness, limit(client_order_id="cancel-1")).json()
            kalshi.fill(placed["id"])

            cancelled = harness.client.delete(f"/trading/orders/{placed['id']}", headers=harness.auth)
            assert cancelled.status_code == 200, cancelled.text
            assert cancelled.json()["status"] == "closed" and D(cancelled.json()["filled"]) == 2
            again = harness.client.delete(f"/trading/orders/{placed['id']}", headers=harness.auth)
            assert again.json()["status"] == "closed" and kalshi.cancels == [placed["id"]]

    def test_a_cancel_the_venue_cannot_place_is_409_cancel_unconfirmed(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = place(harness, limit(client_order_id="cancel-2")).json()
            kalshi.forgotten.add(placed["id"])

            refused = harness.client.delete(f"/trading/orders/{placed['id']}", headers=harness.auth)
            assert refused.status_code == 409
            assert refused.json()["error"]["code"] == "cancel_unconfirmed"
            assert refused.json()["error"]["details"]["retryable"] is False
            shown = harness.client.get(f"/trading/orders/{placed['id']}", headers=harness.auth).json()
            assert shown["status"] == "open"

            client = harness.remote_client()
            try:
                with pytest.raises(CancelUnconfirmed):
                    harness.call(client.cancel_order, placed["id"])
            finally:
                harness.call(client.close)


def orders(harness, **params):
    response = harness.client.get("/trading/orders", headers=harness.auth, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def every_page(harness, **params):
    rows, cursor, pages = [], None, 0
    while True:
        page = orders(harness, **params, **({"cursor": cursor} if cursor else {}))
        rows += page["data"]
        pages += 1
        cursor = page["next_cursor"]
        if not cursor:
            return rows, pages


class TestListing:
    """One of each state a Kalshi limit, stop-market or stop-limit can be in,
    placed a few milliseconds apart so "newest first" is observable."""

    def place_one_of_everything(self, harness, kalshi) -> dict[str, dict]:
        import time

        def step():
            time.sleep(0.005)

        placed: dict[str, dict] = {}
        placed["resting"] = place(harness, limit(client_order_id="list-resting")).json()
        step()
        placed["filled"] = place(harness, limit(client_order_id="list-filled", time_in_force="ioc")).json()
        step()
        cancelled = place(harness, limit(client_order_id="list-cancelled")).json()
        harness.client.delete(f"/trading/orders/{cancelled['id']}", headers=harness.auth)
        placed["cancelled"] = cancelled
        step()
        placed["waiting"] = place(harness, {"market_id": K, "side": "sell", "quantity": "1", "type": "stop_market",
                                            "stop_price": "0.10", "client_order_id": "list-waiting"}).json()
        step()
        placed["triggered"] = place(harness, {"market_id": K, "side": "buy", "quantity": "1", "type": "stop_limit",
                                              "stop_price": "0.45", "price": "0.46", "time_in_force": "gtc",
                                              "client_order_id": "list-triggered"}).json()
        stream = harness.stream("kalshi")
        harness.wait_until(lambda: K in harness.stack.feeds.watching["kalshi"])
        harness.push("kalshi", stream.snapshot(K, bids=[("0.44", "5")], asks=[("0.46", "5")]))
        engine = harness.stack.engine
        harness.wait_until(lambda: bool(engine.orders.get(placed["triggered"]["id"]).children))
        step()
        placed["elsewhere"] = place(harness, limit(market_id="kalshi:KXHARNESS-2",
                                                   client_order_id="list-elsewhere")).json()
        return placed

    def test_open_stays_the_default_and_all_shows_every_state_newest_first(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = self.place_one_of_everything(harness, kalshi)
            parent_id = placed["triggered"]["id"]
            child_id = harness.stack.engine.orders.get(parent_id).children[0].order_id

            default = {row["id"]: row["status"] for row in orders(harness)["data"]}
            assert default == {placed["resting"]["id"]: "open", placed["waiting"]["id"]: "waiting",
                               parent_id: "triggered", child_id: "open", placed["elsewhere"]["id"]: "open"}

            everything = orders(harness, scope="all")
            assert everything["next_cursor"] is None and everything["count"] == 7
            statuses = [(row["client_order_id"], row["status"], row["held_by"]) for row in everything["data"]]
            child = next(row for row in everything["data"] if row["id"] == child_id)
            assert child["parent_id"] == parent_id and child["held_by"] == "venue"
            assert statuses[0] == ("list-elsewhere", "open", "venue"), "newest first"
            assert ("list-triggered", "triggered", "engine") in statuses
            assert statuses[-3:] == [("list-cancelled", "canceled", "venue"), ("list-filled", "closed", "venue"),
                                     ("list-resting", "open", "venue")]
            assert ("list-waiting", "waiting", "engine") in statuses

    def test_all_pages_by_cursor_and_filters_by_market_and_time(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = self.place_one_of_everything(harness, kalshi)
            everything = [row["id"] for row in orders(harness, scope="all")["data"]]

            paged, pages = every_page(harness, scope="all", limit=2)
            assert [row["id"] for row in paged] == everything and pages == 4

            on_market = orders(harness, scope="all", market_id=K)["data"]
            assert len(on_market) == 6 and all(row["market_id"] == K for row in on_market)
            assert [row["id"] for row in orders(harness, market_id="kalshi:KXHARNESS-2")["data"]] == \
                [placed["elsewhere"]["id"]]

            since_ms = harness.call(harness.stack.engine.journal.order, "kalshi", placed["waiting"]["id"]).created_at
            recent = {row["client_order_id"] for row in orders(harness, scope="all", since=since_ms)["data"]}
            assert recent >= {"list-waiting", "list-triggered", "list-elsewhere"}
            assert not recent & {"list-resting", "list-filled", "list-cancelled"}

    def test_a_finished_parent_is_listed_after_a_restart(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            parent = place(harness, {"market_id": K, "side": "sell", "quantity": "1", "type": "stop_market",
                                     "stop_price": "0.10", "client_order_id": "list-ended"}).json()
            harness.client.delete(f"/trading/orders/{parent['id']}", headers=harness.auth)
            engine = harness.stack.engine
            harness.call(lambda: engine.orders.parents.pop(parent["id"]))   # what a restart leaves
            rows = orders(harness, scope="all")["data"]
            assert [(row["id"], row["status"], row["held_by"]) for row in rows] == \
                [(parent["id"], "canceled", "engine")]

    def test_bad_listing_requests_are_refused(self, serve_harness):
        with serve_harness() as harness:
            for params in ({"scope": "everything"}, {"limit": 0}, {"limit": 1001}):
                response = harness.client.get("/trading/orders", headers=harness.auth, params=params)
                assert response.status_code == 422, (params, response.text)
            response = harness.client.get("/trading/orders", headers=harness.auth,
                                          params={"scope": "all", "cursor": "not-a-cursor"})
            assert response.status_code == 400 and response.json()["error"]["code"] == "bad_request"
            assert harness.client.get("/trading/orders", params={"scope": "all"}).status_code == 401

    def test_the_python_client_lists_every_state_through_the_server(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            self.place_one_of_everything(harness, kalshi)
            client = harness.remote_client()
            try:
                first = harness.call(lambda: client.fetch_orders(limit=3))
                assert len(first) == 3 and first.next_cursor
                rest = harness.call(lambda: client.fetch_orders(limit=10, cursor=first.next_cursor))
                assert len(rest) == 4 and rest.next_cursor is None
                assert {o.status for o in [*first, *rest]} >= {OrderStatus.CLOSED, OrderStatus.CANCELED,
                                                               OrderStatus.WAITING, OrderStatus.TRIGGERED}
                open_only = harness.call(lambda: client.fetch_orders(scope="open"))
                assert {o.status for o in open_only} <= {OrderStatus.OPEN, OrderStatus.WAITING,
                                                         OrderStatus.TRIGGERED}
            finally:
                harness.call(client.close)


class TestStopsReadTheBookTheyAreHanded:
    """A stop reads the book the engine already holds the moment it starts
    watching, not only the next change. Kalshi sends no fresh snapshot for a
    market that is already subscribed on the connection (the screen's
    `/ws/market`, say), so a quiet market whose ask already meets the stop
    would otherwise leave it waiting forever."""

    @staticmethod
    def stop(**changes) -> dict:
        return {"market_id": K, "side": "buy", "quantity": "1", "type": "stop_limit", "stop_price": "0.94",
                "price": "0.95", "time_in_force": "gtc", **changes}

    @staticmethod
    def screen_loaded(harness, socket, *, bid: str = "0.92", ask: str = "0.94") -> None:
        receive_until(socket, "subscribed")
        harness.push("kalshi", harness.stream("kalshi").snapshot(K, bids=[(bid, "5")], asks=[(ask, "5")]))
        receive_until(socket, "book", ready=True)

    @staticmethod
    def state(harness, parent_id: str) -> str:
        return harness.stack.engine.orders.get(parent_id).state

    def test_a_stop_already_met_on_a_book_the_screen_loaded_triggers_without_another_update(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}&market_id={K}") as socket:
                self.screen_loaded(harness, socket)
                placed = place(harness, self.stop(client_order_id="loaded-1"))
                assert placed.status_code == 201, placed.text
                harness.wait_until(lambda: len(kalshi.creates) == 1)
                assert kalshi.creates[0]["price"] == "0.9500"
                assert self.state(harness, placed.json()["id"]) == "working"

    def test_a_stop_met_by_the_first_snapshot_of_a_market_nobody_watched_triggers(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            placed = place(harness, self.stop(client_order_id="fresh-1")).json()
            harness.wait_until(lambda: K in harness.stack.feeds.watching["kalshi"])
            assert self.state(harness, placed["id"]) == "waiting" and not kalshi.creates
            harness.push("kalshi", harness.stream("kalshi").snapshot(K, bids=[("0.92", "5")], asks=[("0.94", "5")]))
            harness.wait_until(lambda: len(kalshi.creates) == 1)

    def test_a_stop_not_met_on_a_loaded_book_keeps_waiting(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}&market_id={K}") as socket:
                self.screen_loaded(harness, socket, ask="0.93")
                placed = place(harness, self.stop(client_order_id="loaded-2")).json()
                harness.wait_until(lambda: K in harness.stack.feeds.watching["kalshi"])
                harness.call(harness.stack.feeds.sync)
                assert self.state(harness, placed["id"]) == "waiting" and not kalshi.creates

    def test_a_mid_stop_reads_the_loaded_book_and_a_last_stop_waits_for_a_new_print(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}&market_id={K}") as socket:
                self.screen_loaded(harness, socket, bid="0.92", ask="0.96")
                last = place(harness, self.stop(stop_price="0.90", trigger_source="last",
                                                client_order_id="loaded-last")).json()
                mid = place(harness, self.stop(trigger_source="mid", client_order_id="loaded-mid")).json()
                harness.wait_until(lambda: len(kalshi.creates) == 1)
                assert self.state(harness, mid["id"]) == "working"
                assert self.state(harness, last["id"]) == "waiting", "a book is not a trade"

    def test_a_new_stop_on_a_market_the_engine_already_watches_reads_its_book_at_once(self, serve_harness):
        kalshi = MockKalshi()
        with serve_harness(kalshi=kalshi) as harness:
            place(harness, self.stop(side="sell", type="stop_market", stop_price="0.10", price=None,
                                     client_order_id="watching"))
            harness.wait_until(lambda: K in harness.stack.feeds.watching["kalshi"])
            harness.push("kalshi", harness.stream("kalshi").snapshot(K, bids=[("0.92", "5")], asks=[("0.94", "5")]))
            harness.wait_until(lambda: harness.stack.engine.books[K].best_ask == D("0.94"))

            placed = place(harness, self.stop(client_order_id="second"))
            assert placed.status_code == 201, placed.text
            assert placed.json()["status"] == "triggered" and len(kalshi.creates) == 1
