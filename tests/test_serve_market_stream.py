"""`/ws/market`: live books, trades and quotes over shared, reference-counted venue connections."""
from __future__ import annotations

import pytest

from serve_harness import receive_until, serve_harness  # noqa: F401  (the fixture)

P, P2, K = "polymarket:1", "polymarket:2", "kalshi:KXHARNESS-1"


def url(harness, *markets: str, channels: str = "book", **extra: str) -> str:
    query = "&".join([f"key={harness.token}", f"channels={channels}", *(f"market_id={m}" for m in markets),
                      *(f"{k}={v}" for k, v in extra.items())])
    return f"/ws/market?{query}"


def levels(message, side):
    return [(level["price"], level["size"]) for level in message[side]]


def hub_has_no_connection(harness, venue):
    return lambda: venue not in harness.stack.market_streams.connections


class TestSubscribing:
    def test_a_subscriber_gets_the_book_then_every_update(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(url(harness, P, channels="book,trades,quotes")) as socket:
                subscribed = socket.receive_json()
                assert subscribed == {"type": "subscribed", "venue": "polymarket", "market_id": P, "side": "yes",
                                      "channels": ["book", "quotes", "trades"], "unsupported": []}
                waiting = receive_until(socket, "book")
                assert waiting["ready"] is False and waiting["bids"] == [], "no snapshot yet: no levels"

                stream = harness.stream("polymarket")
                harness.push("polymarket", stream.snapshot(P, bids=[("0.40", "10"), ("0.39", "5")], asks=[("0.45", "3")]))
                book = receive_until(socket, "book")
                assert book["ready"] and levels(book, "bids") == [("0.40", "10"), ("0.39", "5")]
                assert levels(book, "asks") == [("0.45", "3")] and book["best_bid"] == "0.40"

                harness.push("polymarket", stream.delta(P, "bid", "0.41", "2"))
                assert levels(receive_until(socket, "book"), "bids")[0] == ("0.41", "2")

                harness.push("polymarket", stream.trade(P, "0.42", "4"))
                trade = receive_until(socket, "trade")
                assert (trade["market_id"], trade["price"], trade["quantity"]) == (P, "0.42", "4")

                harness.push("polymarket", stream.quote(P, "0.41", "0.45", side="no"))
                harness.push("polymarket", stream.quote(P, "0.41", "0.45"))
                quote = receive_until(socket, "quote")
                assert quote["side"] == "yes" and quote["bid"] == "0.41", "only the subscribed side's quotes"
            assert [kind for kind, _ in stream.watched] == ["book", "quotes", "trades"]

    def test_subscribe_and_unsubscribe_as_messages_with_depth_and_side(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}") as socket:
                socket.send_json({"op": "subscribe", "market_id": P, "side": "no", "depth": 1})
                assert receive_until(socket, "subscribed")["side"] == "no"
                stream = harness.stream("polymarket")
                harness.push("polymarket", stream.snapshot(P, bids=[("0.40", "10"), ("0.39", "5")], asks=[("0.45", "3")]))
                book = receive_until(socket, "book", ready=True)
                assert book["side"] == "no" and levels(book, "bids") == [("0.55", "3")], "the NO view, one level"
                socket.send_json({"op": "unsubscribe", "market_id": P})
                assert receive_until(socket, "unsubscribed")["market_id"] == P
                socket.send_json({"op": "unsubscribe", "market_id": P})
                assert receive_until(socket, "error")["code"] == "not_subscribed"

    @pytest.mark.parametrize("request_, code", [
        ({"op": "subscribe", "market_id": "KXBARE"}, "bad_market_id"),
        ({"op": "subscribe", "market_id": P, "channels": ["orders"]}, "bad_request"),
        ({"op": "subscribe", "market_id": "sx_bet:0xabc"}, "credentials_required"),
        ({"op": "dance"}, "bad_request"),
    ])
    def test_a_request_the_server_cannot_serve_gets_an_error_frame(self, serve_harness, request_, code):
        with serve_harness() as harness:
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}") as socket:
                socket.send_json(request_)
                assert receive_until(socket, "error")["code"] == code
                socket.send_json({"op": "subscribe", "market_id": P})
                assert receive_until(socket, "subscribed")["market_id"] == P, "the socket stays usable"

    def test_a_channel_the_venue_does_not_stream_is_reported_not_faked(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}") as socket:
                socket.send_json({"op": "subscribe", "market_id": "limitless:some-slug", "channels": ["book", "trades"]})
                subscribed = receive_until(socket, "subscribed")
            assert subscribed["channels"] == ["book"] and subscribed["unsupported"] == ["trades"]


class TestSharing:
    def test_two_subscribers_share_one_venue_connection(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(url(harness, P)) as first, \
                    harness.client.websocket_connect(url(harness, P)) as second:
                receive_until(first, "subscribed")
                receive_until(second, "subscribed")
                assert len(harness.opened["polymarket"]) == 1, "one connection"
                stream = harness.stream("polymarket")
                assert stream.watched == [("book", (P,))], "subscribed upstream once"
                harness.push("polymarket", stream.snapshot(P, bids=[("0.40", "10")]))
                assert receive_until(first, "book", ready=True)["best_bid"] == "0.40"
                assert receive_until(second, "book", ready=True)["best_bid"] == "0.40"

    def test_the_last_subscriber_leaving_unwatches_and_closes_the_connection(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(url(harness, P, P2)) as first:
                receive_until(first, "subscribed", market_id=P2)
                with harness.client.websocket_connect(url(harness, P)) as second:
                    receive_until(second, "subscribed")
                stream = harness.stream("polymarket")
                harness.wait_until(lambda: len(harness.stack.market_streams.connections["polymarket"]
                                               .sessions_by_market[P]) == 1)
                assert stream.unwatched == [] and not stream.closed, "the first subscriber still holds both"
                first.send_json({"op": "unsubscribe", "market_id": P2})
                receive_until(first, "unsubscribed")
                assert stream.unwatched == [P2] and not stream.closed
            harness.wait_until(hub_has_no_connection(harness, "polymarket"))
            assert stream.unwatched == [P2, P] and stream.closed

            with harness.client.websocket_connect(url(harness, P)) as again:
                receive_until(again, "subscribed")
            assert len(harness.opened["polymarket"]) == 2, "a new subscriber opens a fresh connection"

    def test_the_engine_and_the_screen_share_the_engines_connection(self, serve_harness):
        with serve_harness() as harness:
            engine_stream = harness.stream("kalshi")
            with harness.client.websocket_connect(url(harness, K)) as socket:
                receive_until(socket, "subscribed")
                assert "kalshi" not in harness.opened, "no second Kalshi connection"
                harness.push("kalshi", engine_stream.snapshot(K, bids=[("0.30", "8")], asks=[("0.35", "2")]))
                assert receive_until(socket, "book", ready=True)["best_ask"] == "0.35"

                # A stop on the same market: the engine watches the very book the screen reads.
                placed = harness.client.post("/trading/orders", headers=harness.auth, json={
                    "market_id": K, "side": "sell", "quantity": "1", "type": "stop_market", "stop_price": "0.10",
                    "time_in_force": "gtc", "book": "harness"})
                assert placed.status_code in (201, 202), placed.text
                feeds = harness.stack.feeds
                harness.wait_until(lambda: K in feeds.watching["kalshi"])
                assert harness.stack.engine.books[K] is engine_stream.books[K]
            harness.wait_until(lambda: not harness.stack.market_streams.connections["kalshi"].sessions_by_market)
            assert engine_stream.watched.count(("book", (K,))) == 2, "the engine's sync re-subscribes idempotently"
            assert not engine_stream.closed, "the engine's connection outlives the screen"
            assert harness.stack.engine.books[K] is engine_stream.books[K]

    def test_a_market_the_engine_watches_is_not_unwatched_when_the_screen_leaves(self, serve_harness):
        with serve_harness() as harness:
            hub = harness.stack.market_streams
            harness.call(lambda: hub.feeds.watching.setdefault("polymarket", set()).add(P))
            with harness.client.websocket_connect(url(harness, P)) as socket:
                receive_until(socket, "subscribed")
            stream = harness.stream("polymarket")
            harness.wait_until(hub_has_no_connection(harness, "polymarket"))
            assert stream.unwatched == [], "the engine still holds it"


class TestReconnecting:
    def test_after_a_drop_the_book_is_marked_not_ready_then_the_fresh_snapshot_is_sent(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(url(harness, P)) as socket:
                receive_until(socket, "subscribed")
                stream = harness.stream("polymarket")
                harness.push("polymarket", stream.snapshot(P, bids=[("0.40", "10")]))
                receive_until(socket, "book", ready=True)

                harness.push("polymarket", stream.drop())
                assert receive_until(socket, "status")["state"] == "disconnected"
                stale = receive_until(socket, "book")
                assert stale["ready"] is False and stale["bids"] == [], "a book the stream lost is not shown"

                harness.push("polymarket", stream.reconnected())
                assert receive_until(socket, "status")["state"] == "connected"
                harness.push("polymarket", stream.snapshot(P, bids=[("0.44", "1")]))
                fresh = receive_until(socket, "book", ready=True)
                assert levels(fresh, "bids") == [("0.44", "1")]

    def test_a_stream_that_ends_tells_its_subscribers_and_the_next_subscribe_reopens(self, serve_harness):
        with serve_harness() as harness:
            with harness.client.websocket_connect(url(harness, P)) as socket:
                receive_until(socket, "subscribed")
                harness.call(harness.stream("polymarket").push, None)     # refused for good: iteration ends
                assert receive_until(socket, "error")["code"] == "stream_ended"
                socket.send_json({"op": "subscribe", "market_id": P})
                receive_until(socket, "subscribed")
            assert len(harness.opened["polymarket"]) == 2


class TestVenueStatus:
    def states(self, harness):
        answer = harness.client.get("/market/streams", headers=harness.auth)
        assert answer.status_code == 200, answer.text
        return {row["venue"]: row["state"] for row in answer.json()}

    def test_each_open_connection_reports_its_own_state(self, serve_harness):
        with serve_harness() as harness:
            assert harness.client.get("/market/streams").status_code == 401, "the token is required"
            assert "polymarket" not in self.states(harness), "nothing streams Polymarket yet"
            with harness.client.websocket_connect(url(harness, P)) as socket:
                receive_until(socket, "subscribed")
                stream = harness.stream("polymarket")
                harness.push("polymarket", stream.reconnected())
                receive_until(socket, "status")
                assert self.states(harness)["polymarket"] == "connected"
                harness.push("polymarket", stream.drop())
                receive_until(socket, "status")
                assert self.states(harness)["polymarket"] == "reconnecting"
            assert "kalshi" in self.states(harness), "the engine's own Kalshi connection is always listed"


def test_streams_turned_off_answer_with_an_error_not_a_poll(serve_harness, tmp_path):
    with serve_harness(config={"streams": False}) as harness:
        with harness.client.websocket_connect(url(harness, P)) as socket:
            assert receive_until(socket, "error")["code"] == "streams_disabled"
        assert "polymarket" not in harness.opened
