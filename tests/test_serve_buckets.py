"""Buckets through `synpath serve`: definitions over REST, the merged book on
`/ws/market`, and the router's preview, each against the library's own router."""
from __future__ import annotations

import uuid
from decimal import Decimal as D
from typing import Any

from synpath.bucket import Bucket, BucketMember
from synpath.engine import router
from synpath.engine.paper import PaperVenue
from synpath.trading.types import Account, Side
from synpath.types import FeeSchedule, OrderBook, OrderLevel

from serve_harness import receive_until, serve_harness  # noqa: F401  (the fixture)

K, P, P2 = "kalshi:KXHARNESS-1", "polymarket:1", "polymarket:2"
POLY_THETA = 0.02


class StubPolymarket:
    """Polymarket's read adapter for buckets: a published fee schedule and a
    book, from memory. Every call is recorded."""

    id = "polymarket"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        self.calls.append(("fee", market_id))
        return FeeSchedule(venue="polymarket", scope="market", scope_id=market_id, fee_type="quadratic_theta",
                           taker_rate=POLY_THETA, maker_rate=0.0)

    def fetch_order_book(self, market_id: str, *, side: str = "yes", depth: int | None = None) -> OrderBook:
        self.calls.append(("book", market_id))
        return OrderBook(market_id=market_id, side=side, venue="polymarket",
                         bids=[OrderLevel(price=0.38, size=20)], asks=[OrderLevel(price=0.44, size=20)])

    def close(self) -> None:
        pass


def bucket_harness(serve_harness, **kwargs: Any):
    poly = PaperVenue(venue="polymarket", cash=D("10000"), account=Account(venue="polymarket", name="paper"))
    return serve_harness(trading={"polymarket": poly}, exchanges={"polymarket": StubPolymarket()}, **kwargs)


def definition(**extra: Any) -> dict[str, Any]:
    return {"book": "terminal", "name": "Same question",
            "members": [{"market_id": K}, {"market_id": P, "flip": True}], **extra}


def create(harness, **extra: Any) -> dict[str, Any]:
    made = harness.client.post("/trading/buckets", json=definition(**extra), headers=harness.auth)
    assert made.status_code == 201, made.text
    return made.json()


def market_url(harness, *markets: str, channels: str = "book", **extra: str) -> str:
    query = "&".join([f"key={harness.token}", f"channels={channels}", *(f"market_id={m}" for m in markets),
                      *(f"{k}={v}" for k, v in extra.items())])
    return f"/ws/market?{query}"


def stored_bucket(raw: dict[str, Any]) -> Bucket:
    return Bucket.model_validate(raw)


def expected_levels(harness, bucket: Bucket) -> tuple[list[tuple], list[tuple]]:
    """The library's own merge of the hub's books, with serve's fee cache."""
    def merge() -> tuple[list[tuple], list[tuple]]:
        hub = harness.stack.market_streams
        views = {m.market_id: router.view(hub.member_book(m.market_id), m) for m in bucket.members}
        fees = hub.buckets.fees
        asks, _ = router.merge(bucket, views, fees.fee_function(bucket, Side.BUY))
        _, bids = router.merge(bucket, views, fees.fee_function(bucket, Side.SELL))
        row = lambda l: (str(l.price), str(l.net_price), str(l.size), l.market_id)  # noqa: E731
        return [row(l) for l in asks], [row(l) for l in bids]
    return harness.call(merge)


def rows(message: dict[str, Any], side: str) -> list[tuple]:
    return [(r["price"], r["net_price"], r["size"], r["market_id"]) for r in message[side]]


def book_where(socket: Any, test: Any, limit: int = 20) -> dict[str, Any]:
    """The next `bucket_book` frame that passes `test`, skipping older ones."""
    for _ in range(limit):
        message = receive_until(socket, "bucket_book")
        if test(message):
            return message
    raise AssertionError("no matching bucket_book frame")


def feed_both_members(harness) -> None:
    kalshi, poly = harness.stream("kalshi"), harness.stream("polymarket")
    harness.push("kalshi", kalshi.snapshot(K, bids=[("0.40", "10")], asks=[("0.45", "5"), ("0.47", "8")]))
    harness.push("polymarket", poly.snapshot(P, bids=[("0.52", "6"), ("0.50", "9")], asks=[("0.58", "4")]))


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

class TestBucketDefinitions:
    def test_create_list_get_rename_edit_archive_and_restore(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            client, auth = harness.client, harness.auth
            bucket = create(harness)
            assert len(bucket["id"]) == 32 and bucket["book"] == "terminal"
            create(harness, book="other")
            listed = client.get("/trading/buckets?book=terminal", headers=auth).json()
            assert [b["id"] for b in listed["data"]] == [bucket["id"]], "listed by book"
            assert client.get(f"/trading/buckets/bucket:{bucket['id']}", headers=auth).json()["name"] == "Same question"

            renamed = client.patch(f"/trading/buckets/{bucket['id']}", json={"name": "Renamed"}, headers=auth)
            assert renamed.status_code == 200, renamed.text
            assert renamed.json()["name"] == "Renamed" and renamed.json()["members"] == bucket["members"]
            edited = client.patch(f"/trading/buckets/{bucket['id']}", headers=auth, json={
                "members": [{"market_id": K, "flip": True}, {"market_id": P}, {"market_id": P2, "flip": True}]})
            assert [(m["market_id"], m["flip"]) for m in edited.json()["members"]] == [(K, True), (P, False), (P2, True)]
            assert edited.json()["created_ms"] == bucket["created_ms"]

            archived = client.delete(f"/trading/buckets/{bucket['id']}", headers=auth).json()
            assert archived["status"] == "archived"
            assert client.get("/trading/buckets?book=terminal", headers=auth).json()["count"] == 0
            restored = client.patch(f"/trading/buckets/{bucket['id']}", json={"status": "active"}, headers=auth)
            assert restored.json()["status"] == "active" and restored.json()["name"] == "Renamed"

    def test_a_client_made_id_is_kept_and_taken_twice_is_409(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            dashed = str(uuid.uuid4())
            made = create(harness, id=dashed.upper())
            assert made["id"] == dashed, "kept as given, in lower case"
            again = harness.client.post("/trading/buckets", json=definition(id=dashed), headers=harness.auth)
            assert again.status_code == 409
            bad = harness.client.post("/trading/buckets", json=definition(id="not-a-uuid"), headers=harness.auth)
            assert bad.status_code == 400

    def test_edits_pass_the_same_checks_as_a_new_definition(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = create(harness)
            one = harness.client.patch(f"/trading/buckets/{bucket['id']}", json={"members": [{"market_id": K}]},
                                       headers=harness.auth)
            assert one.status_code == 400 and "two members" in one.text
            untraded = harness.client.patch(f"/trading/buckets/{bucket['id']}", headers=harness.auth, json={
                "members": [{"market_id": K}, {"market_id": "polymarket_us:abc"}]})
            assert untraded.status_code == 400 and "polymarket_us" in untraded.text
            assert harness.client.patch("/trading/buckets/nope", json={"name": "x"}, headers=harness.auth).status_code == 404

    def test_a_member_switched_off_may_sit_on_a_venue_this_server_only_reads(self, serve_harness):
        # Watched for its prices: never routed to, so it needs no credentials here.
        with bucket_harness(serve_harness) as harness:
            bucket = create(harness)
            watched = {"market_id": "predict_fun:1052063", "enabled": False}
            kept = harness.client.patch(f"/trading/buckets/{bucket['id']}", headers=harness.auth, json={
                "members": [{"market_id": K}, {"market_id": P, "flip": True}, watched]})
            assert kept.status_code == 200, kept.text
            assert [m["market_id"] for m in kept.json()["members"]][-1] == "predict_fun:1052063"
            switched_on = harness.client.patch(f"/trading/buckets/{bucket['id']}", headers=harness.auth, json={
                "members": [{"market_id": K}, {"market_id": P, "flip": True}, {**watched, "enabled": True}]})
            assert switched_on.status_code == 400 and "predict_fun" in switched_on.text

    def test_every_bucket_route_needs_the_token(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = create(harness)
            path = f"/trading/buckets/{bucket['id']}"
            assert harness.client.post("/trading/buckets", json=definition()).status_code == 401
            assert harness.client.get("/trading/buckets").status_code == 401
            assert harness.client.get(path).status_code == 401
            assert harness.client.patch(path, json={"name": "x"}).status_code == 401
            assert harness.client.delete(path).status_code == 401
            plan = {"side": "buy", "quantity": "5", "worst_price": "0.6"}
            assert harness.client.post(f"{path}/plan", json=plan).status_code == 401
            assert harness.client.post(f"{path}/plan", json=plan, headers={"Authorization": "Bearer wrong"}).status_code == 401


# ---------------------------------------------------------------------------
# The merged book
# ---------------------------------------------------------------------------

class TestMergedBook:
    def test_the_merged_book_is_the_routers_in_bucket_terms_net_of_fees(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            raw = create(harness)
            bucket = stored_bucket(raw)
            with harness.client.websocket_connect(market_url(harness, bucket.market_id, channels="book,trades,quotes")) as socket:
                subscribed = receive_until(socket, "subscribed")
                assert subscribed["venue"] == "bucket" and subscribed["channels"] == ["book", "trades"]
                assert subscribed["unsupported"] == ["quotes"] and subscribed["bucket"]["id"] == bucket.id
                waiting = receive_until(socket, "bucket_book")
                assert waiting["ready"] is False and waiting["asks"] == []

                feed_both_members(harness)
                book = receive_until(socket, "bucket_book", ready=True)
                expected_asks, expected_bids = expected_levels(harness, bucket)
                assert rows(book, "asks") == expected_asks and rows(book, "bids") == expected_bids
                # The flipped Polymarket member is read through its NO side:
                # its YES bid at 0.52 is a bucket ask at 0.48.
                assert ("0.48", P) in [(r["price"], r["market_id"]) for r in book["asks"]]
                assert {r["venue"] for r in book["asks"]} == {"kalshi", "polymarket"}
                # Net of fees: Kalshi's quadratic fee at 0.45 is $1.74 per 100 contracts (rounded up to the cent).
                kalshi_ask = next(r for r in book["asks"] if r["market_id"] == K and r["price"] == "0.45")
                assert D(kalshi_ask["net_price"]) == D("0.4674")
                assert book["best_ask"] == "0.45" and book["best_bid"] == "0.42"
                assert book["fees_unknown"] == [] and all(m["ready"] for m in book["members"])

                harness.push("kalshi", harness.stream("kalshi").delta(K, "ask", "0.41", "3"))
                updated = receive_until(socket, "bucket_book", ready=True, best_ask="0.41")
                assert rows(updated, "asks") == expected_levels(harness, bucket)[0]

    def test_a_member_dropping_makes_the_bucket_not_ready_until_it_is_back(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            with harness.client.websocket_connect(market_url(harness, bucket.market_id)) as socket:
                receive_until(socket, "subscribed")
                feed_both_members(harness)
                receive_until(socket, "bucket_book", ready=True)
                poly = harness.stream("polymarket")
                harness.push("polymarket", poly.drop())
                assert receive_until(socket, "status")["state"] == "disconnected"
                dropped = receive_until(socket, "bucket_book")
                assert dropped["ready"] is False and dropped["asks"] == [] and dropped["bids"] == []
                assert [m["ready"] for m in dropped["members"]] == [True, False]
                harness.push("polymarket", poly.reconnected())
                harness.push("polymarket", poly.snapshot(P, bids=[("0.52", "6")], asks=[("0.58", "4")]))
                assert receive_until(socket, "bucket_book", ready=True)["ready"]

    def test_member_trades_arrive_in_bucket_terms(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            with harness.client.websocket_connect(market_url(harness, bucket.market_id, channels="book,trades")) as socket:
                receive_until(socket, "subscribed")
                harness.wait_until(lambda: "polymarket" in harness.opened)
                harness.push("polymarket", harness.stream("polymarket").trade(P, "0.42", "4"))
                trade = receive_until(socket, "trade")
                assert trade["market_id"] == bucket.market_id and trade["member_market_id"] == P
                assert trade["price"] == "0.58" and trade["flip"] is True and trade["quantity"] == "4"

    def test_a_bucket_shares_the_venue_connections_and_releases_them(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            with harness.client.websocket_connect(market_url(harness, bucket.market_id, P)) as socket:
                receive_until(socket, "subscribed", market_id=bucket.market_id)
                receive_until(socket, "subscribed", market_id=P)
                assert len(harness.opened["polymarket"]) == 1, "one Polymarket connection for both"
                stream = harness.stream("polymarket")
                assert stream.watched == [("book", (P,))], "subscribed upstream once"
                socket.send_json({"op": "unsubscribe", "market_id": bucket.market_id})
                receive_until(socket, "unsubscribed")
                assert stream.unwatched == [], "the direct subscription still holds the market"
            harness.wait_until(lambda: "polymarket" not in harness.stack.market_streams.connections)
            assert stream.closed and harness.stack.market_streams.bucket_subscriptions == {}

    def test_an_edit_moves_a_streaming_client_to_the_new_definition(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            with harness.client.websocket_connect(market_url(harness, bucket.market_id)) as socket:
                receive_until(socket, "subscribed")
                feed_both_members(harness)
                receive_until(socket, "bucket_book", ready=True)
                harness.client.patch(f"/trading/buckets/{bucket.id}", headers=harness.auth, json={
                    "name": "Edited", "members": [{"market_id": K}, {"market_id": P2, "flip": True}]})
                edited = receive_until(socket, "bucket_book", name="Edited")
                assert [m["market_id"] for m in edited["members"]] == [K, P2] and edited["ready"] is False
                stream = harness.stream("polymarket")
                harness.wait_until(lambda: P in stream.unwatched)
                harness.push("polymarket", stream.snapshot(P2, bids=[("0.55", "2")], asks=[("0.60", "2")]))
                ready = receive_until(socket, "bucket_book", ready=True)
                assert {r["market_id"] for r in ready["asks"]} == {K, P2}

    def test_unknown_hidden_and_badly_asked_buckets_are_refused_per_subscription(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            viewer_key = harness.call(_kalshi_only_key, harness)
            with harness.client.websocket_connect(f"/ws/market?key={harness.token}") as socket:
                socket.send_json({"op": "subscribe", "market_id": "bucket:nope"})
                assert receive_until(socket, "error")["code"] == "bucket_not_found"
                socket.send_json({"op": "subscribe", "market_id": bucket.market_id, "side": "no"})
                assert receive_until(socket, "error")["code"] == "bad_request"
            with harness.client.websocket_connect(f"/ws/market?key={viewer_key}") as socket:
                socket.send_json({"op": "subscribe", "market_id": bucket.market_id})
                assert receive_until(socket, "error")["code"] == "forbidden"
            with harness.client.websocket_connect(f"/ws/market?market_id={bucket.market_id}") as socket:
                assert socket.receive()["code"] == 4401

    def test_a_member_switched_off_is_not_streamed_merged_or_waited_for_and_patch_toggles_it(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            with harness.client.websocket_connect(market_url(harness, bucket.market_id)) as socket:
                receive_until(socket, "subscribed")
                feed_both_members(harness)
                receive_until(socket, "bucket_book", ready=True)
                stream = harness.stream("polymarket")

                off = harness.client.patch(f"/trading/buckets/{bucket.id}", headers=harness.auth, json={
                    "members": [{"market_id": K}, {"market_id": P, "flip": True, "enabled": False}]})
                assert off.status_code == 200, off.text
                assert [m["enabled"] for m in off.json()["members"]] == [True, False]
                harness.wait_until(lambda: P in stream.unwatched)
                only_k = book_where(socket, lambda m: m["ready"] and [x["enabled"] for x in m["members"]] == [True, False])
                assert {r["market_id"] for r in only_k["asks"] + only_k["bids"]} == {K}
                assert [(m["market_id"], m["enabled"]) for m in only_k["members"]] == [(K, True), (P, False)]
                routable = stored_bucket(off.json()).routable()
                assert rows(only_k, "asks") == expected_levels(harness, routable)[0]

                plan = harness.client.post(f"/trading/buckets/{bucket.id}/plan", headers=harness.auth, json={
                    "side": "buy", "quantity": "10", "worst_price": "0.70"}).json()
                assert {l["market_id"] for l in plan["legs"]} == {K} and plan["books"] == {K: "stream"}

                on = harness.client.patch(f"/trading/buckets/{bucket.id}", headers=harness.auth, json={
                    "members": [{"market_id": K}, {"market_id": P, "flip": True}]})
                assert [m["enabled"] for m in on.json()["members"]] == [True, True]
                # Switched back on: subscribed again (on a fresh connection, if the
                # old one closed once nothing on it was wanted).
                harness.wait_until(lambda: harness.stream("polymarket").watched.count(("book", (P,)))
                                   == (2 if harness.stream("polymarket") is stream else 1))
                stream = harness.stream("polymarket")
                book_where(socket, lambda m: not m["ready"] and [x["enabled"] for x in m["members"]] == [True, True])
                harness.push("polymarket", stream.snapshot(P, bids=[("0.52", "6")], asks=[("0.58", "4")]))
                both = book_where(socket, lambda m: m["ready"] and [x["enabled"] for x in m["members"]] == [True, True])
                assert {r["market_id"] for r in both["asks"]} == {K, P}

    def test_every_member_switched_off_is_refused(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = create(harness)
            none_on = harness.client.patch(f"/trading/buckets/{bucket['id']}", headers=harness.auth, json={
                "members": [{"market_id": K, "enabled": False}, {"market_id": P, "enabled": False}]})
            assert none_on.status_code == 400 and "enabled" in none_on.text
            made = harness.client.post("/trading/buckets", headers=harness.auth, json=definition(members=[
                {"market_id": K, "enabled": False}, {"market_id": P, "enabled": False}]))
            assert made.status_code == 400
            assert all(m["enabled"] for m in bucket["members"]), "members default to enabled"


async def _kalshi_only_key(harness) -> str:
    store = harness.stack.store
    user = await store.create_user("kalshi-viewer")
    await store.grant(user.id, harness.stack.engine.accounts["kalshi"].key, "view")
    return (await store.issue_key(user.id)).secret


# ---------------------------------------------------------------------------
# The router preview
# ---------------------------------------------------------------------------

def library_plan(harness, bucket: Bucket, side: Side, quantity: str, limit: str) -> router.Plan:
    """`router.plan` called directly on the hub's books with serve's fees."""
    def make() -> router.Plan:
        hub = harness.stack.market_streams
        fees = hub.buckets.fees
        books = {m.market_id: hub.member_book(m.market_id) for m in bucket.members}
        precision = {m.market_id: router.default_precision(m.market_id) for m in bucket.members}
        return router.plan(bucket, books, precision, fees.fee_function(bucket, side), side=side,
                           quantity=D(quantity), limit=D(limit), floors=fees.floors(bucket))
    return harness.call(make)


class TestPlanPreview:
    def test_the_preview_is_the_library_plan_on_the_live_books(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            with harness.client.websocket_connect(market_url(harness, bucket.market_id)) as socket:
                receive_until(socket, "subscribed")
                feed_both_members(harness)
                receive_until(socket, "bucket_book", ready=True)
                for side, quantity, worst in ((Side.BUY, "12", "0.50"), (Side.SELL, "10", "0.30"),
                                              (Side.BUY, "100", "0.55")):
                    answer = harness.client.post(f"/trading/buckets/{bucket.id}/plan", headers=harness.auth, json={
                        "side": side.value, "quantity": quantity, "worst_price": worst})
                    assert answer.status_code == 200, answer.text
                    preview = answer.json()
                    expected = library_plan(harness, bucket, side, quantity, worst)
                    assert [(l["market_id"], l["side"], D(l["price"]), D(l["quantity"]), D(l["net_price"]), D(l["fee"]))
                            for l in preview["legs"]] == \
                           [(l.market_id, l.side.value, l.price, l.quantity, l.net_price, l.fee) for l in expected.legs]
                    assert D(preview["unfilled"]) == expected.unfilled
                    assert preview["stop_reason"] == (expected.reason or None)
                    assert D(preview["allocated"]) == expected.allocated <= D(quantity)
                    assert preview["books"] == {K: "stream", P: "stream"}
                big = harness.client.post(f"/trading/buckets/{bucket.id}/plan", headers=harness.auth, json={
                    "side": "buy", "quantity": "100", "worst_price": "0.55"}).json()
                assert big["stop_reason"] in ("worst_price", "liquidity") and D(big["unfilled"]) > 0
                flipped_leg = next(l for l in big["legs"] if l["market_id"] == P)
                assert flipped_leg["flip"] and flipped_leg["side"] == "sell", "buying a flipped member's NO is a sell"
            assert harness.stack.engine.open_orders() == [], "a preview places nothing"

    def test_without_a_stream_the_preview_reads_the_books_from_the_venues(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            answer = harness.client.post(f"/trading/buckets/bucket:{bucket.id}/plan", headers=harness.auth,
                                         json={"side": "buy", "quantity": "3", "worst_price": "0.70"})
            assert answer.status_code == 200, answer.text
            preview = answer.json()
            assert preview["books"] == {K: "rest", P: "rest"}
            assert sum(D(l["quantity"]) for l in preview["legs"]) == D("3") and preview["stop_reason"] is None

    def test_a_bad_preview_request_is_refused(self, serve_harness):
        with bucket_harness(serve_harness) as harness:
            bucket = stored_bucket(create(harness))
            path = f"/trading/buckets/{bucket.id}/plan"
            assert harness.client.post(path, headers=harness.auth, json={
                "side": "buy", "quantity": "0", "worst_price": "0.5"}).status_code in (400, 422)
            assert harness.client.post(path, headers=harness.auth, json={
                "side": "buy", "quantity": "1", "worst_price": "1.5"}).status_code in (400, 422)
            assert harness.client.post("/trading/buckets/nope/plan", headers=harness.auth, json={
                "side": "buy", "quantity": "1", "worst_price": "0.5"}).status_code == 404
