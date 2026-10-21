"""`synpath serve` batch reads and search: many ids or venues in one call, failures reported per id or venue."""
from __future__ import annotations

from typing import Any

from synpath.errors import ExchangeNotAvailable, NotSupported
from synpath.types import Event, Market, OrderBook, OrderLevel, Outcome, Page, Quote

from serve_harness import serve_harness  # noqa: F401  (the fixture)


def market(venue: str, native: str, bid: float = 0.3) -> Market:
    return Market(id=f"{venue}:{native}", venue=venue, venue_market_id=native, title=f"{venue} {native}",
                  yes=Outcome(label="Yes", quote=Quote(bid=bid, ask=bid + 0.05)),
                  no=Outcome(label="No"), status="open", active=True)


class StubPolymarket:
    """Polymarket's read adapter, answering from memory and recording every call."""

    id = "polymarket"

    def __init__(self, known: tuple[str, ...] = ("1", "2")):
        self.known = known
        self.calls: list[tuple[str, Any]] = []

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        self.calls.append(("fetch_markets_by_ids", list(market_ids)))
        return [market("polymarket", m.split(":")[1]) for m in market_ids if m.split(":")[1] in self.known]

    def fetch_order_books(self, market_ids: list[str], *, side: str = "yes", depth: int | None = None):
        self.calls.append(("fetch_order_books", (list(market_ids), side, depth)))
        return {m: OrderBook(market_id=m, side=side, venue="polymarket", bids=[OrderLevel(price=0.3, size=5)])
                for m in market_ids if m.split(":")[1] in self.known}

    def fetch_markets(self, *, query=None, limit=None, cursor=None, status="open", sort=None):
        self.calls.append(("fetch_markets", (query, limit, cursor, status)))
        return Page([market("polymarket", "1")], next_cursor="p2")

    def fetch_events(self, *, query=None, limit=None, cursor=None, status="open"):
        self.calls.append(("fetch_events", (query, limit, cursor, status)))
        return Page([Event(id="polymarket:e1", venue="polymarket", venue_event_id="e1", title="E1", status="open",
                           markets=[market("polymarket", "1")])], next_cursor=None)


class DownVenue:
    """A venue that fails every call, as one in maintenance does."""

    id = "opinion"

    def __getattr__(self, name: str):
        def fail(*args: Any, **kwargs: Any) -> Any:
            raise ExchangeNotAvailable("opinion: down for maintenance")
        return fail


class NoBatchBooks(StubPolymarket):
    id = "limitless"

    def fetch_order_books(self, *args: Any, **kwargs: Any):
        raise NotSupported("limitless: fetch_order_books")

    def fetch_order_book(self, market_id: str, *, side: str = "yes", depth: int | None = None):
        return OrderBook(market_id=market_id, side=side, venue="limitless")


def harness_with_stubs(serve_harness, **stubs):
    return serve_harness(exchanges={"polymarket": StubPolymarket(), "opinion": DownVenue(), **stubs})


def test_batch_markets_answer_every_id_in_data_or_errors(serve_harness):
    with harness_with_stubs(serve_harness) as harness:
        ids = "polymarket:2, kalshi:KXHARNESS-1,polymarket:1,polymarket:2,kalshi:GONE,opinion:9,nope,mars:1"
        response = harness.client.get("/markets", params={"ids": ids}, headers=harness.auth)
        assert response.status_code == 200, response.text
        body = response.json()
        assert [m["id"] for m in body["data"]] == ["polymarket:2", "kalshi:KXHARNESS-1", "polymarket:1"]
        assert body["data"][1]["yes"]["quote"]["bid"] == 0.4, "the quote travels with the market"
        errors = {market_id: error["code"] for market_id, error in body["errors"].items()}
        assert errors == {"kalshi:GONE": "market_not_found", "opinion:9": "exchange_not_available",
                          "nope": "bad_market_id", "mars:1": "bad_market_id"}
        assert body["errors"]["opinion:9"]["details"] == {"venue": "opinion", "retryable": True}
        polymarket = harness.exchanges["polymarket"]
        assert polymarket.calls == [("fetch_markets_by_ids", ["polymarket:2", "polymarket:1"])], "one call per venue"


def test_batch_reads_are_capped_and_need_the_token(serve_harness):
    with harness_with_stubs(serve_harness) as harness:
        too_many = ",".join(f"polymarket:{i}" for i in range(101))
        refused = harness.client.get("/markets", params={"ids": too_many}, headers=harness.auth)
        assert refused.status_code == 400 and refused.json()["error"]["code"] == "too_many_ids"
        assert harness.client.get("/markets", params={"ids": " , "}, headers=harness.auth).status_code == 400
        for route in ("/markets?ids=polymarket:1", "/books?ids=polymarket:1", "/search/markets?query=x",
                      "/search/events?query=x"):
            assert harness.client.get(route).status_code == 401, route
        assert not harness.exchanges["polymarket"].calls


def test_batch_books_by_id_with_a_fallback_for_venues_without_a_batch(serve_harness):
    with harness_with_stubs(serve_harness, limitless=NoBatchBooks()) as harness:
        response = harness.client.get(
            "/books", params={"ids": "polymarket:1,polymarket:7,kalshi:KXHARNESS-1,limitless:a,opinion:3",
                              "side": "no", "depth": 5}, headers=harness.auth)
        assert response.status_code == 200, response.text
        body = response.json()
        assert sorted(body["data"]) == ["kalshi:KXHARNESS-1", "limitless:a", "polymarket:1"]
        assert body["data"]["kalshi:KXHARNESS-1"]["side"] == "no" and body["data"]["kalshi:KXHARNESS-1"]["derived"]
        assert harness.exchanges["polymarket"].calls == [("fetch_order_books", (["polymarket:1", "polymarket:7"], "no", 5))]
        assert {k: v["code"] for k, v in body["errors"].items()} == {
            "polymarket:7": "market_not_found", "opinion:3": "exchange_not_available"}


def test_search_asks_each_venue_and_reports_failures_per_venue(serve_harness):
    with harness_with_stubs(serve_harness) as harness:
        response = harness.client.get("/search/markets", params=[("query", "btc"), ("venue", "polymarket"),
                                                                 ("venue", "opinion"), ("limit", "5")],
                                      headers=harness.auth)
        assert response.status_code == 200, response.text
        body = response.json()
        assert list(body["results"]) == ["polymarket"]
        assert body["results"]["polymarket"]["next_cursor"] == "p2"
        assert body["results"]["polymarket"]["data"][0]["id"] == "polymarket:1"
        assert body["errors"]["opinion"]["code"] == "exchange_not_available"
        assert ("fetch_markets", ("btc", 5, None, "open")) in harness.exchanges["polymarket"].calls

        events = harness.client.get("/search/events", params={"query": "btc", "venue": "polymarket", "cursor": "c"},
                                    headers=harness.auth).json()
        assert events["results"]["polymarket"]["data"][0]["markets"][0]["id"] == "polymarket:1"
        assert ("fetch_events", ("btc", 20, "c", "open")) in harness.exchanges["polymarket"].calls


def test_search_refuses_a_cursor_across_venues_and_unknown_venues(serve_harness):
    with harness_with_stubs(serve_harness) as harness:
        across = harness.client.get("/search/markets", params=[("query", "x"), ("cursor", "c"),
                                                               ("venue", "polymarket"), ("venue", "kalshi")],
                                    headers=harness.auth)
        assert across.status_code == 400
        unknown = harness.client.get("/search/markets", params={"query": "x", "venue": "mars"}, headers=harness.auth)
        assert unknown.status_code == 404 and unknown.json()["error"]["code"] == "unknown_venue"
        assert harness.client.get("/search/markets", params={"query": ""}, headers=harness.auth).status_code == 422


def test_candles_take_a_source_only_where_the_venue_has_one(serve_harness):
    with serve_harness() as harness:
        refused = harness.client.get("/venues/kalshi/markets/KXHARNESS-1/candles", params={"source": "quotes"},
                                     headers=harness.auth)
        assert refused.status_code == 400 and refused.json()["error"]["details"]["field"] == "source"
