"""`synpath serve` matching and tick history: serve calls Synpath's hosted services with its own key."""
from __future__ import annotations

import pytest

from serve_harness import MockHosted, serve_harness  # noqa: F401  (the fixture)

META = {"dataset_version": "processed-v1-test"}
MATCH = {"anchor": "polymarket:1", "event_id": "ev-1",
         "matched": {"id": "kalshi:KX-A", "venue": "kalshi", "side_map": {"yes": "no", "no": "yes"}}}
BOOK_AT = {"metadata": META, "market_id": "kalshi:KX-A", "as_of_ms": 5, "book": None, "absence_reason": "no coverage"}


@pytest.fixture(autouse=True)
def serve_key(monkeypatch, tmp_path):
    monkeypatch.setenv("SYNPATH_API_KEY", "spk_serve")
    monkeypatch.delenv("SYNPATH_MATCHING_URL", raising=False)
    monkeypatch.delenv("SYNPATH_HISTORY_URL", raising=False)
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(tmp_path / "no-credentials.json"))


def test_matching_goes_through_serve_with_serves_key(serve_harness):
    hosted = MockHosted({("GET", "/match/market"): MATCH,
                         ("GET", "/match/event"): {"anchor": "polymarket:1", "event_ids": ["ev-1"],
                                                   "events": {"kalshi": ["KX"], "opinion": None}},
                         ("GET", "/arbitrage"): {"computed_at": "2026-10-08T10:00:00+00:00", "results": []}})
    with serve_harness(hosted=hosted) as harness:
        matched = harness.client.get("/matching/market", params={"id": "polymarket:1", "venue": "kalshi"},
                                     headers=harness.auth)
        assert matched.status_code == 200, matched.text
        assert matched.json()["matched"]["side_map"] == {"yes": "no", "no": "yes"}
        event = harness.client.get("/matching/event", params={"id": "polymarket:1"}, headers=harness.auth).json()
        assert event["events"] == {"kalshi": ["KX"], "opinion": None}
        scan = harness.client.get("/matching/arbitrage", headers=harness.auth).json()
        assert scan["computed_at"].startswith("2026-10-08")
        first = hosted.requests[0]
        assert str(first.url) == "https://api.synpath.dev/match/market?id=polymarket%3A1&venue=kalshi"
        assert {r.headers["authorization"] for r in hosted.requests} == {"Bearer spk_serve"}
        assert all("spk_serve" not in str(r.url) for r in hosted.requests)


def test_history_routes_reach_the_history_service(serve_harness):
    trades = {"metadata": META, "market_id": "kalshi:KX-A", "start_ms": 1, "end_ms": 9, "trades": [], "coverage": []}
    book_range = {"metadata": META, "market_id": "kalshi:KX-A", "start_ms": 1, "end_ms": 9, "segments": []}
    hosted = MockHosted({("POST", "/v1/order-book/at"): BOOK_AT, ("POST", "/v1/order-book/range"): book_range,
                         ("POST", "/v1/trades/range"): trades})
    with serve_harness(hosted=hosted) as harness:
        at = harness.client.get("/history/order-book/at", params={"market_id": "kalshi:KX-A", "as_of_ms": 5,
                                                                  "depth": 3}, headers=harness.auth)
        assert at.status_code == 200, at.text
        assert at.json()["absence_reason"] == "no coverage"
        assert harness.client.get("/history/order-book/range", params={"market_id": "kalshi:KX-A", "start_ms": 1,
                                  "end_ms": 9}, headers=harness.auth).status_code == 200
        assert harness.client.get("/history/trades/range", params={"market_id": "kalshi:KX-A", "start_ms": 1,
                                  "end_ms": 9, "limit": 10}, headers=harness.auth).status_code == 200
        assert [str(r.url) for r in hosted.requests][0] == "https://api2.synpath.dev/v1/order-book/at"
        assert hosted.requests[0].content == b'{"market_id":"kalshi:KX-A","as_of_ms":5,"depth":3}'
        backwards = harness.client.get("/history/trades/range", params={"market_id": "kalshi:KX-A", "start_ms": 9,
                                       "end_ms": 1}, headers=harness.auth)
        assert backwards.status_code == 400 and len(hosted.requests) == 3


@pytest.mark.parametrize("answer, status, code", [
    ((401, {"error": "A valid API key is required"}), 502, "hosted_unauthorized"),
    ((413, {"detail": "result_too_large"}), 413, "result_too_large"),
    ((404, {"detail": "unknown market"}), 404, "market_not_found"),
    (None, 502, "hosted_unavailable"),   # the host in front answers 404 with no Synpath body
])
def test_hosted_failures_get_their_own_codes(serve_harness, answer, status, code):
    routes = {("POST", "/v1/order-book/at"): answer} if answer else {}
    with serve_harness(hosted=MockHosted(routes)) as harness:
        response = harness.client.get("/history/order-book/at", params={"market_id": "kalshi:KX-A", "as_of_ms": 5},
                                      headers=harness.auth)
        assert response.status_code == status, response.text
        error = response.json()["error"]
        assert error["code"] == code and error["details"]["venue"] == "synpath"


def test_hosted_routes_need_serves_token_and_a_synpath_id(serve_harness):
    hosted = MockHosted({("GET", "/match/market"): MATCH})
    with serve_harness(hosted=hosted) as harness:
        for route in ("/matching/market?id=polymarket:1", "/matching/event?id=polymarket:1", "/matching/arbitrage",
                      "/history/order-book/at?market_id=kalshi:A&as_of_ms=1",
                      "/history/order-book/range?market_id=kalshi:A&start_ms=1&end_ms=2",
                      "/history/trades/range?market_id=kalshi:A&start_ms=1&end_ms=2"):
            assert harness.client.get(route).status_code == 401, route
        bad = harness.client.get("/matching/market", params={"id": "KX-A"}, headers=harness.auth)
        assert bad.status_code == 400 and bad.json()["error"]["code"] == "bad_request"
        assert not hosted.requests
