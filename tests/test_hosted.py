"""Synpath's hosted services: default addresses, the API key as a bearer, and plain errors."""
from __future__ import annotations

import json

import httpx
import pytest

import synpath
from synpath.errors import AuthenticationError, BadRequest

BOOK = {"market_id": "kalshi:KX-A", "side": "yes", "venue": "kalshi", "bids": [{"price": 0.33, "size": 66.0}],
        "asks": [{"price": 0.36, "size": 10.0}], "timestamp": 1789509500000, "as_of_ms": 1789509599000}
META = {"dataset_version": "processed-v1-test"}


def client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def answering(payload, seen: list, status: int = 200):
    def handler(request):
        seen.append(request)
        return httpx.Response(status, json=payload)
    return client(handler)


@pytest.fixture(autouse=True)
def no_key(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNPATH_API_KEY", raising=False)
    monkeypatch.delenv("SYNPATH_HISTORY_URL", raising=False)
    # No key saved by `synpath login` either: the tests must not read this machine's.
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(tmp_path / "no-credentials.json"))


def test_the_key_goes_to_both_synpath_services_and_only_when_set(monkeypatch):
    seen: list = []
    match = {"anchor": "kalshi:KX-A", "event_id": None, "matched": None}
    at = {"metadata": META, "market_id": "kalshi:KX-A", "as_of_ms": 1, "book": None, "absence_reason": "x"}
    synpath.match_market("kalshi:KX-A", client=answering(match, seen))
    assert "authorization" not in seen[0].headers, "no key set, no header"
    monkeypatch.setenv("SYNPATH_API_KEY", "spk_from_env")
    synpath.match_market("kalshi:KX-A", client=answering(match, seen))
    synpath.fetch_order_book_at("kalshi:KX-A", as_of_ms=1, client=answering(at, seen))
    assert str(seen[1].url).startswith("https://api.synpath.dev/")
    assert str(seen[2].url).startswith("https://api2.synpath.dev/")
    assert seen[1].headers["authorization"] == seen[2].headers["authorization"] == "Bearer spk_from_env"
    synpath.match_event("kalshi:KX-A", api_key="spk_explicit",
                        client=answering({"anchor": "kalshi:KX-A", "event_ids": [], "events": {}}, seen))
    assert seen[3].headers["authorization"] == "Bearer spk_explicit", "an explicit key wins over the variable"


def test_the_key_saved_by_synpath_login_reaches_both_services(tmp_path, monkeypatch):
    """Regression: matching sent no key unless SYNPATH_API_KEY was set, so the
    key `synpath keys create` saved worked for history but not for matching."""
    from synpath.hosted_auth import save_credentials

    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(tmp_path / "saved" / "credentials.json"))
    save_credentials({"api_key": "spk_saved.value"})
    seen: list = []
    synpath.match_market("kalshi:KX-A", client=answering({"anchor": "kalshi:KX-A", "event_id": None, "matched": None}, seen))
    synpath.match_event("kalshi:KX-A", client=answering({"anchor": "kalshi:KX-A", "event_ids": [], "events": {}}, seen))
    assert [r.headers["authorization"] for r in seen] == ["Bearer spk_saved.value"] * 2


def test_a_refused_key_on_matching_says_what_to_set():
    with pytest.raises(AuthenticationError, match="SYNPATH_API_KEY"):
        synpath.match_market("kalshi:KX-A", client=answering({"error": "A valid API key is required"}, [], status=401))


def test_order_book_at_posts_to_the_history_service_and_parses(monkeypatch):
    monkeypatch.setenv("SYNPATH_API_KEY", "spk_k")
    seen: list = []
    reply = {"metadata": META, "market_id": "kalshi:KX-A", "as_of_ms": 1789509599000, "book": BOOK}
    at = synpath.fetch_order_book_at("kalshi:KX-A", as_of_ms=1789509599000, depth=5, client=answering(reply, seen))
    request = seen[0]
    assert str(request.url) == "https://api2.synpath.dev/v1/order-book/at" and request.method == "POST"
    assert json.loads(request.content) == {"market_id": "kalshi:KX-A", "as_of_ms": 1789509599000, "depth": 5}
    assert request.headers["authorization"] == "Bearer spk_k"
    assert at.book is not None and at.book.best_bid.price == 0.33


def test_a_missing_book_is_an_absence_not_an_error():
    reply = {"metadata": META, "market_id": "kalshi:KX-A", "as_of_ms": 1, "book": None,
             "absence_reason": "outside_loaded_data"}
    at = synpath.fetch_order_book_at("kalshi:KX-A", as_of_ms=1, client=answering(reply, []))
    assert at.book is None and at.absence_reason == "outside_loaded_data"


def test_ranges_and_the_url_override(monkeypatch):
    monkeypatch.setenv("SYNPATH_HISTORY_URL", "http://history.test")
    seen: list = []
    trades = synpath.fetch_trades_range("kalshi:KX-A", 0, 10, limit=100, client=answering(
        {"metadata": META, "market_id": "kalshi:KX-A", "start_ms": 0, "end_ms": 10, "trades": [], "coverage": []}, seen))
    books = synpath.fetch_order_book_range("kalshi:KX-A", 0, 10, side="no", client=answering(
        {"metadata": META, "market_id": "kalshi:KX-A", "start_ms": 0, "end_ms": 10, "segments": []}, seen))
    assert [str(r.url) for r in seen] == ["http://history.test/v1/trades/range", "http://history.test/v1/order-book/range"]
    assert json.loads(seen[1].content) == {"market_id": "kalshi:KX-A", "start_ms": 0, "end_ms": 10, "side": "no"}
    assert trades.trades == [] and books.segments == []


def test_a_refused_key_says_what_to_set_and_a_huge_range_says_to_narrow_it():
    with pytest.raises(AuthenticationError, match="SYNPATH_API_KEY"):
        synpath.fetch_order_book_at("kalshi:KX-A", as_of_ms=1,
                                    client=answering({"error": "A valid API key is required"}, [], status=401))
    with pytest.raises(BadRequest, match="narrow the range"):
        synpath.fetch_order_book_range("kalshi:KX-A", 0, 10**12,
                                       client=answering({"error": "result_too_large"}, [], status=413))


def test_the_market_id_is_checked_before_any_request():
    with pytest.raises(BadRequest):
        synpath.fetch_order_book_at("KX-A", as_of_ms=1, client=answering({}, []))


def test_a_host_with_no_application_reads_as_the_service_being_down():
    """Railway answers 404 for an app that is not running; that is not 'market not found'."""
    from synpath.errors import ExchangeNotAvailable, MarketNotFound

    railway = {"status": "error", "code": 404, "message": "Application not found", "request_id": "x"}
    with pytest.raises(ExchangeNotAvailable, match="not running"):
        synpath.match_market("kalshi:KX-A", client=answering(railway, [], status=404))
    with pytest.raises(MarketNotFound):
        synpath.match_market("kalshi:KX-A", client=answering({"detail": "market not found"}, [], status=404))
    with pytest.raises(MarketNotFound):
        synpath.fetch_order_book_at("kalshi:KX-A", as_of_ms=1,
                                    client=answering({"error": {"code": "not_found", "message": "no"}}, [], status=404))


def test_an_unreachable_host_names_the_address():
    from synpath.errors import NetworkError

    def handler(request):
        raise httpx.ConnectError("connection refused")
    with pytest.raises(NetworkError, match="could not reach https://api.synpath.dev"):
        synpath.match_market("kalshi:KX-A", client=client(handler))
