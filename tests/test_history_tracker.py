"""The standalone historical-book example stays compatible with the API models."""

import json

import httpx
import pytest

from examples.track_historical_book import BookGap, BookState, iter_book_timeline


def _book(size: int) -> dict:
    return {
        "market_id": "kalshi:M", "side": "yes", "venue": "kalshi",
        "bids": [{"price": 0.5, "size": size}], "asks": [],
        "timestamp": 1000, "datetime": "1970-01-01T00:00:01Z",
        "as_of_ms": 1000, "venue_timestamp_ms": None,
        "book_model": "shared_complement", "derived": True,
        "depth_scope": "full", "info": {},
    }


def _response(start: int, end: int, segments: list[dict], version: str = "v1") -> dict:
    return {"metadata": {"dataset_version": version}, "market_id": "kalshi:M",
            "start_ms": start, "end_ms": end, "segments": segments, "next_cursor": None}


def test_replays_full_book_and_preserves_gap_across_calls():
    def respond(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body["start_ms"] == 1000:
            segments = [
                {"kind": "data", "start_ms": 1000, "end_ms": 1500,
                 "initial_book": _book(7), "changes": [{"kind": "delta",
                     "observed_at_ms": 1200, "book_side": "bid",
                     "price_exact": "0.5", "quantity_delta_exact": "-2"}]},
                {"kind": "absent", "start_ms": 1500, "end_ms": 2000,
                 "reason": "connection_gap"},
            ]
        else:
            segments = [{"kind": "data", "start_ms": 2000, "end_ms": 3000,
                         "initial_book": _book(3), "changes": []}]
        return httpx.Response(200, json=_response(body["start_ms"], body["end_ms"], segments))

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        items = list(iter_book_timeline(client, "http://test", "kalshi:M", 1000, 3000,
                                        chunk_ms=1000))
    assert [type(item) for item in items] == [BookState, BookState, BookGap, BookState]
    assert items[0].book.bids[0].size == 7
    assert items[1].book.bids[0].size == 5
    assert items[2] == BookGap(1500, 2000, "connection_gap")
    assert items[3].book.bids[0].size == 3


def test_bisects_413_instead_of_losing_changes():
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        start, end = body["start_ms"], body["end_ms"]
        calls.append((start, end))
        if end - start > 1000:
            return httpx.Response(413, json={"error": {"code": "result_too_large"}})
        return httpx.Response(200, json=_response(start, end, [
            {"kind": "absent", "start_ms": start, "end_ms": end,
             "reason": "outside_loaded_data"}]))

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        items = list(iter_book_timeline(client, "http://test", "kalshi:M", 1000, 3000,
                                        chunk_ms=2000))
    assert calls == [(1000, 3000), (1000, 2000), (2000, 3000)]
    assert items == [BookGap(1000, 2000, "outside_loaded_data"),
                     BookGap(2000, 3000, "outside_loaded_data")]


def test_rejects_dataset_version_change_mid_walk():
    def respond(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        start, end = body["start_ms"], body["end_ms"]
        return httpx.Response(200, json=_response(start, end, [
            {"kind": "absent", "start_ms": start, "end_ms": end,
             "reason": "outside_loaded_data"}], "v1" if start == 1000 else "v2"))

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(RuntimeError, match="Dataset changed"):
            list(iter_book_timeline(client, "http://test", "kalshi:M", 1000, 3000,
                                    chunk_ms=1000))
