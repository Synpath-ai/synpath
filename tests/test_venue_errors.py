"""A venue's refusal reads as `<venue>: <its reason>`; the raw payload stays on the exception."""
from __future__ import annotations

import httpx
import pytest

from synpath.base import HttpClient, venue_reason
from synpath.errors import BadRequest, MarketNotFound


def client_answering(status: int, payload) -> HttpClient:
    def handler(request):
        return httpx.Response(status, json=payload)
    return HttpClient("https://venue.test", limiter=None, client=httpx.Client(transport=httpx.MockTransport(handler)),
                      venue="kalshi", attempts=1)


def test_the_venues_reason_is_read_from_each_payload_shape():
    assert venue_reason({"error": {"code": "insufficient_balance", "message": "insufficient balance"}}, "") == \
        "insufficient balance"
    assert venue_reason({"error": {"message": "insufficient shard balance", "details": "Exchange user not found."}}, "") \
        == "insufficient shard balance (Exchange user not found.)"
    assert venue_reason({"error": "not enough balance / allowance"}, "") == "not enough balance / allowance"
    assert venue_reason({"message": "invalid tick size"}, "") == "invalid tick size"
    assert venue_reason(None, "  plain\n text ") == "plain text"


def test_a_refusal_names_the_venue_and_keeps_the_raw_payload():
    payload = {"error": {"code": "insufficient_balance", "message": "insufficient balance"}}
    with pytest.raises(BadRequest) as refused:
        client_answering(400, payload).get("/portfolio/orders")
    assert str(refused.value) == "kalshi: insufficient balance"
    assert refused.value.body == payload and refused.value.status == 400 and refused.value.code == "insufficient_balance"


def test_a_404_still_says_not_found():
    with pytest.raises(MarketNotFound) as missing:
        client_answering(404, {"error": {"code": "not_found", "message": "market"}}).get("/markets/X")
    assert str(missing.value) == "kalshi: not found: market"
