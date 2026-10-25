"""Gemini combos and combo RFQ: the combo routes against a scripted venue,
the maker side of the sealed auction against a fake socket. Every payload is
one of Gemini's documented examples (`tests/samples/gemini_combo_rfq.json`):
the combo routes are not enabled in production and quoting needs an
eligible account, so nothing was recorded. Nothing here reaches Gemini."""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal

import httpx
import pytest

import synpath
from synpath.errors import AuthenticationError, BadRequest, ExchangeNotAvailable, MarketNotFound, NetworkError, \
    NotSupported, RateLimitExceeded, RequestTimeout
from synpath.trading.credentials import GeminiCredentials
from synpath.trading.errors import CredentialsMissing, InvalidOrder, OrderRejected, PermissionDenied
from synpath.trading.gemini import (
    CombosUnavailable, GeminiSigner, GeminiTrading, RFQ_APP_ONLY, combo_legs_of, combo_of,
)
from synpath.ws.base import StreamStatusEvent, VenueEvent
from synpath.ws.gemini import GeminiRfqStream, delivery_of, quote_params, rfq_of, ws_error

from conftest import load

SAMPLE = load("gemini_combo_rfq.json")
REST, WS = SAMPLE["rest"], SAMPLE["ws"]
SECRET = "synpath-test-secret"
KEY = "account-test0000000000000000"
CREDS = GeminiCredentials(api_key=KEY, api_secret=SECRET)
SANDBOX = GeminiCredentials(api_key=KEY, api_secret=SECRET, env="sandbox")
COMBO = "gemini:GEMI-CMB-0526-A7F3B2C1D4E5"
RFQ = "01J9Z3K7Q2N8M4P6R8T0V2W4X6"
QUOTE = "01J9Z2K7Q2N8M4P6R8T0V2W4X6"

# Computed independently with `openssl dgst -sha384 -hmac synpath-test-secret`
# over the base64 of the payload below.
COMBO_PAYLOAD = (
    "eyJyZXF1ZXN0IjoiL3YxL3ByZWRpY3Rpb24tbWFya2V0cy9jb21ib3MiLCJub25jZSI6IjE3NzYyOTQ0NDciLCJsZWdzIjpbeyJjb250cmFj"
    "dElkIjoiMTAxIiwicmVxdWlyZWRPdXRjb21lIjoiWWVzIn0seyJjb250cmFjdElkIjoiMjAyIiwicmVxdWlyZWRPdXRjb21lIjoiTm8ifV19"
)
COMBO_SIGNATURE = "677b0b807a6705f677d2a55a07d91c21f8d51361c550d0fa59cfacf71d8fdcaea8963a7ce74143923d743b98c8d965a9"


def run(coro):
    return asyncio.run(coro)


def decoded(req: httpx.Request) -> dict:
    return json.loads(base64.b64decode(req.headers["X-GEMINI-PAYLOAD"]))


# ---------------------------------------------------------------------------
# REST: combos
# ---------------------------------------------------------------------------

class Venue:
    """The combo routes as documented; `disabled` answers as production does."""

    def __init__(self, *, disabled: bool = False):
        self.disabled = disabled
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        path = req.url.path
        if self.disabled and path.startswith("/v1/prediction-markets/combos"):
            return httpx.Response(404, json=REST["errors"]["combos_unavailable"])
        if path == "/v1/prediction-markets/combos" and req.method == "GET":
            return httpx.Response(200, json=REST["combos"])
        if path == "/v1/prediction-markets/combos" and req.method == "POST":
            assert req.content == b"", "a private request sends no body"
            legs = decoded(req)["legs"]
            if any(leg["contractId"] == "999" for leg in legs):
                return httpx.Response(404, json=REST["errors"]["leg_not_found"])
            return httpx.Response(201, json=REST["create_combo"])
        if path == "/v1/prediction-markets/combos/GEMI-CMB-0526-A7F3B2C1D4E5":
            return httpx.Response(200, json=REST["combos"]["combos"][0])
        if path.startswith("/v1/prediction-markets/combos/"):
            return httpx.Response(404, json=REST["errors"]["combo_not_found"])
        return httpx.Response(404, json={"error": "no route"})


def trading(venue: Venue, creds: GeminiCredentials = SANDBOX, **kwargs) -> GeminiTrading:
    from synpath.trading.limiter import BudgetLimiter

    return GeminiTrading(creds, client=httpx.AsyncClient(transport=httpx.MockTransport(venue)),
                         limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000), **kwargs)


class TestCombos:
    def test_a_listing_row_reads_as_a_combo_with_its_legs(self):
        combo = combo_of(REST["combos"]["combos"][0])
        assert combo["market_id"] == COMBO and combo["combo_id"] == "456" and combo["status"] == "active"
        assert [(l["contract_id"], l["outcome"], l["market_id"]) for l in combo["legs"]] == [
            ("101", "yes", "gemini:GEMI-BTC-EOY26-HI120000"), ("202", "yes", "gemini:GEMI-ETH-EOY26-HI4000")]
        assert combo["legs"][0]["result"] is None and combo["expires_at"] == 1798761599000

    def test_a_create_answer_reads_the_same_way(self):
        combo = combo_of(REST["create_combo"]["combo"])
        assert combo["market_id"] == COMBO and combo["registered"] is True and combo["combo_id"] == "456"
        assert [(l["contract_id"], l["outcome"]) for l in combo["legs"]] == [("101", "yes"), ("202", "no")]
        unregistered = combo_of({**REST["create_combo"]["combo"], "instrumentRegistered": False,
                                 "instrumentSymbol": None})
        assert unregistered["market_id"] is None

    def test_legs_are_checked_before_signing(self):
        assert combo_legs_of([("101", "yes"), {"contract_id": "202", "outcome": "NO"}]) == [
            {"contractId": "101", "requiredOutcome": "Yes"}, {"contractId": "202", "requiredOutcome": "No"}]
        with pytest.raises(InvalidOrder, match="2 to 6"):
            combo_legs_of([("101", "yes")])
        with pytest.raises(InvalidOrder, match="2 to 6"):
            combo_legs_of([(str(n), "yes") for n in range(1, 8)])
        with pytest.raises(InvalidOrder, match="once"):
            combo_legs_of([("101", "yes"), ("101", "no")])
        with pytest.raises(InvalidOrder, match="numeric contract id"):
            combo_legs_of([("GEMI-BTC-EOY26-HI120000", "yes"), ("202", "no")])
        with pytest.raises(InvalidOrder, match="yes or no"):
            combo_legs_of([("101", "maybe"), ("202", "no")])

    def test_create_combo_signs_its_legs_as_a_known_answer(self):
        headers = GeminiSigner(KEY, SECRET).headers(
            "/v1/prediction-markets/combos", {"legs": combo_legs_of([("101", "yes"), ("202", "no")])},
            nonce="1776294447")
        assert headers["X-GEMINI-PAYLOAD"] == COMBO_PAYLOAD and headers["X-GEMINI-SIGNATURE"] == COMBO_SIGNATURE

    def test_create_combo_against_the_sandbox(self):
        venue = Venue()
        adapter = trading(venue, clock=lambda: 1776294447)
        assert adapter.base_url == "https://api.sandbox.gemini.com"
        combo = run(adapter.create_combo([("101", "yes"), ("202", "no")]))
        [sent] = venue.requests
        assert sent.method == "POST" and sent.headers["X-GEMINI-PAYLOAD"] == COMBO_PAYLOAD
        assert sent.headers["X-GEMINI-SIGNATURE"] == COMBO_SIGNATURE
        assert combo["market_id"] == COMBO and combo["info"]["alreadyExisted"] is False

    def test_listing_and_lookup_are_public_and_page_by_offset(self):
        venue = Venue()
        adapter = trading(venue)
        page = run(adapter.fetch_combos(contract_id="101", registered=True, limit=10))
        assert [c["market_id"] for c in page] == [COMBO] and page.next_cursor is None
        listing = venue.requests[0]
        assert "X-GEMINI-SIGNATURE" not in listing.headers
        assert dict(listing.url.params) == {"contractId": "101", "instrumentRegistered": "true", "limit": "10"}
        assert run(adapter.fetch_combo(COMBO))["legs"][1]["contract_id"] == "202"
        with pytest.raises(MarketNotFound, match="no combo"):
            run(adapter.fetch_combo("gemini:GEMI-CMB-0526-000000000000"))

    def test_production_says_combos_are_unavailable_with_geminis_answer(self):
        adapter = trading(Venue(disabled=True), creds=CREDS)
        for call in (adapter.fetch_combos(), adapter.fetch_combo(COMBO),
                     adapter.create_combo([("101", "yes"), ("202", "no")])):
            with pytest.raises(CombosUnavailable) as caught:
                run(call)
            assert "production" in str(caught.value) and "NOT_FOUND Not Found" in str(caught.value)
            assert caught.value.status == 404 and caught.value.body == REST["errors"]["combos_unavailable"]

    def test_a_missing_leg_is_not_mistaken_for_combos_being_off(self):
        with pytest.raises(MarketNotFound) as caught:
            run(trading(Venue()).create_combo([("101", "yes"), ("999", "no")]))
        assert not isinstance(caught.value, CombosUnavailable)
        assert caught.value.body["code"] == "COMBO_LEG_NOT_FOUND"

    def test_the_requester_side_is_not_an_api(self):
        adapter = trading(Venue())
        for call in (adapter.create_rfq(COMBO, quantity=Decimal("1")), adapter.accept_quote(RFQ, QUOTE)):
            with pytest.raises(NotSupported, match="Gemini app and website"):
                run(call)
        assert "GeminiRfqStream" in RFQ_APP_ONLY
        assert GeminiTrading.has["rfq"] == "partial"

    def test_a_combo_is_traded_as_its_own_instrument(self):
        from synpath.trading.gemini import translate_order
        from synpath.trading.types import OrderRequest, Side

        body = translate_order(OrderRequest(market_id=COMBO, side=Side.BUY, quantity=Decimal("10"),
                                            price=Decimal("0.42")))
        assert body["symbol"] == "GEMI-CMB-0526-A7F3B2C1D4E5" and body["outcome"] == "yes"


# ---------------------------------------------------------------------------
# WebSocket: the maker side of the auction
# ---------------------------------------------------------------------------

class FakeSocket:
    """Records frames; answers a method with `answers[method]` when given."""

    def __init__(self, stream: GeminiRfqStream | None = None, answers: dict | None = None):
        self.sent: list = []
        self.stream = stream
        self.answers = answers or {}

    async def send(self, text):
        frame = json.loads(text)
        self.sent.append(frame)
        answer = self.answers.get(frame.get("method"))
        if answer is not None and self.stream is not None:
            reply = {**answer, "id": frame["id"]}
            asyncio.get_running_loop().call_soon(self.stream._dispatch, json.dumps(reply))


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


def connected(stream: GeminiRfqStream, **answers) -> FakeSocket:
    socket = FakeSocket(stream, {m.replace("_", ".", 1): a for m, a in answers.items()})
    stream._ws = socket
    return socket


class TestRfqEvents:
    def test_the_public_broadcast_reads_as_an_auction(self):
        rfq = rfq_of(WS["rfq_open"])
        assert rfq["rfq_id"] == RFQ and rfq["state"] == "OPEN"
        assert rfq["market_id"] == "gemini:GEMI-CMB-260306-A1B2C3D4E5F6G7H8-CMB"
        assert rfq["legs"] == [
            {"contract_id": "123456", "outcome": "yes", "market_id": "gemini:GEMI-XRPUSD-260828"},
            {"contract_id": "123789", "outcome": "no", "market_id": None}]
        assert rfq["notional"] == Decimal("50000.00") and rfq["quantity"] is None
        assert (rfq["quote_deadline"], rfq["expires_at"], rfq["created_at"]) == (1780000001000, 1780000120000,
                                                                                 1780000000000)

    def test_a_private_delivery_reads_as_the_winning_quote(self):
        delivery = delivery_of(WS["delivery_closed"])
        assert (delivery["transition"], delivery["state"], delivery["quote_id"]) == ("CLOSED", "PENDING_ACCEPTANCE",
                                                                                      QUOTE)
        assert delivery["price"] == Decimal("0.55") and delivery["size"] == Decimal("100000")
        assert delivery["valid_until"] == 1780000001000 and delivery["quote_status"] == "ACTIVE"

    def test_the_stream_keeps_each_auctions_last_state(self):
        stream = GeminiRfqStream()
        [opened] = stream.handle(WS["rfq_open"])
        [finished] = stream.handle(WS["rfq_finalized"])
        assert isinstance(opened, VenueEvent) and opened.name == "rfq"
        # The terminal event leaves out `w`, `x` and `s`; what the opening one said is kept.
        assert finished.payload["state"] == "FINALIZED" and finished.payload["fill_quantity"] == Decimal("90909")
        assert finished.payload["quote_deadline"] == 1780000001000
        assert finished.payload["market_id"] == "gemini:GEMI-CMB-260306-A1B2C3D4E5F6G7H8-CMB"

    def test_deliveries_are_at_least_once_and_are_deduplicated(self):
        stream = GeminiRfqStream(CREDS)
        assert [e.name for e in stream.handle(WS["delivery_closed"])] == ["rfq_delivery"]
        assert stream.handle(WS["delivery_closed"]) == []
        [accepted] = stream.handle(WS["delivery_accepted"])
        assert accepted.payload["transition"] == "ACCEPTED" and accepted.payload["state"] == "CONFIRMING"

    def test_an_unreadable_rfq_is_a_status_not_a_crash(self):
        stream = GeminiRfqStream()
        assert stream.handle({**WS["rfq_open"], "S": "SOMETHING"}) == []
        [status] = drain(stream)
        assert isinstance(status, StreamStatusEvent) and status.state == "error"


class TestRfqSubscriptions:
    def test_it_is_exported_and_discovery_is_public(self):
        assert synpath.GeminiRfqStream is GeminiRfqStream
        stream = GeminiRfqStream()
        assert stream.url == "wss://ws.gemini.com" and stream.headers() == {} and stream.private is False
        assert GeminiRfqStream(sandbox=True).url == "wss://ws.sandbox.gemini.com"
        assert GeminiRfqStream(SANDBOX).url == "wss://ws.sandbox.gemini.com"

        async def go():
            socket = connected(stream)
            await stream.watch_rfqs()
            return socket.sent

        assert run(go()) == [{"id": 1, "method": "SUBSCRIBE", "params": ["requestForQuote"]}]

    def test_deliveries_need_a_key_on_the_signed_handshake(self):
        with pytest.raises(CredentialsMissing):
            run(GeminiRfqStream().watch_deliveries())
        stream = GeminiRfqStream(CREDS, signer=GeminiSigner(KEY, SECRET, clock=lambda: 1776294447))
        assert stream.private is True
        headers = stream.headers()
        assert headers["X-GEMINI-NONCE"] == "1776294447" and headers["X-GEMINI-PAYLOAD"] == "MTc3NjI5NDQ0Nw=="
        assert headers["X-GEMINI-SIGNATURE"] == (
            "37e6fd382f7ec4cf24501d5ebf569a27e73d63fe0cc2a613c82611b9fd83b10d0ec51f4abcc686bb160c9cf9f72104fd")

    def test_a_connection_takes_one_delivery_scope(self):
        async def go():
            stream = GeminiRfqStream(CREDS)
            socket = connected(stream)
            await stream.watch_deliveries()
            with pytest.raises(BadRequest, match="one delivery scope"):
                await stream.watch_deliveries("session")
            with pytest.raises(BadRequest, match="account or session"):
                await stream.watch_deliveries("all")
            return socket.sent

        assert run(go()) == [{"id": 1, "method": "SUBSCRIBE", "params": ["requestForQuote@account"]}]


class TestQuoteMethods:
    def test_submit_sends_the_documented_frame_and_returns_the_quote_id(self):
        async def go():
            stream = GeminiRfqStream(CREDS, clock=lambda: 1780000000.5)
            stream.handle(WS["rfq_open"])  # open, its window closing at 1780000001000
            socket = connected(stream, rfq_submit_quote=WS["submit_quote_response"])
            result = await stream.submit_quote(RFQ, "0.55", "1000", valid_until=1780000060000,
                                               client_id="maker-fill-123")
            return stream, socket.sent, result

        stream, sent, result = run(go())
        [frame] = sent
        assert {k: v for k, v in frame.items() if k != "id"} == {k: v for k, v in WS["submit_quote"].items() if k != "id"}
        assert result == WS["submit_quote_response"]["result"] and stream.quotes[RFQ] == QUOTE

    def test_withdraw_and_confirm_send_the_documented_frames(self):
        async def go():
            stream = GeminiRfqStream(CREDS)
            socket = connected(stream, rfq_withdraw_quote=WS["withdraw_quote_response"],
                               rfq_confirm_quote=WS["confirm_quote_response"])
            withdrawn = await stream.withdraw_quote(RFQ, QUOTE)
            confirmed = await stream.confirm_quote(RFQ, QUOTE, True)
            return socket.sent, withdrawn, confirmed

        sent, withdrawn, confirmed = run(go())
        assert [{k: v for k, v in f.items() if k != "id"} for f in sent] == [
            {k: v for k, v in WS[name].items() if k != "id"} for name in ("withdraw_quote", "confirm_quote")]
        assert withdrawn["quoteId"] == QUOTE and confirmed["confirmed"] is True

    def test_the_last_look_needs_an_explicit_decision(self):
        stream = GeminiRfqStream(CREDS)
        connected(stream)
        with pytest.raises(InvalidOrder, match="True or False"):
            run(stream.confirm_quote(RFQ, QUOTE, None))  # type: ignore[arg-type]

    def test_a_quote_is_checked_before_it_is_sent(self):
        assert quote_params(RFQ, Decimal("0.55"), 100) == {"rfqId": RFQ, "price": "0.55", "quantity": "100"}
        for bad in ({"price": "0.1234"}, {"price": "1"}, {"price": "0"}, {"quantity": "0"},
                    {"quantity": "1.005"}, {"client_id": "x" * 37}, {"client_id": "café"},
                    {"valid_until": 1}, {"valid_until": "1780000060000"}):
            args = {"price": "0.55", "quantity": "100", **bad}
            with pytest.raises(InvalidOrder):
                quote_params(RFQ, args.pop("price"), args.pop("quantity"), now_ms=1780000000000, **args)

    def test_a_closed_window_is_refused_locally(self):
        async def go():
            stream = GeminiRfqStream(CREDS, clock=lambda: 1780000001.0)
            socket = connected(stream)
            stream.handle(WS["rfq_open"])  # its window closes at 1780000001000
            with pytest.raises(InvalidOrder, match="no longer quotable"):
                await stream.submit_quote(RFQ, "0.55", "100000")
            return socket.sent

        assert run(go()) == []

    def test_geminis_error_comes_back_typed_with_its_code(self):
        async def go():
            stream = GeminiRfqStream(CREDS)
            connected(stream, rfq_confirm_quote=WS["closed_auction_error"])
            await stream.confirm_quote(RFQ, QUOTE, True)

        with pytest.raises(OrderRejected) as caught:
            run(go())
        assert caught.value.reason == "-2010" and "not in a valid state" in str(caught.value)

    @pytest.mark.parametrize("code, kind", [
        (-1002, AuthenticationError), (-1004, PermissionDenied), (-1013, BadRequest), (-1003, RateLimitExceeded),
        (-1101, ExchangeNotAvailable),
    ])
    def test_the_documented_error_codes(self, code, kind):
        error = ws_error({"id": 1, "status": 400, "error": {"code": code, "msg": "no"}}, method="rfq.submit_quote")
        assert isinstance(error, kind)

    def test_quote_methods_need_a_key_and_a_live_connection(self):
        with pytest.raises(CredentialsMissing):
            run(GeminiRfqStream().submit_quote(RFQ, "0.55", "100"))
        with pytest.raises(NetworkError, match="never queued"):
            run(GeminiRfqStream(CREDS).submit_quote(RFQ, "0.55", "100"))

    def test_no_answer_is_an_unknown_outcome_and_is_not_resent(self):
        async def go():
            stream = GeminiRfqStream(CREDS, reply_timeout=0.05)
            socket = connected(stream)
            with pytest.raises(RequestTimeout, match="outcome unknown"):
                await stream.submit_quote(RFQ, "0.55", "100")
            return stream, socket.sent

        stream, sent = run(go())
        assert len(sent) == 1 and stream._pending == {}

    def test_a_dropped_connection_fails_the_waiting_method(self):
        async def go():
            stream = GeminiRfqStream(CREDS)
            connected(stream)
            task = asyncio.ensure_future(stream.confirm_quote(RFQ, QUOTE, True))
            await asyncio.sleep(0)
            stream.on_disconnect()
            with pytest.raises(NetworkError, match="connection lost"):
                await task

        run(go())
