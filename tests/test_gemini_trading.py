"""Gemini Predictions order entry: known-answer signing, order bodies on the
YES leg, REST records, error mapping, and the adapter against a scripted
venue. Records are the venue's documented examples
(`tests/samples/gemini_trading.json`): reading them needs an account. Nothing
here reaches Gemini."""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal

import httpx
import pytest

from synpath import AuthenticationError, Client
from synpath.trading.credentials import CredentialsMissing, GeminiCredentials, load_credentials, load_gemini
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderNotFound, PermissionDenied
from synpath.trading.gemini import (
    GeminiSigner, GeminiTrading, balance_of, fill_of_order, map_error, order_of, positions_of, settlement_of,
    translate_order,
)
from synpath.trading.types import (
    OrderRequest, OrderStatus, OrderType, PositionSide, Precision, Side, TimeInForce,
)
from synpath.errors import BadRequest, MarketNotFound

from conftest import load

DOCS = load("gemini_trading.json")
EVENT = load("gemini_event.json")
SECRET = "synpath-test-secret"
KEY = "account-test0000000000000000"
CREDS = GeminiCredentials(api_key=KEY, api_secret=SECRET)
FED = "gemini:GEMI-FEDJAN26-DN25"
MIL = "gemini:GEMI-NBA-2610232000-CHI-MIL-M-MIL"

# Computed independently with `openssl dgst -sha384 -hmac synpath-test-secret`
# over the base64 payload (and, for the WebSocket, over base64("1776294447")).
ORDER_PAYLOAD = (
    "eyJyZXF1ZXN0IjoiL3YxL3ByZWRpY3Rpb24tbWFya2V0cy9vcmRlciIsIm5vbmNlIjoiMTc3NjI5NDQ0NyIsInN5bWJvbCI6IkdFTUktRkVESkFO"
    "MjYtRE4yNSIsIm9yZGVyVHlwZSI6ImxpbWl0Iiwic2lkZSI6ImJ1eSIsIm91dGNvbWUiOiJ5ZXMiLCJxdWFudGl0eSI6IjEiLCJwcmljZSI6IjAu"
    "NTAiLCJ0aW1lSW5Gb3JjZSI6Imdvb2QtdGlsLWNhbmNlbCJ9"
)
ORDER_SIGNATURE = "287a88a2772147fb4d069544d81e5ef6ee4cc496116d544f84ae88a44ffc01a9c92ef28144a462df306f77752f3d960a"
WS_SIGNATURE = "37e6fd382f7ec4cf24501d5ebf569a27e73d63fe0cc2a613c82611b9fd83b10d0ec51f4abcc686bb160c9cf9f72104fd"


def request(**changes) -> OrderRequest:
    base = {"market_id": FED, "side": Side.BUY, "quantity": Decimal("1"), "price": Decimal("0.50")}
    return OrderRequest(**{**base, **changes})


def decoded(req: httpx.Request) -> dict:
    return json.loads(base64.b64decode(req.headers["X-GEMINI-PAYLOAD"]))


class TestSigning:
    def test_the_rest_payload_and_signature_are_known_answers(self):
        signer = GeminiSigner(KEY, SECRET)
        fields = translate_order(request())
        headers = signer.headers("/v1/prediction-markets/order", fields, nonce="1776294447")
        assert headers["X-GEMINI-PAYLOAD"] == ORDER_PAYLOAD
        assert headers["X-GEMINI-SIGNATURE"] == ORDER_SIGNATURE
        assert headers["X-GEMINI-APIKEY"] == KEY
        assert (headers["Content-Type"], headers["Content-Length"], headers["Cache-Control"]) == ("text/plain", "0", "no-cache")

    def test_the_websocket_handshake_is_a_known_answer(self):
        headers = GeminiSigner(KEY, SECRET).ws_headers(nonce="1776294447")
        assert headers["X-GEMINI-NONCE"] == "1776294447" and headers["X-GEMINI-PAYLOAD"] == "MTc3NjI5NDQ0Nw=="
        assert headers["X-GEMINI-SIGNATURE"] == WS_SIGNATURE

    def test_the_nonce_is_unix_seconds(self):
        assert GeminiSigner(KEY, SECRET, clock=lambda: 1776294447.9).nonce() == "1776294447"

    def test_a_master_key_names_its_subaccount(self):
        payload = GeminiSigner("master-x", SECRET, account="primary").payload("/v1/balances", None, nonce="1")
        assert json.loads(base64.b64decode(payload)) == {"request": "/v1/balances", "nonce": "1", "account": "primary"}

    def test_the_secret_is_not_in_the_repr(self):
        assert SECRET not in repr(GeminiSigner(KEY, SECRET)) and SECRET not in repr(CREDS)


class TestOrderBody:
    def test_buy_is_buying_yes_at_the_price(self):
        assert translate_order(request()) == {
            "symbol": "GEMI-FEDJAN26-DN25", "orderType": "limit", "side": "buy", "outcome": "yes",
            "quantity": "1", "price": "0.50", "timeInForce": "good-til-cancel",
        }

    def test_sell_is_buying_no_at_one_minus_the_price(self):
        body = translate_order(request(side=Side.SELL, price=Decimal("0.65")))
        assert (body["side"], body["outcome"], body["price"]) == ("buy", "no", "0.35")

    def test_reduce_only_sells_what_is_held(self):
        sell = translate_order(request(side=Side.SELL, price=Decimal("0.65"), reduce_only=True))
        assert (sell["side"], sell["outcome"], sell["price"]) == ("sell", "yes", "0.65")
        buy = translate_order(request(side=Side.BUY, price=Decimal("0.65"), reduce_only=True))
        assert (buy["side"], buy["outcome"], buy["price"]) == ("sell", "no", "0.35")

    @pytest.mark.parametrize("changes, tif", [
        ({"type": OrderType.MARKET}, "immediate-or-cancel"),
        ({"type": OrderType.MARKET, "time_in_force": TimeInForce.FOK}, "fill-or-kill"),
        ({"time_in_force": TimeInForce.IOC}, "immediate-or-cancel"),
    ])
    def test_time_in_force(self, changes, tif):
        assert translate_order(request(**changes))["timeInForce"] == tif

    def test_post_only_is_maker_or_cancel(self):
        assert translate_order(request(post_only=True))["makerOrCancel"] is True
        with pytest.raises(InvalidOrder):
            translate_order(request(post_only=True, time_in_force=TimeInForce.IOC))

    @pytest.mark.parametrize("changes", [
        {"time_in_force": TimeInForce.GTD, "expires_at": 1},
        {"type": OrderType.STOP_LIMIT, "stop_price": Decimal("0.4"), "price": Decimal("0.45")},
        {"price": Decimal("0.505")},
        {"quantity": Decimal("0.005")},
        {"market_id": "kalshi:KXFOO"},
        {"market_id": "gemini:NBA-2610232000-CHI-MIL-M"},
    ])
    def test_refused_before_signing(self, changes):
        with pytest.raises(InvalidOrder):
            translate_order(request(**changes))

    def test_a_finer_grid_is_accepted_with_its_precision(self):
        precision = Precision(tick=Decimal("0.0001"), min_quantity=Decimal("0.01"), quantity_step=Decimal("0.01"))
        assert translate_order(request(price=Decimal("0.5025")), precision=precision)["price"] == "0.5025"


class TestRecords:
    def test_an_order_is_read_on_the_yes_leg(self):
        yes_buy, yes_sell = DOCS["active_orders"]["orders"]
        order = order_of(yes_buy)
        assert (order.id, order.side, order.price, order.filled, order.remaining) == (
            "12345678901", Side.BUY, Decimal("0.65"), Decimal("25"), Decimal("75"))
        assert order.average_price == Decimal("0.64") and order.status == OrderStatus.OPEN
        sold = order_of(yes_sell)
        assert sold.side == Side.SELL and sold.price == Decimal("0.65") and sold.reduce_only

    def test_a_no_buy_reads_as_a_sell_at_the_complement(self):
        row = {**DOCS["order"], "side": "buy", "outcome": "no", "price": "0.35"}
        order = order_of(row)
        assert order.side == Side.SELL and order.price == Decimal("0.65") and not order.reduce_only
        assert order.info["venue_outcome"] == "no"

    def test_status_words(self):
        assert order_of({**DOCS["order"], "status": "filled"}).status == OrderStatus.CLOSED
        assert order_of({**DOCS["order"], "status": "cancelled"}).status == OrderStatus.CANCELED

    def test_positions_net_on_the_yes_leg_and_keep_both_inventories(self):
        [position] = positions_of(DOCS["positions"]["positions"])
        assert position.market_id == FED and position.side == PositionSide.SHORT
        assert (position.inventory_yes, position.inventory_no, position.contracts) == (Decimal("125"), Decimal("200"), Decimal("75"))
        assert position.entry_price == Decimal("0.64")       # NO bought at 0.36

    def test_a_settlement(self):
        won, lost = (settlement_of(row) for row in DOCS["settled"]["positions"])
        assert won.market_id == FED and won.held == PositionSide.LONG and won.won and won.payout == Decimal("125.00")
        assert won.pnl == Decimal("46.25") and won.cost == Decimal("78.75")
        assert lost.held == PositionSide.SHORT and lost.won is False and lost.quantity == Decimal("200")

    def test_the_usd_balance(self):
        from synpath.trading.types import Account

        balance = balance_of(DOCS["balances"], account=Account(venue="gemini"))
        assert (balance.total, balance.available, balance.locked) == (Decimal("15000.00"), Decimal("5000.00"), Decimal("10000.00"))

    def test_an_order_that_filled_is_one_aggregate_fill(self):
        fill = fill_of_order(DOCS["active_orders"]["orders"][0])
        assert (fill.quantity, fill.price, fill.side) == (Decimal("25"), Decimal("0.64"), Side.BUY)
        assert fill.info["aggregate"] and fill_of_order(DOCS["active_orders"]["orders"][1]) is None


class TestErrors:
    @pytest.mark.parametrize("body, status, kind", [
        ("terms", 403, PermissionDenied),
        ("blocked", 403, PermissionDenied),
        ("auth", 400, AuthenticationError),
        ("funds", 422, InsufficientFunds),
    ])
    def test_mapping(self, body, status, kind):
        exc = BadRequest("gemini: x", body=DOCS["errors"][body], status=status)
        assert isinstance(map_error(exc), kind)

    def test_a_missing_order_on_cancel(self):
        exc = MarketNotFound("gemini: not found", body={"error": "NotFound"}, status=404)
        assert isinstance(map_error(exc, path="/v1/prediction-markets/order/cancel"), OrderNotFound)
        assert isinstance(map_error(BadRequest("x", body=DOCS["errors"]["not_found"], status=404)), OrderNotFound)

    def test_terms_say_what_to_do(self):
        exc = map_error(BadRequest("x", body=DOCS["errors"]["terms"], status=403))
        assert "terms" in str(exc) and "accept" in str(exc)


class TestCredentials:
    def test_key_and_secret(self):
        creds = load_gemini({"GEMINI_API_KEY": KEY, "GEMINI_API_SECRET": SECRET})
        assert creds is not None and creds.env == "prod" and creds.account is None and creds.secrets == [SECRET]

    def test_nothing_set_is_none(self):
        assert load_gemini({}) is None

    @pytest.mark.parametrize("env", [
        {"GEMINI_API_KEY": KEY},
        {"GEMINI_API_SECRET": SECRET},
        {"GEMINI_API_KEY": KEY, "GEMINI_API_SECRET": KEY},
        {"GEMINI_API_KEY": KEY, "GEMINI_API_SECRET": SECRET, "GEMINI_ENV": "staging"},
    ])
    def test_incomplete_or_wrong(self, env):
        with pytest.raises(CredentialsMissing):
            load_gemini(env)

    def test_sandbox_and_a_subaccount(self, tmp_path):
        env = tmp_path / ".env"
        env.write_text(f"GEMINI_API_KEY=master-x\nGEMINI_API_SECRET={SECRET}\nGEMINI_ENV=sandbox\nGEMINI_ACCOUNT=primary\n")
        creds = load_credentials({}, dotenv=env, redact_logs=False)["gemini"]
        assert creds.env == "sandbox" and creds.account == "primary"
        assert GeminiTrading(creds).base_url == "https://api.sandbox.gemini.com"

    def test_the_client_builds_the_adapter(self):
        client = Client({"gemini": CREDS})
        assert isinstance(client.trading("gemini"), GeminiTrading)


# ---------------------------------------------------------------------------
# The adapter against a scripted venue
# ---------------------------------------------------------------------------

class Venue:
    """Answers private and public routes; records every request."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.rate_limited = 0
        self.active = DOCS["active_orders"]

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        path = req.url.path
        if self.rate_limited:
            self.rate_limited -= 1
            return httpx.Response(429, json={"result": "error", "reason": "RateLimited", "message": "slow down"})
        if path.startswith("/v1/prediction-markets/events/"):
            ticker = path.rsplit("/", 1)[1]
            return httpx.Response(200, json=EVENT) if ticker == EVENT["ticker"] else httpx.Response(404, json={"error": "Event not found"})
        if req.method == "POST":
            assert req.content == b"", "a private request sends no body"
            payload = decoded(req)
            assert payload["request"] == path
        routes = {
            "/v1/prediction-markets/order": lambda: DOCS["order"],
            "/v1/prediction-markets/order/batch": lambda: DOCS["order_batch"],
            "/v1/prediction-markets/order/cancel": lambda: DOCS["cancel"],
            "/v1/prediction-markets/order/batch/cancel": lambda: self._cancel(req),
            "/v1/prediction-markets/orders/active": lambda: self.active,
            "/v1/prediction-markets/orders/history": lambda: {"orders": [{**DOCS["order"], "status": "cancelled",
                                                                         "orderId": 777}], "pagination": {}},
            "/v1/prediction-markets/positions": lambda: DOCS["positions"],
            "/v1/prediction-markets/positions/settled": lambda: DOCS["settled"],
            "/v1/prediction-markets/terms/status": lambda: DOCS["terms_status"],
            "/v1/balances": lambda: DOCS["balances"],
        }
        if path in routes:
            return httpx.Response(200, json=routes[path]())
        return httpx.Response(404, json={"error": "no route"})

    def _cancel(self, req: httpx.Request) -> dict:
        ids = decoded(req)["orderIds"]
        return {"results": [{"orderId": i, "result": "ok"} for i in ids]}

    def private(self) -> list[httpx.Request]:
        return [r for r in self.requests if "X-GEMINI-SIGNATURE" in r.headers]


def trading(venue: Venue, **kwargs) -> GeminiTrading:
    from synpath.trading.limiter import BudgetLimiter

    return GeminiTrading(CREDS, client=httpx.AsyncClient(transport=httpx.MockTransport(venue)),
                         limiter=BudgetLimiter(read_per_second=1000, write_per_second=1000), **kwargs)


def run(coro):
    return asyncio.run(coro)


class TestAdapter:
    def test_create_order_sends_the_signed_payload_and_reads_back_the_order(self):
        venue = Venue()
        order = run(trading(venue, clock=lambda: 1776294447).create_order(request(client_order_id="mine")))
        [sent] = venue.private()
        assert sent.url.path == "/v1/prediction-markets/order" and sent.headers["X-GEMINI-PAYLOAD"] == ORDER_PAYLOAD
        assert sent.headers["X-GEMINI-SIGNATURE"] == ORDER_SIGNATURE
        assert order.id == "12345678901" and order.client_order_id == "mine" and order.market_id == FED
        assert order.status == OrderStatus.OPEN and order.side == Side.BUY

    def test_a_batch_comes_back_one_result_per_request(self):
        venue = Venue()
        results = run(trading(venue).create_orders([request(), request(side=Side.SELL), request(price=Decimal("0.505"))]))
        assert results[0].id == "12345678901"
        assert isinstance(results[1], InsufficientFunds)
        assert isinstance(results[2], InvalidOrder)
        [sent] = venue.private()
        assert [o["outcome"] for o in decoded(sent)["orders"]] == ["yes", "no"]

    def test_cancel_reads_the_order_back(self):
        venue = Venue()
        order = run(trading(venue).cancel_order("12345678901"))
        assert decoded(venue.private()[0])["orderId"] == 12345678901
        assert order.status == OrderStatus.CANCELED and order.filled == Decimal("25")

    def test_cancel_of_an_order_no_longer_listed(self):
        venue = Venue()
        order = run(trading(venue).cancel_order("42", market_id=FED))
        assert order.status == OrderStatus.CANCELED and order.id == "42" and order.market_id == FED

    def test_cancel_all_lists_then_cancels_in_batches(self):
        venue = Venue()
        assert run(trading(venue).cancel_all_orders(market_id=FED)) == 2
        listing, cancel = venue.private()
        assert decoded(listing)["symbol"] == "GEMI-FEDJAN26-DN25"
        assert decoded(cancel)["orderIds"] == [12345678901, 12345678902]

    def test_fetch_order_searches_the_active_orders_then_the_history(self):
        venue = Venue()
        assert run(trading(venue).fetch_order("777")).status == OrderStatus.CANCELED
        assert [r.url.path for r in venue.private()] == ["/v1/prediction-markets/orders/active",
                                                         "/v1/prediction-markets/orders/history"]
        with pytest.raises(OrderNotFound):
            run(trading(Venue()).fetch_order("1"))

    def test_positions_take_their_filters_as_query_parameters(self):
        venue = Venue()
        positions = run(trading(venue).fetch_positions(event_id="gemini:FEDJAN26"))
        [sent] = venue.private()
        assert sent.url.params["eventTicker"] == "FEDJAN26"
        assert set(decoded(sent)) == {"request", "nonce"} and decoded(sent)["request"] == "/v1/prediction-markets/positions"
        assert positions[0].side == PositionSide.SHORT

    def test_balance_settlements_and_terms(self):
        venue = Venue()
        adapter = trading(venue)
        assert run(adapter.fetch_balance()).available == Decimal("5000.00")
        assert [s.won for s in run(adapter.fetch_settlements())] == [True, False]
        assert run(adapter.fetch_terms_status())["hasAcceptedLatest"] is False
        assert venue.private()[-1].method == "GET"

    def test_history_pages_by_offset(self):
        venue = Venue()
        page = run(trading(venue).fetch_orders(status="cancelled", limit=1))
        assert page.next_cursor == "1" and decoded(venue.private()[0])["status"] == "cancelled"
        run(trading(venue).fetch_orders(cursor=page.next_cursor, limit=1))
        assert decoded(venue.private()[-1])["offset"] == 1

    def test_since_reads_a_time_window(self):
        venue = Venue()
        page = run(trading(venue).fetch_orders(since=1775001600000))
        fields = decoded(venue.private()[0])
        assert fields["from"] == 1775001600000 and fields["to"] > fields["from"] and page.next_cursor is None

    def test_the_fee_estimate_reads_the_public_contract(self):
        venue = Venue()
        estimate = run(trading(venue).fetch_fee_estimate(MIL, Side.BUY, Decimal("0.40"), Decimal("100")))
        assert estimate.taker_fee == Decimal("1.68") and not venue.private()

    def test_a_rate_limited_read_is_signed_again(self):
        venue = Venue()
        venue.rate_limited = 1
        clock = iter([1000, 1001])
        run(trading(venue, clock=lambda: next(clock)).fetch_balance())
        first, second = venue.private()
        assert decoded(first)["nonce"] == "1000" and decoded(second)["nonce"] == "1001"

    def test_a_rate_limited_write_is_not_resent(self):
        from synpath.errors import RateLimitExceeded

        venue = Venue()
        venue.rate_limited = 1
        with pytest.raises(RateLimitExceeded):
            run(trading(venue).create_order(request()))
        assert len(venue.private()) == 1

    def test_the_capabilities_say_what_the_venue_holds(self):
        assert GeminiTrading.has["create_order"] is True and GeminiTrading.has["edit_order"] is False
        assert GeminiTrading.has["fetch_my_trades"] == "partial" and GeminiTrading.has["rfq"] is False


class TestDoctor:
    def _env(self, monkeypatch, tmp_path, text: str):
        from synpath.trading.credentials import ENV_NAMES

        for names in ENV_NAMES.values():
            for name in names:
                monkeypatch.delenv(name, raising=False)
        monkeypatch.chdir(tmp_path)
        env = tmp_path / ".env"
        env.write_text(text)
        return env

    def test_without_a_key_market_data_still_works(self, monkeypatch, tmp_path, capsys):
        from synpath.trading.__main__ import doctor

        assert doctor(str(self._env(monkeypatch, tmp_path, ""))) == 0
        line = next(l for l in capsys.readouterr().out.splitlines() if l.startswith("gemini"))
        assert "no key (optional)" in line and "GEMINI_API_KEY" in line

    def test_with_a_key_it_names_the_key_and_prints_no_secret(self, monkeypatch, tmp_path, capsys):
        from synpath.trading.__main__ import doctor

        env = self._env(monkeypatch, tmp_path, f"GEMINI_API_KEY={KEY}\nGEMINI_API_SECRET={SECRET}\n")
        assert doctor(str(env)) == 0
        out = capsys.readouterr().out
        line = next(l for l in out.splitlines() if l.startswith("gemini"))
        assert "configured (prod)" in line and KEY[:12] in line and SECRET not in out
