"""Novig order entry: NOVIG-V3 signing against the venue's thirty published
vectors, the price grid, credentials, order bodies, REST records, and the
adapter against a scripted venue that checks every signature. Records
follow the venue's documented shapes (they need an account)."""
from __future__ import annotations

import asyncio
import base64
import json
import uuid
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from synpath.errors import AuthenticationError, MarketNotFound
from synpath.trading import novig_signing as sig
from synpath.trading.credentials import CredentialsMissing, NovigCredentials, load_credentials, load_novig
from synpath.trading.errors import (
    InsufficientFunds, InvalidOrder, MarketHalted, OrderNotFound, OrderRejected, PermissionDenied,
)
from synpath.trading.novig import (
    NovigTrading, build_order, client_id, error_of, fill_of_row, order_of_row, position_of,
)
from synpath.trading.types import (
    Liquidity, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce,
)

from conftest import load

VECTORS = load("novig_signing_vectors.json")
ED25519 = VECTORS["keypairs"]["ed25519-test-1"]
P256 = VECTORS["keypairs"]["ecdsa-p256-test-1"]
KEY_ID = "3b6f0c2e-1d2a-4f5e-9a8b-7c6d5e4f3a21"
MANAGEMENT_ID = "8f14e45f-ceea-467f-a0e6-0b2d4c1f9e10"

MARKET = load("novig_market.json")
MARKET_ID = MARKET["marketId"]
YES, NO = sorted(o["outcomeId"] for o in MARKET["outcomes"])

ORDER_ROW = {
    "orderId": "7c9e6679-7425-40de-944b-e07fc1f90ae7", "clientId": "0b3f5f20-9c1b-4d6e-8f3a-5c2d1e7b4a90",
    "marketId": MARKET_ID, "outcomeId": NO, "price": "0.375", "qty": 11000, "remaining": 7000, "tif": "GTC",
    "status": "OPEN", "createdTs": 1792700000000,
}
FILL_ROW = {
    "fillId": "1e6a2f94-0c8b-4e1d-9f6a-2b7c3d4e5f60", "orderId": ORDER_ROW["orderId"], "marketId": MARKET_ID,
    "outcomeId": YES, "qty": 4000, "cost": "25.00000", "fee": "0.56250", "taker": True, "ts": 1792700001000,
}


def credentials(**changes) -> NovigCredentials:
    base = {"key_id": KEY_ID, "private_key_pem": ED25519["private_key_pkcs8_pem"].encode(), "env": "paper"}
    return NovigCredentials(**{**base, **changes})


def verified(request: httpx.Request, public_pem: str = ED25519["public_key_spki_pem"]) -> bool:
    """Whether a request carries a valid NOVIG-V3 signature over what it sends."""
    path, _, query = request.url.raw_path.decode().partition("?")
    stamp = int(request.headers["Novig-Timestamp"])
    message = sig.string_to_sign(stamp, request.method, path, query, request.content).encode()
    key = serialization.load_pem_public_key(public_pem.encode())
    signature = base64.b64decode(request.headers["Novig-Signature"])
    try:
        if isinstance(key, ec.EllipticCurvePublicKey):
            key.verify(signature, message, ec.ECDSA(hashes.SHA256()))
        else:
            key.verify(signature, message)
    except Exception:
        return False
    return True


class TestSigning:
    @pytest.mark.parametrize("vector", VECTORS["vectors"], ids=lambda v: v["id"])
    def test_the_canonical_string_of_every_vector(self, vector):
        given = vector["input"]
        assert sig.string_to_sign(given["timestamp"], given["method"], given["path"], given["query"],
                                  given["body"].encode()) == vector["string_to_sign"]

    @pytest.mark.parametrize("vector", [v for v in VECTORS["vectors"] if v["algorithm"] == "ed25519"],
                             ids=lambda v: v["id"])
    def test_ed25519_signatures_byte_for_byte(self, vector):
        signer = sig.NovigSigner(KEY_ID, ED25519["private_key_pkcs8_pem"].encode())
        assert signer.algorithm == "Ed25519"
        assert signer.sign(vector["string_to_sign"]) == vector["signature"]

    @pytest.mark.parametrize("vector", [v for v in VECTORS["vectors"] if v["algorithm"] != "ed25519"],
                             ids=lambda v: v["id"])
    def test_p256_signatures_are_der_and_verify(self, vector):
        signer = sig.NovigSigner(KEY_ID, P256["private_key_pkcs8_pem"].encode())
        public = serialization.load_pem_public_key(P256["public_key_spki_pem"].encode())
        mine = base64.b64decode(signer.sign(vector["string_to_sign"]))
        assert signer.algorithm == "P-256" and mine[0] == 0x30 and 68 <= len(mine) <= 72
        public.verify(mine, vector["string_to_sign"].encode(), ec.ECDSA(hashes.SHA256()))
        public.verify(base64.b64decode(vector["signature"]), vector["string_to_sign"].encode(),
                      ec.ECDSA(hashes.SHA256()))

    def test_the_empty_body_hash(self):
        assert sig.string_to_sign(1, "get", "/v3/keys").endswith("\n\n" + sig.EMPTY_SHA256)
        assert sig.EMPTY_SHA256.startswith("e3b0c442")

    def test_fingerprints(self):
        for pair in (ED25519, P256):
            assert "sha256:" + sig.fingerprint(pair["public_key_spki_pem"]) == pair["public_key_fingerprint"]

    @pytest.mark.parametrize("algorithm", ["Ed25519", "P-256"])
    def test_a_fresh_keypair_signs(self, algorithm):
        private, public = sig.generate_key(algorithm)
        signer = sig.NovigSigner(KEY_ID, private)
        assert signer.algorithm == algorithm and signer.public_key_pem() == public
        request = httpx.Request("GET", "https://api.paper.novig.com/v3/time",
                                headers=signer.headers("GET", "/v3/time"))
        assert verified(request, public)

    def test_other_keys_are_refused(self):
        from cryptography.hazmat.primitives.asymmetric import rsa

        pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        with pytest.raises(ValueError, match="Ed25519 or P-256"):
            sig.NovigSigner(KEY_ID, pem)

    def test_the_auth_signs_what_is_sent(self):
        seen: list[httpx.Request] = []

        def venue(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={})

        signer = sig.NovigSigner(KEY_ID, ED25519["private_key_pkcs8_pem"].encode())
        with httpx.Client(transport=httpx.MockTransport(venue), auth=sig.NovigAuth(signer)) as client:
            client.post("https://api.paper.novig.com/v3/orders/batch", params={"q": "New York", "a": "1"},
                        json={"orders": [{"price": "0.665"}]})
            client.get("https://api.paper.novig.com/v3/keys/")
        assert all(verified(r) for r in seen)
        assert seen[0].headers["Novig-Key-Id"] == KEY_ID and seen[0].headers["content-type"] == "application/json"


class TestGrid:
    @pytest.mark.parametrize("price,ok", [
        ("0.001", True), ("0.050", True), ("0.051", False), ("0.055", True), ("0.665", True), ("0.667", False),
        ("0.945", True), ("0.946", False), ("0.950", True), ("0.999", True), ("0.0005", False), ("1", False),
    ])
    def test_on_grid(self, price, ok):
        assert sig.on_grid(Decimal(price)) is ok

    @pytest.mark.parametrize("price,snapped", [
        ("0.667", "0.665"), ("0.6649", "0.660"), ("0.0505", "0.050"), ("0.054", "0.050"), ("0.9505", "0.950"),
        ("0.9999", "0.999"), ("0.5", "0.500"),
    ])
    def test_snap_down(self, price, snapped):
        assert sig.snap_down(Decimal(price)) == Decimal(snapped) and sig.on_grid(Decimal(snapped))

    def test_every_complement_is_on_the_grid(self):
        grid = [Decimal(m) / 1000 for m in range(1, 1000) if sig.on_grid(Decimal(m) / 1000)]
        assert len(grid) == 279 and all(sig.on_grid(1 - p) for p in grid)

    def test_contracts_are_cents(self):
        assert sig.qty_of(Decimal("1.1")) == 110 and sig.contracts_of(110) == Decimal("1.1")
        for bad in ("0.005", "0", "1.234"):
            with pytest.raises(InvalidOrder):
                sig.qty_of(Decimal(bad))


class TestCredentials:
    def write(self, tmp_path: Path, pem: str, name: str = "desk-1.pem") -> str:
        path = tmp_path / name
        path.write_text(pem)
        return str(path)

    def test_a_trading_key(self, tmp_path):
        path = self.write(tmp_path, ED25519["private_key_pkcs8_pem"])
        creds = load_novig({"NOVIG_KEY_ID": KEY_ID, "NOVIG_PRIVATE_KEY_PATH": path, "NOVIG_ENV": "paper"})
        assert creds.key_id == KEY_ID and creds.env == "paper" and creds.management_key_id is None
        assert "PRIVATE" not in repr(creds) and creds.secrets

    def test_production_is_the_default(self, tmp_path):
        path = self.write(tmp_path, P256["private_key_pkcs8_pem"])
        assert load_novig({"NOVIG_KEY_ID": KEY_ID, "NOVIG_PRIVATE_KEY_PATH": path}).env == "production"

    def test_a_management_key_alone(self, tmp_path):
        path = self.write(tmp_path, ED25519["private_key_pkcs8_pem"], "novig-api-key-mgmt-1.pem")
        creds = load_novig({"NOVIG_MANAGEMENT_KEY_ID": MANAGEMENT_ID, "NOVIG_MANAGEMENT_PRIVATE_KEY_PATH": path})
        assert creds.key_id is None and creds.management_key_id == MANAGEMENT_ID

    def test_nothing_set_is_not_configured(self):
        assert load_novig({}) is None
        assert load_credentials({}, redact_logs=False, dotenv="/nonexistent")["novig"] is None

    @pytest.mark.parametrize("env,match", [
        ({"NOVIG_KEY_ID": KEY_ID}, "NOVIG_PRIVATE_KEY_PATH"),
        ({"NOVIG_MANAGEMENT_KEY_ID": MANAGEMENT_ID}, "NOVIG_MANAGEMENT_PRIVATE_KEY_PATH"),
    ])
    def test_a_key_id_needs_its_file(self, env, match):
        with pytest.raises(CredentialsMissing, match=match):
            load_novig(env)

    def test_a_bad_environment(self, tmp_path):
        path = self.write(tmp_path, ED25519["private_key_pkcs8_pem"])
        with pytest.raises(CredentialsMissing, match="production or paper"):
            load_novig({"NOVIG_KEY_ID": KEY_ID, "NOVIG_PRIVATE_KEY_PATH": path, "NOVIG_ENV": "staging"})

    def test_a_file_that_is_not_a_novig_key(self, tmp_path):
        from cryptography.hazmat.primitives.asymmetric import rsa

        pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
        path = self.write(tmp_path, pem)
        with pytest.raises(CredentialsMissing, match="Ed25519 or P-256"):
            load_novig({"NOVIG_KEY_ID": KEY_ID, "NOVIG_PRIVATE_KEY_PATH": path})


def request(**changes) -> OrderRequest:
    base = {"market_id": f"novig:{MARKET_ID}", "side": Side.BUY, "quantity": Decimal("10"), "price": Decimal("0.6")}
    return OrderRequest(**{**base, **changes})


def body(req: OrderRequest, **kwargs):
    return build_order(req, yes_outcome=YES, no_outcome=NO, **kwargs)


class TestOrders:
    def test_a_buy_buys_yes_in_cents(self):
        assert body(request()) == {"outcomeId": YES, "price": "0.600", "qty": 1000, "tif": "GTC"}

    def test_a_sell_buys_no_at_one_minus_the_price(self):
        assert body(request(side=Side.SELL)) == {"outcomeId": NO, "price": "0.400", "qty": 1000, "tif": "GTC"}

    def test_prices_snap_to_the_cheaper_side(self):
        assert body(request(price=Decimal("0.6013")))["price"] == "0.600"       # pays no more than 0.6013
        assert body(request(side=Side.SELL, price=Decimal("0.6013")))["price"] == "0.395"  # NO for <= 0.3987

    def test_time_in_force(self):
        assert body(request(type=OrderType.MARKET))["tif"] == "IOC"
        assert body(request(time_in_force=TimeInForce.IOC))["tif"] == "IOC"
        assert body(request(time_in_force=TimeInForce.FOK))["tif"] == "FOK"
        assert body(request(post_only=True))["tif"] == "PO"
        gtd = body(request(time_in_force=TimeInForce.GTD, expires_at=1_800_000_060_000), now_ms=1_800_000_000_000)
        assert (gtd["tif"], gtd["ttl"]) == ("GTT", 60_000)
        po = body(request(post_only=True, time_in_force=TimeInForce.GTD, expires_at=1_800_000_060_000),
                  now_ms=1_800_000_000_000)
        assert (po["tif"], po["ttl"]) == ("PO", 60_000)

    def test_client_ids_are_uuids(self):
        same = str(uuid.uuid4())
        assert body(request(client_order_id=same))["clientId"] == same
        derived = body(request(client_order_id="strategy-7/order-1"))["clientId"]
        assert uuid.UUID(derived) and derived == client_id("strategy-7/order-1")

    @pytest.mark.parametrize("changes,match", [
        ({"reduce_only": True}, "reduce-only"),
        ({"time_in_force": TimeInForce.DAY}, "day orders"),
        ({"post_only": True, "time_in_force": TimeInForce.IOC}, "post-only"),
        ({"quantity": Decimal("0.005")}, "1-cent"),
        ({"type": OrderType.STOP_MARKET, "stop_price": Decimal("0.5")}, "engine"),
        ({"time_in_force": TimeInForce.GTD, "expires_at": 1}, "past"),
    ])
    def test_refused_before_sending(self, changes, match):
        with pytest.raises(InvalidOrder, match=match):
            body(request(**changes))


class TestRecords:
    def test_an_order_on_no_is_a_sell_at_the_yes_price(self):
        order = order_of_row(ORDER_ROW, yes_outcome=YES)
        assert order.side == Side.SELL and order.price == Decimal("0.625")
        assert (order.quantity, order.filled, order.remaining) == (Decimal("110"), Decimal("40"), Decimal("70"))
        assert order.status == OrderStatus.OPEN and order.market_id == f"novig:{MARKET_ID}"
        assert order.client_order_id == ORDER_ROW["clientId"] and order.created_at == ORDER_ROW["createdTs"]

    @pytest.mark.parametrize("native,status", [
        ("PENDING", OrderStatus.PENDING), ("FILLED", OrderStatus.CLOSED), ("CANCELED", OrderStatus.CANCELED),
        ("REJECTED", OrderStatus.REJECTED),
    ])
    def test_order_states(self, native, status):
        order = order_of_row({**ORDER_ROW, "status": native}, yes_outcome=YES)
        assert order.status == status
        if status == OrderStatus.CLOSED:
            assert order.remaining == 0 and order.filled == order.quantity

    def test_post_only_and_good_til_time(self):
        assert order_of_row({**ORDER_ROW, "tif": "PO"}, yes_outcome=YES).post_only
        gtt = order_of_row({**ORDER_ROW, "tif": "GTT", "expiresTs": 1792800000000}, yes_outcome=YES)
        assert gtt.time_in_force == TimeInForce.GTD and gtt.expires_at == 1792800000000

    def test_a_resting_order_from_the_snapshot(self):
        row = {k: v for k, v in ORDER_ROW.items() if k not in ("remaining", "status", "createdTs")}
        order = order_of_row(row, yes_outcome=YES)
        assert order.status == OrderStatus.OPEN and order.remaining == order.quantity == Decimal("110")

    def test_a_fill_from_rest_prices_from_its_cost(self):
        fill = fill_of_row(FILL_ROW, yes_outcome=YES)
        assert fill.side == Side.BUY and fill.price == Decimal("0.625") and fill.quantity == Decimal("40")
        assert fill.fee == Decimal("0.56250") and fill.liquidity == Liquidity.TAKER and fill.fee_currency == "USD"

    def test_an_uncharged_fill_paid_nothing(self):
        fill = fill_of_row({k: v for k, v in FILL_ROW.items() if k != "fee"} | {"taker": False}, yes_outcome=YES)
        assert fill.fee == 0 and fill.liquidity == Liquidity.MAKER

    def test_a_fill_from_the_stream_carries_its_price_and_no_fee(self):
        event = {"kind": "fill", "fillId": "f-1", "orderId": "o-1", "outcomeId": NO, "price": "0.370", "qty": 40,
                 "remaining": 70, "taker": True}
        fill = fill_of_row(event, yes_outcome=YES, market_id=MARKET_ID)
        assert fill.side == Side.SELL and fill.price == Decimal("0.630") and fill.fee is None
        assert fill.market_id == f"novig:{MARKET_ID}" and fill.quantity == Decimal("0.4")

    def test_a_position_nets_both_outcomes(self):
        rows = [{"marketId": MARKET_ID, "outcomeId": YES, "qty": 11000, "cost": "68.75000"}]
        long = position_of(MARKET_ID, rows, yes_outcome=YES)
        assert long.side == PositionSide.LONG and long.contracts == Decimal("110")
        assert long.entry_price == Decimal("0.625") and long.inventory_no == 0
        rows.append({"marketId": MARKET_ID, "outcomeId": NO, "qty": 20000, "cost": "75.00000"})
        short = position_of(MARKET_ID, rows, yes_outcome=YES)
        assert short.side == PositionSide.SHORT and short.contracts == Decimal("90") and short.entry_price is None
        assert (short.inventory_yes, short.inventory_no, short.margin) == (Decimal("110"), Decimal("200"), Decimal("143.75"))

    def test_a_no_position_has_a_yes_entry_price(self):
        rows = [{"marketId": MARKET_ID, "outcomeId": NO, "qty": 10000, "cost": "37.50000"}]
        assert position_of(MARKET_ID, rows, yes_outcome=YES).entry_price == Decimal("0.625")


class TestErrors:
    @staticmethod
    def refused(status: int, code: str):
        from synpath.errors import ExchangeError

        return error_of(ExchangeError("novig: refused", body={"code": code, "message": code.lower()}, status=status))

    @pytest.mark.parametrize("status,code,kind", [
        (422, "INSUFFICIENT_BALANCE", InsufficientFunds), (400, "INVALID_PRICE", InvalidOrder),
        (409, "MARKET_CLOSED", MarketHalted), (404, "ORDER_NOT_FOUND", OrderNotFound),
        (404, "OUTCOME_NOT_FOUND", MarketNotFound), (403, "KYC_REQUIRED", PermissionDenied),
        (403, "SCOPE_INSUFFICIENT", PermissionDenied), (423, "SELF_EXCLUDED", PermissionDenied),
        (400, "BATCH_REJECTED", OrderRejected), (401, "SIGNATURE_REJECTED", AuthenticationError),
    ])
    def test_codes(self, status, code, kind):
        assert isinstance(self.refused(status, code), kind)

    def test_geolocation_says_why(self):
        error = self.refused(451, "GEOLOCATION_OUT_OF_BOUNDARY")
        assert isinstance(error, PermissionDenied) and "Nevada" in str(error)


class Venue:
    """Answers signed requests by `METHOD /path`, checks each signature, and records them."""

    def __init__(self, **answers):
        self.answers = answers
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        if not verified(req):
            return httpx.Response(401, json={"code": "SIGNATURE_REJECTED", "message": "signature verification failed"})
        answer = self.answers.get(f"{req.method} {req.url.path}")
        if answer is None:
            return httpx.Response(404, json={"code": "NOT_FOUND", "message": "not found"})
        if callable(answer):
            answer = answer(req)
        return answer if isinstance(answer, httpx.Response) else httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


class Catalog:
    def __init__(self):
        self.asked: list[str] = []

    def outcome_ids(self, market_id):
        self.asked.append(market_id)
        if market_id != MARKET_ID:
            raise MarketNotFound(f"novig: no open market {market_id}")
        return YES, NO

    def fetch_fee_schedule(self, market_id):
        from synpath.novig import fee_schedule_of

        return fee_schedule_of(MARKET)

    def close(self):
        pass


def adapter(venue: Venue, **creds) -> NovigTrading:
    transport = httpx.MockTransport(venue)
    return NovigTrading(credentials(**creds), catalog=Catalog(), client=httpx.AsyncClient(transport=transport),
                        management_client=httpx.AsyncClient(transport=transport))


def run(coroutine):
    return asyncio.run(coroutine)


class TestAdapter:
    def test_paper_and_production_hosts(self):
        assert NovigTrading(credentials(), catalog=Catalog()).url == "https://api.paper.novig.com"
        assert NovigTrading(credentials(env="production"), catalog=Catalog()).url == "https://api.novig.com"

    def test_create_order_sends_a_signed_placement(self):
        venue = Venue(**{"POST /v3/orders": httpx.Response(201, json={"orderId": "o-1"})})
        order = run(adapter(venue).create_order(request(side=Side.SELL, client_order_id="c-1")))
        sent = json.loads(venue.sent("POST", "/v3/orders")[0].content)
        assert sent == {"outcomeId": NO, "price": "0.400", "qty": 1000, "tif": "GTC", "clientId": client_id("c-1")}
        assert order.id == "o-1" and order.status == OrderStatus.PENDING and order.side == Side.SELL
        assert order.price == Decimal("0.6") and order.quantity == Decimal("10") and order.client_order_id == "c-1"

    def test_an_unknown_market_is_refused_before_sending(self):
        venue = Venue()
        with pytest.raises(MarketNotFound):
            run(adapter(venue).create_order(request(market_id="novig:00000000-0000-0000-0000-000000000000")))
        assert venue.requests == []

    def test_an_unknown_outcome_is_not_an_unknown_order(self):
        venue = Venue(**{"POST /v3/orders": httpx.Response(
            404, json={"code": "OUTCOME_NOT_FOUND", "message": "Outcome not found"})})
        with pytest.raises(MarketNotFound) as raised:
            run(adapter(venue).create_order(request()))
        assert not isinstance(raised.value, OrderNotFound)

    def test_the_venues_refusal_is_typed(self):
        venue = Venue(**{"POST /v3/orders": httpx.Response(
            422, json={"code": "INSUFFICIENT_BALANCE", "message": "Insufficient balance"})})
        with pytest.raises(InsufficientFunds):
            run(adapter(venue).create_order(request()))

    def test_a_batch_places_in_one_request(self):
        venue = Venue(**{"POST /v3/orders/batch": httpx.Response(201, json=[{"orderId": "a"}, {"orderId": "b"}])})
        results = run(adapter(venue).create_orders([request(), request(quantity=Decimal("0.001")), request(side=Side.SELL)]))
        assert [r.id if not isinstance(r, Exception) else type(r) for r in results] == ["a", InvalidOrder, "b"]
        assert len(json.loads(venue.sent("POST", "/v3/orders/batch")[0].content)["orders"]) == 2

    def test_a_refused_batch_places_nothing(self):
        refusal = {"code": "BATCH_REJECTED", "message": "1 order(s) refused.",
                   "rejected": [{"index": 1, "outcomeId": NO, "reason": "Invalid price"}]}
        venue = Venue(**{"POST /v3/orders/batch": httpx.Response(400, json=refusal)})
        first, second = run(adapter(venue).create_orders([request(), request(side=Side.SELL)]))
        assert isinstance(first, OrderRejected) and "as a whole" in str(first)
        assert isinstance(second, InvalidOrder) and "Invalid price" in str(second)

    def test_cancel_reads_the_order_back(self):
        venue = Venue(**{
            f"DELETE /v3/orders/{ORDER_ROW['orderId']}": {"orderId": ORDER_ROW["orderId"], "status": "OPEN"},
            f"GET /v3/orders/{ORDER_ROW['orderId']}": ORDER_ROW,
        })
        order = run(adapter(venue).cancel_order(ORDER_ROW["orderId"]))
        assert order.status == OrderStatus.PENDING_CANCEL and order.side == Side.SELL

    def test_an_unknown_order(self):
        venue = Venue(**{"GET /v3/orders/nope": httpx.Response(404, json={"code": "ORDER_NOT_FOUND", "message": "x"})})
        with pytest.raises(OrderNotFound):
            run(adapter(venue).fetch_order("nope"))

    def test_batch_cancel_is_partial(self):
        canceled = {**ORDER_ROW, "status": "CANCELED"}
        venue = Venue(**{
            "DELETE /v3/orders/batch": httpx.Response(207, json={
                "canceled": [ORDER_ROW["orderId"]], "notCanceled": [{"orderId": "gone", "reason": "NOT_FOUND"}]}),
            f"GET /v3/orders/{ORDER_ROW['orderId']}": canceled,
        })
        done, missing = run(adapter(venue).cancel_orders([ORDER_ROW["orderId"], "gone"]))
        assert done.status == OrderStatus.CANCELED and isinstance(missing, OrderNotFound)
        assert json.loads(venue.sent("DELETE", "/v3/orders/batch")[0].content) == {"orderIds": [ORDER_ROW["orderId"], "gone"]}

    def test_a_batch_cancel_of_only_unknown_ids(self):
        venue = Venue(**{"DELETE /v3/orders/batch": httpx.Response(404, json={
            "canceled": [], "notCanceled": [{"orderId": "gone", "reason": "NOT_FOUND"}]})})
        (missing,) = run(adapter(venue).cancel_orders(["gone"]))
        assert isinstance(missing, OrderNotFound)

    def test_cancel_all_on_a_market(self):
        venue = Venue(**{"DELETE /v3/orders": {"canceled": 3}})
        assert run(adapter(venue).cancel_all_orders(market_id=f"novig:{MARKET_ID}")) == 3
        assert venue.sent("DELETE", "/v3/orders")[0].url.params["market"] == MARKET_ID

    def test_open_orders_page_by_page(self):
        pages = {None: {"items": [ORDER_ROW], "next": "p2"}, "p2": {"items": [{**ORDER_ROW, "orderId": "o-2"}]}}
        venue = Venue(**{"GET /v3/orders": lambda req: pages[req.url.params.get("after")]})
        orders = run(adapter(venue).fetch_open_orders(market_id=f"novig:{MARKET_ID}"))
        assert [o.id for o in orders] == [ORDER_ROW["orderId"], "o-2"]
        assert all(r.url.params["status"] == "OPEN" and r.url.params["market"] == MARKET_ID for r in venue.requests)

    def test_orders_by_state(self):
        venue = Venue(**{"GET /v3/orders": {"items": [{**ORDER_ROW, "status": "FILLED"}], "next": "n"}})
        page = run(adapter(venue).fetch_orders(status="closed", limit=10))
        assert page[0].status == OrderStatus.CLOSED and page.next_cursor == "n"
        assert venue.requests[0].url.params["status"] == "FILLED" and venue.requests[0].url.params["limit"] == "10"

    def test_fills(self):
        venue = Venue(**{"GET /v3/portfolio/fills": {"items": [FILL_ROW], "next": None}})
        page = run(adapter(venue).fetch_my_trades(since=1792700000000, limit=5))
        assert page[0].id == FILL_ROW["fillId"] and page[0].fee == Decimal("0.56250")
        assert venue.requests[0].url.params["startsAfter"] == "1792699999999"

    def test_positions(self):
        venue = Venue(**{"GET /v3/portfolio/positions": [
            {"marketId": MARKET_ID, "outcomeId": YES, "qty": 11000, "cost": "68.75000"}]})
        (position,) = run(adapter(venue).fetch_positions())
        assert position.side == PositionSide.LONG and position.contracts == Decimal("110")

    def test_balance_is_the_subaccounts(self):
        venue = Venue(**{f"GET /v3/account/subaccounts/{KEY_ID}/balance": {"keyId": KEY_ID, "balance": "1234.50000"}})
        balance = run(adapter(venue).fetch_balance())
        assert balance.total == balance.available == Decimal("1234.50000") and balance.locked is None
        assert balance.currency == "USD" and balance.account.subaccount == KEY_ID

    def test_fee_estimate(self):
        estimate = run(adapter(Venue()).fetch_fee_estimate(f"novig:{MARKET_ID}", Side.BUY, Decimal("0.5"), Decimal("100")))
        assert estimate.taker_fee == Decimal("1.5") and estimate.maker_fee == Decimal("-0.375")

    def test_a_location_refusal(self):
        venue = Venue(**{"POST /v3/orders": httpx.Response(451, json={
            "code": "GEOLOCATION_OUT_OF_BOUNDARY", "message": "outside the boundary"})})
        with pytest.raises(PermissionDenied, match="geolocation"):
            run(adapter(venue).create_order(request()))

    def test_subaccounts_need_the_management_key(self):
        with pytest.raises(CredentialsMissing, match="NOVIG_MANAGEMENT_KEY_ID"):
            run(adapter(Venue()).subaccounts())

    def test_trading_needs_the_trading_key(self):
        creds = {"key_id": None, "private_key_pem": None, "management_key_id": MANAGEMENT_ID,
                 "management_private_key_pem": ED25519["private_key_pkcs8_pem"].encode()}
        with pytest.raises(CredentialsMissing, match="NOVIG_KEY_ID"):
            run(adapter(Venue(), **creds).fetch_balance())

    def test_funding_a_subaccount(self):
        creds = {"management_key_id": MANAGEMENT_ID, "management_private_key_pem": ED25519["private_key_pkcs8_pem"].encode()}
        venue = Venue(**{f"POST /v3/account/subaccounts/{KEY_ID}/transfer": httpx.Response(202, json={
            "transferId": "t-1", "status": "Requested"})})
        answer = run(adapter(venue, **creds).fund_subaccount(KEY_ID, Decimal("250"), client_transfer_id="desk-1-a"))
        sent = venue.sent("POST", f"/v3/account/subaccounts/{KEY_ID}/transfer")[0]
        assert json.loads(sent.content) == {"direction": "fund", "amount": "250.00000", "clientTransferId": "desk-1-a"}
        assert sent.headers["Novig-Key-Id"] == MANAGEMENT_ID and answer["status"] == "Requested"
        with pytest.raises(Exception, match="five decimal"):
            run(adapter(venue, **creds).fund_subaccount(KEY_ID, Decimal("1.000001")))

    def test_opening_a_subaccount_sends_only_the_public_key(self):
        creds = {"management_key_id": MANAGEMENT_ID, "management_private_key_pem": ED25519["private_key_pkcs8_pem"].encode()}
        venue = Venue(**{"POST /v3/account/subaccounts": httpx.Response(201, json={
            "keyId": "k-2", "label": "desk-2", "balance": "0.00000"})})
        private, public = sig.generate_key()
        answer = run(adapter(venue, **creds).open_subaccount("desk-2", public))
        sent = json.loads(venue.sent("POST", "/v3/account/subaccounts")[0].content)
        assert sent == {"label": "desk-2", "publicKey": public, "algorithm": "Ed25519"} and answer["keyId"] == "k-2"
        assert private.decode() not in json.dumps(sent)

    def test_it_is_the_clients_novig_adapter(self):
        import synpath

        client = synpath.Client({"novig": credentials()})
        assert isinstance(client.trading("novig"), NovigTrading)
        assert NovigTrading.has["create_orders"] and not NovigTrading.has["rfq"]


class TestSetup:
    def test_init_writes_a_trading_key(self, tmp_path):
        from synpath.trading.init import Prompter, run
        from synpath.trading.credentials import read_dotenv

        pem = tmp_path / "desk-1.pem"
        pem.write_text(ED25519["private_key_pkcs8_pem"])
        answers = iter(["n"] * 10 + ["y", "paper", KEY_ID, str(pem), ""])
        p = Prompter(ask=lambda _q: next(answers), ask_secret=lambda _q: "", say=lambda _: None)
        env = tmp_path / ".env"
        assert run(env, p) == 0
        assert read_dotenv(env) == {"NOVIG_ENV": "paper", "NOVIG_KEY_ID": KEY_ID, "NOVIG_PRIVATE_KEY_PATH": str(pem.resolve())}
        loaded = load_credentials({}, dotenv=env, redact_logs=False)["novig"]
        assert loaded is not None and loaded.key_id == KEY_ID and loaded.env == "paper"

    def test_doctor_names_the_keys_and_prints_no_secret(self, tmp_path, monkeypatch, capsys):
        from synpath.trading.__main__ import doctor

        pem = tmp_path / "desk-1.pem"
        pem.write_text(ED25519["private_key_pkcs8_pem"])
        env = tmp_path / ".env"
        env.write_text(f"NOVIG_KEY_ID={KEY_ID}\nNOVIG_PRIVATE_KEY_PATH={pem}\nNOVIG_ENV=paper\n")
        for name in ("NOVIG_KEY_ID", "NOVIG_PRIVATE_KEY_PATH", "NOVIG_ENV", "NOVIG_MANAGEMENT_KEY_ID"):
            monkeypatch.delenv(name, raising=False)
        assert doctor(str(env)) == 0
        line = next(row for row in capsys.readouterr().out.splitlines() if row.startswith("novig"))
        assert "configured (paper)" in line and f"trading key {KEY_ID}" in line
        assert "MC4CAQAw" not in line

    def test_the_engine_streams_a_keyed_venue(self):
        from synpath.engine.feeds import default_streams
        from synpath.ws.novig import NovigMarketStream, NovigUserStream

        trading = NovigTrading(credentials(), catalog=Catalog())
        streams = default_streams({"novig": trading}, {"novig": credentials()})["novig"]
        assert isinstance(streams.market, NovigMarketStream) and not streams.market.polling
        assert isinstance(streams.private, NovigUserStream) and streams.private.trading is trading
        managed = credentials(key_id=None, private_key_pem=None, management_key_id=MANAGEMENT_ID,
                              management_private_key_pem=ED25519["private_key_pkcs8_pem"].encode())
        streams = default_streams({"novig": trading}, {"novig": managed})["novig"]
        assert streams.market.polling and streams.private is None
