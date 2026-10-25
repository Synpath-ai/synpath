"""Myriad order entry: signing checked against an independent EIP-712 encoder
and fixed known answers, request signing against the reference's HMAC recipe,
order bodies, REST records, and the adapter against a scripted venue."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from decimal import Decimal

import httpx
import pytest
from eth_account import Account as EthAccount
from eth_account.messages import _hash_eip191_message, encode_defunct, encode_typed_data

from synpath.myriad import Ref, fee_schedule_of
from synpath.trading import myriad_signing as sig
from synpath.trading.credentials import CredentialsMissing, MyriadCredentials, load_myriad
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.myriad import (
    MyriadTrading, build_signed_order, fill_of_action, fills_of_trade, order_of_record, order_of_update,
    positions_of, time_in_force_of,
)
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.types import (
    EditRequest, Liquidity, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce,
)

from conftest import load

KEY = "0x" + "11" * 32
OWNER = EthAccount.from_key(KEY)
MARKET = load("myriad_market.json")
MARKET_ID = "myriad:56-3722"


def request(**changes) -> OrderRequest:
    base = {"market_id": MARKET_ID, "side": Side.BUY, "quantity": Decimal("10"), "price": Decimal("0.4")}
    return OrderRequest(**{**base, **changes})


def signed(req: OrderRequest):
    return build_signed_order(req, signer=WalletSigner(KEY), market=3722, nonce=7)


def recover(entry: dict) -> str:
    order = sig.order_of_wire(entry["order"])
    return EthAccount.recover_message(encode_typed_data(full_message=sig.order_typed_data(order)), signature=entry["signature"])


class TestSigning:
    ORDER = sig.build_order(trader=OWNER.address, market_id=2385, outcome=0, side=0, amount=10**19,
                            price=5 * 10**16, nonce=12345, expiration=0)

    def test_the_digest_is_eip712(self):
        encoded = encode_typed_data(full_message=sig.order_typed_data(self.ORDER))
        assert sig.order_digest(self.ORDER) == _hash_eip191_message(encoded)

    def test_known_answers(self):
        """Fixed key, fixed order: the hash and signature may never drift."""
        assert OWNER.address == "0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A"
        assert sig.order_hash(self.ORDER) == "0xa7a7d48560e9e757da4406970807265acfc4b087246a02c40ab5f23c37739a32"
        signature, digest = sig.sign_order(WalletSigner(KEY), self.ORDER)
        assert digest == sig.order_hash(self.ORDER)
        assert signature == ("0x7291634179ef86834be486a8972f329a31e3b86590a5f7b7c31667f7e3256ee97c13b535a3f0d41c9ed32"
                             "b6b077d900671647223ab277d133338b4805c9dea3d1c")
        assert sig.cancel_all_digest(OWNER.address, 0, 1792772000).hex() == \
            "b51cd34b14c66ed047fa4beebaebd18818a78f31b968019c6e04da476fbf7b74"

    def test_the_signature_is_the_wallets(self):
        signature, _ = sig.sign_order(WalletSigner(KEY), self.ORDER)
        encoded = encode_typed_data(full_message=sig.order_typed_data(self.ORDER))
        assert signature == "0x" + OWNER.sign_message(encoded).signature.hex().removeprefix("0x")

    def test_cancel_all_is_signed_typed_data(self):
        body = sig.sign_cancel_all(WalletSigner(KEY), 42, timestamp=1792772000)
        typed = sig.cancel_all_typed_data(OWNER.address, 42, 1792772000)
        assert EthAccount.recover_message(encode_typed_data(full_message=typed), signature=body["signature"]) == OWNER.address
        assert body["market_id"] == 42 and body["timestamp"] == "1792772000" and body["network_id"] == 56
        assert "market_id" not in sig.sign_cancel_all(WalletSigner(KEY), timestamp=1), "0 is every market"

    def test_the_sign_in_is_a_personal_message(self):
        message = "myriad.markets wants you to sign in with your Ethereum account"
        assert EthAccount.recover_message(encode_defunct(text=message),
                                          signature=sig.sign_login(WalletSigner(KEY), message)) == OWNER.address

    def test_requests_are_signed_as_the_reference_says(self):
        body = '{"a":1}'
        headers = sig.request_headers("key-1", "secret-1", "post", "/orders?x=1", body, now=1792772000)
        expected = hmac.new(b"secret-1", b'1792772000.POST./orders?x=1.{"a":1}', hashlib.sha256).hexdigest()
        assert headers == {"x-api-key": "key-1", "x-api-timestamp": "1792772000", "x-api-signature": expected}
        assert sig.request_signature("s", "1", "GET", "/orders") == hmac.new(b"s", b"1.GET./orders.", hashlib.sha256).hexdigest()

    def test_prices_are_whole_cents(self):
        assert sig.price_wei(Decimal("0.57")) == 570000000000000000
        with pytest.raises(InvalidOrder, match="cent"):
            sig.price_wei(Decimal("0.575"))
        with pytest.raises(InvalidOrder):
            sig.price_wei(Decimal("1"))

    def test_approvals_name_the_exchange(self):
        calls = sig.approval_calls()
        assert [c["to"] for c in calls] == [sig.USD1, sig.CONDITIONAL_TOKENS]
        assert calls[0]["data"].startswith("0x095ea7b3") and sig.EXCHANGE[2:].lower() in calls[0]["data"]
        assert calls[1]["data"].startswith("0xa22cb465")


class TestOrderBody:
    def test_a_buy_buys_outcome_zero(self):
        entry, digest, contracts = signed(request())
        order = entry["order"]
        assert (order["outcomeId"], order["side"], order["price"], order["amount"]) == (0, 0, "400000000000000000",
                                                                                         "10000000000000000000")
        assert order["marketId"] == "3722" and order["expiration"] == "0" and order["nonce"] == "7"
        assert entry["time_in_force"] == "GTC" and contracts == Decimal("10") and recover(entry) == OWNER.address
        assert digest == sig.order_hash(sig.order_of_wire(order))

    def test_a_sell_buys_outcome_one_at_the_complement(self):
        order = signed(request(side=Side.SELL, price=Decimal("0.3")))[0]["order"]
        assert (order["outcomeId"], order["side"], order["price"]) == (1, 0, "700000000000000000")

    def test_reduce_only_sells_what_is_held(self):
        assert [signed(request(side=Side.SELL, reduce_only=True))[0]["order"][k] for k in ("outcomeId", "side")] == [0, 1]
        assert [signed(request(side=Side.BUY, reduce_only=True))[0]["order"][k] for k in ("outcomeId", "side")] == [1, 1]

    @pytest.mark.parametrize("changes, tif, expiration", [
        ({}, "GTC", 0),
        ({"time_in_force": TimeInForce.GTD, "expires_at": 1_900_000_000_000}, "GTD", 1_900_000_000),
        ({"time_in_force": TimeInForce.IOC}, "FAK", 0),
        ({"time_in_force": TimeInForce.FOK}, "FOK", 0),
        ({"type": OrderType.MARKET, "time_in_force": TimeInForce.IOC}, "FAK", 0),
        ({"post_only": True}, "PO", 0),
        ({"post_only": True, "time_in_force": TimeInForce.GTD, "expires_at": 1_900_000_000_000}, "PO", 1_900_000_000),
    ])
    def test_time_in_force(self, changes, tif, expiration):
        assert time_in_force_of(request(**changes)) == (tif, expiration)

    @pytest.mark.parametrize("changes, match", [
        ({"post_only": True, "time_in_force": TimeInForce.IOC}, "post-only"),
        ({"time_in_force": TimeInForce.DAY}, "day"),
        ({"type": OrderType.ICEBERG}, "engine"),
        ({"price": Decimal("0.405")}, "cent"),
    ])
    def test_refusals(self, changes, match):
        with pytest.raises(InvalidOrder, match=match):
            signed(request(**changes))


class TestRecords:
    RECORD = {
        "orderHash": "0xabc", "clientOrderId": "1ccd-17256719159",
        "order": {"trader": OWNER.address, "marketId": 42, "outcomeId": 1, "side": 0, "amount": "10000000000000000000",
                  "price": "300000000000000000", "minFillAmount": "0", "nonce": "1", "expiration": "0"},
        "status": "open", "signatureType": 0, "filledAmount": "4000000000000000000", "timeInForce": "GTC",
        "networkId": 56, "createdAt": "2025-07-01T12:00:00.000Z",
    }

    def test_an_order_on_outcome_one_reads_on_yes(self):
        order = order_of_record(self.RECORD)
        assert (order.side, order.price, order.quantity, order.filled, order.remaining) == (
            Side.SELL, Decimal("0.7"), Decimal("10"), Decimal("4"), Decimal("6"))
        assert order.market_id == "myriad:56-42" and order.status == OrderStatus.OPEN and not order.reduce_only
        assert order.client_order_id == "1ccd-17256719159" and order.created_at == 1751371200000
        assert order_of_record({**self.RECORD, "status": "filled"}).status == OrderStatus.CLOSED
        assert order_of_record({**self.RECORD, "timeInForce": "PO"}).post_only

    def test_an_order_update_from_the_stream(self):
        update = {"type": "PARTIALLY_FILLED", "orderHash": "0x1", "networkId": 56, "marketId": 42, "side": "buy",
                  "outcome": 0, "price": "500000000000000000", "amount": "1000000000000000000",
                  "filledAmount": "400000000000000000", "status": "open", "timeInForce": "GTC"}
        order = order_of_update(update)
        assert (order.side, order.price, order.filled, order.remaining) == (Side.BUY, Decimal("0.5"), Decimal("0.4"),
                                                                           Decimal("0.6"))
        assert order_of_update({**update, "type": "FILLED", "status": "filled"}).status == OrderStatus.CLOSED

    def test_this_accounts_legs_of_a_match_are_its_fills(self):
        trade = {"marketId": 42, "networkId": 56, "txHash": "0xt", "ts": 1719835200123,
                 "taker": {"trader": "0xTAKER", "outcome": 0, "side": "buy", "orderHash": "0xo1",
                           "totalAmount": "1000000000000000000", "averagePrice": "500000000000000000",
                           "totalFees": {"total": "10000000000000000"}},
                 "makers": [{"trader": OWNER.address, "role": "maker", "outcome": 0, "side": "sell", "orderHash": "0xo2",
                             "price": "500000000000000000", "amount": "1000000000000000000", "fees": {"total": "0"}}]}
        [fill] = fills_of_trade(trade, address=OWNER.address)
        assert (fill.side, fill.price, fill.quantity, fill.liquidity, fill.fee) == (
            Side.SELL, Decimal("0.5"), Decimal("1"), Liquidity.MAKER, Decimal("0"))
        assert fill.id == "0xt:0:sell" and fill.order_id == "0xo2" and fill.market_id == "myriad:56-42"
        [taker] = fills_of_trade(trade, address="0xtaker")
        assert taker.liquidity == Liquidity.TAKER and taker.fee == Decimal("0.01")

    def test_history_rows_are_fills_without_an_order(self):
        row = {"user": OWNER.address, "action": "buy", "marketId": 42, "networkId": 56, "outcomeId": 1,
               "shares": 10, "value": 3, "timestamp": 1719835200, "txId": "0xt"}
        fill = fill_of_action(row)
        assert (fill.side, fill.price, fill.quantity, fill.order_id) == (Side.SELL, Decimal("0.7"), Decimal("10"), "")
        assert fill.id == "0xt:1:buy" and fill_of_action({**row, "action": "claim_winnings"}) is None

    def test_positions_net_on_yes(self):
        rows = [
            {"marketId": 42, "networkId": 56, "executionMode": 1, "outcomeId": 0, "shares": 10, "price": 0.4,
             "value": 5, "profit": 1, "status": "ongoing"},
            {"marketId": 42, "networkId": 56, "executionMode": 1, "outcomeId": 1, "shares": 4, "price": 0.5,
             "value": 2, "profit": 0, "status": "ongoing"},
            {"marketId": 412, "networkId": 2741, "executionMode": 0, "outcomeId": 2, "shares": 3, "price": 0.1,
             "value": 0.6, "profit": 0.3, "status": "won", "winningsToClaim": True},
        ]
        binary, outcome = positions_of(rows)
        assert binary.market_id == "myriad:56-42" and binary.side == PositionSide.LONG and binary.contracts == 6
        assert (binary.inventory_yes, binary.inventory_no, binary.entry_price, binary.mark_price) == (
            Decimal("10"), Decimal("4"), Decimal("0.4"), Decimal("0.5"))
        assert outcome.market_id == "myriad:2741-amm-412-2" and outcome.inventory_no is None
        assert outcome.resolved and outcome.won and outcome.redeemable == Decimal("3")


class TestCredentials:
    def test_a_bare_key_is_refused(self):
        with pytest.raises(CredentialsMissing, match="SECRET"):
            load_myriad({"MYRIAD_API_KEY": "k"})
        with pytest.raises(CredentialsMissing):
            load_myriad({"MYRIAD_API_SECRET": "s"})

    def test_a_private_key_alone_signs_in(self):
        creds = load_myriad({"MYRIAD_PRIVATE_KEY": KEY[2:]})
        assert creds.private_key == KEY and creds.api_key is None and creds.address == OWNER.address
        assert KEY not in repr(creds) and creds.secrets == [KEY]

    def test_a_key_alone_reads_for_the_address_given(self):
        creds = load_myriad({"MYRIAD_API_KEY": "k", "MYRIAD_API_SECRET": "the-secret", "MYRIAD_ACCOUNT_ADDRESS": OWNER.address})
        assert creds.private_key is None and creds.address == OWNER.address and "the-secret" not in repr(creds)
        with pytest.raises(CredentialsMissing, match="not the address"):
            load_myriad({"MYRIAD_PRIVATE_KEY": KEY, "MYRIAD_ACCOUNT_ADDRESS": "0x" + "22" * 20})
        with pytest.raises(CredentialsMissing, match="private key"):
            load_myriad({"MYRIAD_API_KEY": "k", "MYRIAD_API_SECRET": "s", "MYRIAD_ACCOUNT_ADDRESS": KEY})
        assert load_myriad({}) is None


# ---------------------------------------------------------------------------
# The adapter against a scripted venue
# ---------------------------------------------------------------------------

class Catalog:
    def _resolve(self, market_id):
        return Ref(56, 3722), MARKET

    def fetch_fee_schedule(self, market_id):
        return fee_schedule_of(MARKET, market_id="56-3722")

    def close(self):
        pass


class Venue:
    """Answers the venue's routes, checks every keyed request's signature, and records them."""

    def __init__(self, keyed=True, **answers):
        self.keyed = keyed
        self.answers = answers
        self.requests: list[httpx.Request] = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        if self.keyed:
            target = req.url.raw_path.decode()
            expected = sig.request_signature("secret-1", req.headers["x-api-timestamp"], req.method, target,
                                             req.content.decode())
            assert req.headers["x-api-signature"] == expected and req.headers["x-api-key"] == "key-1"
        answer = self.answers.get(f"{req.method} {req.url.path}")
        if answer is None:
            return httpx.Response(404, json={"error": "Order not found"})
        return answer(req) if callable(answer) else httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == path]


def adapter(venue: Venue, *, keyed=True, rpc=None) -> MyriadTrading:
    creds = MyriadCredentials(private_key=KEY, api_key="key-1" if keyed else None, api_secret="secret-1" if keyed else None)
    return MyriadTrading(
        creds, catalog=Catalog(), client=httpx.AsyncClient(transport=httpx.MockTransport(venue)),
        rpc_client=rpc or httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))),
    )


def run(coro):
    return asyncio.run(coro)


def echo_order(status="open"):
    def answer(req):
        body = json.loads(req.content)
        digest = sig.order_hash(sig.order_of_wire(body["order"]))
        return httpx.Response(200, json={"orderHash": digest, "status": status, "timeInForce": body["time_in_force"]})
    return answer


def record_of(entry: dict, status="open", filled="0") -> dict:
    digest = sig.order_hash(sig.order_of_wire(entry["order"]))
    return {"orderHash": digest, "order": entry["order"], "status": status, "filledAmount": filled,
            "timeInForce": entry.get("time_in_force", "GTC"), "networkId": 56}


class TestAdapter:
    def test_an_order_is_signed_and_sent_compact(self):
        venue = Venue(**{"POST /orders": echo_order()})

        async def go():
            async with adapter(venue) as myriad:
                return await myriad.create_order(request(client_order_id="c-1"))

        order = run(go())
        [sent] = venue.sent("POST", "/orders")
        body = json.loads(sent.content)
        assert b": " not in sent.content and body["network_id"] == 56 and body["client_order_id"] == "c-1"
        assert recover(body) == OWNER.address and body["signatureType"] == 0
        assert order.id == sig.order_hash(sig.order_of_wire(body["order"])) and order.status == OrderStatus.OPEN
        assert order.market_id == MARKET_ID and order.quantity == Decimal("10") and order.price == Decimal("0.4")
        assert body["signature"] not in json.dumps(order.info)

    def test_a_cancel_repeats_the_order_it_placed(self):
        orders: dict[str, dict] = {}

        def place(req):
            body = json.loads(req.content)
            orders[sig.order_hash(sig.order_of_wire(body["order"]))] = body
            return echo_order()(req)

        def cancel(req):
            return httpx.Response(200, json={"orderHash": req.url.path.rsplit("/", 1)[1], "status": "cancelled"})

        def read(req):
            digest = req.url.path.rsplit("/", 1)[1]
            return httpx.Response(200, json=record_of(orders[digest], status="cancelled"))

        venue = Venue(**{"POST /orders": place})

        async def go():
            async with adapter(venue) as myriad:
                order = await myriad.create_order(request())
                venue.answers[f"DELETE /orders/{order.id}"] = cancel
                venue.answers[f"GET /orders/{order.id}"] = read
                return order, await myriad.cancel_order(order.id)

        placed, cancelled = run(go())
        [delete] = venue.sent("DELETE", f"/orders/{placed.id}")
        body = json.loads(delete.content)
        assert body["signature"] == orders[placed.id]["signature"] and body["order"] == orders[placed.id]["order"]
        assert cancelled.status == OrderStatus.CANCELED

    def test_an_order_placed_elsewhere_is_read_back_and_signed_again(self):
        entry, digest, _ = signed(request())
        venue = Venue(**{
            f"GET /orders/{digest}": record_of(entry),
            f"DELETE /orders/{digest}": {"orderHash": digest, "status": "cancelled"},
        })

        async def go():
            async with adapter(venue) as myriad:
                await myriad.cancel_order(digest)

        run(go())
        body = json.loads(venue.sent("DELETE", f"/orders/{digest}")[0].content)
        assert body["signature"] == entry["signature"], "the same order and key sign the same signature"

    def test_cancel_all_signs_cancel_all(self):
        venue = Venue(**{"POST /orders/cancel-all": {"cancelled_count": 3, "market_ids_affected": []}})

        async def go():
            async with adapter(venue) as myriad:
                return await myriad.cancel_all_orders(market_id=MARKET_ID), await myriad.cancel_all_orders()

        assert run(go()) == (3, 3)
        first, every = (json.loads(r.content) for r in venue.sent("POST", "/orders/cancel-all"))
        assert first["market_id"] == 3722 and "market_id" not in every and first["trader"] == OWNER.address

    def test_a_batch_reports_each_order(self):
        def batch(req):
            body = json.loads(req.content)
            hashes = [sig.order_hash(sig.order_of_wire(e["order"])) for e in body["orders"]]
            return httpx.Response(200, json={"placed": [hashes[0]], "errors": [
                {"index": 1, "orderHash": None, "reason": "Invalid order signature"}]})

        venue = Venue(**{"POST /orders/batch": batch})

        async def go():
            async with adapter(venue) as myriad:
                return await myriad.create_orders([request(), request(price=Decimal("0.3")),
                                                   request(price=Decimal("0.305"))])

        ok, refused, invalid = run(go())
        assert ok.status == OrderStatus.OPEN and isinstance(refused, InvalidOrder) and isinstance(invalid, InvalidOrder)
        [sent] = venue.sent("POST", "/orders/batch")
        assert len(json.loads(sent.content)["orders"]) == 2, "an off-tick order never reaches the venue"

    def test_an_edit_is_one_atomic_cancel_and_place(self):
        entry, digest, _ = signed(request())
        current = order_of_record(record_of(entry))

        def modify(req):
            body = json.loads(req.content)
            new = sig.order_hash(sig.order_of_wire(body["place"][0]["order"]))
            return httpx.Response(200, json={"placed": [new], "cancelled": [digest], "errors": []})

        venue = Venue(**{f"GET /orders/{digest}": record_of(entry), "POST /orders/batch-modify": modify})

        async def go():
            async with adapter(venue) as myriad:
                return await myriad.edit_order(EditRequest(order_id=digest, price=Decimal("0.45")), current=current)

        edited = run(go())
        body = json.loads(venue.sent("POST", "/orders/batch-modify")[0].content)
        assert body["cancel"][0]["signature"] == entry["signature"] and body["allow_partial"] is False
        assert body["place"][0]["order"]["price"] == "450000000000000000"
        assert edited.queue_priority_preserved is False and edited.price == Decimal("0.45")

    def test_refusals_are_typed(self):
        venue = Venue(**{"POST /orders": lambda r: httpx.Response(400, json={"error": "Insufficient balance or allowance"})})

        async def go():
            async with adapter(venue) as myriad:
                return await myriad.create_order(request())

        with pytest.raises(InsufficientFunds):
            run(go())
        venue.answers["POST /orders"] = lambda r: httpx.Response(409, json={"error": "Order already exists"})
        with pytest.raises(OrderRejected):
            run(go())

    def test_an_amm_market_takes_no_resting_order_or_order_book_calldata(self):
        async def go():
            async with adapter(Venue()) as myriad:
                return await myriad.create_order(request(market_id="myriad:56-amm-120473"))

        with pytest.raises(InvalidOrder, match="taker-only"):
            run(go())

        async def calldata():
            async with adapter(Venue()) as myriad:
                return await myriad.position_calldata("redeem", "myriad:56-amm-120473")

        with pytest.raises(InvalidOrder, match="AMM market"):
            run(calldata())

    def test_without_a_key_the_wallet_signs_in(self):
        calls = []

        def nonce(req):
            calls.append("nonce")
            assert req.url.params["address"] == OWNER.address
            return httpx.Response(200, json={"nonce": "n-1", "message": "Sign in to Myriad\nNonce: n-1"})

        def login(req):
            calls.append("login")
            body = json.loads(req.content)
            assert EthAccount.recover_message(encode_defunct(text=body["message"]), signature=body["signature"]) == OWNER.address
            return httpx.Response(200, json={"accessToken": "jwt-1", "refreshToken": "r-1"})

        def orders(req):
            calls.append(req.headers.get("authorization"))
            return httpx.Response(200, json={"data": [], "pagination": {"hasNext": False}})

        venue = Venue(keyed=False, **{"GET /auth/nonce": nonce, "POST /auth/login": login, "GET /orders": orders})

        async def go():
            async with adapter(venue, keyed=False) as myriad:
                await myriad.fetch_open_orders()
                await myriad.fetch_open_orders()
                return await myriad.create_order(request(client_order_id="c-1"))

        venue.answers["POST /orders"] = echo_order()
        run(go())
        assert calls == ["nonce", "login", "Bearer jwt-1", "Bearer jwt-1"]
        body = json.loads(venue.sent("POST", "/orders")[0].content)
        assert "client_order_id" not in body, "the venue takes a client order id only with an API key"
        assert venue.sent("POST", "/orders")[0].headers["authorization"] == "Bearer jwt-1"

    def test_open_orders_positions_and_fills(self):
        entry, digest, _ = signed(request(side=Side.SELL, price=Decimal("0.3")))
        venue = Venue(**{
            "GET /orders": {"data": [record_of(entry)], "pagination": {"hasNext": False}},
            f"GET /users/{OWNER.address}/portfolio": {"data": [
                {"marketId": 3722, "networkId": 56, "executionMode": 1, "outcomeId": 0, "shares": 2, "price": 0.9,
                 "value": 1.8, "profit": 0, "status": "ongoing"}], "pagination": {"hasNext": False}},
            f"GET /users/{OWNER.address}/events": lambda r: httpx.Response(200, json={"data": [
                {"action": "buy", "marketId": 3722, "networkId": 56, "outcomeId": 0, "shares": 2, "value": 1.8,
                 "timestamp": 1792772000, "txId": "0xt"}] if r.url.params["trading_model"] == "ob" else [],
                "pagination": {"hasNext": False}}),
        })

        async def go():
            async with adapter(venue) as myriad:
                return (await myriad.fetch_open_orders(market_id=MARKET_ID), await myriad.fetch_positions(),
                        await myriad.fetch_my_trades(since=1792771000000))

        [order], [position], fills = run(go())
        assert order.id == digest and order.side == Side.SELL and order.price == Decimal("0.3")
        assert venue.sent("GET", "/orders")[0].url.params["market_id"] == "3722"
        assert position.market_id == MARKET_ID and position.contracts == Decimal("2")
        assert [f.id for f in fills] == ["0xt:0:buy"]
        ob, amm = venue.sent("GET", f"/users/{OWNER.address}/events")
        assert ob.url.params["since"] == "1792771000" and ob.url.params["network_id"] == "56"
        assert amm.url.params["trading_model"] == "amm" and "network_id" not in amm.url.params

    def test_the_balance_is_the_wallets_usd1_less_open_buys(self):
        entry, _, _ = signed(request(price=Decimal("0.4")))
        venue = Venue(**{"GET /orders": {"data": [record_of(entry)], "pagination": {}}})

        def rpc(req):
            call = json.loads(req.content)["params"][0]
            assert call["to"] == sig.USD1 and OWNER.address[2:].lower() in call["data"]
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": hex(25 * 10**18)})

        async def go():
            async with adapter(venue, rpc=httpx.AsyncClient(transport=httpx.MockTransport(rpc))) as myriad:
                return await myriad.fetch_balance()

        balance = run(go())
        assert (balance.total, balance.locked, balance.available) == (Decimal("25"), Decimal("4"), Decimal("21"))

    def test_the_fee_estimate_reads_the_outcome_bought(self):
        async def go():
            async with adapter(Venue()) as myriad:
                return await myriad.fetch_fee_estimate(MARKET_ID, Side.SELL, Decimal("0.7"), Decimal("100"))

        estimate = run(go())
        table = MARKET["fees"]["taker_fee_bps_array"]
        assert estimate.taker_fee == pytest.approx(Decimal(str(table[29] / 10_000 * 30)))
        assert estimate.maker_fee == 0

    def test_a_subscription_token_for_the_account_channels(self):
        venue = Venue(**{"POST /auth/centrifugo/subscription-token": lambda r: httpx.Response(
            200, json={"token": "t-" + json.loads(r.content)["channel"], "expiresIn": 300})})

        async def go():
            async with adapter(venue) as myriad:
                return await myriad.subscription_token(f"orders:56:{OWNER.address.lower()}")

        assert run(go()) == f"t-orders:56:{OWNER.address.lower()}"


class TestWiring:
    def test_the_client_and_the_engine_feeds_build_myriad(self):
        from synpath import Client
        from synpath.engine.feeds import default_streams

        creds = MyriadCredentials(private_key=KEY, api_key="key-1", api_secret="secret-1")
        trading = Client({"myriad": creds}).trading("myriad")
        assert isinstance(trading, MyriadTrading)
        streams = default_streams({"myriad": trading}, {"myriad": creds})["myriad"]
        assert streams.market.has["watch_order_book"] and streams.private.address == OWNER.address.lower()
        run(trading.close())

    def test_doctor_says_what_each_credential_does(self):
        from synpath.trading.__main__ import myriad_identity

        both = myriad_identity(MyriadCredentials(private_key=KEY, api_key="key-12345678", api_secret="s"))
        assert OWNER.address in both and "HMAC" in both and "places and cancels" in both
        wallet = myriad_identity(MyriadCredentials(private_key=KEY))
        assert "signs in" in wallet
        reader = myriad_identity(MyriadCredentials(api_key="k", api_secret="s", account_address=OWNER.address))
        assert "reads the account only" in reader and "s" * 3 not in reader
