"""predict.fun order entry: signing checked against independent encoders,
order bodies, REST records, and the adapter against a scripted venue."""
from __future__ import annotations

import asyncio
import base64
import json
from decimal import Decimal

import httpx
import pytest
from eth_abi import encode
from eth_account import Account as EthAccount
from eth_account.messages import _hash_eip191_message, encode_defunct, encode_typed_data
from eth_utils import keccak

from synpath.predict_fun import fee_schedule_of, normalize_market
from synpath.trading import predict_fun_signing as sig
from synpath.trading.credentials import CredentialsMissing, PredictFunCredentials, load_predict_fun
from synpath.trading.errors import InsufficientFunds, InvalidOrder, OrderRejected
from synpath.trading.polymarket_signing import WalletSigner
from synpath.trading.predict_fun import (
    PredictFunTrading, build_signed_order, fills_of_match, jwt_expiry, market_info_of, order_of_row, position_of,
)
from synpath.trading.types import (
    Liquidity, OrderRequest, OrderStatus, OrderType, PositionSide, Side, TimeInForce,
)

from conftest import load

KEY = "0x" + "11" * 32
OWNER = EthAccount.from_key(KEY)
PREDICT_ACCOUNT = "0x1234567890AbcdEF1234567890aBcdef12345678"
MARKET = load("predict_fun_market.json")["data"]
INFO = market_info_of(MARKET)
NATIVE = str(MARKET["id"])
MARKET_ID = f"predict_fun:{NATIVE}"


def request(**changes) -> OrderRequest:
    base = {"market_id": MARKET_ID, "side": Side.BUY, "amount": Decimal("10"), "price": Decimal("0.4")}
    return OrderRequest(**{**base, **changes})


def signed(req: OrderRequest, *, info=INFO, account=None, now_s=1_800_000_000):
    return build_signed_order(
        req, info, signer=WalletSigner(KEY), maker=account or OWNER.address, network=sig.MAINNET,
        account=account, now_s=now_s, salt=7,
    )


def recover(digest: bytes, signature: str) -> str:
    return EthAccount._recover_hash(digest, signature=bytes.fromhex(signature.removeprefix("0x")))


def sdk_kernel_signature(message_hash: bytes, account: str, chain_id: int = 56) -> str:
    """The SDK's `sign_predict_account_message`, written out independently."""
    domain = keccak(encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
         keccak(text="Kernel"), keccak(text="0.3.1"), chain_id, account],
    ))
    inner = keccak(encode(["bytes32", "bytes32"], [keccak(text="Kernel(bytes32 hash)"), message_hash]))
    digest = keccak(b"\x19\x01" + domain + inner)
    signature = OWNER.sign_message(encode_defunct(primitive=digest)).signature.hex().removeprefix("0x")
    return "0x01" + sig.MAINNET.ecdsa_validator[2:] + signature


class TestSigning:
    ORDER = sig.build_order(
        maker=OWNER.address, token_id=INFO.yes_token, maker_amount=4 * 10**18, taker_amount=10 * 10**18,
        side="BUY", fee_rate_bps=200, salt=12345,
    )

    @pytest.mark.parametrize("neg_risk, yield_bearing", [(False, False), (True, False), (False, True), (True, True)])
    def test_the_hash_and_wallet_signature_are_eip712(self, neg_risk, yield_bearing):
        exchange = sig.MAINNET.exchange(neg_risk=neg_risk, yield_bearing=yield_bearing)
        encoded = encode_typed_data(full_message=sig.order_typed_data(self.ORDER, exchange=exchange, chain_id=56))
        signature, order_hash = sig.sign_order(WalletSigner(KEY), self.ORDER, exchange=exchange, network=sig.MAINNET)
        assert order_hash == "0x" + _hash_eip191_message(encoded).hex()
        assert signature == "0x" + OWNER.sign_message(encoded).signature.hex().removeprefix("0x")

    def test_each_market_kind_has_its_exchange(self):
        assert sig.MAINNET.exchange(neg_risk=False, yield_bearing=False) == sig.MAINNET.ctf_exchange
        assert sig.MAINNET.exchange(neg_risk=True, yield_bearing=True) == sig.MAINNET.yield_bearing_neg_risk_ctf_exchange
        assert sig.TESTNET.exchange(neg_risk=True, yield_bearing=False) == sig.TESTNET.neg_risk_ctf_exchange

    def test_a_predict_account_signs_through_its_kernel(self):
        order = {**self.ORDER, "maker": PREDICT_ACCOUNT, "signer": PREDICT_ACCOUNT}
        signature, order_hash = sig.sign_order(
            WalletSigner(KEY), order, exchange=sig.MAINNET.ctf_exchange, network=sig.MAINNET, account=PREDICT_ACCOUNT,
        )
        assert signature == sdk_kernel_signature(bytes.fromhex(order_hash[2:]), PREDICT_ACCOUNT)
        assert signature.startswith("0x01" + sig.MAINNET.ecdsa_validator[2:])

    def test_the_login_message_is_a_personal_sign(self):
        message = "Sign in to predict.fun: 42"
        wallet = sig.sign_login(WalletSigner(KEY), message, network=sig.MAINNET)
        assert wallet == "0x" + OWNER.sign_message(encode_defunct(text=message)).signature.hex().removeprefix("0x")
        account = sig.sign_login(WalletSigner(KEY), message, network=sig.MAINNET, account=PREDICT_ACCOUNT)
        assert account == sdk_kernel_signature(_hash_eip191_message(encode_defunct(text=message)), PREDICT_ACCOUNT)


class TestAmounts:
    def test_the_sdk_example(self):
        """`getLimitOrderAmounts`: 10 shares at 0.4 is 4 USDT for 10 shares."""
        assert sig.limit_amounts("BUY", Decimal("0.4"), Decimal("10"))[:2] == (4 * 10**18, 10 * 10**18)
        assert sig.limit_amounts("SELL", Decimal("0.4"), Decimal("10"))[:2] == (10 * 10**18, 4 * 10**18)

    def test_price_and_size_are_cut_not_rounded(self):
        maker, taker, price, size = sig.limit_amounts("BUY", Decimal("0.12399"), Decimal("12.3459"))
        assert price == 123 * 10**15 and size == 12345 * 10**15
        assert maker == price * size // 10**18 and taker == size

    def test_out_of_range(self):
        with pytest.raises(InvalidOrder):
            sig.limit_amounts("BUY", Decimal("1"), Decimal("10"))
        with pytest.raises(InvalidOrder, match="minimum"):
            sig.limit_amounts("BUY", Decimal("0.5"), Decimal("0.001"))


class TestOrderBody:
    def test_a_buy_is_a_resting_limit_on_yes(self):
        body, contracts = signed(request())
        data = body["data"]
        assert data["strategy"] == "LIMIT" and data["pricePerShare"] == str(4 * 10**17)
        order = data["order"]
        assert order["tokenId"] == INFO.yes_token and order["side"] == sig.BUY and order["feeRateBps"] == "200"
        assert order["expiration"] == sig.NO_EXPIRY and order["maker"] == order["signer"] == OWNER.address
        assert contracts == Decimal("10")
        digest = bytes.fromhex(order["hash"][2:])
        assert recover(digest, order["signature"]) == OWNER.address

    def test_a_sell_buys_no_at_the_complement(self):
        order = signed(request(side=Side.SELL))[0]["data"]["order"]
        assert order["tokenId"] == INFO.no_token and order["side"] == sig.BUY
        assert int(order["makerAmount"]) == 6 * 10**18

    def test_reduce_only_sells_the_token_held(self):
        order = signed(request(side=Side.SELL, reduce_only=True))[0]["data"]["order"]
        assert order["tokenId"] == INFO.yes_token and order["side"] == sig.SELL

    def test_market_ioc_and_fok_take_at_once(self):
        market = signed(request(type=OrderType.MARKET, time_in_force=TimeInForce.IOC))[0]["data"]
        assert market["strategy"] == "MARKET" and market["order"]["expiration"] == 1_800_000_000 + 300
        assert "isFillOrKill" not in market
        fok = signed(request(time_in_force=TimeInForce.FOK))[0]["data"]
        assert fok["strategy"] == "MARKET" and fok["isFillOrKill"] is True

    def test_post_only_and_good_till_date(self):
        data = signed(request(post_only=True, time_in_force=TimeInForce.GTD, expires_at=1_900_000_000_000))[0]["data"]
        assert data["isPostOnly"] is True and data["order"]["expiration"] == 1_900_000_000

    def test_refusals(self):
        with pytest.raises(InvalidOrder, match="day"):
            signed(request(time_in_force=TimeInForce.DAY))
        with pytest.raises(InvalidOrder, match="post-only"):
            signed(request(post_only=True, time_in_force=TimeInForce.IOC))
        with pytest.raises(InvalidOrder, match="past"):
            signed(request(time_in_force=TimeInForce.GTD, expires_at=1_000))

    def test_the_market_kind_picks_the_verifying_exchange(self):
        info = market_info_of({**MARKET, "isNegRisk": True, "isYieldBearing": True})
        order = signed(request(), info=info)[0]["data"]["order"]
        exchange = sig.MAINNET.yield_bearing_neg_risk_ctf_exchange
        typed = sig.order_typed_data(
            {k: int(v) if k not in ("maker", "signer", "taker") else v
             for k, v in order.items() if k not in ("hash", "signature")},
            exchange=exchange, chain_id=56,
        )
        assert order["hash"] == "0x" + _hash_eip191_message(encode_typed_data(full_message=typed)).hex()

    def test_a_predict_account_makes_the_order(self):
        order = signed(request(), account=PREDICT_ACCOUNT)[0]["data"]["order"]
        assert order["maker"] == order["signer"] == PREDICT_ACCOUNT
        assert order["signature"] == sdk_kernel_signature(bytes.fromhex(order["hash"][2:]), PREDICT_ACCOUNT)


def order_row(**changes):
    row = {
        "id": "991", "marketId": int(NATIVE), "currency": "USDT", "amount": str(10 * 10**18), "amountFilled": str(4 * 10**18),
        "isNegRisk": False, "isYieldBearing": False, "strategy": "LIMIT", "status": "OPEN", "rewardEarningRate": 0,
        "order": {"hash": "0xaa", "salt": "1", "maker": OWNER.address, "signer": OWNER.address, "taker": sig.ZERO_ADDRESS,
                  "tokenId": INFO.no_token, "makerAmount": str(3 * 10**18), "takerAmount": str(10 * 10**18),
                  "expiration": sig.NO_EXPIRY, "nonce": "0", "feeRateBps": "200", "side": 0, "signatureType": 0},
    }
    row.update(changes)
    return row


class TestRecords:
    def test_an_order_row_on_no_reads_on_yes(self):
        order = order_of_row(order_row(), INFO)
        assert (order.id, order.side, order.price) == ("0xaa", Side.SELL, Decimal("0.7"))
        assert order.amount == Decimal("10") and order.filled == Decimal("4") and order.remaining == Decimal("6")
        assert order.status == OrderStatus.OPEN and order.time_in_force == TimeInForce.GTC

    @pytest.mark.parametrize("status, expected", [
        ("FILLED", OrderStatus.CLOSED), ("CANCELLED", OrderStatus.CANCELED), ("EXPIRED", OrderStatus.EXPIRED),
        ("INVALIDATED", OrderStatus.CANCELED),
    ])
    def test_row_states(self, status, expected):
        order = order_of_row(order_row(status=status), INFO)
        assert order.status == expected and order.remaining == 0

    def test_my_legs_of_a_match(self):
        match = load("predict_fun_matches.json")["data"][0]
        maker = match["makers"][0]
        [fill] = fills_of_match(match, maker["signer"].lower())
        assert fill.liquidity == Liquidity.MAKER and fill.order_id == maker["hash"]
        assert fill.amount == Decimal(maker["amount"]) / 10**18 and fill.id == match["settlementId"]
        taker = fills_of_match(match, match["taker"]["signer"])
        assert [f.liquidity for f in taker] == [Liquidity.TAKER]
        assert taker[0].amount == Decimal(match["amountFilled"]) / 10**18
        assert fills_of_match(match, "0x" + "00" * 20) == []

    def test_positions_net_on_yes(self):
        rows = [
            {"market": MARKET, "outcome": {"indexSet": 1}, "amount": str(10 * 10**18), "valueUsd": "6", "averageBuyPriceUsd": "0.5", "pnlUsd": "1"},
            {"market": MARKET, "outcome": {"indexSet": 2}, "amount": str(4 * 10**18), "valueUsd": "1.6", "averageBuyPriceUsd": "0.45", "pnlUsd": "-0.2"},
        ]
        position = position_of(rows, market_id=MARKET_ID)
        assert position.side == PositionSide.LONG and position.contracts == Decimal("6")
        assert position.inventory_no == Decimal("4") and position.mark_price == Decimal("0.6")
        assert position.unrealized_pnl == Decimal("0.8")

    def test_jwt_expiry(self):
        payload = base64.urlsafe_b64encode(json.dumps({"exp": 1900000000}).encode()).decode().rstrip("=")
        assert jwt_expiry(f"h.{payload}.s") == 1900000000 and jwt_expiry("opaque") is None


class TestCredentials:
    def test_mainnet_needs_an_api_key(self):
        with pytest.raises(CredentialsMissing, match="PREDICT_FUN_API_KEY"):
            load_predict_fun({"PREDICT_FUN_PRIVATE_KEY": KEY})
        assert load_predict_fun({"PREDICT_FUN_PRIVATE_KEY": KEY[2:], "PREDICT_FUN_TESTNET": "1"}).private_key == KEY

    def test_the_account_and_secrets(self):
        creds = load_predict_fun({"PREDICT_FUN_PRIVATE_KEY": KEY, "PREDICT_FUN_API_KEY": "k" * 20,
                                  "PREDICT_FUN_ACCOUNT_ADDRESS": PREDICT_ACCOUNT})
        assert creds.address == PREDICT_ACCOUNT and creds.secrets == [KEY, "k" * 20]
        assert KEY not in repr(creds) and "k" * 20 not in repr(creds)
        with pytest.raises(CredentialsMissing, match="0x address"):
            load_predict_fun({"PREDICT_FUN_PRIVATE_KEY": KEY, "PREDICT_FUN_TESTNET": "1",
                              "PREDICT_FUN_ACCOUNT_ADDRESS": "nope"})


# ---------------------------------------------------------------------------
# The adapter against a scripted venue
# ---------------------------------------------------------------------------

class Catalog:
    def fetch_market(self, native):
        return normalize_market(MARKET)

    def fetch_fee_schedule(self, market_id):
        return fee_schedule_of(MARKET, market_id=market_id)

    def close(self):
        pass


class Venue:
    """Answers the venue's routes and records every request."""

    def __init__(self, **answers):
        self.answers = answers
        self.requests: list[httpx.Request] = []
        self.logins = 0

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        path = req.url.path.removeprefix("/v1")
        if path == "/auth/message":
            return httpx.Response(200, json={"success": True, "data": {"message": f"login {self.logins}"}})
        if path == "/auth":
            body = json.loads(req.content)
            digest = _hash_eip191_message(encode_defunct(text=body["message"]))
            assert recover(digest, body["signature"]) == body["signer"] == OWNER.address
            self.logins += 1
            return httpx.Response(200, json={"success": True, "data": {"token": f"jwt-{self.logins}"}})
        answer = self.answers.get(f"{req.method} {path}")
        if answer is None:
            return httpx.Response(404, json={"success": False, "message": "not found"})
        return answer(req) if callable(answer) else httpx.Response(200, json=answer)

    def sent(self, method: str, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method and r.url.path == f"/v1{path}"]


def adapter(venue: Venue, rpc=None) -> PredictFunTrading:
    creds = PredictFunCredentials(private_key=KEY, api_key="test-key")
    return PredictFunTrading(
        creds, catalog=Catalog(), client=httpx.AsyncClient(transport=httpx.MockTransport(venue), headers={"x-api-key": "test-key"}),
        rpc_client=rpc or httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))),
    )


def run(coro):
    return asyncio.run(coro)


class TestAdapter:
    def test_an_order_is_signed_sent_with_the_token_and_named_by_its_hash(self):
        venue = Venue(**{"POST /orders": lambda req: httpx.Response(201, json={
            "success": True, "data": {"code": "OK", "orderId": "991", "orderHash": json.loads(req.content)["data"]["order"]["hash"]}})})

        async def go():
            async with adapter(venue) as pf:
                return await pf.create_order(request())

        order = run(go())
        [sent] = venue.sent("POST", "/orders")
        body = json.loads(sent.content)["data"]
        assert sent.headers["authorization"] == "Bearer jwt-1" and sent.headers["x-api-key"] == "test-key"
        assert order.id == body["order"]["hash"] and order.info["venue_order_id"] == "991"
        assert order.status == OrderStatus.OPEN and order.amount == Decimal("10")
        assert "signature" not in order.info["request"]["order"]

    def test_a_refused_token_is_renewed_once(self):
        calls = []

        def orders(req):
            calls.append(req.headers["authorization"])
            if len(calls) == 1:
                return httpx.Response(401, json={"message": "jwt expired"})
            return httpx.Response(200, json={"success": True, "data": [], "cursor": None})

        venue = Venue(**{"GET /orders": orders})

        async def go():
            async with adapter(venue) as pf:
                return await pf.fetch_orders(status="open")

        assert list(run(go())) == [] and calls == ["Bearer jwt-1", "Bearer jwt-2"]

    def test_the_venue_s_refusal_is_typed(self):
        venue = Venue(**{"POST /orders": lambda req: httpx.Response(400, json={"message": "Insufficient collateral balance"})})

        async def go():
            async with adapter(venue) as pf:
                await pf.create_order(request())

        with pytest.raises(InsufficientFunds):
            run(go())

    def test_orders_read_back_on_the_yes_leg(self):
        venue = Venue(**{
            "GET /orders": {"success": True, "data": [order_row(), order_row(marketId=1)], "cursor": "next"},
            "GET /orders/0xaa": {"success": True, "data": order_row()},
        })

        async def go():
            async with adapter(venue) as pf:
                return await pf.fetch_orders(market_id=MARKET_ID), await pf.fetch_order("0xaa")

        page, order = run(go())
        assert [o.id for o in page] == ["0xaa"] and page.next_cursor == "next"
        assert order.side == Side.SELL and order.price == Decimal("0.7")

    def test_cancels_report_removed_noop_and_locked(self):
        def remove(req):
            hashes = json.loads(req.content)["data"]["hashes"]
            assert hashes == ["0xaa", "0xbb", "0xcc"]
            return httpx.Response(200, json={"success": True, "removed": ["0xaa"], "noop": ["0xbb"], "rejected": ["0xcc"]})

        venue = Venue(**{
            "POST /orders/remove-by-hash": remove,
            "GET /orders/0xaa": {"success": True, "data": order_row()},
            "GET /orders/0xbb": {"success": True, "data": order_row(status="FILLED", order={**order_row()["order"], "hash": "0xbb"})},
        })

        async def go():
            async with adapter(venue) as pf:
                return await pf.cancel_orders(["0xaa", "0xbb", "0xcc"])

        removed, noop, locked = run(go())
        assert removed.status == OrderStatus.CANCELED and noop.status == OrderStatus.CLOSED
        assert isinstance(locked, OrderRejected) and locked.reason == "removal_locked"

    def test_the_balance_is_on_chain_less_what_orders_hold(self):
        def rpc(req):
            call = json.loads(req.content)["params"][0]
            assert call["to"] == sig.MAINNET.usdt and call["data"].endswith(OWNER.address[2:].lower())
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": hex(25 * 10**18)})

        venue = Venue(**{"POST /account/reserved-balances/query": {
            "success": True, "data": [{"type": "USDT", "amount": str(4 * 10**18)}]}})

        async def go():
            async with adapter(venue, rpc=httpx.AsyncClient(transport=httpx.MockTransport(rpc))) as pf:
                return await pf.fetch_balance()

        balance = run(go())
        assert (balance.total, balance.locked, balance.available) == (Decimal("25"), Decimal("4"), Decimal("21"))

    def test_my_trades_are_the_public_matches_filtered_to_me(self):
        matches = load("predict_fun_matches.json")
        venue = Venue(**{"GET /orders/matches": matches})
        me = matches["data"][0]["taker"]["signer"]

        async def go():
            pf = adapter(venue)
            pf.address = me
            async with pf:
                return await pf.fetch_my_trades(market_id=MARKET_ID)

        fills = run(go())
        [sent] = venue.sent("GET", "/orders/matches")
        assert sent.url.params["signerAddress"] == me and sent.url.params["marketId"] == NATIVE
        assert fills and all(f.liquidity in (Liquidity.TAKER, Liquidity.MAKER) for f in fills)
        assert "authorization" not in sent.headers

    def test_the_fee_estimate_is_symmetric(self):
        async def go():
            async with adapter(Venue()) as pf:
                return (await pf.fetch_fee_estimate(MARKET_ID, Side.BUY, Decimal("0.3"), Decimal("100")),
                        await pf.fetch_fee_estimate(MARKET_ID, Side.SELL, Decimal("0.7"), Decimal("100")))

        buy, sell = run(go())
        assert buy.taker_fee == sell.taker_fee == Decimal("0.6") and buy.maker_fee == 0

    def test_the_wallet_stream_can_force_a_new_login(self):
        venue = Venue()

        async def go():
            async with adapter(venue) as pf:
                return await pf.jwt(), await pf.jwt(), await pf.jwt(fresh=True)

        assert run(go()) == ("jwt-1", "jwt-1", "jwt-2")
