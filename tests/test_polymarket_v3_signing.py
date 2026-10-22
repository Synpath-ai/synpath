"""Polymarket Exchange v3 order signing (combo RFQs), offline.

Evidence, kept apart:

* **Vectors** in `tests/samples/polymarket_v3_signing_vectors.json`, made
  without synpath: the typed data exactly as the venue's combo docs print it,
  signed by `eth_account`, and the Deposit Wallet signature wrapped by the
  docs' `wrapDepositWalletSignature` algorithm. The venue publishes no
  Exchange v3 vectors of its own. Every order hash and signature must match
  byte for byte, on the Rust core signer and on the `eth_account` path.
* **Recomputation in the test**: the docs' typed data rebuilt here, hashed by
  `eth_account`, and each signature recovered to the signer.
* **CLOB V2 untouched**: the V2 vectors still match after this module loads.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

from synpath.trading import polymarket_signing as v2
from synpath.trading import polymarket_v3_signing as v3
from synpath.trading.errors import InvalidOrder

SAMPLES = Path(__file__).parent / "samples"
VECTORS = json.loads((SAMPLES / "polymarket_v3_signing_vectors.json").read_text())
V2_VECTORS = json.loads((SAMPLES / "polymarket_signing_vectors.json").read_text())
KEY = VECTORS["private_key"]
ADDRESS = VECTORS["address"]
WALLET = VECTORS["wallet"]
CASES = {c["name"]: c for c in VECTORS["orders"]}

DOCS_DOMAIN = {"name": "Polymarket CTF Exchange", "version": "3", "chainId": 137,
               "verifyingContract": "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"}


def signer(native: bool = True) -> v2.WalletSigner:
    s = v2.WalletSigner(KEY)
    if not native:
        s._native = None
    return s


def docs_message(order: dict) -> dict:
    return {
        "salt": int(order["salt"]), "maker": order["maker"], "signer": order["signer"],
        "tokenId": int(order["tokenId"]), "makerAmount": int(order["makerAmount"]),
        "takerAmount": int(order["takerAmount"]), "side": order["side"], "signatureType": order["signatureType"],
        "timestamp": int(order["timestamp"]), "metadata": bytes(32), "builder": bytes(32),
    }


ORDER_FIELDS = [
    {"name": "salt", "type": "uint256"}, {"name": "maker", "type": "address"}, {"name": "signer", "type": "address"},
    {"name": "tokenId", "type": "uint256"}, {"name": "makerAmount", "type": "uint256"},
    {"name": "takerAmount", "type": "uint256"}, {"name": "side", "type": "uint8"},
    {"name": "signatureType", "type": "uint8"}, {"name": "timestamp", "type": "uint256"},
    {"name": "metadata", "type": "bytes32"}, {"name": "builder", "type": "bytes32"},
]
DOMAIN_FIELDS = [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                 {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}]


def docs_order_typed_data(order: dict) -> dict:
    return {"types": {"EIP712Domain": DOMAIN_FIELDS, "Order": ORDER_FIELDS}, "primaryType": "Order",
            "domain": DOCS_DOMAIN, "message": docs_message(order)}


def docs_deposit_typed_data(order: dict) -> dict:
    return {
        "types": {"EIP712Domain": DOMAIN_FIELDS, "Order": ORDER_FIELDS, "TypedDataSign": [
            {"name": "contents", "type": "Order"}, {"name": "name", "type": "string"},
            {"name": "version", "type": "string"}, {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"}, {"name": "salt", "type": "bytes32"}]},
        "primaryType": "TypedDataSign", "domain": DOCS_DOMAIN,
        "message": {"contents": docs_message(order), "name": "DepositWallet", "version": "1", "chainId": 137,
                    "verifyingContract": order["signer"], "salt": bytes(32)},
    }


def digest_of(typed: dict) -> bytes:
    enc = encode_typed_data(full_message=typed)
    return keccak(b"\x19" + enc.version + enc.header + enc.body)


class TestVectors:
    @pytest.mark.parametrize("name", sorted(CASES))
    @pytest.mark.parametrize("native", [True, False], ids=["rust-core", "eth-account"])
    def test_signature_and_hash_match_byte_for_byte(self, name, native):
        case = CASES[name]
        assert v3.order_hash(case["order"]) == case["order_hash"]
        assert v3.sign_order(signer(native), case["order"]) == case["signature"]

    def test_the_rust_core_is_used_when_installed(self):
        from synpath import _native

        if _native.core is None:
            pytest.skip("the Rust core is not installed")
        assert signer()._native is not None

    @pytest.mark.parametrize("name", ["proxy_buy", "safe_sell", "eoa_maker_buy_no"])
    def test_direct_signatures_recover_to_the_signer(self, name):
        case = CASES[name]
        typed = docs_order_typed_data(case["order"])
        assert v3.signing_digest(case["order"]) == digest_of(typed)
        assert Account.recover_message(encode_typed_data(full_message=typed), signature=case["signature"]) == ADDRESS
        assert len(bytes.fromhex(case["signature"][2:])) == 65

    @pytest.mark.parametrize("name", ["deposit_buy", "deposit_sell"])
    def test_deposit_wallet_signature_is_the_erc7739_wrap(self, name):
        case = CASES[name]
        order = case["order"]
        raw = bytes.fromhex(case["signature"][2:])
        inner, separator, contents = raw[:65], raw[65:97], raw[97:129]
        type_string, length = raw[129:-2], int.from_bytes(raw[-2:], "big")
        typed = docs_deposit_typed_data(order)
        assert v3.signing_digest(order) == digest_of(typed)
        assert Account.recover_message(encode_typed_data(full_message=typed), signature=inner) == ADDRESS
        # The app domain separator is the Exchange v3 domain's hash, as eth_account computes it.
        assert separator == encode_typed_data(full_message=docs_order_typed_data(order)).header
        assert contents == encode_typed_data(full_message=docs_order_typed_data(order)).body
        assert type_string == v3.ORDER_TYPE.encode() and length == len(v3.ORDER_TYPE)

    def test_typed_data_matches_the_docs_payloads(self):
        order = CASES["deposit_buy"]["order"]
        assert digest_of(v3.deposit_wallet_typed_data(order)) == digest_of(docs_deposit_typed_data(order))
        assert digest_of(v3.order_typed_data(order)) == digest_of(docs_order_typed_data(order))
        assert v3.typed_data_for(order)["primaryType"] == "TypedDataSign"
        assert v3.typed_data_for(CASES["proxy_buy"]["order"])["primaryType"] == "Order"

    def test_the_domain(self):
        assert v3.domain() == DOCS_DOMAIN
        assert v3.domain_separator() == keccak(encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
             keccak(text="Polymarket CTF Exchange"), keccak(text="3"), 137, DOCS_DOMAIN["verifyingContract"]]))


class TestOrders:
    def test_build_order_is_the_wire_shape(self):
        order = v3.build_order(maker=WALLET, signer=WALLET, token_id=789, maker_amount=966191, taker_amount=1932381,
                               side="BUY", signature_type=3, timestamp_s=1773890760, salt=12345)
        assert order == CASES["deposit_buy"]["order"]
        assert isinstance(order["side"], int) and isinstance(order["tokenId"], str)

    def test_refusals(self):
        with pytest.raises(InvalidOrder):
            v3.build_order(maker=WALLET, signer=WALLET, token_id=1, maker_amount=0, taker_amount=1, side="BUY",
                           signature_type=3, timestamp_s=1, salt=1)
        with pytest.raises(InvalidOrder):
            v3.build_order(maker=WALLET, signer=WALLET, token_id=1, maker_amount=1, taker_amount=1, side="HOLD",
                           signature_type=3, timestamp_s=1, salt=1)
        with pytest.raises(InvalidOrder):
            v3.build_order(maker=WALLET, signer=WALLET, token_id=1, maker_amount=1, taker_amount=1, side="BUY",
                           signature_type=7, timestamp_s=1, salt=1)

    def test_identity_per_wallet_type(self):
        assert v3.order_identity(3, signer=ADDRESS, wallet=WALLET) == (WALLET, WALLET)
        assert v3.order_identity(1, signer=ADDRESS, wallet=WALLET) == (ADDRESS, WALLET)
        assert v3.order_identity(2, signer=ADDRESS, wallet=WALLET) == (ADDRESS, WALLET)
        assert v3.order_identity(0, signer=ADDRESS, wallet=WALLET) == (ADDRESS, ADDRESS)

    def test_requesters_refuse_an_eoa(self):
        with pytest.raises(InvalidOrder, match="refuse EOA wallets.*UNSUPPORTED_REQUESTER_SIGNATURE_TYPE"):
            v3.check_requester(0)
        for kind in (1, 2, 3):
            v3.check_requester(kind)


class TestAmounts:
    """The market-makers guide's tables, to the base unit."""

    def test_collateral_quote_amounts(self):
        assert v3.maker_quote_amounts("SELL", 450_000, 1_000_000) == ("yes", 450_000, 1_000_000)
        assert v3.maker_quote_amounts("BUY", 450_000, 1_000_000) == ("no", 550_000, 1_000_000)
        assert v3.maker_quote_amounts("SELL", 450_000, 500_000) == ("yes", 225_000, 500_000)
        assert v3.maker_quote_amounts("BUY", 450_000, 500_000) == ("no", 275_000, 500_000)
        # ceil, not round
        assert v3.maker_quote_amounts("SELL", 333_333, 3) == ("yes", 1, 3)
        assert v3.maker_quote_amounts("BUY", 999_999, 1) == ("no", 1, 1)

    def test_quote_size(self):
        assert v3.quote_size("shares", 1_000_000, 450_000) == 1_000_000
        assert v3.quote_size("notional", 1_000_000, 450_000) == 2_222_222
        with pytest.raises(InvalidOrder):
            v3.quote_size("lots", 1, 1)

    def test_bad_quotes(self):
        for args in (("BUY", 0, 1), ("BUY", 1_000_000, 1), ("SELL", 1, 0), ("HOLD", 1, 1)):
            with pytest.raises(InvalidOrder):
                v3.maker_quote_amounts(*args)

    def test_implied_price(self):
        assert str(v3.implied_price(v3.BUY, 500_000, 1_000_000)) == "0.5"
        assert str(v3.implied_price(v3.SELL, 1_000_000, 450_000)) == "0.45"

    def test_approval_calls(self):
        calls = v3.combo_approval_calls()
        assert [c["target"] for c in calls] == [v3.PUSD, v3.POSITION_MANAGER]
        assert calls[0]["data"] == v2.approve_calldata(v3.EXCHANGE_V3)
        assert calls[1]["data"] == v2.set_approval_for_all_calldata(v3.EXCHANGE_V3)


class TestClobV2Unchanged:
    @pytest.mark.parametrize("vector", V2_VECTORS["orders"],
                             ids=lambda v: f"type{v['signature_type']}-{'negrisk' if v['neg_risk'] else 'std'}-{v['side']}")
    def test_v2_vectors_still_match(self, vector):
        """Loading the v3 module changes nothing about CLOB V2 orders."""
        order = v2.build_order(
            maker=vector["maker"], signer=vector["signer"], token_id=vector["token_id"],
            maker_amount=vector["maker_amount"], taker_amount=vector["taker_amount"], side=vector["side"],
            signature_type=vector["signature_type"], timestamp_ms=vector["timestamp"], salt=vector["salt"],
            expiration=int(vector["expiration"]), builder=vector["builder"],
        )
        assert v2.sign_order(v2.WalletSigner(V2_VECTORS["private_key"]), order, neg_risk=vector["neg_risk"]) == vector["signature"]
        assert v2.order_hash(order, neg_risk=vector["neg_risk"]) == vector["hash"]
        assert v2.EXCHANGE_DOMAIN_VERSION == "2"
