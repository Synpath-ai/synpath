"""Order signing on the Rust core against `eth_account`.

The venues' own vectors (`test_polymarket_trading`, `test_opinion_trading`)
pin the signatures; they run on whichever path is installed. These tests
sign the same random orders both ways -- the Rust core, and the
`eth_account` encoder it replaces -- and require the same bytes, and the
same error for an order the encoder refuses.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from synpath import _native
from synpath.trading import opinion_signing as osig
from synpath.trading import polymarket_signing as sig

pytestmark = pytest.mark.skipif(_native.core is None, reason="the Rust core is not built")

VECTORS = json.loads((Path(__file__).parent / "samples" / "polymarket_signing_vectors.json").read_text())
KEY = VECTORS["private_key"]


def python_signer(key: str) -> sig.WalletSigner:
    signer = sig.WalletSigner(key)
    signer._native = None
    return signer


@pytest.fixture
def no_core(monkeypatch):
    """`order_hash` with the core switched off, for the Python answer."""
    def python_hash(order, *, neg_risk):
        monkeypatch.setattr(_native, "core", None)
        try:
            return sig.order_hash(order, neg_risk=neg_risk)
        finally:
            monkeypatch.undo()
    return python_hash


def random_address(rng: random.Random, *, case: str) -> str:
    raw = "".join(rng.choice("0123456789abcdef") for _ in range(40))
    if case == "lower":
        return "0x" + raw
    if case == "upper":
        return "0x" + raw.upper()
    from eth_utils import to_checksum_address
    return to_checksum_address("0x" + raw)


def random_key(rng: random.Random) -> str:
    return "0x" + "".join(rng.choice("0123456789abcdef") for _ in range(64))


def polymarket_order(rng: random.Random, signer: sig.WalletSigner, signature_type: int) -> dict:
    maker = signer.address if signature_type == sig.EOA else random_address(rng, case=rng.choice(["lower", "checksum", "upper"]))
    return sig.build_order(
        maker=maker, signer=signer.address, token_id=str(rng.getrandbits(rng.choice([8, 64, 255]))),
        maker_amount=rng.randint(1, 10**12), taker_amount=rng.randint(1, 10**12),
        side=rng.choice(["BUY", "SELL"]), signature_type=signature_type,
        timestamp_ms=1789659362000 + rng.randint(0, 10**9), salt=sig.new_salt(),
        metadata=rng.choice([sig.ZERO_BYTES32, "0x" + "ab" * 32, "0x01"]),
        builder=rng.choice([None, "0x" + "cd" * 32]),
    )


class TestPolymarket:
    @pytest.mark.parametrize("seed", range(40))
    def test_random_orders_sign_alike(self, seed, no_core):
        rng = random.Random(seed)
        key = random_key(rng) if seed % 2 else KEY
        rust, python = sig.WalletSigner(key), python_signer(key)
        assert rust._native is not None and rust.address == python.address
        for signature_type in (sig.EOA, sig.POLY_PROXY, sig.POLY_GNOSIS_SAFE, sig.DEPOSIT_WALLET):
            order = polymarket_order(rng, rust, signature_type)
            for neg_risk in (False, True):
                assert sig.sign_order(rust, order, neg_risk=neg_risk) == sig.sign_order(python, order, neg_risk=neg_risk)
                assert sig.order_hash(order, neg_risk=neg_risk) == no_core(order, neg_risk=neg_risk)

    def test_digests_sign_alike(self):
        rng = random.Random(7)
        rust, python = sig.WalletSigner(KEY), python_signer(KEY)
        for _ in range(50):
            digest = rng.randbytes(32)
            assert rust.sign_digest(digest) == python.sign_digest(digest)

    @pytest.mark.parametrize("change", [
        {"maker": "0xF39fd6e51aad88F6F4ce6aB8827279cffFb92266"},       # a broken checksum
        {"maker": "0x1234"},                                             # too short
        {"tokenId": "-5"},
        {"tokenId": str(2**256)},
        {"metadata": "0x" + "00" * 33},
        {"signatureType": 300},
    ])
    def test_a_refused_order_fails_as_it_always_did(self, change, no_core):
        signer = sig.WalletSigner(KEY)
        order = {**polymarket_order(random.Random(1), signer, sig.POLY_GNOSIS_SAFE), **change}

        def outcome(call):
            try:
                return "ok", call()
            except Exception as exc:  # noqa: BLE001 - comparing whatever each path raises
                return type(exc).__name__, None

        assert outcome(lambda: sig.sign_order(signer, order, neg_risk=False)) == \
            outcome(lambda: sig.sign_order(python_signer(KEY), order, neg_risk=False))
        assert outcome(lambda: sig.order_hash(order, neg_risk=False)) == outcome(lambda: no_core(order, neg_risk=False))


class TestOpinion:
    EXCHANGE = "0x5F45344126D6488025B0b84A3A8189F2487a7246"

    @pytest.mark.parametrize("seed", range(40))
    def test_random_orders_sign_alike(self, seed):
        rng = random.Random(seed)
        key = random_key(rng)
        rust, python = sig.WalletSigner(key), python_signer(key)
        price = rng.choice(["0.5", "0.123", "0.999", "0.001", "0.42"])
        side = rng.choice(["BUY", "SELL"])
        maker, taker = osig.limit_amounts(side, osig.Decimal(price), rng.randint(10**18, 10**21))
        order = osig.build_order(
            maker=random_address(rng, case=rng.choice(["lower", "checksum", "upper"])), signer=rust.address,
            token_id=str(rng.getrandbits(255)), maker_amount=maker, taker_amount=taker, side=side, salt=osig.new_salt(),
        )
        exchange = rng.choice([self.EXCHANGE, self.EXCHANGE.lower()])
        assert osig.sign_order(rust, order, exchange=exchange) == osig.sign_order(python, order, exchange=exchange)

    def test_the_rust_path_is_taken(self, monkeypatch):
        signer = sig.WalletSigner(KEY)
        monkeypatch.setattr(sig.WalletSigner, "sign_typed_data", lambda *a, **k: pytest.fail("signed by eth_account"))
        order = osig.build_order(maker=signer.address, signer=signer.address, token_id="1", maker_amount=10,
                                 taker_amount=20, side="BUY", salt=1)
        assert osig.sign_order(signer, order, exchange=self.EXCHANGE).startswith("0x")
        poly = polymarket_order(random.Random(2), signer, sig.POLY_GNOSIS_SAFE)
        assert sig.sign_order(signer, poly, neg_risk=False).startswith("0x")
