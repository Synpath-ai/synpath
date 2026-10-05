"""predict.fun order signing, with no network in sight.

predict.fun's exchanges are CTF exchanges on BNB Chain, and its order is the
original CTF `Order` struct (salt, maker, signer, taker, tokenId,
makerAmount, takerAmount, expiration, nonce, feeRateBps, side,
signatureType) under the domain `predict.fun CTF Exchange`, version `1`.
Which of four exchanges verifies it depends on the market: neg-risk or not,
yield-bearing or not.

The EIP-712 digest is the order's hash, the id the venue's lookups and
cancels take. It is signed one of two ways:

* **A plain wallet (EOA)** signs the digest itself; it is maker and signer.
* **A Predict account** (the smart wallet predict.fun makes for a web
  user, a Kernel 0.3.1 account) is maker and signer, and its owner key
  signs for it: the digest is wrapped in the account's own EIP-712 domain
  (`Kernel`, `0.3.1`, the account as verifying contract), signed as a
  personal message, and prefixed with `0x01` and the ECDSA validator's
  address.

The login message is signed the same two ways, as text. Amounts are
18-decimal integers, built as the venue's SDK (`predict-sdk` 0.0.22) builds
them: the price kept to three significant digits, the size to five.
"""
from __future__ import annotations

import secrets
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal

from .errors import InvalidOrder
from .polymarket_signing import WalletSigner, _eth

DOMAIN_NAME = "predict.fun CTF Exchange"
DOMAIN_VERSION = "1"
KERNEL_NAME = "Kernel"
KERNEL_VERSION = "0.3.1"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
SCALE = 10**18
BUY, SELL = 0, 1
EOA = 0
"""The only signature type the venue takes; a Predict account signs under it
too, through EIP-1271."""

MAX_SALT = 2_147_483_648
NO_EXPIRY = 4_102_444_800
"""2100-01-01, the SDK's expiration for an order that rests until cancelled."""

MIN_SHARES_WEI = 10**16
"""The SDK's smallest order: 0.01 shares."""


@dataclass(frozen=True)
class Network:
    chain_id: int
    ctf_exchange: str
    neg_risk_ctf_exchange: str
    yield_bearing_ctf_exchange: str
    yield_bearing_neg_risk_ctf_exchange: str
    usdt: str
    ecdsa_validator: str

    def exchange(self, *, neg_risk: bool, yield_bearing: bool) -> str:
        if neg_risk:
            return self.yield_bearing_neg_risk_ctf_exchange if yield_bearing else self.neg_risk_ctf_exchange
        return self.yield_bearing_ctf_exchange if yield_bearing else self.ctf_exchange


MAINNET = Network(
    chain_id=56,
    ctf_exchange="0x8BC070BEdAB741406F4B1Eb65A72bee27894B689",
    neg_risk_ctf_exchange="0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A",
    yield_bearing_ctf_exchange="0x6bEb5a40C032AFc305961162d8204CDA16DECFa5",
    yield_bearing_neg_risk_ctf_exchange="0x8A289d458f5a134bA40015085A8F50Ffb681B41d",
    usdt="0x55d398326f99059fF775485246999027B3197955",
    ecdsa_validator="0x845ADb2C711129d4f3966735eD98a9F09fC4cE57",
)
TESTNET = Network(
    chain_id=97,
    ctf_exchange="0x2A6413639BD3d73a20ed8C95F634Ce198ABbd2d7",
    neg_risk_ctf_exchange="0xd690b2bd441bE36431F6F6639D7Ad351e7B29680",
    yield_bearing_ctf_exchange="0x8a6B4Fa700A1e310b106E7a48bAFa29111f66e89",
    yield_bearing_neg_risk_ctf_exchange="0x95D5113bc50eD201e319101bbca3e0E250662fCC",
    usdt="0xB32171ecD878607FFc4F8FC0bCcE6852BB3149E0",
    ecdsa_validator="0x845ADb2C711129d4f3966735eD98a9F09fC4cE57",
)

ORDER_TYPE = (
    "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,"
    "uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)"
)
DOMAIN_TYPE = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
KERNEL_TYPE = "Kernel(bytes32 hash)"


# ---------------------------------------------------------------------------
# Amounts
# ---------------------------------------------------------------------------

def significant(value: int, digits: int) -> int:
    """`value` cut (not rounded) to `digits` significant digits, as the SDK
    cuts it."""
    if value == 0:
        return 0
    excess = len(str(abs(value))) - digits
    if excess <= 0:
        return value
    divisor = 10**excess
    cut = (abs(value) // divisor) * divisor
    return -cut if value < 0 else cut


def to_wei(value: Decimal) -> int:
    return int((Decimal(str(value)) * SCALE).quantize(Decimal("1"), rounding=ROUND_DOWN))


def from_wei(value: Any) -> Decimal:
    return Decimal(int(value)) / SCALE


def limit_amounts(side: Literal["BUY", "SELL"], price: Decimal, shares: Decimal) -> tuple[int, int, int, int]:
    """`(makerAmount, takerAmount, price_wei, shares_wei)` for a limit order
    on one token, as the SDK's `getLimitOrderAmounts` makes them: the price
    kept to three significant digits, the size to five, and the collateral
    side `price * shares`, rounded down."""
    price_wei = significant(to_wei(price), 3)
    shares_wei = significant(to_wei(shares), 5)
    if price_wei <= 0 or price_wei >= SCALE:
        raise InvalidOrder(f"predict_fun: price {price} is outside (0, 1)")
    if shares_wei < MIN_SHARES_WEI:
        raise InvalidOrder(f"predict_fun: {shares} shares is below the venue's minimum of 0.01")
    value = price_wei * shares_wei // SCALE
    if side == "BUY":
        return value, shares_wei, price_wei, shares_wei
    return shares_wei, value, price_wei, shares_wei


def new_salt() -> int:
    return secrets.randbelow(MAX_SALT + 1)


# ---------------------------------------------------------------------------
# The order
# ---------------------------------------------------------------------------

def build_order(
    *, maker: str, token_id: str, maker_amount: int, taker_amount: int, side: Literal["BUY", "SELL"],
    fee_rate_bps: int, salt: int, expiration: int = NO_EXPIRY,
) -> dict[str, Any]:
    """The order struct's values, before the signature. `maker` is the
    signer too: the wallet, or the Predict account its key signs for."""
    return {
        "salt": int(salt),
        "maker": maker,
        "signer": maker,
        "taker": ZERO_ADDRESS,
        "tokenId": int(token_id),
        "makerAmount": int(maker_amount),
        "takerAmount": int(taker_amount),
        "expiration": int(expiration),
        "nonce": 0,
        "feeRateBps": int(fee_rate_bps),
        "side": BUY if side == "BUY" else SELL,
        "signatureType": EOA,
    }


def _domain_separator(name: str, version: str, chain_id: int, contract: str) -> bytes:
    abi_encode, _, _, keccak, to_checksum_address = _eth()
    return keccak(abi_encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [keccak(text=DOMAIN_TYPE), keccak(text=name), keccak(text=version), chain_id, to_checksum_address(contract)],
    ))


def _struct_hash(order: dict[str, Any]) -> bytes:
    abi_encode, _, _, keccak, to_checksum_address = _eth()
    return keccak(abi_encode(
        ["bytes32", "uint256", "address", "address", "address", "uint256", "uint256", "uint256",
         "uint256", "uint256", "uint256", "uint8", "uint8"],
        [
            keccak(text=ORDER_TYPE), int(order["salt"]), to_checksum_address(order["maker"]),
            to_checksum_address(order["signer"]), to_checksum_address(order["taker"]), int(order["tokenId"]),
            int(order["makerAmount"]), int(order["takerAmount"]), int(order["expiration"]),
            int(order["nonce"]), int(order["feeRateBps"]), int(order["side"]), int(order["signatureType"]),
        ],
    ))


def order_digest(order: dict[str, Any], *, exchange: str, chain_id: int) -> bytes:
    """The order's EIP-712 digest: its hash on the venue."""
    _, _, _, keccak, _ = _eth()
    domain = _domain_separator(DOMAIN_NAME, DOMAIN_VERSION, chain_id, exchange)
    return keccak(b"\x19\x01" + domain + _struct_hash(order))


def order_typed_data(order: dict[str, Any], *, exchange: str, chain_id: int) -> dict[str, Any]:
    """The same order as EIP-712 typed data, for wallets that sign that."""
    from eth_utils import to_checksum_address

    message = dict(order)
    for key in ("maker", "signer", "taker"):
        message[key] = to_checksum_address(message[key])
    fields = [p.split(" ") for p in ORDER_TYPE[len("Order("):-1].split(",")]
    return {
        "primaryType": "Order",
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"},
            ],
            "Order": [{"name": name, "type": kind} for kind, name in fields],
        },
        "domain": {
            "name": DOMAIN_NAME, "version": DOMAIN_VERSION, "chainId": chain_id,
            "verifyingContract": to_checksum_address(exchange),
        },
        "message": message,
    }


# ---------------------------------------------------------------------------
# Signatures
# ---------------------------------------------------------------------------

def _personal_digest(data: bytes) -> bytes:
    _, _, _, keccak, _ = _eth()
    return keccak(b"\x19Ethereum Signed Message:\n" + str(len(data)).encode() + data)


def _hex(signature: bytes) -> str:
    return "0x" + bytes(signature).hex()


def kernel_digest(message_hash: bytes, *, account: str, chain_id: int) -> bytes:
    """`message_hash` wrapped for a Kernel account: `Kernel(bytes32 hash)`
    under the account's own domain."""
    abi_encode, _, _, keccak, _ = _eth()
    domain = _domain_separator(KERNEL_NAME, KERNEL_VERSION, chain_id, account)
    inner = keccak(abi_encode(["bytes32", "bytes32"], [keccak(text=KERNEL_TYPE), message_hash]))
    return keccak(b"\x19\x01" + domain + inner)


def sign_for_account(signer: WalletSigner, message_hash: bytes, *, account: str, network: Network) -> str:
    """The owner key's signature for a Predict account: the wrapped hash
    signed as a personal message, behind `0x01` and the validator."""
    wrapped = kernel_digest(message_hash, account=account, chain_id=network.chain_id)
    signature = signer.sign_digest(_personal_digest(wrapped))
    return "0x01" + network.ecdsa_validator[2:] + bytes(signature).hex()


def sign_order(
    signer: WalletSigner, order: dict[str, Any], *, exchange: str, network: Network, account: str | None = None,
) -> tuple[str, str]:
    """`(signature, hash)` for an order: signed by the wallet, or for the
    Predict account `account`."""
    digest = order_digest(order, exchange=exchange, chain_id=network.chain_id)
    if account:
        signature = sign_for_account(signer, digest, account=account, network=network)
    else:
        signature = _hex(signer.sign_digest(digest))
    return signature, "0x" + digest.hex()


def sign_login(signer: WalletSigner, message: str, *, network: Network, account: str | None = None) -> str:
    """The signature `POST /auth` takes for the venue's login message."""
    digest = _personal_digest(message.encode())
    if account:
        return sign_for_account(signer, digest, account=account, network=network)
    return _hex(signer.sign_digest(digest))
