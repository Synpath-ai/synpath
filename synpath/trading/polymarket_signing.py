"""Polymarket CLOB V2 signing, with no network in sight.

Everything that turns intent into bytes lives here, so it can be checked
byte for byte against the venue's own client (`py-clob-client-v2`) without
a wallet, a balance, or a request:

* **L1** -- the `ClobAuth` EIP-712 message that proves control of the signer
  and creates or derives the CLOB API credentials.
* **L2** -- the HMAC-SHA256 over `timestamp + METHOD + path + body` that
  authenticates every private request.
* **Orders** -- the V2 `Order` struct (domain version `"2"`, no nonce, no
  fee, a millisecond `timestamp`, `metadata` and `builder` bytes32), its
  amounts rounded exactly as the venue requires, signed directly for an EOA,
  proxy or Safe wallet, or wrapped for ERC-7739 validation for a Deposit
  Wallet.
* **Wallet calls** -- ABI-encoded approvals, split, merge and redeem, and
  the Deposit Wallet `Batch` the relayer executes gaslessly.

Money is `Decimal` throughout. The venue's client computes amounts in
floating point; the rounding rules are the same, and the vectors in the
tests pin that both agree on every case they cover.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any, Literal

from .errors import InvalidOrder

CHAIN_ID = 137

EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF_COLLATERAL_ADAPTER = "0xAdA100Db00Ca00073811820692005400218FcE1f"
NEG_RISK_CTF_COLLATERAL_ADAPTER = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"
DEPOSIT_WALLET_FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
"""Addresses from the venue's contract page, Polygon mainnet, CLOB V2."""

ZERO_BYTES32 = "0x" + "00" * 32
MAX_UINT256 = 2**256 - 1

EXCHANGE_DOMAIN_NAME = "Polymarket CTF Exchange"
EXCHANGE_DOMAIN_VERSION = "2"
CLOB_AUTH_DOMAIN = {"name": "ClobAuthDomain", "version": "1", "chainId": CHAIN_ID}
CLOB_AUTH_MESSAGE = "This message attests that I control the given wallet"

ORDER_TYPE = (
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
)
ORDER_FIELDS = [
    {"name": "salt", "type": "uint256"},
    {"name": "maker", "type": "address"},
    {"name": "signer", "type": "address"},
    {"name": "tokenId", "type": "uint256"},
    {"name": "makerAmount", "type": "uint256"},
    {"name": "takerAmount", "type": "uint256"},
    {"name": "side", "type": "uint8"},
    {"name": "signatureType", "type": "uint8"},
    {"name": "timestamp", "type": "uint256"},
    {"name": "metadata", "type": "bytes32"},
    {"name": "builder", "type": "bytes32"},
]
DOMAIN_FIELDS = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]

EOA, POLY_PROXY, POLY_GNOSIS_SAFE, DEPOSIT_WALLET = 0, 1, 2, 3

SCALE = Decimal(10) ** 6
"""pUSD and outcome tokens both carry six decimals."""


def _eth():
    try:
        from eth_abi import encode as abi_encode
        from eth_account import Account
        from eth_account.messages import encode_typed_data
        from eth_utils import keccak, to_checksum_address
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError("Polymarket signing needs eth-account: pip install synpath") from exc
    return abi_encode, Account, encode_typed_data, keccak, to_checksum_address


# ---------------------------------------------------------------------------
# The signer
# ---------------------------------------------------------------------------

class WalletSigner:
    """Holds the private key. Nothing else in the package sees it."""

    def __init__(self, private_key: str):
        _, Account, _, _, _ = _eth()
        self._account = Account.from_key(private_key)

    @property
    def address(self) -> str:
        return self._account.address

    def sign_typed_data(self, typed: dict[str, Any]) -> str:
        _, Account, encode_typed_data, _, _ = _eth()
        signed = Account.sign_message(encode_typed_data(full_message=typed), private_key=self._account.key)
        return "0x" + signed.signature.hex().removeprefix("0x")

    def sign_digest(self, digest: bytes) -> bytes:
        _, Account, _, _, _ = _eth()
        return bytes(Account._sign_hash(digest, private_key=self._account.key).signature)

    def sign_transaction(self, tx: dict[str, Any]) -> str:
        signed = self._account.sign_transaction(tx)
        raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
        return "0x" + bytes(raw).hex()


# ---------------------------------------------------------------------------
# L1 and L2 authentication
# ---------------------------------------------------------------------------

def clob_auth_typed_data(address: str, timestamp: int, nonce: int = 0) -> dict[str, Any]:
    return {
        "primaryType": "ClobAuth",
        "types": {
            "EIP712Domain": DOMAIN_FIELDS[:3],
            "ClobAuth": [
                {"name": "address", "type": "address"},
                {"name": "timestamp", "type": "string"},
                {"name": "nonce", "type": "uint256"},
                {"name": "message", "type": "string"},
            ],
        },
        "domain": CLOB_AUTH_DOMAIN,
        "message": {"address": address, "timestamp": str(timestamp), "nonce": nonce, "message": CLOB_AUTH_MESSAGE},
    }


def l1_headers(signer: WalletSigner, timestamp: int, nonce: int = 0) -> dict[str, str]:
    """Headers for `POST /auth/api-key` and `GET /auth/derive-api-key`."""
    return {
        "POLY_ADDRESS": signer.address,
        "POLY_SIGNATURE": signer.sign_typed_data(clob_auth_typed_data(signer.address, timestamp, nonce)),
        "POLY_TIMESTAMP": str(timestamp),
        "POLY_NONCE": str(nonce),
    }


def l2_signature(secret: str, timestamp: int, method: str, path: str, body: str | None = None) -> str:
    """URL-safe base64 of HMAC-SHA256(base64(secret), ts + METHOD + path + body).

    `path` is the route without its query string; `body` is the exact text
    sent on the wire. The secret decodes as URL-safe base64, which also reads
    the standard alphabet the venue issues.
    """
    key = base64.urlsafe_b64decode(secret)
    message = f"{timestamp}{method.upper()}{path}{body or ''}"
    digest = hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8")


def l2_headers(
    *, address: str, api_key: str, secret: str, passphrase: str,
    timestamp: int, method: str, path: str, body: str | None = None,
) -> dict[str, str]:
    return {
        "POLY_ADDRESS": address,
        "POLY_SIGNATURE": l2_signature(secret, timestamp, method, path, body),
        "POLY_TIMESTAMP": str(timestamp),
        "POLY_API_KEY": api_key,
        "POLY_PASSPHRASE": passphrase,
    }


# ---------------------------------------------------------------------------
# Order amounts
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RoundConfig:
    price: int
    size: int
    amount: int


ROUNDING: dict[str, RoundConfig] = {
    "0.1": RoundConfig(price=1, size=2, amount=3),
    "0.01": RoundConfig(price=2, size=2, amount=4),
    "0.005": RoundConfig(price=3, size=2, amount=5),
    "0.0025": RoundConfig(price=4, size=2, amount=6),
    "0.001": RoundConfig(price=3, size=2, amount=5),
    "0.0001": RoundConfig(price=4, size=2, amount=6),
}
"""The venue's precision table: decimals for price, share size and the
USD amount at each tick size."""


def round_config(tick: Decimal) -> RoundConfig:
    key = format(tick.normalize(), "f")
    try:
        return ROUNDING[key]
    except KeyError:
        raise InvalidOrder(f"polymarket: no precision rule for tick size {tick}; known {sorted(ROUNDING)}") from None


def _places(value: Decimal) -> int:
    exponent = value.normalize().as_tuple().exponent
    return max(0, -exponent) if isinstance(exponent, int) else 0


def _q(value: Decimal, places: int, rounding: str) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=rounding)


def _fit_amount(value: Decimal, config: RoundConfig) -> Decimal:
    """The venue's rule for the derived USD (or share) amount: if it carries
    more decimals than allowed, round up at four extra places, then down."""
    if _places(value) > config.amount:
        value = _q(value, config.amount + 4, ROUND_CEILING)
        if _places(value) > config.amount:
            value = _q(value, config.amount, ROUND_FLOOR)
    return value


def to_raw(value: Decimal) -> int:
    """A six-decimal integer; half-up at the last place, as the venue's client."""
    return int((value * SCALE).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def limit_amounts(side: Literal["BUY", "SELL"], price: Decimal, size: Decimal, tick: Decimal) -> tuple[int, int]:
    """`(makerAmount, takerAmount)` for a limit order of `size` shares at `price`.

    A BUY gives `price * size` USD for `size` shares; a SELL gives `size`
    shares for `price * size` USD. The size is rounded down to two places
    and the price to the tick's places, exactly as the venue does. Callers
    validate first, so neither rounding changes an order that was typed on
    the grid.
    """
    config = round_config(tick)
    price = _q(price, config.price, ROUND_HALF_UP)
    shares = _q(size, config.size, ROUND_FLOOR)
    usd = _fit_amount(shares * price, config)
    if side == "BUY":
        return to_raw(usd), to_raw(shares)
    return to_raw(shares), to_raw(usd)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def new_salt() -> int:
    """A random salt that survives a JSON number in JavaScript clients."""
    return secrets.randbelow(2**53 - 1)


def exchange_for(neg_risk: bool) -> str:
    return NEG_RISK_EXCHANGE if neg_risk else EXCHANGE


def build_order(
    *, maker: str, signer: str, token_id: str, maker_amount: int, taker_amount: int,
    side: Literal["BUY", "SELL"], signature_type: int, timestamp_ms: int, salt: int,
    expiration: int = 0, metadata: str = ZERO_BYTES32, builder: str | None = None,
) -> dict[str, Any]:
    """The order as the wire carries it, before the signature."""
    return {
        "salt": int(salt),
        "maker": maker,
        "signer": signer,
        "tokenId": str(token_id),
        "makerAmount": str(maker_amount),
        "takerAmount": str(taker_amount),
        "side": side,
        "expiration": str(expiration),
        "signatureType": int(signature_type),
        "timestamp": str(timestamp_ms),
        "metadata": metadata,
        "builder": builder or ZERO_BYTES32,
    }


def _bytes32(value: str) -> bytes:
    return bytes.fromhex(value.removeprefix("0x").zfill(64))


def order_typed_data(order: dict[str, Any], *, neg_risk: bool) -> dict[str, Any]:
    return {
        "primaryType": "Order",
        "types": {"EIP712Domain": DOMAIN_FIELDS, "Order": ORDER_FIELDS},
        "domain": {
            "name": EXCHANGE_DOMAIN_NAME, "version": EXCHANGE_DOMAIN_VERSION,
            "chainId": CHAIN_ID, "verifyingContract": exchange_for(neg_risk),
        },
        "message": {
            "salt": int(order["salt"]),
            "maker": order["maker"],
            "signer": order["signer"],
            "tokenId": int(order["tokenId"]),
            "makerAmount": int(order["makerAmount"]),
            "takerAmount": int(order["takerAmount"]),
            "side": 0 if order["side"] == "BUY" else 1,
            "signatureType": int(order["signatureType"]),
            "timestamp": int(order["timestamp"]),
            "metadata": _bytes32(order["metadata"]),
            "builder": _bytes32(order["builder"]),
        },
    }


def order_hash(order: dict[str, Any], *, neg_risk: bool) -> str:
    """The EIP-712 digest of the order, which is the id the CLOB gives it.

    Known before the order is sent, so a journal can record the id first
    and a lost response is recoverable by asking for that id.
    """
    _, _, encode_typed_data, keccak, _ = _eth()
    message = encode_typed_data(full_message=order_typed_data(order, neg_risk=neg_risk))
    return "0x" + keccak(b"\x19" + message.version + message.header + message.body).hex().removeprefix("0x")


def _app_domain_separator(neg_risk: bool) -> bytes:
    abi_encode, _, _, keccak, _ = _eth()
    return keccak(abi_encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [
            keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
            keccak(text=EXCHANGE_DOMAIN_NAME), keccak(text=EXCHANGE_DOMAIN_VERSION),
            CHAIN_ID, exchange_for(neg_risk),
        ],
    ))


def _order_contents_hash(order: dict[str, Any]) -> bytes:
    abi_encode, _, _, keccak, _ = _eth()
    return keccak(abi_encode(
        ["bytes32", "uint256", "address", "address", "uint256", "uint256", "uint256",
         "uint8", "uint8", "uint256", "bytes32", "bytes32"],
        [
            keccak(text=ORDER_TYPE), int(order["salt"]), order["maker"], order["signer"],
            int(order["tokenId"]), int(order["makerAmount"]), int(order["takerAmount"]),
            0 if order["side"] == "BUY" else 1, int(order["signatureType"]), int(order["timestamp"]),
            _bytes32(order["metadata"]), _bytes32(order["builder"]),
        ],
    ))


def sign_order(signer: WalletSigner, order: dict[str, Any], *, neg_risk: bool) -> str:
    """The order's signature for its wallet type.

    Types 0-2 sign the Exchange `Order` typed data directly. A Deposit Wallet
    (type 3) signs Solady's `TypedDataSign` envelope -- the order nested in
    the wallet's own `DepositWallet` domain -- and the result is wrapped as
    ERC-7739 expects: inner signature, app domain separator, contents hash,
    the contents type string and its two-byte length.
    """
    if int(order["signatureType"]) != DEPOSIT_WALLET:
        return signer.sign_typed_data(order_typed_data(order, neg_risk=neg_risk))
    abi_encode, _, _, keccak, _ = _eth()
    separator = _app_domain_separator(neg_risk)
    contents = _order_contents_hash(order)
    envelope = keccak(abi_encode(
        ["bytes32", "bytes32", "bytes32", "bytes32", "uint256", "address", "bytes32"],
        [
            keccak(text=(
                "TypedDataSign(Order contents,string name,string version,uint256 chainId,"
                "address verifyingContract,bytes32 salt)" + ORDER_TYPE
            )),
            contents, keccak(text="DepositWallet"), keccak(text="1"), CHAIN_ID, order["signer"],
            b"\x00" * 32,
        ],
    ))
    inner = signer.sign_digest(keccak(b"\x19\x01" + separator + envelope))
    return "0x" + (
        inner + separator + contents + ORDER_TYPE.encode() + len(ORDER_TYPE).to_bytes(2, "big")
    ).hex()


# ---------------------------------------------------------------------------
# Wallet calls
# ---------------------------------------------------------------------------

def _call(signature: str, types: list[str], values: list[Any]) -> str:
    abi_encode, _, _, keccak, _ = _eth()
    return "0x" + (keccak(text=signature)[:4] + abi_encode(types, values)).hex()


def approve_calldata(spender: str, amount: int = MAX_UINT256) -> str:
    return _call("approve(address,uint256)", ["address", "uint256"], [spender, amount])


def set_approval_for_all_calldata(operator: str, approved: bool = True) -> str:
    return _call("setApprovalForAll(address,bool)", ["address", "bool"], [operator, approved])


def _condition(condition_id: str) -> bytes:
    raw = condition_id.removeprefix("0x")
    if len(raw) != 64:
        raise InvalidOrder(f"polymarket: {condition_id!r} is not a condition id (0x + 64 hex)")
    return bytes.fromhex(raw)


def split_calldata(condition_id: str, amount_raw: int) -> str:
    return _call(
        "splitPosition(address,bytes32,bytes32,uint256[],uint256)",
        ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
        [PUSD, b"\x00" * 32, _condition(condition_id), [1, 2], amount_raw],
    )


def merge_calldata(condition_id: str, amount_raw: int) -> str:
    return _call(
        "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
        ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
        [PUSD, b"\x00" * 32, _condition(condition_id), [1, 2], amount_raw],
    )


def redeem_calldata(condition_id: str) -> str:
    return _call(
        "redeemPositions(address,bytes32,bytes32,uint256[])",
        ["address", "bytes32", "bytes32", "uint256[]"],
        [PUSD, b"\x00" * 32, _condition(condition_id), [1, 2]],
    )


def collateral_adapter_for(neg_risk: bool) -> str:
    return NEG_RISK_CTF_COLLATERAL_ADAPTER if neg_risk else CTF_COLLATERAL_ADAPTER


def trading_approval_calls(spenders: list[str] | None = None) -> list[dict[str, str]]:
    """Unlimited pUSD and conditional-token approvals for each spender: by
    default both exchanges (trading) and both collateral adapters (split,
    merge, redeem)."""
    spenders = spenders or [EXCHANGE, NEG_RISK_EXCHANGE, CTF_COLLATERAL_ADAPTER, NEG_RISK_CTF_COLLATERAL_ADAPTER]
    calls = [{"target": PUSD, "value": "0", "data": approve_calldata(spender)} for spender in spenders]
    calls += [{"target": CONDITIONAL_TOKENS, "value": "0", "data": set_approval_for_all_calldata(spender)} for spender in spenders]
    return calls


def wallet_batch_typed_data(wallet: str, nonce: int, deadline: int, calls: list[dict[str, str]]) -> dict[str, Any]:
    """The Deposit Wallet `Batch` the relayer executes: every call, in order,
    authorised by one signature that expires at `deadline`."""
    return {
        "primaryType": "Batch",
        "types": {
            "EIP712Domain": DOMAIN_FIELDS,
            "Call": [
                {"name": "target", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"},
            ],
            "Batch": [
                {"name": "wallet", "type": "address"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
                {"name": "calls", "type": "Call[]"},
            ],
        },
        "domain": {"name": "DepositWallet", "version": "1", "chainId": CHAIN_ID, "verifyingContract": wallet},
        "message": {
            "wallet": wallet,
            "nonce": int(nonce),
            "deadline": int(deadline),
            "calls": [
                {"target": c["target"], "value": int(c["value"]), "data": bytes.fromhex(c["data"].removeprefix("0x"))}
                for c in calls
            ],
        },
    }
