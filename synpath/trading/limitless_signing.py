"""Limitless order signing and request authentication, with no network in sight.

**Orders.** Limitless's exchanges are CTF exchanges on Base, and its order is
the original CTF `Order` struct (salt, maker, signer, taker, tokenId,
makerAmount, takerAmount, expiration, nonce, feeRateBps, side, signatureType)
under the domain `Limitless CTF Exchange`, version `1`, chain 8453, with the
market's exchange (`venue.exchange`) as the verifying contract. The wallet
signs its own orders (signature type 0, maker = signer). Expiration and nonce
must be 0; the fee rate must be the profile's.

**Amounts** are 6-decimal integers. A limit order's price has at most three
decimals and `price * contracts` must come out as a whole number of units, so
contracts are counted in thousandths of a share.

**Requests** are signed with the scoped API token: HMAC-SHA256, under the
base64-decoded secret, of `{ISO timestamp}\\n{METHOD}\\n{path and query}\\n{body}`.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal

from .errors import InvalidOrder
from .polymarket_signing import WalletSigner, _eth
from .predict_fun_signing import ORDER_TYPE, _domain_separator, _struct_hash

DOMAIN_NAME = "Limitless CTF Exchange"
DOMAIN_VERSION = "1"
CHAIN_ID = 8453
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
SCALE = 1_000_000
BUY, SELL = 0, 1
EOA = 0

MIN_PRICE, MAX_PRICE = Decimal("0.01"), Decimal("0.99")
MIN_MAKER_UNITS = 100
"""The venue's dust floor on `makerAmount`, in raw units."""


def contracts_units(contracts: Decimal) -> int:
    """`contracts` in raw units, cut to thousandths of a share, so any
    three-decimal price times it is a whole number of units."""
    thousandths = int((Decimal(str(contracts)) * 1000).quantize(Decimal("1"), rounding=ROUND_DOWN))
    return thousandths * 1000


def limit_amounts(side: Literal["BUY", "SELL"], price: Decimal, contracts: Decimal) -> tuple[int, int, int]:
    """`(makerAmount, takerAmount, contract_units)` for a limit order on one
    token: the collateral side is `price * contracts`, exact."""
    price = Decimal(str(price))
    if not MIN_PRICE <= price <= MAX_PRICE:
        raise InvalidOrder(f"limitless: price {price} is outside [{MIN_PRICE}, {MAX_PRICE}]")
    if price != price.quantize(Decimal("0.001")):
        raise InvalidOrder(f"limitless: price {price} has more than three decimal places")
    units = contracts_units(contracts)
    collateral = units * int(price * 1000) // 1000
    maker, taker = (collateral, units) if side == "BUY" else (units, collateral)
    if maker < MIN_MAKER_UNITS:
        raise InvalidOrder(f"limitless: {contracts} contracts at {price} is below the venue's minimum order")
    return maker, taker, units


def new_salt() -> int:
    return secrets.randbelow(2**53)


def build_order(
    *, maker: str, token_id: str, maker_amount: int, taker_amount: int, side: Literal["BUY", "SELL"],
    fee_rate_bps: int, salt: int,
) -> dict[str, Any]:
    """The order struct's values, before the signature."""
    _, _, _, _, to_checksum_address = _eth()
    address = to_checksum_address(maker)
    return {
        "salt": int(salt),
        "maker": address,
        "signer": address,
        "taker": ZERO_ADDRESS,
        "tokenId": int(token_id),
        "makerAmount": int(maker_amount),
        "takerAmount": int(taker_amount),
        "expiration": 0,
        "nonce": 0,
        "feeRateBps": int(fee_rate_bps),
        "side": BUY if side == "BUY" else SELL,
        "signatureType": EOA,
    }


def order_digest(order: dict[str, Any], *, exchange: str, chain_id: int = CHAIN_ID) -> bytes:
    _, _, _, keccak, _ = _eth()
    return keccak(b"\x19\x01" + _domain_separator(DOMAIN_NAME, DOMAIN_VERSION, chain_id, exchange) + _struct_hash(order))


def order_typed_data(order: dict[str, Any], *, exchange: str, chain_id: int = CHAIN_ID) -> dict[str, Any]:
    """The same order as EIP-712 typed data, for wallets that sign that."""
    from eth_utils import to_checksum_address

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
        "domain": {"name": DOMAIN_NAME, "version": DOMAIN_VERSION, "chainId": chain_id,
                   "verifyingContract": to_checksum_address(exchange)},
        "message": dict(order),
    }


def sign_order(signer: WalletSigner, order: dict[str, Any], *, exchange: str, chain_id: int = CHAIN_ID) -> str:
    """The wallet's signature over the order, `0x`-hex."""
    return "0x" + bytes(signer.sign_digest(order_digest(order, exchange=exchange, chain_id=chain_id))).hex()


def wire_order(order: dict[str, Any], *, signature: str, price: Decimal) -> dict[str, Any]:
    """The signed order as `POST /orders` takes it: big integers as decimal
    strings, the price beside them."""
    return {
        "salt": str(order["salt"]),
        "maker": order["maker"],
        "signer": order["signer"],
        "taker": order["taker"],
        "tokenId": str(order["tokenId"]),
        "makerAmount": int(order["makerAmount"]),
        "takerAmount": int(order["takerAmount"]),
        "expiration": "0",
        "nonce": 0,
        "price": float(price),
        "feeRateBps": int(order["feeRateBps"]),
        "side": int(order["side"]),
        "signature": signature,
        "signatureType": int(order["signatureType"]),
    }


def request_headers(token_id: str, secret: str, method: str, path: str, body: str = "", *,
                    now: datetime | None = None) -> dict[str, str]:
    """The three `lmts-*` headers for one request. `path` includes the query string."""
    stamp = (now or datetime.now(timezone.utc)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    message = f"{stamp}\n{method.upper()}\n{path}\n{body}"
    signature = base64.b64encode(hmac.new(base64.b64decode(secret), message.encode(), hashlib.sha256).digest()).decode()
    return {"lmts-api-key": token_id, "lmts-timestamp": stamp, "lmts-signature": signature}
