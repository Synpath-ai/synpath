"""Opinion order signing, with no network in sight.

Opinion's exchange is a CTF exchange on BNB Chain, and its order is the
original (V1) CTF `Order` struct: salt, maker, signer, taker, tokenId,
makerAmount, takerAmount, expiration, nonce, feeRateBps, side and
signatureType, under the domain `OPINION CTF Exchange`, version `1`,
chain 56, with the market's exchange as the verifying contract.

Orders are signed by the wallet's key for its Gnosis Safe (signature type
2): the Safe is the maker and holds the funds, the key is the signer.

Amounts are 18-decimal integers. The venue requires the two amounts of a
limit order to state its price exactly, so they are built as its client
builds them: the maker amount is cut to four significant digits, then both
are set to a whole multiple of the price's fraction. `tests/samples/
opinion_signing_vectors.json` pins the result, signature included, against
the venue's own SDK code.
"""
from __future__ import annotations

import secrets
from decimal import Decimal
from fractions import Fraction
from typing import Any, Literal

from .errors import InvalidOrder
from .polymarket_signing import WalletSigner

CHAIN_ID = 56
DOMAIN_NAME = "OPINION CTF Exchange"
DOMAIN_VERSION = "1"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
DECIMALS = 18
SCALE = 10**DECIMALS

EOA, POLY_PROXY, POLY_GNOSIS_SAFE = 0, 1, 2
BUY, SELL = 0, 1
MARKET_ORDER, LIMIT_ORDER = 1, 2

MIN_PRICE = Decimal("0.001")
MAX_PRICE = Decimal("0.999")
MAX_PRICE_PLACES = 6
"""The venue's bounds on a limit price, as its client enforces them."""

ORDER_FIELDS = [
    {"name": "salt", "type": "uint256"},
    {"name": "maker", "type": "address"},
    {"name": "signer", "type": "address"},
    {"name": "taker", "type": "address"},
    {"name": "tokenId", "type": "uint256"},
    {"name": "makerAmount", "type": "uint256"},
    {"name": "takerAmount", "type": "uint256"},
    {"name": "expiration", "type": "uint256"},
    {"name": "nonce", "type": "uint256"},
    {"name": "feeRateBps", "type": "uint256"},
    {"name": "side", "type": "uint8"},
    {"name": "signatureType", "type": "uint8"},
]
DOMAIN_FIELDS = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]


def to_wei(amount: Decimal) -> int:
    """A decimal amount of USDT or tokens as its 18-decimal integer."""
    if amount <= 0:
        raise InvalidOrder(f"opinion: amount must be positive, got {amount}")
    return int(Decimal(str(amount)) * SCALE)


def from_wei(value: Any) -> Decimal:
    return Decimal(int(value)) / SCALE


def significant(value: int, digits: int) -> int:
    """`value` rounded to `digits` significant digits, as the venue's client
    rounds it (the division is a float there, so it is here)."""
    if value == 0:
        return 0
    magnitude = len(str(abs(value)))
    if magnitude <= digits:
        return value
    divisor = 10 ** (magnitude - digits)
    return int(round(value / divisor) * divisor)


def check_price(price: Decimal) -> Decimal:
    if not MIN_PRICE <= price <= MAX_PRICE:
        raise InvalidOrder(f"opinion: price {price} is outside [{MIN_PRICE}, {MAX_PRICE}]")
    if -price.as_tuple().exponent > MAX_PRICE_PLACES:  # type: ignore[operator]
        raise InvalidOrder(f"opinion: price {price} has more than {MAX_PRICE_PLACES} decimal places")
    return price


def limit_amounts(side: Literal["BUY", "SELL"], price: Decimal, maker_wei: int) -> tuple[int, int]:
    """`(makerAmount, takerAmount)` for a limit order whose maker gives
    `maker_wei`: USDT on a buy, tokens on a sell.

    The maker amount is cut to four significant digits, then both are set to
    `k` times the price's numerator and denominator, so `maker / taker` (a
    buy) or `taker / maker` (a sell) is the price exactly.
    """
    check_price(price)
    fraction = Fraction(str(price)).limit_denominator(1_000_000)
    target = significant(maker_wei, 4)
    if side == "BUY":
        k = max(1, target // fraction.numerator)
        maker, taker = k * fraction.numerator, k * fraction.denominator
    else:
        k = max(1, target // fraction.denominator)
        maker, taker = k * fraction.denominator, k * fraction.numerator
    return max(1, maker), max(1, taker)


def new_salt() -> int:
    return secrets.randbelow(2**53 - 1)


def build_order(
    *, maker: str, signer: str, token_id: str, maker_amount: int, taker_amount: int,
    side: Literal["BUY", "SELL"], salt: int, signature_type: int = POLY_GNOSIS_SAFE,
) -> dict[str, Any]:
    """The order struct's values, before the signature. Expiration, nonce and
    fee rate are 0: the venue's client signs them so, and charges fees at
    match time rather than through the order."""
    return {
        "salt": int(salt),
        "maker": maker,
        "signer": signer,
        "taker": ZERO_ADDRESS,
        "tokenId": int(token_id),
        "makerAmount": int(maker_amount),
        "takerAmount": int(taker_amount),
        "expiration": 0,
        "nonce": 0,
        "feeRateBps": 0,
        "side": BUY if side == "BUY" else SELL,
        "signatureType": int(signature_type),
    }


def order_typed_data(order: dict[str, Any], *, exchange: str) -> dict[str, Any]:
    from eth_utils import to_checksum_address

    message = dict(order)
    for key in ("maker", "signer", "taker"):
        message[key] = to_checksum_address(message[key])
    return {
        "primaryType": "Order",
        "types": {"EIP712Domain": DOMAIN_FIELDS, "Order": ORDER_FIELDS},
        "domain": {
            "name": DOMAIN_NAME, "version": DOMAIN_VERSION, "chainId": CHAIN_ID,
            "verifyingContract": to_checksum_address(exchange),
        },
        "message": message,
    }


def sign_order(signer: WalletSigner, order: dict[str, Any], *, exchange: str) -> str:
    native = getattr(signer, "_native", None)
    if native is not None:
        try:
            return native.opinion_sign_order(
                str(int(order["salt"])), order["maker"], order["signer"], order["taker"],
                str(int(order["tokenId"])), str(int(order["makerAmount"])), str(int(order["takerAmount"])),
                str(int(order["expiration"])), str(int(order["nonce"])), str(int(order["feeRateBps"])),
                int(order["side"]), int(order["signatureType"]), exchange,
            )
        except (KeyError, TypeError, ValueError, OverflowError):
            pass  # the Python encoder reads it, and raises what it always raised
    return signer.sign_typed_data(order_typed_data(order, exchange=exchange))
