"""Myriad order signing and request authentication, with no network in sight.

**Orders.** An order-book order is the venue's own EIP-712 `Order` struct
(trader, marketId, outcomeId, side, amount, price, minFillAmount, nonce,
expiration) under the domain `MyriadCTFExchange`, version `1`, chain 56, with
the order-book exchange as the verifying contract. The digest is the order's
hash, the id the venue's lookups and cancels take. The wallet signs its own
orders (signature type 0); a cancel repeats the order and its signature, and
cancelling everything signs a `CancelAll(trader, marketId, timestamp)`.

**Amounts.** `amount` is shares in 18-decimal units, `price` a 1e18 fraction
of a token per share on a whole-cent grid (a multiple of 1e16).

**Requests.** An API key and its secret sign every request: HMAC-SHA256, hex,
of `{unix seconds}.{METHOD}.{path and query}.{body}` under the secret, sent
as `x-api-key`, `x-api-timestamp` and `x-api-signature`. The venue refuses a
bare key. Without a key the wallet signs in (SIWE): the venue's login message,
signed as a personal message, buys a short-lived session token.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any, Iterator

import httpx

from .errors import InvalidOrder
from .polymarket_signing import WalletSigner, _eth, approve_calldata, set_approval_for_all_calldata
from .predict_fun_signing import _domain_separator, _personal_digest

DOMAIN_NAME = "MyriadCTFExchange"
DOMAIN_VERSION = "1"
CHAIN_ID = 56
EXCHANGE = "0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A"
CONDITIONAL_TOKENS = "0x6413734f92248D4B29ae35883290BD93212654Dc"
NEG_RISK_ADAPTER = "0xd96F26703Ddbf7d1Cb6858640eca34cF1893d53A"
USD1 = "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d"
USDT = "0x55d398326f99059fF775485246999027B3197955"

SCALE = 10**18
TICK_WEI = 10**16
"""Prices must be whole cents: a multiple of 1e16."""

BUY, SELL = 0, 1
YES, NO = 0, 1
EOA = 0

ORDER_TYPE = (
    "Order(address trader,uint256 marketId,uint8 outcomeId,uint8 side,uint256 amount,uint256 price,"
    "uint256 minFillAmount,uint256 nonce,uint256 expiration)"
)
CANCEL_ALL_TYPE = "CancelAll(address trader,uint256 marketId,uint256 timestamp)"

ORDER_FIELDS = [
    {"name": "trader", "type": "address"},
    {"name": "marketId", "type": "uint256"},
    {"name": "outcomeId", "type": "uint8"},
    {"name": "side", "type": "uint8"},
    {"name": "amount", "type": "uint256"},
    {"name": "price", "type": "uint256"},
    {"name": "minFillAmount", "type": "uint256"},
    {"name": "nonce", "type": "uint256"},
    {"name": "expiration", "type": "uint256"},
]
CANCEL_ALL_FIELDS = [
    {"name": "trader", "type": "address"},
    {"name": "marketId", "type": "uint256"},
    {"name": "timestamp", "type": "uint256"},
]
DOMAIN_FIELDS = [
    {"name": "name", "type": "string"}, {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"},
]


# ---------------------------------------------------------------------------
# Amounts
# ---------------------------------------------------------------------------

def to_wei(value: Decimal) -> int:
    return int((Decimal(str(value)) * SCALE).quantize(Decimal("1"), rounding=ROUND_DOWN))


def from_wei(value: Any) -> Decimal:
    return Decimal(int(str(value))) / SCALE


def price_wei(price: Decimal) -> int:
    """A token price as the venue's 1e18 integer, refused off the cent grid
    or outside (0, 1)."""
    price = Decimal(str(price))
    if not Decimal("0") < price < Decimal("1"):
        raise InvalidOrder(f"myriad: price {price} is outside (0, 1)")
    if price != price.quantize(Decimal("0.01")):
        raise InvalidOrder(f"myriad: price {price} is not a whole cent; the venue takes prices on a 0.01 grid")
    return int(price * 100) * TICK_WEI


def shares_wei(shares: Decimal) -> int:
    amount = to_wei(shares)
    if amount <= 0:
        raise InvalidOrder(f"myriad: {shares} shares is not a size")
    return amount


def new_nonce() -> int:
    """A random 256-bit nonce, as the venue's own client makes them."""
    return secrets.randbits(256)


# ---------------------------------------------------------------------------
# The order
# ---------------------------------------------------------------------------

def build_order(
    *, trader: str, market_id: int, outcome: int, side: int, amount: int, price: int,
    min_fill: int = 0, nonce: int, expiration: int = 0,
) -> dict[str, Any]:
    """The order struct's values, before the signature."""
    _, _, _, _, to_checksum_address = _eth()
    if outcome not in (YES, NO) or side not in (BUY, SELL):
        raise InvalidOrder(f"myriad: outcome {outcome} / side {side}: each is 0 or 1")
    return {
        "trader": to_checksum_address(trader),
        "marketId": int(market_id),
        "outcomeId": int(outcome),
        "side": int(side),
        "amount": int(amount),
        "price": int(price),
        "minFillAmount": int(min_fill),
        "nonce": int(nonce),
        "expiration": int(expiration),
    }


def order_of_wire(wire: dict[str, Any]) -> dict[str, Any]:
    """An order as the venue writes it (numbers as strings) as the struct."""
    return build_order(
        trader=str(wire["trader"]), market_id=int(str(wire["marketId"])), outcome=int(wire["outcomeId"]),
        side=int(wire["side"]), amount=int(str(wire["amount"])), price=int(str(wire["price"])),
        min_fill=int(str(wire.get("minFillAmount") or 0)), nonce=int(str(wire["nonce"])),
        expiration=int(str(wire.get("expiration") or 0)),
    )


def wire_order(order: dict[str, Any]) -> dict[str, Any]:
    """The order as `POST /orders` takes it: big integers as decimal strings."""
    return {
        "trader": order["trader"],
        "marketId": str(order["marketId"]),
        "outcomeId": int(order["outcomeId"]),
        "side": int(order["side"]),
        "amount": str(order["amount"]),
        "price": str(order["price"]),
        "minFillAmount": str(order["minFillAmount"]),
        "nonce": str(order["nonce"]),
        "expiration": str(order["expiration"]),
    }


def _order_struct_hash(order: dict[str, Any]) -> bytes:
    abi_encode, _, _, keccak, to_checksum_address = _eth()
    return keccak(abi_encode(
        ["bytes32", "address", "uint256", "uint8", "uint8", "uint256", "uint256", "uint256", "uint256", "uint256"],
        [
            keccak(text=ORDER_TYPE), to_checksum_address(order["trader"]), int(order["marketId"]),
            int(order["outcomeId"]), int(order["side"]), int(order["amount"]), int(order["price"]),
            int(order["minFillAmount"]), int(order["nonce"]), int(order["expiration"]),
        ],
    ))


def _domain(*, exchange: str, chain_id: int) -> bytes:
    return _domain_separator(DOMAIN_NAME, DOMAIN_VERSION, chain_id, exchange)


def order_digest(order: dict[str, Any], *, exchange: str = EXCHANGE, chain_id: int = CHAIN_ID) -> bytes:
    """The order's EIP-712 digest: its hash on the venue."""
    _, _, _, keccak, _ = _eth()
    return keccak(b"\x19\x01" + _domain(exchange=exchange, chain_id=chain_id) + _order_struct_hash(order))


def order_hash(order: dict[str, Any], *, exchange: str = EXCHANGE, chain_id: int = CHAIN_ID) -> str:
    return "0x" + order_digest(order, exchange=exchange, chain_id=chain_id).hex()


def _typed(primary: str, fields: list[dict[str, str]], message: dict[str, Any], *, exchange: str,
           chain_id: int) -> dict[str, Any]:
    from eth_utils import to_checksum_address

    return {
        "primaryType": primary,
        "types": {"EIP712Domain": DOMAIN_FIELDS, primary: fields},
        "domain": {"name": DOMAIN_NAME, "version": DOMAIN_VERSION, "chainId": chain_id,
                   "verifyingContract": to_checksum_address(exchange)},
        "message": dict(message),
    }


def order_typed_data(order: dict[str, Any], *, exchange: str = EXCHANGE, chain_id: int = CHAIN_ID) -> dict[str, Any]:
    """The same order as EIP-712 typed data, for wallets that sign that."""
    return _typed("Order", ORDER_FIELDS, order, exchange=exchange, chain_id=chain_id)


def sign_order(signer: WalletSigner, order: dict[str, Any], *, exchange: str = EXCHANGE,
               chain_id: int = CHAIN_ID) -> tuple[str, str]:
    """`(signature, hash)` for an order, the signature `0x`-hex."""
    digest = order_digest(order, exchange=exchange, chain_id=chain_id)
    return "0x" + bytes(signer.sign_digest(digest)).hex(), "0x" + digest.hex()


def cancel_all_digest(trader: str, market_id: int, timestamp: int, *, exchange: str = EXCHANGE,
                      chain_id: int = CHAIN_ID) -> bytes:
    abi_encode, _, _, keccak, to_checksum_address = _eth()
    struct = keccak(abi_encode(
        ["bytes32", "address", "uint256", "uint256"],
        [keccak(text=CANCEL_ALL_TYPE), to_checksum_address(trader), int(market_id), int(timestamp)],
    ))
    return keccak(b"\x19\x01" + _domain(exchange=exchange, chain_id=chain_id) + struct)


def cancel_all_typed_data(trader: str, market_id: int, timestamp: int, *, exchange: str = EXCHANGE,
                          chain_id: int = CHAIN_ID) -> dict[str, Any]:
    from eth_utils import to_checksum_address

    return _typed("CancelAll", CANCEL_ALL_FIELDS,
                  {"trader": to_checksum_address(trader), "marketId": int(market_id), "timestamp": int(timestamp)},
                  exchange=exchange, chain_id=chain_id)


def sign_cancel_all(signer: WalletSigner, market_id: int = 0, *, timestamp: int | None = None,
                    exchange: str = EXCHANGE, chain_id: int = CHAIN_ID) -> dict[str, Any]:
    """The `POST /orders/cancel-all` body: every open order, or one market's
    (`market_id`; 0 is every market)."""
    stamp = int(timestamp if timestamp is not None else time.time())
    digest = cancel_all_digest(signer.address, market_id, stamp, exchange=exchange, chain_id=chain_id)
    body: dict[str, Any] = {
        "trader": signer.address, "timestamp": str(stamp),
        "signature": "0x" + bytes(signer.sign_digest(digest)).hex(), "signatureType": EOA, "network_id": chain_id,
    }
    if market_id:
        body["market_id"] = int(market_id)
    return body


def sign_login(signer: WalletSigner, message: str) -> str:
    """The wallet's personal-message signature over the venue's SIWE message."""
    return "0x" + bytes(signer.sign_digest(_personal_digest(message.encode()))).hex()


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def request_signature(secret: str, timestamp: str, method: str, path: str, body: str = "") -> str:
    """HMAC-SHA256, hex, of `timestamp.METHOD.path.body`. `path` includes the query string."""
    message = f"{timestamp}.{method.upper()}.{path}.{body}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def request_headers(api_key: str, secret: str, method: str, path: str, body: str = "", *,
                    now: float | None = None) -> dict[str, str]:
    """The three `x-api-*` headers for one request."""
    stamp = str(int(now if now is not None else time.time()))
    return {"x-api-key": api_key, "x-api-timestamp": stamp,
            "x-api-signature": request_signature(secret, stamp, method, path, body)}


class HmacAuth(httpx.Auth):
    """Signs every request an httpx client sends with the API key: the path
    and query exactly as sent, and the exact body bytes."""

    def __init__(self, api_key: str, secret: str):
        self.api_key = api_key
        self._secret = secret

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        path = request.url.raw_path.decode("ascii")
        body = request.content.decode("utf-8") if request.content else ""
        request.headers.update(request_headers(self.api_key, self._secret, request.method, path, body))
        yield request


# ---------------------------------------------------------------------------
# Approvals
# ---------------------------------------------------------------------------

def approval_calls(collateral: str = USD1) -> list[dict[str, str]]:
    """The two one-time transactions that let the exchange settle this
    wallet's orders: the collateral approved to it (buys), and the outcome
    shares (sells). `{to, data}` each, for the wallet to send."""
    return [
        {"to": collateral, "data": approve_calldata(EXCHANGE)},
        {"to": CONDITIONAL_TOKENS, "data": set_approval_for_all_calldata(EXCHANGE)},
    ]
