"""Polymarket Exchange v3 order signing, for combo RFQs. No network in sight.

Combo trades settle through **Exchange v3** (`0xe3333700...00Aa` on Polygon),
not the CLOB V2 exchanges, so the requester's accept and the quoter's quote
each carry an Exchange v3 order (docs.polymarket.com, /trading/combos/
requesters and /trading/combos/market-makers). The struct is the same
eleven fields as CLOB V2 -- `salt, maker, signer, tokenId, makerAmount,
takerAmount, side, signatureType, timestamp, metadata, builder` -- under a
different EIP-712 domain:

    {name: "Polymarket CTF Exchange", version: "3", chainId: 137,
     verifyingContract: 0xe3333700cA9d93003F00f0F71f8515005F6c00Aa}

There is one Exchange v3 and no neg-risk variant. The differences from the
CLOB V2 order in `polymarket_signing` are the domain version and contract,
a `timestamp` in **seconds** (V2 uses milliseconds), no `expiration` on the
wire, `side` sent as `0`/`1`, and `builder` that must be zero on the
requester gateway (`BUILDER_ATTRIBUTION_NOT_ALLOWED`).

How each wallet signs:

* **Proxy (1), Safe (2) and EOA (0)** sign the Exchange v3 `Order` typed data
  directly; the signature is the standard 65 bytes.
* **Deposit Wallet (3)** signs Solady's `TypedDataSign` envelope -- the order
  as `contents`, in the wallet's own `DepositWallet` v1 domain with the
  wallet as `verifyingContract` and a zero `salt` -- and the signature is
  wrapped for ERC-7739: inner signature, the Exchange v3 domain separator,
  the order's struct hash, the `Order(...)` type string, its two-byte length.

The requester gateway refuses EOAs (`UNSUPPORTED_REQUESTER_SIGNATURE_TYPE`);
quoters may use all four. The digest is signed by `WalletSigner.sign_digest`,
which is the Rust core's secp256k1 signer when it is installed and
`eth_account` otherwise; the bytes are the same. Nothing here touches the
CLOB V2 order signing.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from . import polymarket_signing as v2
from .errors import InvalidOrder

CHAIN_ID = v2.CHAIN_ID

EXCHANGE_V3 = "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"
POSITION_MANAGER = "0x006F54F7f9A22e0000CC2AB60031000000ae9fEF"
"""Owns the ERC-1155 combo positions (Polymarket Protocol V2)."""
ROUTER = "0x12121212006e4CD160D18e3f00711DA5c3372600"
AUTO_REDEEMER = "0xa1200000d0002264C9a1698e001292D00E1b00af"
PUSD = v2.PUSD
"""Addresses from the venue's contracts page and the market-makers guide, Polygon mainnet."""

DOMAIN_NAME = v2.EXCHANGE_DOMAIN_NAME
DOMAIN_VERSION = "3"
DEPOSIT_WALLET_DOMAIN_NAME = "DepositWallet"
DEPOSIT_WALLET_DOMAIN_VERSION = "1"

ORDER_TYPE = v2.ORDER_TYPE
ORDER_FIELDS = v2.ORDER_FIELDS
DOMAIN_FIELDS = v2.DOMAIN_FIELDS
TYPED_DATA_SIGN_FIELDS = [
    {"name": "contents", "type": "Order"},
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
    {"name": "salt", "type": "bytes32"},
]
TYPED_DATA_SIGN_TYPE = (
    "TypedDataSign(Order contents,string name,string version,uint256 chainId,"
    "address verifyingContract,bytes32 salt)" + ORDER_TYPE
)

ZERO_BYTES32 = v2.ZERO_BYTES32
EOA, POLY_PROXY, POLY_GNOSIS_SAFE, DEPOSIT_WALLET = v2.EOA, v2.POLY_PROXY, v2.POLY_GNOSIS_SAFE, v2.DEPOSIT_WALLET
SIGNATURE_TYPES = (EOA, POLY_PROXY, POLY_GNOSIS_SAFE, DEPOSIT_WALLET)
REQUESTER_SIGNATURE_TYPES = (POLY_PROXY, POLY_GNOSIS_SAFE, DEPOSIT_WALLET)
"""Both requester gateways refuse an EOA."""

E6 = 1_000_000
BUY, SELL = 0, 1

SideLike = Literal["BUY", "SELL", 0, 1]


def domain() -> dict[str, Any]:
    return {"name": DOMAIN_NAME, "version": DOMAIN_VERSION, "chainId": CHAIN_ID, "verifyingContract": EXCHANGE_V3}


def side_code(side: Any) -> int:
    if side in (BUY, "BUY", "buy"):
        return BUY
    if side in (SELL, "SELL", "sell"):
        return SELL
    raise InvalidOrder(f"polymarket: an Exchange v3 order side is BUY (0) or SELL (1), not {side!r}")


# ---------------------------------------------------------------------------
# Identity: which address signs and which wallet the order spends from
# ---------------------------------------------------------------------------

def order_identity(signature_type: int, *, signer: str, wallet: str) -> tuple[str, str]:
    """`(signer_address, maker_address)` for a wallet type, as both RFQ
    gateways define them: a Deposit Wallet is both (the account or session
    key signs for it through ERC-1271); a proxy or Safe is signed for by the
    account signer; an EOA is both itself."""
    if signature_type == DEPOSIT_WALLET:
        return wallet, wallet
    if signature_type in (POLY_PROXY, POLY_GNOSIS_SAFE):
        return signer, wallet
    if signature_type == EOA:
        return signer, signer
    raise InvalidOrder(f"polymarket: unknown signature type {signature_type}; expected one of {SIGNATURE_TYPES}")


def check_requester(signature_type: int) -> None:
    """The requester gateways' wallet rule, before anything is sent."""
    if signature_type not in REQUESTER_SIGNATURE_TYPES:
        raise InvalidOrder(
            "polymarket: combo RFQs refuse EOA wallets (UNSUPPORTED_REQUESTER_SIGNATURE_TYPE); request from a "
            "Deposit Wallet (POLYMARKET_SIGNATURE_TYPE=3, the default since May 2026), a proxy (1) or a Safe (2), "
            "with POLYMARKET_FUNDER set to that wallet"
        )


# ---------------------------------------------------------------------------
# The order
# ---------------------------------------------------------------------------

def build_order(
    *, maker: str, signer: str, token_id: str | int, maker_amount: int, taker_amount: int, side: SideLike,
    signature_type: int, timestamp_s: int, salt: int, metadata: str = ZERO_BYTES32, builder: str = ZERO_BYTES32,
) -> dict[str, Any]:
    """The order as both gateways carry it (`ExchangeV3Order`), before the
    signature: uint256 values as decimal strings, `side` and
    `signatureType` as integers, `timestamp` in Unix seconds."""
    if int(maker_amount) <= 0 or int(taker_amount) <= 0:
        raise InvalidOrder(f"polymarket: Exchange v3 amounts must be positive, got {maker_amount}/{taker_amount}")
    if signature_type not in SIGNATURE_TYPES:
        raise InvalidOrder(f"polymarket: unknown signature type {signature_type}; expected one of {SIGNATURE_TYPES}")
    return {
        "salt": str(int(salt)),
        "maker": maker,
        "signer": signer,
        "tokenId": str(int(token_id)),
        "makerAmount": str(int(maker_amount)),
        "takerAmount": str(int(taker_amount)),
        "side": side_code(side),
        "signatureType": int(signature_type),
        "timestamp": str(int(timestamp_s)),
        "metadata": metadata or ZERO_BYTES32,
        "builder": builder or ZERO_BYTES32,
    }


def _message(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "salt": int(order["salt"]),
        "maker": order["maker"],
        "signer": order["signer"],
        "tokenId": int(order["tokenId"]),
        "makerAmount": int(order["makerAmount"]),
        "takerAmount": int(order["takerAmount"]),
        "side": side_code(order["side"]),
        "signatureType": int(order["signatureType"]),
        "timestamp": int(order["timestamp"]),
        "metadata": v2._bytes32(order["metadata"]),
        "builder": v2._bytes32(order["builder"]),
    }


def order_typed_data(order: dict[str, Any]) -> dict[str, Any]:
    """`exchangeV3OrderTypedData`: what a proxy, Safe or EOA signs."""
    return {
        "primaryType": "Order",
        "types": {"EIP712Domain": DOMAIN_FIELDS, "Order": ORDER_FIELDS},
        "domain": domain(),
        "message": _message(order),
    }


def deposit_wallet_typed_data(order: dict[str, Any]) -> dict[str, Any]:
    """`depositWalletTypedData`: the order nested in the Deposit Wallet's
    `TypedDataSign` envelope, which a Deposit Wallet's signer signs."""
    return {
        "primaryType": "TypedDataSign",
        "types": {"EIP712Domain": DOMAIN_FIELDS, "Order": ORDER_FIELDS, "TypedDataSign": TYPED_DATA_SIGN_FIELDS},
        "domain": domain(),
        "message": {
            "contents": _message(order),
            "name": DEPOSIT_WALLET_DOMAIN_NAME,
            "version": DEPOSIT_WALLET_DOMAIN_VERSION,
            "chainId": CHAIN_ID,
            "verifyingContract": order["signer"],
            "salt": b"\x00" * 32,
        },
    }


def typed_data_for(order: dict[str, Any]) -> dict[str, Any]:
    """The payload this order's wallet type signs."""
    if int(order["signatureType"]) == DEPOSIT_WALLET:
        return deposit_wallet_typed_data(order)
    return order_typed_data(order)


def domain_separator() -> bytes:
    abi_encode, _, _, keccak, _ = v2._eth()
    return keccak(abi_encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [
            keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
            keccak(text=DOMAIN_NAME), keccak(text=DOMAIN_VERSION), CHAIN_ID, EXCHANGE_V3,
        ],
    ))


def struct_hash(order: dict[str, Any]) -> bytes:
    """`hashStruct(Order)`, the ERC-7739 `contentsHash`."""
    abi_encode, _, _, keccak, _ = v2._eth()
    m = _message(order)
    return keccak(abi_encode(
        ["bytes32", "uint256", "address", "address", "uint256", "uint256", "uint256",
         "uint8", "uint8", "uint256", "bytes32", "bytes32"],
        [keccak(text=ORDER_TYPE), m["salt"], m["maker"], m["signer"], m["tokenId"], m["makerAmount"],
         m["takerAmount"], m["side"], m["signatureType"], m["timestamp"], m["metadata"], m["builder"]],
    ))


def order_hash(order: dict[str, Any]) -> str:
    """The EIP-712 digest of the Exchange v3 order: the order's id on chain
    and, UNVERIFIED until P10, the `taker_order_hash` the gateway returns."""
    _, _, _, keccak, _ = v2._eth()
    return "0x" + keccak(b"\x19\x01" + domain_separator() + struct_hash(order)).hex().removeprefix("0x")


def signing_digest(order: dict[str, Any]) -> bytes:
    """The 32 bytes the signer signs for this order's wallet type."""
    _, _, _, keccak, _ = v2._eth()
    contents = struct_hash(order)
    if int(order["signatureType"]) != DEPOSIT_WALLET:
        return keccak(b"\x19\x01" + domain_separator() + contents)
    abi_encode = v2._eth()[0]
    envelope = keccak(abi_encode(
        ["bytes32", "bytes32", "bytes32", "bytes32", "uint256", "address", "bytes32"],
        [
            keccak(text=TYPED_DATA_SIGN_TYPE), contents, keccak(text=DEPOSIT_WALLET_DOMAIN_NAME),
            keccak(text=DEPOSIT_WALLET_DOMAIN_VERSION), CHAIN_ID, order["signer"], b"\x00" * 32,
        ],
    ))
    return keccak(b"\x19\x01" + domain_separator() + envelope)


def wrap_erc7739(inner: bytes, order: dict[str, Any]) -> str:
    """A Deposit Wallet's submitted signature: `inner || appDomainSeparator
    || contentsHash || contentsType || uint16(len(contentsType))`."""
    if len(inner) != 65:
        raise InvalidOrder(f"polymarket: an inner signature is 65 bytes, got {len(inner)}")
    return "0x" + (
        inner + domain_separator() + struct_hash(order) + ORDER_TYPE.encode() + len(ORDER_TYPE).to_bytes(2, "big")
    ).hex()


def sign_order(signer: v2.WalletSigner, order: dict[str, Any]) -> str:
    """The order's `signature` for its wallet type (see the module doc)."""
    inner = signer.sign_digest(signing_digest(order))
    if int(order["signatureType"]) == DEPOSIT_WALLET:
        return wrap_erc7739(inner, order)
    return "0x" + bytes(inner).hex()


def signed_order(signer: v2.WalletSigner, order: dict[str, Any]) -> dict[str, Any]:
    return {**order, "signature": sign_order(signer, order)}


# ---------------------------------------------------------------------------
# Amounts (six-decimal base units throughout)
# ---------------------------------------------------------------------------

def quote_size(unit: str, value_e6: int, price_e6: int) -> int:
    """A request's full size in share base units at a quote price: a
    `notional` (BUY) request buys `floor(value * 1e6 / price)` shares; a
    `shares` (SELL) request is its value."""
    if unit == "shares":
        return int(value_e6)
    if unit == "notional":
        if price_e6 <= 0:
            raise InvalidOrder("polymarket: a quote price must be positive")
        return (int(value_e6) * E6) // int(price_e6)
    raise InvalidOrder(f"polymarket: unknown requested_size unit {unit!r}")


def maker_quote_amounts(direction: str, price_e6: int, size_e6: int) -> tuple[str, int, int]:
    """`(token, makerAmount, takerAmount)` of a quoter's collateral-funded
    order (the market-makers guide): to fill a requester's SELL of YES the
    quoter buys YES (`ceil(price * size)` pUSD for `size` YES); to fill a BUY
    of YES it buys NO (`ceil((1 - price) * size)` pUSD for `size` NO). Both
    orders are BUYs; `token` names which position id is bought."""
    price_e6, size_e6 = int(price_e6), int(size_e6)
    if not 0 < price_e6 < E6:
        raise InvalidOrder(f"polymarket: a quote price is inside (0, 1); got {price_e6 / E6}")
    if size_e6 <= 0:
        raise InvalidOrder("polymarket: a quote size must be positive")
    if direction == "SELL":
        return "yes", _ceil_div(price_e6 * size_e6, E6), size_e6
    if direction == "BUY":
        return "no", _ceil_div((E6 - price_e6) * size_e6, E6), size_e6
    raise InvalidOrder(f"polymarket: an RFQ direction is BUY or SELL, not {direction!r}")


def _ceil_div(a: int, b: int) -> int:
    return -(-a // b)


def implied_price(side: int, maker_amount: int, taker_amount: int) -> Decimal:
    """The YES limit price a requester's order commits to: pUSD per share
    for a BUY (`maker / taker`), proceeds per share for a SELL (`taker / maker`)."""
    maker, taker = Decimal(int(maker_amount)), Decimal(int(taker_amount))
    return maker / taker if side == BUY else taker / maker


# ---------------------------------------------------------------------------
# Wallet calls
# ---------------------------------------------------------------------------

def combo_approval_calls(*, include_router: bool = False, include_auto_redeem: bool = False) -> list[dict[str, str]]:
    """The approvals a combo trade needs from the account wallet
    (https://docs.polymarket.com/trading/combos/market-makers.md):

    * always: pUSD `approve(Exchange v3)` (an order that spends collateral)
      and PositionManager `setApprovalForAll(Exchange v3)` (an order that
      transfers combo positions);
    * `include_router`: pUSD `approve(Router)` and PositionManager
      `setApprovalForAll(Router)`, to split pUSD into positions and to
      manage or redeem them through the Router;
    * `include_auto_redeem`: PositionManager `setApprovalForAll(AutoRedeemer)`,
      for automatic redemption.
    """
    calls = [
        {"target": PUSD, "value": "0", "data": v2.approve_calldata(EXCHANGE_V3)},
        {"target": POSITION_MANAGER, "value": "0", "data": v2.set_approval_for_all_calldata(EXCHANGE_V3)},
    ]
    if include_router:
        calls += [
            {"target": PUSD, "value": "0", "data": v2.approve_calldata(ROUTER)},
            {"target": POSITION_MANAGER, "value": "0", "data": v2.set_approval_for_all_calldata(ROUTER)},
        ]
    if include_auto_redeem:
        calls.append({"target": POSITION_MANAGER, "value": "0", "data": v2.set_approval_for_all_calldata(AUTO_REDEEMER)})
    return calls
