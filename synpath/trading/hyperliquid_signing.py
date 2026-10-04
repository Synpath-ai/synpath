"""Hyperliquid action signing: what the venue's own SDK does, without its
dependencies.

An exchange action (an order, a cancel) is signed as an "L1 action": the
action is MessagePack-encoded, the nonce (8 bytes, big-endian) and a vault
flag are appended, and the keccak of that is the `connectionId` of an
EIP-712 `Agent` message (`source` "a" on mainnet, "b" on testnet) in the
`Exchange` domain, chain id 1337. The wallet signs that digest.

The encoder below covers exactly the values actions hold -- maps, arrays,
strings, integers, booleans, null -- and orders map keys as given, as
`msgpack.packb` does. It is checked byte for byte against the SDK's own test
vectors (`tests/test_hyperliquid_trading.py`), because a signature over one
byte more or less is refused as a signature from a different wallet.
"""
from __future__ import annotations

import struct
from decimal import Decimal
from typing import Any

from eth_utils import keccak

from .polymarket_signing import WalletSigner

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


# ---------------------------------------------------------------------------
# MessagePack
# ---------------------------------------------------------------------------

def packb(value: Any) -> bytes:
    """`msgpack.packb(value)` for the types an action holds."""
    out = bytearray()
    _pack(value, out)
    return bytes(out)


def _pack(value: Any, out: bytearray) -> None:
    if value is None:
        out.append(0xC0)
    elif value is True:
        out.append(0xC3)
    elif value is False:
        out.append(0xC2)
    elif isinstance(value, int):
        _pack_int(value, out)
    elif isinstance(value, str):
        data = value.encode("utf-8")
        size = len(data)
        if size < 32:
            out.append(0xA0 | size)
        elif size < 0x100:
            out += bytes((0xD9, size))
        elif size < 0x10000:
            out += b"\xda" + struct.pack(">H", size)
        else:
            out += b"\xdb" + struct.pack(">I", size)
        out += data
    elif isinstance(value, (bytes, bytearray)):
        size = len(value)
        if size < 0x100:
            out += bytes((0xC4, size))
        elif size < 0x10000:
            out += b"\xc5" + struct.pack(">H", size)
        else:
            out += b"\xc6" + struct.pack(">I", size)
        out += bytes(value)
    elif isinstance(value, (list, tuple)):
        size = len(value)
        if size < 16:
            out.append(0x90 | size)
        elif size < 0x10000:
            out += b"\xdc" + struct.pack(">H", size)
        else:
            out += b"\xdd" + struct.pack(">I", size)
        for item in value:
            _pack(item, out)
    elif isinstance(value, dict):
        size = len(value)
        if size < 16:
            out.append(0x80 | size)
        elif size < 0x10000:
            out += b"\xde" + struct.pack(">H", size)
        else:
            out += b"\xdf" + struct.pack(">I", size)
        for key, item in value.items():
            _pack(key, out)
            _pack(item, out)
    else:
        raise TypeError(f"hyperliquid: cannot encode {type(value).__name__} in an action")


def _pack_int(value: int, out: bytearray) -> None:
    if 0 <= value < 0x80:
        out.append(value)
    elif -32 <= value < 0:
        out.append(value & 0xFF)
    elif value >= 0:
        if value < 0x100:
            out += bytes((0xCC, value))
        elif value < 0x10000:
            out += b"\xcd" + struct.pack(">H", value)
        elif value < 0x100000000:
            out += b"\xce" + struct.pack(">I", value)
        elif value < 0x10000000000000000:
            out += b"\xcf" + struct.pack(">Q", value)
        else:
            raise OverflowError("hyperliquid: integer too large for an action")
    else:
        if value >= -0x80:
            out += b"\xd0" + struct.pack(">b", value)
        elif value >= -0x8000:
            out += b"\xd1" + struct.pack(">h", value)
        elif value >= -0x80000000:
            out += b"\xd2" + struct.pack(">i", value)
        else:
            out += b"\xd3" + struct.pack(">q", value)


# ---------------------------------------------------------------------------
# L1 actions
# ---------------------------------------------------------------------------

def action_hash(action: dict[str, Any], vault_address: str | None, nonce: int, expires_after: int | None = None) -> bytes:
    data = packb(action) + nonce.to_bytes(8, "big")
    if vault_address is None:
        data += b"\x00"
    else:
        data += b"\x01" + bytes.fromhex(vault_address.removeprefix("0x"))
    if expires_after is not None:
        data += b"\x00" + expires_after.to_bytes(8, "big")
    return keccak(data)


def _type_hash(signature: str) -> bytes:
    return keccak(text=signature)


DOMAIN_SEPARATOR = keccak(
    _type_hash("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
    + keccak(text="Exchange") + keccak(text="1") + (1337).to_bytes(32, "big") + bytes(32)
)
"""The `Exchange` domain every L1 action is signed in: chain id 1337, no contract."""

AGENT_TYPE_HASH = _type_hash("Agent(string source,bytes32 connectionId)")


def agent_digest(connection_id: bytes, *, mainnet: bool) -> bytes:
    """The EIP-712 digest of the phantom `Agent` message for an action hash."""
    struct_hash = keccak(AGENT_TYPE_HASH + keccak(text="a" if mainnet else "b") + connection_id)
    return keccak(b"\x19\x01" + DOMAIN_SEPARATOR + struct_hash)


def sign_l1_action(
    signer: WalletSigner, action: dict[str, Any], *, nonce: int, mainnet: bool,
    vault_address: str | None = None, expires_after: int | None = None,
) -> dict[str, Any]:
    """The `signature` field of an exchange request: `{r, s, v}`, `r` and `s`
    as minimal hex the way the SDK writes them."""
    digest = agent_digest(action_hash(action, vault_address, nonce, expires_after), mainnet=mainnet)
    raw = signer.sign_digest(digest)
    r, s, v = int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:64], "big"), raw[64]
    return {"r": hex(r), "s": hex(s), "v": v if v >= 27 else v + 27}


# ---------------------------------------------------------------------------
# Wire values
# ---------------------------------------------------------------------------

def to_wire(value: Decimal | int | str) -> str:
    """A price or size as the venue's string: at most 8 decimals, no
    trailing zeros, no exponent. A value with more precision is refused
    rather than rounded -- rounding a price changes the order."""
    number = Decimal(str(value))
    if number != number.quantize(Decimal("1e-8")):
        raise ValueError(f"hyperliquid: {value} has more than 8 decimals")
    text = f"{number.normalize():f}"
    return "0" if text in ("-0", "") else text


def significant_figures(value: Decimal) -> int:
    """How many significant figures a positive decimal has."""
    digits = value.normalize().as_tuple().digits
    return len(digits)
