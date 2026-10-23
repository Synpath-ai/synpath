"""Novig request signing (NOVIG-V3), the price grid and contract units, with
no network in sight.

**Every signed request carries three headers**: `Novig-Key-Id` (the key's
UUID), `Novig-Timestamp` (Unix milliseconds, within 30 s of the venue's
clock) and `Novig-Signature`, the standard padded base64 of a signature over
six lines joined by LF, with no trailing newline:

    NOVIG-V3
    {unix_millis}
    {METHOD}
    {path, exactly as sent: never decoded or normalized}
    {canonical query, or an empty line}
    {lowercase hex SHA-256 of the raw body bytes}

The canonical query splits the raw query on `&` and each pair at its first
`=`, decodes only `%XX` (a bare `+` stays a `+`), re-encodes everything but
`A-Z a-z 0-9 - . _ ~` as uppercase `%XX`, and sorts by name, then value.

**Two algorithms.** The venue reads the algorithm from the stored key, so
no header names it: an Ed25519 key signs the string itself (64 bytes); a
P-256 key signs it with ECDSA and SHA-256, DER-encoded (70 to 72 bytes).
`tests/samples/novig_signing_vectors.json` is the venue's published set of
thirty vectors and two test keypairs; the canonical string matches all of
them, and the Ed25519 signatures byte for byte.

**Prices** are probabilities on a grid of 279: steps of 0.001 from 0.001 to
0.050 and from 0.950 to 0.999, steps of 0.005 between. **Quantities** are
whole numbers of 1-cent contracts, so this library's contract (one dollar)
is 100 of the venue's.
"""
from __future__ import annotations

import base64
import hashlib
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any, Iterator, Literal

import httpx

from .errors import InvalidOrder

SCHEME = "NOVIG-V3"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
UNRESERVED = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")

QTY_PER_CONTRACT = 100
"""The venue's contracts pay 1 cent; this library's pay a dollar."""

Algorithm = Literal["Ed25519", "P-256"]


# ---------------------------------------------------------------------------
# The canonical string
# ---------------------------------------------------------------------------

def _decode(part: str) -> bytes:
    """Only `%XX` decodes; a bare `+` stays a `+`, a stray `%` stays a `%`."""
    raw = part.encode("utf-8")
    out = bytearray()
    i = 0
    while i < len(raw):
        if raw[i] == 0x25 and _is_hex(raw[i + 1:i + 3]):
            out.append(int(raw[i + 1:i + 3], 16))
            i += 3
            continue
        out.append(raw[i])
        i += 1
    return bytes(out)


def _is_hex(pair: bytes) -> bool:
    return len(pair) == 2 and all(chr(b) in "0123456789abcdefABCDEF" for b in pair)


def _encode(raw: bytes) -> str:
    return "".join(chr(b) if b in UNRESERVED else f"%{b:02X}" for b in raw)


def canonical_query(raw: str) -> str:
    """Line 5 from the raw query string (without the `?`)."""
    pairs = []
    for token in raw.split("&"):
        if not token:
            continue
        name, _, value = token.partition("=")
        pairs.append((_encode(_decode(name)), _encode(_decode(value))))
    pairs.sort(key=lambda pair: (pair[0].encode(), pair[1].encode()))
    return "&".join(f"{name}={value}" for name, value in pairs)


def string_to_sign(timestamp_ms: int, method: str, path: str, query: str = "", body: bytes = b"") -> str:
    """The six lines a request's signature covers."""
    digest = hashlib.sha256(body or b"").hexdigest()
    return "\n".join((SCHEME, str(int(timestamp_ms)), method.upper(), path, canonical_query(query), digest))


# ---------------------------------------------------------------------------
# Keys and the signer
# ---------------------------------------------------------------------------

def load_private_key(pem: bytes) -> Any:
    """An Ed25519 or P-256 private key from a PKCS#8 PEM."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519

    key = serialization.load_pem_private_key(pem, password=None)
    if isinstance(key, ed25519.Ed25519PrivateKey):
        return key
    if isinstance(key, ec.EllipticCurvePrivateKey) and isinstance(key.curve, ec.SECP256R1):
        return key
    raise ValueError(f"Novig keys are Ed25519 or P-256; this one is {type(key).__name__}")


def algorithm_of(key: Any) -> Algorithm:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    return "Ed25519" if isinstance(key, ed25519.Ed25519PrivateKey) else "P-256"


class NovigSigner:
    """One key: its id, and the private half that signs. The algorithm comes
    from the key itself."""

    def __init__(self, key_id: str, private_key_pem: bytes):
        self.key_id = key_id
        self._key = load_private_key(private_key_pem)
        self.algorithm: Algorithm = algorithm_of(self._key)

    def __repr__(self) -> str:
        return f"NovigSigner(key_id={self.key_id!r}, algorithm={self.algorithm!r})"

    def sign(self, message: str) -> str:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ec

        data = message.encode("utf-8")
        if self.algorithm == "Ed25519":
            signature = self._key.sign(data)
        else:
            signature = self._key.sign(data, ec.ECDSA(hashes.SHA256()))
        return base64.b64encode(signature).decode()

    def headers(
        self, method: str, path: str, query: str = "", body: bytes = b"", *, timestamp_ms: int | None = None,
    ) -> dict[str, str]:
        stamp = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
        return {
            "Novig-Key-Id": self.key_id,
            "Novig-Timestamp": str(stamp),
            "Novig-Signature": self.sign(string_to_sign(stamp, method, path, query, body)),
        }

    def public_key_pem(self) -> str:
        return public_key_pem(self._key)


class NovigAuth(httpx.Auth):
    """Signs every request as it is sent, for sync and async clients alike,
    over the exact path, query and body bytes on the wire. A retried request
    is signed again, with a fresh timestamp."""

    requires_request_body = True

    def __init__(self, signer: NovigSigner):
        self.signer = signer

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        path, _, query = request.url.raw_path.decode("ascii").partition("?")
        request.headers.update(self.signer.headers(request.method, path, query, request.content or b""))
        yield request


def public_key_pem(private_key: Any) -> str:
    from cryptography.hazmat.primitives import serialization

    return private_key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()


def generate_key(algorithm: Algorithm = "Ed25519") -> tuple[bytes, str]:
    """A fresh keypair for a new subaccount or key: `(private PKCS#8 PEM,
    public SPKI PEM)`. Only the public half is ever sent; keep the private
    half in a secret store."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519

    if algorithm == "Ed25519":
        key: Any = ed25519.Ed25519PrivateKey.generate()
    elif algorithm == "P-256":
        key = ec.generate_private_key(ec.SECP256R1())
    else:
        raise ValueError(f"algorithm must be Ed25519 or P-256, got {algorithm!r}")
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
    return private, public_key_pem(key)


def fingerprint(public_pem: str | bytes) -> str:
    """The SHA-256 of a public key's SPKI DER, in hex: what the venue reports
    as a key's `fingerprint`."""
    from cryptography.hazmat.primitives import serialization

    data = public_pem.encode() if isinstance(public_pem, str) else public_pem
    key = serialization.load_pem_public_key(data)
    der = key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()


# ---------------------------------------------------------------------------
# Prices and quantities
# ---------------------------------------------------------------------------

MILLI = Decimal("0.001")


def on_grid(price: Decimal) -> bool:
    """Whether a price is one the venue accepts."""
    milli = Decimal(str(price)) / MILLI
    if milli != milli.to_integral_value():
        return False
    value = int(milli)
    return 1 <= value <= 50 or 950 <= value <= 999 or (51 <= value <= 949 and value % 5 == 0)


def snap_down(price: Decimal) -> Decimal:
    """The highest grid price at or below `price`: what an order that buys
    pays at most."""
    milli = int((Decimal(str(price)) / MILLI).to_integral_value(rounding=ROUND_DOWN))
    if 51 <= milli <= 949:
        milli -= milli % 5
    if not 1 <= milli <= 999:
        raise InvalidOrder(f"novig: price {price} is outside the venue's grid (0.001 to 0.999)")
    return (Decimal(milli) * MILLI).quantize(MILLI)


def wire_price(price: Decimal) -> str:
    """A grid price as the venue writes it: three places."""
    return str(Decimal(str(price)).quantize(MILLI))


def qty_of(contracts: Decimal) -> int:
    """This library's contracts as the venue's whole 1-cent contracts. A size
    that is not a whole number of cents is refused, not rounded."""
    qty = Decimal(str(contracts)) * QTY_PER_CONTRACT
    if qty != qty.to_integral_value() or qty < 1:
        raise InvalidOrder(
            f"novig: {contracts} contracts is not a whole number of the venue's 1-cent contracts "
            f"(sizes step by 0.01, at least 0.01)"
        )
    return int(qty)


def contracts_of(qty: Any) -> Decimal:
    """The venue's 1-cent contracts as this library's dollar contracts."""
    return Decimal(int(qty or 0)) / QTY_PER_CONTRACT
