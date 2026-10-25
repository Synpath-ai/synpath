"""Novig's parlay RFQ webhook signature (`X-Novig-Signature`), with no
network in sight.

Every `/quote`, `/confirm` and `/ping` request Novig sends a pricer's
webhook carries

    X-Novig-Signature = hex(BLAKE3_keyed_hash(shared_secret_bytes, raw_body_bytes))

The key is the pricer's shared secret (64 lowercase hex characters from
`POST /rfq/pricer`, decoded to 32 bytes), the message is the request body
exactly as received, before any JSON parsing, and the output is 32 bytes as
64 lowercase hex characters. A request whose signature does not match is
answered `401` and never processed.

BLAKE3 is not in the standard library. The `blake3` package is used when it
is installed; otherwise the keyed hash below, a direct transcription of the
BLAKE3 reference implementation, does the work. Webhook bodies are a few
hundred bytes, one chunk, so the pure version costs well under a millisecond.
It is checked against the official BLAKE3 test vectors
(`tests/samples/blake3_vectors.json`), and against the `blake3` package
where that is installed.
"""
from __future__ import annotations

import hmac
import struct

SIGNATURE_HEADER = "X-Novig-Signature"

KEY_LEN = 32
OUT_LEN = 32
BLOCK_LEN = 64
CHUNK_LEN = 1024

CHUNK_START = 1 << 0
CHUNK_END = 1 << 1
PARENT = 1 << 2
ROOT = 1 << 3
KEYED_HASH = 1 << 4

MASK = 0xFFFFFFFF
IV = (0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19)
PERMUTATION = (2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8)


# ---------------------------------------------------------------------------
# BLAKE3, keyed mode, 32-byte output
# ---------------------------------------------------------------------------

def _g(s: list[int], a: int, b: int, c: int, d: int, x: int, y: int) -> None:
    s[a] = (s[a] + s[b] + x) & MASK
    v = s[d] ^ s[a]
    s[d] = ((v >> 16) | (v << 16)) & MASK
    s[c] = (s[c] + s[d]) & MASK
    v = s[b] ^ s[c]
    s[b] = ((v >> 12) | (v << 20)) & MASK
    s[a] = (s[a] + s[b] + y) & MASK
    v = s[d] ^ s[a]
    s[d] = ((v >> 8) | (v << 24)) & MASK
    s[c] = (s[c] + s[d]) & MASK
    v = s[b] ^ s[c]
    s[b] = ((v >> 7) | (v << 25)) & MASK


def _compress(cv: tuple[int, ...] | list[int], block: list[int], counter: int, block_len: int,
              flags: int) -> list[int]:
    s = [*cv[:8], *IV[:4], counter & MASK, (counter >> 32) & MASK, block_len, flags]
    m = list(block)
    for r in range(7):
        _g(s, 0, 4, 8, 12, m[0], m[1])
        _g(s, 1, 5, 9, 13, m[2], m[3])
        _g(s, 2, 6, 10, 14, m[4], m[5])
        _g(s, 3, 7, 11, 15, m[6], m[7])
        _g(s, 0, 5, 10, 15, m[8], m[9])
        _g(s, 1, 6, 11, 12, m[10], m[11])
        _g(s, 2, 7, 8, 13, m[12], m[13])
        _g(s, 3, 4, 9, 14, m[14], m[15])
        if r < 6:
            m = [m[i] for i in PERMUTATION]
    for i in range(8):
        s[i] ^= s[i + 8]
        s[i + 8] ^= cv[i]
    return s


def _words(block: bytes) -> list[int]:
    return list(struct.unpack("<16I", block.ljust(BLOCK_LEN, b"\0")))


class _Output:
    """What a node would compress: its chaining value is the first eight
    words; the root is the same compression with `ROOT` set."""

    __slots__ = ("cv", "block", "counter", "block_len", "flags")

    def __init__(self, cv: tuple[int, ...] | list[int], block: list[int], counter: int, block_len: int, flags: int):
        self.cv, self.block, self.counter, self.block_len, self.flags = cv, block, counter, block_len, flags

    def chaining_value(self) -> list[int]:
        return _compress(self.cv, self.block, self.counter, self.block_len, self.flags)[:8]

    def root(self) -> bytes:
        words = _compress(self.cv, self.block, 0, self.block_len, self.flags | ROOT)[:8]
        return struct.pack("<8I", *words)


def _chunk(key: tuple[int, ...], chunk: bytes, counter: int, flags: int) -> _Output:
    cv: tuple[int, ...] | list[int] = key
    blocks = [chunk[i:i + BLOCK_LEN] for i in range(0, len(chunk), BLOCK_LEN)] or [b""]
    for index, block in enumerate(blocks[:-1]):
        start = CHUNK_START if index == 0 else 0
        cv = _compress(cv, _words(block), counter, BLOCK_LEN, flags | start)[:8]
    last = blocks[-1]
    start = CHUNK_START if len(blocks) == 1 else 0
    return _Output(cv, _words(last), counter, len(last), flags | start | CHUNK_END)


def _parent(left: list[int], right: list[int], key: tuple[int, ...], flags: int) -> _Output:
    return _Output(key, [*left, *right], 0, BLOCK_LEN, flags | PARENT)


def blake3_keyed_hash_pure(key: bytes, data: bytes) -> bytes:
    """BLAKE3 in keyed mode, 32 bytes out, in pure Python."""
    if len(key) != KEY_LEN:
        raise ValueError(f"a BLAKE3 key is {KEY_LEN} bytes, got {len(key)}")
    key_words = struct.unpack("<8I", key)
    flags = KEYED_HASH
    chunks = [data[i:i + CHUNK_LEN] for i in range(0, len(data), CHUNK_LEN)] or [b""]
    stack: list[list[int]] = []
    output = _chunk(key_words, chunks[0], 0, flags)
    for counter in range(1, len(chunks)):
        cv = output.chaining_value()
        total = counter
        while total & 1 == 0:
            cv = _parent(stack.pop(), cv, key_words, flags).chaining_value()
            total >>= 1
        stack.append(cv)
        output = _chunk(key_words, chunks[counter], counter, flags)
    while stack:
        output = _parent(stack.pop(), output.chaining_value(), key_words, flags)
    return output.root()


def blake3_keyed_hash(key: bytes, data: bytes) -> bytes:
    """BLAKE3 in keyed mode, 32 bytes out: the `blake3` package when it is
    installed, the pure version otherwise."""
    try:
        from blake3 import blake3  # type: ignore[import-not-found]
    except ImportError:
        return blake3_keyed_hash_pure(key, data)
    if len(key) != KEY_LEN:
        raise ValueError(f"a BLAKE3 key is {KEY_LEN} bytes, got {len(key)}")
    return blake3(data, key=key).digest()


# ---------------------------------------------------------------------------
# The webhook signature
# ---------------------------------------------------------------------------

def secret_bytes(shared_secret: str | bytes) -> bytes:
    """The shared secret as the 32-byte key: 64 hex characters as Novig
    returns it, or the raw 32 bytes."""
    if isinstance(shared_secret, bytes) and len(shared_secret) == KEY_LEN:
        return shared_secret
    text = shared_secret.decode() if isinstance(shared_secret, bytes) else shared_secret
    try:
        key = bytes.fromhex(text.strip())
    except ValueError:
        raise ValueError("the Novig RFQ shared secret is 64 hex characters (32 bytes)") from None
    if len(key) != KEY_LEN:
        raise ValueError(f"the Novig RFQ shared secret is 64 hex characters (32 bytes), got {len(key)} bytes")
    return key


def sign_webhook(shared_secret: str | bytes, raw_body: bytes) -> str:
    """The `X-Novig-Signature` value for a body: what Novig sends, and what a
    test sends to a local pricer."""
    return blake3_keyed_hash(secret_bytes(shared_secret), raw_body).hex()


def verify_webhook(shared_secret: str | bytes, raw_body: bytes, signature: str | None) -> bool:
    """Whether `signature` is Novig's over these exact body bytes, compared
    in constant time. A missing or malformed header is `False`."""
    if not signature:
        return False
    expected = sign_webhook(shared_secret, raw_body)
    return hmac.compare_digest(expected.encode(), signature.strip().lower().encode())
