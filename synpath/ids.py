"""Synpath ids: `venue:native`, one per market and one per event.

A Synpath id names one listing on one venue and carries the venue in front of
the venue's own id, separated by a colon: `kalshi:KXFEDDECISION-26SEP-C25`,
`polymarket:2252244`, `polymarket_us:tec-mlb-nlchamp-2026-09-27-atl`. Nothing
is hashed or re-keyed, so the part after the colon is what the venue's own
website and API call the thing.

The venue in the id is what lets one field route a call: `synpath.Client`
reads the prefix and hands the call to that venue's adapter. Every venue
adapter also accepts the bare native id, because a caller holding a ticker
already knows which adapter it is talking to.

The colon is safe as a separator: no venue's native id contains one.
"""
from __future__ import annotations

VENUES: tuple[str, ...] = ("kalshi", "polymarket", "polymarket_us")
"""Every venue this library speaks, by id. `synpath.exchanges` is the same
list with the adapter classes attached; this one exists so the id helpers do
not import the adapters."""


def qualify(venue: str, native_id: str) -> str:
    """`("kalshi", "KXFOO-25")` -> `"kalshi:KXFOO-25"`. Idempotent: an id
    that already carries this venue's prefix is returned as is."""
    if native_id.startswith(f"{venue}:"):
        return native_id
    return f"{venue}:{native_id}"


def split(synpath_id: str) -> tuple[str | None, str]:
    """`"kalshi:KXFOO-25"` -> `("kalshi", "KXFOO-25")`; a bare native id ->
    `(None, native_id)`. Only a known venue counts as a prefix, so a native
    id that happens to contain a colon is never mis-split."""
    head, sep, tail = synpath_id.partition(":")
    if sep and head in VENUES and tail:
        return head, tail
    return None, synpath_id


def native(venue: str, synpath_id: str) -> str:
    """The venue's own id, from a Synpath id or a bare native id. An id that
    names a different venue is refused rather than sent to the wrong one."""
    found, native_id = split(synpath_id)
    if found is not None and found != venue:
        from .errors import BadRequest
        raise BadRequest(f"{venue}: {synpath_id!r} belongs to {found}, not to this venue")
    return native_id


def venue_of(synpath_id: str) -> str:
    """The venue named by a Synpath id. A bare native id has none and is
    refused: the caller wanted routing, and there is nothing to route on."""
    found, _ = split(synpath_id)
    if found is None:
        from .errors import BadRequest
        raise BadRequest(
            f"{synpath_id!r} is not a Synpath id: expected 'venue:native_id' with venue one of {', '.join(VENUES)}"
        )
    return found
