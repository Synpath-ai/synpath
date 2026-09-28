"""Cross-venue matching: "this market/event on venue A -- what is it on venue B?"

Every other call in this package asks one venue about itself. Matching takes both catalogs and a
judgement about whether two differently worded listings settle on the same fact, which no single
venue adapter can make -- so it is answered by a hosted matching service over HTTP, not computed
here. This module is the HTTP half; the actual matching (event identity, then market alignment)
lives in the service it calls.

The query is anchored, never a text search: give the market or event you already hold as a
Synpath id (`kalshi:KXHIGHTATL-26SEP23-B80`, `polymarket:2252244`), get back what the other
venue(s) call the same thing. Nothing here computes a similarity score or a confidence -- a
match is a deterministic parse both listings landed on, not a judgement call, and a caller that
wants to browse for candidates does that with each venue's own `fetch_markets(query=...)`.

Three answers, always:

  * the anchor is not a listing this service knows -- `MarketNotFound` / `BadSymbol`
  * it is, and the other venue has the same question   -- a `MarketLink` / a list of ids
  * it is, and the other venue does not                -- `None`

That third case is a real, common answer (most brackets on a weather card never line up across
venues; most native events are single-venue), and it is never confused with the first: a caller
that gets `None` knows the anchor is good and there is simply nothing to pair it with.
"""
from __future__ import annotations

from typing import Any

from . import ids
from .base import HttpClient
from .errors import BadRequest
from .hosted import call, hosted_client
from .types import EventMatch, MarketLink, MarketMatch

BASE_URL_ENV = "SYNPATH_MATCHING_URL"
DEFAULT_URL = "https://api.synpath.dev"
"""Where the hosted matching service lives: Synpath's own, unless `base_url=` or
`SYNPATH_MATCHING_URL` points somewhere else (a staging deployment, a local one)."""


def _client(base_url: str | None, http_client: Any = None) -> HttpClient:
    return hosted_client(base_url=base_url, env=BASE_URL_ENV, default=DEFAULT_URL, http_client=http_client)


def _validate(anchor: str) -> None:
    """Same shape check the service itself makes, run here first so a caller gets `BadRequest`
    without a round trip for something that could never have answered."""
    venue, sep, native = anchor.partition(":")
    if not sep or not native or venue not in ids.VENUES:
        raise BadRequest(
            f"{anchor!r} is not a Synpath id: expected 'venue:native_id' with venue one of {', '.join(ids.VENUES)}"
        )


def match_market(anchor: str, *, base_url: str | None = None, client: Any = None,
                 api_key: str | None = None) -> MarketMatch:
    """The other venue's listing of the same proposition as `anchor`.

    `anchor` is a Synpath id for a market you already hold, never a query -- browse each venue's
    own catalog with `fetch_markets(query=...)` first if you do not have one yet. `client` is an
    injectable `httpx.Client`, for tests and for a caller pooling connections itself.

    Raises `MarketNotFound` when `anchor` is not a listing this service knows. Otherwise returns
    a `MarketMatch` whose `matched` is a `MarketLink` when the other venue has the same
    proposition, or `None` when the anchor is a real listing with nothing to pair it with -- a
    different, and much more common, answer than "no such market".
    """
    _validate(anchor)
    # Only the client this call built for itself is closed after, never one the caller owns.
    body = call(_client(base_url, client), "GET", "/match/market", params={"id": anchor},
                api_key=api_key, owned=client is None, use_stored=True)
    return MarketMatch.model_validate(body)


def match_event(anchor: str, *, base_url: str | None = None, client: Any = None,
                api_key: str | None = None) -> EventMatch:
    """The other venues' native events that are the same event as `anchor`.

    One native event on one venue is often several on another (a game split by market type, a
    recurring series split by window), so each entry in `events` is a list, not a single id, and
    is `None` rather than `[]` when that venue lists nothing on the same event.
    """
    _validate(anchor)
    body = call(_client(base_url, client), "GET", "/match/event", params={"id": anchor},
                api_key=api_key, owned=client is None, use_stored=True)
    return EventMatch.model_validate(body)
