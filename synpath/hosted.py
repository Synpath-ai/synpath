"""Synpath's own hosted services, as opposed to the venues.

Two of them, each with a default address and an environment variable that
overrides it:

  matching   https://api.synpath.dev    SYNPATH_MATCHING_URL   `match_market`, `match_event`
  history    https://api2.synpath.dev   SYNPATH_HISTORY_URL    `fetch_order_book_at`, ...

Both take a Synpath API key as `Authorization: Bearer <key>`, from `api_key=`,
`SYNPATH_API_KEY`, or the key `synpath keys create` saved. The key goes to Synpath's services only, never to a
venue, and nothing that runs on your own machine needs it.
"""
from __future__ import annotations

import os
from typing import Any

from .base import HttpClient, RateLimiter
from .errors import AuthenticationError, BadRequest, ExchangeError, ExchangeNotAvailable, MarketNotFound, NetworkError

API_KEY_ENV = "SYNPATH_API_KEY"
LABEL = "synpath"


def api_key_from(explicit: str | None = None, *, use_stored: bool = False) -> str | None:
    if explicit or os.environ.get(API_KEY_ENV):
        return explicit or os.environ.get(API_KEY_ENV)
    if use_stored:
        from .hosted_auth import stored_api_key
        return stored_api_key()
    return None


def auth_headers(api_key: str | None = None, *, use_stored: bool = False) -> dict[str, str]:
    key = api_key_from(api_key, use_stored=use_stored)
    return {"Authorization": f"Bearer {key}"} if key else {}


def hosted_client(*, base_url: str | None, env: str, default: str, http_client: Any = None) -> HttpClient:
    url = base_url or os.environ.get(env) or default
    return HttpClient(url, limiter=RateLimiter(rate_per_second=10), client=http_client, venue=LABEL)


def _is_ours(body: Any) -> bool:
    """Whether an error body came from a Synpath service rather than the host in front of it.
    Ours carry `detail` (FastAPI) or `error` with a `code` (the newer shape)."""
    if not isinstance(body, dict):
        return False
    if "detail" in body:
        return True
    error = body.get("error")
    return isinstance(error, dict) and "code" in error


def call(http: HttpClient, method: str, path: str, *, api_key: str | None, owned: bool,
         use_stored: bool = False, **kwargs: Any) -> Any:
    """One request to a hosted service, with the key and plainer errors: a
    refused key says which variable to set, and a range too large to answer
    whole says to narrow it."""
    try:
        return http.request(method, path, headers=auth_headers(api_key, use_stored=use_stored), **kwargs)
    except MarketNotFound as exc:
        if _is_ours(exc.body):
            raise
        # A 404 that is not our service's own answer is the host in front of it saying there is
        # no application there: the service is down, not the market missing.
        raise ExchangeNotAvailable(
            f"{LABEL}: the service at {http.base_url} is not running or not deployed (it answered 404 "
            f"without a Synpath error body); try again later or check {http.base_url}",
            body=exc.body, status=exc.status,
        ) from None
    except NetworkError as exc:
        reason = str(exc).removeprefix(f"{LABEL}: ")
        raise NetworkError(f"{LABEL}: could not reach {http.base_url}: {reason}") from None
    except AuthenticationError as exc:
        hint = "" if api_key_from(api_key, use_stored=use_stored) else (
            f"; run `synpath login` then `synpath keys create`, or set {API_KEY_ENV}")
        raise AuthenticationError(f"{exc}{hint}", body=exc.body, status=exc.status) from None
    except ExchangeError as exc:
        if exc.status == 413:
            raise BadRequest(f"{LABEL}: the result is too large to return whole; narrow the range or raise limit",
                             body=exc.body, status=exc.status) from None
        raise
    finally:
        if owned:
            http.close()
