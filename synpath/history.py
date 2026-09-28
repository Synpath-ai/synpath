"""Historical order books and trades, from Synpath's hosted history service.

```python
import synpath

at = synpath.fetch_order_book_at("kalshi:KXQUANTUM-30", as_of_ms=1789509599000)
if at.book is None:
    print("no book:", at.absence_reason)       # coverage is not continuous
else:
    print(at.book.best_bid, at.book.best_ask)
```

Needs a Synpath API key (`SYNPATH_API_KEY`, or `api_key=`). The service lives at
`https://api2.synpath.dev` unless `base_url=` or `SYNPATH_HISTORY_URL` says otherwise.
Times are recorder receive times in Unix milliseconds. Coverage varies by market and
has gaps, which every answer says explicitly: a missing book comes back with an
`absence_reason`, never as an empty book. A range too large to return whole raises
`BadRequest`; narrow it or raise `limit` (`examples/track_historical_book.py`
splits long walks for you).
"""
from __future__ import annotations

from typing import Any

from .hosted import call, hosted_client
from .matching import _validate
from .types import BookSide, OrderBookAtResponse, OrderBookRangeResponse, TradesRangeResponse

BASE_URL_ENV = "SYNPATH_HISTORY_URL"
DEFAULT_URL = "https://api2.synpath.dev"


def _post(path: str, body: dict[str, Any], *, base_url: str | None, client: Any, api_key: str | None) -> Any:
    _validate(body["market_id"])
    http = hosted_client(base_url=base_url, env=BASE_URL_ENV, default=DEFAULT_URL, http_client=client)
    payload = {k: v for k, v in body.items() if v is not None}
    return call(http, "POST", path, json=payload, api_key=api_key, owned=client is None, use_stored=True)


def fetch_order_book_at(
    market_id: str, as_of_ms: int, *, side: BookSide | None = None, depth: int | None = None,
    api_key: str | None = None, base_url: str | None = None, client: Any = None,
) -> OrderBookAtResponse:
    """The book as it stood at `as_of_ms`. `book` is `None`, with `absence_reason`, where
    there is no coverage at that time."""
    body = _post("/v1/order-book/at", {"market_id": market_id, "as_of_ms": as_of_ms, "side": side, "depth": depth},
                 base_url=base_url, client=client, api_key=api_key)
    return OrderBookAtResponse.model_validate(body)


def fetch_order_book_range(
    market_id: str, start_ms: int, end_ms: int, *, side: BookSide | None = None, limit: int | None = None,
    api_key: str | None = None, base_url: str | None = None, client: Any = None,
) -> OrderBookRangeResponse:
    """Every change to the book between `start_ms` and `end_ms`: each segment starts from a
    full book, and an interval without coverage is its own segment rather than a gap."""
    body = _post("/v1/order-book/range", {"market_id": market_id, "start_ms": start_ms, "end_ms": end_ms,
                                          "side": side, "limit": limit},
                 base_url=base_url, client=client, api_key=api_key)
    return OrderBookRangeResponse.model_validate(body)


def fetch_trades_range(
    market_id: str, start_ms: int, end_ms: int, *, limit: int | None = None,
    api_key: str | None = None, base_url: str | None = None, client: Any = None,
) -> TradesRangeResponse:
    """Trades between `start_ms` and `end_ms`, with the coverage they were recorded under."""
    body = _post("/v1/trades/range", {"market_id": market_id, "start_ms": start_ms, "end_ms": end_ms,
                                      "limit": limit},
                 base_url=base_url, client=client, api_key=api_key)
    return TradesRangeResponse.model_validate(body)
