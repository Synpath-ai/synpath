"""Stream full historical book states and gaps from the Synpath preview API.

Run from the synpath_public directory after `pip install -e .`:

    python examples/track_historical_book.py \
      --base-url http://127.0.0.1:8787 --market-id kalshi:YOUR-TICKER \
      --start 2026-09-23T14:00:00Z --end 2026-09-26T14:00:00Z \
      --output book-timeline.jsonl

Omit --output for a compact summary. JSONL output contains a complete book
after each recorded change, so it can be large for a busy market.
"""

from __future__ import annotations

import argparse
import os
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Literal

import httpx

from synpath import HistoricalOrderBook, OrderBookRangeResponse, OrderLevel
from synpath.types import iso

HOUR_MS = 60 * 60 * 1000


@dataclass(frozen=True)
class BookState:
    at_ms: int
    book: HistoricalOrderBook
    source: Literal["initial", "snapshot", "delta"]


@dataclass(frozen=True)
class BookGap:
    start_ms: int
    end_ms: int
    reason: str


TimelineItem = BookState | BookGap


class _ReplayBook:
    def __init__(self, book: HistoricalOrderBook):
        self.replace(book)

    def replace(self, book: HistoricalOrderBook) -> None:
        if book.depth_scope != "full":
            raise ValueError("Book replay requires full-depth snapshots")
        self.template = book
        self.bids = {Decimal(str(level.price)): Decimal(str(level.size)) for level in book.bids}
        self.asks = {Decimal(str(level.price)): Decimal(str(level.size)) for level in book.asks}

    def delta(self, side: str | None, price: str | None, quantity_delta: str | None) -> None:
        if side not in ("bid", "ask") or price is None or quantity_delta is None:
            raise ValueError("Malformed book delta")
        levels = self.bids if side == "bid" else self.asks
        key = Decimal(price)
        quantity = levels.get(key, Decimal(0)) + Decimal(quantity_delta)
        if quantity < 0:
            raise ValueError(f"Negative book quantity at {side} {price}")
        if quantity == 0:
            levels.pop(key, None)
        else:
            levels[key] = quantity

    def materialize(self, timestamp_ms: int, venue_timestamp_ms: int | None) -> HistoricalOrderBook:
        def sorted_levels(levels: dict[Decimal, Decimal], reverse: bool) -> list[OrderLevel]:
            return [OrderLevel(price=float(price), size=float(levels[price]))
                    for price in sorted(levels, reverse=reverse)]

        return HistoricalOrderBook(
            market_id=self.template.market_id, side=self.template.side,
            venue=self.template.venue, bids=sorted_levels(self.bids, True),
            asks=sorted_levels(self.asks, False), timestamp=timestamp_ms,
            datetime=iso(timestamp_ms), book_model=self.template.book_model,
            derived=self.template.derived, depth_scope="full", info=self.template.info,
            as_of_ms=timestamp_ms, venue_timestamp_ms=venue_timestamp_ms,
        )


def _fetch_ranges(
    client: httpx.Client, base_url: str, market_id: str, side: str,
    start_ms: int, end_ms: int, limit: int,
) -> Iterator[OrderBookRangeResponse]:
    response = client.post(
        f"{base_url.rstrip('/')}/v1/order-book/range",
        json={"market_id": market_id, "start_ms": start_ms, "end_ms": end_ms,
              "side": side, "limit": limit},
    )
    if response.status_code == 413:
        if end_ms - start_ms <= 1:
            raise RuntimeError(f"Book replay cannot fit even a 1 ms request at {start_ms}: {response.text}")
        middle = start_ms + (end_ms - start_ms) // 2
        yield from _fetch_ranges(client, base_url, market_id, side, start_ms, middle, limit)
        yield from _fetch_ranges(client, base_url, market_id, side, middle, end_ms, limit)
        return
    response.raise_for_status()
    parsed = OrderBookRangeResponse.model_validate(response.json())
    if (parsed.start_ms, parsed.end_ms, parsed.market_id) != (start_ms, end_ms, market_id):
        raise ValueError("API response range or market does not match the request")
    if parsed.next_cursor is not None:
        raise ValueError("This example requires an exhaustive, unpaginated response")
    yield parsed


def iter_book_timeline(
    client: httpx.Client, base_url: str, market_id: str, start_ms: int, end_ms: int,
    *, side: Literal["yes", "no"] = "yes", chunk_ms: int = HOUR_MS, limit: int = 10_000,
) -> Iterator[TimelineItem]:
    """Yield a complete book after each change, or an explicit absent interval.

    Processes at most one hour per request, recursively bisecting 413 responses.
    Memory is bounded to one API response plus the current full book. A version
    change aborts the walk rather than silently mixing two published datasets.
    """
    if start_ms < 0 or end_ms <= start_ms or not 0 < chunk_ms <= HOUR_MS:
        raise ValueError("Expected a nonempty range and chunk_ms in (0, one hour]")
    if not 0 < limit <= 10_000:
        raise ValueError("limit must be in [1, 10000]")

    version: str | None = None
    for chunk_start in range(start_ms, end_ms, chunk_ms):
        chunk_end = min(chunk_start + chunk_ms, end_ms)
        for result in _fetch_ranges(client, base_url, market_id, side, chunk_start, chunk_end, limit):
            if version is None:
                version = result.metadata.dataset_version
            elif result.metadata.dataset_version != version:
                raise RuntimeError("Dataset changed during historical walk; restart with one pinned version")
            position = result.start_ms
            for segment in result.segments:
                if segment.start_ms != position or segment.end_ms <= position:
                    raise ValueError("API response has missing, overlapping, or empty segments")
                position = segment.end_ms
                if segment.kind == "absent":
                    yield BookGap(segment.start_ms, segment.end_ms, segment.reason or "unknown")
                    continue
                if segment.initial_book is None:
                    raise ValueError("Data segment has no full initial book")
                replay = _ReplayBook(segment.initial_book)
                yield BookState(segment.start_ms, segment.initial_book, "initial")
                for change in segment.changes:
                    if not segment.start_ms <= change.observed_at_ms < segment.end_ms:
                        raise ValueError("Book change is outside its data segment")
                    if change.kind == "snapshot":
                        if change.book is None:
                            raise ValueError("Snapshot change has no full book")
                        replay.replace(change.book)
                    else:
                        replay.delta(change.book_side, change.price_exact, change.quantity_delta_exact)
                    yield BookState(change.observed_at_ms,
                                    replay.materialize(change.observed_at_ms, change.venue_timestamp_ms),
                                    change.kind)
            if position != result.end_ms:
                raise ValueError("API response does not cover its requested end")


def _timestamp(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--start and --end require a timezone")
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("SYNPATH_HISTORY_URL", "https://api2.synpath.dev"),
                        help="the history service (default: Synpath's, or $SYNPATH_HISTORY_URL)")
    parser.add_argument("--api-key", default=os.environ.get("SYNPATH_API_KEY"),
                        help="a Synpath API key (default: $SYNPATH_API_KEY)")
    parser.add_argument("--market-id", required=True)
    parser.add_argument("--start", required=True, help="ISO 8601 time with timezone")
    parser.add_argument("--end", required=True, help="ISO 8601 time with timezone")
    parser.add_argument("--side", choices=("yes", "no"), default="yes")
    parser.add_argument("--chunk-minutes", type=int, default=60)
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--output", type=Path, help="Write every reconstructed state and gap as JSONL; fail if file exists")
    args = parser.parse_args()

    states = 0
    gaps: list[BookGap] = []
    headers = {"Authorization": f"Bearer {args.api_key}"} if args.api_key else {}
    with httpx.Client(timeout=60, headers=headers) as client:
        with args.output.open("x", encoding="utf-8") if args.output else _NullWriter() as destination:
            for item in iter_book_timeline(
                client, args.base_url, args.market_id, _timestamp(args.start), _timestamp(args.end),
                side=args.side, chunk_ms=args.chunk_minutes * 60_000, limit=args.limit,
            ):
                if isinstance(item, BookState):
                    states += 1
                    if args.output:
                        destination.write(json.dumps({"kind": "book", "at_ms": item.at_ms,
                            "source": item.source, "book": item.book.model_dump()}) + "\n")
                else:
                    if gaps and gaps[-1].end_ms == item.start_ms and gaps[-1].reason == item.reason:
                        gaps[-1] = BookGap(gaps[-1].start_ms, item.end_ms, item.reason)
                    else:
                        gaps.append(item)
                    if args.output:
                        destination.write(json.dumps({"kind": "gap", "start_ms": item.start_ms,
                            "end_ms": item.end_ms, "reason": item.reason}) + "\n")
    print(json.dumps({"market_id": args.market_id, "states": states,
        "gaps": [gap.__dict__ for gap in gaps], "output": str(args.output) if args.output else None}))


class _NullWriter:
    def __enter__(self) -> _NullWriter:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


if __name__ == "__main__":
    main()
