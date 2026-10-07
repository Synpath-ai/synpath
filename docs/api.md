# Unified API

## Historical API contract (preview)

Historical HTTP requests use Synpath IDs and snake-case JSON fields:

| Route | Request fields | Response model |
| --- | --- | --- |
| `POST /v1/order-book/at` | `market_id`, `as_of_ms`, optional `side`, `depth` | `OrderBookAtResponse` |
| `POST /v1/order-book/range` | `market_id`, `start_ms`, `end_ms`, optional `side`, `limit` | `OrderBookRangeResponse` |
| `POST /v1/trades/range` | `market_id`, `start_ms`, `end_ms`, optional `limit` | `TradesRangeResponse` |

Historical objects extend the existing market-data types: `HistoricalTrade(Trade)` and `HistoricalOrderBook(OrderBook)`. A trade appears directly in `TradesRangeResponse.trades` (for example, `response.trades[0].price`), with `observed_at_ms` and `timestamp_source` added. `Trade.timestamp` remains venue execution time when supplied. Historical books add `as_of_ms` (the time the state was requested/valued for) and optional `venue_timestamp_ms`; their inherited `timestamp` is the recorder time of the last incorporated book update. Thus an unchanged but valid book may have `timestamp < as_of_ms`. Queries and coverage use recorder receive time. A book range starts with a full-depth historical book and view-relative changes; an unavailable interval is explicit rather than an empty book. The server reads published processed hours from one or more non-overlapping runs and includes their `processed-v1-*` dataset version and watermark. A new run starts with unavailable coverage until fresh book snapshots and a trade subscription acknowledgment. The server caps ranges and returns `result_too_large` instead of a cursor when a response exceeds `limit`.

For a multi-hour or multi-day book walk, use [`examples/track_historical_book.py`](../examples/track_historical_book.py). It requests at most one hour at a time, splits HTTP 413 responses into smaller ranges, replays each full starting book and its exact delta strings into successive `HistoricalOrderBook` objects, and emits `BookGap` intervals instead of carrying stale state through an absence. It aborts if the dataset version changes mid-walk. For example:

```bash
# the history service needs a Synpath API key: synpath login, then synpath keys create
python examples/track_historical_book.py \
  --market-id kalshi:YOUR-TICKER \
  --start 2026-09-23T00:00:00Z --end 2026-09-26T00:00:00Z \
  --output book-timeline.jsonl
```

Without `--output`, it prints counts and merged gap intervals. With `--output`, it writes one JSON line for each complete reconstructed book state (including each change and chunk boundary) and each gap. This may be very large for a busy market. A market with no events in an otherwise published hour retains its preceding valid book; a wholly unpublished hour is reported as missing coverage. The current `OrderBook` model exposes floating-point levels, so replay starts from the API's public float snapshot; delta strings are applied with `Decimal`, but this example is not a lossless decimal-level archival format.

### Fetch history from Python

The history service is Synpath's, at `https://api2.synpath.dev`. Install the package from this repository and sign in with a Google account that has a verified email. While the Google OAuth app is in testing, the account must also be in Google's test-user audience:

```bash
pip install -e synpath_public
synpath login
synpath keys create my-laptop
synpath keys list
synpath keys revoke <key-id>
synpath logout
```

`synpath login` opens Google sign-in in your browser and returns to the CLI through a local loopback callback. The CLI saves its 12-hour management session and the latest issued API key in `~/.config/synpath/credentials.json` with restricted permissions. Key creation prints the secret once; `--no-print-key` saves it without printing. Revocation takes effect immediately. The API client uses the saved key automatically. `SYNPATH_API_KEY` or `api_key=` overrides it. `SYNPATH_HISTORY_URL` or `base_url=` points the calls at another deployment, such as a local one.

```python
import synpath

history = synpath.fetch_order_book_at("kalshi:KXQUANTUM-30", as_of_ms=1789509599000)
if history.book is None:
    print("No book:", history.absence_reason)       # e.g. outside_loaded_data
else:
    print("Dataset:", history.metadata.dataset_version)
    print("Best bid/ask:", history.book.best_bid, history.book.best_ask)

trades = synpath.fetch_trades_range("kalshi:KXQUANTUM-30", start_ms, end_ms, limit=1000)
changes = synpath.fetch_order_book_range("kalshi:KXQUANTUM-30", start_ms, end_ms, side="no")
```

Coverage is not continuous and varies by market, so check each answer's
`absence_reason` or `coverage`. A range too large to return whole raises
`BadRequest`: narrow it or raise `limit`.

Over HTTP, range calls use `/v1/order-book/range` or `/v1/trades/range` with `start_ms`, `end_ms`, and an optional `limit`, and a Synpath API key as `Authorization: Bearer <key>`. HTTP 413 means the response would not be exhaustive under the requested limit; narrow the interval or raise the limit instead of treating a partial response as complete.

Back to the [README](../README.md).

Method names are borrowed from [ccxt](https://github.com/ccxt/ccxt), so the
reflexes transfer from crypto exchanges, with the tiers prediction markets have
and spot exchanges do not.

| Method | Returns | Notes |
|---|---|---|
| `fetch_markets(query, limit, cursor, status, sort)` | `Page[Market]` | `limit` up to 100 |
| `fetch_markets_by_ids(ids)` | `list[Market]` | batched (Kalshi 200, Polymarket 100, Polymarket US 50 ids a request; Opinion has no batch lookup, one request each; Hyperliquid answers any number from one catalog read; predict.fun and Limitless, one request each), order preserved, closed and settled markets included |
| `fetch_events(query, limit, cursor, status)` | `Page[Event]` | markets nested |
| `fetch_market(market_id)` | `Market` | |
| `fetch_order_book(market_id, side, depth)` | `OrderBook` | `side="yes"` (default) or `"no"` |
| `fetch_order_books(market_ids, side, depth)` | `dict[str, OrderBook]` | many books; one request on Polymarket (500 books), one per market elsewhere |
| `fetch_trades(market_id, since, limit, cursor)` | `Page[Trade]` | newest page first, each page oldest first; `since` in ms; `next_cursor` null at the end |
| `fetch_ohlcv(market_id, timeframe, since, until, limit)` | `list[Candle]` | in the YES price; check `price_source`. With `since`, the first `limit` bars from it; without, the newest `limit` |
| `fetch_series(series_id)` | `Series` | Kalshi only |
| `fetch_fee_schedule(market_id)` | `FeeSchedule` | |
| `iter_events(status)` | iterator of `Event` | walks the whole catalog for you |

Polymarket and Polymarket US add `refresh_quotes(market)`, which replaces the
catalog's quotes with the live book. Polymarket also has
`fetch_order_books_by_token(token_ids, depth)`, by CLOB token id.

### Search

```python
markets = kalshi.fetch_markets(query="trump", limit=10)
events = kalshi.fetch_events(query="trump", limit=10)
```

Results come back most relevant first, with a cursor for the next page, and
`limit` and `status` apply. Kalshi takes two requests a page whatever the
catalog size; Polymarket and Polymarket US read up to five search pages to fill
`limit`.

Kalshi's documented Trade API has no search: its `/events` endpoint accepts a
`query` parameter and silently ignores it. So this calls the endpoint
kalshi.com's own search box uses, then batch-fetches the full events behind the
hits from the documented API, so results are the same shape a listing returns,
with the same category, series and `neg_risk`.

That endpoint is undocumented and can change without notice. If it answers in
a shape this library does not recognise, the call raises rather than returning
an empty result that would read as "no matches".

Because results are ranked by relevance, paging deep into a large result set
is best effort. To enumerate every market, walk `fetch_markets()` without a
query.

### Sort

`volume`, `liquidity` or `newest`, on every venue -- but not the same way on
every venue, and the difference matters.

Polymarket orders at the venue, so `sort="volume"` is the top of the whole
catalog by 24-hour volume, and paging continues down that order.

Kalshi and Polymarket US accept a sort parameter and ignore it: Kalshi's
`/events` takes `order=`, `sort=` and `order_by=` and returns the same page
regardless; Polymarket US's `orderBy=volume24hr` answers 200 with the default
order, and only `orderBy=id` changes anything. On those two the page is
ordered after it is read, the way ccxt and pmxt do it. That is "this page,
ordered by", not "the top of the catalog": a walk with `sort="volume"` gives
each page in volume order, not the whole catalog.

What each key reads is the venue's own figure, named in the adapter's
`fetch_markets` docstring. Two are worth knowing: Kalshi publishes no
liquidity number (`liquidity_dollars` is 0 on every open market), so its
`liquidity` is the size resting at the touch; Polymarket US publishes neither
volume nor liquidity in its catalog, so those two keys read each market's
best-bid/offer first, one request per market on the page.

### Matching across venues

```python
synpath.match_market("kalshi:KXHIGHTATL-26SEP23-B80")   # api.synpath.dev, with your saved Synpath API key
synpath.match_event("kalshi:KXHIGHTATL-26SEP23")
```

Every other call asks one venue about itself. These two ask whether a market
or event you already hold is the same question as one on another venue, which
takes both catalogs and a settlement-relevant parse, so they are answered by a
hosted matching service over HTTP rather than computed here: Synpath's, at
`https://api.synpath.dev`, which takes a Synpath API key from `SYNPATH_API_KEY`
(or `api_key=`). `SYNPATH_MATCHING_URL` or `base_url=` points them at another
deployment.

The query is **anchored, never a text search**: pass the Synpath id of a
market or event you already have (from `fetch_markets`, a URL, a ticker), get
back what the other venue calls the same thing. There is no similarity score
and no confidence — a match is a deterministic parse both listings landed on,
the same event identity and market alignment the service uses internally, not
a judgement call a caller has to weigh.

```python
result = synpath.match_market("kalshi:KXHIGHTATL-26SEP23-B80")
result.event_id   # the canonical event both sides belong to
result.matched     # MarketLink | None
```

Three outcomes, always distinguishable:

| | |
|---|---|
| the anchor is not a listing this service knows | `MarketNotFound` |
| it is, and the other venue asks the same question | `MarketMatch.matched` is a `MarketLink` |
| it is, and the other venue does not | `MarketMatch.matched` is `None` |

`None` is a common, real answer — most brackets on a weather card never line
up across venues, most native events exist on one venue only — and it is
never confused with "no such market": a `MarketNotFound` means the id itself
is bad, a `None` means the id is fine and there is nothing to pair it with.

`MarketLink.side_map` says which side of the anchor is which side of the
match: `{"yes": "yes", "no": "no"}` when the two venues agree, `{"yes": "no",
"no": "yes"}` when they list the proposition on opposite sides (Kalshi's
"Mashtakov wins?" YES is Polymarket's "Pieczonka / Mashtakov" NO).

`match_event` returns a list of native event ids per venue instead of a
single id, because one venue often splits an event the other keeps whole
(a game split by market type, a recurring series split by window):

```python
synpath.match_event("kalshi:KXHIGHTATL-26SEP23")
# EventMatch(anchor=..., event_ids=["weather:atlanta:temperature:max:at:2026-09-23"],
#            events={"polymarket": ["polymarket:1003798"]})
```

### Status

One vocabulary, one meaning per word, on every venue.

| | |
|---|---|
| `open` | listed and trading |
| `closed` | trading stopped, outcome not final |
| `settled` | outcome final and paid |
| `all` | no filter |

A venue that cannot filter by one of them raises `NotSupported`. Polymarket
does for `settled`, since its catalog does not publish resolution status.
Anything outside the four raises `BadRequest` rather than being handed to the
venue to interpret for itself.

### Identifiers

One id per market and per event, `venue:native` (`kalshi:KXELONMARS-99`, `polymarket:2252244`). The full rules are on [Synpath IDs](https://synpath.dev/docs/concepts/ids).

### Paging

`Page` is a `list` subclass carrying `next_cursor`, so code that only wants the
rows can ignore it.

```python
page = kalshi.fetch_markets(limit=50)
len(page), page[0], page.next_cursor
```

There is no `offset`. The catalog changes between calls, and a row number
shifts whenever a market is added or removed ahead of it, repeating one market
or skipping another. Cursors resume after the last item returned instead, the
way Kalshi's and Polymarket's own cursors do.

A walk therefore never repeats a market and never skips one that existed when
it started. A market listed mid-walk is picked up if it sorts after the cursor,
and on the next walk otherwise.

Kalshi has no market-level catalog endpoint, so markets are reached by walking
events and unpacking them, ordered by ticker within each page. Its cursor also
carries a fingerprint of the query it belongs to; reuse it with a different
`query` or `status` and it is refused rather than landing somewhere arbitrary.

Polymarket US offers offset paging and nothing else, so its cursor carries an
offset underneath, tagged with the same kind of fingerprint. The walk is in id
order, so a market listed mid-walk gets a higher id and lands after the cursor
without shifting a row; a market removed mid-walk does shift the rows after it
by one, which no offset can see. That is the venue's limit, stated here rather
than hidden.

Kalshi moves markets settled, and trades made, before its historical cutoff
onto separate archive endpoints. With `status="settled"` or `"all"`, a listing
walks the live catalog and then continues into the archive in the same cursor;
`fetch_trades` does the same, its cursor starting `historical:` once it is in
the archive.

Polymarket's trade tape ends paging (`next_cursor` null) once a page reaches
back past `since`, and at the deepest page its API serves, about the newest
10,500 trades of a market.

`iter_events()` walks the whole catalog at the venue's own maximum page size.

`limit` caps at 100 on every venue, because Polymarket's endpoint caps there
and one argument should not mean two page sizes.

---

---

## Data structures and errors

`Market`, `Quote`, `OrderBook`, `Trade`, `Candle`, `FeeSchedule`, `Series` and `MarketStats` are on [Schemas and Data Formats](https://synpath.dev/docs/concepts/schemas), and the error hierarchy on the [Python SDK page](https://synpath.dev/docs/sdks/python). Rate limiting is built in and shared process-wide, because a venue counts requests per account and IP, not per client object.

---

## Conventions

- **Timestamps**: milliseconds since epoch (`timestamp`) plus an ISO 8601
  string (`datetime`).
- **Prices**: floats in `(0, face_value)`. The venue's original string is in
  `info` if you need the exact decimal.
- **`info`**: every object carries the venue's untouched payload.
- **`face_value`**: read from the venue, not hardcoded to 1. Every complement
  transform depends on it.
- **Sides**: found by position (`market.yes`, `market.no`), never by label
  text, because some Kalshi markets label both sides identically.

---
