# HTTP server

Back to the [README](../README.md).

```bash
pip install synpath
synpath serve                     # your self-hosted server: market data at /, matching and history at /matching and /history,
                                  # live streams at /ws/market, trading at /trading
python -m synpath.server          # market data only, no authentication: http://127.0.0.1:8000/docs
```

`synpath serve` is the whole self-hosted stack in one process; see
[docs/engine.md](engine.md#running-it). The rest of this page is the two
apps it serves, which can also be built and mounted yourself, and the live
market socket it adds.

**Under `synpath serve` every route needs the access token**: the market-data
routes (batch reads and search included), `/matching`, `/history`,
`/ws/market` and everything under `/trading`. Only `/` (what this
server is), `/health` and the API docs (`/docs`, `/openapi.json`, and the
same under `/trading`) stay open: they touch no venue and say nothing about
an account. Send the token as `Authorization: Bearer <token>` (or
`X-API-Key`); a socket may pass it as `?key=` instead, since a browser cannot
set headers on one. A missing or wrong token is a `401`; a socket is accepted
and then closed with `4401`. The standalone `python -m synpath.server` keeps
no accounts, so it has no token; bind it to localhost or mount it behind your
own auth (below).

```bash
TOKEN=...   # ~/.synpath/servers.json, or what `synpath serve` printed
curl -H "Authorization: Bearer $TOKEN" "localhost:8000/venues/kalshi/markets?limit=1"
curl -H "Authorization: Bearer $TOKEN" "localhost:8000/venues/kalshi/markets/KXELONMARS-99/book?side=no"
```

| Route | |
|---|---|
| `GET /` | what this server is, with links; open |
| `GET /health` | liveness; open |
| `GET /venues` | ids, book models, capabilities |
| `GET /venues/{venue}/markets` | `?query= &limit= &cursor= &status= &sort=` (`volume`, `liquidity`, `newest`; anything else is a 400) |
| `GET /venues/{venue}/markets/{market_id}` | |
| `GET /venues/{venue}/events` | |
| `GET /venues/{venue}/markets/{market_id}/book` | `?side=yes|no &depth=` |
| `GET /venues/{venue}/markets/{market_id}/trades` | `?since= &limit= &cursor=`; `limit` up to 1000 |
| `GET /venues/{venue}/markets/{market_id}/candles` | `?timeframe= &since= &until= &limit= &cursor= &source=`, in the YES price; with `since`, paged forward by `next_cursor`; `limit` up to 20,000; `source=quotes` is Polymarket's sampled midpoint history (other venues refuse `source` with 400) |
| `GET /venues/{venue}/markets/{market_id}/fee` | |
| `GET /venues/{venue}/series/{series_id}` | Kalshi and Polymarket US |
| `GET /markets?ids=` | many markets by Synpath id, across venues; see [batch reads](#batch-reads-and-search) |
| `GET /books?ids= &side= &depth=` | many books by Synpath id, across venues |
| `GET /search/markets?query= &venue= &limit= &cursor= &status=` | one search on several venues |
| `GET /search/events?query= &venue= &limit= &cursor= &status=` | the same, events with markets nested |
| `GET /matching/market?id= &venue=`, `GET /matching/event?id=`, `GET /matching/arbitrage` | `synpath serve` only; Synpath's hosted matching, see [hosted services](#hosted-services-matching-and-tick-history) |
| `GET /history/order-book/at`, `/history/order-book/range`, `/history/trades/range` | `synpath serve` only; Synpath's hosted tick history |

Lists are enveloped with the cursor; single resources are not.

```json
{ "data": [ ... ], "next_cursor": "eyJjIjoiQ0RJIiwibyI6NX0", "count": 100 }
```

Every error, on both apps, is one shape. `code` is the library's exception in
snake case, or the HTTP layer's own (`unauthorized`, `forbidden`,
`validation_error`, `unknown_venue`); `details` always has `venue` and
`retryable`, plus `rule` on a risk refusal, `reason` on a venue refusal,
`retry_after` on a rate limit, and `errors` on a malformed request.

```json
{ "error": { "code": "rate_limit_exceeded", "message": "...", "details": { "venue": "kalshi", "retryable": true } } }
```

`400` bad parameters or a venue's refusal of an order, `401` no token, `403` not
permitted, `404` no such thing, `409` a risk rule or a halt, `422` a malformed
body, `429` slow down, `501` the venue has no such capability, `502` the venue
failed, `504` it did not answer. The sockets (`/ws/market`,
`/trading/ws/events`) close with `4401` without a valid token.

## Batch reads and search

The HTTP form of `synpath.Client`'s cross-venue reads: `fetch_markets_by_ids`,
`fetch_order_books`, and `fetch_markets` / `fetch_events` with a query and no
venue. Ids are grouped by venue and each venue is asked once, in parallel,
through the same adapters (and rate limiters) as every other route; each
venue batches as the [API table](api.md) says (Polymarket answers 100 markets
or 500 books in one request).

| Route | Parameters | Response |
|---|---|---|
| `GET /markets` | `ids`: comma-separated Synpath ids, at most 100 (duplicates once) | `{"data": [Market, …], "errors": {id: error}}`; `data` in the order asked |
| `GET /books` | `ids` as above, `side` (`yes`, default, or `no`), `depth` (1–1000) | `{"data": {id: OrderBook}, "errors": {id: error}}` |
| `GET /search/markets` | `query` (1–200 characters), `venue` (repeatable; default every venue), `limit` (per venue, 1–100, default 20), `cursor` (only with exactly one `venue`), `status` | `{"results": {venue: {"data", "next_cursor", "count"}}, "errors": {venue: error}}` |
| `GET /search/events` | the same | the same, with `Event`s |

`GET /markets` is also the batch **quote** read: each market carries
`yes.quote` and `no.quote` (bid, ask, last, sizes) and `stats`, exactly as
`fetch_markets_by_ids` returns them.

**Partial results, never a failed call.** Every id asked for is in exactly
one of `data` and `errors`, and every venue searched in one of `results` and
`errors`. Each error is the same `{"code", "message", "details"}` a single
route would answer with: `bad_market_id` (not `venue:native`),
`unknown_venue`, `market_not_found` (the venue no longer serves it), or the
venue's own failure, such as `exchange_not_available` with
`details.retryable: true`. A venue without a batch book endpoint is read one
book per market. Only a request wrong as a whole is refused: more than 100
ids (`400 too_many_ids`), no ids, a `cursor` with several venues (`400`), an
unknown `venue` in a search (`404 unknown_venue`).

## Hosted services: matching and tick history

`synpath serve` only. These call Synpath's hosted services, matching at
`https://api.synpath.dev` (`SYNPATH_MATCHING_URL`) and tick history at
`https://api2.synpath.dev` (`SYNPATH_HISTORY_URL`), with **serve's** Synpath
API key: `SYNPATH_API_KEY` in serve's environment, otherwise the key
`synpath login` and `synpath keys create` saved. A client of serve needs no
Synpath key and never connects to either service itself.

| Route | Parameters | Response (library call) |
|---|---|---|
| `GET /matching/market` | `id` (Synpath id), `venue` (optional: where to look) | `MarketMatch` (`synpath.match_market`); `matched: null` means nothing on the other venue |
| `GET /matching/event` | `id` | `EventMatch` (`synpath.match_event`) |
| `GET /matching/arbitrage` | | the matching service's cross-venue scan, as it answers: `computed_at` and `results` (each outcome's `yes` and `no` legs with `venue`, `native_market_id`, `title`, `side`, `price`, `fee`, `url`; `gross_spread`, `fees`, `net_profit`) |
| `GET /history/order-book/at` | `market_id`, `as_of_ms`, `side`, `depth` (1–1000) | `OrderBookAtResponse` (`fetch_order_book_at`) |
| `GET /history/order-book/range` | `market_id`, `start_ms`, `end_ms`, `side`, `limit` | `OrderBookRangeResponse` (`fetch_order_book_range`) |
| `GET /history/trades/range` | `market_id`, `start_ms`, `end_ms`, `limit` | `TradesRangeResponse` (`fetch_trades_range`) |

A hosted failure has its own code, so it is never mistaken for serve
refusing the caller's token (`401`):

| The hosted service | Serve answers |
|---|---|
| refuses serve's Synpath API key, or there is none (401, 403) | `502 hosted_unauthorized`; the message says to set `SYNPATH_API_KEY` or run `synpath login` |
| has no such listing (its own 404) | `404 market_not_found` |
| is not deployed (a 404 from the host in front of it) | `502 hosted_unavailable`, retryable |
| cannot return the range whole (413) | `413 result_too_large`: narrow the window or raise `limit` |
| rate limits (429) | `429 rate_limit_exceeded`, retryable |
| does not answer in time | `504 hosted_timeout`, retryable |
| is unreachable or fails (5xx) | `502 hosted_unavailable`, retryable |

Every error carries `details.venue: "synpath"`. An id that is not a Synpath
id, or a window whose `end_ms` is not after `start_ms`, is a `400` before
any hosted call.

## Live market data: `/ws/market`

`synpath serve` only. One socket carries any number of markets: books,
public trades and quotes, from the library's own venue streams
([streaming](streaming.md)), on all eight venues.

```bash
websocat "ws://127.0.0.1:8000/ws/market?key=$TOKEN&market_id=kalshi:KXELONMARS-99&channels=book,trades&depth=16"
```

Subscriptions can be named on the URL (`market_id`, repeatable; `channels`,
default `book`; `side`, default `yes`; `depth`, default the whole book), and
added or dropped at any time with messages:

```json
{"op": "subscribe", "market_id": "polymarket:2252244", "channels": ["book", "trades", "quotes"], "side": "yes", "depth": 16}
{"op": "unsubscribe", "market_id": "polymarket:2252244"}
```

Market ids are Synpath ids (`venue:native`), or a bucket's `bucket:<id>`
([below](#a-buckets-merged-book)). The server answers with JSON
objects, each with a `type`:

| `type` | When | Fields |
|---|---|---|
| `subscribed` | a subscribe took | `venue`, `market_id`, `side`, `channels` (those the venue streams), `unsupported` (asked for, not streamed by this venue) |
| `unsubscribed` | an unsubscribe took | `market_id` |
| `book` | on subscribe, then after every change, at most every `book_interval_s` (config, default 0.1 s) | `venue`, `market_id`, `side`, `ready`, `bids` and `asks` (`[{"price", "size"}]`, best first, decimal strings), `best_bid`, `best_ask`, `sequence`, `timestamp` (venue), `sent_at` |
| `trade` | channel `trades` | `venue`, `market_id`, `id`, `price` (YES), `quantity`, `taker_side` (`buy` took YES), `timestamp`, `received_at` |
| `quote` | channel `quotes`, the subscribed side only | `venue`, `market_id`, `side`, `bid`, `ask`, `bid_size`, `ask_size`, `last`, `volume`, `open_interest`, `timestamp`, `received_at` |
| `market_status` | the venue reports a market opening, pausing, closing or resolving | `venue`, `market_id`, `state`, `native`, `result`, `timestamp` |
| `status` | the venue connection itself: `connected`, `disconnected`, `gap`, `resynced`, `error`, `failed`, … | `venue`, `state`, `detail` |
| `error` | a request refused, or the stream lost | `code`, `message`, `market_id` |

**A book is the stream's maintained book, never re-derived.** Every `book`
message is the market's whole book (to `depth`) as the venue stream keeps
it, so a client needs no delta logic. `ready: false` (with no levels) means
the stream has no snapshot yet, or stopped trusting the one it had after a
drop or a gap; the venue stream reconnects by itself, and the fresh snapshot
is sent as soon as it lands. A book changing faster than `book_interval_s`
is coalesced: the client always gets the latest state, not every step.

**One venue connection, shared.** The server opens one market connection per
venue and reference-counts markets on it across every socket. A venue the
engine trades is read from the engine's own feed connection, so the screen
and the stops see the same book; the server never opens a second one. When
the last socket leaves a market it is unwatched upstream (Kalshi and
Polymarket US streams cannot unwatch, so their subscription stays until the
connection closes) unless the engine still watches it; a connection the
server opened for sockets alone closes with its last subscriber.

**Credentials come from the server.** Polymarket, Hyperliquid and Limitless
stream without any. Kalshi, Polymarket US, Opinion, predict.fun and SX Bet
need their keys in the server's environment or `.env`; without them a
subscribe answers `error` with `credentials_required` rather than falling
back to polling. Other codes: `bad_market_id`, `bad_request`,
`not_subscribed`, `subscribe_failed` (the venue refused the market),
`streams_disabled` (`--no-streams`), `stream_ended` (the venue refused the
connection for good; subscribe again to reopen it). Not every venue streams
every channel: Limitless and predict.fun carry no public trades, and that is
said in `unsupported`. A client that falls more than 10,000 messages behind
is closed with `1013`.

### A bucket's merged book

A bucket (`bucket:<id>`, defined through `/trading/buckets`) subscribes like
a market, on the same socket:

```json
{"op": "subscribe", "market_id": "bucket:3f2a…", "channels": ["book", "trades"], "depth": 16}
```

The server subscribes every enabled member on the shared venue connections
above (a bucket opens none of its own) and merges their books with the library's
router (`synpath.engine.router.view` and `merge`): each member's book in
bucket terms, a flipped member read through its NO side, every level net of
its venue's taker fee, labelled with the member it came from. `side` must be
`yes` (the book is already in bucket terms); `depth` is levels per member and
side; `quotes` is not streamed for a bucket. The key must see every member
venue, as on the bucket routes.

| `type` | When | Fields |
|---|---|---|
| `subscribed` | the subscribe took | as for a market, with `venue: "bucket"` and `bucket` (the definition) |
| `bucket_book` | on subscribe, then after any member's book changes (coalesced as `book` is) | `market_id`, `bucket_id`, `name`, `side`, `ready`, `asks` (cheapest net first) and `bids` (richest net first), each `[{"price", "net_price", "size", "market_id", "venue"}]`; `best_ask`, `best_bid` (best price before fees), `best_ask_net`, `best_bid_net`; `members` (`market_id`, `venue`, `flip`, `enabled`, `ready`, `sequence`, `timestamp`, `error`; a member switched off is listed with `enabled: false`, `ready: false` and no book); `fees_unknown` (members whose fee the library cannot price, counted as free); `fee_floors` (per-order minimum fee by member); `timestamp`; `sent_at` |
| `trade` | channel `trades`: a member's public trade | as for a market, with `market_id` the bucket's, `member_market_id`, `flip`, and `price` and `taker_side` in bucket terms (a flipped member's YES trade at p is 1 - p, the taker on the other side) |
| `status`, `market_status` | as for a market | the member venue's own |
| `error` | a member the server cannot stream (`credentials_required`, …, with `market_id` the bucket's), or `bucket_not_found`, `forbidden`, `buckets_unavailable`, `bad_request` | `code`, `message`, `market_id` |

**Ready only while every enabled member is.** A member switched off
(`enabled: false` on the definition) is not subscribed, adds no levels and
does not hold `ready` back. A bucket missing an enabled member is not the
bucket, so while any enabled member's book has no snapshot, is untrusted after a drop
or a gap, or is crossed, `bucket_book` comes with `ready: false` and no levels;
`members` says which. Fees are each member's published fee schedule
(`fetch_fee_schedule`), read once and kept ten minutes, quoted for 100
contracts and divided back to one, as an order on the bucket prices its levels.
Editing a bucket (`PATCH`, including switching a member on or off, or archive)
moves every socket streaming it to the new definition: a member switched on is
subscribed, one switched off released. A member's stream ending (`stream_ended`) ends the bucket
subscription too; subscribe again.

### Previewing an order on a bucket

`POST /trading/buckets/{id}/plan` with `{"side": "buy", "quantity": "100",
"worst_price": "0.45"}` answers the router's `plan()` for a market order on
the bucket, now, over its enabled members only: `legs` (per member: `market_id`, `venue`, `flip`, the
member's own `side` and YES `price` after the flip and tick rounding, as the
venue would receive them; `quantity`, `bucket_price`, `net_price`, `fee`,
`fee_floor`), `allocated` (never more than `quantity`), `unfilled`,
`stop_reason` (`worst_price`, `liquidity`, `min_quantity`, or null when it
fills), `expected_net_price`, `fees`, `fees_unknown`, `computed_at`, and
`books`: per enabled member, `stream` when the server holds its live book, else `rest`
(read from the venue for this request) or `missing`. Fees and precision are
the ones an order on the bucket uses. Nothing is placed or reserved; the
books can move before an order arrives.

### Typed clients in any language

Routes are explicit rather than dispatched from a method name, so
`GET /openapi.json` describes every response with a real schema, not `object`.
Generate a client instead of hand-writing one:

```bash
python -m synpath.server schema --out openapi.json          # the read contract
python -m synpath.server schema --trading --out openapi.json   # the trading one
npx openapi-typescript openapi.json --default-non-nullable false -o src/synpath.d.ts
```

`schema` writes the document without starting a server or holding any
credentials, which is what a CI job wants. Pass `--default-non-nullable
false` or every field with a default (`time_in_force`, `post_only`) is
generated as required. A worked example, place through cancel in TypeScript,
is in [`clients/typescript`](https://synpath.dev/docs/sdks/typescript).

## Trading

Placing an order needs to know who is asking, so trading is a second app with
its own keys and permissions:

```python
from synpath.engine import Engine, EngineConfig
from synpath import ControlStore, create_trading_app

store = await ControlStore("control.db").open()
user, key = await store.bootstrap("owner")     # once; the key is shown once
app = create_trading_app(engine, store)
```

```bash
python -m synpath.server bootstrap --control control.db   # the same, from a shell
```

| Route | Permission | What it does |
|---|---|---|
| `GET /me`, `GET /accounts`, `GET /status` | any key / `view` | who this key is, what it may touch, what the engine is doing |
| `POST /orders`, `PATCH /orders/{id}`, `DELETE /orders/{id}` | `trade` on that account | place, amend, cancel; engine-held types too |
| `POST /buckets`, `PATCH /buckets/{id}`, `DELETE /buckets/{id}` | `trade` on every member venue | define a bucket (the server assigns its id, or keeps a client-made UUID `id`; taken is `409`); rename it, move it to another `book`, replace its `members` (each `market_id`, `flip`, and `enabled`, default `true`: a member switched off stays in the definition but is not routed to or merged; at least two members, at least one enabled), or archive or restore it (`status`); archive one |
| `GET /buckets` (`book`, `status`), `GET /buckets/{id}`, `GET /buckets/{id}/orders`, `GET /buckets/{id}/orders/{order_id}`, `GET /buckets/{id}/position` | `view` on every member venue | buckets, the orders on one with fills by venue, the position netted in bucket terms |
| `POST /buckets/{id}/plan` | `view` on every member venue | preview where an order on the bucket would go now (below); nothing is placed |
| `GET /orders`, `GET /orders/{id}`, `GET /fills`, `GET /positions`, `GET /balances`, `GET /pnl` | `view` | what the engine and the venues hold |
| `GET`/`PUT /fair-values` | `view` / `trade` | the marks positions are valued at |
| `GET /risk`, `PUT /risk` | `view` / `manage_credentials` | the rules in force, versioned on change |
| `POST /halt`, `POST /resume` | `trade` | the kill switch |
| `GET`/`POST`/`DELETE /grants`, `POST /keys`, `DELETE /keys/{id}` | `manage_members` / `manage_credentials` | who may do what |
| `GET /audit` | `manage_members` | the append-only history of those changes |
| `WS /ws/events` | `view` | the engine's events, replayed from a cursor then live |

**Permissions are per subaccount and nothing is implied.** A grant is
`(user, account, permission)` where the account is `venue:name` or `*`, and
the permission is one of `view`, `trade`, `manage_credentials`,
`manage_members`. A key that may trade may not grant; a key that may grant
may not trade.

**Keys are stored as hashes.** The secret is shown once, when it is issued.
A stolen database cannot be used to trade.

**The audit log is append-only, enforced by the database.** Triggers record
every grant and key change with the actor, the request, and the row before
and after; triggers on the audit table itself raise on update and delete. No
route writes it, and none could.

**The event socket replays.** `GET /ws/events?since=<seq>` sends everything
after that journal sequence number, then stays open. A client that
reconnects does not miss the fill that happened while it was away.

```bash
websocat "ws://127.0.0.1:8000/trading/ws/events?key=$SYNPATH_ACCESS_TOKEN&since=0&kinds=order,fill"
```

### Mounting it

The *read* app ships **no authentication** of its own, and none is planned. A service with auth
baked into its open-source core forces every host to work around it. Put your
own in front, either as route dependencies (`create_app(dependencies=[Depends(my_auth)])`
protects every route but `/` and `/health`; this is how `synpath serve` requires its
token) or around the router:

```python
from fastapi import Depends, FastAPI
from synpath import VenueRegistry, create_app

outer = FastAPI()
outer.state.registry = VenueRegistry()
outer.include_router(create_app(docs=False).router, dependencies=[Depends(my_auth)])
```

Adapters are reused per venue across requests, because the rate limiter and
connection pool have to be shared: a fresh client per request would hand every
request a full token budget and sail past the venue's ceiling.

---
