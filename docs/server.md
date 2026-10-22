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
`retry_after` on a rate limit, `client_order_id` on an order whose outcome is
not known yet, and `errors` on a malformed request.

```json
{ "error": { "code": "rate_limit_exceeded", "message": "...", "details": { "venue": "kalshi", "retryable": true } } }
```

`202` an order whose outcome is not known yet (below), `400` bad parameters or
a venue's refusal of an order, `401` no token, `403` not permitted, `404` no
such thing, `409` a risk rule, a halt, or a cancel the venue could not confirm,
`422` a malformed body, `429` slow down, `501` the venue has no such
capability, `502` the venue failed, `504` it did not answer. The sockets (`/ws/market`,
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
and the stops see the same book; the server never opens a second one. A stop
placed on a market a screen already shows reads that book at once: the venue
sends no new snapshot for it, and the stop does not wait for the next change. When
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

`GET /market/streams` says what each open venue connection is doing:
`connected`, `connecting`, `reconnecting` (the stream retries by itself) or
`failed` (refused for good), with the last thing it reported. A venue not
listed has no connection open. The engine's own connections are always
listed; serve subscribes the trades of every market a stop watches or a
position holds, beside its book.

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
| `GET /order-log` (`order_id`, `limit`, `before`) | `view` | every action on orders from the journal, newest first, repeats folded (`synpath.engine.order_log`): saved, sent, accepted, triggered, cancel requested, cancelled, filled, refused; one order with its cancel, fills and children |
| `GET /journal` (`kind`, `limit`, `before`) | `view` | the journal's events as recorded, newest first, every kind or one prefix |
| `GET /risk`, `PUT /risk` | `view` / `manage_credentials` | the rules in force, versioned on change |
| `POST /halt`, `POST /resume` | `trade` | the kill switch; with the `cancel` policy it first cancels our open RFQs and quotes (`rfq` in the result) |
| `POST /rfqs`, `DELETE /rfqs/{id}` | `trade` on the venue's account | ask for quotes on a market or a combo, by `quantity` or `amount`; cancel one ([RFQs](#rfqs-quotes-and-combos)) |
| `GET /rfqs` (`venue`, `status`, `market_id`, `source`, `limit`), `GET /rfqs/{id}`, `GET /rfqs/{id}/quotes` (`status`) | `view` | our RFQs, and the quotes on one |
| `POST /quotes/{id}/accept` | `trade` | take one side of a quote: `202` at once, `200` when `wait` and the quote is final |
| `GET /rfqs/incoming`, `GET /quotes` (`venue`, `rfq_id`, `status`) | `view` | the quoter side: others' open RFQs, our quotes |
| `POST /quotes`, `PUT /quotes/{id}`, `DELETE /quotes/{id}`, `POST /quotes/{id}/confirm` | `trade` | send, replace, withdraw a quote; confirm an accepted one (`202`/`200` as accept) |
| `POST /combos`, `POST /combos/lookup`, `GET /combos/{market_id}` | `trade` / `view` / `view` | create or look up the combo for some legs (idempotent; `collection_id` on Kalshi); look one up without creating it; read its legs |
| `GET /combos/markets` (`venue`, `event_id`, `tag`, `limit`, `cursor`), `GET /combos/collections` (`venue`, `status`, `event_id`, `limit`, `cursor`) | `view` | where combos can be built: Polymarket's and Polymarket US's combo-eligible markets; Kalshi's collections with their event tickers. Cached 15 s |
| `GET /combos/board` (`venue`, `pages`) | `view` | a combo venue's eligible markets as games (moneyline / spread / total picks per participant) and YES / NO questions, with categories and counts ([combo board](#the-combo-board)). Cached 30 s |
| `GET`/`POST`/`DELETE /grants`, `POST /keys`, `DELETE /keys/{id}` | `manage_members` / `manage_credentials` | who may do what |
| `GET /audit` | `manage_members` | the append-only history of those changes |
| `WS /ws/events` | `view` | the engine's events, replayed from a cursor then live |

**Listing orders.** `GET /orders` takes `scope` (`open`, the default: resting
venue orders and live engine-held parents, `waiting`, `triggered` or
`pending_cancel`; `all`: every state, `closed`, `canceled`, `rejected` and
`expired` too), `market_id`, `venue`, `book`, `since` (created at or after,
milliseconds since the epoch), `limit` (1–1000; default every open order for
`open`, 100 for `all`) and `cursor`. Rows are `Order`s, newest first by
creation. An engine-held parent has `held_by: "engine"`; a venue order a
parent placed has `held_by: "venue"` and the parent's id in `parent_id`, so
children group under their parent. Follow `next_cursor` until it is null; a
page can hold fewer than `limit` rows when some are on accounts the key may
not view. A cursor this server did not give out is `400`.

**An order whose outcome is not known yet is `202`, not a failure.** When the
venue gives no verdict (a timeout, a lost response, a 5xx), the send may have
landed, so `POST /orders` answers `202` with the error envelope, code
`submission_in_doubt`, `details.retryable: true` and `details.client_order_id`
(the one sent, or the one the engine assigned). Retry with the same body and
`client_order_id`: the engine asks the venue first and answers `201` with the
order it finds, `202 submission_pending` while the venue has no answer yet, or
the original refusal; it never sends a second order. Reusing the id for a
different instruction is `400`. `synpath.Client(server=...)` raises
`SubmissionInDoubt` and `SubmissionPending`, each with `client_order_id`.

```json
{ "error": { "code": "submission_in_doubt", "message": "...",
             "details": { "venue": "kalshi", "retryable": true, "client_order_id": "6f1c…" } } }
```

**A cancel never relabels a fill.** `DELETE /orders/{id}` on an order already
finished returns it as it is. When the venue answers "not found", the server
reads the order back and returns the venue's verdict: `closed` with its
filled quantity if it filled first, `canceled` only if the venue cancelled
it. An engine-held parent may answer `pending_cancel` until its child's fate
is known. A venue with no record of the order at all is `409
cancel_unconfirmed` (`CancelUnconfirmed` in Python); the order keeps its last
observed status.

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

### RFQs, quotes and combos

The engine's RFQ desk ([docs/rfq-combo.md](rfq-combo.md)) over HTTP. Every
write goes through the engine, so risk, the journal, the halt and recovery
are the same as for an order. Prices are YES prices; a quote's `ask` is what
we can buy YES at, its `bid` what we can sell at.

**Which venue.** RFQ and quote ids are the venue's own, not Synpath ids. A
route takes the venue from `venue` (query or body), the `account` named, a
Synpath market id, what the engine already holds about the id, or the only
venue with the capability; otherwise it is `400`. The grant is checked on
that venue's account (`venue:name`) before anything is sent. A venue without
the capability (`has["rfq_taker"]`, `rfq_quoter`, `combos`) is `400`; a call
the venue does not offer (Polymarket US has no read-only combo lookup) is
`501 not_supported`; an unknown id is `404`.

**Accept and confirm answer `202`.** `POST /quotes/{id}/accept` takes
`AcceptQuote` (`rfq_id`, `outcome`, `action`, `worst_price`, `quantity` or
`amount`, `client_order_id`, `account`, `book`, `trader`, `tags`) plus `wait`
and `timeout_s` (default 30, at most 120). It answers `QuoteResult`: the
`quote`, the intent key `client_order_id`, `final`, `intent_state` (`sending`
until the quote is final, then `sent` or `failed`) and `order_id` once the
execution's order is adopted. The status is `202` unless `wait` was true and
the quote ended (`executed`, `canceled`, `expired`, `failed`), then `200`.
The rest arrives as events. A send with no answer is `202
submission_in_doubt` with `details.client_order_id`; a retry under that key
reads the venue first and never sends twice. `POST /quotes/{id}/confirm`
(the maker's last look; body optional: `rfq_id`, `venue`, `wait`,
`timeout_s`) is the same, keyed `rfq-confirm:{venue}:{quote_id}`.

**Refusals.** A risk rule is `409 risk_rejected` with `details.rule`:
`worst_price` (the quote is worse than the caller's limit), `kill_switch`,
`max_order_contracts`, `max_order_notional`, `max_position`, and the other
order rules. Accept and confirm run every order rule; creating an RFQ and
sending a quote run the halt, a paused book, the restricted list and the
size limits. Cancels (`DELETE /rfqs/{id}`, `DELETE /quotes/{id}`) are never
refused by risk.

**Lists.** `GET /rfqs` and `GET /rfqs/incoming` read each venue by default;
`source=engine` returns what the engine already holds (our open RFQs, the
incoming feed's last view) without a venue call. Rows are filtered to the
accounts the key may view. `GET /rfqs` rows carry the engine's `book` and
`trader`; `GET /quotes` fills in the `expires_at` the engine holds (venues
may not take one).

**Combos.** `POST /combos {legs: [{market_id, outcome}], venue?, collection_id?}`
creates or returns the combo (the same legs give the same id; an existing
combo costs no venue quota) and keeps its legs with the engine
(`combo.saved`). A combo is its own instrument: `POST /rfqs` with its
`market_id`, and its position carries `info.combo` with the legs.
`collection_id` is Kalshi's: the collection to build in. Without it the one
open collection that takes every leg's event is used; when several do, the
answer is `400` naming them, with the tickers in `details.collections`.
Polymarket and Polymarket US refuse it (`400`).

**Where combos can be built.** Two reads, each served from memory for 15 s
(per server, by query) so the terminal can poll them:

- `GET /combos/markets?venue=polymarket|polymarket_us` pages the venue's own
  list of combo-eligible markets as `ComboMarket` rows:
  `{market_id, venue, title, event_id, event_title, position_ids, min_legs,
  max_legs, status, tags, info}` in `{data, next_cursor, count}`.
  `market_id` is the leg's. Polymarket (the public catalogue, by volume):
  `position_ids` `{yes, no}` name the leg's `info.position_id`; `tag` is
  applied to each page, `event_id` is a `400`. Polymarket US (open events'
  `comboEnabled` markets, newest first): `event_id`
  (`polymarket_us:<event slug>`) and `tag` (a tag slug) are the venue's
  filters, and `limit` counts events read. Either way a page can be short
  or empty and still carry a `next_cursor`. `501` elsewhere.
- `GET /combos/collections?venue=kalshi` pages Kalshi's collections as the
  venue's objects; each carries `associated_event_tickers` (every event a
  leg may come from), `size_min`/`size_max` and `associated_events`.

**Events.** `/ws/events` carries `rfq.created`, `rfq.closed`, `rfq.in_doubt`,
`rfq.side_mismatch`, `quote.received`, `quote.sent`, `quote.replaced`,
`quote.accepted`, `quote.confirmed`, `quote.executed`, `quote.canceled`,
`quote.expired`, `quote.failed`, `quote.in_doubt`, `combo.saved`, then the
resulting order's `order.*` and `fill.booked`. Quote events are keyed
`{venue}:quote:{id}`. `rfq.incoming` is not journaled, so it is live only: a
replay from a cursor does not include it. `kinds=rfq,quote,combo` filters
the replay and the live stream alike.

### Cross-venue RFQ groups

One RFQ on every venue that lists the same market or combo, ranked as one
list; accepting one quote cancels the rest
([design](rfq-combo.md#10-cross-venue-rfq)).

| Route | Grant | Answer |
|---|---|---|
| `POST /rfq-groups` | `trade` on each venue asked | `201` `RfqGroup` |
| `POST /rfq-groups/preview` | none beyond the key (venues it may not trade are `forbidden`) | `200` `GroupPreview`: a dry run, nothing created |
| `GET /rfq-groups?status=&limit=` | `view` on every venue of a group | `{data, next_cursor, count}` of `RfqGroup` |
| `GET /rfq-groups/{id}?refresh=` | `view` on every venue of it | `RfqGroup`; `refresh=true` reads each open member's quotes first |
| `POST /rfq-groups/{id}/accept` | `trade` on every venue of it (the others are cancelled) | `202` `GroupAcceptResult`, `200` with `wait` once final |
| `DELETE /rfq-groups/{id}` | `trade` on every venue of it | `RfqGroup`, `canceled` |

- **Create body**: `instrument_id` (a market, `bucket:<id>`, or a combo's
  own id) or `legs` (`[{market_id, outcome}]`, each on any venue or a
  bucket), exactly one; `quantity` or `amount`, exactly one; `outcome`
  (`yes`) and `action` (`buy`), in the instrument's terms; `cross_venue`
  (a plain market: also ask every other RFQ venue's match); `venues` (only
  these) and `exclude_venues`; `book`, `trader`, `tags`. A venue the key may
  not trade is skipped as `forbidden` (`403` when named in `venues`, or when
  the key may trade none). No venue left is `400 no_venue` with
  `details.skipped`.
- **`RfqGroup`**: `id`, `instrument`, the size, `outcome`, `action`,
  `members` (one per venue: `venue`, `instrument_id`, `flip`, `via`, mapped
  `legs`, `combo`, `collection_id`, `rfq_id`, `status`, `detail`),
  `skipped` (`venue`, `code`, `reason`, `leg`), `quotes` best first
  (`quote_id`, `venue`, `rfq_id`, `raw_bid`/`raw_ask`, `raw_price` (the
  member's YES price), `price` (the group's), `outcome_price`, `quantity`,
  `fee`, `fee_per_contract`, `effective_price`, `effective_outcome_price`,
  `partial`, `rank`, and the venue's deadlines: `expires_at` (the quote's
  own expiry, else its RFQ's; null where the venue gives none, as Kalshi and
  Polymarket US do), `expires_from` (`quote`, `rfq`), `confirm_by`,
  `execute_by`), `expires_at` (the soonest expiry among the quotes that can
  be accepted now), `status`, `claim_key`, `accepted_quote_id`,
  `accepted_venue`, `order_id`, `failures`, timestamps. A group has no
  window of its own: it is `expired` when every member RFQ has closed or
  expired on its venue (the follower re-reads each open member RFQ every
  10 s, and at once past its `expires_at`), or `canceled` by `DELETE`.
- **Accept body**: `quote_id`, `venue` (the quote's: quote ids are the
  venues' own, so two venues may return the same one; required on a group
  that spans more than one venue, else `400 venue_required` with
  `details.venues`; a venue outside the group is `400 venue_not_in_group`),
  `worst_price` (the group's YES terms), `wait`, `timeout_s`, `tags`. Checked like `POST /quotes/{id}/accept` before
  anything is written; a refusal leaves every venue standing. Then the
  group is claimed in the journal, every other member RFQ cancelled
  (Polymarket global's left to expire: member `lapsing`), and the quote
  accepted. `GroupAcceptResult`: `group`, `quote`, `venue`,
  `client_order_id` (the member accept's intent), `claim_key`, `final`,
  `intent_state`, `order_id`. A second accept on the group is `409` with
  `details.rule` `rfq_group_accept_in_flight` or `rfq_group_accepted`; a
  failed accept frees the group for another quote. `DELETE` while an
  accept is in flight is `409` too. A quote on a member RFQ accepted through
  `POST /quotes/{id}/accept` is `400`.
- **Preview body**: the create body with the size optional (unsized, a buy by amount is planned).
  `GroupPreview`: `instrument`, `offered` (one `PreviewVenue` per venue the group would ask: `venue`,
  `via`, `instrument_id`, `flip`, mapped `legs`, `collection_id`, `eligibility`, and from the
  server's catalogue `leg_prices` (each leg's price in its outcome's terms: YES ask, or 1 - YES bid)
  and `combined_price` (their product, an estimate, not a quote)), and `skipped` as on a group. The
  matching service and the venues' eligibility lists are read; nothing is journaled or sent to an RFQ
  desk. Served from memory for 10 s, and matching answers are reused for 60 s, so the group that
  follows a preview asks the matching service nothing new.
- **Events** on `/ws/events` (`kinds=rfq_group`): `rfq_group.created`,
  `rfq_group.quote` (one ranked `GroupQuote`, with `group_id`),
  `rfq_group.updated`, `rfq_group.accepting`, `rfq_group.accepted`,
  `rfq_group.executed`, `rfq_group.accept_failed`, `rfq_group.canceled`,
  keyed `rfq_group:{id}`; all but `rfq_group.quote` carry the whole group in
  `payload.group`. The members' own `rfq.*` and `quote.*` events flow as
  before.
- **Halt**: `POST /halt` with the `cancel` policy cancels every open group's
  members first (`rfq.groups` in the result).

### The combo board

`GET /trading/combos/board?venue=` turns a combo venue's eligibility into what a combo builder draws
(`synpath/engine/combo_board.py`): `ComboBoard` `{venue, count, complete, as_of, categories, events}`.

- **Events**: a `game` (`rows`: one per participant, `name`, `abbr` and `picks` by `moneyline`,
  `spread`, `total`, each a `BoardPick` or null where the venue lists no such market) or a
  `question` (`yes`, `no`). A `BoardPick` is a leg: `market_id`, `outcome`, `label` (`CHI`,
  `CHI -2.5`, `O 41.5`), `kind`, `line`, `title`, and the catalogue's `yes_bid`, `yes_ask` and
  `price` (what the pick costs now). Each event has `category` / `category_label` (a league or a
  kind, from tags or the Kalshi series), `group` (`sports`, `other`), `start_time`, `volume` and
  `market_ids`.
- Everything comes from the venue's own fields, never a URL slug or title wording; a participant
  the venue gives no abbreviation shows its name.
- **Kalshi**: the events of its open collections (`associated_event_tickers`), their markets read
  from the server's catalogue (`fetch_events_by_ids`, at most 150 events). The type is the event's
  `product_metadata.competition_scope` (`Game`, `Spread`, `Point Total`; any other scope, such as
  `1st Half Winner` or `Team Total`, is a question); a game joins the events that share the
  `sub_title` (`NE vs CHI (Oct 22)`) within one `competition`. Participants are the winner markets'
  `yes_sub_title` (`Tie` is the draw), identified by `custom_strike` (the team's id, which a spread
  market names too); codes are the `sub_title`'s, side by side with the title's names; a spread's
  line is its `floor_strike` (`CHI -2.5` for "Chicago wins by over 2.5"), a total's too.
- **Polymarket**: its catalogue rows (up to `pages` pages), with Gamma's market for each (from the
  server's catalogue: `sportsMarketType` `moneyline` / `spreads` / `totals`, `line`,
  `groupItemTitle`, its event) and the games' Gamma events (`fetch_events_by_ids`: `teams` with
  `abbreviation`). A two-sided market backs its outcomes' teams (YES the first; a spread at `line`
  for the first, the opposite for the second); a YES / NO market (a soccer result) backs its
  `groupItemTitle`'s team, or the draw. Without Gamma's market a row is a question.
- **Polymarket US**: its eligible list (events with markets nested): the type from `marketType`
  (`moneyline`, `drawable_outcome`, ...), each outcome's team from `marketSides[].team` (`name`,
  `displayAbbreviation`), the event's `teams` in order; a drawable result is one YES market per team
  and one for the draw.
- Catalogue quotes replace the listed prices. Anything that is not a game is a question (an event
  shows its three markets nearest even); a question with named outcomes says them on its buttons.
- `categories`: `key`, `label`, `group`, `count` (events), sports first, by count.

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
