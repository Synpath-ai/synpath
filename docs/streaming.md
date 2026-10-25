# Streaming

Back to the [README](../README.md).

`synpath.ws` turns each venue's WebSocket (and the Polymarket US exchange
API's gRPC streams) into an async stream of typed events that reconnects on
its own and says when it may have missed something.

```python
import asyncio
from synpath import PolymarketMarketStream, BookEvent, StreamStatusEvent

async def main():
    async with PolymarketMarketStream() as stream:
        await stream.watch_order_book(["polymarket:2252244"])     # both sides of the market
        async for event in stream:
            if isinstance(event, BookEvent):
                print(event.market_id, event.side, event.best_bid, event.best_ask)
            elif isinstance(event, StreamStatusEvent):
                print(event.state, event.detail)

asyncio.run(main())
```

## Streams

| Stream | Endpoint | Credentials | Carries |
|---|---|---|---|
| `KalshiStream` | Kalshi trade API WebSocket, demo or prod | Kalshi key (the handshake is signed even for market data) | books, tickers, public trades, market lifecycle; orders, fills, positions, order groups |
| `PolymarketMarketStream` | CLOB market channel | none | books, top of book, trades, new markets and resolutions, tick size changes |
| `PolymarketUserStream` | CLOB user channel | CLOB API credentials, or a `PolymarketTrading` to derive them | orders; fills with settlement state |
| `PolymarketUSMarketStream` | `api.polymarket.us/v1/ws/markets` | retail API key | full books, top of book, trades, market state |
| `PolymarketUSPrivateStream` | `api.polymarket.us/v1/ws/private` | retail API key | orders, fills, positions, balances |
| `OpinionMarketStream` | `ws.opinion.trade` | Opinion API key (every channel needs one) | books, trades, last prices |
| `OpinionUserStream` | `ws.opinion.trade` | Opinion API key | orders; fills once the chain confirms them |
| `HyperliquidMarketStream` | `api.hyperliquid.xyz/ws` (or the testnet) | none | outcome books, prints, top of book, 24h volume |
| `HyperliquidUserStream` | `api.hyperliquid.xyz/ws` | none: the account's address only | orders; fills (splits, merges and settlements as venue events) |
| `PredictFunMarketStream` | `ws.predict.fun` | predict.fun API key | whole books, last settled price, market and trading status |
| `PredictFunUserStream` | `ws.predict.fun` | API key and a wallet JWT | orders; fills as matched, then confirmed or failed on chain |
| `LimitlessMarketStream` | `ws.limitless.exchange` | none | whole books, market resolutions |
| `LimitlessUserStream` | `ws.limitless.exchange` | a Limitless API token | orders; fills as matched, then mined or failed on chain |
| `SxBetMarketStream` | `realtime.sx.bet` (or the testnet) | SX Bet API key (every channel needs one) | whole books, top of book, the tape of taker bets, market changes and settlements |
| `SxBetUserStream` | `realtime.sx.bet` | SX Bet API key and the account's address (from the private key, or `SX_BET_ACCOUNT_ADDRESS`) | orders; fills as matched, then escrowed or failed on chain |
| `BetfairMarketStream` | `stream-api.betfair.com:443`, a TLS socket (Exchange Stream API), not a WebSocket | Betfair application key and a login (every Betfair call needs one) | best offers per runner to 10 levels, runner and market status; conflated to 3 minutes on a delayed key |
| `AzuroMarketStream` | `streams.onchainfeed.org` (`dev-streams` for the testnets) | none | each condition's odds as quotes, one-level books sized by the pool's limits, condition status changes |
| `SmarketsMarketStream` | `api.smarkets.com` REST, polled (no public socket) | none; an optional login undelays the quotes | contract books and top of book at every poll (5 s by default), contract status every few polls |
| `NovigMarketStream` | `api.novig.com/v3/ws` (or Paper's); without a key, the public REST book polled | a subaccount's trading key for the WebSocket (the handshake is signed); none to poll | books kept per order, top of book, the tape, market transitions (going in play voids resting orders) |
| `NovigUserStream` | `api.novig.com/v3/ws` | a subaccount's trading key | orders (open, fill, cancel, reject) and positions of that subaccount |
| `MatchbookMarketStream` | `api.matchbook.com` REST, polled (Matchbook has no push API) | none | runner books, top of book and runner status at every poll (5 s by default) |
| `ProphetXMarketStream` | the host `GET /websocket/connection-config` names (ProphetX's Talaria, Pusher protocol 7) | the Trading API key pair (every channel is private) | each line's ten best levels a side on every change, top of book, the tournament's tape of fills |
| `ProphetXUserStream` | the same host | the Trading API key pair | orders on every change; a fill with each match |
| `MyriadMarketStream` | `ws.myriadprotocol.com/ws` | none | order books (both outcomes), top of book and last per outcome, the tape of matches, markets created and resolved |
| `MyriadUserStream` | `ws.myriadprotocol.com/ws` | the wallet's address, and a subscription token per account channel (`MyriadTrading.subscription_token`) | orders; fills from the wallet's public tape; share balances |
| `GeminiMarketStream` | `ws.gemini.com` (or the sandbox) | none | differential books in YES space from a snapshot, top of book with the last trade, public prints, contract status and strikes |
| `GeminiUserStream` | `ws.gemini.com` | an account-scoped Gemini API key with a time-based nonce (the handshake is signed) | orders; each execution with its fee and maker flag; positions, settlement payouts included; balances |

Each stream's `has` answers the `watch_*` capability keys (`watch_order_book`,
`watch_ticker`, `watch_trades`, `watch_market_status`, `watch_orders`,
`watch_my_trades`, `watch_positions`, `watch_balance`) with `True`, `False` or
`"partial"`.

The Polymarket US exchange API's streams are gRPC; they are described
[below](#polymarket-us-exchange-api-grpc).

### What market streams need

No market stream needs a private key. Which credential each needs, and the
variable `synpath serve` reads it from:

| Venue | Market stream needs | Variable |
|---|---|---|
| Kalshi | the API key: Key ID and private key file (the handshake is signed) | `KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH` |
| Polymarket US | the retail API key: Key ID and Secret Key | `POLYMARKET_US_KEY_ID`, `POLYMARKET_US_SECRET_KEY` |
| Polymarket | nothing | |
| Hyperliquid | nothing | |
| Limitless | nothing | |
| Myriad | nothing | |
| Opinion | the Open API key | `OPINION_API_KEY` |
| predict.fun | the API key (`pred_sk_...`, from developers.predict.fun); mainnet only | `PREDICT_FUN_API_KEY` |
| SX Bet | the API key (sx.bet -> Account -> Overview -> API Credentials) | `SX_BET_API_KEY` |
| Betfair | the application key and a login (every Betfair call needs one) | `BETFAIR_APP_KEY`, `BETFAIR_USERNAME`, `BETFAIR_PASSWORD` |
| Smarkets | nothing; a login undelays the quotes (the same one `SmarketsTrading` uses) | `SMARKETS_USERNAME`, `SMARKETS_PASSWORD` |
| Novig | the WebSocket: a subaccount's trading key (key id and private key file; the handshake is signed). Without one the public book is polled | `NOVIG_KEY_ID`, `NOVIG_PRIVATE_KEY_PATH`, `NOVIG_ENV` |
| Matchbook | nothing | |
| ProphetX | the Trading API key pair (prophetx.co -> API Integration); the Market Data API key has no stream | `PROPHETX_ACCESS_KEY`, `PROPHETX_SECRET_KEY` |
| Azuro | nothing | `AZURO_CHAIN` picks the pool when trading |
| Gemini | nothing | |

`synpath serve` builds a market stream for Opinion, predict.fun and SX Bet
from the API key alone (`load_market_data`), so a parent on any venue can
watch their books without their private keys on disk. Opinion's and
predict.fun's account streams, and their trading, still need the full set
([Trading](trading.md#credentials)). SX Bet's key also trades everything but
new orders, so its account stream runs on the key once the wallet address is
known: from `SX_BET_PRIVATE_KEY`, or `SX_BET_ACCOUNT_ADDRESS`. Without either,
serve logs why and polls the account instead.
Its `--dotenv` file is also where the read app at `/` finds these keys,
whatever folder `serve` runs from.

## Events

All events are frozen dataclasses with `venue` and `received_at` (local
milliseconds). Prices and sizes are `Decimal`.

| Event | Meaning |
|---|---|
| `BookEvent` | `kind="snapshot"` replaces the book; `kind="delta"` lists only the changed levels, each with its size *after* the change (zero means removed). Carries `best_bid`/`best_ask` after applying, and the venue's `sequence` where it has one. |
| `QuoteEvent` | the venue's own top-of-book summary and statistics |
| `TradeEvent` | a public print; `taker_side` where the venue says |
| `OrderEvent` | one of your orders, as a trading `Order`; `native` names the venue's event |
| `FillEvent` | one of your fills, as a trading `Fill`. On Polymarket the same fill id arrives again as settlement moves from `matched` to `confirmed` or `failed`. |
| `PositionEvent`, `BalanceEvent` | trading `Position` and `Balance` |
| `MarketStatusEvent` | `state` is `created`, `open`, `paused`, `closed`, `determined`, `settled` or `updated`; `native` keeps the venue's word |
| `VenueEvent` | venue-specific: Kalshi order groups, Polymarket tick size changes |
| `StreamStatusEvent` | the stream itself: `connected`, `disconnected`, `connect_failed`, `subscribed`, `gap`, `resynced`, `error`, and `failed` when the venue refuses the stream for good (no permission, a request it rejects); iteration ends after `failed` |

Every `watch_*` takes Synpath market ids. Books are also kept locally:
`stream.book(market_id, side="yes")` returns the `LocalBook` (with `ready`,
`best_bid`, `best_ask`, `levels(depth)`). Kalshi and Polymarket US books are
kept on the YES leg and `side="no"` is the mirrored view; on Polymarket each
side is its own book. `BookEvent` and `QuoteEvent` carry `side`; a
`TradeEvent` is in the YES price with `taker_side` on the YES leg.

The books and the book messages run on the Rust core (`synpath._core`):
snapshots and deltas from Kalshi, Polymarket, Polymarket US and Opinion are
parsed and applied there, and the events come out as the same Python objects.
`book.bids` and `book.asks` read and write like the `dict`s they replace. Set
`SYNPATH_PURE_PYTHON=1` to run everything on the pure-Python twins instead;
the test suite runs both and requires them to agree frame by frame.

## What the streams promise

**Subscriptions survive reconnects.** `watch_*` records what you asked for;
it is sent now if connected and again after every reconnect, with
jittered exponential backoff between attempts.

**Gaps are detected where the venue makes that possible, and repaired.**

- *Kalshi* numbers every book, trade, lifecycle and order-group message per
  subscription. A missing number is a `gap`; for books the stream requests
  fresh snapshots and ignores deltas until they arrive, then reports
  `resynced`. A gap in trades or lifecycle cannot be refetched and is
  reported for you to read over REST.
- *Polymarket* has no sequence numbers, but every book change carries the
  venue's best bid and ask. The stream checks its own top of book against
  them once all changes sharing a timestamp are applied (one trade can arrive
  as two messages stamped alike, whose top of book already reflects both).
  A disagreement is a `gap`: the stream resubscribes the token, which brings
  a fresh snapshot.
- *Polymarket US* sends the whole visible book every time, so books cannot
  drift.
- *Opinion* sends one changed level per message, with no snapshot and no
  sequence number. The stream reads each book over REST once the
  subscription is live and replays the changes that arrived during the read.
  A change is read as the level's whole size (the venue documents `size` as
  the level's shares), so replaying one the snapshot already holds is
  harmless; `tests/test_opinion_ws_live.py` checks this against the venue.
  Nothing in the feed reveals a missed message, so every book is read again
  on a timer (`resync_interval`, 60 s by default): a book that disagrees
  while no change was in flight is a `gap`, and is replaced.

- *Hyperliquid* sends the whole book (up to 20 levels a side) on every
  change, so every message is a snapshot and a missed one is repaired by the
  next. Books are kept on the YES coin; the NO view is the mirror, which
  `tests/test_hyperliquid_ws_live.py` checks against the venue's own NO
  coin. A trades subscription opens with a replay of recent prints; prints
  older than the subscription are dropped, so a stop never fires on one.

- *predict.fun* sends the whole book, in YES prices, on every change and on
  subscribing, so a missed message is repaired by the next. The server's
  heartbeat (every 15 seconds) must be echoed with its exact value or the
  connection closes; the stream echoes it. There is no public trades channel:
  the last settled price rides on the book.
- *Limitless* speaks Socket.IO; the stream handles its framing and answers
  its pings, so no Socket.IO client is needed. Each book frame is the whole
  YES book, sent again on every subscribe, and a subscription replaces the
  last, so the stream always sends its whole set. No public trades channel.
  Order-engine frames name a token rather than a market; the account stream
  places them from the markets given to `watch_orders`, then from one walk
  of the open catalog. The venue closes every connection after 24 hours; the
  stream reconnects with a freshly signed handshake.
- *SX Bet* speaks Centrifugo's JSON protocol; the stream sends its commands
  and answers its pings itself, so no Centrifugo client is needed. Every
  channel, public ones included, needs a short-lived token made from an SX Bet
  API key; the stream fetches one on every connect and renews it before it
  expires. Each book publication is the whole book with a version that only
  grows, and a quiet market publishes nothing, so each book is seeded from the
  REST snapshot when its subscription is confirmed. Trades and market changes
  are venue-wide channels, filtered here to the markets asked for.
- *Myriad* speaks Centrifugo's JSON protocol too, anonymously: the books,
  prices, trades and markets channels are public, and so is one wallet's tape
  on every market (`trades:56:*:<wallet>`), which carries the account's fills.
  The account's orders and positions channels each take a short-lived token
  that the REST API issues for the signed-in wallet's own channels; the user
  stream asks the trading adapter for one per channel on every subscribe. A
  book delta is the new size of each changed level, for both outcomes at once;
  both books are seeded from REST when the subscription is confirmed, and
  deltas that arrived first are replayed on the snapshot, which is safe since
  each is a level's absolute size. A reconnect reseeds them. Publications are
  coalesced into windows of about 100 ms. Order-book markets only: pools
  publish nothing here.
- *Betfair* is not a WebSocket: its Exchange Stream API is one TLS socket to
  `stream-api.betfair.com:443` carrying CRLF-delimited JSON. The stream
  authenticates with the application key and a session token straight away
  (Betfair drops a connection whose first message has not come within 15
  seconds), then sends one `marketSubscription` for every market wanted, at
  most 200 on a connection; a new subscription replaces the old one, so a
  change of markets resends the whole set and starts a fresh image. Best
  offers arrive as positional ladders (`batb`, `batl`: `[level, odds,
  stake]`, a stake of 0 deleting the level), read on YES as the REST books
  are: resting backs are bids, resting lays asks, at 1/odds, stake times odds
  contracts. Every message carries a clock; a reconnect resubscribes with
  `initialClk` and `clk`, and Betfair answers with a `RESUB_DELTA` of what
  changed while disconnected, after which every book is rebuilt from the
  cache and reported `resynced`. **A delayed application key is conflated to
  180,000 ms**: at most one update per market every three minutes, whatever
  `conflate_ms` asks. The stream logs a warning once when it sees that, and
  waits a whole conflation window before treating silence as a dead socket.
  No public trades, so no `watch_trades`; no account stream yet.
- *Azuro* speaks plain JSON on `wss://streams.onchainfeed.org/v1/streams/feed`:
  `SubscribeConditions` (and `UnsubscribeConditions`) with the condition ids
  and the chain's environment (`PolygonUSDT`, `BaseWETH`), and a
  `ConditionUpdated` message whenever a condition's odds or state change,
  carrying every outcome's `currentOdds`. Each update is read as the REST
  quotes are: buying YES at `1 / odds` of the YES outcome and, on a
  two-outcome market, selling at one minus the price of the other. An update
  that names only some outcomes keeps the others' last odds. A pool has no
  book, so `watch_order_book` keeps the quote as one level a side sized by the
  pool's limits, read over REST (`Azuro.fetch_order_book`) when the market is
  watched and after every reconnect; updates move the prices and keep those
  sizes. A condition stopping, resuming, resolving or being cancelled is a
  `MarketStatusEvent` (`paused`, `open`, `settled` with `yes`, `no` or
  `void`). No trades channel: bets are on the subgraph (`Azuro.fetch_trades`).
  The feed's socket answers some regions with a 503.
- *Smarkets* has no public stream: its socket feed (the protobuf stream on
  `api.smarkets.com:3701` that `smk_python_sdk` speaks) is for approved API
  users and market makers. `SmarketsMarketStream` polls the REST quotes
  instead, two hundred Smarkets markets a request, and emits the difference
  between one answer and the next: a snapshot first, then deltas of the
  levels that changed, nothing when nothing did. Polls share the adapter's
  rate budget (`QUOTES_LIMITER`: 20 quotes requests a minute anonymously), so
  the default 5 s interval is 12 a minute for up to 200 markets. Status is
  read every `status_every` polls (6 by default) while `watch_market_status`
  is on. A refused or failed poll is a dropped connection: the base loop
  backs off and reconnects, and the next poll rebuilds every book and
  reports it `resynced`. Quotes are delayed without a login, and every event
  says so in `info["delayed"]`. A change shows at the next poll, not as it
  happens, so its `watch_*` keys are `partial`. No `watch_trades`: the recent
  tape is short and unpaged; read it with `Smarkets.fetch_trades`. There is
  no account stream either (Smarkets' trading API is HTTP only), so the
  engine polls `SmarketsTrading` for orders and fills; under the engine the
  market stream polls on the trading adapter's session, so its quotes are
  undelayed.
- *Novig* carries every channel on one WebSocket, `wss://api.novig.com/v3/ws`
  (Paper: `wss://api.paper.novig.com/v3/ws`), opened with a subaccount's
  `trading` or `trading::read` key: the upgrade is signed like any request
  (NOVIG-V3 over `GET /v3/ws`). Requests are JSON verbs with nonces that
  grow within a connection. `subscribe` sets one channel per market --
  `book`, `bbo`, `trades` or `lifecycle`, the first three carrying
  `lifecycle` too -- and is answered with a snapshot at a `seq`; each delta
  carries the next `seq` of its market and channel, and a skipped one is a
  gap, recovered with a `snapshot` request that leaves the subscriptions as
  they are. Books are kept per resting order (`add`, `update`, `remove`) and
  read on YES like the REST book: the YES outcome's bids are bids, the NO
  outcome's asks at one minus their price, sizes in dollar contracts. A
  market watched for its book and its trades is subscribed on `book`, and
  its trades are read off the book's fills, without the tape's ids; a market
  watched for trades alone is on `trades`, for its ticker alone on `bbo`.
  `GOLIVE` voids every resting order on the event, so the book empties and a
  `MarketStatusEvent` says so. Subscriptions go out sixteen markets a
  request, paced under the venue's `stream` throttle. Without a key,
  `NovigMarketStream` polls the public book (every 5 s by default), the tape
  for watched trades and the market for its status, and its `watch_*` keys
  are `partial`; the edge caches those answers for up to fifteen seconds. A
  poll the edge throttles keeps the last answer and is retried at the next.
  `NovigUserStream` subscribes the subaccount's `orders` and `positions`:
  the snapshot lists the resting orders and the nonzero positions, then
  `open`, `fill`, `cancel` and `reject` events and position changes follow,
  each channel with its own `seq`. A heartbeat every 15 s names the last
  `seq` of each; one ahead of the stream's is a gap, reported with
  `reconcile_required` and recovered with a snapshot. Fills matched on entry
  arrive before the order's `open`. A `fill` carries no fee; read it with
  `NovigTrading.fetch_my_trades`.
- *Matchbook* has no push API: no WebSocket, SSE or FIX. `MatchbookMarketStream`
  polls `GET /edge/rest/events` with prices, twenty Matchbook events a
  request, and emits the difference between one answer and the next: a
  snapshot first, then deltas of the levels that changed, nothing when
  nothing did. One read carries every market, runner and price of an event,
  so books, tickers and runner status (suspended, closed, withdrawn) come from
  the same polls. Polls share the adapter's `LIMITER` (Matchbook's events
  group allows 700 requests a minute). Every GET is chargeable on Matchbook's
  API plans (£100 per million a month): the default 5 s interval is 12
  requests a minute for up to twenty events, about 520,000 a month if left
  running, so raise `interval` to spend less. The stream polls without a
  session, so the polls are not the account's. A refused or failed poll is a
  dropped connection: the base loop backs off and reconnects, and the next
  poll rebuilds every book and reports it `resynced`. A change shows at the
  next poll, so its `watch_*` keys are `partial`. Matchbook publishes no
  trades, so there is no `watch_trades`, and no account stream: the engine
  polls the account's offers.
- *ProphetX* streams over Talaria, its own server for the Pusher wire
  protocol (protocol 7); the stream speaks it directly, so no Pusher client is
  needed. On every connect it reads `GET /websocket/connection-config` for the
  host (`ws_host`, else the Pusher cluster), opens `/app/<key>`, takes the
  `socket_id` from `pusher:connection_established`, and registers the
  complete set of subscriptions at `POST /v4/mm/websocket` with
  `X-CFTC-Terminology: true`. The answer authorizes each channel with its own
  signature; the stream signs in with its `authenticated` block and joins
  the channels it wants once `pusher:signin_success` arrives. Every message's
  data is JSON in a string whose `payload` is base64 JSON. Books come from the
  event channel (`{"type": "event"}`) as `v2.market_selections`: each message
  is one line's two sides, ten levels each, so it replaces the book, and a
  quiet line publishes nothing, so each book is seeded from REST when its
  channel is joined. Messages carry a sequence number and an older one is
  dropped. Trades come from the tournament channel as `trade_execution`
  (bound as `v2.tournament_<id>`), anonymous fills with the strike, its price
  and the stake; `watch_trades` reads each line's tournament first. The
  account's `orders` arrive on its private channel, one message per fill,
  with `health_check` every five seconds. The stream sends `pusher:ping`
  every 30 s and answers the server's. Every channel is private, so both
  streams need the Trading API key pair; the engine runs them on the trading
  adapter's session, since ProphetX counts sessions per key.
- *Gemini* takes `{"id", "method": "SUBSCRIBE", "params": [stream names]}`
  and answers each with its `id` and a status. The market stream connects
  with `snapshot=-1` and subscribes `{symbol}@depth@100ms`, so the first frame
  for a book is the whole book and the rest are the levels that changed (a
  size of zero removes one). Frames chain by update id: each frame's first id
  `U` is the last id `u` of the frame before, ids being the exchange's own
  across all books. A frame whose `U` is past the last applied `u` is a gap:
  the book is dropped, the stream unsubscribes and subscribes again, and the
  fresh snapshot is reported `resynced`. Books are in YES space; `book(id,
  "no")` mirrors one. `@bookTicker` is the top of book (and the last trade
  price once there is one), `@trade` the tape (`m` says the buyer was the
  maker, so the taker sold), `contractStatus` every contract's status changes.
  Gemini sends symbols lower case; events carry them upper case, as REST
  does. The account stream signs its handshake (`X-GEMINI-APIKEY`, `-NONCE`,
  `-PAYLOAD`, `-SIGNATURE`) with an account-scoped key that uses a time-based
  nonce; master keys are refused. An order frame carries only what changed,
  so the stream keeps each order's last known fields; a `FILLED` or
  `PARTIALLY_FILLED` frame is also a `FillEvent` (`Z` the execution's size,
  `L` its price, `n` its fee). The first `positions@account` frame is the open
  positions, then changes; a settlement closes a position with its payout.

**A book is right or marked not ready.** After a gap or a disconnect the
book's `ready` is false until a snapshot arrives; deltas are not applied to
a stale base.

**Private channels ask for reconciliation.** No venue replays orders, fills
or positions missed while disconnected. A reconnect on a stream with private
subscriptions emits `StreamStatusEvent(state="connected",
reconcile_required=True)`: read the venue's REST state before trusting
anything derived from the stream.

**One bad message does not end the stream.** An unreadable payload becomes
`StreamStatusEvent(state="error")` and the connection stays up.

**Dead connections are noticed, even after the machine sleeps.** Polymarket
and Polymarket US heartbeats arrive as data, so a minute of silence
reconnects. Kalshi keeps the connection alive with protocol pings that are
invisible to the reader, and a quiet market can be silent for minutes on a
healthy connection, so there the WebSocket library's ping/pong detects a
dead peer instead. Silence is measured on the wall clock as well as on the
event loop's timers: a suspended laptop freezes those timers while the
connection dies underneath them, and the operating system can take another
quarter of an hour to notice the socket is gone. A stream whose machine
slept reconnects as soon as it wakes.

## Polymarket US exchange API (gRPC)

```bash
pip install synpath[grpc]
```

Onboarded firms stream over gRPC at
`grpc-api.{preprod,prod}.polymarketexchange.com`, with the same credentials
as `PolymarketUSExchangeTrading`. Each stream takes the credentials or a
trading adapter; passing the adapter shares its access token and its cache of
instrument scales.

**Protos.** Polymarket publishes its `.proto` files as a download with no
license, so synpath does not ship them or code generated from them. Download
the bundle from the venue's documentation and point streams at it, as the
unzipped directory or the zip itself:

```bash
export SYNPATH_POLYMARKET_US_PROTOS=~/Downloads/polymarket-protos.zip
```

or `protos=` on any stream. The bundle is compiled once, in process, into a
descriptor set cached under `~/.cache/synpath/protos`; nothing is generated
into the package or put on `sys.path`.

```python
from synpath import PolymarketUSExchangeTrading, load_credentials, require
from synpath import PolymarketUSExchangeDropCopyStream, FillEvent, VenueEvent

async with PolymarketUSExchangeTrading(require("polymarket_us_exchange", load_credentials())) as pmx:
    async with PolymarketUSExchangeDropCopyStream(pmx, resume_token=saved_token) as stream:
        async for event in stream:
            if isinstance(event, FillEvent):
                book_fill(event.fill)
            elif isinstance(event, VenueEvent) and event.name == "checkpoint":
                save_token(event.payload["resume_token"])
```

| Stream | Carries | After a reconnect |
|---|---|---|
| `PolymarketUSExchangeOrderStream` | this participant's open orders as a snapshot, then every execution; fills; refused cancels as `VenueEvent("cancel_rejected")` | fresh snapshot, `reconcile_required` (fills are not replayed) |
| `PolymarketUSExchangeDropCopyStream` | every execution in the firm: orders and fills, any account | resumes from its token |
| `PolymarketUSExchangeTradeCaptureStream` | the firm's trades as fills whose `settlement` follows clearing | resumes from its token |
| `PolymarketUSExchangePositionChangeStream` | position changes across the firm | resumes from its token |
| `PolymarketUSExchangeInstrumentStream` | instrument state as `MarketStatusEvent`; needs no participant id | resumes from its token |
| `PolymarketUSExchangePositionStream` | positions as a snapshot, then changes | fresh snapshot; a position missing from it is reported flat |
| `PolymarketUSExchangeMarketDataStream` | books to a depth and statistics, up to 1,000 symbols | fresh books |
| `PolymarketUSExchangeBalanceLedgerStream` | balance ledger entries as `VenueEvent("balance_ledger")` with `before`, `after`, `change` | replays from the last entry's time |

What they promise, beyond the WebSocket streams':

- **Duplicates are dropped.** Delivery is at least once. Executions are
  deduplicated by id, ledger entries by id, and trade reports by id and state.
- **Drop copy does not need reconciling.** The last resume token goes out on
  every reconnect. After each batch that moves it, a
  `VenueEvent(name="checkpoint")` carries the token; save it once the events
  before it are handled and pass it back as `resume_token=` after a restart.
  If the venue refuses a token, the stream resumes from the time of the last
  event instead.
- **A bust is a failed fill.** A trade report becomes a `FillEvent` with the
  fill id it had on the order stream. Its `settlement` is `matched` while the
  trade is in clearing, `confirmed` once cleared, and `failed` when the
  exchange busts it or the clearing house rejects it. Treat `failed` as
  reversing the fill.
- **Fees are dollars.** A fill's `fee` is its commission, decoded from
  notional units. It is negative for a rebate.
- **Refusals are sorted.** An expired token is refreshed and the call retried
  at once. Too many streams (the firm limit is 20) waits the longest backoff.
  Missing permission or a rejected request ends the stream with `failed`.

Books and positions carry no scales, so their scales come from reference
data, which is limited to six requests a minute across the firm. Each symbol
is read once and cached on the trading adapter. Order and execution messages
carry their own scales.

Two things are inferred rather than observed. A market-data update is read
as the whole book to the subscribed depth, because the message has no way to
mark a level removed. Heartbeat intervals are not published, so the order,
position and market-data streams reconnect after two minutes of silence
(`idle_timeout=` changes it). The drop-copy and ledger streams send no
documented heartbeat and rely on gRPC keepalive.

## How it was verified

- **Kalshi:** a transcript recorded on the demo environment while orders
  rested, were cancelled and filled. The local book built from 136 real
  deltas equals the snapshot the venue sent next, level for level; order,
  fill and position events match the REST reads. A demo test places and
  cancels an order and checks the stream against REST.
- **Polymarket:** a transcript from the live market channel replays with no
  false gaps across 242 deltas, including a trade split over two
  same-stamped messages; removing one real change is caught as a gap.
- **Connection handling:** a real local WebSocket server that drops the
  client; scripted connections for backoff, silence, bad messages, a
  connection that died while the machine slept, and private reconnects.
- **An hour of live streaming** on Kalshi demo (all channels, 8 markets) and
  Polymarket (24 tokens): 495,000 messages, no gaps, no unreadable messages,
  88 local books equal to the venue's own snapshots, 6 order reads agreeing
  with REST. Kalshi demo closed the connection normally three times; each
  reconnect took about a second and resynced every book. The run ended with
  the laptop suspending, which is what the wall-clock watchdog above now
  covers.
- **Polymarket US:** built to the venue's documentation and SDK, with the
  envelope spellings both of them accept.
- **Opinion:** built to the venue's documentation and SDK samples, with the
  REST books recorded for the read adapter. `tests/test_opinion_ws_live.py`
  checks streamed books against the venue's own over a live minute; it needs
  `OPINION_API_KEY`.
- **Hyperliquid:** recorded messages from every channel, and fills and
  orders of active accounts (buys and sells on both coins, splits, merges,
  settlements, every order state). `tests/test_hyperliquid_ws_live.py` runs
  both streams against the venue without a key.
- **Polymarket US exchange API (gRPC):** a real local gRPC server speaking
  the venue's own compiled messages. It covers a refused token, a snapshot,
  an execution report and a drop, and a drop-copy resume token surviving a
  reconnect byte for byte. Scripted calls cover resume fallback, busts,
  deduplication, flat positions, books and refusals. The fee decoding
  reproduces the venue's worked example. Both production and preprod
  endpoints answer an invalid token with `UNAUTHENTICATED` as the streams
  expect.

- **Betfair:** built to Betfair's Exchange Stream API documentation, with
  hand-built messages in its documented shapes (no account was available):
  a TLS-shaped socket carrying CRLF framing, authentication, an image then
  deltas and deletes, a resubscribe from the clock, heartbeats, status
  changes and the 200-market cap. Not yet run against Betfair.

- **Smarkets:** polls answered from REST quotes recorded anonymously from
  api.smarkets.com: a snapshot, unchanged polls, deltas, a whole market,
  tickers, halted and settled contract states, a refused poll and a resync
  after a drop, through the base loop.

Run the live checks with:

```bash
pytest -m live tests/test_ws_live.py     # Polymarket market channel
pytest -m demo tests/test_ws_live.py     # Kalshi demo: order events against REST
SYNPATH_POLYMARKET_US_PROTOS=<bundle> pytest tests/test_ws_grpc.py   # adds the real gRPC server tests
```
