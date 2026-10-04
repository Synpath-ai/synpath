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

Each stream's `has` answers the `watch_*` capability keys (`watch_order_book`,
`watch_ticker`, `watch_trades`, `watch_market_status`, `watch_orders`,
`watch_my_trades`, `watch_positions`, `watch_balance`) with `True`, `False` or
`"partial"`.

The Polymarket US exchange API's streams are gRPC; they are described
[below](#polymarket-us-exchange-api-grpc).

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

Run the live checks with:

```bash
pytest -m live tests/test_ws_live.py     # Polymarket market channel
pytest -m demo tests/test_ws_live.py     # Kalshi demo: order events against REST
SYNPATH_POLYMARKET_US_PROTOS=<bundle> pytest tests/test_ws_grpc.py   # adds the real gRPC server tests
```
