# The execution engine

Back to the [README](../README.md).

`synpath.engine` is what sits between a strategy and the venue adapters: a
journal that is written before anything is sent, pre-trade risk rules that
name themselves when they refuse, a ledger that nets fills into positions
and profit, reconciliation against the venue, a kill switch, and a paper
venue that fills orders the way a real book would.

```python
from decimal import Decimal
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath import KalshiTrading, OrderRequest, Side

async with Engine({"kalshi": KalshiTrading(credentials)},
                  EngineConfig(journal_path="trading.db"),
                  risk=RiskConfig(max_order_contracts=Decimal("100"),
                                  daily_loss_limit=Decimal("500"))) as engine:
    order = await engine.submit(OrderRequest(
        market_id="kalshi:KXBTCD-25SEP18-B95000", side=Side.BUY,
        quantity=Decimal("10"), price=Decimal("0.42"), book="alpha",
    ))
```

The experimental storage branch adds [per-type records, migrations and storage configuration](../synpath/engine/README.md).
Normal shutdown preserves orders by default; an operator halt is explicit.

## What it promises

**Nothing is sent that was not written down first.** `submit` runs risk,
writes the intent with its client order id, calls the venue, then records the
answer. A process killed between the write and the answer restarts holding an
intent marked `sending`, asks the venue whether an order with that client
order id exists, and adopts it or sweeps it. The write is committed with
SQLite's `synchronous=FULL`, so this survives a power cut, not only a crash.

**An order is never declared lost early.** Venues do not list an order the
instant they accept it: Kalshi's demo takes a few hundred milliseconds. An
intent the venue cannot account for stays in doubt until `lost_after_s`
(default a minute) has passed, and only then is swept. The caller of a send
with no verdict gets `SubmissionInDoubt` with the `client_order_id`; a retry
under it reconciles first and gets the order, or `SubmissionPending` while the
venue cannot say yet, never a second order.

**A cancel never relabels a fill.** A venue's "not found" to a cancel is not
a cancellation: the engine reads the order back and records what the venue
says, `closed` with its fills if it filled first. A venue with no record of
it at all raises `CancelUnconfirmed` and changes nothing.

**One engine per journal.** Starting takes a lease. A second engine on the
same file refuses to trade and says who holds it. The lease is renewed while
the engine runs; an engine that loses it halts itself.

**Every refusal names its rule.** `RiskRejected.rule` is one of
`kill_switch`, `book_paused`, `restricted`, `price_bounds`, `price_collar`,
`max_order_contracts`, `max_order_notional`, `duplicate`, `order_rate`,
`closing_soon`, `max_open_orders`, `self_trade`, `max_position`,
`exchange_limit`, `max_event`, `daily_loss` or `max_venue_notional`. The
configuration is versioned in the journal, and a rejection records the
version that refused it. `duplicate` refuses a repeat of the same market,
side, price and size inside `duplicate_window_ms`; it skips the children the
engine's own order types place, and any order whose `client_order_id` the
caller set. `price_collar` refuses a limit order priced further than the
collar from the book midpoint; market orders (their price is a protection
bound) and the children of engine-held orders are not collared.

**Halting uses the venue's own mechanism, then checks.** `halt()` calls each
adapter's cancel-all, reads the book back, and cancels anything still resting
one order at a time. New orders are refused from the moment it engages.
Policies are `cancel`, `hold` (leave them resting, refuse new ones) and
`rearm` (refuse, then lift itself after a timer). With `cancel` it first
cancels every open RFQ the engine asked and every quote it made.

## The pieces

| Module | What it owns |
|---|---|
| `journal.py` | the append-only event log, the materialized orders, fills and intents, the single-writer lease, versioned configuration |
| `events.py` | the in-process bus; a slow reader drops its oldest events rather than stalling order entry |
| `engine.py` | submit, cancel, edit, recovery, the sweep, halting, polling |
| `risk.py` | the rules, the kill switch, the strategy pause |
| `ledger.py` | positions netted on the YES leg, average cost, realized and unrealized profit, rolled up by market, book and account |
| `reconcile.py` | orphans, ghosts, drift, position and balance differences |
| `rfq.py` | RFQs and combos, both sides: accept and confirm through risk and the journal, the quote follower, recovery, halt |
| `paper.py` | a venue that fills against a real book with queue position and the venue's fee model; `PaperRfqVenue` adds RFQs with configurable makers |
| `alerts.py` | the few events worth waking someone for |
| `eod.py` | settlements, the daily report, rolling the risk day |
| `__main__.py` | daemon mode, `status`, `halt`, `resume`, `eod` |

## Order types the venues do not hold

```python
from synpath import OrderRequest, OrderType, Side

await engine.submit(OrderRequest(
    market_id="kalshi:KXX", side=Side.SELL, quantity=Decimal("20"),
    type=OrderType.TRAILING_STOP, stop_price=Decimal("0.40"),
    trigger_source="touch", params={"trail": "0.03"},
))
```

A parent is journaled with `held_by="engine"` and a status of `waiting`
until its condition fires, then `triggered` while its children work. The
children are ordinary venue orders, so the risk rules, the ledger and
reconciliation treat them like anything else, and reconciliation knows to
skip the parent because the venue was never told about it. A parent's
filled quantity is the sum of its children's, as the venues' order records
last reported them; see the note under buckets on why fill events are the
ledger's alone.

| Type | `params` | What it does |
|---|---|---|
| `stop_market` | none | fires once at `stop_price`; the child is a `market` order, `ioc`, priced by the engine |
| `stop_limit` | none | fires once at `stop_price`; the child is a `limit` at `price` that inherits the order's time in force and expiry |
| `trailing_stop` | `trail` or `trail_percent` | the stop follows the market one way and never back |
| `iceberg` | `display`, `reload_delay_s`, `jitter_s`, `follow` | shows one slice; reloads after the delay, at the back of the queue |
| `oco` | `legs` (two specifications) | one cancels the other, and a partial fill resizes the other |
| `bracket` | `entry`, `take_profit`, `stop_loss` | protects what the entry fills, as it fills |
| `twap` | `window_s`, `slices`, `style`, `limit`, `finish` | slices across a window by the clock, patient or aggressive |
| `peg` | `reference`, `offset`, `min_stay_s`, `level_cap`, `min_price`, `max_price` | follows the touch, with a minimum stay and a cap on how far it chases |
| `smart_taker` | `clip`, `interval_s`, `limit`, `max_slippage`, `expires_s` | takes in clips over time, inside a price bound |
| `market` with `params={"walk": True}` | `max_slippage`, `limit` | walks the book now, level by level, inside the bound |

### Stops

Both stop types are held by the engine: nothing reaches the venue until the
stop triggers. A buy stop triggers when the watched price rises to
`stop_price`, a sell stop when it falls to it. Triggering is one-way; the
child goes out for the remaining quantity and names the parent in
`parent_id`.

| Field | Stop market | Stop limit |
|---|---|---|
| `market_id`, `side` or `outcome` + `action`, `quantity`, `client_order_id` | as on any order (`buy` is YES, `sell` is NO; or buy/sell YES/NO) | the same |
| `type` | `stop_market` | `stop_limit` |
| `stop_price` | the trigger level | the trigger level |
| `price` | not used | the child's limit: at or below `stop_price` for a sell, at or above it for a buy |
| `trigger_source` | `touch` (default), `last` or `mid` | the same |
| `time_in_force` | how long the whole order lives: `gtc` (default), `gtd`, `day`. `ioc` and `fok` are refused. It bounds the armed period; the child is `ioc` | the same, and the limit child inherits it: `gtc` stays `gtc`, `gtd` and `day` go as `gtd` at the order's `expires_at` |
| `expires_at` | with `gtd` only | with `gtd` only |

**A stop watches the price that would fill it.** `trigger_source="touch"`,
the default, watches the best ask for a buy stop and the best bid for a sell
stop. Not the last trade, which on a prediction market can be hours stale,
and not the mid, which between a 0.30 bid and a 0.70 ask is a number nobody
can trade at. `last` and `mid` are there for callers who disagree.

**A stop reads the book it is handed.** A new stop is checked against the
book the engine already holds, and again when its market is subscribed: a
venue sends no fresh snapshot for a market its connection already carries (a
screen's `/ws/market`, say), so a quiet market whose ask already meets the
stop triggers it at once rather than at the next change. A `last` stop waits
for a print made after it.

**One time in force for the whole order**, as IBKR and FIX use it: a stop's
`time_in_force` is how long the order lives, armed and after it triggers.
A stop limit's child carries the same expiry, so a `day` stop limit whose
child still rests at the session end has the child canceled and ends
`expired`.

**A stop market's child is priced by the engine.** No venue here takes a
market order without a worst price, so at the trigger the engine sends the
`market` child, `ioc`, at the legal venue bound for the side, on the market's supplied tick
(or the conservative venue grid when no market precision is registered). There is no parameter
for it. With no book to price against at that moment, the stop ends
`rejected` rather than send at any price.

**How a stop ends.** The parent reports `waiting`, then `triggered`, then one
of: `closed` (filled; for a stop market, also when the IOC child's remainder
did not fill, with the unfilled quantity on the order and in its detail),
`canceled` (cancelled while waiting, or for a stop limit after the trigger
too, which pulls the resting child), `expired` (a `gtd` or `day` stop whose
time ran out before it triggered, or a `gtd` or `day` stop limit whose child
was still resting), or `rejected` (no book for a stop market's child, or the venue
refused the child).

**Day is not one of these.** All three venues run nearly around the clock, so
Day has no venue meaning, and an engine-held timer would die with the
process. A `day` order is rewritten to `gtd` at the end of the configured
session (`session_timezone`, `session_end`) before it reaches the adapter, so
the venue holds the expiry; the journal keeps `day` alongside it.

**The order's own fields still apply.** The outcome and action pass to every child: a Sell YES stop's child sells YES held.
`expires_at` expires the whole order, children included, and the parent
ends `expired`. `post_only` is
accepted only by types that rest on the book (`iceberg`, `peg`, a `twap` with
`style="limit"`, and a `stop_limit`); an `oco` or `bracket` takes it on its legs, and a type that
takes liquidity refuses it rather than ignoring it.

**Everything survives a restart.** Each parent's whole state is written on
every change: a trailing stop resumes from the level the market reached, a
TWAP from the clock, an iceberg from the slice it was on.

## Buckets: one order, legs on several venues

A bucket is your own definition of one tradable thing made of several
venues' listings: a list of member markets and, for each, whether that
member's YES is the bucket's YES or the opposite (`flip`). It sits where a
single market does. An order names `bucket:<id>` the way it names
`kalshi:KXX`, and the engine holds it as a `routed_limit` parent whose
children are one leg per member venue.

A member can be switched off without removing it: `BucketMember(...,
enabled=False)`. It stays in the definition (and in `bucket_position`, so
anything already held there still nets in), but the router leaves it out:
no leg goes to it, its book is not merged, and the merged book in `synpath
serve` does not stream it. An order placed while a member is off carries the
bucket with only its enabled members. A definition needs at least two members
and at least one of them enabled; members default to `enabled=True`, so
definitions stored before the field existed route as before.

```python
from synpath import Bucket, BucketMember, OrderRequest, Side

bucket = await engine.save_bucket(Bucket(book="alpha", name="Falcons -7.5 2H", members=[
    BucketMember(market_id="kalshi:KXNFL2HSPREAD-26SEP24ATLGB-ATL8"),
    BucketMember(market_id="polymarket:4851855"),
]))
parent = await engine.submit(OrderRequest(
    market_id=bucket.market_id, side=Side.BUY, quantity=Decimal("500"), type="market", price=Decimal("0.42"),
    book="alpha", params={"min_stay_s": 5, "max_rounds": 20},
))
engine.orders.get(parent.id).report()          # filled, average, per venue, why it stopped
await engine.bucket_position(bucket.id)        # the ledger's positions, netted in bucket terms
```

An order on a bucket is a market order, and its `price` is required: the
worst price accepted, in bucket terms. A sweep across venues with no price
bound would walk each book to its end; a limit order on a bucket is refused. The router (`synpath.engine.router`)
lays the members' books side by side in bucket terms, a flipped member's
YES book read as its NO book, prices every level net of the taker fee, and
walks from the best net price to that worst price. A leg under its venue's minimum
-- in contracts, or in value where the venue counts value (Opinion: 5 USDT
of the token bought) -- is dropped and its size moves to the next venue. A
venue that charges a minimum fee per order (Opinion: 0.25 USDT) has the
floor added to each leg's cost: a leg it pushes past the worst price is
dropped, and one whose size fills as fully for less elsewhere is moved. The
legs never sum to more than was asked.

The merge and the walk run on the Rust core (`synpath._core`), which makes a
re-plan over three venues of fifty levels a side about four times faster.
The leg sizing, rounding and floors stay in Python, and a plan whose numbers
Rust cannot carry exactly as Python's `Decimal` would is made in Python from
the start, so the plan is the same either way.

The parent re-plans whenever a leg fills, a book moves or a leg is pulled.
A leg still at a price the plan wants is kept, so it holds its place in the
queue; only a leg whose price is wrong is cancelled and re-placed, and not
before `min_stay_s`. It stops, and says why, when the worst price you set
is reached with nothing resting (`worst_price`), when what is left is under every venue's minimum, or
at `max_rounds` or `max_age_s`. A stop with a partial fill finishes
`canceled`, because `done` means filled.

| `params` | Meaning |
|---|---|
| `min_stay_s` | seconds a leg rests before it may be moved |
| `max_rounds` | re-allocations that changed a leg, after which it stops |
| `max_age_s` | seconds from acceptance, after which it stops |
| `precision` | per-market `tick`, `min_quantity`, `quantity_step`, `whole_contracts`, overriding the venue's published rules |

**One channel for what has filled.** A parent learns its children's fills
from the venue's order record only: the placement's answer, then every
order-status update, then a direct read after a restart. Each is a whole
snapshot and the latest overwrites; nothing is added across channels. The
venue's fill events go to the ledger, for prices, fees and P&L, and never
reach a parent. This holds for every engine-held type, not only buckets;
it is why a leg that fills on the way in is never bought twice. After a
restart the bucket parent reads every leg back from its venue, takes in
any child the journal links to it that the snapshot did not carry, trims
newest-first until what rests fits what is left, and only then plans
again. Nothing is placed before that has run.

Buckets are stored in the journal (`Journal.save_bucket`, `bucket`,
`buckets`, `archive_bucket`); an archived bucket's orders still resolve.
Through your running self-hosted server (`synpath serve`), define one with `POST /trading/buckets`
or `Client(server=...).create_bucket(book=, name=, members=)`; the server
assigns the id and refuses a caller without `trade` on every member venue.
The client also has `fetch_buckets`, `fetch_bucket`, `archive_bucket`,
`fetch_bucket_orders`, `fetch_bucket_order` (the `report()` above, typed as
`BucketOrderReport`, rebuilt from the journal for an order that finished
before a restart) and `fetch_bucket_position`. Over HTTP the server also
renames, edits and restores a bucket (`PATCH /trading/buckets/{id}`), keeps a
client-made UUID id when one is given, streams the merged book in bucket
terms (`bucket:<id>` on `/ws/market`) and previews `router.plan` for an order
without placing it (`POST /trading/buckets/{id}/plan`); see
[the server](server.md#a-buckets-merged-book).
Ids are UUIDs made locally, so a definition made offline stays unique when
it later moves to a hosted account. Nothing here judges whether the members
are really the same question.

## RFQs and combos

`engine.rfq` (`rfq.py`) runs both sides of an RFQ on the venues that declare
`rfq_taker` / `rfq_quoter`; [`rfq-combo.md`](rfq-combo.md) has the design and
the venue facts.

```python
rfq = await engine.create_rfq("polymarket_us:caoc-...", quantity=Decimal("10"), book="parlays")
# quotes arrive as quote.received events
quote = await engine.accept_quote(AcceptQuote(quote_id=q.id, rfq_id=rfq.id, outcome="yes", action="buy",
                                              worst_price=Decimal("0.30")))     # returns at once: accepted
# then quote.confirmed, quote.executed, order.accepted, order.closed, fill.booked
```

- **An accept is an order.** The quote is re-read; a price worse than
  `worst_price` is refused (rule `worst_price`); an `amount` becomes contracts
  rounded down to the venue's step; the `rfq_taker` request runs every risk
  rule. Then the intent `rfq-accept:{venue}:{quote_id}` is written and the
  venue called. `wait=True` follows the quote to its end.
- **Results are events.** The intent stays `sending` through the maker's last
  look. The `rfq` loop (or a venue's quote stream, or the sweep) reads the
  quote; when it executes, the order is recorded through the normal order
  path and its fills booked, and the intent becomes `sent`; when it ends
  without a trade, `failed` with `quote.failed`.
- **Recovery** reads the quote back: an accept that never reached the venue
  is `rejected` after `lost_after_s` and may be retried; one that did is
  followed and adopted; nothing is sent twice.
- **The side the venue reports is booked.** A mismatch with the intent
  publishes `rfq.side_mismatch` and a critical alert.
- **Quoter.** `send_quote`, `replace_quote`, `cancel_quote` are journaled and
  checked against the halt, a paused book, the restricted list and the size
  limits; `confirm_quote` runs every rule on our side of the trade and is
  followed like an accept (`rfq_maker` order). The engine holds a quote's
  `expires_at` and cancels it when it passes. `rfq.incoming` announces others'
  RFQs.
- **Quoting in time.** Register a callback with `engine.rfq.on_incoming_rfq`;
  it runs the moment an incoming RFQ is recorded and `rfq.incoming` is out.
  Polymarket global's quoter session and SX Bet's parlay channel push their
  requests (`subscribe_incoming_rfqs`), so there the callback runs as the
  stream delivers, with no poll in between (well under a millisecond in
  process; the Polymarket window is 400 ms). Kalshi and Polymarket US have
  no RFQ stream here and are polled every `rfq_incoming_interval_s` (2 s).
  The streams start with the `rfq` loop (`rfq_stream_incoming`, on by
  default); `send_quote` finds the RFQ in `engine.rfq.incoming` and makes
  no venue read before its own call.

  ```python
  async def quoter(rfq):                     # runs inside the venue's window
      if rfq.combo is None or len(rfq.combo.legs) > 4:
          return
      await engine.send_quote(MakerQuote(rfq_id=rfq.id, bid=Decimal("0.18"), ask=Decimal("0.22"),
                                         book="mm"), venue=rfq.venue)

  stop = engine.rfq.on_incoming_rfq(quoter)  # stop() unsubscribes
  ```
- **Restarts on a venue that lists nothing.** Polymarket global's gateway
  has no list of a requester's RFQs, so at start (`recover()`) the engine
  hands each adapter its open RFQs from the working set and the quotes whose
  accept or confirm is in doubt (`TradingExchange.restore_rfqs`, a no-op
  elsewhere). An accept in flight is then followed to executed or failed,
  and an open RFQ can still be cancelled.
- **A combo is an instrument.** Its own market id carries the order, fills,
  position, P&L and reconciliation; its legs are kept as `combo.saved` events
  and shown in `Position.info["combo"]`.

## The ledger

Positions net on the YES leg, which is the leg every fill arrives on: a
`sell` fill is the NO side at the YES price, so a book holding both holds
neither. What was actually bought is kept per side as well, because on
Polymarket those are real tokens.

Realized profit is taken on the way out against average cost, with fees
realized when charged. Unrealized profit needs a mark: the
price the position could be closed at on the engine's live book, the best bid
for a long YES and the best ask for a short (a long NO). The engine subscribes
the book of every market it holds. With no ready book, or nobody on that side,
the mark is the last trade; with no trade either, the position has no mark and
no unrealized number.

Three levels roll up:

```python
engine.pnl("market")      # per contract
engine.pnl("book")        # per strategy
engine.pnl("account")     # per venue account
engine.ledger.merged(await adapter.fetch_positions())   # engine against venue
```

## Reconciliation

On a timer and at startup, the engine compares itself with each venue:

| Finding | Meaning | Default |
|---|---|---|
| `orphan` | resting at the venue, unknown to the journal | reported; `adopt` or `cancel` on request |
| `ghost` | open in the journal, gone at the venue | closed from the venue's answer |
| `drift` | both know it, they disagree | the venue's version wins |
| `position` | the ledger and the venue disagree | reported |
| `fill` | a fill the journal did not have | booked |
| `balance` | cash moved with no fill to explain it | reported |

A balance difference is how a deposit, a withdrawal, a settlement or a fee
charged outside a fill shows up.

## Paper trading

`PaperVenue` implements the same interface as a real adapter, so the engine,
the journal and the risk rules run unchanged. Feed it books and trades (from
`synpath.ws`, or a recorded tape) and it fills orders pessimistically:

- a marketable order walks the book and pays the average of what it ate;
- a resting order joins the back of the queue at its price and fills only
  after the size ahead of it has traded;
- fees come from the venue's own schedule, passed in as a callable.

## Running it

Everything the engine needs, in one process:

```bash
synpath serve                        # start your self-hosted server with every venue whose keys are in the environment or .env
synpath serve --config engine.toml   # the venues, risk rules and journal the file names
```

`synpath serve`, your self-hosted server, starts the engine with every loop it publishes
(`Engine.background()`: lease, in-doubt sweep, poll, the managed-order clock),
subscribes each venue's market and account streams (`synpath.engine.feeds`),
runs reconciliation and the end-of-day close, and serves the HTTP API: market
data at `/`, live books, trades and quotes at `/ws/market` (on the same venue
connections the stops read), trading at `/trading`. Every route but `/`,
`/health` and the API docs needs the access token. The first start on a new control database
makes an owner access token and leaves it in `~/.synpath/servers.json` (readable by you
only), where `synpath.Client(server=...)` and the TypeScript client on the
same machine find it; a server bound to a network address prints the token
once instead. Stopping it (`Ctrl-C`, `SIGTERM`) applies the halt policy,
closes the streams and releases the journal's lease.

**The streams are what make engine-held orders work.** A stop reads the book
the venue's stream keeps up to date; a parent learns what its children filled
from the venue's order updates on the account stream. Without streams
(`--no-streams`, or a venue with none) the engine still trades, learning of
fills by polling every few seconds, but a stop has no price to watch. A book
the stream has not snapshotted yet, or lost after a gap, reads as no book, so
nothing fires on stale levels.

The engine alone, without HTTP, and the operator commands:

```bash
synpath run --config engine.toml
synpath status --journal trading.db
synpath halt --journal trading.db --reason "manual" --policy cancel
synpath resume --journal trading.db
synpath eod --config engine.toml
```

Each is also `python -m synpath.engine <command>`.

```toml
journal = "trading.db"
halt_policy = "cancel"
orphan_policy = "report"
eod_hour_utc = 0

[venues.kalshi]
enabled = true

[risk]
max_order_contracts = 100
max_order_notional = 500
price_collar = "0.10"
daily_loss_limit = 500
max_open_orders = 50
self_trade_prevention = "firm"
```

Credentials never appear in the configuration: they come from the
environment or a `.env`, as the adapters read them (`synpath doctor` says which
load). `streams = false` turns the streams off. `halt` writes its request
into the journal, so an operator can stop a running engine without reaching
its process.

## How it was verified

- **Offline, 43 tests:** a process killed mid-submit restarts and adopts its
  order with nothing sent twice; an order that never arrived is swept, and
  one the venue has not listed yet is not; every risk rule refuses once
  before anything is sent; orphans, ghosts, drift, missing fills and
  unexplained cash each surface as findings; the ledger's arithmetic,
  including a fill that crosses through zero and a NO fill netting against a
  YES position; a second engine refuses the journal; a week of paper trading
  on an accelerated clock, through the real engine, journal and risk rules.
- **Order types, 25 offline tests against the paper venue:** each type
  through the real engine, plus a restart that brings a trailing stop back
  at the level the market reached, a halt that stands every parent down, and
  a cancel that pulls the children first.
- **Order types on Kalshi's demo, six tests:** an iceberg that shows one of
  four contracts at the venue, a stop that watches the real book and does not
  fire early, a TWAP whose slices are real orders, a parent that survives a
  restart without sending a second child, a halt, and a Day order the venue
  holds the expiry for.
- **RFQ and combos, 26 offline tests against `PaperRfqVenue`** (each on
  SQLite and PostgreSQL): accept to order, fill and position with the events
  in order; accept by amount; the worst-price, risk and kill-switch refusals
  sending nothing; a partial fill; a maker letting the last look lapse; a
  stream and a fill poll moving the accept; a crash between the intent and
  the call, and between the call and its result, on both sides; the side
  guard; a combo position reconciled on its own instrument; the quoter flow,
  confirm through risk, expiry, replace; a halt cancelling every RFQ and
  quote; and a pushed incoming RFQ reaching the quoting callback, which
  quotes it, with no poll (`TestIncomingPush`). The Polymarket global tests
  add a restart that follows an accept to a position, one whose accept never
  landed, a cancel after a restart, and a confirm in doubt.
- **On Kalshi's demo, four tests, twice in a row:** a submitted order is
  found at the venue with the client order id the journal wrote; the restart
  path adopts a real order rather than sending a second one; a halt cancels
  through the venue and is verified by reading the book back; and the ledger
  agrees with Kalshi once settlements are booked.

Run them with:

```bash
pytest tests/test_engine.py tests/test_engine_orders.py tests/test_engine_rfq.py
pytest -m demo tests/test_engine_demo.py tests/test_engine_orders_demo.py
```

Three demo behaviours the tests pinned down, because the engine has to live
with them: the order store lists a new order about three hundred
milliseconds after accepting it; an account-wide cancel takes about half a
second to be reflected and answers with no count; and a cancel is answered
with the order still `resting`, so a parent standing down trusts the cancel
it sent rather than the status it got back.
