# Unified API

## Historical API contract (preview)

Historical HTTP requests use Synpath IDs and snake-case JSON fields:

| Route | Request fields | Response model |
| --- | --- | --- |
| `POST /v1/order-book/at` | `market_id`, `as_of_ms`, optional `side`, `depth` | `OrderBookAtResponse` |
| `POST /v1/order-book/range` | `market_id`, `start_ms`, `end_ms`, optional `side`, `limit` | `OrderBookRangeResponse` |
| `POST /v1/trades/range` | `market_id`, `start_ms`, `end_ms`, optional `limit` | `TradesRangeResponse` |

Request times `as_of_ms`, `start_ms`, and `end_ms` are **Kalshi venue time in Unix milliseconds**. Range windows are half-open: `[start_ms, end_ms)`. Current responses identify this clock with `metadata.time_basis = "venue"`; `"recorder_receive"` is supported for older deployments.

Historical objects extend the existing market-data types: `HistoricalTrade(Trade)` and `HistoricalOrderBook(OrderBook)`:

- A trade appears directly in `TradesRangeResponse.trades` (for example, `response.trades[0].price`). `timestamp` is venue execution time when supplied; `timestamp_source` identifies venue time or a recorder fallback. `observed_at_ms` is when the recorder received it, which can differ from execution time and fall outside the requested venue-time window.
- A historical book adds `as_of_ms` (the requested valuation time) and optional `venue_timestamp_ms` (the original exchange time). Its inherited `timestamp` is the last incorporated update on the response's clock: venue time on the current service, recorder time on older deployments. An unchanged valid book can have `timestamp < as_of_ms`.
- A book range starts with a full-depth historical book and view-relative changes. Each change retains recorder receive time in `observed_at_ms` and exchange time in `venue_timestamp_ms`. Use venue time to place changes in the current request window; do not filter them by recorder arrival time.

Coverage varies by market; unavailable intervals are explicit rather than empty books. Recording-health evidence is observed on the recorder clock and does not establish exact venue-event loss boundaries. The server reads published reconciled hours and includes an opaque `dataset_version` and `processed_through_ms` watermark. A published hour alone does not guarantee continuous coverage. The server caps ranges and returns `result_too_large` instead of a cursor when a response exceeds `limit`.

For a multi-hour or multi-day book walk, use [`examples/track_historical_book.py`](../examples/track_historical_book.py). It requests at most one hour at a time, splits HTTP 413 responses into smaller ranges, replays each full starting book and its exact delta strings into successive `HistoricalOrderBook` objects, and emits `BookGap` intervals instead of carrying stale state through an absence. It follows the response time basis and aborts if the dataset version changes mid-walk. For example:

```bash
# the history service needs a Synpath API key: synpath login, then synpath keys create
python examples/track_historical_book.py \
  --market-id kalshi:YOUR-TICKER \
  --start 2026-10-15T12:00:00Z --end 2026-10-15T13:00:00Z \
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

history = synpath.fetch_order_book_at("kalshi:KXQUANTUM-30", as_of_ms=1792069199000)
if history.book is None:
    print("No book:", history.absence_reason)       # e.g. outside_published_data
else:
    print("Dataset:", history.metadata.dataset_version)
    print("Best bid/ask:", history.book.best_bid, history.book.best_ask)

start_ms, end_ms = 1792065600000, 1792069200000  # 2026-10-15 12:00–13:00 UTC, venue time
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
| `fetch_markets_by_ids(ids)` | `list[Market]` | batched (Kalshi 200, Polymarket 100, Polymarket US 50 ids a request; Opinion has no batch lookup, one request each; Hyperliquid answers any number from one catalog read; predict.fun and Limitless, one request each; SX Bet 30 ids a request; Betfair 100 markets a catalogue request and 40 a book request; Smarkets 300 markets a lookup and 100 a contracts read; Novig one request each; Matchbook 20 events a request, every market of each; ProphetX 20 events a request; Azuro reads each game's conditions once, ten games a request; Myriad 100 a request per network and kind of market; Gemini one event read per event), order preserved, closed and settled markets included |
| `fetch_events(query, limit, cursor, status)` | `Page[Event]` | markets nested |
| `fetch_market(market_id)` | `Market` | |
| `fetch_order_book(market_id, side, depth)` | `OrderBook` | `side="yes"` (default) or `"no"` |
| `fetch_order_books(market_ids, side, depth)` | `dict[str, OrderBook]` | many books; one request on Polymarket (500 books), on Betfair (40 Betfair markets, every runner of each), on Smarkets (200 Smarkets markets, every contract of each), on Matchbook (20 Matchbook events, every runner of each) and on ProphetX (20 events, every line of each), one per market elsewhere |
| `fetch_trades(market_id, since, limit, cursor)` | `Page[Trade]` | newest page first, each page oldest first; `since` in ms; `next_cursor` null at the end |
| `fetch_ohlcv(market_id, timeframe, since, until, limit)` | `list[Candle]` | in the YES price; check `price_source`. With `since`, the first `limit` bars from it; without, the newest `limit` |
| `fetch_series(series_id)` | `Series` | Kalshi only |
| `fetch_fee_schedule(market_id)` | `FeeSchedule` | |
| `iter_events(status)` | iterator of `Event` | walks the whole catalog for you |

Polymarket and Polymarket US add `refresh_quotes(market)`, which replaces the
catalog's quotes with the live book.

Betfair adds `fetch_event_types()` and `fetch_betfair_events(event_type,
competition, query)`, the venue's own sports and fixtures as it lists them,
and takes `event_type` (an id or a name, `"Soccer"`), `competition`, `event`
and `market_type` (`"MATCH_ODDS"`) on `fetch_markets` and `fetch_events`.
Every Betfair call needs your own account and application key; see
[Betfair](capabilities.md#betfair). Betting is `BetfairTrading`
([trading](trading.md#betfair)).

```python
bf = synpath.Betfair()                    # BETFAIR_APP_KEY, BETFAIR_USERNAME, BETFAIR_PASSWORD
runners = bf.fetch_markets(event_type="Soccer", market_type="MATCH_ODDS", limit=9)
book = bf.fetch_order_book(runners[0].id)  # resting backs are YES bids, resting lays asks
book.info["back"], book.info["lay"]        # Betfair's own [odds, stake] pairs
book.info["delayed"]                       # True on a delayed key
```

Smarkets needs no account. It adds `fetch_event(id)` (one Smarkets market
with its contracts, open or settled), `fetch_smarkets_events(ids)`,
`fetch_event_states(ids)` (scores, clock, period), and the raw
`fetch_quotes`, `fetch_volumes` and `fetch_last_executed_prices` by Smarkets
market id, and takes `domain` (`"football"`, `"politics"`), `scope`
(`"single_event"`, `"outright"`) and `market_type` (`"WINNER_3_WAY"`) on
`fetch_markets` and `fetch_events`. See [Smarkets](capabilities.md#smarkets).
Order entry is `SmarketsTrading`, with the same login; see
[trading](trading.md#smarkets).

```python
smk = synpath.Smarkets()                   # no account; SMARKETS_USERNAME / _PASSWORD undelay the quotes
contracts = smk.fetch_markets(domain="football", market_type="WINNER_3_WAY", limit=9)
book = smk.fetch_order_book(contracts[0].id)  # Smarkets bids are YES bids, offers YES asks
book.info["bids"], book.info["offers"]     # Smarkets' own [price bps, quantity] pairs
smk.fetch_trades(contracts[0].id)          # the short recent tape
```

Novig needs no key for market data. It adds `fetch_event(id)` (one game with
its markets), `outcome_ids(market_id)` (the YES and NO outcome ids an order
names), and the raw `fetch_novig_market` and `fetch_novig_event`, and takes
`league` (`"NFL"`, `"NBA"`), `market_type` (`"MONEY"`, `"SPREAD"`, `"TOTAL"`)
and `event` on `fetch_markets` (`league` on `fetch_events`). `env="paper"`
reads the Paper exchange, whose ids differ from Production's. See
[Novig](capabilities.md#novig).

```python
nv = synpath.Novig()                       # no key; Novig(env="paper") for Paper
games = nv.fetch_markets(league="NFL", market_type="MONEY", limit=9)
book = nv.fetch_order_book(games[0].id)    # bids on the YES outcome, bids on the NO outcome as asks
book.info["orders"]                        # Novig's own resting orders by outcome id
nv.fetch_fee_schedule(games[0].id).info["charged"]   # "WHEN_LIVE": takers pay only in play
```

Matchbook needs no account. It adds `fetch_event(id)` (one Matchbook market
with its runners, open or closed), `fetch_sports()`,
`fetch_matchbook_events(ids)` (Matchbook's own events with their markets and
prices) and `fetch_runner_prices(event_id, market_id, runner_id)`, and takes
`sport` (an id or a name, `"Soccer"`), `tag` (Matchbook's tag url names,
`"england-premier-league"`) and `market_type` (`"one_x_two"`, `"total"`) on
`fetch_markets` and `fetch_events`. It has no text search. `currency=` asks
for prices in USD, EUR, GBP, AUD, CAD or HKD instead of the region's default,
and `depth=` sets the price levels read a side. See
[Matchbook](capabilities.md#matchbook).

```python
mb = synpath.Matchbook()                   # no account
runners = mb.fetch_markets(sport="Soccer", market_type="one_x_two", limit=9)
book = mb.fetch_order_book(runners[0].id, depth=5)  # lay prices are YES bids, back prices YES asks
book.info["back"], book.info["lay"]        # Matchbook's own [odds, available amount] pairs
```

ProphetX needs a key for every call: the Trading API key pair
(`PROPHETX_ACCESS_KEY`, `PROPHETX_SECRET_KEY`) or a Market Data API key
(`PROPHETX_AFFILIATE_KEY`). It adds `fetch_event(id)` (one open game with its
lines), `fetch_tournaments()`, `fetch_sport_events(tournament_id, event_ids)`,
the raw `fetch_event_markets(event_ids)` and `lines(event_id)`, and, with the
key pair, `fetch_strikes(strike_ids)`; it takes `tournament_id` on
`fetch_markets` and `fetch_events`. It lists only games not started or live,
and serves no trades or candles over REST. See [ProphetX](capabilities.md#prophetx).

```python
px = synpath.ProphetX()                    # PROPHETX_ACCESS_KEY / _SECRET_KEY, or PROPHETX_AFFILIATE_KEY
lines = px.fetch_markets(tournament_id=31, limit=9)   # 31 is the NFL
book = px.fetch_order_book(lines[0].id)    # side one's offers are YES asks, side two's YES bids
book.info["yes"], book.info["no"]          # ProphetX's own levels, American prices
```

Azuro needs no key. It adds `fetch_sports()` (the feed's sports, countries
and leagues with their open game counts), `fetch_limits(market_id, side)`
(the pool's minimum and maximum bet and maximum payout on one selection) and
`fetch_condition_states(condition_ids)` (fresh odds and states, without the
games), and takes `sport` and `league` (the feed's slugs, `"football"`,
`"premier-league"`) on `fetch_markets` and `fetch_events`. `chain=` picks the
pool read: `polygon` (the default), `base`, or a testnet. See
[Azuro](capabilities.md#azuro).

```python
azuro = synpath.Azuro()                     # the Polygon pool; Azuro(chain="base") for Base
markets = azuro.fetch_markets(sport="football", limit=20)
market = markets[0]
market.yes.quote.ask, market.info["odds"]   # 1 / odds, and the venue's odds
azuro.fetch_limits(market.id)               # {"min_bet", "max_bet", "max_payout", ...}
azuro.fetch_trades(market.id)               # single bets, from the bets subgraph
```

Myriad needs no key (an API key and secret raise its rate limit). It takes
`topic` (`"sports"`, `"crypto"`), `tag` (a league, team or asset slug),
`in_play`, `trading_model` (`"all"`, the default, `"ob"` or `"amm"`) and
`network` (56 BNB Chain, 2741 Abstract, 59144 Linea) on `fetch_markets` and
`fetch_events`, and adds `fetch_event(id)`, `fetch_topics()`, `fetch_tags(type)`
and `raw_market(id)`. Any id also takes the market's slug. See
[Myriad](capabilities.md#myriad). Both kinds trade through `MyriadTrading`:
order-book markets with signed orders, AMM markets as market orders sent on
chain ([Trading](trading.md#myriad)).

```python
myriad = synpath.Myriad()                  # no key needed
games = myriad.fetch_markets(topic="sports", trading_model="ob", limit=9)
book = myriad.fetch_order_book(games[0].id, side="no")   # outcome 1's own book
pool = myriad.fetch_order_book("myriad:56-amm-120473")   # built from the pool: book.info["depth_source"] == "amm"
myriad.fetch_fee_schedule(games[0].id).estimate(0.40, 100)   # the taker fee at 40c
```

Gemini needs no account for market data. It adds `fetch_event(id)` (one event
with its contracts, any status), `fetch_gemini_event(ticker)` (the raw event,
its child events included), `fetch_categories(status)` and `fetch_strike(id)`
(a crypto Up/Down event's strike once set), and takes `category` and the sports
taxonomy, `sport`, `sports_market_type`, `sports_market_subject`,
`sports_market_scope` and `sports_market_metric` (each a value or a list), on
`fetch_markets` and `fetch_events`. See [Gemini](capabilities.md#gemini).

```python
gemini = synpath.Gemini()                 # no account
lines = gemini.fetch_markets(sport="basketball", sports_market_type="spread", limit=9)
lines[0].info["sports_market"]            # {"sport": "basketball", "type": "spread", "subject": "team", ...}
lines[0].info["strike"]                   # {"type": "spread", "value": "1.5"}
book = gemini.fetch_order_book(lines[0].id)   # YES space; side="no" mirrors it
```

Polymarket also has
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

Matching covers every venue this library speaks: `kalshi`, `polymarket`,
`polymarket_us`, `opinion`, `hyperliquid`, `predict_fun`, `limitless` and
`sx_bet` (not Betfair, Smarkets, Novig, Matchbook, ProphetX, Azuro or Gemini yet). `venue=` asks one of them in particular; without it the answer is
the first venue listing the same question, in the order Polymarket, Kalshi,
Opinion, Hyperliquid, predict.fun, Limitless, SX Bet, Polymarket US.

```python
synpath.match_market("kalshi:KXMLB-26-LAD", venue="polymarket_us")
synpath.match_market("polymarket_us:tec-mlb-champ-2026-09-27-lad", venue="kalshi")
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
does for `settled`, since its catalog does not publish resolution status, and
Betfair for everything but `open`, since a closed market leaves its catalogue
(read it by id instead). Smarkets filters on its events' states: `closed` is an
ended or suspended event, `settled` a settled or cancelled one. Novig filters
on its events' states too, `closed` being a final or cancelled event whose
markets await their grades; its public catalog drops a market once it
settles, so `settled` raises (read a settled market by id with a key). Matchbook
lists only open events with their markets and raises `NotSupported` for the
rest (read a closed market by id). ProphetX lists
only games not started or live, so it raises for everything but `open`. Azuro lists
`open` games only and raises for the rest; a settled market is read by id. Gemini's `closed`
is a closed or under-review event, `settled` a settled or invalid one.
Anything outside the four raises `BadRequest` rather than being handed to the
venue to interpret for itself.

### Identifiers

One id per market and per event, `venue:native` (`kalshi:KXELONMARS-99`, `polymarket:2252244`). On Betfair a market is a runner, `betfair:<market id>-<selection id>` with `-<handicap>` added when it is not zero (`betfair:1.234567890-47972`, `betfair:1.234567892-2426--0.5`), and its event the Betfair market, `betfair:1.234567890`. On Smarkets a market is a contract, `smarkets:<market id>-<contract id>` (`smarkets:164973231-456060183`), and its event the Smarkets market, `smarkets:164973231`. On Novig a market is a two-outcome market, `novig:<market id>` (`novig:01a1577c-d666-7e81-852e-68bed1446801`), and its event the game, `novig:<event id>`. On Matchbook a market is a runner, `matchbook:<event id>-<market id>-<runner id>` (`matchbook:34569494493900023-34569495835700023-34569495836701023`), and its event the Matchbook market, `matchbook:<event id>-<market id>`. On ProphetX a market is a line, `prophetx:<event id>-<strike id>` with the strike id of its YES side (`prophetx:44285780-90de769f4f4977a04446ed203e957351`), and its event the game, `prophetx:44285780`. On Azuro a two-outcome condition is one market, `azuro:<condition id>`, a condition with three or more outcomes one market per outcome, `azuro:<condition id>-<outcome id>`, and the game is the event, `azuro:<game id>`; the ids are the same on every chain. On Myriad a market is `myriad:<network>-<market id>` on an order book (`myriad:56-2385`) and `myriad:<network>-amm-<market id>` on a pool (`myriad:56-amm-120473`), since the two number their markets separately; one outcome of a pool with more than two is `myriad:<network>-amm-<market id>-<outcome>`, and that pool its event. A venue event is `myriad:<uuid>`. On Gemini a market is a contract named by its instrument symbol, `gemini:GEMI-NBA-2610232000-CHI-MIL-M-MIL`, and its event the Gemini event, `gemini:NBA-2610232000-CHI-MIL-M`. The full rules are on [Synpath IDs](https://synpath.dev/docs/concepts/ids).

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
