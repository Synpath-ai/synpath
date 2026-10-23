# Capabilities

Back to the [README](../README.md).

Read `has` before you call. A capability a venue lacks raises `NotSupported`
rather than returning an empty list, so "cannot" is never mistaken for "there
is nothing".

| Method | `kalshi` | `polymarket` | `polymarket_us` | `opinion` | `hyperliquid` | `predict_fun` | `limitless` | `sx_bet` | `betfair` | `smarkets` | `gemini` |
|---|---|---|---|---|---|---|---|---|---|---| --- |
| `fetch_markets` | yes | yes | yes | yes, categorical topics flattened into their options | yes, every outcome of a question is a market, the fallback ("Other") included | yes, quoted from the listing | yes, a group's options flattened; quoted from the venue's prices, bid and ask on `refresh_quotes` | yes, outcome one as YES, under the venue's names for both sides; bid and ask on `refresh_quotes` | yes, each runner a market, soonest start first, with its best offers; filter by event type, competition, event or market type | yes, each contract a market, Smarkets events in id order; quotes, last executed price and market volume attached; filter by domain, scope or market type, no login | yes, each contract a market, events in the venue's order; filter by category and the sports taxonomy (`sport`, `sports_market_type`, subject, scope, metric), no account |
| `fetch_events` | yes | yes | yes | yes, one per topic | yes, one per question; a standalone outcome is its own event | yes, one per category | yes, one per group; a standalone market is its own event | yes, one per fixture, one league a page | yes, one per Betfair market (all its runners); the fixture is `info["betfair_event"]` | yes, one per Smarkets market (all its contracts); the fixture is `info["smarkets_event"]` | yes, one per Gemini event (its own contracts); a game's spreads, totals and props are their own events, named in `info["child_events"]` |
| `fetch_market` | yes | yes | yes | yes; a categorical option costs a second read for its topic | yes, from the cached catalog | yes | yes; a group option costs a second read for its question | yes, open or settled | yes, open or closed; a closed market is read from its book alone (no names, but the result) | yes, open or settled | yes, any status; a symbol not seen in a listing costs one event read per candidate ticker |
| `fetch_order_book` | yes | yes | yes | yes | yes, top 20 levels a side; NO mirrored from YES | yes; NO mirrored from YES | yes; NO mirrored from YES | yes; orders on outcome two read as YES asks; NO mirrored | yes, best offers (3 levels on a delayed key); resting backs are YES bids, resting lays asks; NO mirrored | yes, the venue's quotes: bids are YES bids, offers YES asks; delayed without a login; NO mirrored | yes, the whole book in YES space; NO mirrored |
| `fetch_order_books` (batched) | yes, one request per market | yes, one round trip | yes, one request per market | yes, one request per market | yes, one request per market | yes, one request per 50 | yes, one request per market | yes, one request per market | yes, forty Betfair markets a request (the weight limit) | yes, two hundred Smarkets markets a request | yes, one request per market |
| `fetch_trades` | yes | yes | no public tape over REST; the last trade is on the book | no public tape; the last trade is on `refresh_quotes` | partial: the venue's last few prints only, no history | yes, the venue's matches | yes, the venue's settled fills | yes, the venue's tape of taker bets | no: Betfair publishes no individual trades, only volume per price and the last traded price | partial: the venue's short recent tape per contract (a handful of trades), no paging | yes, seven days of public trades, 500 a request, paged forward by trade id |
| `fetch_ohlcv` | yes, the venue's candles | yes, built from the trade tape, both tokens folded into the YES price; the venue's quote samples via `source="quotes"` | partial: quote-derived bid/ask midpoints, no volume; no public tape to build from | partial: last-trade samples, hourly or daily, no volume | yes, the venue's traded candles with volume; 6h built from 2h | partial: the venue's probability samples, no volume | partial: the venue's price samples over fixed lookbacks, no volume | partial: built from the tape of taker bets, with volume; at most 2,000 bets back | no: no trades to build from | no: the venue's executions time series answers `CHARTS_UNAVAILABLE` | partial: the venue's candles over a fixed recent window per timeframe; a bar without trades repeats the last close (`sampled_last`) |
| `fetch_series` | yes | no series tier | yes, but fees are per market, not per series | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier |
| `fetch_fee_schedule` | yes | yes | yes | yes, read from the venue's fee contract on BNB Chain | yes, at the lowest volume tier; charged on closing fills only | yes, taker only | yes, the venue's buy-fee curve, taker only | yes, the venue's default rates: 1% of a winning taker's profit | yes, commission at the market's base rate on net market winnings; discount and Expert Fee not included | yes, 2% (the standard tier) on net market winnings; tier and promotional rates not included | yes, from the contract's listing date: `rate * C * P * (1 - P)` (0.07 taker, 0.0175 maker, 0 during the maker promotion to 31 December 2026), or the legacy `min(P, 1 - P)` schedule; up to the cent |
| `search` | yes ([undocumented host](api.md#search)) | yes | yes | partial: no venue search; titles matched here over 25 catalog pages | yes, matched here over the whole catalog | yes, at the venue | yes, at the venue | yes, fixtures by team name at the venue (at most eight) | yes, Betfair's text search | yes, Smarkets' event search | yes, the venue's title search |
| `fetch_markets_by_ids` | yes | yes | yes | yes, one request per market | yes, one catalog read | yes, one request per market | yes, one request per market | yes, 30 a request | yes, a hundred Betfair markets a catalogue request, forty a book request | yes, three hundred markets a lookup, a hundred a contracts read | yes, one event read per event |
| `sort` | yes, the page is ordered after it is read | yes, at the venue | yes, the page is ordered after it is read; `volume` and `liquidity` cost one read per market | partial: `volume` and `newest` at the venue; no liquidity figure exists | partial: `volume` and `newest`; no liquidity figure exists | partial: `volume` only | partial: `newest` only | no: the venue publishes no volume, liquidity or listing time | partial: `volume` (matched) and `liquidity` (available) at the venue, one page; no listing time | no: the venue lists by id, start time or display order only | no: the listing takes no sort |
| `match_market` / `match_event` | no | no | no | no | no | no | no | no | no | no | no |

Polymarket US is the CFTC-regulated exchange, not the on-chain CLOB the
`polymarket` adapter reads; the two share a brand and nothing else. It has
one book per market, the YES side, and the NO view is that book reflected,
marked `derived=True` as on Kalshi. Its catalog payloads carry no volume, no
sizes and no last trade; `refresh_quotes(market)` reads the book once for all
of them, and for the market's live state.

Opinion lists *topics*: a binary topic is one market, a categorical topic
holds one binary market per option, and only those trade. Each topic is an
event here. Its catalog carries no prices at all, so `refresh_quotes(market)`
reads both books and the last trade. Statuses `open`, `settled` and `all`
filter at the venue; `closed` cannot and raises. An API key is optional and
only raises the rate limit (`Opinion(api_key=...)`).

Hyperliquid lists outcome markets (HIP-4) next to its perps. A question
("2026/2027 English Premier League winner") holds one binary outcome per
option plus a fallback ("Other"), exactly one of which resolves Yes; each
question is an event (`hyperliquid:q198`) and each outcome a market
(`hyperliquid:1473`). Titles are rendered from the venue's published
templates. The whole catalog is one document, read once and reused for 30
seconds, so search covers every market. There is no `closed` state: an
outcome trades until it settles, so `closed` raises. Fees are charged only
on fills that close a position, never on opening ones.

predict.fun groups markets into *categories*, each an event here (keyed by
its slug), with every market a binary market of its own. Every mainnet request
needs an API key (`PredictFun(api_key=...)` or `PREDICT_FUN_API_KEY`, created at
developers.predict.fun); `testnet=True` needs none. The listing carries each
market's best bid and ask, so a page of markets arrives quoted, and almost every
market names the Polymarket market it mirrors (`info["polymarket_condition_ids"]`).
Fees are taker-only: `rate * min(p, 1 - p)` a share.

Limitless names every market by its slug (`limitless:btc-up-or-down-daily-…`),
which is what its API takes. A *group* ("What price will Bitcoin hit on
October 7?") is an event of binary options; a neg-risk group's options exclude
each other, while a price ladder's do not. A group stays listed while some of
its options have already resolved, so an open listing leaves those out. The
listing carries each market's price but no bid or ask; `refresh_quotes(market)`
reads the book. Many markets copy a Polymarket market and say which
(`info["polymarket_slug"]`). Fees are taker-only and fall as the price rises:
3% of the tokens bought up to 50c, down to 0.4% near $1. A handful of older
markets trade against an automated market maker and have no book. No key.

SX Bet is a sports exchange. Every market is a binary question with two named
sides ("Over 49.5" against "Under 49.5", one team against the other): outcome one
is YES and outcome two is NO, and the names stay on `market.yes.label` and
`market.no.label`. Each game is an event (`sx_bet:L18876509`) holding its
moneyline, spreads and totals at several lines, period lines and player props;
`info["type_name"]` and `info["line"]` say which. A market can be voided (a
cancelled game, a spread landing on the line), which returns every bet;
`info["void_condition"]` names when, and a settled market's
`info["winning_outcome"]` is `yes`, `no` or `void`. Prices sit on a 0.125%
ladder. Fees are charged on winnings: 1% of a taker's profit on a winning bet,
nothing for a maker. No key for market data.

### Betfair

Betfair Exchange is a betting exchange, read as YES/NO markets. A Betfair
*market* ("Match Odds" on Arsenal v Chelsea) holds *runners* (Arsenal, Chelsea,
the Draw); each runner is a Synpath market, `betfair:1.234567890-1096` (the
handicap follows a second dash on a handicap runner: `betfair:1.234567892-2426--0.5`),
and the Betfair market is its event, `betfair:1.234567890`, the way a Polymarket
neg-risk event holds one market per candidate. The Betfair event (the fixture,
which holds many Betfair markets) is in `info["betfair_event"]`.

Backing a runner at decimal odds O buys its YES at 1/O; laying it sells YES at
1/O. Betfair names its book after what a taker can do, so `availableToBack`
(resting lays) are YES **asks** at 1/O and `availableToLay` (resting backs) are
YES **bids**. Sizes are stakes in the account's currency (`info["currency"]`);
a stake S at odds O is S·O contracts. The venue's own odds and stakes stay in
the book's `info["back"]` and `info["lay"]`; prices are always Betfair's,
never moved off its ladder (1.01 to 1000). Handicap and line runners can
half-win or be voided on the line, so they are `market_type="unknown"`.

A market's status is `OPEN`, `SUSPENDED` (still `open` here, with
`active=False`: Betfair suspends for a goal or the off and reopens),
`INACTIVE` (`unopened`) or `CLOSED`; each runner is `ACTIVE`, `WINNER`,
`LOSER`, `PLACED`, `REMOVED` or `HIDDEN`. The result is the runner's status:
`info["result"]` is `yes`, `no` or `void` (a removed runner). Only open markets
are listed; a closed one is still read by id.

**Your account, your key.** Every Betfair call, market data included, needs
your own Betfair account and application key (`BETFAIR_APP_KEY`,
`BETFAIR_USERNAME`, `BETFAIR_PASSWORD`; `BETFAIR_CERT_PATH` and
`BETFAIR_KEY_PATH` for certificate login; `BETFAIR_JURISDICTION` for the .it,
.es, .ro or .com.au sites). The adapter logs in on the first call and keeps the
session token in memory only, extending it with `keepAlive`. The free
**delayed** key serves snapshots 1 to 180 seconds late, three price levels a
side, with no full ladder and no traded volume per runner; every book and
market says `info["delayed"]` (Betfair's own `isMarketDataDelayed` when it
sends it). Requests are batched to stay inside Betfair's weight limit (200 a
request: best offers weigh 5 a market). There is no order entry for Betfair
yet.

**Where it works, and what you may do with it.** Betfair refuses its API from
several countries, among them the US, Germany, France, the Netherlands,
Singapore, India, Turkey and the UAE. Its keys are for an account holder's own
betting: displaying Betfair prices to anyone else needs a Betfair data
licence.

### Smarkets

Smarkets is a UK betting exchange, read as YES/NO markets the way Betfair is.
A Smarkets *market* ("Full-time result" on Fulham v Crystal Palace) holds
*contracts* (Fulham, Crystal Palace, the Draw); each contract is a Synpath
market, `smarkets:164973231-456060183` (market id, contract id), and the
Smarkets market is its event, `smarkets:164973231`. The Smarkets event (the
fixture, which holds a hundred markets) is in `info["smarkets_event"]`.

Smarkets already prices in probability: basis points of 100% (5000 = 50%,
decimal odds 2.0). Its `bids` are resting buys (backs), which are YES bids,
and its `offers` resting sells (lays), YES asks, so nothing is inverted: the
YES price is the venue's price over 10,000. A level's `quantity` is the pot
(back stake plus lay liability) in 1/100 of a penny, and a pot of £Q pays £Q to
the winner, so it is Q contracts paying 1 each: quantity 100,000 at 5000 is 10
contracts costing £5. Amounts are GBP. The venue's own rows stay in the book's
`info["bids"]` and `info["offers"]`.

Volumes come per Smarkets market, not per contract, in whole pounds (back
stakes plus lay liabilities matched): on the event as `info["volume_gbp"]` and
on each contract as `info["market_volume_gbp"]`, never as the contract's own
volume. The last executed price comes per contract, as a percentage, and is the
quote's `last` (a "0" means it never traded). The recent-trades tape is short
(a handful per contract) and unpaged, so `fetch_trades` is partial; the
executions time series behind Smarkets' charts answers `CHARTS_UNAVAILABLE`,
so there are no candles.

A market is `new`, `open`, `live` (in-play), `halted`, `settled`, `voided` or
`unavailable`; a contract `new`, `open`, `live`, `halted`, `unavailable`, or
its outcome `winner`, `loser`, `deadheat`, `reduced` (a withdrawn runner, the
others reduced by its factor) or `voided`. Halted and unavailable are still
`open` here, with `active=False`. `info["result"]` is `yes`, `no`, `void` (a
voided market or contract, or a reduced one) or `dead_heat`. Listings take
`status`: open is upcoming or live events, closed ended or suspended, settled
settled or cancelled.

Commission is 2% of an account's net winnings in each Smarkets market at
settlement on the standard tier (`net_market_winnings`); an account's tier or
promotion can change it, so pass `commission=` to the adapter if yours differs.

**No account for market data.** Every read here is an anonymous GET. Without a
session Smarkets serves its quotes *delayed* (it does not say by how much) and
at lower limits: about 60 requests a minute, 20 on `/quotes/`, against 1,200
per 60 s (50 on `/quotes/`) for an API account. Every book and market says
`info["delayed"]`. An optional login (`SMARKETS_USERNAME`,
`SMARKETS_PASSWORD`) sends a session token (`Authorization: Session-Token`)
for undelayed quotes; it lasts 30 minutes from its last use, and Smarkets
allows 5 logins per 5 minutes. Accounts with two-factor login are refused with
a clear error. **Betting needs Smarkets' approved API access** (applied for,
with a one-off £150 fee); there is no Smarkets order entry here yet.

Requests carry a `synpath/<version>` User-Agent: Smarkets' Cloudflare front
answers the default ones of HTTP libraries (python-httpx, curl) with a 403
page. A 403 that is not Smarkets' own JSON is reported as Cloudflare's.

**What you may do with it.** Smarkets' API terms allow viewing prices and
betting on your own account; odds scraping is prohibited. Which countries
Smarkets refuses API use from is not published with its API documentation and
is unverified here.

### Gemini

Gemini Predictions is the event-contract exchange of Gemini Titan, a
CFTC-registered exchange. An *event* ("Chicago vs Milwaukee",
`gemini:NBA-2610232000-CHI-MIL-M`) holds *contracts*, one yes/no question
each; a contract is a Synpath market named by its instrument symbol,
`gemini:GEMI-NBA-2610232000-CHI-MIL-M-MIL`, the symbol Gemini's books,
streams and orders use. Gemini says not to build a symbol from its parts, and
nothing here does: a symbol not met in a listing is looked up by asking the
events it could belong to, most likely first, and the event that lists it is
the answer.

Every book is in YES space: a NO buyer at 0.30 is a YES ask at 0.70, so the
NO view is the YES book mirrored (`derived=True`). The event payload carries
each contract's best bid and ask, which become the quotes; it gives the last
trade price without a time, so that stays in `info["last_trade_price"]`
rather than the quote. Each contract publishes its own grid
(`info["price_increment"]`, `quantity_increment`, `quantity_minimum`; a cent
and a hundredth of a contract on every one read so far). Volume is per event,
in USD (`info["volume_usd"]`, `volume_24h_usd`).

**Sports.** A sports event carries Gemini's taxonomy in `sportsMarket`, on
every market as `info["sports_market"]`: the sport, the family (`moneyline`,
`spread`, `total`, `prop`, `correct_score`, `to_advance`, `futures`), the
subject (`contest`, `team`, `player`, `participant`), the scope
(`full_contest`, `regulation`, `half` 1, innings 1 to 5) and the statistic
(`points`, `rushing_yards`). Listings filter on each (`sport=`,
`sports_market_type=`, `sports_market_subject=`, `sports_market_scope=`,
`sports_market_metric=`; a list matches any). A game's moneyline event names
its spread, total and prop events in `info["child_events"]`; the venue lists
those on their own as well, and a listing returns each once. Contracts carry
their strike (`info["strike"]`: a spread margin, a total line, or a crypto
Up/Down reference price once set). A moneyline or correct-score event is
mutually exclusive; nothing else is claimed.

An event is `approved` (listed, not yet trading: `unopened`), `active`,
`closed`, `under_review` (awaiting its result: `closed`), `settled` or
`invalid` (voided: `settled`). An active contract whose `marketState` is
`closed` is `closed` once its expiry has passed and `open` with `active=False`
before. Listings take `status`: open is active, closed is closed or under
review, settled is settled or invalid.

**Fees** depend on when a contract was listed. From 18 March 2026: `rate * C *
P * (1 - P)`, taker 0.07 and maker 0.0175; before: `rate * C * min(P, 1 - P)`,
taker 0.05 and maker 0.01. Fees round up to the cent. Gemini waives maker fees
from 2 September to 31 December 2026, and the schedule says so. There are no
settlement fees.

**No account for market data.** Events, books, trades and candles are public
GETs (120 a minute, kept to two a second here). Trades come from Gemini's
seven-day public window; candles from a fixed recent window per timeframe.
`Gemini(sandbox=True)` reads api.sandbox.gemini.com.

**Trading** needs a Gemini account with Prediction Markets access (US) and an
API key: account-scoped, with the Trader role and a time-based nonce (the
WebSocket takes no other kind). The account must have accepted Gemini's
current Prediction Markets terms; `GeminiTrading.fetch_terms_status()` says
whether it has. See [trading](trading.md#gemini).

Combo RFQ is not offered. It is a sealed one-second auction for market makers
over the WebSocket, in beta, and does not map onto the request-and-accept RFQ
the Kalshi adapter has.

Matching spans venues, so no adapter claims it. `synpath.match_market(id)`
and `synpath.match_event(id)` are answered by Synpath's hosted matching service
at `api.synpath.dev`, with a Synpath API key (the one `synpath keys create` saved, `SYNPATH_API_KEY`, or `api_key=`). See
[api.md](api.md#matching-across-venues).

```python
if kalshi.has["fetch_ohlcv"] is True:
    candles = kalshi.fetch_ohlcv(market_id, timeframe="1h")
```

Values are `True`, `False` or `"partial"` (thinner than the type suggests; the
docstring says how). Where a venue answers a question differently from the
others, the method's docstring says how, and the table above says what it
costs.
`has` is always complete: every venue answers every capability question, so
`venue.has[anything_in_the_table]` returns `False` rather than raising. An
adapter declares only what it supports, the base class fills the rest in when
the class is created, and a misspelled capability fails at import.

## Order entry

The trading adapters answer the same way, and they are separate objects:
`synpath.KalshiTrading` and its siblings, part of the base
install. Polymarket US has two, because the venue has
two: a retail API any verified account uses, and an exchange API for
onboarded firms.

| Method | `kalshi` | `polymarket` | `polymarket_us` (retail) | `polymarket_us` (exchange) | `opinion` | `hyperliquid` | `predict_fun` | `limitless` | `sx_bet` | `gemini` |
|---|---|---|---|---|---|---|---|---|---| --- |
| `create_order`, `create_orders` | yes | yes | yes | yes | one at a time; `market` and `ioc` are limits with the rest cancelled | yes, a batch is one signed action; `market` and `ioc` are `Ioc` limits | one at a time; `market`, `ioc` and `fok` are the venue's `MARKET` strategy at the worst price given | one at a time; `market` and `ioc` are the venue's fill-and-kill at the worst price given | yes, ten a request; `market` and `ioc` are the venue's `IOC` at the worst price given, `fok` its `FOK` | yes, twenty a request; `market` and `ioc` are the venue's `immediate-or-cancel` at the worst price given, `fok` its `fill-or-kill` |
| `cancel_order`, `cancel_orders`, `cancel_all_orders` | yes | yes | yes | yes | one at a time; `cancel_all_orders` lists then cancels | yes; `cancel_all_orders` is one action | yes, up to 100 a request, off the book (not on chain) | yes, up to 50 a request; `cancel_all_orders` per market | yes, up to 100 a request; `cancel_all_orders` per market or account-wide | yes, twenty a request; `cancel_all_orders` lists then cancels |
| `edit_order` | yes, amend or decrease | no; an edit is a cancel and a new order | yes, price, quantity and time in force | yes | no | no | no | no | no | no |
| `fetch_order`, `fetch_open_orders` | yes | yes | yes | yes | yes | yes | yes | yes | yes; any order by id, in any state | yes; an order by id is searched among the active orders and the latest thousand of the history |
| `fetch_orders` (by status or time) | yes | no | no | yes, rationed to 12 requests a minute | yes, by status | partial: the venue's last 2000, filtered here | yes, open or filled | partial: per market, open or closed | no; the venue lists only resting orders | yes, filled or cancelled, by offset or a time window |
| `fetch_my_trades` | yes | yes | no; the activity feed carries no order id | yes | yes, confirmed fills | yes, from a time with `since` | yes, settled matches | yes, settled fills from the account's history | yes, the account's fills | partial: one aggregate fill per order, at its average price; each execution is on `GeminiUserStream` |
| `fetch_positions`, `fetch_balance` | yes | yes | yes | yes | yes | yes, the spot side's USDC and outcome coins | yes, USDT on chain less what orders hold | yes, USDC on Base less what orders hold | yes, the proxy wallet's USDC, free and escrowed | yes, netted on YES with both inventories kept; USD available and total |
| `fetch_settlements` | yes | yes, from the redemption activity | yes | no | no | no | no | no | no | yes |
| `fetch_queue_position` | yes | no | no | no | no | no | no | no | no | no |
| `fetch_fee_estimate` | yes | yes | yes | yes | yes, from the fee contract, 0.25 USDT minimum | yes, the closing charge; opening fills pay nothing | yes, `rate * min(p, 1 - p)` a share | yes, the venue's buy-fee curve on the token bought | yes, 1% of a winning taker bet's profit | yes, the contract's schedule, up to the cent |
| `rfq` | yes | no | no | no | no | no | no | no | no | no: combo RFQ is a WebSocket auction for market makers, in beta |
| `split_merge` | no; contracts are not tokens | partial: Deposit Wallets and EOAs | no | no | no | no | no | no | no | no |

Order types are a separate question. A venue holds `limit` and `market`;
everything else -- stops, icebergs, brackets, TWAP, pegs -- is held by the
execution engine, which reports them itself
([docs/engine.md](engine.md#order-types-the-venues-do-not-hold)). Polymarket
US's exchange API is the exception: its published schema carries stop and
stop-limit orders, so those are sent natively and listed in
`native_order_types`.

## Streams

Every stream answers the `watch_*` keys the same way
(WebSocket streams are in the base install; `pip install synpath[grpc]` adds the exchange API's gRPC streams).

| Stream | order book | ticker | trades | market status | orders | fills | positions | balance |
|---|---|---|---|---|---|---|---|---|
| `KalshiStream` | yes | yes | yes | yes | yes | yes | yes | no |
| `PolymarketMarketStream` | yes | yes | yes | partial | no | no | no | no |
| `PolymarketUserStream` | no | no | no | no | yes | yes | no | no |
| `PolymarketUSMarketStream` | yes | yes | yes | partial | no | no | no | no |
| `PolymarketUSPrivateStream` | no | no | no | no | yes | yes | yes | yes |
| `PolymarketUSExchangeMarketDataStream` | yes | yes | no | no | no | no | no | no |
| `PolymarketUSExchangeOrderStream` | no | no | no | no | yes | yes | no | no |
| `OpinionMarketStream` | yes | partial | yes | no | no | no | no | no |
| `OpinionUserStream` | no | no | no | no | yes | yes | no | no |
| `HyperliquidMarketStream` | yes | yes | yes | no | no | no | no | no |
| `HyperliquidUserStream` | no | no | no | no | yes | yes | no | no |
| `PredictFunMarketStream` | yes | partial | no | yes | no | no | no | no |
| `PredictFunUserStream` | no | no | no | no | yes | yes | no | no |
| `LimitlessMarketStream` | yes | partial | no | partial | no | no | no | no |
| `LimitlessUserStream` | no | no | no | no | yes | yes | no | no |
| `SxBetMarketStream` | yes | partial | yes | yes | no | no | no | no |
| `SxBetUserStream` | no | no | no | no | yes | yes | no | no |
| `BetfairMarketStream` | yes, best offers to 10 levels | partial: top of book | no: no public trades | yes, per runner | no | no | no | no |
| `SmarketsMarketStream` | partial: polled from REST quotes, every 5 s by default | partial: polled | no: read the recent tape with `fetch_trades` | partial: polled every few rounds | no | no | no | no |
| `GeminiMarketStream` | yes, differential, from a snapshot; a skipped update resubscribes | yes, top of book and last trade | yes | yes, contract status and strikes | no | no | no | no |
| `GeminiUserStream` | no | no | no | no | yes | yes, each execution with its fee | yes, settlement payouts included | yes |

The exchange API's other gRPC streams carry one thing each and say so:
drop copy and trade capture carry fills, position and position-change carry
positions, instrument state carries market status, and the balance ledger
carries account movements as venue events.

---
