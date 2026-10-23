# Capabilities

Back to the [README](../README.md).

Read `has` before you call. A capability a venue lacks raises `NotSupported`
rather than returning an empty list, so "cannot" is never mistaken for "there
is nothing".

| Method | `kalshi` | `polymarket` | `polymarket_us` | `opinion` | `hyperliquid` | `predict_fun` | `limitless` | `sx_bet` | `betfair` | `smarkets` | `novig` | `matchbook` | `prophetx` | `azuro` | `myriad` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `fetch_markets` | yes | yes | yes | yes, categorical topics flattened into their options | yes, every outcome of a question is a market, the fallback ("Other") included | yes, quoted from the listing | yes, a group's options flattened; quoted from the venue's prices, bid and ask on `refresh_quotes` | yes, outcome one as YES, under the venue's names for both sides; bid and ask on `refresh_quotes` | yes, each runner a market, soonest start first, with its best offers; filter by event type, competition, event or market type | yes, each contract a market, Smarkets events in id order; quotes, last executed price and market volume attached; filter by domain, scope or market type, no login | yes, the outcome Novig lists first as YES, under the venue's names for both sides, newest first, with each outcome's best bid; filter by league, market type or event, no key | yes, each runner a market, Matchbook events by start time with every market priced in one request; filter by sport, tag or market type; no login | yes, each line (a moneyline, a spread or total at one number) a market, its first side as YES under the venue's names; games not started or live, tournament by tournament; filter by tournament | yes, a two-outcome condition is one market (lower outcome id YES), a larger one a market per outcome; live games first, then upcoming; quoted from the pool's odds; filter by sport or league | yes, order-book and AMM markets, outcome 0 as YES under the venue's names; a pool with more than two outcomes flattened into one market per outcome; points markets left out; filter by topic, tag, in-play, trading model or network |
| `fetch_events` | yes | yes | yes | yes, one per topic | yes, one per question; a standalone outcome is its own event | yes, one per category | yes, one per group; a standalone market is its own event | yes, one per fixture, one league a page | yes, one per Betfair market (all its runners); the fixture is `info["betfair_event"]` | yes, one per Smarkets market (all its contracts); the fixture is `info["smarkets_event"]` | yes, one per game (or futures question), its markets read one request per event | yes, one per Matchbook market (all its runners); the fixture or race is `info["matchbook_event"]` | yes, one per game, one tournament a page | yes, one per game, every condition's markets | yes, the venue's events; every other market is its own event |
| `fetch_market` | yes | yes | yes | yes; a categorical option costs a second read for its topic | yes, from the cached catalog | yes | yes; a group option costs a second read for its question | yes, open or settled | yes, open or closed; a closed market is read from its book alone (no names, but the result) | yes, open or settled | yes, open or closed; settled from history with a key | yes, open or closed; a closed market costs three more reads (Matchbook lists its runners separately) | yes, while the game is listed (not started or live) | yes, open or settled | yes, open or settled, by id or slug |
| `fetch_order_book` | yes | yes | yes | yes | yes, top 20 levels a side; NO mirrored from YES | yes; NO mirrored from YES | yes; NO mirrored from YES | yes; orders on outcome two read as YES asks; NO mirrored | yes, best offers (3 levels on a delayed key); resting backs are YES bids, resting lays asks; NO mirrored | yes, the venue's quotes: bids are YES bids, offers YES asks; delayed without a login; NO mirrored | yes, top 20 levels per outcome: bids on the YES outcome are YES bids, bids on the NO outcome YES asks; NO mirrored | yes, the runner's prices: lay prices are YES bids, back prices YES asks, three levels a side by default; NO mirrored | yes, ten best levels a side: side one's offers are YES asks, side two's YES bids; NO mirrored | partial: no book; the pool's quote as one level a side, sized by its limits (`/bet/calculation`); NO mirrored | yes; NO is outcome 1's own book, which holds what YES orders imply; an AMM market's built from its pool, one level a cent |
| `fetch_order_books` (batched) | yes, one request per market | yes, one round trip | yes, one request per market | yes, one request per market | yes, one request per market | yes, one request per 50 | yes, one request per market | yes, one request per market | yes, forty Betfair markets a request (the weight limit) | yes, two hundred Smarkets markets a request | yes, one request per market | yes, twenty Matchbook events a request, every runner of each | yes, twenty events a request | yes, up to three requests per market | yes, one request per market |
| `fetch_trades` | yes | yes | no public tape over REST; the last trade is on the book | no public tape; the last trade is on `refresh_quotes` | partial: the venue's last few prints only, no history | yes, the venue's matches | yes, the venue's settled fills | yes, the venue's tape of taker bets | no: Betfair publishes no individual trades, only volume per price and the last traded price | partial: the venue's short recent tape per contract (a handful of trades), no paging | yes, the venue's public tape, paged | no: Matchbook publishes no trades, only the volume matched per runner | no: fills are broadcast on the WebSocket only (`watch_trades`) | yes, every single bet, from the bets subgraph | yes: on a book one trade per taker fill; on a pool its buys and sells |
| `fetch_ohlcv` | yes, the venue's candles | yes, built from the trade tape, both tokens folded into the YES price; the venue's quote samples via `source="quotes"` | partial: quote-derived bid/ask midpoints, no volume; no public tape to build from | partial: last-trade samples, hourly or daily, no volume | yes, the venue's traded candles with volume; 6h built from 2h | partial: the venue's probability samples, no volume | partial: the venue's price samples over fixed lookbacks, no volume | partial: built from the tape of taker bets, with volume; at most 2,000 bets back | no: no trades to build from | no: the venue's executions time series answers `CHARTS_UNAVAILABLE` | partial: built from the public tape, with volume; at most ten pages of 500 prints back | no | no: no price history | partial: built from the bets, with volume; at most 2,000 bets back | partial: the venue's price samples (5 minutes over a day, 30 over a week, 4 hours beyond), no volume |
| `fetch_series` | yes | no series tier | yes, but fees are per market, not per series | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier | no series tier |
| `fetch_fee_schedule` | yes | yes | yes | yes, read from the venue's fee contract on BNB Chain | yes, at the lowest volume tier; charged on closing fills only | yes, taker only | yes, the venue's buy-fee curve, taker only | yes, the venue's default rates: 1% of a winning taker's profit | yes, commission at the market's base rate on net market winnings; discount and Expert Fee not included | yes, 2% (the standard tier) on net market winnings; tier and promotional rates not included | yes, the market's own coefficient and maker credit, and when it charges (in play on game markets) | yes, 2% on net market winnings (UK and Ireland; 4% elsewhere, pass `commission=`) | no: the API documentation publishes no fee rate; each order and fill carries its fee | yes, nothing on top of the odds (`pool_margin`); the relayer's flat fee per bet is on `fetch_fee_estimate` | yes: a book's taker table by price (3 to 180 bps of the notional), makers free; a pool's share of the value |
| `search` | yes ([undocumented host](api.md#search)) | yes | yes | partial: no venue search; titles matched here over 25 catalog pages | yes, matched here over the whole catalog | yes, at the venue | yes, at the venue | yes, fixtures by team name at the venue (at most eight) | yes, Betfair's text search | yes, Smarkets' event search | partial: no venue search; event descriptions matched here over the open catalog | no: the events API has no text search; filter by `sport` or `tag` | partial: no venue search; event, tournament and competitor names matched here over the open games | yes, the feed's game search | yes, at the venue (titles, descriptions, outcomes) |
| `fetch_markets_by_ids` | yes | yes | yes | yes, one request per market | yes, one catalog read | yes, one request per market | yes, one request per market | yes, 30 a request | yes, a hundred Betfair markets a catalogue request, forty a book request | yes, three hundred markets a lookup, a hundred a contracts read | yes, one request per market | yes, twenty events a request, every market of each | yes, twenty events a request | yes, each game's conditions once, ten games a request | yes, a hundred a request per network and kind of market |
| `sort` | yes, the page is ordered after it is read | yes, at the venue | yes, the page is ordered after it is read; `volume` and `liquidity` cost one read per market | partial: `volume` and `newest` at the venue; no liquidity figure exists | partial: `volume` and `newest`; no liquidity figure exists | partial: `volume` only | partial: `newest` only | no: the venue publishes no volume, liquidity or listing time | partial: `volume` (matched) and `liquidity` (available) at the venue, one page; no listing time | no: the venue lists by id, start time or display order only | no: the catalog lists newest first | no: the venue lists by start time only | no: the venue lists in its own order | no: the feed orders games, not markets | yes, at the venue (24-hour volume, liquidity, publication time) |
| `match_market` / `match_event` | no | no | no | no | no | no | no | no | no | no | no | no | no | no | no |

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

### Novig

Novig is a US sports exchange. Every market is a question with two named
outcomes ("EDM -1.5" against "TOR +1.5", "Over 6.5" against "Under 6.5",
"Yes" against "No" on a prop; a soccer result is three Yes/No markets). The
outcome Novig lists first is YES and the other NO, and the names stay on
`market.yes.label` and `market.no.label`; `market.yes.venue_token_id` and
`market.no.venue_token_id` are the venue's outcome ids. Each game is an event
(`novig:<event id>`) holding its moneyline, spreads, totals and props;
`info["market_type"]` and `info["strike"]` say which. A futures question is
an event of its own.

Every order buys one outcome, so a bid on one outcome is an offer of the
other at one minus its price: the book read on YES has the YES outcome's bids
as bids and the NO outcome's as asks. Prices sit on a grid of 0.001 near the
ends (to 0.050 and from 0.950) and 0.005 between. The venue's contract pays 1
cent, so its quantities, volumes and open interest are divided by 100 here;
its own figures stay in `info`.

The public catalog holds a market from its listing until it settles. `open`
lists the markets of events before or in play; `closed` those of finished or
cancelled events awaiting their grades; `settled` raises. With a key,
`fetch_market` reads a settled market, and its grade, from market history: a
winner (`info["result"]` `yes` or `no`), a push, or a fair-market-value
settlement (`void`, with the YES price in `info["fmv_yes"]`). The edge in
front caches public answers for a few seconds and throttles per IP.

Fees are per market: a taker pays `coefficient * P * (1 - P)` per contract
(0.06 today) and the maker earns `makerCredit` of that back. Game markets
charge only while the event is in play (`info["charged"]` `WHEN_LIVE`), so a
fill matched pregame pays nothing and the estimate is the in-play charge;
futures markets on some leagues charge always (`ALWAYS`). Every resting order
on an event is voided when it goes in play.

Production (`api.novig.com`) trades real money for customers who have passed
Novig's identity check, from a US location outside Arizona, Michigan and
Nevada, with a recent device geolocation from the Novig app; a refused
location is a `PermissionDenied` naming the check. Paper (`api.paper.novig.com`,
`NOVIG_ENV=paper`) is the same API with play money and no location check.
Ids and keys differ between the two. Novig's parlay RFQ, for registered
liquidity providers, is not supported yet.

**What you may do with it.** Smarkets' API terms allow viewing prices and
betting on your own account; odds scraping is prohibited. Which countries
Smarkets refuses API use from is not published with its API documentation and
is unverified here.

### Matchbook

Matchbook is a betting exchange licensed in the UK and Alderney, read as YES/NO
markets the way Betfair is. A Matchbook *market* ("Match Odds" on Arsenal vs
Everton) holds *runners* (Arsenal, Everton, the Draw); each runner is a Synpath
market, `matchbook:<event id>-<market id>-<runner id>`, since Matchbook
addresses a runner under its event and market, and the Matchbook market is its
event, `matchbook:<event id>-<market id>`. The Matchbook event (the fixture or
race, which holds dozens of markets) is in `info["matchbook_event"]`.

Back and lay read as YES the way Betfair's do. A `back` price is one a taker
can back at, a resting lay, so it is a YES **ask** at 1/odds; a `lay` price is
one a taker can lay at, a resting back, so it is a YES **bid**.
`available-amount` is the backer's stake on both sides, so a level of stake S
at odds O is S·O contracts paying 1 each. Prices are requested as decimal odds
on a back/lay book whatever the account prefers, and binary markets (totals,
handicaps, yes/no props) include the prices mirrored from the other runner, as
Matchbook shows them. The venue's own rows stay in the book's `info["back"]`
and `info["lay"]`, with the price currency in `info["currency"]`: the one
Matchbook picks for the request's region, unless the adapter's `currency=`
asks for USD, EUR, GBP, AUD, CAD or HKD.

Each runner carries the volume matched on it (`stats.volume_total`, in the
currency); Matchbook publishes no individual trades and no last traded price,
so there is no `fetch_trades` and no candles.

Events, markets and runners are `open`, `suspended` or `closed`, an event also
`graded`; a runner `paid` once its bets are settled, and a withdrawn runner
(a non-runner) carries `withdrawn: true`. Suspended is still `open` here, with
`active=False`. A withdrawn runner is `settled` with `info["result"]` `void`;
Matchbook publishes no other result, so a closed market is `closed`. Only open
events are listed, each with its markets; a closed market is read by id. A
total or handicap on a whole line (`2`, `-1`) can push and an Asian line
(`-0.5/1.0`) can half-win, so those runners are `market_type="unknown"`;
half-point lines are binary. The line is `info["line"]` (`info["asian_handicap"]`
for an Asian one).

Commission is 2% of an account's net winnings in each Matchbook market at
settlement for customers in the UK, Ireland, the Channel Islands and the Isle
of Man, and 4% elsewhere, by Matchbook's terms (`net_market_winnings`); pass
`commission=0.04` if that is yours. API and commercial accounts can be on
volume commission instead; each offer names its `commission-type`.

**No account for market data.** Every read here is an anonymous GET. The read
adapter sends a session only when given one (`credentials=`, `session_token=`,
or `login=True` to read `MATCHBOOK_USERNAME` and `MATCHBOOK_PASSWORD`), since
a session counts the reads against the account. Betting logs in; see
[trading](trading.md#matchbook).

**What it costs.** Matchbook rate-limits each account or IP by group: events
700 requests a minute, navigation 100, reports 40, account 300, betting
writes 3,000. Past a limit it blocks the caller for ten minutes. On its API
plans every GET is chargeable, £100 per million in a calendar month; writes
are free at a reasonable rate. Reads here are batched (twenty events a
request, every market and price in them) to keep the count down.

**What you may do with it.** Matchbook's terms allow viewing prices and
betting on your own account. Redistributing its prices, statuses or event data
to third parties, or showing them in a public price comparison, needs
Matchbook's written consent.

### ProphetX

ProphetX (formerly Prophet Exchange) is a US peer-to-peer sports exchange,
read the way SX Bet is. A ProphetX *market* at one strike (a moneyline; a
spread or total at one number) has two sides, and each side is its own
buy-only instrument with its own `strike_id`. That line is a Synpath market:
its first side is YES and its second NO, under the venue's names
(`market.yes.label`, `market.no.label`), and its id is the game and the YES
side's strike, `prophetx:44285780-90de769f4f4977a04446ed203e957351`. The game
is its event, `prophetx:44285780`. Both strike ids are on the outcomes'
`venue_token_id` and in `info["yes_strike_id"]` and `info["no_strike_id"]`;
`info["strike"]`, `info["type"]` and `info["sub_type"]` say which line it is.
A market with other than two sides (a three-way soccer moneyline) is not
binary and is left out. A spread or total can push, returning the stake.

Prices are American odds. A side priced `-A` risks A to win 100, so its
implied probability is A/(A+100); one priced `+A` risks 100 to win A, so
100/(A+100); `+100` is one half. The conversion is exact (a fraction,
`synpath.prophetx.implied_probability`) and read prices carry it to eight
places. Each side's levels are what a taker can buy, built from the orders
resting on the other side: side one's are YES **asks** at their probability,
side two's are NO asks, which are YES **bids** at one minus theirs. A level's
`value` is the USD a taker can stake at it, and a stake S at probability P
returns S/P, so a level holds `value / P` contracts paying 1 each. ProphetX
lists each side's ten best levels; its own levels stay in the book's
`info["yes"]` and `info["no"]`.

ProphetX lists only games not started or live, so listings take `status="open"`
and nothing else, and a game that has ended leaves the catalog. A game
`suspended`, `delayed` or `interrupted` is still `open` here, with
`active=False`. Tournaments (leagues) are walked in ProphetX's order;
`tournament_id=` reads one. There is no search route: `query` matches event,
tournament and competitor names over the open games. There are no trades or
candles over REST: fills are broadcast on the WebSocket, and
`ProphetXMarketStream.watch_trades` reads them. ProphetX charges a fee per
order, reported on orders and fills; its API documentation publishes no rate,
only a maker rebate on monthly maker volume, so there is no fee schedule.

**Every call needs a key.** Two kinds read market data. The Trading API key
pair (`PROPHETX_ACCESS_KEY`, `PROPHETX_SECRET_KEY`) is generated by any
KYC-approved account under API Integration on prophetx.co (the secret is
shown once); it is exchanged at `/auth/login` for a bearer token that lasts
ten minutes and is renewed with the refresh token, and it also trades and
streams. ProphetX counts live sessions per access key and a second login adds
one, so one process should share one `ProphetXSession` (the trading adapter
shares its own with its catalog and streams), and two processes should use two
key pairs. A Market Data API key (`PROPHETX_AFFILIATE_KEY`) is issued by
ProphetX's market data team on request (jordan.grzeczka@prophetexchange.com);
it reads market data only, from the `/affiliate` routes. With both set the
market data key reads market data. `PROPHETX_SANDBOX=1` uses ProphetX's
sandbox (api.sandbox.prophetx.dev, demo funds), whose keys are made on
sandbox.prophetx.dev. A self-service key starts on ProphetX's default rate
tier, one request a second per endpoint, which the adapter keeps to.

### Azuro

Azuro is an onchain sports betting protocol. Every bet is taken by a shared
liquidity pool at the odds the protocol quotes, so there is no order book and
nothing rests. The feed (`api.onchainfeed.org`) lists sports, games and their
*conditions* ("Match Winner", "Total Goals Over/Under 2.5", "Correct Score"),
each with two or more outcomes, exactly one of which wins, or the condition is
cancelled and every bet returned.

A two-outcome condition is one market, `azuro:<conditionId>`: the outcome with
the lower `outcomeId` is YES and the other NO, under the venue's names for
them, so betting the second outcome is selling YES. A condition with three or
more outcomes (1X2, double chance, correct score) is one market per outcome,
`azuro:<conditionId>-<outcomeId>`, answering "does this outcome win?"; only its
YES can be bought, since the pool sells no "not this outcome". The game is the
event, `azuro:<gameId>`, for every condition on it.

Prices are the pool's decimal odds `O` as `1 / O`, rounded to 8 decimal places
(odds of 1 or less, and outcomes not taking bets, are no price). A contract
pays one token. On a two-outcome market the YES ask is `1 / O` of the YES
outcome and the YES bid one minus `1 / O` of the other, so the pool's margin is
the spread; one outcome of a larger condition has an ask only. The venue's
odds stay on `info["odds"]` and on each outcome's `info`. Depth is the pool's
limit on a selection: `fetch_limits(market_id, side=)` reads `/bet/calculation`
(`min_bet` and `max_bet` in the bet token, `max_payout` the most contracts it
sells), and `fetch_order_book` is the quote as one level a side sized by it.

The protocol runs a pool per chain: Polygon (USDT, six decimals) and Base
(WETH, eighteen), with the Polygon Amoy and Base Sepolia testnets; Gnosis and
Chiliz are discontinued and every condition there reads stopped. A game and its
conditions have the same ids on every chain, so ids carry no chain;
`Azuro(chain="base")` reads the Base pool's states and odds, and its contracts
pay out in WETH. Listings are open games only, live first, then upcoming by
start time, and take `sport` and `league` (the feed's slugs). `fetch_sports()`
returns the feed's sports, countries and leagues with their open game counts.

The tape is every single bet on a condition, from Azuro's bets subgraph
(`thegraph.onchainfeed.org`), in the YES price with the bettor's side; combo
bets are left out. There is no fee on top of the odds (`pool_margin`); a bet
placed through the relayer pays it a flat fee per bet in the bet token, which
`AzuroTrading.fetch_fee_estimate` reads.

The feed's market routes and the subgraph answer from anywhere; its bet,
cash-out and stream routes (`/bet/*`, `/cashout/*`, the WebSocket) answer some
regions with a 503, so `fetch_limits`, `fetch_order_book` and the stream need a
region Azuro serves.

### Myriad

Myriad is two venues behind one API. *Order-book* markets (BNB Chain, in USD1)
trade EIP-712 orders that the venue's matcher settles on chain; *AMM* markets
(BNB Chain in USD1 or USDT, Abstract in USDC.e, Linea in USDC) trade against a
fixed-product pool, one contract call per trade. Both are read here: every
listing asks the venue for both kinds (its default is AMM only), and
`trading_model="ob"` or `"amm"` narrows it. Markets in Myriad's points token
(PTS) are not money and are left out, and Celo, where every market is in
points, is not read.

The two kinds number their markets separately, so the same number on BNB
Chain is two unrelated markets; the id carries the network and the kind:
`myriad:56-2385` (order book), `myriad:56-amm-2385` (pool). A market's slug
works wherever an id does. An order-book market is binary, outcome 0 YES and
outcome 1 NO, under the venue's own names (`Patriots` and `Bears` on a game);
the venue's events (`myriad:<uuid>`) group several, mutually exclusive when the
venue marks them neg-risk. A two-outcome pool is one binary market, outcome 0
YES; a pool with more outcomes is an event of its own, one binary market per
outcome (`myriad:2741-amm-412-3`), NO being the rest of the pool. Many markets
name the Polymarket market they follow (`info["polymarket_ids"]`). Sports
markets carry the venue's scoreboard (`info["scoreboard"]`), whether they are
in play, and their teams and leagues as tags; `fetch_markets(topic="sports",
tag="nfl", in_play=True)` filters at the venue.

Order-book prices are whole cents and sizes 18-decimal shares. The venue
serves each outcome's book with what the other outcome's orders imply already
in it (a NO bid at 0.94 is a YES ask at 0.06), so `side="no"` reads outcome 1's
own book. A pool has no orders: its book is built from the pool's balances,
one level per cent of price, each the shares a trade moving the price into that
cent would fill at that slice's average price, fees included
(`info["depth_source"] == "amm"`). The quote on a pool market is its price as
`mid`, and the marginal buy and sell prices, after fees, as `ask` and `bid`.

The order-book tape lists every leg of a match, the taker's and each maker's;
only a taker pays a fee, so each taker leg is a trade here, and a match where
no leg paid one is reported once with its side unknown. Splits and merges of
positions are not trades and are left out. A pool's tape is its buys and sells.
Candles are the venue's price samples per outcome: 5-minute over the last day,
30-minute over a week, 4-hour beyond.

Fees: an order-book taker pays a share of the notional that depends on the
price, from the market's own table of basis points by cent (3 bps at 1c, 180
at 50c; `bps_by_price`); a maker pays nothing. A pool charges a share of the
value traded on each side, set per market (`share_of_value`), often zero.

No key for market data, at 30 requests per 10 seconds; an API key and its
secret (`Myriad(api_key=..., api_secret=...)`) raise that to 200 a second, and
then every request is HMAC-signed, since the venue refuses a bare key. Check
Myriad's terms for where it may be used before trading.

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

| Method | `kalshi` | `polymarket` | `polymarket_us` (retail) | `polymarket_us` (exchange) | `opinion` | `hyperliquid` | `predict_fun` | `limitless` | `sx_bet` | `novig` | `matchbook` | `prophetx` | `azuro` | `myriad` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `create_order`, `create_orders` | yes | yes | yes | yes | one at a time; `market` and `ioc` are limits with the rest cancelled | yes, a batch is one signed action; `market` and `ioc` are `Ioc` limits | one at a time; `market`, `ioc` and `fok` are the venue's `MARKET` strategy at the worst price given | one at a time; `market` and `ioc` are the venue's fill-and-kill at the worst price given | yes, ten a request; `market` and `ioc` are the venue's `IOC` at the worst price given, `fok` its `FOK` | yes, 256 a request, all or nothing; queued (`pending`) until the private stream says it rests; `market` and `ioc` are `IOC` at the worst price given, `fok` `FOK`, post-only `PO` | yes, fifty a request; `market` and `ioc` are offers at the worst price given with the rest cancelled (after the in-play delay); no `fok`, `gtd` or post-only | one at a time (the batch route is off on the default tier); answered before matching, so `pending`; `market` and `ioc` are the venue's `fillOrKill` at the worst price given, `post_only` its `ALO`; no `fok` or `gtd` | one at a time; `market`, `ioc` and `fok` are a bet with minimum odds `1 / price`, taken whole by the pool or refused | yes, 200 a request; `market` and `ioc` are the venue's `FAK` at the worst price given, `fok` its `FOK`, post-only its `PO`; order books only |
| `cancel_order`, `cancel_orders`, `cancel_all_orders` | yes | yes | yes | yes | one at a time; `cancel_all_orders` lists then cancels | yes; `cancel_all_orders` is one action | yes, up to 100 a request, off the book (not on chain) | yes, up to 50 a request; `cancel_all_orders` per market | yes, up to 100 a request; `cancel_all_orders` per market or account-wide | yes, up to 256 a request; `cancel_all_orders` per market or the whole subaccount | yes, fifty a request; `cancel_all_orders` per runner, market or account-wide | one at a time; `cancel_all_orders` per line (both sides) or account-wide, in the background | no: a signed bet cannot be cancelled; `cash_out` where the venue offers it | yes, up to 200 a request, each repeating the signed order; `cancel_all_orders` one signed message, per market or account-wide |
| `edit_order` | yes, amend or decrease | no; an edit is a cancel and a new order | yes, price, quantity and time in force | yes | no | no | no | no | no | no | yes, odds and remaining stake in place | no | no | yes, the venue's atomic cancel-and-place (the queue place is lost) |
| `fetch_order`, `fetch_open_orders` | yes | yes | yes | yes | yes | yes | yes | yes | yes; any order by id, in any state | yes; any order by id, in any state | yes, while the market is unsettled | yes; reads lag placing by seconds | yes; open orders are bets not yet on chain | yes |
| `fetch_orders` (by status or time) | yes | no | no | yes, rationed to 12 requests a minute | yes, by status | partial: the venue's last 2000, filtered here | yes, open or filled | partial: per market, open or closed | no; the venue lists only resting orders | yes, by status | partial: offers on unsettled markets, by status | yes, by status and creation time, sixty days a read | yes, the wallet's bets, open or closed | yes, by status |
| `fetch_my_trades` | yes | yes | no; the activity feed carries no order id | yes | yes, confirmed fills | yes, from a time with `since` | yes, settled matches | yes, settled fills from the account's history | yes, the account's fills | yes, the subaccount's fills, each with its fee | partial: the matched bets on unsettled markets | yes, the account's fills, or one order's | yes, accepted bets, one fill each | partial: the account's history names neither the order nor the fee |
| `fetch_positions`, `fetch_balance` | yes | yes | yes | yes | yes | yes, the spot side's USDC and outcome coins | yes, USDT on chain less what orders hold | yes, USDC on Base less what orders hold | yes, the proxy wallet's USDC, free and escrowed | yes, both outcomes' holdings netted on YES; the subaccount's USD balance | yes, current matched bets netted per runner; the account's balance, exposure and free funds | partial: positions built from the live orders' matched stakes; the wallet's USD, free and locked | yes, accepted unsettled bets netted on YES; the wallet's bet token on chain | yes, USD1 on BNB Chain less what open buys hold; positions on books and pools |
| `fetch_settlements` | yes | yes, from the redemption activity | yes | no | no | no | no | no | no | no | yes, from the settled-bets report | no | yes, settled bets: won, lost or cancelled | no |
| `fetch_queue_position` | yes | no | no | no | no | no | no | no | no | no | no | no | no | no |
| `fetch_fee_estimate` | yes | yes | yes | yes | yes, from the fee contract, 0.25 USDT minimum | yes, the closing charge; opening fills pay nothing | yes, `rate * min(p, 1 - p)` a share | yes, the venue's buy-fee curve on the token bought | yes, 1% of a winning taker bet's profit | yes, the market's coefficient, and the maker credit as a negative fee | yes, commission on what the bet wins, makers and takers alike | no: no published rate | yes, the relayer's flat fee per bet | yes, the market's taker table at the price of the outcome bought |
| `rfq` | yes | no | no | no | no | no | no | no | no | no; Novig's parlay RFQ for liquidity providers is not supported yet | no | no | no | no |
| `split_merge` | no; contracts are not tokens | partial: Deposit Wallets and EOAs | no | no | no | no | no | no | no | no | no | no | no | no; `position_calldata()` returns the venue's calldata for the wallet to send |

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
| `NovigMarketStream` | yes, per order, with a trading key; partial without one: the public book polled every 5 s | yes; partial when polled | yes; partial when polled | yes; partial when polled | no | no | no | no |
| `NovigUserStream` | no | no | no | no | yes | yes | yes | no |
| `MatchbookMarketStream` | partial: polled from REST, every 5 s by default | partial: polled | no: no public trades | partial: polled | no | no | no | no |
| `ProphetXMarketStream` | yes, ten best levels a side, seeded from REST | partial: top of book | yes, the tournament's tape of fills | no | no | no | no | no |
| `ProphetXUserStream` | no | no | no | no | yes | yes | no | no |
| `AzuroMarketStream` | partial: the pool's quote as one level a side, sized by its limits read over REST | yes, every change of odds | no: read the bets with `fetch_trades` | yes, per condition | no | no | no | no |
| `MyriadMarketStream` | yes, order books, both outcomes | yes | yes | partial: markets created and resolved | no | no | no | no |
| `MyriadUserStream` | no | no | no | no | yes | yes | partial: one outcome's balance per update | no |

The exchange API's other gRPC streams carry one thing each and say so:
drop copy and trade capture carry fills, position and position-change carry
positions, instrument state carries market status, and the balance ledger
carries account movements as venue events.

---
