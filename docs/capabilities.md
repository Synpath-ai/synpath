# Capabilities

Back to the [README](../README.md).

Read `has` before you call. A capability a venue lacks raises `NotSupported`
rather than returning an empty list, so "cannot" is never mistaken for "there
is nothing".

| Method | `kalshi` | `polymarket` | `polymarket_us` | `opinion` | `hyperliquid` | `predict_fun` |
|---|---|---|---|---|---|---|
| `fetch_markets` | yes | yes | yes | yes, categorical topics flattened into their options | yes, every outcome of a question is a market, the fallback ("Other") included | yes, quoted from the listing |
| `fetch_events` | yes | yes | yes | yes, one per topic | yes, one per question; a standalone outcome is its own event | yes, one per category |
| `fetch_market` | yes | yes | yes | yes; a categorical option costs a second read for its topic | yes, from the cached catalog | yes |
| `fetch_order_book` | yes | yes | yes | yes | yes, top 20 levels a side; NO mirrored from YES | yes; NO mirrored from YES |
| `fetch_order_books` (batched) | yes, one request per market | yes, one round trip | yes, one request per market | yes, one request per market | yes, one request per market | yes, one request per 50 |
| `fetch_trades` | yes | yes | no public tape over REST; the last trade is on the book | no public tape; the last trade is on `refresh_quotes` | partial: the venue's last few prints only, no history | yes, the venue's matches |
| `fetch_ohlcv` | yes, the venue's candles | yes, built from the trade tape, both tokens folded into the YES price; the venue's quote samples via `source="quotes"` | partial: quote-derived bid/ask midpoints, no volume; no public tape to build from | partial: last-trade samples, hourly or daily, no volume | yes, the venue's traded candles with volume; 6h built from 2h | partial: the venue's probability samples, no volume |
| `fetch_series` | yes | no series tier | yes, but fees are per market, not per series | no series tier | no series tier | no series tier |
| `fetch_fee_schedule` | yes | yes | yes | yes, read from the venue's fee contract on BNB Chain | yes, at the lowest volume tier; charged on closing fills only | yes, taker only |
| `search` | yes ([undocumented host](api.md#search)) | yes | yes | partial: no venue search; titles matched here over 25 catalog pages | yes, matched here over the whole catalog | yes, at the venue |
| `fetch_markets_by_ids` | yes | yes | yes | yes, one request per market | yes, one catalog read | yes, one request per market |
| `sort` | yes, the page is ordered after it is read | yes, at the venue | yes, the page is ordered after it is read; `volume` and `liquidity` cost one read per market | partial: `volume` and `newest` at the venue; no liquidity figure exists | partial: `volume` and `newest`; no liquidity figure exists | partial: `volume` only |
| `match_market` / `match_event` | no | no | no | no | no | no |

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

| Method | `kalshi` | `polymarket` | `polymarket_us` (retail) | `polymarket_us` (exchange) | `opinion` (not yet live-tested) | `hyperliquid` (testnet-checked) | `predict_fun` (not yet live-tested) |
|---|---|---|---|---|---|---|---|
| `create_order`, `create_orders` | yes | yes | yes | yes | one at a time; `market` and `ioc` are limits with the rest cancelled | yes, a batch is one signed action; `market` and `ioc` are `Ioc` limits | one at a time; `market`, `ioc` and `fok` are the venue's `MARKET` strategy at the worst price given |
| `cancel_order`, `cancel_orders`, `cancel_all_orders` | yes | yes | yes | yes | one at a time; `cancel_all_orders` lists then cancels | yes; `cancel_all_orders` is one action | yes, up to 100 a request, off the book (not on chain) |
| `edit_order` | yes, amend or decrease | no; an edit is a cancel and a new order | yes, price, quantity and time in force | yes | no | no | no |
| `fetch_order`, `fetch_open_orders` | yes | yes | yes | yes | yes | yes | yes |
| `fetch_orders` (by status or time) | yes | no | no | yes, rationed to 12 requests a minute | yes, by status | partial: the venue's last 2000, filtered here | yes, open or filled |
| `fetch_my_trades` | yes | yes | no; the activity feed carries no order id | yes | yes, confirmed fills | yes, from a time with `since` | yes, settled matches |
| `fetch_positions`, `fetch_balance` | yes | yes | yes | yes | yes | yes, the spot side's USDC and outcome coins | yes, USDT on chain less what orders hold |
| `fetch_settlements` | yes | yes, from the redemption activity | yes | no | no | no | no |
| `fetch_queue_position` | yes | no | no | no | no | no | no |
| `fetch_fee_estimate` | yes | yes | yes | yes | yes, from the fee contract, 0.25 USDT minimum | yes, the closing charge; opening fills pay nothing | yes, `rate * min(p, 1 - p)` a share |
| `rfq` | yes | no | no | no | no | no | no |
| `split_merge` | no; contracts are not tokens | partial: Deposit Wallets and EOAs | no | no | no | no | no |

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

The exchange API's other gRPC streams carry one thing each and say so:
drop copy and trade capture carry fills, position and position-change carry
positions, instrument state carries market status, and the balance ledger
carries account movements as venue events.

---
