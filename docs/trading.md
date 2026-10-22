# Trading

Back to the [README](../README.md).

Order entry is part of the same package, importable from `synpath` directly, one async adapter per venue API:
`KalshiTrading`, `PolymarketTrading` (the global CLOB V2),
`PolymarketUSTrading` (Polymarket US's retail API) and
`PolymarketUSExchangeTrading` (its exchange API for onboarded firms). Each
does what its venue does natively and says so in `has`; stops, icebergs and
the other synthetic orders belong to the execution engine
([docs/engine.md](engine.md)), which holds them and submits venue orders as
children.

| Adapter | Built against |
|---|---|
| Kalshi | the venue's V2 API, exercised on its demo environment |
| Polymarket | the venue's own client (`py-clob-client-v2`), matched byte for byte on signing, and the published CLOB V2 documentation |
| Polymarket US (both APIs) | the venue's published OpenAPI and AsyncAPI schemas and its SDK |

## Install

```bash
pip install synpath
```

Order entry ships in the base package. The install carries the venues' signing
stacks: Ed25519 or RSA-PSS for Kalshi, EIP-712 for Polymarket, Opinion, Hyperliquid, predict.fun, Limitless and SX Bet,
Ed25519 and private-key JWT for Polymarket US. Nothing further is needed to place an order.

Polymarket and Opinion orders are hashed and signed by the Rust core
(`synpath._core`), in about 20 microseconds rather than the 3 milliseconds
`eth_account` takes; the signatures are the same bytes, pinned against the
venues' own signing vectors. The key stays in the process either way, and
`SYNPATH_PURE_PYTHON=1` signs with `eth_account` instead.

## Credentials

Credentials come from the environment, or from a git-ignored `.env` next to
the caller (see `.env.example`). They are loaded once into objects whose
`repr` shows nothing secret, and every secret loaded is registered with a
logging filter so that a stray `%r` prints `***` rather than a private key.

```bash
synpath init              # asks for each venue's keys, writes .env readable by you only
synpath doctor
```

`synpath init` checks each value as it is typed (a Kalshi key file that is
PEM, a Polymarket key and wallet address of the right shape), hides secrets
while they are typed, and updates an existing `.env` in place. Writing the
file by hand from `.env.example` works the same.

`doctor` reports, per venue, whether credentials loaded, which environment
they point at and the public half of the identity. It prints no secret. A
venue set up with only its API key shows as `market data`: `synpath serve`
streams its books, and placing an order there asks for the private key.

### What each venue needs

A private key is 64 hex characters (optionally `0x`-prefixed) exported from
the wallet; a wallet address is `0x` and 40 hex characters and is never the
private key. `synpath init` tells them apart.

| Venue | Market data | Trading | Where to get them |
|---|---|---|---|
| Kalshi | REST: nothing. Books over the WebSocket: the API key (its handshake is signed) | the API key: a Key ID and its private key file (PEM, Ed25519 or RSA) | kalshi.com/account/profile -> API Keys -> Create New API Key (demo.kalshi.co for demo); the key file is shown once |
| Polymarket | nothing | the wallet's private key and the trading wallet address (the funder); CLOB API credentials are derived from the key | polymarket.com -> Settings -> Private Key -> Start Export; the address from the profile menu |
| Polymarket US | REST: nothing. Books over the WebSocket: the API key | the API key: a Key ID and a base64 Secret Key, no wallet | polymarket.us/developer, after identity verification; the secret is shown once |
| Opinion | the Open API key | the API key and the private key of the wallet connected on opinion.trade | `synpath init` creates the key by signing with the wallet (`POST /auth/api-key`); one key per wallet |
| Hyperliquid | nothing | a private key: an API wallet's (it signs but cannot withdraw) with the account's address, or the account's own | app.hyperliquid.xyz/API (More -> API) |
| predict.fun | mainnet: the API key (`pred_sk_...`). Test network: nothing | the API key and a private key: a plain wallet's, or a Predict account's owner key with its deposit address | the key at developers.predict.fun; a web-app account's owner key under Account -> Settings -> Privy wallet |
| Limitless | nothing | the private key of the wallet connected on limitless.exchange (EOA trading mode) and a scoped API token: id and base64 secret | profile menu -> Api keys -> API Tokens -> Derive; the secret is shown once |
| SX Bet | REST: nothing. Books over the WebSocket: the API key (every channel needs it) | the API key alone reads the account (orders, fills, positions, balance) and cancels; placing an order also needs the private key of the wallet you sign in with (the account's own, not its proxy wallet). The account stream needs the wallet address: from the private key, or `SX_BET_ACCOUNT_ADDRESS` | sx.bet -> Account -> Overview -> API Credentials -> Generate API key (toronto.sx.bet for the test network); shown once |

For Opinion, predict.fun and SX Bet the API key alone is a complete
market-data setup: `synpath init` asks for it first and takes an empty
private key, writing only `OPINION_API_KEY`, `PREDICT_FUN_API_KEY` or
`SX_BET_API_KEY`. SX Bet's key goes further: the venue authorizes the
account's reads, every cancel and the heartbeat by the key, so `synpath
serve` shows that account's balance, positions and orders and cancels them
with the key alone; only placing an order asks for `SX_BET_PRIVATE_KEY`. Opinion's key is created by signing with the wallet, so
making one needs the private key once; a user who has one already enters it
alone.

The variables each venue reads:

| Venue | Variables |
|---|---|
| Kalshi | `KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH` (the PEM file, Ed25519 or RSA, the BEGIN…END block only), `KALSHI_ENV` = `prod` (the default, real money) or `demo` (Kalshi's practice exchange) |
| Polymarket | `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_SIGNATURE_TYPE` (3 Deposit Wallet, used by every account created since May 2026 and what `synpath init` suggests; 1 proxy, 2 Safe, 0 allowlisted EOA, which is also what an unset value means), `POLYMARKET_FUNDER` (the wallet address, required for 1-3); optional `POLYMARKET_API_KEY` / `_API_SECRET` / `_API_PASSPHRASE` (derived when absent), `POLYMARKET_BUILDER_CODE`, `POLYMARKET_RELAYER_API_KEY` / `_RELAYER_API_KEY_ADDRESS` (gasless wallet transactions), `POLYMARKET_RPC_URL` (an EOA's own transactions) |
| Polymarket US, retail API | `POLYMARKET_US_KEY_ID`, `POLYMARKET_US_SECRET_KEY` (from polymarket.us/developer) |
| Polymarket US, exchange API | `POLYMARKET_US_CLIENT_ID`, `POLYMARKET_US_PRIVATE_KEY_PATH`, `POLYMARKET_US_PARTICIPANT_ID`, `POLYMARKET_US_ACCOUNT` (optional), `POLYMARKET_US_ENV` = `preprod` or `prod` |
| Opinion | `OPINION_API_KEY` (market data and trading; `synpath init` creates it by signing with the wallet), `OPINION_PRIVATE_KEY` (trading; the wallet connected on opinion.trade), `OPINION_MULTISIG_ADDRESS` (optional; the account's Safe, read from the venue when absent) |
| Hyperliquid | `HYPERLIQUID_PRIVATE_KEY` (an API wallet's key is safest: it trades but cannot withdraw), `HYPERLIQUID_ACCOUNT_ADDRESS` (the account an API wallet trades for; not needed with the account's own key), `HYPERLIQUID_TESTNET` (`1` for the test network) |
| predict.fun | `PREDICT_FUN_API_KEY` (mainnet market data and trading; from developers.predict.fun), `PREDICT_FUN_PRIVATE_KEY` (trading; a plain wallet's key, or a Predict account's owner key: the Privy wallet exported under Account -> Settings), `PREDICT_FUN_ACCOUNT_ADDRESS` (the Predict account's deposit address; unset for a plain wallet), `PREDICT_FUN_TESTNET` (`1` for the test network), `PREDICT_FUN_RPC_URL` (optional; the BNB Chain node the balance is read from) |
| Limitless | `LIMITLESS_PRIVATE_KEY` (the wallet connected on limitless.exchange, in EOA trading mode), `LIMITLESS_API_TOKEN_ID` and `LIMITLESS_API_SECRET` (a scoped API token derived on limitless.exchange: profile, API tokens, Derive), `LIMITLESS_RPC_URL` (optional; the Base node the balance is read from) |
| SX Bet | `SX_BET_API_KEY` (market data streams, reading the account, cancels and the heartbeat; generated on sx.bet under Account -> Overview -> API Credentials), `SX_BET_PRIVATE_KEY` (optional; placing orders only: the wallet you sign in to sx.bet with, the account's own key, not its proxy wallet's), `SX_BET_ACCOUNT_ADDRESS` (optional; the wallet's public 0x address, not its private key, which names the account stream's channels when no private key is set; the venue documents no route that returns a key's owner), `SX_BET_TESTNET=1` (optional; the Toronto test network) |

Betfair's variables (`BETFAIR_APP_KEY`, `BETFAIR_USERNAME`, `BETFAIR_PASSWORD`,
optional `BETFAIR_CERT_PATH` / `BETFAIR_KEY_PATH` and `BETFAIR_JURISDICTION`)
load the same way and `synpath doctor` reports them, but they serve market data
and the stream only: there is no Betfair order entry yet. See
[Betfair](capabilities.md#betfair).

Smarkets needs no credentials for market data. `SMARKETS_USERNAME` and
`SMARKETS_PASSWORD` are optional: they log in for undelayed quotes, and
`synpath doctor` reports them. There is no Smarkets order entry yet: betting
through Smarkets' API needs its approved API access (applied for, with a
one-off £150 fee). See [Smarkets](capabilities.md#smarkets).

## Through your own server

The engine's order types (stops, icebergs, TWAP, orders on a bucket) live in
your self-hosted server (`synpath serve`), not at a venue. Point
`synpath.Client(server=...)` at it and the same calls go there. Setup, the
access token and the three kinds of credentials are on
[synpath.dev](https://synpath.dev/docs/quickstart/local-server).

## Contracts are `quantity`

A number of contracts is `quantity` everywhere: `OrderRequest.quantity`,
`Order.quantity` and `last_fill_quantity`, `Fill.quantity`,
`Trade.quantity`, `Precision.min_quantity` and `quantity_step`, and every
argument that takes a number of contracts. Before 0.8 it was `amount`. For
this release the old names still work -- as model input, as a read-only
attribute and as a keyword argument -- with a `DeprecationWarning`; they are
removed in the next release. JSON from the server is `quantity` only.

## Money is `Decimal`

Prices and amounts that will be signed are `Decimal`, checked against the
instrument's precision before anything is sent, and rendered into the
venue's wire format only at the boundary. A price off the tick is refused
with the two nearest ticks named, not rounded to one of them.

```python
from decimal import Decimal
from synpath.trading import money, Precision

precision = Precision(tick=Decimal("0.001"))
money.validate_price(Decimal("0.107"), precision)     # ok
money.validate_price(Decimal("0.1065"), precision)    # InvalidOrder: not on the 0.001 tick
```

Venue rules the payloads do not state are applied here: Kalshi counts
contracts in hundredths; Polymarket shares carry two decimals and each
market has its own minimum (usually five); Polymarket US markets set their
own minimum quantity, which is one whole contract on every live market today
but may be fractional.

## Types

`OrderRequest` → `Order`, `Fill`, `Position`, `Settlement`, `Balance`,
`FeeEstimate`, and `Account`. Field names follow ccxt where ccxt has them
and FIX where it does not: one `status`, separate `filled` and `remaining`,
and named `pending_cancel` / `pending_replace` states for the race where a
fill lands while a cancel is in flight.

Three venue disagreements are carried explicitly:

- **Exposure versus inventory.** Kalshi and Polymarket US net a position;
  Polymarket holds two token inventories until they are merged. `Position`
  has both `contracts` and `inventory`.
- **Fill finality.** A Polymarket fill is `matched` before the chain
  `confirmed` it, and can `failed` in between. `Fill.settlement` says which.
- **Buying power.** Polymarket US computes it; the others lock full cost.
  `Balance.buying_power` is the venue's figure or `None`, never zero.

Balances are per venue account and never pooled: a consolidated figure is a
roll-up on read, not something an order can spend from.

## The budget

`synpath.trading.limiter.BudgetLimiter` meters reads and writes separately,
the way Kalshi does (an order costs 10 write tokens on a budget set by
account tier). A cancel or a kill switch takes the fast lane and goes at
once; the normal callers already waiting are pushed back by what it took. A
call whose queue wait would exceed its deadline raises
`RateBudgetExceeded` before anything is sent, because a stop that fires late
is worse than one that fails loudly.

## Capabilities

Every read adapter answers the trading keys in `has` with `False`. A
trading adapter (`synpath.trading.TradingExchange`) is a separate class from
the venue's read adapter -- it holds credentials and a signed async
transport the read side has no use for -- and its `has` is completed by the
same rule, so every key answers `True`, `False` or `"partial"` on both.
Anything the execution engine builds on top (a stop, an iceberg, a bracket)
is the engine's capability, reported by the engine, never claimed by a
venue adapter.

## Kalshi

```python
import asyncio
from decimal import Decimal
from synpath import KalshiTrading, OrderRequest, EditRequest, Side, TimeInForce, load_credentials, require

async def main():
    creds = require("kalshi", load_credentials())        # KALSHI_* from the environment or .env
    async with KalshiTrading(creds) as kalshi:
        await kalshi.fetch_limits()                       # adopt the account's real token budget
        print(await kalshi.fetch_balance())
        order = await kalshi.create_order(OrderRequest(
            market_id="kalshi:KXBTCD-26SEP1717-T115000", side=Side.SELL,   # sell takes NO
            quantity=Decimal("5"), price=Decimal("0.70"),                     # at the YES price
        ))
        order = await kalshi.edit_order(EditRequest(order_id=order.id, quantity=Decimal("3")), current=order)
        await kalshi.cancel_order(order.id, market_id=order.market_id)

asyncio.run(main())
```

**One leg, everywhere.** An order names a market and a side on the YES leg:
`buy` takes YES, `sell` takes NO, and `price` is always the YES price, so
"buy NO at 0.30" is written `side="sell", price=0.70`. Kalshi itself quotes
the YES leg only, so that is literally the order sent, and how the venue
reports it back. The request as you wrote it is kept in `order.info["request"]`.

**What the venue holds.** Limit orders, with `gtc`, `ioc`, `fok`, and `gtd`
(`gtc` plus an expiry). There is no native market order: `type="market"`
is sent as an immediate-or-cancel limit at the `price` you give, which is
the protection price, and a market order without one is refused before
anything is signed. `day` never reaches the adapter -- the engine rewrites
it to `gtd` at the session end. Stops, icebergs and the rest are the
engine's.

**Edits.** `edit_order` is a *decrease* when only the quantity comes down,
which keeps the order's place in the queue, and an *amend* for anything
else, which does not. `Order.queue_priority_preserved` says which one
happened. Pass `current=` when you hold the order; otherwise it is read
first. Time in force cannot be edited: cancel and replace.

**Cancels.** `cancel_order` returns the order as cancelled with whatever
matched first in `filled`. Account-wide `cancel_all_orders()` returns
`None`: the venue acknowledges with no count and works asynchronously,
cancelling orders placed in the following minute too. Per-market
`cancel_all_orders(market_id=...)` is a batch cancel and returns the count.

**Reads lag writes.** The venue's order store can answer 404 for a few
hundred milliseconds after an order is placed. `fetch_order` retries a
miss briefly; the demo tests wait a second between a write and its read.

**The budget.** `fetch_limits()` reads `GET /account/limits` and sets the
limiter to the account's real refill rates (basic tier: 200 read / 100
write tokens a second; an order costs 10, a batch costs 10 per order).
Cancels take the fast lane. Until it is called the limiter assumes the
basic tier.

**Kill switch.** `create_order_group(contracts_limit)` makes a venue-side
order group: a rolling 15-second contract limit that, when breached,
cancels every order in the group and blocks new ones until
`reset_order_group`. Orders join it with
`params={"order_group_id": group}`. `trigger_order_group` pulls the switch
by hand.

**Errors.** A venue refusal is typed: `OrderRejected` with the venue's own
`reason` (`fill_or_kill_insufficient_resting_volume`, ...), `OrderNotFound`,
`InsufficientFunds`, `MarketHalted`. Every `ExchangeError` carries the
venue's parsed `body` and HTTP `status`.

**Combos and RFQ** (`rfq_taker`, `rfq_quoter`, `combos`). A combo is a market
created in a multivariate event collection: an ordinary binary market with a
`KXMVE…` ticker, its own book, positions and settlement, so its id is
`kalshi:KXMVE…`. `fetch_combo_collections()` lists the collections (which
events each takes, always as `associated_event_tickers`, plus
`associated_events`, `size_min`/`size_max`, `active_quoters`).
`create_combo([ComboLeg(market_id=..., outcome="yes"|"no", event_id=...), ...], collection_id=...)`
returns the combo market for those legs, creating it if needed; without
`collection_id` the one open collection that takes every leg's event is used,
and a leg without `event_id` has its market read for it. Several fitting
collections are `InvalidOrder` naming them (and listing them on
`exc.collections`); pass one as `collection_id`. Today's three open
collections (`KXMVESPORTSMULTIGAMEEXTENDED-R`, `KXMVECROSSCATEGORY-R`,
`KXMVECROSSCATEGORY-SHARD1-R`) take the same events, so name one. Creation is signed
and capped at 5,000 a week, and a combo must be created once before it can
be traded or read. `fetch_combo(id)` reads one, legs from `mve_selected_legs`;
there is no lookup by legs that does not create.

An RFQ is two-sided: `create_rfq(market_or_combo_id, quantity=...)` (sent as
`contracts_fp`) or `amount=` (USD, sent as `target_cost_dollars`). By default
the target cost covers principal and Kalshi's fees, so each quote's size
shrinks to fit the fees; `target_cost_excludes_fees=True` makes it principal
only, with the taker fee on top. At most 100 open RFQs, one per market. A
quote is two bids: `RfqQuote.bid` is the maker's `yes_bid`, `ask` is
`1 - no_bid`, a "0" bid a declined side (`None`).
`accept_quote(AcceptQuote(quote_id=, rfq_id=, outcome=, action=, worst_price=))`
sends `accepted_side`, which names the maker's bid you take: buying YES sends
`no` (at `1 - no_bid`), buying NO sends `yes` (at `1 - yes_bid`). It reads the
quote first and refuses, before sending, a quote that is not open, a declined
side, or a price worse than `worst_price`; REST takes the full size only. The
maker confirms within 30 s (3 s on combos), the orders follow, and
`taker_order_id` (`rfq_creator_order_id`) is your order; its fills are in
`fetch_my_trades`. As a quoter: `fetch_incoming_rfqs`,
`send_quote(MakerQuote(rfq_id=, bid=, ask=, post_only=))` (sent as
`yes_bid = bid`, `no_bid = 1 - ask`, the RFQ's full size), `cancel_quote`,
`confirm_quote` and `fetch_my_quotes`. Kalshi has no replace, so
`replace_quote` deletes the old quote and sends the new one: for one round
trip the RFQ has no quote of yours. A quote holds collateral when it is
sent; too little is `InsufficientFunds` (`insufficient_balance`). See
[RFQ and combos](rfq-combo.md).

### Running against the demo

```bash
KALSHI_ENV=demo pytest -m demo tests/test_kalshi_demo.py
```

Needs a demo API key (`KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`) and refuses
to run against `prod`. Every order rests far from the touch and every test
ends with the account flat of resting orders.

---

## Polymarket

```python
import asyncio
from decimal import Decimal
from synpath import PolymarketTrading, OrderRequest, Side, load_credentials, require

async def main():
    creds = require("polymarket", load_credentials())
    async with PolymarketTrading(creds) as poly:
        await poly.ensure_api_credentials()          # derived from the wallet key once
        if not all((await poly.check_approvals()).values()):
            await poly.approve_trading()             # one gasless wallet transaction
        poly.start_heartbeat()                       # resting orders die with the process
        order = await poly.create_order(OrderRequest(
            market_id="polymarket:2252244", side=Side.BUY,   # the YES token, at 0.52
            quantity=Decimal("10"), price=Decimal("0.52"),
        ))
        print(order.id)                              # the order hash, known before sending
        await poly.cancel_order(order.id)

asyncio.run(main())
```

**CLOB V2.** Since 2026-04-28 orders are signed against exchange domain
version `"2"`, carry a millisecond timestamp instead of a nonce, set no fee
(fees are charged at match), and settle in pUSD. The signing module is
checked byte for byte against the venue's `py-clob-client-v2`: every order
signature and hash for the four wallet types on both exchanges, the L1
auth signature, the L2 HMAC, and 1,140 limit-order amount cases.

**Two tokens, one leg.** YES and NO are separate tokens with separate books
and no netting, so an order says which token it trades with `outcome` and
`action`: Buy YES buys the YES token at `price`, Sell YES sells YES tokens
held, Buy NO buys the NO token at `1 - price`, Sell NO sells NO tokens held.
An order with neither takes its side: `buy` buys YES, `sell` buys NO,
because you cannot sell YES you do not hold. Everything the venue
reports on the NO token comes back on the YES leg (a NO buy at 0.30 reads
as a sell at 0.70), and a position is the two inventories netted with both
kept on `inventory_yes` / `inventory_no`. Which token is which side comes
from the Gamma catalog, one read per market, cached.

**What the venue holds.** Limit orders, `gtc`, `gtd`, and `ioc` / `fok`
(the venue's FAK and FOK). A market order is an immediate limit at your
protection price. Post-only for `gtc` and `gtd`. No amend (`edit_order` is
`False`: a change is a cancel and a new signed order), no
client order id -- but the order id is the order's hash, computed before it
is sent. A `gtd` order is sent with the venue's one-minute early expiry
added back, so it expires when you asked, and must live at least two
minutes.

**A match is not a fill yet.** `Fill.settlement` is `matched` until the
chain confirms (`confirmed`) or the trade fails (`failed`). Positions come
from the Data API as token inventories, with `resolved` and `redeemable`
once the market settles.

**Heartbeats.** `start_heartbeat()` sends the venue's dead man's switch every
five seconds from a task on your loop; if it stops for ten seconds, the
venue cancels every order on those credentials. A failed beat is recorded in
`heartbeat_error`, not swallowed.

**Wallet transactions.** `approve_trading`, `split`, `merge` and `redeem`
run through Polymarket's gasless relayer for a Deposit Wallet (with no key
of your own needed; a Relayer API key, if set, is used when Synpath's relayer
access is unavailable) and as signed Polygon transactions for an EOA (an RPC endpoint and
POL for gas). `approve_trading` covers both exchanges and the neg-risk
adapter, which orders on a neg-risk market (several outcomes of one event)
also need. Legacy proxy and Safe wallets: these calls are not built for them,
hence `split_merge = "partial"`, and the exchange now refuses their API
orders too ("please use the deposit wallet flow"). Such an account moves to
its signer's Deposit Wallet (deploy it, move the pUSD, approve trading) and
then trades with `POLYMARKET_SIGNATURE_TYPE=3` and that wallet as
`POLYMARKET_FUNDER`. The funder is the address in polymarket.com's profile
menu, not the deposit address its Deposit window shows. NegRisk conversion is
not built: the V2 documentation publishes no interface for it.

**Errors.** The venue answers in words, not codes; the documented phrases
map to `InsufficientFunds`, `InvalidOrder` (tick, minimum size, expiry),
`MarketHalted` (cancel-only, post-only mode, closed-only accounts) and
`OrderRejected` with a `reason` (`post_only_would_cross`, `fok_not_filled`,
`fak_no_match`, `duplicate`, and for a wallet set up wrongly
`deposit_wallet_required` or `funder_not_signers_wallet`, each with what to
change).

**Combos and RFQ** (`rfq_taker`, `rfq_quoter`, `combos`). A combo is 2–50
leg outcomes of combo-enabled markets with its own YES/NO pair, traded only
by RFQ and settled through **Exchange v3** (`0xe3333700…00Aa`), not the CLOB.
No credential beyond the CLOB API key is needed: both RFQ gateways take the
same L2 headers. The requester side refuses EOA wallets
(`UNSUPPORTED_REQUESTER_SIGNATURE_TYPE`, also checked before anything is
sent): request from a Deposit Wallet (3), proxy (1) or Safe (2). Quoting
works for all four types. Quoting with last look also needs the venue's sign-off.

Approvals, once per wallet, from the wallet (`maker_address`), with
`approve_combo_trading(include_router=False, include_auto_redeem=False)`:

| Approval | Who needs it | Flag |
|---|---|---|
| pUSD `approve(Exchange v3)`, PositionManager `setApprovalForAll(Exchange v3)` | quoters, when the quoted order moves pUSD or combo positions | always sent |
| pUSD `approve(Router)`, PositionManager `setApprovalForAll(Router)` (`0x12121212006e4CD160D18e3f00711DA5c3372600`) | anyone splitting pUSD into positions, or managing or redeeming them through the Router | `include_router=True` |
| PositionManager `setApprovalForAll(AutoRedeemer)` (`0xa1200000d0002264C9a1698e001292D00E1b00af`) | anyone wanting automatic redemption | `include_auto_redeem=True` |

The venue's market-makers guide lists these for quoters. For requesters it
says nothing: whether a requester needs the Exchange v3 approvals (a BUY
spends pUSD, a SELL moves combo positions) is UNVERIFIED.

- **Combos.** The venue has no create or lookup endpoint. `create_combo([ComboLeg(market_id="polymarket:<gamma id>", outcome="yes"|"no"), ...])`
  is offline. It maps each leg to its combo position id (from
  `fetch_combo_markets()`, the public catalogue; from `leg.info["position_id"]`;
  or from Gamma) and returns the combo under a provisional id,
  `polymarket:combo-legs-<hex>.<hex>`, which spells out its legs. The first
  RFQ's answer gives the combo's real id, `polymarket:combo-<condition_id>`;
  `create_combo` and `fetch_combo` return that id from then on, and positions
  and fills live on it. A `collection_id` is refused (no collections).
- **Eligible markets.** `fetch_combo_eligible_markets(tag=, limit=, cursor=)`
  is the catalogue as `ComboMarket` rows (`market_id`, `title`,
  `position_ids={"yes", "no"}`, `max_legs` 50, `tags`, the row in `info`), by
  volume. The catalogue takes no filter, so `tag` is applied to each page
  (a page can be short); `event_id` is `BadRequest` (the catalogue names no
  events). `fetch_combo_markets()` stays the raw rows.
- **One side per RFQ.** `create_rfq(combo_id, outcome="yes", action="buy", amount=...)`
  sizes a buy in pUSD, fees included. `action="sell", quantity=...` sizes a
  sell in shares. The venue quotes the combo's YES only. The call returns
  once the 400 ms competition is over, with the best quote; `fetch_quotes`
  returns it as `ask` (for a buy) or `bid` (for a sell), the blended YES
  price, with the shares traded. An answer with no quote is a closed RFQ
  with `info["error"]` (`NO_QUOTES`).
- **Accept** within the quote's `expires_at` (5–10 s):
  `accept_quote(AcceptQuote(quote_id=, rfq_id=, outcome="yes", action=, worst_price=))`
  signs an Exchange v3 order for the quote's exact amounts. It is refused
  before signing if the blended price or the order's own price is worse
  than `worst_price`; fees come on top (`info["total_required"]`). Only the
  full size can be accepted. A retry never sends twice. The quote then
  moves `accepted` → `confirmed` (on its way to the chain) → `executed`
  (FILLED/CONFIRMED). Its order is `rfq:<rfq_id>`: `fetch_order` and
  `fetch_my_trades` answer for it from the RFQ, because the trade never
  touches the CLOB.
- **What the gateway lacks.** It has no list of your RFQs and no cancel.
  `fetch_rfqs` lists the RFQs this adapter created in this process, and
  `cancel_rfq` drops one locally (an unaccepted RFQ lapses at `expires_at`).
  Before an accept, status reads answer 409, so an RFQ is read from its
  create answer until then.
- **No idempotency key on create.** A create that timed out may have made an
  RFQ whose id you never got. Nothing can trade on it, but retrying makes a
  second RFQ, using up the 15 creates a minute. The engine journals each
  create and raises `SubmissionInDoubt`. `create_rfq` returns an open RFQ
  it already holds for the same request instead of making a new one. After
  a restart, `remember_rfq(rfq)` re-seeds an RFQ from its `Rfq.info`, and
  `restore_rfqs(rfqs, quotes)` (which the engine calls from `recover()`)
  re-seeds its open RFQs and the quotes whose accept or confirm was in
  doubt, so an in-flight accept is followed to FILLED or FAILED.
- **Quoting.** Quote requests arrive only on the quoter WebSocket.
  `await poly.start_rfq_session(on_rfq=...)` opens it, and quotes are due
  within 400 ms, so answer from `on_rfq` (or `subscribe_incoming_rfqs`,
  which the engine uses: `engine.rfq.on_incoming_rfq(callback)`) rather
  than a poll. `start_incoming_rfqs()` opens the session if it is not open.
  `fetch_incoming_rfqs` lists the requests received that are still open.
  `send_quote(MakerQuote(rfq_id=, ask=|bid=, quantity=, expires_at=))`
  prices the request's one side: the `ask` for a buy request (filled by
  buying NO at `1 - ask`), the `bid` for a sell request. It signs the
  Exchange v3 order and posts it over REST. A buy quote above 0.90909 is
  refused. `cancel_quote` withdraws a quote, or declines it while its last
  look is open. `confirm_quote` is the last look (1 s). `replace_quote`
  cancels the old quote, then sends a new one. Your fill is
  `rfq:<rfq_id>:<quote_id>`.

See [RFQ and combos](rfq-combo.md).

## Polymarket US

Two APIs trade the same exchange.

**Retail API** (`PolymarketUSTrading`): any verified account, a key from
polymarket.us/developer, Ed25519-signed requests, twenty a second. Orders
name their outcome; every order here is sent on the YES side, a `sell` being
the same order as a NO buy on this netting venue, and an order the venue
holds as NO is read back on the YES leg. Native modify of
price, quantity, time in force and expiry; preview; close-position.
Positions net: long or short the YES side. The activity feed's trades carry
no order id or side, so `fetch_my_trades` is `False` and
`fetch_activities` returns them as sent.

**Exchange API** (`PolymarketUSExchangeTrading`): an onboarded firm, an
Auth0 private-key JWT refreshed every three minutes, a participant id and a
trading account, a preprod environment. Prices and quantities are integers
scaled per instrument from reference data (cached; reference data is six
requests a minute). Orders are on the YES leg, as on Kalshi. The published
schema has exchange-held `stop` and `stop_limit` orders, so those two types
are sent natively (`native_order_types`). Order and
execution search are twelve requests a minute and have their own budgets.
A fill's `fee` is the execution's commission in dollars: commission fields
are notional units, `price_scale * fractional_quantity_scale` to the dollar,
negative for a rebate. Its order, execution, position, drop-copy, market-data
and balance-ledger streams are gRPC; see [Streaming](streaming.md#polymarket-us-exchange-api-grpc).

**Combos and RFQ** (retail API; `rfq_taker`, `rfq_quoter`, `combos`). A combo
is an instrument of 2 to 50 legs with its own symbol, so its id is
`polymarket_us:caoc-…` (or `crossd-…`) and it trades, holds a position and
settles like any market. `create_combo([ComboLeg(market_id=..., outcome="yes"|"no"), ...])`
returns the venue's canonical combo for those legs (`no` sells the leg); the
same legs always give the same symbol, so it is safe to call again.
`fetch_combo(id)` reads one by symbol; there is no lookup by legs that does
not create. A `collection_id` is refused before anything is sent.
`fetch_combo_eligible_markets(event_id=, tag=, limit=, cursor=)` lists the
`comboEnabled` markets of the public gateway's open events (`GET /v1/events`,
no auth), newest event first, as `ComboMarket` rows with `event_id`
(`polymarket_us:<event slug>`), `event_title`, `max_legs` 50 and the venue's
`status`. `event_id` and `tag` (a tag slug: `nfl`, `soccer`) are the venue's
filters; `limit` counts events read (default 50), so a page can hold no
markets yet carry a cursor (an offset).

An RFQ is two-sided: `create_rfq(combo_id, quantity=...)` or `amount=` (USD,
sent as `cashOrderQty`; the venue sizes each side to it, taker fee
included). Quotes come back from `fetch_quotes(rfq_id)` as `RfqQuote` in YES
prices: `ask` is the venue's `buyPrice`, `bid` its `sellPrice`, a declined
side `None`. `accept_quote(AcceptQuote(quote_id=, rfq_id=, outcome=, action=, worst_price=))`
sends `acceptedSide` as the requester's own side: buy YES (or sell NO) is
`SIDE_BUY` at the ask, sell YES (or buy NO) `SIDE_SELL` at the bid. It reads
the quote first and refuses, before sending, a quote that is not active, a
declined side, or a price worse than `worst_price`; a retry of an accept
already made is answered from the quote without resending. It returns at
once with the quote `accepted`. The maker then has 3 s to confirm; `executed`
means the two orders were submitted (the requester's id is
`taker_order_id`), not that they filled; read fills from the order. RFQs do
not expire: `cancel_rfq` closes one. As a quoter: `fetch_incoming_rfqs`,
`send_quote(MakerQuote(rfq_id=, bid=, ask=, post_only=))` (no quantity: the
RFQ's size), `replace_quote` (a new id), `cancel_quote` (also declines in the
last look) and `confirm_quote`. Combo and RFQ creation share 1 request a
second. See [RFQ and combos](rfq-combo.md).

On both, entry is asynchronous: create, cancel and modify return an id or
nothing, and the result is `pending`, `pending_cancel` or `pending_replace`
until read back. The venue's "Global Rate Limit Exceeded" reject is its
five-second latency stopgap, not a rate limit, and maps to `OrderRejected`
with `reason="latency_stopgap"`: safe to resend. Venue `day` orders do not
cancel at the session roll, so `day` is refused here as on the other venues.

## Opinion

`OpinionTrading`, built to Opinion's documentation and its own SDK, with
signing and amounts checked byte for byte against the SDK's code.

Orders are the CTF exchange's EIP-712 `Order` on BNB Chain, signed by the
wallet for the account's Safe, which holds the USDT and the tokens; the
Safe's address is read from the venue when not configured. YES and NO are
separate tokens, as on Polymarket: an order buys or sells the token its
`outcome` and `action` name (with neither, `buy` buys YES, `sell` buys NO).

The venue rests limit orders until cancelled and has nothing else: no time
in force, no post-only, and a market buy that spends a USDT amount rather
than taking a number of contracts. So `market` and `ioc` are sent as a limit
at the price given and the unmatched rest is cancelled at once; `fok`, `gtd`
and `post_only` are refused. The two amounts of an order must state its
price exactly, which trims the size to four significant digits of its USDT
value: the order's `quantity` is what was actually sent. The minimum order is
5 USDT. Fees are taker only, `rate * notional * p * (1 - p)` with the rate
read from the venue's fee contract, and at least 0.25 USDT an order.

A match moves an order's filled figures, and produces a trade, only once the
chain confirms it. `OpinionUserStream` carries both
([Streaming](streaming.md)). The one-time approvals ("enable trading") are
made on opinion.trade; split, merge and redeem are not built here.

Opinion's terms bar residents and citizens of several countries, the UK and
US among them: check them before trading.

## Hyperliquid

`HyperliquidTrading`, for outcome markets (HIP-4). Signing is pinned against
the venue SDK's own test vectors.

An outcome trades as two coins, YES and NO, as on Polymarket: `buy` buys the
YES coin, `sell` buys the NO coin at `1 - price`, and an order's `outcome`
and `action` (Sell YES, Sell NO) sell the coin held instead. Orders are L1 actions signed by the key in
`HYPERLIQUID_PRIVATE_KEY` for the account; with an API wallet (approved on
app.hyperliquid.xyz under More -> API), `HYPERLIQUID_ACCOUNT_ADDRESS` names
the account, since its orders, fills and balances are what is read.

Limits rest until cancelled (`gtc`); `ioc` and `market` are sent as `Ioc`
limits at the price given, and `post_only` as `Alo`. `fok` and `gtd` are
refused. Prices take at most five significant figures and sizes are whole
contracts, at least 10 USDC an order, counted on the coin bought; anything
else is refused before signing rather than rounded. A `client_order_id` is
sent as the venue's 16-byte cloid (hashed when it is not one already).
`create_orders` and `cancel_all_orders` are one signed action each.

Fees are charged only on a fill that closes a position, and at settlement:
the spot rate (7 bps taker, 4 bps maker at the lowest tier) times the
market's deployer scale. `fetch_fee_estimate` reports that closing charge
and says an opening fill pays nothing. Balances are the spot side's USDC;
positions are the two coins' balances netted on the YES leg.
`HyperliquidUserStream` carries orders and fills ([Streaming](streaming.md)).

Hyperliquid's terms bar the US, Ontario and sanctioned places; some outcome
front ends bar more, the UK among them. Check what applies to you before
trading with real funds.

## predict.fun

`PredictFunTrading`, built to predict.fun's documentation and its SDK
(`predict-sdk` 0.0.22). Order hashes and both kinds of signature are checked
against independent EIP-712 encoders and the SDK's algorithm.

Orders are the CTF exchange's EIP-712 `Order` on BNB Chain, verified by one
of four exchanges by the market's kind (neg-risk or not, yield-bearing or
not). A plain wallet signs its own orders. A Predict account (the smart
wallet the web app makes) is the orders' maker and holds the USDT; its owner
key signs for it, wrapped for the account's Kernel validator. YES and NO are
separate tokens: an order buys or sells the token its `outcome` and
`action` name (with neither, `buy` buys YES, `sell` buys NO).

Orders, cancels and account reads need a login token, which the adapter gets
by signing the venue's login message with the same key, and renews when the
venue refuses it. **Signing that message accepts predict.fun's Terms of
Service**, as logging in on the site does: read them, and check they allow
you to trade, before the first call.

Limits rest until cancelled (`gtc`) or until `expires_at` (`gtd`), post-only
on request. `market`, `ioc` and `fok` go as the venue's `MARKET` strategy at
the price given as the worst accepted, so whatever does not match at once is
dropped; `fok` sets the venue's fill-or-kill flag. Prices keep three
significant digits and sizes five, as the venue's SDK keeps them: the
order's `quantity` is what was actually sent. Fees are taker only,
`rate * min(p, 1 - p)` a share (2% on every market so far); makers pay
nothing.

A cancel takes the order off the venue's book (`cancel_orders` takes up to
100 a request). The signed order stays valid on chain until cancelled there,
which this adapter does not do; only the venue's operator matches orders,
so a removed order does not fill through it. A market can lock an order
against removal for a while after it is placed; such a cancel is refused
with `reason="removal_locked"`.

Fills are settled matches, read from the venue's public match record for the
account's address; `PredictFunUserStream` carries orders, and fills as
matched, then confirmed or failed on chain ([Streaming](streaming.md)). The
balance is the address's USDT on chain, less what open orders hold.
Approvals (the one-time "enable trading") come with a Predict account; a
plain wallet sets them with the venue's SDK. Split, merge and redeem are not
built here.

## Limitless

`LimitlessTrading`, built to Limitless's documentation and its SDK
(`limitless-sdk` 1.1.1). Order hashes and signatures are checked against an
independent EIP-712 encoder, and amounts against the docs' worked examples.

Orders are the CTF exchange's EIP-712 `Order` on Base, signed by the wallet
itself, with the market's exchange as the verifying contract. Every request is
also signed with a scoped API token. The profile must be in EOA trading mode:
an account that once turned on the web app's one-click smart wallet rejects
orders its own wallet signs, until `use_eoa_trading_mode()` switches it back
(the web app's one-click trading then stops until it is switched again
there). YES and NO are separate tokens: an order buys or sells the token
its `outcome` and `action` name (with neither, `buy` buys YES, `sell` buys NO).

Limits rest until cancelled (`gtc`), post-only on request. `market` and `ioc`
go as the venue's fill-and-kill at the price given as the worst accepted. The
venue's fill-or-kill spends a USDC amount with no price limit, so `fok` is
refused, and orders cannot expire, so `gtd` and `day` are refused too. Prices
take at most three decimals between 1c and 99c; sizes are cut to thousandths
of a share so the collateral is exact. Fees are taker only and fall as the
price rises; every order carries the profile's fee rate, which the venue
checks. A `client_order_id` is sent as the venue's own and comes back on
every order event.

The venue lists orders market by market: `fetch_orders` takes a market, and
`fetch_open_orders` and `cancel_all_orders` across the account read the
markets with open orders from the account's positions first. Fills are the
settled trades in the account's history; `LimitlessUserStream` carries orders,
and fills as matched, then mined or failed on chain ([Streaming](streaming.md)).
The balance is the wallet's USDC on Base, less what open orders hold.
Approvals (USDC and the outcome tokens to each market's exchange), split,
merge and redeem are not built here.

Limitless's terms bar use wherever it would be unlawful, and sanctioned
persons. Check what applies to you before trading.

## SX Bet

`SxBetTrading`, built to SX Bet's documentation and OpenAPI (its third order
book, OBv3). Order hashes and signatures are checked against an independent
EIP-712 encoder.

Every request carries the SX Bet API key, and the key alone reads the
account, cancels and arms the heartbeat. Placing is the one thing that needs
the private key: every order is also the eight-field
EIP-712 `Order`, signed by the wallet under the domain the venue publishes
(`GET /metadata/obv3`). The order's id is its digest, known before it is sent.
The wallet is the maker, but the funds sit in a *proxy wallet* the venue
derives from it: deploy it once with `deploy_proxy()` (`proxy()` says when it
is live) and fund it with sx.bet's deposit before the first order.

An order bets one outcome of a market. `buy` bets outcome one (YES) at the
price given; `sell` bets outcome two at `1 - price`. The venue counts a stake
in USDC, so `n` contracts stake `n * price` (or `n * (1 - price)`), at least 1
USDC, and prices are snapped onto the venue's odds ladder (0.125% today) on
the cheaper side, so an order never pays more than asked. Every bet on a
market nets.

Limits rest until cancelled (`gtc`), post-only on request, or until
`expires_at` (`gtd`). `market` and `ioc` go as the venue's `IOC` at the price
given as the worst accepted, `fok` as its `FOK`. Orders are placed in the
venue's synchronous mode, so what matched on arrival is on the order
returned; `create_orders` sends ten a request. Resting orders on a game are
swept by the venue at kickoff. In-play orders wait out the venue's betting
delay (a few seconds, by sport and league) before they match.

`heartbeat(seconds)` arms the venue's dead man's switch: unless it is called
again in time, every open order on the account is cancelled. Fees are charged
at settlement on winnings: 1% of a winning taker bet's profit, nothing for a
maker; the venue can set other rates per account.

The venue lists only resting orders, so `fetch_orders` is not offered; any
order, in any state, is read by id with `fetch_order`. Fills are read from the
account's fill history; `SxBetUserStream` carries orders, and fills as matched,
then escrowed or failed on chain ([Streaming](streaming.md)). Positions net
each market on the YES leg. The balance is the proxy wallet's USDC: what is
free to bet, and what matched bets hold in escrow.

**Parlays (quoter side).** A bettor's parlay request on sx.bet makes a real
market, the parlay's own id (`sx_bet:{parent hash}`), that takes makers'
orders for 60 seconds. `SxBetTrading` hears each request on
`parlay_markets:global` (`SxBetParlayStream`, the same API key) and keeps it as
an incoming `Rfq` (`mine=False`): a two-sided request on a `Combo` of its legs
(`yes` where the bettor needs outcome one of a leg), sized by `amount`, the
stake asked for (`requestSize`, which does not cap what a maker offers). The
venue replays nothing and lists no open requests over REST, so
`fetch_incoming_rfqs` returns the requests heard and still open (its first
call starts the stream, as does `start_incoming_rfqs()`);
`subscribe_incoming_rfqs(listener)` hears each one at once, and the engine
subscribes there (`engine.rfq.on_incoming_rfq`), so SX Bet is not polled. `fetch_rfq` and `fetch_combo` read the ones heard; `lookup_combo` finds a
heard parlay by its legs; `create_combo` is not supported.

`send_quote(MakerQuote)` signs ordinary orders on the parlay market: the bid
buys YES (outcome one: the parlay wins), the ask sells YES (outcome two at
`1 - ask`). Without a `quantity`, each side covers the requested stake at the
bettor's price. Both rest as the venue's `GTC` with an expiry no later than
the parlay's end: the request names no end, so it is counted as 60 s from its
arrival less 3 s (`PARLAY_EXPIRY_MARGIN_S`), or `expires_at` if sooner; an
order signed past the end is refused with the exact end
(`PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY`) and re-signed to it once. The
quote's id names both orders (`0xbid~0xask`), so `cancel_quote` cancels both,
`replace_quote` is a cancel and a new send, and `fetch_quote` reads it back:
open while an order rests, executed once one filled and none rests, else
expired or canceled. The bettor takes a quote with FOK or IOC; there is no last
look, so `confirm_quote` is not supported, and no API requests a parlay, so
there is no taker side. A parlay that has ended is refused before anything is
sent; the venue's `INVALID_MARKET` (after the end) and `PARLAY_LEG_INACTIVE`
are `OrderRejected` with reasons `parlay_expired` and `parlay_leg_inactive`.
A parlay's fills are on the parlay market, so they book on the combo's id.

The documents disagree on a VOID leg: the developer docs say it voids the
parlay and returns every stake; the user guide says a VOID or push leg loses a
parlay WIN bet, and a parlay LOSE bet wins only with no VOID leg.
`parlay_result(legs, outcome_one=)` gives both readings and the worse one for
the side held; Synpath books nothing from a leg itself and takes the venue's
settlement as the record. Parlay fees are separate rates (the user guide says
5% of a winning parlay's profit), not the single-bet estimate.

Check SX Bet's terms for where it may be used before trading.
