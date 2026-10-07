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
stacks: RSA-PSS for Kalshi, EIP-712 for Polymarket, Opinion, Hyperliquid, predict.fun and Limitless,
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
they point at and the public half of the identity. It prints no secret.

| Venue | Variables |
|---|---|
| Kalshi | `KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH` (PEM, the BEGIN…END block only), `KALSHI_ENV` = `prod` (the default, real money) or `demo` (Kalshi's practice exchange) |
| Polymarket | `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_SIGNATURE_TYPE` (3 Deposit Wallet, used by every account created since May 2026 and what `synpath init` suggests; 1 proxy, 2 Safe, 0 allowlisted EOA, which is also what an unset value means), `POLYMARKET_FUNDER` (the wallet address, required for 1-3); optional `POLYMARKET_API_KEY` / `_API_SECRET` / `_API_PASSPHRASE` (derived when absent), `POLYMARKET_BUILDER_CODE`, `POLYMARKET_RELAYER_API_KEY` / `_RELAYER_API_KEY_ADDRESS` (gasless wallet transactions), `POLYMARKET_RPC_URL` (an EOA's own transactions) |
| Polymarket US, retail API | `POLYMARKET_US_KEY_ID`, `POLYMARKET_US_SECRET_KEY` (from polymarket.us/developer) |
| Polymarket US, exchange API | `POLYMARKET_US_CLIENT_ID`, `POLYMARKET_US_PRIVATE_KEY_PATH`, `POLYMARKET_US_PARTICIPANT_ID`, `POLYMARKET_US_ACCOUNT` (optional), `POLYMARKET_US_ENV` = `preprod` or `prod` |
| Opinion | `OPINION_PRIVATE_KEY` (the wallet connected on opinion.trade), `OPINION_API_KEY` (`synpath init` creates it by signing with that wallet), `OPINION_MULTISIG_ADDRESS` (optional; the account's Safe, read from the venue when absent) |
| Hyperliquid | `HYPERLIQUID_PRIVATE_KEY` (an API wallet's key is safest: it trades but cannot withdraw), `HYPERLIQUID_ACCOUNT_ADDRESS` (the account an API wallet trades for; not needed with the account's own key), `HYPERLIQUID_TESTNET` (`1` for the test network) |
| predict.fun | `PREDICT_FUN_PRIVATE_KEY` (a plain wallet's key, or a Predict account's owner key: the Privy wallet exported under Account -> Settings), `PREDICT_FUN_API_KEY` (mainnet; from developers.predict.fun), `PREDICT_FUN_ACCOUNT_ADDRESS` (the Predict account's deposit address; unset for a plain wallet), `PREDICT_FUN_TESTNET` (`1` for the test network), `PREDICT_FUN_RPC_URL` (optional; the BNB Chain node the balance is read from) |
| Limitless | `LIMITLESS_PRIVATE_KEY` (the wallet connected on limitless.exchange, in EOA trading mode), `LIMITLESS_API_TOKEN_ID` and `LIMITLESS_API_SECRET` (a scoped API token derived on limitless.exchange: profile, API tokens, Derive), `LIMITLESS_RPC_URL` (optional; the Base node the balance is read from) |

## Through your own server

The engine's order types (stops, icebergs, TWAP, orders on a bucket) live in
your self-hosted server (`synpath serve`), not at a venue. Point
`synpath.Client(server=...)` at it and the same calls go there. Setup, the
access token and the three kinds of credentials are on
[synpath.dev](https://synpath.dev/docs/quickstart/local-server).

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
            amount=Decimal("5"), price=Decimal("0.70"),                     # at the YES price
        ))
        order = await kalshi.edit_order(EditRequest(order_id=order.id, amount=Decimal("3")), current=order)
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

**Edits.** `edit_order` is a *decrease* when only the amount comes down,
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
            amount=Decimal("10"), price=Decimal("0.52"),
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
and no netting, so the adapter picks the token from the side: `buy` buys the
YES token at `price`; `sell` buys the NO token at `1 - price`, because you
cannot sell YES you do not hold. With `reduce_only`, `sell` sells the YES
tokens held instead and `buy` sells NO tokens held. Everything the venue
reports on the NO token comes back on the YES leg (a NO buy at 0.30 reads
as a sell at 0.70), and a position is the two inventories netted with both
kept on `inventory_yes` / `inventory_no`. Which token is which side comes
from the Gamma catalog, one read per market, cached.

**What the venue holds.** Limit orders, `gtc`, `gtd`, and `ioc` / `fok`
(the venue's FAK and FOK). A market order is an immediate limit at your
protection price. Post-only for `gtc` and `gtd`. No amend (`edit_order` is
`False`: a change is a cancel and a new signed order), no reduce-only, no
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
POL for gas). Legacy proxy and Safe wallets can trade but these calls are
not built for them, hence `split_merge = "partial"`. NegRisk conversion is
not built: the V2 documentation publishes no interface for it.

**Errors.** The venue answers in words, not codes; the documented phrases
map to `InsufficientFunds`, `InvalidOrder` (tick, minimum size, expiry),
`MarketHalted` (cancel-only, post-only mode, closed-only accounts) and
`OrderRejected` with a `reason` (`post_only_would_cross`, `fok_not_filled`,
`fak_no_match`, `duplicate`).

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
separate tokens, as on Polymarket: `buy` buys YES, `sell` buys NO at
`1 - price`, and with `reduce_only` they sell the tokens held instead.

The venue rests limit orders until cancelled and has nothing else: no time
in force, no post-only, and a market buy that spends a USDT amount rather
than taking a number of contracts. So `market` and `ioc` are sent as a limit
at the price given and the unmatched rest is cancelled at once; `fok`, `gtd`
and `post_only` are refused. The two amounts of an order must state its
price exactly, which trims the size to four significant digits of its USDT
value: the order's `amount` is what was actually sent. The minimum order is
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
YES coin, `sell` buys the NO coin at `1 - price`, and with `reduce_only` they
sell the coin held instead. Orders are L1 actions signed by the key in
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
separate tokens: `buy` buys YES, `sell` buys NO at `1 - price`, and with
`reduce_only` they sell the tokens held instead.

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
order's `amount` is what was actually sent. Fees are taker only,
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
there). YES and NO are separate tokens: `buy` buys YES, `sell` buys NO at
`1 - price`, and with `reduce_only` they sell the tokens held instead.

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
