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
stacks: Ed25519 or RSA-PSS for Kalshi, EIP-712 for Polymarket, Opinion, Hyperliquid, predict.fun, Limitless, SX Bet, Azuro and Myriad,
Ed25519 and private-key JWT for Polymarket US, Ed25519 or P-256 request signing (NOVIG-V3) for Novig; ProphetX signs nothing (a key pair buys a bearer token), HMAC-SHA384 for Gemini. Nothing further is needed to place an order.

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
| Novig | REST: nothing. Books over the WebSocket: a subaccount's trading key (without one, the public book is polled) | a subaccount's trading key: a key id and its private key file (PEM, Ed25519 or P-256). The management key, optional, opens, funds and labels subaccounts and never trades | the management key on novig.com (paper.novig.com for Paper) -> Profile -> Settings -> Novig API; then `NovigTrading.open_subaccount` with a keypair from `novig_signing.generate_key()` issues the trading key. The private key is never sent |
| Betfair | the application key (the free delayed key) and the account's login, for every call | a live application key (Betfair activates it on request) and the same login: username and password, interactive or by a self-signed client certificate | developer.betfair.com (or the Accounts API visualiser) for the keys; for certificate login, a self-signed certificate uploaded at myaccount.betfair.com/accountdetails/mysecurity?showAPI=1 (Automated Betting Program Access) |
| Smarkets | nothing (a login undelays the quotes) | the account's login: username and password (an account with two-factor login passes a session token made elsewhere); placing orders needs Smarkets' approved API access | your smarkets.com account; API access is applied for from Smarkets (a one-off £150 fee) |
| Matchbook | nothing | the account's login: username and password (an account with two-factor login passes a session token made elsewhere) | your matchbook.com account; higher API limits need an API plan from api@matchbook.com |
| ProphetX | the Trading API key pair, or a Market Data API key | the Trading API key pair: an access key and a secret key, exchanged for a ten-minute bearer token | prophetx.co -> Menu -> API Integration, on any KYC-approved account (sandbox.prophetx.dev for the sandbox); the secret is shown once. The Market Data API key is issued by ProphetX on request |
| Azuro | nothing | the private key of the wallet that signs each bet and holds the bet token, which must allow Azuro's relayer to spend it (`approve()`, once) | exported from the wallet |
| Myriad | nothing (an API key and secret raise the rate limit) | the private key of the wallet the Myriad account is connected to (it signs every order and cancel), and an API key with its secret, or nothing more: without a key the wallet signs in (SIWE) for a session token. A key alone reads the account, with `MYRIAD_ACCOUNT_ADDRESS` | Myriad Account settings, or `MyriadTrading.create_api_key()` from a signed-in session; the secret is shown once |
| Gemini | nothing | an API key and its secret: account-scoped, Trader role, time-based nonce; a Gemini account with Prediction Markets access (US) that has accepted the current Prediction Markets terms | exchange.gemini.com/settings/api -> Create API key; tick "Uses a time-based nonce"; the secret is shown once |

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
| Myriad | `MYRIAD_PRIVATE_KEY` (signs orders and cancels, and the sign-in when there is no key), `MYRIAD_API_KEY` and `MYRIAD_API_SECRET` (optional together: HMAC-signed requests; the venue refuses a key without its signature), `MYRIAD_ACCOUNT_ADDRESS` (optional; the wallet's public 0x address when no private key names it), `MYRIAD_RPC_URL` (optional; the BNB Chain node the balance is read from and AMM trades are sent to) |
| SX Bet | `SX_BET_API_KEY` (market data streams, reading the account, cancels and the heartbeat; generated on sx.bet under Account -> Overview -> API Credentials), `SX_BET_PRIVATE_KEY` (optional; placing orders only: the wallet you sign in to sx.bet with, the account's own key, not its proxy wallet's), `SX_BET_ACCOUNT_ADDRESS` (optional; the wallet's public 0x address, not its private key, which names the account stream's channels when no private key is set; the venue documents no route that returns a key's owner), `SX_BET_TESTNET=1` (optional; the Toronto test network) |
| Novig | `NOVIG_KEY_ID` and `NOVIG_PRIVATE_KEY_PATH` (a subaccount's trading key: its id, which is also the subaccount's address, and its PKCS#8 PEM file), `NOVIG_ENV` = `production` (the default, real money) or `paper` (play money; Paper's keys and ids are its own), `NOVIG_MANAGEMENT_KEY_ID` / `NOVIG_MANAGEMENT_PRIVATE_KEY_PATH` (optional; the account's management key, for `open_subaccount`, `fund_subaccount`, `label_subaccount`, `issue_subaccount_key` and `subaccounts`) |
| Betfair | `BETFAIR_APP_KEY` (a live key bets; the delayed key reads market data and the account), `BETFAIR_USERNAME`, `BETFAIR_PASSWORD`, `BETFAIR_CERT_PATH` / `BETFAIR_KEY_PATH` (optional together; Betfair's certificate login), `BETFAIR_JURISDICTION` (optional; `com`, the default, `it`, `es`, `ro` or `com.au`, the site the account is on) |
| Smarkets | `SMARKETS_USERNAME`, `SMARKETS_PASSWORD` (betting and the account's orders, statement and balance; market data needs neither, and with them its quotes are undelayed) |
| Matchbook | `MATCHBOOK_USERNAME`, `MATCHBOOK_PASSWORD` (betting and the account's offers, bets and balance; market data needs neither) |
| ProphetX | `PROPHETX_ACCESS_KEY` and `PROPHETX_SECRET_KEY` (the Trading API: market data, streams, orders), `PROPHETX_AFFILIATE_KEY` (optional; the Market Data API, market data only), `PROPHETX_SANDBOX=1` (optional; ProphetX's sandbox, with demo funds) |
| Azuro | `AZURO_PRIVATE_KEY` (the wallet that signs each bet and holds the bet token), `AZURO_CHAIN` (optional; `polygon`, the default, `base`, or the testnets `polygon_amoy` and `base_sepolia`), `AZURO_AFFILIATE` (optional; the affiliate address every bet names, the wallet's own when unset), `AZURO_RPC_URL` (optional; the chain's node the balance and allowance are read from and the approval is sent through; a public one when unset) |
| Gemini | `GEMINI_API_KEY` (`account-...`), `GEMINI_API_SECRET`, `GEMINI_ACCOUNT` (optional; the subaccount a `master-...` key acts for, REST only: the WebSocket refuses master keys), `GEMINI_ENV` = `prod` (the default, real money) or `sandbox` (api.sandbox.gemini.com, with a sandbox key) |

Smarkets needs no credentials for market data. `SMARKETS_USERNAME` and
`SMARKETS_PASSWORD` log in for undelayed quotes and for order entry; placing
orders through Smarkets' API needs its approved API access (applied for, with
a one-off £150 fee). See [Smarkets](#smarkets).

Novig trades on one subaccount per trading key. An order buys an outcome:
`buy` the market's YES outcome at the price given, `sell` its NO outcome at
one minus it, snapped onto the venue's price grid on the cheaper side, in
sizes of 0.01 contract (the venue's 1-cent contract). Placement is queued:
`create_order` returns the order `pending`, and the private stream or a read
says when it rests, fills or is rejected. A batch of up to 256 is placed all
or nothing. Every resting order on an event is voided when the event goes in
play. Production needs Novig's identity check and a US location outside
Arizona, Michigan and Nevada, with a recent device geolocation from the Novig
app (a VPN is refused); Paper is the same API with play money and no location
check. `NovigTrading.echo()` proves the host, key, clock and signature in one
call. Liquidity providers can price Novig's parlay RFQs with `NovigPricer`
([Novig parlay RFQ](#novig-parlay-rfq)). See [Novig](capabilities.md#novig).

```python
from decimal import Decimal

from synpath.trading import novig_signing
from synpath.trading.credentials import load_credentials, require
from synpath.trading.novig import NovigTrading

async with NovigTrading(require("novig", load_credentials())) as nv:   # with the management key loaded
    private_pem, public_pem = novig_signing.generate_key()           # keep private_pem in a secret store
    desk = await nv.open_subaccount("desk-1", public_pem)            # desk["keyId"] is NOVIG_KEY_ID
    await nv.fund_subaccount(desk["keyId"], Decimal("250"), client_transfer_id="desk-1-topup-1")
```

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
market nets, so `reduce_only` is refused.

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

Check SX Bet's terms for where it may be used before trading.

## Betfair

`BetfairTrading`, built to Betfair's Betting API (`placeOrders`,
`cancelOrders`, `replaceOrders`, `updateOrders`, `listCurrentOrders`,
`listClearedOrders`, `listMarketProfitAndLoss`) and Accounts API
(`getAccountFunds`, `getAccountDetails`) documentation. Every call carries
the application key and the session token of the account's login, made the
way the read adapter makes it: by certificate when `BETFAIR_CERT_PATH` and
`BETFAIR_KEY_PATH` are set, else interactively, extended with `keepAlive`
and kept in memory only; a refused session logs in again once.
`session_token=` reuses a login made elsewhere. **Betting needs a live
application key**: Betfair refuses every bet from the delayed key
(`PermissionDenied`), which still reads orders, positions and the balance.

An order is a bet to back or lay one runner,
`betfair:<market id>-<selection id>` (with `-<handicap>` on a handicap
runner). `buy` backs it (takes its YES) at decimal odds `1 / price`; `sell`
lays it (takes its NO) at the same odds. Betfair counts both in the backer's
stake, so `n` contracts stake `n / odds`, rounded down to the penny; a lay's
liability is that stake times `odds - 1`. Prices are snapped onto Betfair's
CLASSIC odds ladder (1.01 to 1000) on the cheaper side: a buy at the next
higher odds, a sell at the next lower, so an order never pays more than
asked. A bet stakes at least the currency's minimum bet size (£1, €1, $1)
unless its payout (stake times odds, its contracts) reaches the minimum bet
payout (£10, €20, $20); the payout rule is not offered on the .it and .es
sites. Orders below both are refused before anything is sent.

Limits (`gtc`) are Betfair's `LIMIT` with persistence `LAPSE`: the unmatched
part is cancelled when the market turns in play. `params={"persistence":
"PERSIST"}` (or `keep_in_play=True`) keeps it in play, and
`"MARKET_ON_CLOSE"` moves it into the starting-price auction at the off.
`fok` is Betfair's `FILL_OR_KILL`, whose price Betfair reads as the worst
*average* matched price; `params={"min_fill": contracts}` accepts a part of
at least that much. Betfair has no immediate-or-cancel bet, so `market` and
`ioc` are `LAPSE` bets at the price given as the worst accepted, whose
unmatched rest is cancelled as soon as Betfair answers. `gtd`, day and
post-only orders are refused, and so is `reduce_only`, since every bet on a
runner nets. `params={"market_version": n}` lapses the bet if the market has
changed materially since version `n` (a runner removed, turned in play).

On an in-play market Betfair holds a bet for the market's delay (1 to 10
seconds) before matching it, and answers only then, so `create_order`
returns the bet already matched or resting. `create_orders` sends one
`placeOrders` per Betfair market, 200 bets a request (50 on the Italian
exchange), which Betfair places all or none, and returns one result each.

`client_order_id` (up to 32 characters) is sent as the bet's
`customerOrderRef`, which Betfair keeps on every record and on the order
stream; a single order's id also de-duplicates a resubmission within 60
seconds (`customerRef`), refused as `DuplicateClientOrderId`. A placement
Betfair could not finish in time (`TIMEOUT`) is looked up by that id; without
one it raises `RequestTimeout`. `fetch_order_by_client_id` reads a bet by it.

`edit_order` with a smaller `quantity` (the order's new total, filled
contracts included) is a partial cancel, which keeps the bet's place in the
queue; a new price is `replaceOrders`, which cancels the unmatched part and
places it again at the new odds as a new bet with a new id
(`queue_priority_preserved` is false, `info["replaced"]` names the old bet).
Betfair cannot raise a stake. `update_persistence(order_id, "PERSIST")`
changes what happens at the off in place. Cancels and edits name the bet's
Betfair market; one this adapter did not place is read first to find it.

Bets on unsettled markets are `fetch_order`, `fetch_open_orders` and
`fetch_orders` (by status: `open`, `closed`, `canceled`); a bet that has left
them (cancelled, lapsed, settled, voided) is found by `fetch_order` among the
cleared orders. Betfair lists bets, not their matched fragments, so
`fetch_my_trades` is one fill per bet at its average matched odds, for bets
that have stopped matching; each match as it happens is on
`BetfairUserStream` ([Streaming](streaming.md)). Settled bets are
`fetch_settlements`, one per bet. Positions net the matched bets per runner
on the YES leg: backs are YES contracts, lays NO contracts.
`fetch_profit_and_loss(market_ids)` is Betfair's own if-win and if-lose
profit per runner. The balance is what is free to bet
(`availableToBetBalance`), what open bets hold (`exposure`, `locked`), and
the two together, in the account's currency.

Commission is charged at settlement on net winnings per Betfair market, at
the market's base rate (5% on most .com markets), before the account's
discount; `fetch_fee_estimate` is that rate on what one bet would win, an
upper bound for a bet among others on the same market. Betfair charges
accounts that send very many bets and changes (its Transaction Charges), so
batch where you can.

Check Betfair's terms for where it may be used before trading: its API is
refused from several countries (the US, Germany, France, the Netherlands,
Singapore, India, Turkey, the UAE and others), and its keys are for an
account holder's own betting.

## Smarkets

`SmarketsTrading`, built to Smarkets' trading API reference
(docs.smarkets.com, API v3). Every call carries the session token of an
account login (`SMARKETS_USERNAME`, `SMARKETS_PASSWORD`), made on the first
call and kept in memory only, as `Authorization: Session-Token <token>`. A
token lasts 30 minutes from its last use: one idle for 25 is replaced before
the next call, and a 401 logs in again once. Smarkets allows five logins per
five minutes. An account with two-factor login cannot log in from here; pass
`session_token=` from a login made elsewhere. **Placing orders needs
Smarkets' approved API access** (applied for, with a one-off £150 fee): any
account reads its balance, orders and statement, and an account without it
is refused a new order with `PermissionDenied`.

An order buys or sells one contract's YES, `smarkets:<market id>-<contract
id>`. `buy` backs the contract (takes its YES), `sell` lays it (takes its
NO), and the price is the YES price, as Smarkets' book reads: its `bids` are
resting backs, YES bids. Smarkets prices in basis points of 100%, and only on
its odds ladder (decimal odds 1.01 to 1000 in Betfair's bands, as basis
points, and the ends 1 and 9999). A price within half a basis point of a tick
is that tick; any other is snapped onto the ladder on the cheaper side: a buy
at the tick below, a sell at the tick above. An order's size is its pot (back
stake plus lay liability) in 1/10,000 of the account's currency, so `n`
contracts send `n * 10,000`, rounded down; the smallest back stake is £0.05.

A limit is `good_til_halted` by default, Smarkets' own default: it rests
through the market going in play and is cancelled when the market halts (a
goal, a red card). `params={"order_type": "keep_in_play"}` (or
`{"keep_in_play": True}`) keeps it through halts as well, and
`"good_til_cancelled"` cancels it when the market goes in play. Smarkets
cancels every resting order when it applies a reduction factor (a withdrawn
runner). `market` and `ioc` are `immediate_or_cancel` at the price given as
the worst accepted, answered once they have matched what they could; `fok` is
the same with `minimum_accepted_quantity` set to the whole order, and
`params={"minimum_quantity": n}` sets a smaller floor on an `ioc`. One that
cannot meet its floor is refused (`OrderRejected`,
`ORDER_CANCELLED_INSUFFICIENT_LIQUIDITY`). `gtd`, `day`, post-only and
`reduce_only` are refused. There is no amend route, so `edit_order` is not
offered; cancel and replace. `create_orders` and `cancel_orders` send one
request per order and return one result each.

Smarkets takes no client order id, but every order carries a free `label`:
`client_order_id` (or `params["label"]`) is sent as the label and read back
as `client_order_id`. After a timeout, list the open orders on the contract
(`fetch_open_orders(market_id=...)`) and match the label to see what was
placed.

Orders are read from `GET /v3/orders/`, which lists current and recently
settled orders: `fetch_order`, `fetch_open_orders`, `fetch_orders` (by
status, oldest first) and `fetch_settlements` (settled orders, one per
contract and side, with `payout` and `pnl` before commission). Smarkets has
no cancelled state: an order with nothing left unmatched that did not match
in full is `canceled`. `cancel_order` reads the order back; one the exchange
has not finished cancelling reads `pending_cancel`. `cancel_all_orders`
cancels the account's orders, or a Smarkets market's
(`smarkets:<market id>`), in one request that Smarkets acknowledges without
a count, once a minute; a second inside the minute, or one naming a single
contract, cancels the open orders one by one and returns how many. In play,
matched bets wait about eight seconds for confirmation and can be voided in
that time; the pending part counts as filled (`info["quantity_filled_pending"]`).

Fills are the account statement's executions (`order.execute` rows of
`/v3/accounts/activity/`), newest first. Positions net the account's matched,
unsettled orders per contract on the YES leg: backs are YES contracts, lays
NO contracts, and `margin` is the most the contract can lose. The balance is
the account's cash, what its bets hold (`exposure`, `locked`), and what is
free to bet, in the account's currency. Commission is charged at settlement
on net winnings per Smarkets market, 2% on the standard tier; fee estimates
use the account's own rate (`commission_type`).

Smarkets allows 1,200 requests a minute per account, ten order placements
and ten cancels a second, and one cancel-all a minute; the adapter paces
itself inside them. Smarkets' trading API is HTTP only (its socket feed is
for approved market makers), so there is no account stream and the engine
polls the orders.

Check Smarkets' terms for where it may be used before trading: its API terms
allow viewing prices and betting on your own account only.

## Matchbook

`MatchbookTrading`, built to Matchbook's API reference
(developers.matchbook.com). Every call carries the session token of an
account login (`MATCHBOOK_USERNAME`, `MATCHBOOK_PASSWORD`), made on the first
call and kept in memory only. A session lasts about six hours: one older than
five is replaced before the next call, and a 401 logs in again once. An
account with two-factor login cannot log in from here; pass
`session_token=` from a login made elsewhere.

An order is an offer to back or lay one runner,
`matchbook:<event id>-<market id>-<runner id>`. `buy` backs it (takes its
YES) at decimal odds `1 / price`; `sell` lays it (takes its NO) at the same
odds. Matchbook counts both in the backer's stake, so `n` contracts stake
`n / odds`, rounded down to the penny; a lay's liability is that stake times
`odds - 1`. Prices are snapped onto Matchbook's odds ladder (1.01 to 1000, the
same bands as Betfair's) on the cheaper side: a buy at the next higher odds, a
sell at the next lower, so an order never pays more than asked.

Limits rest until matched or cancelled (`gtc`). Matchbook has no
immediate-or-cancel offer, so `market` and `ioc` are offers at the price
given as the worst accepted, whose unmatched rest is cancelled at once; in
play, the offer first waits out Matchbook's delay (1 to 10 seconds), then
the rest is cancelled. `fok`, `gtd` and post-only are refused, and so is
`reduce_only`, since every bet on a runner nets. An offer's unmatched part is
cancelled when its event goes in play unless it is sent with
`params={"keep_in_play": True}`. `create_orders` sends fifty offers a request
and returns one result each. `edit_order` changes the odds or the remaining
stake in place (`quantity` is the order's new total, filled contracts
included).

Matchbook takes no client order id: `client_order_id` stays on the returned
order only. After a timeout, read the open offers on the runner
(`fetch_open_orders(market_id=...)`) to see what was placed.

Offers and their matched bets are readable while their market is unsettled:
`fetch_order`, `fetch_open_orders`, `fetch_orders` (by status) and
`fetch_my_trades` (one fill per matched bet). Settled bets are
`fetch_settlements`. Positions net the account's current matched bets per
runner on the YES leg: backs are YES contracts, lays NO contracts. The
balance is the account's cash, what its bets hold (exposure and the
commission reserve, `locked`), and what is free to bet, in the account's
currency.

`heartbeat(seconds)` arms Matchbook's dead man's switch: unless it is called
again in time, every open offer on the account is cancelled; `heartbeat(0)`
turns it off. Commission is charged at settlement on net winnings per market
(2% in the UK and Ireland, 4% elsewhere), makers and takers alike, unless the
account is on volume commission. Betting writes are free at a reasonable
rate; reads count towards the GETs Matchbook's API plans charge for.

Check Matchbook's terms for where it may be used before trading: it is
licensed in the UK and Alderney, and its terms allow betting on your own
account only.

## ProphetX

`ProphetXTrading`, built to ProphetX's Trading API reference (the v4 `/mm`
routes at docs.prophetx.co).

The access key and secret key are exchanged at `POST /auth/login` for a
bearer token that lasts ten minutes; the adapter renews it with the refresh
token before it lapses and logs in again only when that fails. ProphetX counts
live sessions per access key and a second login adds one, so the adapter, its
catalog and its streams share one session; run each process on its own key
pair. Nothing is signed.

Every ProphetX order is a buy: each side of a line is its own instrument
(`strike_id`), and there is no sell. `buy` buys the line's first side (YES) at
the price given; `sell` buys the second side at `1 - price`. The price goes as
American odds on ProphetX's ladder (`GET /v4/mm/get_price_ladder`), snapped on
the cheaper side, so an order never pays more than asked. ProphetX counts the
stake in USD, so `n` contracts stake `n * P` at the ladder price's
probability `P`, rounded down to the cent, at least $1. A fill's contracts are
its stake over its price's probability: its stake plus its profit.
`reduce_only` is refused. `client_order_id` is ProphetX's `external_id`, its
idempotency key: up to 100 letters, digits, `-` and `_`, generated when absent;
a repeat is refused as `DuplicateClientOrderId`.

Limits rest until filled or cancelled (`gtc`); `post_only` is ProphetX's
`ALO`, which ProphetX rejects after the fact (`wiped`) when it would have
crossed. `market` and `ioc` go as its `fillOrKill` at the price given as the
worst accepted: whatever is there fills and the rest is cancelled about two
seconds after the last fill. There is no all-or-none (`fok`) and no expiry
(`gtd`). An order whose event goes live is wiped, and so are two orders of one
account that would cross each other.

`submit_order` answers before matching runs, so the order returned is
`pending`; `ProphetXUserStream` carries what became of it, one message per
fill ([Streaming](streaming.md)). Reads (`fetch_order`, the history, the fills)
come from a replica that can lag by seconds, while `cancel_order` is final when
it answers: an order it cancelled is reported cancelled even before the read
catches up. A partly filled order whose rest was cancelled or killed is
rewritten by ProphetX as a smaller, fully matched one; here it is `canceled`
with its fill. The default rate tier allows one request a second per endpoint
and turns the batch routes off, so `create_orders` and `cancel_orders` send
one at a time; `cancel_all_orders` cancels in the background, account-wide or
for both sides of one line.

ProphetX has no positions route: `fetch_positions` nets the matched stakes of
the last sixty days' live orders per line on the YES leg, each at its limit
price. The balance is the wallet's USD: what is free to stake, and what
resting and matched orders hold; an exposure credit line, where ProphetX
grants one, is in `info`. The fee is reported on each order and fill;
ProphetX's API documentation publishes no rate, so there is no fee estimate.

Check ProphetX's terms for where it may be used before trading.

## Azuro

`AzuroTrading`, built to `@azuro-org/toolkit` and the Azuro backend API. Bet
and cash-out digests and signatures are checked against the ones the toolkit
and viem produce.

The one credential is the wallet. Every bet is the EIP-712 message
`ClientBetData` (the consent text, the affiliate, the pool's core contract, a
five-minute expiry, the chain id, the relayer's fee, and one `SubBet`: the
condition, the outcome, the minimum odds, the stake and a nonce) under the
domain `Live Betting` `1.0.0` with the core contract as verifying contract,
signed by the wallet and sent to `/bet/orders/ordinar`. Azuro's relayer puts
it on chain and is paid a flat fee per bet (`/bet/gas-info`) from the wallet's
bet token, so before the first bet the wallet allows the relayer to spend the
token: `fetch_allowance()` reads the allowance, `approve()` sends the
approval, an on-chain transaction whose gas the wallet pays in the chain's
coin. `fetch_balance()` is the wallet's bet token, read on chain.

A bet is a market order with a worst price. The pool takes it whole at the
odds it quotes when the bet reaches the chain, or refuses it, so an order is
`market`, or a `limit` that is `ioc` or `fok`; anything that would rest, and
`post_only` and `reduce_only`, are refused. `buy` backs the market's YES
outcome with minimum odds `1 / price`, rounded up to the protocol's twelve
decimals; `sell` backs the other outcome of a two-outcome market with minimum
odds `1 / (1 - price)`. One outcome of a larger condition has no NO to sell.
`n` contracts stake `n` times the price paid, rounded down to the token's
decimals, so at the quoted odds or better the bet returns at least `n`.

Placing is asynchronous: the API answers with the bet order's id
(`<bettor>_<nonce>`) in state `Created`, and `create_order` returns it
`pending`. The relayer sends it on (`Placed`, `Sent`) and it becomes
`Accepted`, which is `closed` and filled here, or `Rejected`.
`wait_for_order(id)` reads it until then. A signed bet cannot be cancelled;
an accepted one can be closed early where the venue offers a price:
`fetch_cashout_quote(bet_id)` reads the offer by the bet's on-chain id
(`Order.info["betId"]`), `cash_out(bet_id)` signs it (`CashOutOrder` under
the domain `Cash Out` `1.0.0`, the offer's odds as the minimum) and sends it,
and `fetch_cashout(id)` follows it.

`fetch_orders`, `fetch_open_orders` (bets not yet on chain), `fetch_my_trades`
(one fill per accepted bet) and `fetch_settlements` (won, lost, or cancelled
with the stake returned) read the wallet's bet orders. Positions net each
market's accepted, unsettled single bets on the YES leg. Combo bets are read
on their first leg's market and marked `info["combo"]`; this library places
single bets only. A winning bet's payout is claimed on chain from the pool,
which this library does not do.

Gnosis and Chiliz are discontinued and take no bets. Azuro's bet API answers
some regions with a 503; check where Azuro may be used before betting.

## Myriad

`MyriadTrading`, built to Myriad's API reference, for its order book on BNB
Chain and its AMM (pool) markets on BNB Chain, Abstract and Linea. Order
hashes and signatures are checked against an independent EIP-712 encoder and
fixed known answers; pool calls against the venue's own quote calldata.

Every order is the venue's nine-field EIP-712 `Order` (trader, market,
outcome, side, amount, price, minimum fill, nonce, expiration), signed by the
wallet under the domain `MyriadCTFExchange` on chain 56; its digest is the
order's hash, the id every lookup and cancel takes, known before it is sent.
Requests to the account's endpoints are signed with the API key and its secret
(HMAC-SHA256 over the timestamp, method, path with query, and body), or,
without a key, carry a session token the wallet signs in for (SIWE: the
venue's login message signed as a personal message; the session is renewed
before it lapses). The first sign-in of a wallet creates its Myriad account
and ties it to that wallet; orders must be that wallet's own.

An order trades one outcome of a binary market. `buy` buys outcome 0 (YES) at
the price given; `sell` buys outcome 1 at `1 - price`; with `reduce_only`,
`sell` sells outcome 0 held and `buy` sells outcome 1 held. Prices are whole
cents and sizes shares to 18 decimals. Limits rest until cancelled (`gtc`), or
until `expires_at` (`gtd`), post-only (`PO`) on request; `market` and `ioc` go
as the venue's fill-and-kill (`FAK`) at the price given as the worst accepted,
`fok` as its fill-or-kill. The matcher settles fills on chain after the order
is accepted, so what an order filled is read from the order, its stream, or
the account's history. `create_orders` sends up to 200 a request, and the
venue checks the collateral of a batch at match time rather than on arrival.
A client order id is sent only with an API key, as the venue requires.

A cancel repeats the order and its signature. Orders placed through the
adapter are remembered; any other is read back from the venue and signed
again, which gives the same signature. `cancel_all_orders` is one signed
`CancelAll`, for a market or the whole account. `edit_order` is the venue's
atomic cancel-and-place: the matcher never sees both orders, and the queue
place is lost.

Fees are taker-only: a share of the notional by price, from the market's own
table (3 bps at 1c, 180 bps at 50c). Before the first order the wallet
approves USD1 and its outcome shares to the exchange: `approval_calls()`
returns the two transactions for the wallet to send. `position_calldata()`
returns the venue's calldata to split collateral into YES and NO, merge them
back, or redeem after resolution.

Orders are read by status (`open`, `filled`, `cancelled`, `expired`). The
account's trade history names neither the order behind a fill nor its fee,
so `fetch_my_trades` is partial; `MyriadUserStream` carries orders, fills
from the wallet's public tape with their orders and fees, and share balances
([Streaming](streaming.md)). Positions come from the venue's portfolio, order
book and AMM alike, netted per binary market on the YES leg. The balance is
the wallet's USD1 on BNB Chain, less what open buy orders reserve.

AMM markets (`myriad:56-amm-120473`, one outcome of a larger pool
`myriad:2741-amm-412-3`) trade as market orders: each is one transaction from
the wallet to the network's `PredictionMarket` contract, which pays its gas in
the chain's coin. There is nothing to rest, so a pool order is `market`,
`ioc` or `fok` with a worst price; a `gtc` or `gtd` limit and post-only are
refused. The worst price bounds the average price, fees in: the order is sized
from the pool read on chain, `market` and `ioc` trading the most shares up to
the quantity whose average is within it, `fok` the whole quantity or nothing
(an order that fits nothing is `canceled` and sends nothing). The bound goes on
chain as the contract's own guard, the minimum shares a buy's value must buy
(`value / worst`) or the most shares a sell may burn for its payout, so the
transaction reverts rather than fill worse if the pool moves first. YES and NO
are outcomes 0 and 1 of a two-outcome pool, as on the order book; on one
outcome of a larger pool YES is that outcome, and NO, the rest of the pool, is
not one trade. A sell (`reduce_only`) checks the shares held, a buy the
token's balance and its approval: `approve_amm(market_id)` approves the
market's token (USD1 or USDT, USDC.e, USDC) to that network's contract once.
`create_order` waits for the receipt (`params={"wait": False}` returns it
`pending`, its id the transaction hash, for `fetch_order` to read again) and
reports the fill from the receipt's `MarketActionTx`: shares, average price and
the fee, a share of the value set per market (`share_of_value`; on a buy it
comes out of the value spent, on a sell it is on top of the payout). The price
is the value over the shares, fees in, as the account's history gives it, and
the fill's id is the history's, so `fetch_my_trades`, which reads order-book
and pool trades alike, shows the same trade once. A reverted trade comes back
`rejected`. `plan_amm_order(request)` sizes an order and builds its call
without sending it, and `redeem_amm(market_id)` claims a resolved market's
winnings (or a voided one's shares). Nodes default to public ones;
`MYRIAD_RPC_URL` sets BNB Chain's and `rpc_urls={2741: ..., 59144: ...}` the
others.

Check Myriad's terms for where it may be used before trading.

## Gemini

`GeminiTrading`, on Gemini's private REST API for Prediction Markets. It needs
a Gemini account with Prediction Markets access, which Gemini offers in the
US, and an API key from exchange.gemini.com/settings/api: account-scoped,
with the Trader role, and a time-based nonce, which the account stream
requires. The account must have accepted Gemini's current Prediction Markets
terms: `fetch_terms_status()` says whether it has, `fetch_terms()` reads
them, and `accept_terms()` accepts them for the account group. Nothing here
accepts them on its own; an order sent before is refused with
`PermissionDenied`.

Every private request sends an empty body. The JSON payload (the request path,
a nonce, and the endpoint's fields) goes base64-encoded in `X-GEMINI-PAYLOAD`,
signed with HMAC-SHA384 under the API secret into `X-GEMINI-SIGNATURE`. The
nonce is Unix seconds, which a time-based key accepts within 30 seconds of
Gemini's clock. Signed requests go one at a time, as Gemini's own SDK sends
them, under the venue's 600 requests a minute. A rate-limited read is signed
again and retried; a write is sent once.

An order names its outcome. `buy` at *p* is sent as buying YES at *p*;
`sell` at *p* as buying NO at `1 - p`, Gemini's way to hold NO. With
`reduce_only`, the order sells what is held instead: `sell` sells YES at *p*,
`buy` sells NO at `1 - p`. Orders, fills and positions read back on the YES
leg, with the venue's side and outcome in `info`. Positions net on YES:
`inventory_yes` and `inventory_no` keep what each outcome holds, and
`entry_price` is on the YES leg.

The venue takes limit orders. `market` and `ioc` are sent as
`immediate-or-cancel` at the price given as the worst accepted, `fok` as
`fill-or-kill`; post-only is `makerOrCancel`. There is no good-till-date, no
amend and no client order id on the wire: `client_order_id` stays on the
returned `Order`. Prices and quantities are checked against the cent tick
and the hundredth-contract grid unless a `Precision` from the contract's own
grid is passed. `create_orders` and `cancel_orders` send twenty a request;
Gemini checks a whole batch before sending any of it.

There is no single-order route, so `fetch_order` searches the active orders
and the latest thousand of the history. `fetch_orders` reads the history
(`filled` or `cancelled`) by offset, or a time window with `since`. There is
no fills route either: `fetch_my_trades` is one aggregate fill per order at
its average price (`info["aggregate"]`), and `GeminiUserStream` carries each
execution with its fee and whether it made or took ([Streaming](streaming.md)).
`fetch_settlements` reads settled positions with their payouts.
`fetch_balance` is the account's USD: `available` to trade, `total`
confirmed.

Fees follow the contract's listing date and are rounded up to the cent; see
[Gemini](capabilities.md#gemini). `fetch_fee_estimate` reads the contract's
schedule with one public request.

A combo (two to six contracts, each on a YES or NO outcome, settling YES
only if every leg does) is its own instrument, `gemini:GEMI-CMB-...`, with
its own book, orders and positions: `create_order`, `fetch_orders` and
`fetch_positions` take it as they take any contract. `fetch_combos` and
`fetch_combo` (public) list combos and look one up with its legs, each leg
Gemini's numeric contract id with the outcome the combo needs from it;
any listed contract can be a leg. `create_combo([("101", "yes"), ("202",
"no")])` returns the canonical combo for a set of legs, creating it if
Gemini has none (signed; it places nothing). Gemini's documentation says
these combo routes are not enabled in production yet: there they raise
`CombosUnavailable`, carrying Gemini's answer. The sandbox
(`GEMINI_ENV=sandbox`) has them.

Combo RFQ is a sealed auction on Gemini's WebSocket, in beta. A requester
opens an RFQ and `requestForQuote` broadcasts it, anonymously, with its legs
and its size in contracts or USD. Makers each send one immutable,
single-sided quote (the price they sell the combo's YES at) within one
second. At the close only the best eligible quote (lowest price, then
earliest) is disclosed, privately, to the requester, who has five seconds to
accept. The winning maker then has one second to confirm or decline (its
last look). The requester side, opening an RFQ and accepting its quote, is
in the Gemini app and website only, and Gemini publishes no API for it, so
`create_rfq` and `accept_quote` raise `NotSupported`.

The maker side is `synpath.ws.gemini.GeminiRfqStream`:

```python
from synpath.ws.gemini import GeminiRfqStream

async with GeminiRfqStream(creds) as stream:     # GEMINI_ENV=sandbox for the sandbox
    await stream.watch_rfqs()                    # public, anonymous
    await stream.watch_deliveries()              # this account's acceptances and outcomes
    async for event in stream:
        if getattr(event, "name", None) == "rfq" and event.payload["state"] == "OPEN":
            await stream.submit_quote(event.payload["rfq_id"], "0.55", "100")
        elif getattr(event, "name", None) == "rfq_delivery" and event.payload["transition"] == "ACCEPTED":
            await stream.confirm_quote(event.payload["rfq_id"], event.payload["quote_id"], True)
```

| Request, quotes, accept | Gemini |
|---|---|
| Ask for a price | the requester opens an RFQ in the Gemini app; no API |
| Quotes arrive | sealed: none while the window is open; at close one `rfq_delivery` with `transition="CLOSED"` and the winning quote (`quote_id`, `price`, `size`), to the requester only |
| Quote expires | five seconds after the close (`PENDING_ACCEPTANCE`), or at the RFQ's `expires_at` if sooner; `valid_until` only decides eligibility at the close |
| Accept | the requester, in the app; no API |
| Maker confirms | `confirm_quote(rfq_id, quote_id, True or False)` within one second of the `ACCEPTED` delivery |
| Fill, position | the RFQ ends `FINALIZED` (`fill_quantity` is the requester's); the trade is an order on the combo's instrument, on `GeminiUserStream` and `fetch_positions` |

Quotes are checked before sending: a price on the cent tick inside (0, 1), a
quantity on the hundredth-contract grid that must cover the whole request
to win (`quantity * price >= notional` on a USD-sized RFQ), a `client_id`
of at most 36 printable ASCII characters (it comes back as the maker fill's
client order id), and an RFQ the stream saw close is refused. A quote
method is sent only on a live connection and never resent: no answer, or a
connection lost before one, is an unknown outcome to reconcile on the
delivery stream. Gemini's errors come back typed with its code (`-2010`, a
quote it refused, is `OrderRejected`). Deliveries are at least once and are
deduplicated by Gemini's delivery id. The maker's order rests on the book
before the requester's fill-or-kill order arrives, so it can fill in part,
or not at all, whatever the RFQ's outcome: read the maker's fills from
`GeminiUserStream`, not from the RFQ's state. Quoting needs an
account-scoped key on the signed handshake and an account Gemini has made
eligible; `withdraw_quote` also needs the session's `cancel_orders`.

WebSocket order placement and `cancelOnDisconnect` are not offered.

## Novig parlay RFQ

Novig sends every parlay a user builds (2 to 20 legs, as outcome ids) to
each registered liquidity provider's pricer as an RFQ. The auction lasts 3
seconds; the lowest `price` wins (a tie goes to the larger `max_wager`),
and the winner gets a 1-second last look at the user's actual stake to
confirm or reject. `synpath.trading.novig_rfq.NovigPricer` is that pricer:
you supply the pricing callback, and it answers over the maker WebSocket,
over webhooks, or both. Registering is self-serve but needs an LP account
(developers@novig.com), and the pricer is separate from order entry: the
RFQ routes take Novig's OAuth market-maker bearer token, not a NOVIG-V3
key.

```bash
NOVIG_RFQ_CLIENT_ID=...         # the OAuth client credentials Novig issues to LPs
NOVIG_RFQ_CLIENT_SECRET=...     # (or NOVIG_RFQ_ACCESS_TOKEN, a token used as it is)
NOVIG_RFQ_SHARED_SECRET=...     # webhooks only: 64 hex characters, returned once by POST /rfq/pricer
NOVIG_RFQ_ENV=qa                # production (the default) or qa; registrations are separate
```

```python
from decimal import Decimal

from synpath.trading.credentials import load_novig_rfq
from synpath.trading.novig_rfq import NovigPricer, ParlayQuote

async def price(rfq):                      # a ParlayRFQ: rfq_id, outcome_ids, min_wager, deadline()
    if rfq.legs > 6:
        return None                        # decline
    return ParlayQuote(price=Decimal("0.18"), max_wager=Decimal("500"), quote_id="mm-1")

pricer = NovigPricer(price, credentials=load_novig_rfq())

async with pricer.stream() as stream:      # the maker WebSocket
    async for event in stream:             # every frame as a VenueEvent, and the stream's status
        print(event)

app = pricer.webhook_app(prefix="/novig")  # or webhooks: POST /novig/quote, /confirm, /ping
```

**A round.** `price` gets each round as a `ParlayRFQ` and returns a
`ParlayQuote` or `None`, and may be a coroutine. `price` is the taker's
stake per $1 of payout (0.18 means the taker stakes $0.18 to win $1.00, so
the pricer puts up $0.82 a dollar); it is rounded up onto Novig's 0.001
grid, and a quote Novig would drop (a price outside 0.001 to 0.999, a
`max_wager` of zero, a `quote_id` over 256 bytes) is refused before it is
sent. The callback has until the auction closes, less `margin` (0.25 s); a
slower answer, an exception, or a `max_wager` below the round's minimum
stake declines. On the socket a decline is sent as `decline`, on a webhook
as `204`. A round reaching both transports is priced once and Novig keeps
the first quote.

**The last look.** `confirm` gets each win as a `LastLook` (the stake,
the price, and the pricer's own round and quote) and returns whether to
take it, within `confirm_timeout` (0.75 s) or the trade is rejected.
Without one, the pricer confirms a trade that matches the quote it sent: its
price, no more than its `max_wager`, its `quote_id`.

**The confirm rate.** Novig locks a pricer out of quoting for 5 minutes
when it confirms fewer than 60% of its last looks over 30 minutes, once 20
count. `pricer.confirm_rate` keeps the same count (`standing()`,
`would_lock(False)`, `rejects_left()`), records a lock from the socket's
`FORBIDDEN` error, and the pricer stops quoting while one is known.
`await pricer.sync_standing()` takes Novig's own count from
`GET /rfq/pricer`, which also sees the last looks this process did not.

**Webhooks.** `webhook_app` checks `X-Novig-Signature` (a BLAKE3 keyed hash
of the raw body under the shared secret; the `blake3` package is used when
installed, a pure-Python version otherwise) on every request and answers a
bad one `401` without reading it. Serve it over public HTTPS at the base URL
you register. `handle_webhook(route, raw_body, signature)` is the same logic
for another web framework.

**Executions and collateral.** `NovigRFQ` reads the RFQs won
(`fetch_executions`, newest first, paged with `after`; `iter_executions`),
catches up after a gap (`reconcile_executions(known_ids)`, paging from the
newest until a held id), and the collateral held in open parlays
(`fetch_collateral`). `NovigExecutionFeed` is the public tape of every RFQ
execution, marking a jump in `offset` as a gap. The maker socket is never
replayed, so a reconnect is marked `reconcile_required`.

```python
from synpath.trading.novig_rfq import NovigRFQ, settle

async with NovigRFQ(load_novig_rfq()) as rfq:
    print(await rfq.fetch_pricer())                 # registration, confirm standing, any lock
    for row in await rfq.fetch_executions(status="open"):
        print(row.rfq_id, row.wager, row.price, row.liability)
    print(await rfq.fetch_collateral())

settle("42", "0.42", [1, "0.5", 1])   # a voided leg at 0.5: the taker gets $50 of the $100 pot
```

**Settlement.** A parlay settles as one position: any losing leg settles it
at once as a loss, a voided leg counts at its fair market value, and the
taker gets the pot times the product of the leg values (rounded to three
places, at least 0.001). `settle` and `settlement_scalar` compute it;
`RFQExecution.pricer_payout` reads it off a settled execution.
`eligibility_problems` checks legs against Novig's published combination
rules (2 to 20 legs, no same-kind lines on one game, no NO side of a
yes-or-no market, ...), from leg details you supply; Novig changes the
blocked pairs over time, so its own answer is the RFQ arriving or not.

`register_pricer(webhook_url=None)` and `ping_webhook()` are there for the
one-time setup; the shared secret is returned only by the first
registration with a webhook URL, so store it then.
