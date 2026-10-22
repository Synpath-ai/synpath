# RFQ and combos: research and plan

Status: **P1 (models), P2 (Polymarket US adapter), P3 (engine, taker), P4 (engine, quoter) and P5 (serve routes) built; P6 (Kalshi adapter) built; P7 (Polymarket global combos, RFQ and Exchange v3 signing) built; P8 (SX Bet parlay quoter) built; the two engine gaps (pushed incoming RFQs, Polymarket global restarts) closed** (2026-10-21, "Gaps closed" below); **cross-venue RFQ built** (2026-10-22, §10). Decisions from Yuki are in §2 and §9. Experimental line only (base
`origin/codex/order-lifecycle-storage`, never merged into `main`). Nothing here
has been run against a live or demo account. Anything marked UNVERIFIED is
not confirmed by a venue document or a public read.

## 1. What each venue offers

| Venue | RFQ | Combos / multi-leg | How a combo trades | Public activity signal |
|---|---|---|---|---|
| Kalshi | both (requester and quoter), REST, WS, FIX | venue-created combo **markets** in multivariate event collections | ordinary binary market; RFQ in practice | sampled combo books empty; volume present |
| Polymarket (global) | both, separate RFQ gateways | combo **positions** built from 2–50 legs, on-chain | RFQ only, signed Exchange v3 order | 50+ combo-enabled markets in the public catalogue |
| Polymarket US | both, on the retail API | user-created combo **instruments**, 2–50 legs (FAQ says 2–10) | ordinary order book; RFQ optional | anonymous RFQ prints (needs auth) |
| SX Bet | quoter only over the API | parlay **market** created per request, lives 60 s | makers post orders; bettor takes FOK/IOC | public WS channel `parlay_markets:global` |
| Limitless | none on the API (UNVERIFIED: "RFQ purchases" exist outside it) | curated **Packs**, a house-backed parlay with 2–10 legs | quote, then buy against a treasury vault | 37 active packs with 2–8 legs (public list) |
| Smarkets | none | venue-created accumulator markets (`domain=accumulator`) | ordinary back/lay contracts | 0 accumulator events listed today |
| Betfair | none | none on the Exchange API (UNVERIFIED; accas are a Sportsbook product) | — | — |
| Opinion | none | none | — | — |
| Hyperliquid | none | none (HIP-4 outcome markets only) | — | — |
| predict.fun | none | none | — | — |

### Kalshi
- **RFQ endpoints** (signed with the trading key; any API member; whether quoting needs maker status is UNVERIFIED, though collections list `active_quoters` per event):
  `POST/GET /communications/rfqs`, `GET/DELETE /communications/rfqs/{id}`,
  `GET/POST /communications/quotes` (filters `rfq_id`, `rfq_user_filter=self`, `user_filter=self`),
  `GET/DELETE /communications/rfqs/{rfq}/quotes/{q}`,
  `PUT .../accept` with `{accepted_side: yes|no}`, `PUT .../confirm` (quoter), `GET /communications/id`.
  The `/communications/quotes/{id}/...` forms are deprecated. Source: https://docs.kalshi.com/openapi.yaml (v3.34.0).
- **Semantics** (https://docs.kalshi.com/getting_started/rfqs.md): an RFQ is sized by `contracts_fp` or `target_cost_dollars`. A quote carries `yes_bid` and `no_bid` for the full size, with either one "0" and their sum at most $1. The requester accepts one side, and the maker must confirm within 30 s (3 s on combo/HVM markets). Orders reach the book after 15 s (1 s on HVM markets). Fills appear in `/portfolio/fills`, matched on `rfq_creator_order_id`. At most 100 open RFQs, one per market (409 otherwise). REST accept takes no client id; FIX accept takes `ClOrdID` and a partial `OrderQty`.
- **Side mapping, UNVERIFIED:** the docs define `yes_bid` as the maker's YES bid, so buying YES costs `1 − no_bid` and selling YES gets `yes_bid`. The REST docs call `accepted_side` "the side of the quote to accept". FIX says "BUY accepts the maker's NO quote". Either way the economics are fixed, but whether Buy YES sends `accepted_side=no` or `yes` must be confirmed on the demo before anything ships. The same doubt applies to `yes_contracts_fp`/`no_contracts_fp`.
- **Combos:** `GET /multivariate_event_collections` (public; 3 open today: `KXMVECROSSCATEGORY-R`, `KXMVECROSSCATEGORY-SHARD1-R`, `KXMVESPORTSMULTIGAMEEXTENDED-R`) and `GET /multivariate_event_collections/{t}`. `POST /multivariate_event_collections/{t}` creates or returns the market for `selected_markets:[{market_ticker,event_ticker,side}]`. It is signed, capped at 5,000 a week, and "must be hit at least once before trading or looking up a market". There is no lookup endpoint left in the spec. `GET /events/multivariate?collection_ticker=` lists existing combos, and each market carries `mve_collection_ticker` and `mve_selected_legs`. A combo is a normal binary market (`KXMVE…` ticker) with its own book, positions and settlement. REST RFQs need `market_ticker`, so a combo is created before its RFQ. (FIX can send legs directly.)
- **Activity** (public read, 2026-10-21): in one page of 170 cross-category combos, 7 had volume (1,509 contracts in total). All 23 multi-game combos had volume (16,000 in total). Every sampled book was empty, so liquidity comes from RFQ.

### Polymarket (global)
- **Requester:** `https://combos-rfq-gateway-requester-api.polymarket.com/v1/requester/rfq`. `POST /requests` returns the best quote at once. `POST /requests/{id}/accept` takes a signed **Exchange v3** EIP-712 order. Poll `GET /requests/{id}` (FILLED/CONFIRMED, or FAILED/EXPIRED/CANCELED). Auth is CLOB L2 HMAC. EOA wallets are refused; Deposit, Proxy or Safe wallets only. Only `side: "YES"`. A BUY is sized in notional, a SELL in shares. You get 5–10 s to accept (the docs disagree), and there are 15 creates a minute. The docs warn there is **no idempotency key** on create. Source: https://docs.polymarket.com/trading/combos/requesters.md, /trading/combos/overview.md.
- **Quoter:** a WS session plus `/v1/maker/quotes`, `/v1/maker/quotes/cancel` and `/v1/maker/confirmations`. Quotes are due within 400 ms; last look is 1 s. On-chain approvals are needed. No approval programme is stated, only a Telegram group. Source: https://docs.polymarket.com/trading/combos/market-makers.md, https://docs.polymarket.com/api-spec/combos-rfq-openapi.yaml.
- **Catalogue:** `GET https://combos-rfq-api.polymarket.com/v1/rfq/combo-markets` (public).
- **Exchange v3 signing:** built in P7 (`synpath/trading/polymarket_v3_signing.py`), beside the CLOB V2 signing, which it does not change.

### Polymarket US (retail API, the same Ed25519 key as orders)
- **Combos:** `POST /v1/combos {legs:[{symbol, side: SIDE_BUY|SIDE_SELL}]}` is canonical: the same legs always give the same `caoc-…`/`crossd-…` symbol, with a quota of 1,000 a week. `GET /v1/combos?symbol=`.
- **Eligibility:** no combo-markets list. Each gateway market carries `comboEnabled` (public `GET /v1/events`, markets nested; no filter on it, so it is read per event), and the docs say there is no way to validate legs before `POST /v1/combos`. Read 2026-10-22: 54 of 862 markets in the top 100 open events were combo-enabled, all sports.
- **RFQ:** `GET/POST /v1/rfqs` (`qtyDecimal` or `cashOrderQty`, `restRemainder`), `DELETE /v1/rfqs/{id}`, `GET/POST /v1/rfqs/quotes`, `DELETE …/quotes/{q}`, `PUT …/accept {acceptedSide}`, `PUT …/confirm`, `GET /v1/rfqs/trades`, `GET /v1/rfqs/user-id`. Combo and RFQ creation share a limit of 10 per 10 s.
- **Unambiguous sides:** `buyPrice` is the price for a requester BUY, and `acceptedSide=SIDE_BUY` takes it.
- **Execution:** the exchange submits two ordinary orders, the maker's first. The requester's `clord_id` is `rfq1.…` and deterministic, so a retried accept cannot make a second order. `rfqCreatorOrderId` is ours. EXECUTED means submitted, not filled. With `restRemainder: false` the order is IOC, so it can fill partly. Last look is 3 s, then 1 s before submission. A combo settles on its own symbol at the product of its leg values, and shows up in positions like any instrument.
- Sources: https://docs.polymarket.us/api-reference/combos/overview.md, /api-reference/rfqs/overview.md, /partners/orders/combos-takers.md, /trader-guide/combos.md, /faqs/combos-faqs.md, and the schemas under /api-reference/oapi-schemas/{combos,rfqs}-schema.json.

### SX Bet
- A bettor's parlay request makes a real market (`marketHash`, `legs[{marketHash, bettingOutcomeOne}]`) that expires after 60 s. Makers hear about it on the Centrifuge channel `parlay_markets:global`, using a token from `GET /user/realtime-token-v3/api-key` (needs an API key). They post ordinary signed orders, which must expire by then. The taker uses FOK/IOC. Fees are `makerParlayPayoutFee`/`takerParlayPayoutFee` (`GET /user/fees-v3`); the user guide says 5% on profit.
- There is **no documented API to create a parlay request**, so the requester side is UNVERIFIED and appears to be UI-only.
- Settlement wording conflicts: the developer docs say one VOID leg voids the parlay (every stake back), while the user guide says a VOID or push leg loses a parlay WIN bet and a parlay LOSE bet needs no VOID leg to win. Synpath takes the worse reading for the side held (P8).
- The request carries no expiry and no bettor side for the parent; `requestSize` is a stake in base units that "does not limit how much you can offer". An order signed past the parlay's end is refused with `PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY` and the exact end (`parlayExpiry`, unix seconds); after the end, new orders get `INVALID_MARKET`; an inactive leg gives `PARLAY_LEG_INACTIVE` (OpenAPI `ErrorOrderRule`). A parlay fill's `marketHash` is the parent. Sources add /api-reference/channel-parlay-requests.md, /developers/time-in-force.md, /openapi.json.
- Sources: https://docs.sx.bet/developers/parlays.md, /developers/market-making-parlays.md, /api-reference/parlay-markets.md, /user-guides/trading/parlays.md.

### Limitless
- Packs are curated by the venue and backed by a treasury vault. They are not an order book.
- Public endpoints: `GET /parlay/packs`, search and detail.
- Partner endpoints: `POST /parlay/partner/quote` and `/buy` (HMAC with the `trading` and `delegated_signing` scopes; buy takes an idempotency key), plus positions and claim.
- Stakes run $1–70, up to 20x, with a 10% vig.
- The docs mention "custom baskets and RFQ purchases" outside this API (UNVERIFIED).
- Source: https://docs.limitless.exchange/user-guide/packs.md, /developers/partner-packs.md.

### Smarkets, Betfair, Opinion, Hyperliquid, predict.fun
- **Smarkets:** the trading OpenAPI (https://api.smarkets.com/v0/control/openapi/?tags=trading_api) describes accumulator markets, built by the venue, whose `contract_selections` name the component contracts. It has no endpoint to create one, and `GET /v3/events/?type_domain=accumulator` returned none today.
- **Betfair:** placeOrders takes single selections only; no multiples are documented (https://betfair-developer-docs.atlassian.net, UNVERIFIED).
- **Opinion, Hyperliquid, predict.fun:** no RFQ or combo terms in their doc indexes (https://docs.opinion.trade/llms.txt, https://hyperliquid.gitbook.io/hyperliquid-docs/llms.txt, https://dev.predict.fun/llms.txt).

## 2. Scope (Yuki, 2026-10-21)

Every venue whose API allows it, on both sides, plus combos. The quoter side
is in scope from the start.

| Venue | Taker (`rfq_taker`) | Quoter (`rfq_quoter`) | Combos (`combos`) | Notes |
|---|---|---|---|---|
| Polymarket US | yes | yes | yes | user-created combo instruments, on the API we already sign |
| Kalshi | yes | yes | yes | combos through multivariate event collections |
| Polymarket global | yes | yes | yes | needs **Exchange v3 order signing** (new) and a non-EOA wallet |
| SX Bet | no | yes (parlay) | parlay markets, quoter side | no API to request a parlay |
| Limitless | no | no | no | not in scope, see below |
| Smarkets, Betfair, Opinion, Hyperliquid, predict.fun | no | no | no | not in scope, see below |

**Not in scope, with the reason:**
- **Limitless:** Packs are a house-backed parlay (a treasury vault, not RFQ), and the
  quote and buy endpoints need partner credentials (HMAC with the `trading` and
  `delegated_signing` scopes and an owned managed server-wallet sub-account).
  Deferred (Yuki, 2026-10-21); revisit if partner access is wanted.
- **Smarkets:** accumulator markets exist but are venue-built, with no create
  endpoint and none listed today. They trade as ordinary contracts already.
- **Betfair:** placeOrders takes single selections; multiples are a Sportsbook product.
- **Opinion, Hyperliquid, predict.fun:** no RFQ or combo API.

The capability keys go True phase by phase, when the adapter that answers them
lands (§8); until then every venue declares them False. A venue outside the
scope column above never claims them.

## 3. Model (in `synpath/trading/types.py`)

Every price is a YES price and every object keeps the venue's raw payload in
`info`, as for orders. Every outcome and action follows `OrderRequest`.

- `ComboLeg {market_id, outcome: yes|no, event_id?, info}`. On Polymarket US, `outcome=no` means `SIDE_SELL`.
- `Combo {market_id (the combo's own id, kalshi:KXMVE… / polymarket_us:caoc-…), venue, collection_id?, legs[2+], status?, created_at?, info}`.
- `Rfq {id, venue, account?, market_id | combo_id (exactly one), combo?, outcome?, action?, quantity | amount (exactly one), status, closed_reason?, mine, created_at?, expires_at?, updated_at?, info}`. `combo_id` is the combo's own market id; `combo` carries its legs where known. `outcome` and `action` are `None` on a two-sided request (Kalshi, Polymarket US); Polymarket global asks one side.
- `RfqQuote {id, rfq_id, venue, market_id?, bid, ask, bid_quantity, ask_quantity, status, accepted_side?, confirm_by?, execute_by?, expires_at?, taker_order_id?, maker_order_id?, mine, created_at?, updated_at?, info}`. `ask` is the YES price we can buy at (Kalshi `1−no_bid`, Polymarket US `buyPrice`); `bid` the YES price we can sell at (Kalshi `yes_bid`, Polymarket US `sellPrice`). A side the maker declined is `None`. Named `RfqQuote` because `synpath.Quote` is already the market-data quote.
- `AcceptQuote {quote_id, rfq_id?, outcome, action, worst_price?, quantity?, amount?, client_order_id?, book, trader, tags}`. `worst_price` is a YES price, like every price.
- `MakerQuote {rfq_id, bid?, ask?, quantity?, expires_at?, client_order_id?, rest_remainder, post_only, book, trader, tags}`. At least one of `bid`/`ask`; `bid < ask`. `post_only` (added in P2) is Polymarket US `postOnly`.
- Status enums `RfqStatus`, `RfqCloseReason`, `QuoteStatus` (§4).
- Capability keys `rfq_taker`, `rfq_quoter` and `combos`, declared by every trading adapter. The old `rfq` key stays for one release: it says the adapter has the raw Kalshi RFQ methods.

### Sizing by amount

An RFQ is sized by contracts (`quantity`) **or** money (`amount`), never both.

- Where the venue takes money natively (Kalshi `target_cost_dollars`,
  Polymarket US `cashOrderQty`, a Polymarket global BUY) the amount is sent as is.
- Otherwise it is converted at accept time: `contracts = amount ÷ price`, where
  `price` is the quote's price for the token traded, rounded **down** to the
  venue's quantity step (`VENUE_RULES` / `Precision`), and refused below the
  venue's minimum quantity or notional. `worst_price` caps the spend: a quote
  worse than it is refused, so the spend is at most `contracts × worst_price ≤ amount`.
- The inverse (`contracts × price`) is for display.
- Helpers: `synpath.trading.money.contracts_for_amount`, `amount_for_contracts`,
  `outcome_price`; `synpath.trading.instruments.venue_precision` for a venue with no market loaded.

## 4. Lifecycle

- **RFQ** (`RfqStatus`): `open` → `closed`, with `closed_reason` one of `canceled`
  (we deleted it), `expired`, `executed` (a quote traded) or `replaced`.
- **Quote** (`QuoteStatus`): `open` → `accepted` (by the requester) → `confirmed`
  (the maker's last look) → `executed` (orders submitted) → order and fills.
  Before `executed` a quote can end `canceled` (the maker withdrew or the RFQ
  closed), `expired` (no accept, or no confirm, by the deadline) or `failed`
  (the venue reported the execution failed). Terminal: `executed`, `canceled`,
  `expired`, `failed`.
- An executed quote becomes an order: an `rfq_taker`-type `Order` on the quote's
  market, stamped with the caller's outcome and action. Its fills are ordinary fills.

## 5. Engine integration (a new `synpath/engine/rfq.py`, attached as `engine.rfq`)

1. **Accept is an order, and returns at once.**
   - Re-read the quote (it must be `open`), take the price for the requested outcome and action, and refuse if it is worse than `worst_price`. Size an `amount` here (§3).
   - Build an `OrderRequest(type=rfq_taker, outcome, action, quantity, price, params={rfq_id, quote_id})` and run `engine.check`: the kill switch, a paused book, the restricted list, size and notional, position caps, closing soon, daily loss. The price collar is skipped, as for market orders, because combos rarely have a mark.
   - Journal the intent (`operation="accept_quote"`, `target_order_id=quote_id`, key `rfq-accept:{venue}:{quote_id}`), mark it `sending`, call the venue, and return the intent. A network error raises `SubmissionInDoubt` and leaves the intent `sending`.
   - An optional `wait` (with `timeout_s`) blocks until the quote is terminal; the default does not wait.
2. **Results arrive as events.** A follower polls the quote (or reads the venue's stream):
   - `executed`: fetch the taker order id and pass it to `on_order`; read fills (`fetch_my_trades(order_id=)` on Kalshi, the private stream on Polymarket US) and pass each to `on_fill`. Publish `quote.executed`; the intent becomes `sent`.
   - `canceled`, `expired` or `failed` after our accept: publish `quote.failed` with the reason; the intent becomes `failed` ("nothing traded").
   - Book the side the **venue** reports. If it differs from the intent's side, publish `rfq.side_mismatch` and alert (the guard for the unverified Kalshi mapping, until P0 settles it).
3. **Crash recovery.** `Engine._resolve` hands `accept_quote` and `confirm_quote` intents to `engine.rfq.resolve`, which reads the quote: `executed` adopts the order; still `open` after `lost_after_s` marks the intent `rejected` (built that way in P3: nothing traded, and the accept may be retried); `accepted`/`confirmed` stay pending; venue unreachable stays unresolved.
4. **Combo as an instrument.** Positions are keyed by market id, so a combo's position, P&L and settlement live on its own id. Leg definitions go in the journal (`combo.saved`). No schema change: RFQ records are `rfq.*`/`quote.*` events, and the venue is the source of truth for listing them.
5. **Quoter side.** Sending and cancelling a quote commits nothing until a requester accepts, so neither runs the position rules, but both are journalled; sending (and creating an RFQ) is checked against the halt, a paused book, the restricted list and the order size limits (P4). **Confirm** commits capital: it goes through risk and the journal like accept, with `operation="confirm_quote"` and the opposite side.
6. **Halt.** The kill switch (`engine.halt`, `cancel` policy) cancels every open RFQ we created and every open quote we made, on every venue in scope, before cancelling orders; accept and confirm are refused while halted. An accepted quote awaiting our confirm is left to expire rather than confirmed.

## 6. serve routes, events, grants

Routes under `/trading` (final paths, P5; quote ids are flat, with `rfq_id`
in the body or query where the venue addresses a quote under its RFQ):

| Route | Grant |
|---|---|
| `POST /rfqs`, `DELETE /rfqs/{id}` | `trade` |
| `GET /rfqs` (`venue`, `status`, `market_id`, `source`, `limit`), `GET /rfqs/{id}`, `GET /rfqs/{id}/quotes` | `view` |
| `POST /quotes/{id}/accept` (`AcceptQuote` plus `wait`, `timeout_s`) | `trade`; answers 202 at once (or when in doubt), 200 when it waited and the quote is final |
| `GET /rfqs/incoming`, `GET /quotes` (quoter) | `view` |
| `POST /quotes`, `PUT /quotes/{id}`, `DELETE /quotes/{id}`, `POST /quotes/{id}/confirm` (quoter) | `trade` |
| `GET /combos/{market_id}`, `POST /combos/lookup`, `GET /combos/collections` (Kalshi, P6) | `view` |
| `GET /combos/markets` (Polymarket, Polymarket US; `event_id`, `tag`, `limit`, `cursor`) | `view`; cached 15 s with collections |
| `POST /combos` (`collection_id` on Kalshi) | `trade` (a signed write against a weekly quota) |

- Grants apply on the account of the venue the route names.
- `/ws/events` carries `rfq.created`, `rfq.closed`, `quote.received`, `quote.accepted`, `quote.confirmed`, `quote.executed`, `quote.failed`, `quote.canceled`, `rfq.side_mismatch`, and the ordinary `order.*`/`fill.*` events of the resulting order.
- The OpenAPI document picks the routes up by itself; the TypeScript client gains typed methods and the generated types (`npm run types`).

## 7. What the terminal needs (separate terminal PRs, Richard OK'd)

- **T1, RFQ ticket and blotter** (after P5): the ticket's RFQ type ("Max price", "Quote window", contracts or amount) maps to `POST /rfqs`, the `quote.*` events, then accept with `worst_price` = Max price. The quote window is a client timer plus `DELETE /rfqs/{id}`. The blotter (product plan G10) reads `GET /rfqs` and `GET /rfqs/{id}/quotes`.
- **T2, combo builder and combo positions** (after P5 and P2/P6): reads collections (P6; each with `associated_event_tickers`) or `GET /combos/markets` (Polymarket, Polymarket US) to decide which legs are eligible, then `POST /combos` (naming `collection_id` on Kalshi, where today's three collections take the same events); Positions and Fills show a combo as one row with its legs from `GET /combos/{id}`.
- **T3, quoter panel** (after P4): incoming RFQs, quote entry, confirm, and a cancel-all wired to halt.

## 8. Phases and exit criteria

Dependencies: P0 ∥ P1 → P2 → P3 → P4 → P5 → (P6 ∥ P7 ∥ P8) → P10. (P9, Limitless Packs, was removed on 2026-10-21; the number is not reused.) T1 after P5; T2 after P5 and P2/P6; T3 after P4.

| Phase | Work | Exit |
|---|---|---|
| P0 | Kalshi demo check: the accept side (create, read, delete an RFQ; accept if a demo quote appears) and the quoter flow (quote a demo RFQ, confirm). Needs the demo key; **Yuki is told before it runs.** | `accepted_side`, `yes/no_contracts_fp` and confirm mapping settled; samples recorded under `tests/samples/` |
| P1 | models, status enums, capability keys, sizing helpers, exports | `tests/test_rfq_models.py` and contract tests pass; full suite green |
| P2 | Polymarket US adapter: combos, RFQ, accept, quoter (quote, cancel, confirm), typed normalisers | mocked-HTTP tests of every call; its three keys True. **Done 2026-10-21** (below) |
| P3 | engine, taker side: risk, intent, follow, events, recovery, ledger, amount sizing | accept → order and fill → position; a crash at each step recovers; a risk refusal sends nothing; SQLite and PostgreSQL. **Done 2026-10-21** (below) |
| P4 | engine, quoter side: quote, cancel, confirm through risk and journal; halt cancels every open RFQ and quote | confirm → order and fill; halt leaves no open RFQ or quote (mocked); recovery of `confirm_quote`. **Done 2026-10-21** (below) |
| P5 | serve routes, events, grants, TypeScript client | route tests including 403 for view-only, 202 by default and 200 with `wait`; OpenAPI lists the routes; TS client typechecks. **Done 2026-10-21** (below) |
| P6 | Kalshi typed adapter: RFQ both sides, multivariate collections and combo create | mocked tests; demo test updated (`demo` marker); its three keys True. **Done 2026-10-21** (below) |
| P7 | Polymarket global: Exchange v3 EIP-712 signing, requester and maker gateways, combo catalogue | signing vectors match the venue's published examples; mocked tests; its three keys True. **Done 2026-10-21** (below; the venue publishes no v3 vectors, so they are an independent recomputation of its documented payloads) |
| P8 | SX Bet parlay quoter: `parlay_markets:global` stream, orders that expire with the parlay | mocked stream and order tests; `rfq_quoter` and `combos` True. **Done 2026-10-21** (below) |
| P10 | tiny live tests, one per venue: one small RFQ (or quote) executed and reconciled. Pre-approved by Yuki; **Yuki is told before each run.** | one execution per venue reconciled against venue positions and fills |

### Typed interface (P2, 2026-10-21)

Defined on `TradingExchange` with `NotSupported` defaults, so the engine (P3)
and the Kalshi (P6) and Polymarket global (P7) adapters share one set of names:

| Side | Method |
|---|---|
| combos | `create_combo(legs) -> Combo`, `fetch_combo(combo_id) -> Combo`, `lookup_combo(legs) -> Combo \| None` |
| taker | `create_rfq(instrument_id, *, quantity \| amount, outcome?, action?, rest_remainder=False, combo?) -> Rfq`, `fetch_rfq(id) -> Rfq`, `fetch_rfqs(*, status?, market_id?, limit?, cursor?) -> Page[Rfq]` (own), `cancel_rfq(id) -> None`, `fetch_quotes(rfq_id, *, status?) -> list[RfqQuote]`, `fetch_quote(quote_id, *, rfq_id?) -> RfqQuote`, `accept_quote(AcceptQuote) -> RfqQuote` |
| quoter | `fetch_incoming_rfqs(*, market_id?, limit?, cursor?) -> Page[Rfq]`, `send_quote(MakerQuote) -> RfqQuote`, `replace_quote(quote_id, MakerQuote) -> RfqQuote`, `cancel_quote(quote_id, *, rfq_id?) -> None`, `confirm_quote(RfqQuote) -> RfqQuote`, `fetch_my_quotes(*, rfq_id?, status?) -> list[RfqQuote]` |

A missing RFQ, quote or combo is `RfqNotFound` (a subclass of `OrderNotFound`).
`KalshiTrading`'s raw RFQ methods were replaced by these in P6 (`delete_rfq` is now `cancel_rfq`);
the `rfq` key stays True for one release.

### P2 result (Polymarket US, offline, 2026-10-21)

Built in `synpath/trading/polymarket_us.py`, tested offline in
`tests/test_polymarket_us_rfq.py` (samples `tests/samples/polymarket_us_rfq_*.json`).
`rfq_taker`, `rfq_quoter` and `combos` are True on `PolymarketUSTrading` only. Decisions:

- **Ids.** A combo symbol is a market slug like any other (the docs use the slug as `symbol` everywhere), so a combo's id is `polymarket_us:caoc-…`/`polymarket_us:crossd-…` and its orders and positions use it.
- **Combos.** `create_combo` is idempotent (same legs, same symbol; an existing combo does not use the weekly quota). The venue has **no read-only lookup by legs**, so `lookup_combo` is `NotSupported` and says to use `create_combo`. A venue 400 on legs (incompatible, redundant, not tradable) is `InvalidOrder` with the venue's message. Instrument state `OPEN` is `open`; every other state reads `closed`.
- **Sides.** `ask` = `buyPrice`, `bid` = `sellPrice`, both YES prices; "0" is a declined side (`None`). `acceptedSide` is the **requester's** side: buy YES or sell NO is `SIDE_BUY`, sell YES or buy NO is `SIDE_SELL`. Evidence: the schema says `buyPrice` is the "price offered for a requester BUY order; the quote creator takes SELL"; the trader guide maps `SIDE_BUY` to `buyPrice`/`buyQtyDecimal` and a maker sell; the takers page says a requester sell costs `1 − price`. This is the opposite of Kalshi's `accepted_side` (P0).
- **RFQs are two-sided** and have no expiry: `create_rfq` refuses `outcome`/`action`; an RFQ stays open until `cancel_rfq` or an accept. `amount` is sent as `cashOrderQty` (the venue sizes each side, taker fee included, and reports `buyQtyDecimal`/`sellQtyDecimal`). The venue returns only the id, so `create_rfq` returns the RFQ as sent. A closed RFQ has no reason on the venue, so `closed_reason` is `None`.
- **Combo or single market.** An RFQ is on a combo when it carries `comboLegs`, or, failing that, when its symbol starts `caoc-`/`crossd-` (the venue calls symbols opaque; this is a fallback).
- **Accept.** Re-reads the quote; refuses before sending if it is not `QUOTE_STATUS_ACTIVE`, the side is declined, or the price is worse than `worst_price`. No partial accept (`quantity`/`amount` refused). Returns the quote `accepted` without a second read. **Retry:** a quote already accepted on the same side is returned without resending, and the venue's requester `clord_id` (`rfq1.…`) is deterministic per quote and side, so neither layer can double-order. The idempotency key defaults to `rfq-accept:polymarket_us:{quote_id}` (kept in `info`; the venue takes no client id). `taker_order_id` is `rfqCreatorOrderId`, `maker_order_id` `creatorOrderId`; `executed` means submitted, not filled.
- **Statuses.** `ACTIVE`→`open`, `ACCEPTED`, `CONFIRMED`, `EXECUTED`, `DELETED`→`canceled` (withdrawn, replaced or declined in last look). The venue has no expired or failed quote status, so `fetch_quotes(status="expired"|"failed")` is refused.
- **Quoter.** `send_quote` sends `buyPrice` = our ask and `sellPrice` = our bid ("0" for a missing side), `restRemainder` and `postOnly`; the venue takes no quantity, expiry or client id, so those are refused. `replace_quote` posts again on the same RFQ (a new id; the venue deletes the old). `cancel_quote` also declines during last look; `confirm_quote` takes the accepted `RfqQuote`. `mine` comes from `GET /v1/rfqs/user-id`, read once.
- **Not built:** the RFQ WebSocket subscription (`SUBSCRIPTION_TYPE_RFQ`) and `GET /v1/rfqs/trades`. Per-endpoint pacing (GET `/v1/rfqs`, `/v1/rfqs/quotes` and `POST /v1/combos`+`/v1/rfqs` at 1 request/s) was added in P5.
- **UNVERIFIED, for P10:** the error body shape (assumed gRPC-gateway `{code, message}`); whether `RESOURCE_EXHAUSTED` (weekly combo quota) arrives as 429 (then retried as a rate limit) or 400; what status an accepted quote takes when the maker lets the last look lapse; whether `rfqId` + `userFilter` together are accepted (the code sends one filter); whether a crossed two-sided quote can exist (the model refuses `bid > ask`); whether a single-market (non-combo) RFQ is accepted on the retail API (its summary says "combo RFQ").

### P3 and P4 result (engine, offline, 2026-10-21)

Built in `synpath/engine/rfq.py` (`RfqDesk`, attached as `engine.rfq`; the
`Engine` methods delegate to it), with `PaperRfqVenue` and `PaperMaker` in
`synpath/engine/paper.py` and tests in `tests/test_engine_rfq.py` (26, each
on SQLite and, with `SYNPATH_TEST_POSTGRES_DSN`, PostgreSQL).

**Engine API.**

| Side | Method |
|---|---|
| combos | `create_combo(legs, *, venue?) -> Combo`; `engine.rfq.combo(market_id)`; `engine.rfq.save_combo(combo)` |
| taker | `create_rfq(instrument_id, *, quantity \| amount, outcome?, action?, rest_remainder?, combo?, venue?, account?, book?, trader?, tags?) -> Rfq`, `cancel_rfq(rfq_id, *, venue?)`, `accept_quote(AcceptQuote, *, venue?, wait=False, timeout_s=30) -> RfqQuote` |
| quoter | `send_quote(MakerQuote, *, venue?) -> RfqQuote`, `replace_quote(quote_id, MakerQuote, *, venue?)`, `cancel_quote(quote_id, *, rfq_id?, venue?)`, `confirm_quote(quote \| quote_id, *, rfq_id?, venue?, wait=False, timeout_s=30) -> RfqQuote` |
| state | `engine.rfq.rfqs` (our open RFQs), `.maker_quotes` (our live quotes, with the engine-held expiry), `.incoming` (others' open RFQs), `.combos`, `.following` |
| driving | `engine.rfq.tick()` (one follower pass), `engine.rfq.on_quote(quote)` (a stream's entry point), the `rfq` loop in `Engine.background()` |

**Events** (all journaled except `rfq.incoming`): `rfq.created`, `rfq.closed`
(`reason` canceled, executed, expired), `rfq.incoming`, `rfq.in_doubt`,
`quote.received` (a quote on our RFQ), `quote.sent` and `quote.replaced` (ours),
`quote.accepted`, `quote.confirmed`, `quote.executed`, `quote.canceled`,
`quote.expired`, `quote.failed` (an accept or confirm that ended without a
trade, with `reason`), `quote.in_doubt`, `rfq.side_mismatch`, `combo.saved`,
then the resulting order's `order.accepted` and `order.{status}` and its
`fill.booked`. Each quote event is keyed `{venue}:quote:{id}` and carries
`status`; a jump along open → accepted → confirmed → executed between two
reads publishes the steps in between, marked `inferred`. Refusals are
`risk.rejected` with `operation` `create_rfq`, `rfq_taker` or `rfq_quoter`.

**Decisions.**

- **One intent per accept or confirm, `sending` until the quote is final.**
  No new intent state: the accept intent (`rfq-accept:{venue}:{quote_id}`,
  or the caller's `client_order_id`) is written with `synchronous=FULL` before
  the call and stays `sending` through the last look; it becomes `sent`
  (with the order id) when the quote executes and the order is adopted, and
  `failed` when the quote is canceled, expires or fails. So the follower,
  a stream, `wait=True`, the sweep and `recover()` are one function
  (`RfqDesk.resolve`), which reads the quote back. The detail
  `accepted at the venue` / `confirmed at the venue` marks a call answered.
- **Recovery.** A quote still `open` (taker) or `accepted` (quoter) with no
  answered-call mark after `lost_after_s` means the call never landed: the
  intent is `rejected`, nothing traded, and the same accept may be made again
  (the intent reopens). `accepted`/`confirmed` stay pending; `executed` adopts
  the order; unreachable stays unresolved. Creates, cancels and quote sends in
  doubt are read back too: an RFQ or quote of ours on the same instrument
  created since the intent is adopted, otherwise `lost` after `lost_after_s`.
  A retried accept or confirm never sends twice.
- **The resulting order goes through the normal order path**: journaled
  with the intent's request (type `rfq_taker` or the new `rfq_maker`, book,
  trader, outcome and action), `orders.on_order`, then its fills through
  `on_fill` into the ledger. A fill that arrives (stream or poll) before its
  order is adopted makes the desk resolve the followed quotes first
  (`claim_fill`), so it is never booked unattributed. Tags `rfq_id` and
  `quote_id` link the order to its quote.
- **Risk.** Accept and confirm run `Engine.check`, every order rule, on
  the size and YES price that would trade (the price collar skips them, as it
  skips market orders). `worst_price` is checked by the engine first (rule
  `worst_price`). Creating an RFQ and sending or replacing a quote run
  `RiskEngine.check_offer`: the kill switch, a paused book, the restricted
  list, `max_order_contracts` and `max_order_notional` on the most the offer
  could commit; position caps and the daily loss wait for the trade. A confirm
  is booked on our side: it closes a holding where it can (`yes`/`sell` out of
  a long), else opens (`default_leg`).
- **Amount at accept.** `contracts_for_amount` at the quote's price for the
  token traded, down to the venue's step (the market master's precision,
  else `venue_precision`), capped at the quote's size; an amount covering
  the full size takes it whole. Below the venue minimum is refused before
  sending. A venue without partial accepts (Polymarket US) then refuses a
  partial size, so size such RFQs by amount at creation.
- **Side guard.** The order and fills are booked on the side the venue
  reports. A mismatch with the intent (at the accept, on the quote, or on the
  order) publishes `rfq.side_mismatch` once per intent; `Alerts` raises it as
  a critical `rfq_side` alert; the order's outcome and action are cleared
  rather than contradict its side.
- **Combos.** A combo is its own instrument in the ledger, positions and
  reconciliation; its legs are `combo.saved` events (no schema change),
  restored at start and shown as `Position.info["combo"]`.
- **Expiry is the engine's.** `MakerQuote.expires_at` is kept by the engine
  and not sent (Polymarket US refuses one); the follower cancels the quote
  when it passes (`quote.expired`, the cancel intent's detail `expired`).
- **Working sets survive restarts** in the journal's `cursors` table
  (`rfq:open:{venue}`, `rfq:maker_quotes:{venue}`), not a new table.
- **Halt.** With the `cancel` policy, `halt()` cancels every open RFQ we asked
  and every live quote we made, those the engine holds and those the venue
  lists, on every venue in scope, before cancelling orders
  (`result["rfq"]`). `hold` and `rearm` leave them up, as they leave orders;
  accept, confirm, create and send are refused while halted, so an accepted
  quote awaiting our confirm lapses.
- **Polling.** One `rfq` loop every `rfq_poll_interval_s` (1 s) follows
  accepts and confirms, the quotes on our open RFQs and our own quotes; the
  incoming feed polls every `rfq_incoming_interval_s` (2 s, `None` off) on
  venues without an incoming stream. A venue with a
  `subscribe_quotes(listener)` stream is subscribed at start, and one with
  `subscribe_incoming_rfqs(listener)` is pushed, not polled (see "Gaps closed").
- **Not built:** a venue RFQ stream for Polymarket US (P2 left it out), so it
  is polled. Polymarket US's 1 request/s RFQ endpoints are paced in the
  adapter since P5.

### What P5 (serve routes) needs from P3/P4

- Routes call the `Engine` methods above; `POST .../accept` maps `wait` and
  `timeout_s` straight through and answers 202 with the returned quote unless
  it waited and the quote is final (200). `SubmissionInDoubt` is 202 with the
  intent key, as for orders.
- `RiskRejected` carries `rule` (`worst_price`, `kill_switch`, `max_position`, ...)
  for the 4xx body; `BadRequest` for a venue without the capability.
- `GET /rfqs` can read `engine.rfq.rfqs` plus the adapter's `fetch_rfqs`;
  `GET /rfqs/incoming` reads `engine.rfq.incoming`; positions already carry
  `info["combo"]`.
- `/ws/events` should forward the `rfq.*`, `quote.*` and `combo.saved` kinds;
  `rfq.incoming` is not journaled, so a replaying client gets it live only.
- Grants: accept, confirm, create, send, replace and cancel are `trade` on the
  venue's account; the reads are `view`.

### P5 result (serve routes, offline, 2026-10-21)

Built in `synpath/server/rfq_routes.py` (mounted by `create_trading_router`),
tested in `tests/test_server_rfq.py` (22 tests, against `PaperRfqVenue` beside
a plain paper venue). User docs: `docs/server.md`, "RFQs, quotes and combos".

- **Paths.** Quote routes are flat (`/quotes/{id}/accept`, not
  `/rfqs/{id}/quotes/{q}/accept`): the engine methods take a quote id, and
  `rfq_id` rides in the body or query for venues that need it. `GET
  /combos/collections` is not built (no typed method until P6; added in P6).
- **Venue of an id.** `venue` (query or body), the account, a Synpath market
  id, the desk's working sets (`rfqs`, `incoming`, `maker_quotes`,
  `following`), else the only venue with the capability; otherwise 400.
  Grants are checked on that venue's account before any call.
- **Status codes.** Creates are 201 (`RfqView`, `RfqQuote`); `POST /combos`
  is 200 (create or look up). Accept and confirm answer `QuoteResult`
  (`quote`, `client_order_id`, `final`, `intent_state`, `order_id`): 202 by
  default, 200 when `wait` and the quote is final. `SubmissionInDoubt` is the
  existing 202 envelope with `details.client_order_id`. `RiskRejected` is 409
  with `details.rule`; `RfqNotFound` 404; `NotSupported` is now 501 on the
  trading app too; a venue without the capability 400.
- **Lists** read the venue by default; `source=engine` serves the desk's own
  state without a venue call (useful under Polymarket US's 1/s pace).
- **Halt.** `HaltResult` gained `rfq` (`rfqs`/`quotes` per venue).
- **Events.** `/ws/events` already forwarded every kind; its `kinds` filter
  now applies to the replay too (it applied only to live events).
  `rfq.incoming` stays live only.
- **Audit.** The audit log records grant and key changes only (triggers on
  those tables); no trading route writes it. RFQ writes are journaled by the
  engine, with `acting_as` set per request as for orders.
- **Pacing.** `PolymarketUSTrading` paces `GET /v1/rfqs`, `GET
  /v1/rfqs/quotes`, and `POST /v1/rfqs` with `POST /v1/combos` (one shared
  group) at 1 request/s, ahead of the shared 20/s limiter
  (`PACED_ENDPOINTS`, `rfq_pace_per_second=None` turns it off); a queue
  longer than 15 s refuses with `RateBudgetExceeded` before sending.
- **TypeScript.** The client's types are generated, not committed
  (`npm run types`); the regenerated `synpath.d.ts` typechecks with the RFQ
  schemas (`RfqBody`, `RfqView`, `AcceptBody`, `QuoteResult`, `MakerQuoteBody`,
  `Combo`).

### P0 result (Kalshi demo, 2026-10-21)

**Taker side: settled.**
- `accepted_side` names the side of the maker's bid being taken, not the taker's outcome.
- **To buy YES, send `accepted_side=no`.** The price is 1 − the maker's `no_bid`. To buy NO, send `yes`; the price is 1 − `yes_bid`.
- Evidence:
  - A quote with only a NO bid (`yes_bid 0.00`, `no_bid 0.592`) refused `accepted_side=yes` as invalid parameters.
  - A quote with `yes_bid 0.45` / `no_bid 0.55`, accepted with `no`, went to `executed`. The fill was `side yes, action buy, 5 @ yes_price 0.45, is_taker true` on `rfq_creator_order_id`, and the position became +5 YES.
- Combo RFQs on demo get quotes within seconds. An unrequested RFQ expires on its own (its delete answered "expired").

**Quoter side: still open.**
- `POST /communications/quotes` (13 contracts at 0.01/0.01) returned 400 `insufficient_balance` with $95.28 of demo funds.
- The collateral rule for quotes is unverified. It may need more demo funds, or a maker permission the error does not name.
- Needs a funded demo account to finish.

The demo account kept 5 YES on `KXMVECROSSCATEGORY-S2026GBYNMLIOB4P5GM6R-6TO3QWLMW` ($2.33 of demo funds including fees). No real money was used.

### P6 result (Kalshi, offline, 2026-10-21)

Built in `synpath/trading/kalshi.py`, tested offline in `tests/test_kalshi_rfq.py`
(samples `tests/samples/kalshi_rfq_*.json`, built to the OpenAPI v3.34.0 schemas
and the P0 shapes; nothing captured live). `rfq_taker`, `rfq_quoter` and
`combos` are True; `rfq` stays True for one release. Decisions:

- **Combos.** A combo is a `KXMVE…` market, so its id is `kalshi:<ticker>`
  and its legs come from `mve_selected_legs` (`ComboLeg.event_id` is the bare
  `event_ticker`). `create_combo(legs, collection_id=None)` posts
  `selected_markets` with `with_market_payload`; without `collection_id` it
  picks the one open collection whose `associated_events` take every leg's
  event at that size (none is `InvalidOrder`; several is `InvalidOrder`
  naming them, on `exc.collections` and served as `details.collections`), and
  reads a leg's market when `event_id` is missing. `engine.create_combo` and
  `POST /combos` pass `collection_id` through. 5,000 creations a week; a 400 on legs is
  `InvalidOrder`. `fetch_combo` reads `GET /markets/{ticker}`; a market
  without selected legs is `RfqNotFound`. `lookup_combo` is `NotSupported`
  (no lookup endpoint left). `fetch_combo_collections` (Kalshi only) lists
  collections as the venue's objects, each with `associated_event_tickers`
  (filled from `associated_events` if the venue leaves it out), served at
  `GET /combos/collections`.
- **Taker.** `create_rfq` sends `contracts_fp` or `target_cost_dollars`
  (`target_cost_excludes_fees` optional, amount only; `replace_existing`
  optional) and refuses `outcome`/`action`. An RFQ with a target cost reads
  back as `amount`. `fetch_rfqs` is `user_filter=self`; `fetch_quotes` sends
  `rfq_user_filter=self` with `rfq_id` and also drops other RFQs' rows.
  `cancellation_reason` is free text, read into `closed_reason` where it says
  expired, executed, replaced or cancelled.
- **Accept.** The P0 mapping: buy YES (or sell NO) sends `accepted_side=no`
  at `1 − no_bid`; buy NO (or sell YES) sends `yes` at `1 − yes_bid` a NO
  contract. A reported `no` reads as `Side.BUY`. Refused before sending when
  the quote is not open, the side's bid is 0, or the price is worse than
  `worst_price`; full size only (REST takes no partial); a retry on the same
  side returns the quote without resending. `taker_order_id` is
  `rfq_creator_order_id`, `maker_order_id` `creator_order_id`.
- **Quoter.** `send_quote` posts `yes_bid` = bid, `no_bid` = 1 − ask ("0.0000"
  for a missing side), `rest_remainder`, `post_only`; quantity and expiry are
  refused (the engine keeps expiry). `fetch_my_quotes` uses `user_filter=self`,
  the v3.34.0 filter for quotes the caller made (`quote_creator_user_id` is
  deprecated). `replace_quote` is DELETE then POST: for one round trip the RFQ
  has no quote of ours, and an old quote already gone does not stop the send.
- **Statuses.** Quote `open`, `accepted`, `confirmed`, `executed`, `cancelled`
  (→ `canceled`); no expired or failed status, so those filters are refused.
- **Errors.** A 404 under `/communications` or `/multivariate_event_collections`
  is `RfqNotFound`; `insufficient_balance` is `InsufficientFunds`; other 400/409
  are `OrderRejected` with the venue's code.
- **Engine.** One end-to-end test runs `engine.rfq` on the Kalshi adapter with
  mocked HTTP: combo RFQ, accept YES (sent as `no`), quote executed, order
  adopted, fill booked, position long 5.
- **UNVERIFIED, for P10:** the quoter's collateral rule (P0's 400
  `insufficient_balance` with $95 for 13 contracts at 0.01/0.01); confirm on
  the demo; which of `yes_contracts_fp`/`no_contracts_fp` is which side on a
  target-cost RFQ (read here as YES/NO contracts the requester can buy);
  whether `rfq_id` with `rfq_user_filter=self` is accepted together; the
  `cancellation_reason` words; whether recreating an existing combo uses the
  weekly quota, and whether that quota answers 429 (retried as a rate limit)
  or 400; and the timing of `rfq_creator_order_id` and fills after executed.


### P8 result (SX Bet, offline, 2026-10-21)

Built in `synpath/trading/sx_bet.py` (`SxBetTrading`) and `synpath/ws/sx_bet.py`
(`SxBetParlayStream`), tested offline in `tests/test_sx_bet_parlays.py` (sample
`tests/samples/sx_bet_parlay_request.json`, the channel page's example).
`rfq_quoter` and `combos` are True; `rfq_taker` stays False. Decisions:

- **Requests.** Each `parlay_markets:global` message is an incoming `Rfq`: `id` the parent hash, `combo_id` `sx_bet:{parent}`, `combo` the legs (`yes` where `bettingOutcomeOne`), two-sided (`outcome`/`action` `None`), `amount` = `requestSize` / 1e6, `mine=False`. A request with fewer than two distinct legs is dropped. The venue replays nothing and has no REST list of open requests, so the adapter keeps what its stream heard (`SxBetTrading.parlays`, ten minutes after the end); `fetch_incoming_rfqs` returns the open ones and starts the stream on its first call. `subscribe_incoming_rfqs(listener)` is the stream hook; the engine subscribes there at start and does not poll SX Bet (`engine.rfq.on_incoming_rfq`, "Gaps closed").
- **Expiry.** The request names no end, so it is estimated as 60 s from arrival (`expires_at`, `info["expiry_exact"]` False) and orders are signed to 3 s before that (`PARLAY_EXPIRY_MARGIN_S`), or to `MakerQuote.expires_at` if sooner (the engine keeps its own and cancels at it). A `PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY` refusal carries the exact end; the quote is re-signed to it once and the request marked exact. An ended request is `closed`/`expired` and refused before anything is sent.
- **Quotes.** A quote is one order per side on the parlay market: the bid on outcome one at `bid`, the ask on outcome two at `1 − ask` (the adapter's usual buy/sell mapping), venue `GTC` with the expiry above, `postOnly` from `post_only`, client ids `{key}:bid`/`{key}:ask`. No `quantity` sizes each side to the requested stake at the bettor's price (`amount / ask` contracts on the ask, `amount / (1 − bid)` on the bid). The quote id names both orders (`0xbid~0xask`), so cancel and read-back need no local state. Status from the orders: `open` while one rests, `executed` when one filled and none rests (`maker_order_id` the filled one), `expired` if both expired, else `canceled`. One side refused leaves a one-sided quote (`info["refused"]`). `replace_quote` is cancel then send (a new id). `confirm_quote` is `NotSupported`: no last look. `fetch_my_quotes(rfq_id=)` also pairs resting orders on the parlay by client id, so a send in doubt is found again.
- **Combos.** `fetch_combo` and `lookup_combo` read the requests heard; `create_combo` is `NotSupported` (no API requests a parlay). Parlay fills are no longer dropped: their `marketHash` is the parent, so they book on the combo's id.
- **Void legs.** The two documents disagree (§1). `parlay_result(legs, outcome_one=)` returns both readings and the `worst` for the side held; nothing is booked from a leg result, and the venue's settlement is the record. For risk, a VOID leg is assumed to lose the stake on either side, never to refund it.
- **Errors.** `INVALID_MARKET` on a quote is `OrderRejected(reason="parlay_expired")`, `PARLAY_LEG_INACTIVE` is `OrderRejected(reason="parlay_leg_inactive")`, the expiry refusal (if the retry fails) and size refusals are `InvalidOrder`; the rest as for any SX Bet order.
- **Not built:** quote status from the account's order stream (the engine polls `fetch_my_quotes`); a fee estimate at the parlay rates (`makerParlayPayoutFee`, `takerParlayPayoutFee`); leg prices for pricing (`GET /markets/find`, `/orders-v3/odds/best`) are the quoter's own business.
- **UNVERIFIED, for P10:** how late requests arrive against the 60 s (is 3 s enough?); whether `requestSize` is always USDC (6 decimals) and whether the bettor always backs outcome one; that a parlay order's `expiry` reads back as ISO; that parlay fills and positions come through `/fills-v3` and `/positions-v3` on the parent; how the venue settles a VOID leg in practice; whether a maker's ask and bid on one parlay can both fill.

### P7 result (Polymarket global, offline, 2026-10-21)

Built in `synpath/trading/polymarket_v3_signing.py` (Exchange v3 orders) and
`synpath/trading/polymarket_rfq.py` (`PolymarketRfqMixin` on
`PolymarketTrading`, and `PolymarketRfqSession` for the quoter WebSocket).
Tested offline in `tests/test_polymarket_v3_signing.py` (vectors in
`tests/samples/polymarket_v3_signing_vectors.json`) and
`tests/test_polymarket_rfq.py` (samples `tests/samples/polymarket_rfq_*.json`,
the documented shapes; one engine run from create to position). `rfq_taker`,
`rfq_quoter` and `combos` are True on `PolymarketTrading`. Decisions:

- **Signing.** The Exchange v3 `Order` has the same eleven fields as CLOB V2,
  under the domain `{name "Polymarket CTF Exchange", version "3", chainId 137,
  verifyingContract 0xe3333700cA9d93003F00f0F71f8515005F6c00Aa}`. There is
  one Exchange v3 and no neg-risk variant. `timestamp` is in seconds; `side`
  is `0`/`1` on the wire. Types 0, 1 and 2 sign the order directly (65
  bytes). A Deposit Wallet (3) signs the `TypedDataSign` envelope (its
  `DepositWallet` v1 domain, the wallet as `verifyingContract`, zero salt),
  wrapped for ERC-7739. The digest is signed by `WalletSigner.sign_digest`,
  which uses the Rust core's secp256k1 signer when present. The venue
  publishes no v3 vectors; py-sdk's golden tests cover V2 only. The vectors
  were therefore made without Synpath code: the docs' typed data was signed
  by `eth_account`, and the docs' `wrapDepositWalletSignature` was rewritten
  with `eth_abi`. Synpath matches them byte for byte on both signers, and
  every signature recovers to the signer. The CLOB V2 vectors still match.
- **Wallets.** The requester gateways refuse EOAs
  (`UNSUPPORTED_REQUESTER_SIGNATURE_TYPE`). This is also checked locally,
  before anything is sent, with a message that says what to change. Accepted:
  Deposit Wallet (signer = maker = the wallet), proxy or Safe (signer = the
  account key, maker = the wallet). Quoting accepts all four types. Combos
  need no key beyond the CLOB API credentials, and both gateways take the L2
  headers. A quoter needs `approve_combo_trading()` (pUSD and PositionManager
  to Exchange v3; `include_router` and `include_auto_redeem` add the
  Router and AutoRedeemer approvals, "Gaps closed"). Last look needs the venue's opt-in.
- **Combos.** The venue has no combo create or lookup; an RFQ names the
  combo. A combo id is `polymarket:combo-<condition_id>` once an RFQ has
  returned it. Before that, it is the provisional `polymarket:combo-legs-<hex>.<hex>`
  (the leg position ids, ascending), which `create_rfq` reads with no state.
  `create_combo` is offline and uses no quota. Leg position ids are not CLOB
  token ids. They come from the public catalogue (`fetch_combo_markets`, and
  typed as `fetch_combo_eligible_markets`, served at `GET /combos/markets`),
  `leg.info["position_id"]`, Gamma `/markets/{id}` `positionIds`, and, in
  reverse, Gamma `/markets/keyset?position_ids=`. `lookup_combo` is
  `NotSupported`.
- **Sides.** An RFQ asks one direction on the combo's YES (`outcome="yes"`;
  the venue's `side` is YES only). A buy is sized by `amount` (pUSD, fees
  included, `notional`) and a sell by `quantity` (shares). The gateway's one
  quote is `ask` = `blended_price_e6` on a buy (`ask_quantity` = the shares
  received, `taker_amount_e6`) and `bid` on a sell (`bid_quantity` =
  `maker_amount_e6`). Accept signs `tokenId = yes_position_id`, side 0 (buy)
  or 1 (sell), and the quote's exact `maker_amount_e6`/`taker_amount_e6`,
  with a zero builder. As a quoter, the request's side decides which price
  counts: a buy request takes our `ask` and is filled by buying NO,
  `ceil((1 − ask) × size)`; a sell request takes our `bid` and is filled by
  buying YES, `ceil(bid × size)`. The full size is
  `floor(notional × 1e6 / price)`. Inventory quotes (selling held tokens)
  are not built.
- **Worst price.** Accept is refused before signing if the blended price or
  the order's own price (`maker/taker`, to the base unit) is worse than
  `worst_price`. Fees come on top (`info["total_required"]`). Only the full
  size can be accepted.
- **Statuses.** An RFQ's quote reads `CREATED`/`COLLECTING_QUOTES`/`AWAITING_REQUESTER_ACCEPTANCE`
  → `open` (`expired` after `expires_at`), `AWAITING_MAKER_CONFIRMATION` →
  `accepted`, and `EXECUTING`/`MATCHED`/`MINED`/`RETRYING` → `confirmed` (on
  its way to the chain, not a fill). `FILLED`/`CONFIRMED` is `executed`;
  `FAILED`/`REJECTED` is `failed` (`MAKER_DECLINED` is `canceled`).
  `EXPIRED` and `CANCELED` map as named. Once an RFQ ends without a trade,
  `fetch_quotes` raises `RfqNotFound`, so the engine closes it. Our quotes
  as a quoter move `open` → `accepted` (`RFQ_CONFIRMATION_REQUEST`) →
  `confirmed` → `executed`/`failed` (`RFQ_EXECUTION_UPDATE`). A quote is
  read as lost (`canceled`) on an `RFQ_TRADE` broadcast with no execution
  of ours, and as `expired` 30 s after sending if never selected.
- **Orders and fills.** The trade settles on chain, not on the CLOB. Its
  orders have synthetic ids: `rfq:<rfq_id>` for the requester and
  `rfq:<rfq_id>:<quote_id>` for a quoter. `fetch_order` and
  `fetch_my_trades` (on a combo id or such an order id) answer them from
  the RFQ: filled on `executed`. The taker fee is `total_required −
  maker_amount` on a buy and `taker_amount − net_receive` on a sell.
- **What the gateway lacks.** It has no list of a requester's RFQs and no
  requester cancel, and status reads answer 409 before an accept. So the
  adapter keeps the RFQs it created in this process: `fetch_rfqs` lists
  them, `cancel_rfq` drops one locally (it lapses at `expires_at`), and
  reads come from the create answer until the accept. `remember_rfq(rfq)`
  re-seeds an RFQ after a restart from `Rfq.info`, which the engine keeps
  in its working set; the engine calls it through `restore_rfqs` at start
  ("Gaps closed").
- **Create retries.** Create takes no idempotency key. A create that timed
  out may have made an RFQ whose id was never returned. That RFQ cannot be
  accepted, so nothing trades, and it lapses within seconds; a retry may
  make a second one, at the cost of the 15-a-minute create budget. Guards:
  the engine's journaled intent and `SubmissionInDoubt`, and `create_rfq`
  returning an open RFQ it already holds for the same legs, direction and
  size.
- **Retries of accept.** An accepted quote is returned without sending
  again, and the gateway does not execute one acceptance twice. A 5xx
  (`SERVICE_UNAVAILABLE`, `TRADE_SUBMISSION_FAILED`) stays
  `ExchangeNotAvailable`, which the engine treats as in doubt.
- **Errors** (the `{error, code}` body): `UNKNOWN_RFQ` → `RfqNotFound`;
  the EOA code and other 400s → `InvalidOrder`; `EXPIRED_RFQ`,
  `INVALID_RFQ_STATE`, `QUOTE_MISMATCH`, `INVALID_SIGNATURE`, the quote
  validation codes and similar → `OrderRejected(reason=code.lower())`;
  `PRE_EXECUTION_BALANCE_RESERVATION_FAILED` and the balance and allowance
  codes → `InsufficientFunds`; 401/403 → `AuthenticationError`; 429 →
  `RateLimitExceeded`.
- **Quoting.** Quote requests arrive only on
  `wss://combos-rfq-gateway-quoter.polymarket.com/ws/rfq`.
  `start_rfq_session(on_rfq=)` opens it: auth comes first with the CLOB
  credentials and the identity; the library answers pings. Quotes go over
  REST (`POST /v1/maker/quotes`, with our own `quote_` + 32-hex id derived
  from the engine's key, and `valid_until` from `expires_at`). Cancel is
  `POST /v1/maker/quotes/cancel`. While a last look is open, `cancel_quote`
  sends `DECLINE` to `/v1/maker/confirmations`, and `confirm_quote` sends
  `CONFIRM`. `replace_quote` is cancel then send. A buy quote above 0.90909
  is refused locally. The window is 400 ms; the engine hears each request
  from the session as it lands (`engine.rfq.on_incoming_rfq`, "Gaps closed").
- **Not built:** the Builder gateway (`/v1/builder/rfq/requests`, Builder
  API key), inventory quotes, collateral return, combo positions from the
  Data API, quoting over the WebSocket (`RFQ_QUOTE`; REST is used), and
  Session Keys (py-sdk refuses them for combos).
- **UNVERIFIED, for P10:** whether `taker_order_hash` equals our Exchange v3
  order hash (a mismatch is logged); the acceptance window (5 s or 10 s);
  whether `GET /requests/{id}` carries more than `{rfq_id, status, tx_hash}`;
  the fee arithmetic above; whether a requester also needs the Exchange v3
  approvals; Gamma `positionIds` on `/markets/{id}` and the shape of
  `/markets/keyset`; the quoter fill size when the bundle splits across
  makers (`fill_size_e6` is used when sent); the REST maker snapshot's
  status words; and whether the requester's Deposit Wallet signer may be a
  session key.

### Gaps closed (2026-10-21, offline)

**1. Incoming RFQs are pushed, not polled every 2 s.**

- `engine.rfq.on_incoming(venue, rfq)` is the one entry point for an
  incoming RFQ, from a stream or the poll. In order, it records the RFQ in
  `engine.rfq.incoming`, publishes `rfq.incoming` (not journaled), then runs
  every callback registered with `engine.rfq.on_incoming_rfq(callback)`,
  awaited. The combo's legs (`combo.saved`) are journaled after the
  callbacks, and `send_quote` now saves them after its venue call. An RFQ
  already known, or one of ours, is not heard twice.
- New optional hooks on `TradingExchange`: `subscribe_incoming_rfqs(listener)`
  returns the unsubscribe, or `None` (the default) where there is no stream;
  `start_incoming_rfqs()` starts the stream (default no-op).
- At start (`recover()`), the desk subscribes every quoting venue whose hook
  returns an unsubscribe. Those venues are in `engine.rfq.pushed` and are
  left out of the 2 s poll. Their streams are started by the `rfq` loop (and
  by `tick()`), and once started, what they already heard is taken. A stream
  that will not start falls back to the poll. `rfq_stream_incoming=False`
  leaves starting them to the caller, and `rfq_incoming_interval_s=None`
  still turns the whole feed off. Pushed RFQs leave `incoming` a minute
  after their window ends.
- **Polymarket global:** the quoter session tells the adapter's listeners
  first (scheduled as tasks, so the socket keeps reading), then `on_rfq`.
  `start_incoming_rfqs()` opens the session.
- **SX Bet:** `subscribe_incoming_rfqs` as before; `start_incoming_rfqs()` is
  `watch_parlays()`.
- **Kalshi and Polymarket US:** neither `synpath/ws/kalshi.py` nor the
  Polymarket US streams have an RFQ channel, so both keep the poll.
- **The send path:** `send_quote` reads the RFQ from `engine.rfq.incoming`
  (no venue read), runs `check_offer` (in memory), writes the intent, and
  calls the venue.
- **Timing (in process):**
  - `PaperRfqVenue(stream_incoming=True)`: push to callback 0.05 ms; push to
    the quote sent through `engine.send_quote` about 4 ms (SQLite, with the
    journal writes).
  - Polymarket's session (`RFQ_REQUEST` to the callback, then a mocked
    `POST /v1/maker/quotes`): 0.3 ms and about 5.5 ms.
  - The tests assert under 50 ms to the callback and under 400 ms to the quote.
  - `/ws/events` reads the bus subscription's queue directly, so a pushed
    `rfq.incoming` reaches a connected socket with no poll; a test asserts it.

```python
async def quoter(rfq):                 # runs inside the window: 400 ms on Polymarket global
    if rfq.venue != "polymarket" or rfq.action != "buy":
        return
    await engine.send_quote(MakerQuote(rfq_id=rfq.id, ask=Decimal("0.42"), book="mm",
                                       expires_at=rfq.expires_at), venue=rfq.venue)

stop = engine.rfq.on_incoming_rfq(quoter)   # stop() unsubscribes
```

**2. Polymarket global remembers its RFQs after a restart.**

- New optional hook: `TradingExchange.restore_rfqs(rfqs, quotes)`, a no-op by
  default. `RfqDesk.restore()` (called from `recover()`, before the in-doubt
  intents are resolved) passes each adapter:
  - our open RFQs from the working set (`Rfq` with its `info`). An RFQ with
    an in-doubt accept that has left the working set is rebuilt from its
    `rfq.created` event.
  - a quote for each in-doubt accept (`mine=False`, `accepted`, the
    intent's side);
  - for each in-doubt confirm, our quote from its latest `quote.sent` /
    `quote.accepted` / `quote.confirmed` event (`mine=True`), at `confirmed`
    if the confirm was answered, else `accepted`.
- `PolymarketTrading.restore_rfqs` calls `remember_rfq`. For an accept, it
  marks the RFQ accepted, so its status is read from `GET /requests/{id}`:
  - FILLED adopts the order and books the position;
  - FAILED is `quote.failed`;
  - a 409 (the accept never landed) reads `open`, and after `lost_after_s`
    the intent is `rejected` with nothing sent;
  - a retried accept on such an RFQ sends again.
- Our quotes are re-seeded at their status, with the requester's
  `fill_size_e6` and `confirm_by`. A confirm in doubt therefore stays
  pending, instead of reading as unknown and failing. Its execution still
  comes only from the quoter session (`RFQ_EXECUTION_UPDATE`); the gateway
  replays nothing.
- Tests (`tests/test_polymarket_rfq.py`, mocked HTTP, SQLite):
  - create, accept and crash, then rebuild on the same journal: the accept
    resolves, the order `rfq:<id>` is adopted, and the position is 1.932381;
  - an accept that never landed is `rejected` and not resent;
  - an open RFQ is cancelled after a restart;
  - a confirm in doubt is followed to execution and a position of 2.

**3. Polymarket global approvals.**

`approve_combo_trading(include_router=False, include_auto_redeem=False)` and
`v3.combo_approval_calls(...)`:

| Approval | Role (market-makers guide) | Sent |
|---|---|---|
| pUSD `approve(Exchange v3)`, PositionManager `setApprovalForAll(Exchange v3)` | quoter, when the quoted order moves pUSD or combo positions | always |
| pUSD `approve(Router)`, PositionManager `setApprovalForAll(Router)` | splitting pUSD into positions; managing or redeeming them through the Router (optional) | `include_router` |
| PositionManager `setApprovalForAll(AutoRedeemer)` | automatic redemption (optional) | `include_auto_redeem` |

- Router is `0x12121212006e4CD160D18e3f00711DA5c3372600`; AutoRedeemer is
  `0xa1200000d0002264C9a1698e001292D00E1b00af`.
- **Requesters: UNVERIFIED.** The guide names no approvals for them. A BUY
  spends pUSD and a SELL moves combo positions, so the Exchange v3 pair is
  probably needed; P10 settles it.
- The calldata is tested byte for byte: the selectors `0x095ea7b3` and
  `0xa22cb465`, the spender word, and max uint / true.

## 9. Settled questions (2026-10-21)

1. P0 on the Kalshi demo: yes, with notice before it runs.
2. Order: Polymarket US first (P2), then Kalshi, Polymarket global and SX Bet in parallel after the serve layer.
3. Quoter side: in scope now (P4, P6–P8).
4. Amount sizing: yes, venue-native where possible, otherwise converted at accept.
5. Polymarket global: yes, including Exchange v3 signing (P7).
6. Accept returns at once; `wait` is optional.
7. Limitless Packs: deferred (not in scope).

## 10. Cross-venue RFQ

Built 2026-10-22, offline (Yuki approved, Richard OK'd). One request goes to
every venue that lists the same market or combo, every quote lands in one
ranked list, and an accept cancels the rest. Code:
`synpath/engine/rfq_group.py` (`engine.rfq_groups`), routes in
`synpath/server/rfq_routes.py`. Each member is an ordinary RFQ of the desk
(§5): created, watched, accepted, followed and recovered exactly as one
asked directly.

### Instrument resolution

| Asked on | Members |
|---|---|
| `bucket:<id>` | one per enabled bucket member, on its market, with its `flip`; a switched-off member is skipped (`member_disabled`) |
| `legs` (a combo of propositions; each leg any venue's market or a bucket), or a combo's own id the engine holds legs for | every venue with `combos` and `rfq_taker`: each leg mapped (itself on its own venue, a bucket's member there with its flip, else hosted matching with its `side_map`), then checked, then that venue's combo created (Kalshi with the `collection_id` the check chose) |
| a plain market | its own venue; with `cross_venue=True` also every other RFQ venue's match (`match_market(anchor, venue=v)`), its `side_map` giving `flip`. Without it, a group of one |

- **Matching** is `engine.rfq_groups.matcher(anchor, venue)`, by default the
  hosted service (`synpath.matching.match_market`, run in a thread). `Engine(...,
  matcher=...)` replaces it. A leg whose venue `side_map` swaps YES and NO is
  asked on the other outcome there.
- **Eligibility.** Kalshi (an adapter with `fetch_combo_collections`): the leg
  events (`event_id`, else the market's own `event_ticker`) must all be in
  one open collection's `associated_event_tickers`, within `size_min` and
  `size_max`; the first such collection (sorted) is the `collection_id`.
  Polymarket and Polymarket US (an adapter with its own
  `fetch_combo_eligible_markets`): every mapped leg must be on the list, read
  up to 20 pages and kept 60 s; Polymarket legs get their
  `info.position_id` from it. A venue with neither is `unchecked`.
- **Skipped venues**, each with `venue`, `code`, `reason` and the leg:
  `excluded` (`venues`, `exclude_venues`), `forbidden` (the HTTP key may not
  trade it), `not_configured`, `no_rfq`, `no_combos`, `no_match`,
  `matching_error`, `missing_leg`, `ineligible_leg`, `duplicate_leg`,
  `one_sided_size` (Polymarket global asks a YES buy by amount and a YES
  sell by quantity), `member_disabled`. No venue left is `BadRequest` with the
  list on `exc.skipped` (`400 no_venue` over HTTP), and nothing is journaled.
- **One-sided venues.** An adapter with `rfq_one_sided = True` (Polymarket
  global) is asked the YES side the group takes there; two-sided venues are
  asked no side.

### The model: `RfqGroup`

`id`, `instrument` (`kind` market/bucket/combo, `market_id`, `legs`),
`quantity` or `amount`, `outcome` and `action` (the requester's, in the
instrument's terms), `cross_venue`, `book`, `trader`, `tags`, `members`,
`skipped`, `quotes` (ranked), `status`, `accept_attempts`, `claim_key`,
`accepted_quote_id`, `accepted_venue`, `order_id`, `failures` (venue,
stage, error), `expires_at` (the soonest venue expiry among the quotes that
can be accepted now), `created_at`, `updated_at`.

- `GroupMember`: `venue`, `instrument_id`, `flip`, `via`
  (anchor/bucket/matching/combo), the mapped `legs`, `combo`,
  `collection_id`, `eligibility`, `create_key`, `rfq_id`, `rfq`, `status`,
  `detail`, `accept_key`. Status: `creating` (also an in-doubt create),
  `open`, `failed`, `canceled`, `lapsing` (no requester cancel: left to
  expire), `closed`, `accepting`, `accepted`, `executed`.
- Group status: `creating` → `open` → `accepting` (claimed, cancelling) →
  `accepted` (sent, followed) → `executed`; or `canceled`, `expired` (no
  member left), `failed` (every create failed). A failed accept goes back to
  `open`.

**Journal.** The intent `rfq-group:{id}` (`rfq_group_create`) is written
before any venue call, then each member's create under its own key
`rfq-create:{venue}:{group}-{n}` (passed to `create_rfq(key=...)`). The
group is saved to the cursor `rfq:groups` before each venue call and after
each answer, and published as `rfq_group.*` events with the whole group in
`payload.group`. A create that fails marks that member `failed`; the group
stays open with the rest, and the create intent is `sent` (`rejected` when
no member stands).

### Ranking

Every quote becomes a `GroupQuote` in the group's terms. With `S` the
group's YES side (`yes_side(outcome, action)`) and a member's side `S'`
(`S`, or the opposite for a flipped member):

```
raw     = quote.ask if S' is buy else quote.bid          # the member's own YES price
price   = raw, or 1 - raw for a flipped member            # the group's YES price
token   = price for outcome yes, 1 - price for outcome no
fee     = the venue's taker fee for one order of `quantity` at `raw` on `S'`
          (adapter.fetch_fee_estimate: the venue's fee schedule; unknown counts as 0)
per     = fee / quantity
effective_price         = price + per (YES buy) | price - per (YES sell)
effective_outcome_price = token + per (action buy) | token - per (action sell)
```

- Quantity-sized: `quantity` is the group's (the quote's size if smaller:
  `partial`). Rank by `effective_outcome_price`, lowest first for a buy,
  highest first for a sell.
- Amount-sized buy: `quantity` is the most contracts with
  `quantity * token + fee(quantity) <= amount`, on the venue's step (sized,
  then re-sized net of the fee until it fits). Rank by `quantity`, most
  first, then by `effective_outcome_price`. An amount-sized sell ranks as a
  quantity-sized one.
- Quotes that cannot be accepted now (not open, their member not open, no
  price on the side asked, no size) follow, with `rank: null`.
- Each change to a member's quote (from the desk's follower, a venue
  stream, or `refresh`) publishes `rfq_group.quote` (the `GroupQuote`, with
  `group_id`) and then `rfq_group.updated`.

### Quote lifetime (2026-10-22)

There is no window of the engine's or of a caller's. A `GroupQuote` carries
the venue's own deadlines: `expires_at` (the quote's `expires_at`, else its
member RFQ's; `expires_from` says which), `confirm_by` and `execute_by`
(after an accept). Where the venue gives none, it is null and the quote is
live until the venue closes it:

| Venue | Quote expiry | RFQ expiry |
|---|---|---|
| Polymarket global | `expires_at` (the gateway's) | `expires_at` |
| Kalshi | none in the API (`Quote` has no expiry field) | none (`RFQ` has no expiry field; the venue closes RFQs itself, `cancellation_reason: expired`) |
| Polymarket US | none (`confirmationDeadline` / `executionDeadline` after an accept) | none |

- The desk's follower re-reads each open RFQ of ours from its venue every
  `RFQ_STATUS_INTERVAL_S` (10 s), and at once once its `expires_at` has
  passed; an RFQ the venue has closed is closed here (`rfq.closed` with the
  venue's reason), its open quotes `expired` (or `canceled`).
- A group is `expired` when every member RFQ has closed or expired on its
  venue; a caller cancels it at any time (`DELETE`).
- Quotes and status changes reach a caller as `rfq_group.*` (and the members'
  `quote.*`) events on `/ws/events`; polling is only a fallback.
- `PaperRfqVenue(rfq_ttl_s=...)` and `PaperMaker(ttl_s=..., delay_s=...)`
  give paper RFQs and quotes an expiry and a delay, for tests and demos.

### Accept: one per group, the rest cancelled

`engine.accept_group_quote(group_id, quote_id, venue=None, worst_price=None, wait=False)`:

0. The quote is named by `(venue, quote_id)`: quote ids are the venues' own,
   and two venues may return the same one. `venue` is required when the group
   spans more than one venue (`BadRequest`, reason `venue_required`; over HTTP
   `400 venue_required`); a venue outside the group is `venue_not_in_group`.
   The claim's journal row records the venue, and is read back against it.
1. Under the group's lock, the group is reconciled with the journal and
   refused if an accept is in flight or done (`RiskRejected`, rule
   `rfq_group_accept_in_flight` or `rfq_group_accepted`).
2. `RfqDesk.check_accept` runs everything the accept will check (the quote
   re-read, `worst_price` turned over for a flipped member, the size, every
   order risk rule, the halt) without writing or sending. A refusal leaves
   every venue's RFQ standing.
3. **The claim**: the intent `rfq-group-accept:{group}:{n}`
   (`rfq_group_accept`, `target_order_id` the quote). Attempt `n` is written
   only when every earlier one is `failed`, `rejected` or `lost`, and is
   read back after the write (an insert that loses a race reads the
   winner's quote and is refused). Its params name the member accept's key
   and the other members to cancel.
4. **Cancel every other member first** (`RfqDesk.cancel_rfq`, journaled).
   A venue whose requester cannot cancel (`rfq_requester_cancel = False`,
   Polymarket global) is dropped and left to lapse at its expiry: member
   `lapsing`, with the reason. A cancel that fails is recorded in
   `failures`; nothing trades on an RFQ that is never accepted.
5. The claim is marked `sending`, and the quote accepted through
   `RfqDesk.accept_quote(..., _group=id)` under the key
   `rfq-accept:{venue}:{quote_id}`: risk, the journal, following, recovery.
6. The group follows the member accept's intent: `sent` (executed) makes the
   group `executed` and the claim `sent`; `failed`/`rejected` releases the
   claim (`failed`), and the group is `open` again on what is still open
   (`expired` if nothing is). What was cancelled stays cancelled.

**Guarantees.**

- At most one claim per group is ever live, by the journal (step 3), not by
  memory: a restarted engine, or one whose in-memory group is wrong, still
  refuses a second accept.
- A quote on a member RFQ cannot be accepted around the group:
  `RfqDesk.accept_quote` (and `POST /quotes/{id}/accept`) refuses it.
- The other venues are cancelled before the accept is sent.

### Restarts, halt

- `recover()`: `RfqDesk.restore()`, then `RfqGroupDesk.restore()` reads
  `rfq:groups` and re-seeds any open member RFQ the desk's working set lacks
  (`restore_rfqs`); then the in-doubt intents (group intents included); then
  every group is reconciled. A member create in doubt joins its group when
  the desk adopts it (it is cancelled at once if the group was accepted
  elsewhere meanwhile). A claim whose member accept was never written (the
  process died between the cancels and the accept) finishes the cancels it
  listed and is released: nothing traded, the group is open, a retry is
  attempt `n + 1`. A claim whose accept was sent is followed by the desk.
- `halt()` with the `cancel` policy cancels the members in scope of every
  open group first (`result["rfq"]["groups"]`); a group left with no open
  member is `canceled`. An accept in flight is left to finish. Under any
  policy, a new group accept is refused at step 2.
- A group every venue refuses before sending (the halt, a paused book, a
  size limit) is journaled `failed` and the refusal raised (`409` over
  HTTP).

### Preview and the combo board (2026-10-22, for the terminal's Combos screen)

- `engine.preview_rfq_group(...)` (`RfqGroupDesk.preview`) runs a create's planning (instrument,
  matching, eligibility, one-sided sizes, refusals) and returns `GroupPreview` (`offered`
  `PreviewVenue`s and `skipped`) without a lease, a journal write or a venue RFQ call. Matching
  answers are kept 60 s (`MATCH_TTL_S`), shared with creates. `POST /trading/rfq-groups/preview`
  adds each offered venue's leg prices from the server's catalogue and their product (cached 10 s).
- A Kalshi collection's `size_max` of 0 (or none) is no maximum; it was read as a maximum of 0,
  which refused every Kalshi combo in such a collection.
- `GET /trading/combos/board` (`synpath/engine/combo_board.py`): a combo venue's eligible markets
  as games and questions with categories ([server](server.md#the-combo-board)); Kalshi's needs the
  read adapter's new `fetch_events_by_ids` (one batched `/events?tickers=` read). Serve's trading
  app now holds the read registry (`app.state.registry`) for both.
- **From the venues' own fields (2026-10-22, checked on live data).** Team codes and labels no
  longer come from URL slugs, nor types from titles ([server](server.md#the-combo-board)):
  - Polymarket: the catalogue row has no type, team or line (`id`, `slug`, `title`, `outcomes`,
    `outcome_prices`, `tags`, `volume` only), so the board reads Gamma's market
    (`sportsMarketType`: `moneyline`, `spreads`, `totals`, and many others such as
    `soccer_exact_score`, `child_moneyline`, `second_half_spreads`; `line`, signed for the first
    outcome; `groupItemTitle`, which names a soccer result's team or `Draw (...)`) and the game's
    Gamma event (`teams`: `name`, `abbreviation` in lower case, `alias`, `ordering`). The read
    adapter's new `fetch_events_by_ids` (Gamma `/events?id=`, 20 a call) gives the teams. A gamma
    market has no team fields; outcomes are matched to the event's teams by name or alias.
  - Kalshi: the event's `product_metadata` (`competition`, `competition_scope`: `Game`, `Spread`,
    `Point Total`, `1st Half Winner`, `Team Total`, `BTTS`...) and `sub_title` (`NE vs CHI (Oct
    22)`: the codes); the market's `yes_sub_title` (`Chicago`, `Tie`, `Chicago wins by over 16.5
    points`, `Over 27.5 points`), `custom_strike` (`{"football_team": <uuid>}`, the same id on the
    team's winner and spread markets) and `floor_strike` (the line). Kalshi has no team
    abbreviation field other than the `sub_title`, and no game start time on the event (the board
    keeps the market's close).
  - Polymarket US: `marketType` (`moneyline`, `drawable_outcome`; every combo-enabled market read
    was one of these: no spreads or totals were combo-enabled), `sportsMarketTypeV2`,
    `marketSides[].team` (`name`, `displayAbbreviation`, `alias`) and the event's `teams`; there is
    no `line` field on the markets read.
- Tests: `tests/test_combo_board.py` (19) on `tests/samples/polymarket_combo_board_live.json`,
  `polymarket_us_combo_board_live.json` and `kalshi_combo_board_live.json` (trimmed live reads:
  4 Polymarket games, 7 Polymarket US games, 5 Kalshi games and Kalshi's other scopes), the readers
  and Gamma's events by id; `TestPreview` in `tests/test_engine_rfq_group.py` and
  `tests/test_server_rfq_group.py`, `TestEventsByIds` in `tests/test_kalshi.py`.

### Over HTTP

`POST /trading/rfq-groups`, `POST /trading/rfq-groups/preview`, `GET /trading/combos/board`, `GET /trading/rfq-groups`,
`GET /trading/rfq-groups/{id}`, `POST /trading/rfq-groups/{id}/accept`,
`DELETE /trading/rfq-groups/{id}` ([server](server.md#cross-venue-rfq-groups)).

### Tests and what is unverified

`tests/test_engine_rfq_group.py` (25: accept by venue with colliding quote
ids, expiry) and `tests/test_server_rfq_group.py` (16: `400 venue_required`): paper RFQ venues under the names `kalshi` (collections, quadratic 7 %
fee), `polymarket` (eligible list, no fee, no requester cancel),
`polymarket_us` (eligible list, 2c a contract) and `limitless`, with the
hosted matching service mocked under the real `match_market`
(`httpx.MockTransport`).

UNVERIFIED against a venue: combo fee estimates (`fetch_fee_estimate` on a
combo market id; unknown counts as zero); Polymarket global's
`rfq_one_sided` sizing in a group; how fast each venue's combo-eligible list
finds a leg beyond its first pages; whether the matching service's
`side_map` holds for every combo leg; a Kalshi collection choice when
several fit (the group takes the first, sorted).
