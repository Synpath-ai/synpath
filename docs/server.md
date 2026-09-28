# HTTP server

Back to the [README](../README.md).

```bash
pip install synpath
synpath serve                     # your self-hosted server: market data at /, trading at /trading, the engine behind it
python -m synpath.server          # market data only: http://127.0.0.1:8000/docs
```

`synpath serve` is the whole self-hosted stack in one process; see
[docs/engine.md](engine.md#running-it). The rest of this page is the two
apps it serves, which can also be built and mounted yourself.

```bash
curl "localhost:8000/venues/kalshi/markets?limit=1"
curl "localhost:8000/venues/kalshi/markets/KXELONMARS-99/book?side=no"
```

| Route | |
|---|---|
| `GET /health` | |
| `GET /venues` | ids, book models, capabilities |
| `GET /venues/{venue}/markets` | `?query= &limit= &cursor= &status= &sort=` (`volume`, `liquidity`, `newest`; anything else is a 400) |
| `GET /venues/{venue}/markets/{market_id}` | |
| `GET /venues/{venue}/events` | |
| `GET /venues/{venue}/markets/{market_id}/book` | `?side=yes|no &depth=` |
| `GET /venues/{venue}/markets/{market_id}/trades` | `?since= &limit= &cursor=` |
| `GET /venues/{venue}/markets/{market_id}/candles` | `?timeframe= &since= &until= &limit= &cursor=`, in the YES price; with `since`, paged forward by `next_cursor` |
| `GET /venues/{venue}/markets/{market_id}/fee` | |
| `GET /venues/{venue}/series/{series_id}` | Kalshi and Polymarket US |

Lists are enveloped with the cursor; single resources are not.

```json
{ "data": [ ... ], "next_cursor": "eyJjIjoiQ0RJIiwibyI6NX0", "count": 100 }
```

Every error, on both apps, is one shape. `code` is the library's exception in
snake case, or the HTTP layer's own (`unauthorized`, `forbidden`,
`validation_error`, `unknown_venue`); `details` always has `venue` and
`retryable`, plus `rule` on a risk refusal, `reason` on a venue refusal,
`retry_after` on a rate limit, and `errors` on a malformed request.

```json
{ "error": { "code": "rate_limit_exceeded", "message": "...", "details": { "venue": "kalshi", "retryable": true } } }
```

`400` bad parameters or a venue's refusal of an order, `401` no token, `403` not
permitted, `404` no such thing, `409` a risk rule or a halt, `422` a malformed
body, `429` slow down, `501` the venue has no such capability, `502` the venue
failed, `504` it did not answer. The events socket closes with `4401` without a
valid token.

### Typed clients in any language

Routes are explicit rather than dispatched from a method name, so
`GET /openapi.json` describes every response with a real schema, not `object`.
Generate a client instead of hand-writing one:

```bash
python -m synpath.server schema --out openapi.json          # the read contract
python -m synpath.server schema --trading --out openapi.json   # the trading one
npx openapi-typescript openapi.json --default-non-nullable false -o src/synpath.d.ts
```

`schema` writes the document without starting a server or holding any
credentials, which is what a CI job wants. Pass `--default-non-nullable
false` or every field with a default (`time_in_force`, `post_only`) is
generated as required. A worked example, place through cancel in TypeScript,
is in [`clients/typescript`](https://synpath.dev/docs/sdks/typescript).

## Trading

Placing an order needs to know who is asking, so trading is a second app with
its own keys and permissions:

```python
from synpath.engine import Engine, EngineConfig
from synpath import ControlStore, create_trading_app

store = await ControlStore("control.db").open()
user, key = await store.bootstrap("owner")     # once; the key is shown once
app = create_trading_app(engine, store)
```

```bash
python -m synpath.server bootstrap --control control.db   # the same, from a shell
```

| Route | Permission | What it does |
|---|---|---|
| `GET /me`, `GET /accounts`, `GET /status` | any key / `view` | who this key is, what it may touch, what the engine is doing |
| `POST /orders`, `PATCH /orders/{id}`, `DELETE /orders/{id}` | `trade` on that account | place, amend, cancel; engine-held types too |
| `POST /buckets`, `DELETE /buckets/{id}` | `trade` on every member venue | define a bucket (the server assigns its id); archive one |
| `GET /buckets`, `GET /buckets/{id}`, `GET /buckets/{id}/orders`, `GET /buckets/{id}/orders/{order_id}`, `GET /buckets/{id}/position` | `view` on every member venue | buckets, the orders on one with fills by venue, the position netted in bucket terms |
| `GET /orders`, `GET /orders/{id}`, `GET /fills`, `GET /positions`, `GET /balances`, `GET /pnl` | `view` | what the engine and the venues hold |
| `GET`/`PUT /fair-values` | `view` / `trade` | the marks positions are valued at |
| `GET /risk`, `PUT /risk` | `view` / `manage_credentials` | the rules in force, versioned on change |
| `POST /halt`, `POST /resume` | `trade` | the kill switch |
| `GET`/`POST`/`DELETE /grants`, `POST /keys`, `DELETE /keys/{id}` | `manage_members` / `manage_credentials` | who may do what |
| `GET /audit` | `manage_members` | the append-only history of those changes |
| `WS /ws/events` | `view` | the engine's events, replayed from a cursor then live |

**Permissions are per subaccount and nothing is implied.** A grant is
`(user, account, permission)` where the account is `venue:name` or `*`, and
the permission is one of `view`, `trade`, `manage_credentials`,
`manage_members`. A key that may trade may not grant; a key that may grant
may not trade.

**Keys are stored as hashes.** The secret is shown once, when it is issued.
A stolen database cannot be used to trade.

**The audit log is append-only, enforced by the database.** Triggers record
every grant and key change with the actor, the request, and the row before
and after; triggers on the audit table itself raise on update and delete. No
route writes it, and none could.

**The event socket replays.** `GET /ws/events?since=<seq>` sends everything
after that journal sequence number, then stays open. A client that
reconnects does not miss the fill that happened while it was away.

```bash
websocat "ws://127.0.0.1:8000/trading/ws/events?key=$SYNPATH_ACCESS_TOKEN&since=0&kinds=order,fill"
```

### Mounting it

The *read* app ships **no authentication**, and none is planned. A service with auth
baked into its open-source core forces every host to work around it. Put your
own middleware in front:

```python
from fastapi import Depends, FastAPI
from synpath import VenueRegistry, create_app

outer = FastAPI()
outer.state.registry = VenueRegistry()
outer.include_router(create_app(docs=False).router, dependencies=[Depends(my_auth)])
```

Adapters are reused per venue across requests, because the rate limiter and
connection pool have to be shared: a fresh client per request would hand every
request a full token budget and sail past the venue's ceiling.

---
