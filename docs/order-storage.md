# Order storage and execution ownership

The experimental `codex/order-lifecycle-storage` branch introduces a storage
contract for the execution engine. SQLite is the default; PostgreSQL is an
explicitly selected backend using the same logical schema and durable workflows.
Unsupported configuration or a missing PostgreSQL driver raises an error; there
is no fallback to SQLite.

## Configuration

```toml
storage_backend = "sqlite"
journal = "/absolute/persistent/path/synpath.db"
session_timezone = "Europe/London"
session_end = "23:59:59"
# Normal shutdown preserves orders. Operator halt is a separate operation.
halt_on_exit = false
```

For PostgreSQL, install `synpath[engine,postgres]` (`synpath[server,postgres]`
for the sidecar) and configure a connection string and schema explicitly:

```toml
storage_backend = "postgresql"
journal = "postgresql://synpath@localhost/synpath_orders"
storage_schema = "execution"
halt_on_exit = false
```

The same selection is available through
`EngineConfig(storage_backend="postgresql", journal_path=dsn, storage_schema="execution")`.
Use libpq's password file or another deployment secret mechanism for credentials;
do not commit a credentialed connection string. The sidecar's display and local
registry use `postgresql:<schema>` instead of the DSN. Its control/authentication
store remains separate SQLite storage.

A deployment selects one store. This checkpoint creates a fresh PostgreSQL
journal; it does not copy an existing SQLite journal or run two authoritative
stores. Stop the old execution worker before selecting another backend.
Moving existing armed orders requires an explicit, separately verified import.

Set `SYNPATH_USER_ID` to the single execution worker's stable owner identity.
It defaults to `local-user`. This is attribution, not customer authentication
or multi-user execution. Child requests inherit their parent's owner.
A request cannot override the worker's configured identity.

One worker holds the journal lease. Normal shutdown releases it without
implicitly canceling orders. An operator can still call halt or configure
`halt_on_exit = true` to cancel explicitly. Stops cannot trigger while the
worker is down. DAY applies only when requested, and the worker converts it
to an absolute expiry in the configured timezone before persisting it.

## Storage boundary

`JournalStore` is the engine's persistence contract. `create_journal` selects
one implementation. SQLite SQL and connection details live inside `Journal`
and `SQLiteOrderRecords`; execution, events, and fair-value code use the
contract. SQLite uses WAL, full synchronous durability, and transactions.

`market_orders`, `limit_orders`, `stop_market_orders`, and `stop_limit_orders`
are separate type-owned records. Each includes nonempty `user_id`, account,
client key, instrument, side, size, lifecycle state, original request and
versioned mutable snapshot. Type-specific columns include the required
limit, stop, and trigger fields. The `orders` table remains a shared execution
projection for API reads, reconciliation and the ledger, not a second editable
copy of a user's instruction. Other advanced types retain their existing
managed-order persistence until their migration is explicitly implemented.

A type record and its accompanying intent or event are written in the same
transaction. Venue updates replace execution snapshots and progress while
preserving the original submitted request. Venue orders use a stable local
instruction ID (the dispatch client key) and store the venue ID separately;
managed parents use their own parent ID. Historical execution records may
lack fields not reported by a venue; they carry no invented original request.
Unknown historical ownership is `legacy:unattributed`.

## Dispatch and retry identity

The four supported instruction types use the shared `submissions` table to
reserve a client key and retain the original request separately from its
resolved execution request. This keeps DAY's first computed absolute expiry
on retries. The single-worker journal reserves keys globally: reusing a key
for a different account, owner or instruction is a conflict. Multi-user
execution remains outside this checkpoint.

A same-key retry returns the existing parent or venue result, including
terminal results. A definitive risk or venue refusal is retained and
returned on retry. A send marked `sending` is reconciled by client key;
unknown or lost outcomes are never automatically resent. History pagination
must finish before the engine concludes that no matching order was listed.
Historical instructions from the earlier checkpoint are migrated into this
reservation table. Existing records without a verifiable original instruction
cannot be silently reused for another send. Earlier DAY records preserve their
stored GTD instruction; callers can retrieve those orders instead of recreating
an expiry through a retry.

## Stop transition transaction

```text
waiting + eligible price
          |
          v
ONE TRANSACTION: working parent + stable child request + planned intent
          |
          v
sending intent -> venue call -> accepted child result
          |                         |
          +-- ambiguous ----------> reconcile the same client key
                                    |
                                    v
                         attach child + derive parent progress
```

Stop parents retain a pending child request and precision in their snapshot.
The trigger and pending intent commit together before any venue request.
The child key is derived once from the owner, account and parent ID. A restart
restores that key; a planned action that was never sent may dispatch, while
an uncertain action must be reconciled first. An accepted child is attached
before the pending request is cleared. Terminal IOC children close the parent
immediately, recording remaining quantity. The parent's average price comes
from venue execution averages, not from the child's worst permissible price.

Price, expiry, cancellation and child notifications serialize per parent.
`Engine.background()` includes one pending-action dispatcher. Price callbacks
commit and enqueue stop actions while the dispatcher handles venue latency;
queue entries are hints, so losing the queue cannot lose durable work.
Direct hosts that do not run the background loops dispatch synchronously.
Fixed stops capture the observed price before committing other parents, so a
slow action cannot erase another stop’s observed threshold crossing.
Expiry is checked on price observations as well as the timer. Cancellation
persists its intended end state before acting on children. A planned but
unsent child is settled without dispatch; an uncertain child must be found
and canceled before the parent can report cancellation. Acknowledgements
that still report an open child keep the parent pending. Startup expires
overdue waiting stops and resumes pending actions after reconciliation.

Stop-market children use legal venue bounds rather than a fixed five-cent
protection range. Registered `MarketMaster` precision supplies the market
rule; when absent, the engine uses the venue's documented conservative grid.
Kalshi's adapter accepts the persisted precision explicitly. Actual venue
metadata, time-in-force and reduce-only behavior remain venue-verification
work; the offline paper and scripted venues do not establish live guarantees.
Risk checks remain active on the submitted worst price and can refuse a child.
Order value uses the priced outcome: an opening sell buys NO at `1 - YES
price`, so a 0.01 YES limit is valued at 0.99 per NO contract. Reduce-only
values the outcome being closed. Fees remain separate from order notional.

Timestamped trades predating worker startup or the armed parent, and trades
older than the latest accepted print, do not trigger stops. Unready and crossed
books are ineligible. Untimestamped custom events retain the caller's event
ordering; hosts must supply timestamps to reject replayed trades reliably.

## Verification

Checkpoint 1B runs entirely offline against paper and scripted venues. A
file-backed fake venue survives a worker process calling `os._exit`, testing
crashes before sending, after acceptance, and before recording the response.
The suite uses fake venues and captured payloads; live and demo exchange
tests are explicitly excluded.
Real venue submissions require an operator-approved test with a small,
bounded size and explicit cleanup; they are separate from the offline suite.

The bounded [Kalshi stop-limit check](../examples/verify_kalshi_stop_limit.py)
is covered end to end by a mocked HTTP adapter, including delayed single-order
reads and a lost create response. Its wire guard permits at most one create:
one YES contract at 0.01, post-only, with an expiry within 90 seconds. It
forbids broader writes and cancels only a child matched to the test client key.
Without `--execute`, the script performs public market/fee reads only. Execution
requires separate operator authorization; credentials and the journal stay
outside the repository. A 2026-10-14 authorized production check accepted and
canceled that child with zero fills, preserving the parent across waiting and
terminal restarts. This establishes that bounded stop-limit path; live
stop-market fills and reduce-only guarantees remain unverified.

The [lifecycle documents](../synpath/engine/orders/lifecycle/README.md) describe
the four types. Both storage backends use those workflows.

The [crypto fill check](../examples/verify_kalshi_stop_fill.py) arms a buy
stop above the current real ask on a 15-minute market, waits at most four
minutes, and submits one IOC limit child capped at 0.75. It permits one
native reduce-only exit of only the entry’s confirmed filled quantity.
Mocked cases cover fills, response loss, timeout and definitive refusal.
Selection checks time, an empty initial position, no existing market orders,
fees, and the market’s funded exchange shard. It performs no cash transfers;
funding a shard needs separate operator authorization.

An authorized 2026-10-14 BTC check observed an ask crossing from 0.46 to
its 0.47 stop, then Kalshi refused the 0.50 child limit for insufficient
crypto-shard balance. The journal records a rejected parent and failed
child intent. No child was accepted and no position remained. This provides
real trigger/refusal evidence, not live fill evidence. Kalshi documents
shard funding in its [exchange sharding guide](https://docs.kalshi.com/getting_started/exchange_sharding).

## Polymarket verification limits

Mocked HTTP tests now exercise Polymarket stop-limit YES/NO direction,
post-only, waiting/terminal restart, targeted cancellation, stop-market FAK
children, and asynchronous fill read-back. Native order rows do not echo the
engine's client key. A create response that is lost stays unresolved and is
never resent; automatic adoption requires a persisted signed order identity.
This gap must be addressed before enabling unattended execution on that venue.
No international Polymarket trade was attempted: the venue's eligibility
endpoint reported this host blocked in GB.

The [Polymarket US check](../examples/verify_polymarket_us_stop_fill.py) uses
one 0.01-contract stop-market entry, a 0.99 YES price bound and native
close-position only after verifying the initially flat market holds exactly
the test fill. Its host renews its execution lease while watching the real
book. US absolute YES bounds are 0.01–0.99 even on a sub-cent tick grid.
US receipts are asynchronous; a pending response is not evidence of a fill.

A 2026-10-14 authorized US BTC 15-minute test observed an ask jump from 0.43
to 0.76 across its 0.44 buy stop. The IOC child filled 0.01 at 0.75, costing
0.0075 USD, then native close-position filled the same quantity at 0.99.
Venue reads confirmed zero position and zero open market orders; both legs
reported zero fees. The harness initially failed to read a streamed receipt;
its native identity was recovered manually from own trade activity, with no
second entry. The parent was audited, reconciled to closed and retained that
state after restart. Streaming receipt handling and lease renewal now have
regression coverage.

An unexpected decoder/host exception after a stop intent is marked sending
must remain unresolved rather than falsely reject an order that may exist.
The supervisor now preserves the pending dispatch for sending/sent intents.
US lacks native client keys and reduce-only on ordinary orders. Automatic
response-lost adoption and general reduce-only stop support remain separate
architecture work; this bounded manually recovered test does not prove them.


## PostgreSQL implementation and verification

`PostgresJournal` reuses the shared journal transactions and type records. Its
connection translates fixed journal SQL and bound parameters, serializes entire
transactions against other tasks on the session, and rolls back canceled writes.
Schema setup is transactional and guarded by a database advisory lock. The first
PostgreSQL baseline is logical schema version 6; reopen is idempotent, and an
unknown version fails without modifying it. SQLite's historical file migrations
remain SQLite-only. New PostgreSQL migrations must extend this explicit baseline.

PostgreSQL uses 64-bit timestamps, server-generated event/configuration sequence
IDs, and the same exact decimal and versioned JSON text representation as SQLite.
Sequences can have gaps after rollback; event consumers use ordering, not
contiguous IDs. `synchronous_commit=on` requests durable commit acknowledgement;
operators must retain PostgreSQL's normal WAL/fsync durability settings.

One execution worker owns a database/schema through an atomic lease upsert.
Expiry and renewal use the database clock, not the application's simulated clock.
An expired owner cannot renew itself, and releasing a stale owner cannot remove
the replacement worker's lease. PostgreSQL does not enable concurrent execution
workers or multi-user authorization.

The opt-in test suite uses a real disposable PostgreSQL database and unique test
schemas, plus paper/scripted venues and mocked Kalshi, Polymarket and Polymarket
US HTTP adapters. It covers type ownership, immutable requests, atomic audit and
child planning, cancellation, expiry, IOC remainders, lost responses, concurrent
initialization/reservations, lease loss/takeover, transaction cancellation, failed
initialization/commit, and hard process crashes at all three dispatch boundaries.
It uses no real credentials or exchange connections:

```sh
# This must be a disposable test database: each test creates and drops its schema.
SYNPATH_TEST_POSTGRES_DSN=postgresql://localhost/synpath_test \
  python -m pytest tests/test_postgres_storage.py
```

Live PostgreSQL-backed exchange verification remains pending. The venue-specific
identity and reduce-only limitations above apply equally to both backends.


Local checkpoint evidence on PostgreSQL 18.6: the full compiled offline SDK suite
passed 2,372 tests (3 skipped, 137 live/demo deselected). The final storage subset
passed 89 tests, including 74 PostgreSQL cases. A dedicated PostgreSQL service
job in `.github/workflows/tests.yml` repeats those database and mocked-adapter
contracts on main, pull requests and the experimental branch.
