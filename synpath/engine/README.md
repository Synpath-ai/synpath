# Execution engine

`synpath.engine` owns user instructions, durable lifecycle state, risk checks,
venue dispatch and reconciliation. The terminal consumes the engine; it does
not maintain another authoritative order database. See the public
[engine guide](../../docs/engine.md) for examples and
[order lifecycles](orders/lifecycle/README.md) for type-specific behavior.

## Storage configuration

SQLite is the default. PostgreSQL requires the optional `synpath[postgres]`
driver. SQLAlchemy asynchronous connections serve both databases; Alembic
owns versioned schema and reviewed data migrations.

```toml
storage_backend = "sqlite"
journal = "/absolute/persistent/path/synpath.db"
halt_on_exit = false
session_timezone = "Europe/London"
session_end = "23:59:59"
```

For PostgreSQL:

```toml
storage_backend = "postgresql"
journal = "postgresql://synpath@localhost/synpath_orders"
storage_schema = "execution"
halt_on_exit = false
```

The equivalent Python configuration is
`EngineConfig(storage_backend="postgresql", journal_path=dsn, storage_schema="execution")`.
Use a password file or deployment secret mechanism; do not commit a credentialed
DSN. Sidecar display/registry metadata uses `postgresql:<schema>`. The sidecar's
control/authentication database remains separate from execution storage.

Select one backend and one active execution worker. PostgreSQL does not enable
multiple workers. SQLite uses WAL, full synchronous durability and foreign-key
enforcement; PostgreSQL requests synchronous commits and uses database-clock
execution leases. Operators must retain PostgreSQL WAL/fsync durability settings.

## Application owner

Set `SYNPATH_USER_ID`, or pass `EngineConfig(user_id=...)`, to a stable nonempty
identity. The default is `local-user`. Migration seeds `users.user_id` once; a
unique singleton column permits one user row. Restart with another configured
identity fails. An injected journal must match its engine's identity.

All four instruction tables and submission reservations reference that user.
Requests cannot override the engine's owner; children inherit their parent's
owner. This is ownership enforcement, not customer authentication. Future
multi-user support requires a migration of the singleton restriction and
separate execution/authentication design.

## Schema and migration compatibility

The current logical version is 11. Alembic revision `0001_journal_v6` adopts the
frozen pre-Alembic baseline; `0002_single_user` adds the owner and foreign keys;
`0003_bucket_member_enabled` adds `bucket_members.enabled` (existing members stay on);
`0004_orders_recent` indexes `orders` newest first, overall and per market, and
gives any row without a `created_ts` its `updated_ts`; `0005_drop_fair_values` drops
the unused `fair_values` table. `0006_routed_execution_scope` adds indexed
execution-account membership for routed parents and removes their nominal venue
from snapshots and readable projections. See the upgrade preflight below.
See the [migration entry point](migrations/README.md). Historical SQLite
conversions prepare old files for the baseline; new changes use new revisions.

Startup runs migrations through the selected SQLAlchemy connection under the
write/schema lock and transaction. Unknown schema or revision versions fail.
New migrations are reviewed explicitly. Current SQLAlchemy metadata describes
ownership/history objects; unrestricted autogeneration against it would not
represent the entire journal schema and must not be used.

Existing instructions must belong to the configured user. Unknown or conflicting
instruction ownership stops migration for explicit review. Never relabel old
instructions silently. Unattributed venue observations with no original request
are preserved in `historical_order_records`, with their original identity and
snapshot still available through `typed_order`.

Back up and verify restore before upgrading an existing store. The owner migration refuses downgrade; later revision downgrades may remove
fields or recreate dropped tables empty. Rollback requires compatible code and
a verified schema/data backup. Migrations are application-managed because they require
owner selection and locking; standalone/offline Alembic without that context is
refused. Selecting PostgreSQL creates a new store. Importing an existing SQLite
journal, including armed orders, is a separate operation and is not automatic.

## Instruction tables and dispatch

`market_orders`, `limit_orders`, `stop_market_orders`, and `stop_limit_orders`
are separate authoritative records. They retain ownership, account, client key,
original request, type-specific fields, lifecycle state and a versioned snapshot.
`orders` is the shared execution projection for reconciliation, ledger and API
reads. Other advanced types retain their existing managed-order persistence.
It holds venue orders and engine-held parents alike, so `orders_page` lists
every state newest first by `(created_ts, venue, id)`, a unique key that pages
without repeats or gaps (`GET /trading/orders?scope=all`); a child's
`parent_id` groups it under its parent.

A type record, its event and pending action commit together before a venue call.
Venue responses update execution state while preserving the original instruction.
Submission keys are reserved globally within this single-user store. A same-key
retry returns the existing result; a different instruction is a conflict. DAY
expiry is resolved once to an absolute timestamp, preventing restart/retry from
extending its life.

A stop crossing commits the irreversible parent transition, stable pending child
request and planned intent in one transaction. Sending happens afterwards. An
accepted child is attached before pending state is cleared. A restart may dispatch
an unsent planned action; a sending/unknown action must be reconciled first and
must never be blindly resent. Venue history pagination must complete. Venues
without echoed client keys require a durable native identity for adoption.

The background dispatcher keeps venue latency off price observations. Its queue
is a hint; losing it cannot lose committed work. Parent hooks serialize price,
expiry, cancellation and child updates. Cancellation of uncertain children remains
pending until venue status is known.

Only the venue confirms a cancellation. Cancelling a finished order, or an
engine-held parent no longer running because it finished before a restart,
returns its record without a venue call. A venue that answers "not found" to a
cancel is asked for the order itself and its verdict is recorded with the
filled quantity: `closed` if it filled, `canceled` only if the venue says so.
A venue with no record at all raises `CancelUnconfirmed` and the last observed
status stands; a parent pulling such a child stays `pending_cancel` and retries.
A parent whose children turn out to have filled everything ends `closed`.

A send that gets no verdict raises `SubmissionInDoubt` with its
`client_order_id`; its intent stays `sending`. A retry under that id
reconciles with the venue first: it returns the adopted order, or raises
`SubmissionPending` while the venue has no answer yet, and never sends twice. Normal shutdown preserves orders; operator
halt/cancellation is explicit. Stops cannot observe prices while the worker is down.

Stop-market children use venue legal bounds and IOC, recording unfilled quantity.
Stop-limit children retain their requested price and inherit the parent's TIF
and expiry (one TIF for the whole order; DAY goes as GTD at the session end).
A stop reads the book the engine already holds when it is created and when its
market is subscribed, not only on the next change. Precision
comes from registered market rules, with conservative venue defaults otherwise.
Risk checks still apply at the worst submitted price. Opening NO notional uses
`1 - YES price`: a contract is priced as its outcome (`outcome`), bought or sold. Fees are separate.

All displayed prices/averages use the YES leg. Kalshi read projections may report
NO outcome costs alongside `side=yes`; execution averages use `outcome_side`
first to normalize them. Create averages already use YES. `Order.cost` retains
native outcome notional and need not represent YES sale proceeds.

## Adding or consolidating order types

Define and document fields, states, observations, actions, cancel/expiry semantics,
ownership and recovery before implementation. Register validation and processing;
add explicit Alembic schema/data revisions where needed. Shared machinery owns
retry identity, transactions, risk and reconciliation. A new type may use its own
table or a compatible existing table after an explicit design decision; a merge
must preserve instructions, identities, indexes, state and recovery semantics.

Add a small lifecycle document beside the implementation and link this README for
shared rules. Verify fresh stores, migration, both backends, retries, failures,
cancellation, expiry, restart, process crashes and mocked adapter payloads.

## Verification and remaining venue limits

Offline tests use paper/scripted venues and mocked HTTP. Enable database tests
only against a disposable PostgreSQL database; each test creates/drops its own
schema:

```sh
SYNPATH_TEST_POSTGRES_DSN=postgresql://localhost/synpath_test \
  python -m pytest tests/test_postgres_storage.py tests/test_owner_migrations.py
```

Live/demo suites are excluded by default. Real-money checks require explicit,
bounded operator authorization, a specific credential source, guarded writes and
matched cleanup. Private credentials, account records and receipts stay private.
Verification counts, SDK revisions and sanitized results belong in the relevant PR.

Bounded live checks established a PostgreSQL Kalshi stop-limit crossing/fill and
reduce-only exit, plus a Polymarket US eligible stop-market fill and native close,
with terminal replay and no residual test position or orders. These do not prove
all venue guarantees. International Polymarket eligibility remains blocked on the
previous test host. Polymarket/US do not echo client keys; automatic adoption after
response loss needs persisted native identities. US ordinary orders lack general
reduce-only support. Its already-eligible test is not evidence of a future crossing.


## Cross-venue parent identity and upgrade preflight

A bucket order is owned by the engine's stable `user_id`, with `venue=null` and
`account=null`. `execution_accounts` captures each enabled venue's exact Account;
venue children keep their concrete venue/account. A single captured bucket/account
scope is authorized, risk-checked and saved before routing; later bucket edits do
not alter it. One transaction commits the snapshot, account membership, readable
projection, submission result and events. The private `__managed__` storage key
must never be configured as a venue. Normal venue orders retain their concrete
identity. Without an explicit account mapping, the engine uses an adapter's
reported Account when available, otherwise the venue's default Account.

Bucket requests cannot name a singular account or venue. Configure per-venue
accounts instead. Admission requires full-quantity capacity at every eligible
member (including correctly flipped outcome/price), then every actual child
passes current risk checks again. A valid split can therefore be refused when
no individual destination has capacity for the whole quantity. This reserves
neither funds nor capacity. Existing parent and venue states are unchanged.

Viewing a complete parent/report or its audit history requires visibility of all
captured accounts; acting on it requires trade permission on all of them. Child
records retain their individual account access. `/journal`, `/order-log`, live
events and replay use the same parent-scope rules. Caller-provided request params
never establish audit visibility. Old order-list cursors must be discarded; the
new opaque cursor has version 2. Parent lookup works after the order finishes
and the process restarts; venue/account filters use captured membership.

Back up and verify restore before upgrading. Migration preserves user, parent,
client and child IDs, status and fills, and records old nominal attribution as
`legacy_execution_identity`. It infers historical accounts only from the saved
parent account and persisted child orders/intents. When an enabled venue has no
unambiguous historical account evidence, startup refuses the migration and names
the affected parent/venue. Do not replace it with today's configured account.

For a reviewed historical mapping, while the old engine is stopped, store a JSON
object under `meta.key='legacy_execution_accounts'`: parent ID -> venue -> complete
Account object (`venue`, `name`, optional `subaccount`). For example, an explicitly
reviewed mapping has shape `{"mo-example":{"polymarket":{"venue":"polymarket",
"name":"historical-desk","subaccount":null}}}`. This is an operator decision,
not an automatically generated guess. Insert/update that meta row in the existing
backend with bound values, retain the review evidence, then start the new engine
through its normal locked migration path. Conflicting historical evidence still
fails; an explicit mapping cannot silently override a persisted account. Missing
and conflicting cases roll back and can be retried after review on both backends.

A legacy client ID without a verifiable submission reservation is refused rather
than submitted again; retrieve its existing parent by ID. New bucket client-ID
retries use the original captured scope even after a bucket edit. A mismatched
configured/adapter account blocks affected submissions, early uncertain-intent
recovery, child refresh, polling, reconciliation and cancellation. The original
activity stays unresolved until the original execution identity is available;
absence in a replacement account cannot establish that an order is lost.

Do not run older code on the migrated store: restore a compatible backup, with
outstanding venue activity reviewed first. Do not run both engines concurrently.
This upgrade does not resolve the broader uncertain-send/resync limitations in
[the routed lifecycle](orders/lifecycle/routed.md). All PRE-6 verification uses
offline scripted venues; this documentation does not authorize actual trades.
