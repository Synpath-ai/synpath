# Order storage and execution ownership

The experimental `codex/order-lifecycle-storage` branch introduces a storage
contract for the execution engine. SQLite is the implemented backend;
PostgreSQL will use the same logical schema and contract at a later checkpoint.
Selecting an unavailable backend raises an error rather than silently using SQLite.

## Configuration

```toml
storage_backend = "sqlite"
journal = "/absolute/persistent/path/synpath.db"
session_timezone = "Europe/London"
session_end = "23:59:59"
# Normal shutdown preserves orders. Operator halt is a separate operation.
halt_on_exit = false
```

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

## Checkpoint limits

Checkpoint 1A establishes these tables, migrations, ownership and restart
configuration. Checkpoint 1B must finish submission deduplication, atomic
trigger plus dispatch planning, stable child retry identity, cancel/expiry
serialization and synthetic recovery. The existence of type tables alone does
not establish those execution guarantees. The current stop-market pricing
still uses its existing protection policy until checkpoint 1B changes and
verifies it.

The [lifecycle documents](../synpath/engine/orders/lifecycle/README.md) describe
the four types. Shared pending actions and recovery will be documented here
as their implementation lands.
