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

The [lifecycle documents](../synpath/engine/orders/lifecycle/README.md) describe
the four types. PostgreSQL must implement and pass this same storage and
lifecycle contract before it is enabled.
