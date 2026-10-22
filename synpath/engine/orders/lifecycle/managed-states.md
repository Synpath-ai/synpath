# Managed order states in the current code

Status: current implementation walkthrough, read on 22 October 2026 at SDK commit `5ae2269` with local readability refactors. Proposed changes belong in [the routed lifecycle plan](routed-lifecycle-plan.md).

A managed order is an instruction the Engine continues running, such as a stop, iceberg or routed bucket order. Its state describes its execution phase. Its filled quantity is a separate measurement. This distinction lets an order be both canceled and partially filled.

## Internal state and public status

[`base.py`](../base.py) defines `State`, `LIVE_STATES` and `STATUS_OF`. `ManagedOrder.as_order()` translates internal state into the public `Order.status` using that shared mapping.

| Internal state | Public status today | Meaning |
|---|---|---|
| `waiting` | `waiting` | Initial state; for a stop, its condition has not fired. For a bucket, this is only the initial state before `start()`. |
| `working` | `triggered` | The implementation has started its active behavior. For a routed order, read `triggered` as active managed execution, not a price trigger; the existing mapping is being retained. |
| `cancelling` | `pending_cancel` | The parent is trying to stop its children; it is not yet finished. |
| `done` | `closed` | The implementation considers its instruction complete. Routed orders use this for full fills. Other order types need their own completion rules. |
| `canceled` | `canceled` | The parent has ended without completing its requested quantity; fills may already exist. |
| `rejected` | `rejected` | An error path ended the managed order. This is distinct from rejecting a request before a parent is created. |
| `expired` | `expired` | The parent ended through the shared explicit-expiry path. Type-specific timers do not necessarily use this state. |

Only `waiting`, `working` and `cancelling` are considered live. Final states are excluded from the controller's periodic processing. Therefore choosing a final state too early can prevent subsequent cleanup.

The seven names form a shared vocabulary, not a complete transition specification for every type. The implementation classes decide which transitions occur.

## Quantity is independent of state

`ManagedOrder.filled` sums the latest recorded filled quantity of each child. `remaining` is `max(0, requested - filled)`. The controller's `on_order()` records a child's latest venue result, derives any new fill progress, and calls the parent's `on_fill()` and `on_child_order_updated()` methods. Separate venue fill notifications go to the ledger, not directly to parent progress.

For an instruction to buy 100:

| State | Filled | Remaining | Interpretation |
|---|---:|---:|---|
| `working` | 30 | 70 | Still trying to fill the remaining 70. |
| `cancelling` | 30 | 70 | Trying to stop; some of the remaining 70 may still fill. |
| `canceled` | 30 | 70 | Stopped with 30 filled and 70 unfilled. |
| `done` | 100 | 0 | Fully filled, for this routed order. |

`remaining` is unfilled requested quantity, not necessarily quantity still resting at venues. A final canceled order can retain a positive `remaining`.

## Who changes the state

1. `ManagedOrderController.create()` constructs the implementation, saves and registers it, then calls its `start()` through `run()`.
2. The controller forwards book updates, child order updates and timer ticks to that implementation.
3. Type-specific methods decide whether to continue, stop or finish. `finish()` sets state and detail, saves, then publishes an event.
4. The shared `cancel()` sets `cancelling` before pulling children. It stays pending while children remain live; otherwise it ends `done` if filled or uses the requested final state.
5. Shared `expire()` calls `cancel(..., end="expired")`. The controller checks explicit `expires_at` on timer ticks.
6. `_run_hook()` handles errors and saves the resulting parent. Network errors and timeouts retain the current state with a detail message. Other errors can end `rejected`, subject to the implementation's recorded uncertain-child information.

A call to `finish()` does not itself verify that every child is terminal. Each caller is responsible for satisfying that condition. Read type-specific paths before assuming the shared helpers enforce it.

## Code reading order

- [`base.py`](../base.py): `State`, `STATUS_OF`, `filled`, `cancel()`, `expire()`, `finish()`, `complete_if_done()`, `as_order()`.
- [`manager.py`](../manager.py): `create()`, `_run_hook()`, `on_order()`, `on_timer()`.
- [Routed order lifecycle](routed.md): the bucket-specific path and its current gaps.

Storage transactions, restart guarantees and uncertain-send recovery are the next review phase. This walkthrough does not establish those guarantees; see the [engine README](../../README.md) for the existing shared design.
