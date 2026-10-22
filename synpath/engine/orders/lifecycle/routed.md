# Routed bucket order lifecycle

Status: current behavior, not the proposed contract. Read alongside [shared managed states](managed-states.md) and [the draft improvement plan](routed-lifecycle-plan.md).

## Accepted instruction

[`Engine._submit_bucket()`](../../engine.py) accepts only `type="market"` with a required `price`: the worst accepted price in bucket terms. The bucket must be saved and active, have an enabled member, and have adapters for its enabled venues. The parent captures the enabled-member definition; later bucket edits do not change that order.

[`RoutedLimit`](../routed.py) manages this instruction despite its historical class name. Its optional settings include `min_stay_s`, `max_rounds`, `max_age_s` and precision overrides. Zero routing limits mean disabled bounds. Size and price validation also comes from `OrderRequest` and risk checks. These are parent instructions; children are priced GTC limit orders.

## Current transitions

| Observation or action | Current transition and behavior |
|---|---|
| Construct parent | Begins `waiting`; it has no price trigger to wait for. |
| `start()` | Sets `working`, records start time, and plans immediately. Public status is currently `triggered`. |
| Books change or child progress arrives | While ready and `working`, replan remaining quantity and keep, cancel or add children. A partial fill normally leaves the parent `working`. |
| No member books available | Remain `working`, publish `reason=no_books`, save and wait. |
| Full fill | Completion paths use `done` / public `closed`. `complete_if_done()` also requires no live children. Routed full-fill paths now use cancellation cleanup if other tracked children remain live. |
| Plan has an unfilled remainder and a reason, with no live children | `stop(reason)` ends `canceled` if incomplete. Reasons include `worst_price`, `liquidity`, `min_quantity`. |
| `max_rounds` reached | `stop("max_rounds")` enters `cancelling` before venue calls, then ends `canceled` if incomplete once tracked children have ended. |
| `max_age_s` reached on timer | Same stop path with `max_age`; currently an incomplete order ends `canceled`, not `expired`. |
| User cancel or halt | Routed `cancel()` enters `cancelling` before venue calls, retains the intended final state, and finishes after all tracked children have ended. Timer ticks retry pending cancellations. |
| Explicit `expires_at` reached | Shared timer calls inherited `expire()`, requesting final state `expired` after children are stopped. |
| Error during a controller callback | Shared `_run_hook()` handles it. A parent can become `rejected`; uncertainty handling is not equivalent for all order types. |

`max_rounds` counts later planning rounds that actually placed or pulled an order. The first placement does not increment it. `max_age_s` is elapsed time from `start()`, checked by the timer; it is not a venue-native order expiry.

Explicit expiry plumbing exists, so “bucket orders can never expire” is inaccurate. Bucket submission bypasses the ordinary DAY-to-absolute-expiry conversion, however, and bucket-specific time-in-force semantics are not fully specified. GTD, DAY, IOC and FOK acceptance and behavior need dedicated decisions and tests.

## Current outcome fields

`as_order()` exposes public status, filled and remaining quantity. `report()` additionally exposes internal `state`, `stop_reason`, `filled`, `unfilled`, rounds and per-venue results. A stopped partial fill is structured data, not only prose.

For 100 requested, 30 filled and no liquidity remaining, the routed report can say `state=canceled`, `filled=30`, `unfilled=70`, `stop_reason=liquidity`. A client can display “30/100 filled · stopped: liquidity”. This does not require a separate partial-fill state.

The user-cancel/expiry path does not populate routed `stop_reason` in the same way as `stop()`. General error paths also use detail text. The reason is therefore not consistent across every ending.

## Lifecycle behavior to review

- The shared mapping names active routing `triggered`. We are keeping that mapping; document it as active managed execution rather than treating it as a defect.
- `max_age` and explicit expiry use different final states for time-based endings.
- Stopping now marks `cancelling` before any cancellation calls. Reentrant callbacks process child progress without resuming routing.
- `on_child_order_updated()` and `on_fill()` can finish a cancelling parent after its last tracked child ends. `on_timer()` retries cancellation when a response still reports open or a request fails.
- The intended incomplete ending (`pending_final_state`) is included in the existing managed snapshot so delayed explicit expiry remains `expired`. The first stopping cause/end is retained across repeated cancellation attempts.
- Shared error paths still need a separate review before claiming all unresolved venue submissions prevent finalization. This fix handles tracked children, not every unknown-send case.
- End reasons differ between routing stop, user cancellation, expiry and errors.

Offline regression tests in `tests/test_routed_limit.py: TestPendingCancellation` reproduce delayed cancellation, fill races, retries and reentrant callbacks. Public status names, routing reasons and `max_age` semantics are unchanged.

## What resync means

`resync()` attempts to refresh child order records after the Engine restarts. It fetches each known child from its venue, replaces saved status/progress, and adopts additional children linked to the parent in the journal. A working parent can then consider its remainder; a cancelling parent only continues cleanup.

`_resynced` means this refresh has been attempted in the current process. New parents set it true in `start()` because they have no restored children. Restored parents begin false. The method sets it true before venue reads and currently logs/skips failed reads or missing adapters. Therefore true does **not** mean all children were successfully refreshed.

`_refresh_restored_children_if_needed()` performs that attempt independently of lifecycle state. Routing callbacks then check `state == "working"`; cancelling callbacks instead advance cleanup. There is no longer a working-only `_ready()` gate that drops stopping updates.

## Next phase boundary

The Engine owns the instruction; venues own child acceptance and fills. Routing progress and the intended pending ending use the existing managed snapshot path; no schema or table layout changed. The following phase must assess saved child identities, failed venue reads, uncertain sends and cancellation races across process failure before stronger recovery guarantees can be claimed.
