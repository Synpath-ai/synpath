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
| Full fill | Completion paths use `done` / public `closed`. `complete_if_done()` also requires no live children. Some other routed paths pull children then call `finish()` directly. |
| Plan has an unfilled remainder and a reason, with no live children | `stop(reason)` ends `canceled` if incomplete. Reasons include `worst_price`, `liquidity`, `min_quantity`. |
| `max_rounds` reached | `stop("max_rounds")` pulls children, then ends `canceled` if incomplete or waits in `cancelling` if children remain live. |
| `max_age_s` reached on timer | Same stop path with `max_age`; currently an incomplete order ends `canceled`, not `expired`. |
| User cancel or halt | Inherited `cancel()` enters `cancelling`, pulls children, then ends `done` if filled or `canceled` if incomplete and no live children remain. |
| Explicit `expires_at` reached | Shared timer calls inherited `expire()`, requesting final state `expired` after children are stopped. |
| Error during a controller callback | Shared `_run_hook()` handles it. A parent can become `rejected`; uncertainty handling is not equivalent for all order types. |

`max_rounds` counts later planning rounds that actually placed or pulled an order. The first placement does not increment it. `max_age_s` is elapsed time from `start()`, checked by the timer; it is not a venue-native order expiry.

Explicit expiry plumbing exists, so “bucket orders can never expire” is inaccurate. Bucket submission bypasses the ordinary DAY-to-absolute-expiry conversion, however, and bucket-specific time-in-force semantics are not fully specified. GTD, DAY, IOC and FOK acceptance and behavior need dedicated decisions and tests.

## Current outcome fields

`as_order()` exposes public status, filled and remaining quantity. `report()` additionally exposes internal `state`, `stop_reason`, `filled`, `unfilled`, rounds and per-venue results. A stopped partial fill is structured data, not only prose.

For 100 requested, 30 filled and no liquidity remaining, the routed report can say `state=canceled`, `filled=30`, `unfilled=70`, `stop_reason=liquidity`. A client can display “30/100 filled · stopped: liquidity”. This does not require a separate partial-fill state.

The inherited cancel/expiry path does not populate routed `stop_reason` in the same way as `stop()`. General error paths also use detail text. The reason is therefore not consistent across every ending.

## Lifecycle behavior to review

- The shared mapping names active routing `triggered`. We are keeping that mapping; document it as active managed execution rather than treating it as a defect.
- `max_age` and explicit expiry use different final states for time-based endings.
- `stop()` attempts child cancellation before setting `cancelling`, unlike the shared cancel path. Child updates can call back into the parent while it is still working.
- After `cancelling`, routed `_ready()` returns false. Routed `on_timer()` and `on_child_order_updated()` then skip their ordinary work. General pending-cancellation retry and completion are not clearly covered by that path; repeated explicit expiry has separate controller handling.
- Some full-fill/error paths can call `finish()` without a shared guarantee that all children are resolved. Tests must establish when a final state is safe.
- End reasons differ between routing stop, user cancellation, expiry and errors.

These are code-path observations, not a claim that every scenario has been reproduced. The draft plan identifies the offline cases needed before changing behavior.

## Next phase boundary

The Engine owns the instruction; venues own child acceptance and fills. Current routing progress is saved through the managed snapshot path and child records. This phase changes neither storage nor restart behavior. The following phase must assess saved child identities, failed venue reads, uncertain sends and cancellation races before live guarantees can be claimed.
