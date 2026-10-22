# Stop limit order

Required fields are market ID, side, quantity, `stop_price` and `price`.
A sell limit is at or below its stop; a buy limit is at or above it.
Trigger source is touch, last or mid. TIF is one for the whole order, armed
and after trigger, as in IBKR and FIX: GTC, GTD (needs expiry) or DAY (held as
GTD at the session end, an absolute expiry persisted at submission). IOC and
FOK are refused.

Creation persists waiting without a venue order, then reads the book the
engine already holds: a stop already met triggers at once. Subscribing the
market does the same, since a venue sends no fresh snapshot for a market the
shared connection already carries. A `last` stop waits for a print made after
it. A threshold observation
moves irreversibly to triggered and plans a limit child using the requested
price, the parent's TIF and expiry (GTC stays GTC; GTD and DAY go as GTD at
the same `expires_at`) and remaining quantity. The child can rest after a price
gap. Parent progress is derived from child results and ends closed, canceled,
rejected or expired. Cancel after trigger pulls the resting child; remain
pending until its outcome is known. Expiry (GTD or DAY) applies before
trigger and to the working child: a parent that expires with its child resting
cancels the child and ends `expired`. Children inherit user identity and the order's outcome and action.

`stop_limit_orders` owns the instruction and durable state. The trigger and
pending child intent commit together. Stable child identity survives restart;
uncertain submissions are reconciled before any repeat effect. Expiry is
checked before triggering and before resuming an unsent limit child.
Cancellation that reports a still-open child remains pending and is retried.
If the venue answers the child's cancel with "not found", the child is read
back: a child that filled ends the parent `closed` with its filled quantity,
not `canceled`. A child the venue has no record of keeps the parent
`pending_cancel` and is retried; it is never assumed cancelled. A finished
parent is never sent to the venue, before or after a restart.
Example: sell 100 with stop 0.40 and limit 0.38; a bid gap to 0.35 can leave
the triggered limit unfilled.

See [shared storage and ownership](../../README.md).

A `post_only` stop-limit propagates that flag to its child. The venue may
reject a child that would take liquidity.
