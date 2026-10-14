# Stop limit order

Required fields are market ID, side, quantity, `stop_price` and `price`.
A sell limit is at or below its stop; a buy limit is at or above it.
Trigger source is touch, last or mid. Child TIF is GTC, GTD, IOC or FOK.

Creation persists waiting without a venue order. A threshold observation
moves irreversibly to triggered and plans a limit child using the requested
price, TIF, expiry and remaining quantity. The child can rest after a price
gap. Parent progress is derived from child results and ends closed, canceled,
rejected or expired. Cancel after trigger pulls the resting child; remain
pending until its outcome is known. GTD applies before trigger and to the
working child. Children inherit user identity and reduce-only.

`stop_limit_orders` owns the instruction and durable state. The trigger and
pending child intent commit together. Stable child identity survives restart;
uncertain submissions are reconciled before any repeat effect. Expiry is
checked before triggering and before resuming an unsent limit child.
Cancellation that reports a still-open child remains pending and is retried.
Example: sell 100 with stop 0.40 and limit 0.38; a bid gap to 0.35 can leave
the triggered limit unfilled.

See [shared storage and ownership](../../../../docs/order-storage.md).
