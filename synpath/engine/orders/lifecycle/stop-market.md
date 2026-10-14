# Stop market order

Required fields are market ID, side, quantity and `stop_price`. Trigger source
is touch (default), last or mid. Armed TIF is GTC, GTD or DAY; GTD needs expiry.

Creation persists a waiting parent without contacting the venue. Buy fires
when the watched YES price reaches or exceeds the stop; sell fires when it
reaches or falls below it. Touch uses the YES ask for buys and bid for sells.
The irreversible triggered state dispatches one market IOC child for the
remaining quantity. The intended experimental policy uses legal venue price
bounds; checkpoint 1B must replace the existing fixed protection policy.

The parent ends closed (including recorded IOC remainder), canceled,
rejected or expired. No executable book at a last-trade trigger rejects it.
Expiry applies while armed. A child inherits owner and reduce-only; actual
no-flip enforcement requires venue verification. Cancel before trigger ends
without a child. Pending child actions must be reconciled after restart.

`stop_market_orders` owns the request and durable state. Checkpoint 1A restores
waiting snapshots and identity; atomic trigger/dispatch and expiry-race
hardening are checkpoint 1B work. Example: sell 100 at stop 0.40 after the
YES bid falls to 0.39.

See [shared storage and ownership](../../../../docs/order-storage.md).
