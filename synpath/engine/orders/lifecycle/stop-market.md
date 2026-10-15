# Stop market order

Required fields are market ID, side, quantity and `stop_price`. Trigger source
is touch (default), last or mid. TIF is one for the whole order, as in IBKR
and FIX: GTC, GTD (needs expiry) or DAY (held as GTD at the session end). IOC
and FOK are refused. It bounds the armed period; the triggered child is IOC
and ends on its own within it.

Creation persists a waiting parent without contacting the venue, then reads
the book the engine already holds, as does subscribing the market: a stop
already met triggers without waiting for the next change. Buy fires
when the watched YES price reaches or exceeds the stop; sell fires when it
reaches or falls below it. Touch uses the YES ask for buys and bid for sells.
The irreversible triggered state dispatches one market IOC child for the
remaining quantity. The child uses the legal venue grid bound for its side, using registered
market precision when available. An unready, crossed or missing executable
book prevents market dispatch.

The parent ends closed (including recorded IOC remainder), canceled,
rejected or expired. No executable book at a last-trade trigger rejects it.
Expiry applies while armed. A child inherits owner and reduce-only; actual
no-flip enforcement requires venue verification. Cancel before trigger ends
without a child. Pending child actions must be reconciled after restart.

`stop_market_orders` owns the request and durable state. The trigger and
stable pending child intent commit atomically before sending. Startup restores
waiting parents, expires overdue armed stops, and reconciles uncertain
children before resuming. Cancellation remains pending until child status
is known. A cancel never relabels a fill: a parent whose IOC child filled is
already `closed` and stays so, and once a restart leaves it unloaded a cancel
returns its journal record without contacting the venue. The parent reports actual execution averages and IOC remainder. Example: sell 100 at stop 0.40 after the
YES bid falls to 0.39.

See [shared storage and ownership](../../README.md).

Polymarket US clamps children to its absolute 0.01–0.99 YES bounds even
when the tick is finer. Its create response remains pending until a venue
order update confirms execution. A decoder exception after sending preserves
the pending dispatch; it does not establish rejection. See shared verification
limits for venues without native client-key recovery.
