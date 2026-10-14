# Market order

Required fields are market ID, side and quantity. Venues requiring a worst
price also require `price`. The worker assigns `user_id` and the client key.

Risk validation precedes the durable instruction and dispatch intent, then
the venue receives the order. The venue owns acceptance and fills; local
observations record pending/open/closed/canceled/rejected/expired results.
IOC may finish with an unfilled remainder. Cancellation acts on an accepted
venue order; uncertain sends must be reconciled by the existing client key.

The `market_orders` table preserves the original instruction separately from
the latest venue result. For example, buying 10 contracts at the best available
price with an internal worst price of 0.99 persists before the adapter call.

See [shared storage and ownership](../../../../docs/order-storage.md).
