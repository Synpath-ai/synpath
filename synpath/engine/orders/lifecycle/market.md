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

See [shared storage and ownership](../../README.md).

A same-key retry returns the original result. Reusing the key for another
instruction is a conflict. An ambiguous earlier send is reconciled; it is
never automatically resent.

## On Kalshi

Kalshi has no market order type. A `market` request is sent as one
immediate-or-cancel limit at `price` (`fill_or_kill` with `time_in_force=fok`),
so it takes what the book offers up to that price and never rests: it ends
`closed` when anything filled (the unfilled remainder recorded) and
`canceled` when nothing did. A request without `price` is refused before
anything is sent. A caller that wants no worst price of its own passes the
legal bound for its side, as a stop market's child does: 0.99 for a buy (a
YES bid), 0.01 for a sell (a YES ask); the terminal does exactly that.
Risk values the order at its worst price, so `max_order_notional` counts a
market buy of 10 at 0.99 as 9.90 whatever it fills at.

Risk refusals (size, notional, kill switch) are raised before the instruction
is written; nothing reaches the venue.

## Tests

`tests/test_serve_market_orders.py`, through a whole `synpath serve` on the
mocked Kalshi, on SQLite and PostgreSQL: the instruction row in
`market_orders` before the send, one IOC at the worst price, the order
`closed`; a Sell YES at 0.01 keeping its outcome and action (`reduce_only` refused); a same-key retry sends once; a lost
response is `submission_in_doubt` and the retry adopts it without resending;
another instruction under the same key is refused; no worst price is refused
with nothing sent. A stop market's child is also a `market` order on this path
(`stop-market.md`).
