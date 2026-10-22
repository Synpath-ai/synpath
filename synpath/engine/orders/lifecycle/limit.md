# Limit order

Required fields are market ID, side, quantity and `price`. Supported time
in force and expiry follow the adapter; GTD requires an absolute expiry.

Risk validation precedes persistence and dispatch. The venue can rest the
order and owns its fills and eventual cancellation or expiry. A later venue
observation updates progress without changing the submitted limit instruction.
An uncertain send is reconciled before retrying. A normal worker restart
preserves the order; it does not imply cancel-and-replace.

The `limit_orders` table stores the instruction and latest execution state.
For example, a sell for 10 at 0.38 can fill at 0.39; retain the original 0.38
instruction alongside the reported execution result.

See [shared storage and ownership](../../README.md).

A same-key retry returns the original result. Reusing the key for another
instruction is a conflict. An ambiguous earlier send is reconciled; it is
never automatically resent. A send with no verdict raises `SubmissionInDoubt`
(`202 submission_in_doubt` over HTTP); a retry before the venue answers raises
`SubmissionPending` (`202 submission_pending`).

Cancelling an order already `closed`, `canceled`, `rejected` or `expired`
returns it unchanged without a venue call. If the venue answers a cancel with
"not found", the engine reads the order back and records the venue's verdict
with its filled quantity: a limit that filled before the cancel arrived ends
`closed`, never `canceled`. A venue with no record of the order confirms
nothing (`CancelUnconfirmed`, `409 cancel_unconfirmed`); the last observed
status stands.
