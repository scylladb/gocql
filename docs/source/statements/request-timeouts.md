# Deadlines and cancellation

Pass a bounded context to Connect, Prepare and every execution. Profile.Timeout
and per-call options bound request execution separately from connect, read,
write, keepalive and schema-agreement budgets. Custom providers and hooks honor
the caller's cancellation.

Cancellation keeps transmitted stream IDs reserved until their late response
or connection close. Accepted writers finish or fail before borrowed input is
released. Configure orphan budgets independently of request timeouts.

`query.Close()` stops admission and cancels active execution. `query.Wait(ctx)`
joins its logical execution, page fetches, speculative losers, callbacks and
accepted writers. A cancelled/timed-out Wait leaves the query busy. All parameter
input is already owned; caller input can be reused after capture. Session.Close
cancels requests and joins workers; retained output page leases remain valid.

RowStream.Close interrupts and joins in-progress destination access. Cancellation
can leave partially written application storage; only a true NextInto return
publishes a complete validated row. Never reuse the destination until Close has
returned when another goroutine may still be decoding.
