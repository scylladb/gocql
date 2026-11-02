# Batches

Capture each typed member with `query.BatchEntry(parameters)` and append the
returned item, then call `ExecBatch(ctx, session, kind, items, options)`.
Parameters become owned when the entry is created; entries have no Close/Wait
obligation. Query reuse or later input mutation cannot change an existing entry.
`LoggedBatch`, `UnloggedBatch` and `CounterBatch` select native batch kind.
A counter batch contains only counter updates. Prepared metadata, routing,
parameter schemas and observation remain typed. Batches do not speculate.

Conditional batch results use `RunBatch` with an explicit row decoder. See
[lightweight transactions](lightweight-transactions.md).
