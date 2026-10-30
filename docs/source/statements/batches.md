# Batches

Convert each typed binding with `BatchEntry`, then call
`ExecBatch(ctx, session, kind, items, options)`. `LoggedBatch`, `UnloggedBatch`
and `CounterBatch` select native batch kind. A counter batch contains only
counter updates. Prepared metadata and parameter schemas remain typed.

Bindings remain borrowed until their own Close and successful Wait after every
admitted batch has drained. Batches preserve per-statement routing metadata,
custom payload and observation. Explicitly idempotent ordinary statements may
retry according to policy; batches do not speculate.

Conditional batch results use `RunBatch` with an explicit row decoder. See
[lightweight transactions](lightweight-transactions.md).
