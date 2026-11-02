# Executing statements

`Statement[P,R]` pairs CQL with `ParamEncoder[P]` and `RowDecoder[R]`. Prepare
once and share immutable prepared handles. Create one `NewQuery(prepared,
options)` per worker, then pass parameters directly to its `Exec`, `Run`,
`RunPage` or `RunUnpaged` methods. `NewUnpreparedQuery` requires explicit parameter
Columns. `NewStreamQuery` uses a streaming decoder with page buffering by
default; set `StreamOptions.Incremental` for bounded incremental receive.
Queries reject overlap with `ErrBusy` and own captured parameters through drain.

`Options` overlays the selected profile using explicit `Option[T]` values for
settings where zero is meaningful. It carries consistency, paging, tracing,
custom payload, idempotence and host/shard targeting. `Kind` classifies known
ordinary/conditional statements; unknown or conditional requests cannot
speculate. Server-confirmed conditional metadata takes precedence.

```{toctree}
:maxdepth: 1

paging
streaming
batches
lightweight-transactions
request-timeouts
```
