# Executing statements

`Statement[P,R]` pairs CQL with `ParamEncoder[P]` and `RowDecoder[R]`. Prepare
once, bind typed parameters and reuse immutable prepared handles/bindings.
`BindUnpreparedOwned` and `BindUnpreparedBorrowed` require explicit parameter
Columns. `Exec` returns Result for statements without result columns; `Run`,
`RunPage`, `RunUnpaged` and `RunStream` select the receive/paging model.

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
