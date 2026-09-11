# Batch statements

Use batches for related DML statements. Prefer single-partition batches;
multi-partition batches add coordinator work and network hops. Updates within
one partition are atomic. Across partitions, a failed unlogged batch may be
partly applied.

```go
batch := session.Batch(gocql.UnloggedBatch).WithContext(ctx)
batch.Query(
	"INSERT INTO events (account_id, event_id, payload) VALUES (?, ?, ?)",
	accountID,
	firstEventID,
	firstPayload,
)
batch.Query(
	"INSERT INTO events (account_id, event_id, payload) VALUES (?, ?, ?)",
	accountID,
	secondEventID,
	secondPayload,
)

if err := session.ExecuteBatch(batch); err != nil {
	return err
}
```

Multi-partition logged batches use the batch log to ensure all mutations
eventually complete or none do. They do not provide cross-partition isolation:
concurrent readers may observe mutations to different partitions separately.
Recovery completes outstanding mutations rather than rolling back applied
ones. An error does not always prove that no mutation occurred. After a timeout
or post-send transport error, a `gocql.QueryError` with
`PotentiallyExecuted() == true` means the final outcome is unknown. Reconcile
application state before retrying a non-idempotent batch.

Use logged batches when all-or-none completion across partitions is required,
not as a general throughput optimization. Counter updates require
`gocql.CounterBatch`.

For variable-length batches that repeatedly use the same statement,
`Session.ExecuteBatch` prepares and caches individual statements more
efficiently than sending a complete `BEGIN BATCH ... APPLY BATCH` string
through `Query.Exec`.
