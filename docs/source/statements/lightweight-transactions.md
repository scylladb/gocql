# Lightweight transactions

Use `ScanCAS` or `MapScanCAS` for a conditional statement. Always inspect the
returned `applied` value:

```go
var currentVersion int
applied, err := session.Query(
	"UPDATE documents SET body = ?, version = ? WHERE id = ? IF version = ?",
	newBody,
	nextVersion,
	documentID,
	expectedVersion,
).ScanCAS(&currentVersion)
if err != nil {
	return err
}
if !applied {
	// Another writer changed the document.
}
```

Use `ExecuteBatchCAS` or `MapExecuteBatchCAS` for conditional batches. At least
one statement must be conditional. Every statement, including unconditional
statements, must target the same table and partition, and every condition must
succeed for the batch to apply.

Both methods return an iterator after scanning the first result row. Always
close a non-nil iterator, even when no additional rows are needed, and handle
the error from `Iter.Close` so its page buffers and other resources are
released.

The default serial phase uses `LOCAL_SERIAL`; the commit phase uses
`LOCAL_QUORUM`. Both local levels refer to the coordinator's datacenter.
`LOCAL_SERIAL` provides serial ordering within that datacenter, not across
datacenters. LWTs coordinated in different datacenters do not have a shared
global serial order. Configure all clients that require the same local LWT
ordering to use the same datacenter, with cross-datacenter failover disabled;
the default host selection policy is not datacenter-aware. See
[Load balancing](../load-balancing.md) for the policy configuration.

Set `ClusterConfig.SerialConsistency` or the query/batch
`SerialConsistency` setter to `gocql.Serial` when the serial phase must span
datacenters. Set the regular consistency separately for the commit phase.

All clients issuing conditional statements or serial reads on the same data
must agree on the serial consistency scope. For a serial read, select
`gocql.Serial` or `gocql.LocalSerial` with the query's `Consistency` setter;
`SerialConsistency` configures the serial phase of conditional statements.
Mixing `SERIAL` and `LOCAL_SERIAL` can use disjoint Paxos quorums and lose
cross-client LWT ordering. This includes older clients
that leave serial consistency unset and therefore use the server's `SERIAL`
default. Explicitly retain `gocql.Serial` on all competing clients throughout a
rolling driver upgrade. An intentional move to `LOCAL_SERIAL` is a separate
migration that must be validated for the workload and server version, including
existing data and Paxos state. Prevent old `SERIAL` requests, retries and
unresolved in-flight operations from overlapping new local-scope requests.
Draining clients alone does not establish state convergence or make the scope
change safe; retain `SERIAL` until that migration has been validated. The local
serial scope also requires all such clients to use the same datacenter.

Lightweight transactions use Paxos and cost more than ordinary reads and
writes. Use them only when compare-and-set semantics are required.
