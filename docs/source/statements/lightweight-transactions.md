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

Lightweight transactions use Paxos and cost more than ordinary reads and
writes. Use them only when compare-and-set semantics are required.
