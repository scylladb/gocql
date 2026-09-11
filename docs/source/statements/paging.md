# Paging

Automatic paging uses `ClusterConfig.PageSize`, or the value set by
`Query.PageSize`. Very small pages increase round trips; the default is 5000.

For manual paging, keep the returned page state opaque and pass it only to the
same query and protocol version:

```go
var pageState []byte
var eventID gocql.UUID
var payload string

for {
	iter := session.Query(
		"SELECT event_id, payload FROM events WHERE account_id = ?",
		accountID,
	).PageSize(100).PageState(pageState).Iter()

	nextPageState := iter.PageState()
	for iter.Scan(&eventID, &payload) {
		// Process the row.
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if len(nextPageState) == 0 {
		break
	}
	pageState = nextPageState
}
```

A server can return an empty intermediate page for range or `ALLOW FILTERING`
queries. Use `Iter.Scan`, `Iter.LastPage`, or the returned page state instead
of treating an empty page as completion.

Page state contains primary-key data. Protect it with authenticated encryption
before exposing it in a URL or another untrusted location. Never modify it or
reuse it with a different statement or protocol version.
