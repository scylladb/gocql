# Bind markers

A statement takes values through bind markers: anonymous `?` markers or named
`:name` markers. Bind `?` markers positionally, and use `gocql.NamedValue` only
for `:name` markers you wrote yourself.

## Positional binding

Pass one value per `?`, in the order the markers appear. An `IN` restriction
takes either a single marker bound to a slice, or one marker per element:

```go
iter := session.Query(
	"SELECT payload FROM events WHERE account_id = ? AND event_id IN ?",
	accountID,
	[]gocql.UUID{firstEventID, secondEventID},
).WithContext(ctx).Iter()

iter = session.Query(
	"SELECT payload FROM events WHERE account_id = ? AND event_id IN (?, ?)",
	accountID,
	firstEventID,
	secondEventID,
).WithContext(ctx).Iter()
```

Positional binding sends no names to the server, so it works the same against
every server version.

## Named binding

Name a marker with `:name` and bind it with `gocql.NamedValue`. Named values
can be passed in any order; the driver matches each one to the marker with the
same name:

```go
iter := session.Query(
	"SELECT payload FROM events WHERE account_id = :account AND event_id IN :events",
	gocql.NamedValue("events", []gocql.UUID{firstEventID, secondEventID}),
	gocql.NamedValue("account", accountID),
).WithContext(ctx).Iter()
```

- Marker names follow CQL identifier rules: an unquoted `:Account` is named
  `account`, while a quoted `:"Account"` keeps its case. Pass the name as the
  server reports it.
- Bind either every value by name or none. The driver rejects a mix of named
  and positional values, and a name that matches no marker.
- Pass each name once, even if the marker appears several times in the
  statement; the value binds every occurrence. The driver rejects a name passed
  more than once.
- Batches do not support named values. Use positional values in a batch.
- With native protocol v3, a named marker whose name differs from its partition
  key column is not used for token-aware routing. Use protocol v4 or newer, or
  name the marker after the column.

## Do not bind anonymous markers by name

The server also reports a name for each `?` marker. For a marker that stands
for a column, that is the column name. For other markers, such as those in an
`IN` restriction, the server makes a name up. For example, ScyllaDB 2026.3
reports `IN(ck)` for `ck IN ?`, and `value(IN(ck))` for each marker of
`ck IN (?, ?)`. Other ScyllaDB versions and Apache Cassandra spell these
differently, for example `in(ck)`.

Do not use these names to bind values, whether through `gocql.NamedValue` or by
looking them up in a binding callback. They can change when a node is
upgraded, can differ between nodes during a rolling upgrade, and are not
unique: several markers can share one. Name the marker explicitly with
`:name` instead, or bind it by position.

## Binding callbacks

`Session.Bind` and `Batch.Bind` call a function with the statement's prepared
metadata, `gocql.QueryInfo`, and use the values it returns. `QueryInfo.Args`
lists the markers in order. Return values in that order, and identify markers
by index rather than by `Args[i].Name`:

```go
query := session.Bind(
	"SELECT payload FROM events WHERE account_id = ? AND event_id IN ?",
	func(info *gocql.QueryInfo) ([]any, error) {
		return []any{accountID, eventIDs}, nil
	},
)
```

The callback can run more than once for a single execution, and each run can
see different metadata, as described below. Keep it free of side effects.

## Repreparation and rolling upgrades

Each node prepares statements independently, and the driver caches the
prepared metadata separately for each node. When a node no longer knows a
prepared statement, for example after a restart, an upgrade, a schema change
or a cache eviction, it rejects the request as unprepared. The driver then
prepares the statement again on that node, binds the values against the new
metadata, and retries. This happens transparently; the binding callback runs
again with the new metadata.

During a rolling upgrade, this has some consequences:

- The metadata a binding callback receives comes from the node chosen for that
  attempt. Server-generated marker names can differ from one attempt to the
  next, including retries and speculative executions.
- Named values are sent to the node with their names, and the node matches
  them against its own markers. Names you wrote with `:name` are the same on
  every node; server-generated names are not.
- Positional values carry no names and are unaffected.
