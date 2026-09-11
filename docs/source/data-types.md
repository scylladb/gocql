# Data types

The driver marshals CQL values to and from idiomatic Go values. Common
mappings are:

| CQL type | Typical Go type |
| --- | --- |
| `ascii`, `text`, `varchar` | `string` |
| `bigint`, `counter` | `int64` |
| `blob` | `[]byte` |
| `boolean` | `bool` |
| `date` | `time.Time` |
| `decimal` | `*inf.Dec` |
| `double` | `float64` |
| `duration` | `gocql.Duration` |
| `float` | `float32` |
| `inet` | `net.IP` or `string` |
| `int` | `int32` |
| `list`, `set` | slice or array |
| `map` | `map[K]V` |
| `smallint` | `int16` |
| `time` | `time.Duration` or `int64` |
| `timestamp` | `time.Time` or `int64` |
| `timeuuid`, `uuid` | `gocql.UUID`, `string`, or `[]byte` |
| `tinyint` | `int8` |
| `tuple` | slice, array, or struct |
| user-defined type | struct, `map[string]any`, or `gocql.UDTUnmarshaler` |
| `varint` | `*big.Int` |

Integer CQL values can also be unmarshaled into other integer types,
`big.Int`, or their base-10 string representation when the value fits the
destination.

See the [`Marshal` and `Unmarshal` API
documentation](https://github.com/scylladb/gocql/blob/master/marshal.go) for
the full conversion matrix and custom marshaling interfaces.

## Null values

CQL `NULL` normally becomes the destination type's zero value. Scan into a
pointer when the application must distinguish a null value from an empty one:

```go
var value *string
if err := query.Scan(&value); err != nil {
	return err
}
if value == nil {
	// The CQL value was NULL.
}
```

A null `list` or `set` scanned directly into an array returns an error. For a
nullable fixed-size collection, declare a pointer to the array and scan its
address so `NULL` can be represented by a nil pointer.

```go
var values *[3]int
if err := query.Scan(&values); err != nil {
	return err
}
```

## Byte slice memory reuse

When scanning a scalar `blob`, `text`, `varchar`, or `ascii` value into a
`[]byte`, the driver can reuse the destination slice's backing memory on the
next scan. Declare a new slice inside the loop when rows must be retained after
scanning:

```go
scanner := query.Iter().Scanner()
var rows [][]byte

for scanner.Next() {
	var value []byte
	if scanErr := scanner.Scan(&value); scanErr != nil {
		_ = scanner.Err() // Close the iterator before returning.
		return scanErr
	}
	rows = append(rows, value)
}
if err := scanner.Err(); err != nil {
	return err
}
```

Collection slices such as `[]int` receive a newly allocated backing array for
each non-null value and can be retained directly.

## User-defined types

Map a CQL user-defined type to a Go struct with `cql` tags:

```go
type Address struct {
	Street string `cql:"street"`
	City   string `cql:"city"`
}

var address Address
if err := session.Query(
	"SELECT address FROM users WHERE id = ?",
	id,
).Scan(&address); err != nil {
	return err
}
```

UDTs can also use `map[string]any`, `gocql.UDTMarshaler`, or
`gocql.UDTUnmarshaler`. Implement `gocql.Marshaler` and `gocql.Unmarshaler`
when a type needs complete control over its CQL binary representation.
