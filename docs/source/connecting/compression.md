# Compression

Set `ClusterConfig.Compressor` before creating a session to enable Snappy or
LZ4 compression:

```go
cluster.Compressor = &gocql.SnappyCompressor{}
```

LZ4 is a separate module imported from the ScyllaDB fork:

```go
import "github.com/scylladb/gocql/lz4"

cluster.Compressor = &lz4.LZ4Compressor{}
```

When native protocol v5 is explicitly enabled, use LZ4 or no compression.
Snappy does not support v5 transport segments. Protocol v5 is not selected by
automatic protocol discovery and must currently be configured explicitly.

The LZ4 module is versioned independently. Repository tags have an `lz4/`
prefix, while `go.mod` uses the unprefixed version:

```text
require github.com/scylladb/gocql/lz4 v1.19.0
```

Do not replace `github.com/gocql/gocql/lz4`; upstream folded that package into
its main module. Import `github.com/scylladb/gocql/lz4` directly instead.
