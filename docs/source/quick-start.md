# Quick start

## Supported Go versions

The module's `go.mod` declares Go 1.25 as its minimum version. CI builds and
tests with that version.

## Install

This driver is a drop-in replacement for `github.com/gocql/gocql` and retains
that module path for compatibility. Add the replacement, then require the
package:

```console
go mod edit -replace=github.com/gocql/gocql=github.com/scylladb/gocql@v1.19.0
go get github.com/gocql/gocql
```

Replace `v1.19.0` with the intended released `v1` tag or a pseudo-version. The
module path has no `/v2` suffix, so `v2` tags cannot be used here. See
[available releases](https://github.com/scylladb/gocql/releases). Run
`go mod tidy` after adding the Go source below.

## Run ScyllaDB locally

Start a single-node ScyllaDB instance:

```console
docker run --name scylla --rm -p 127.0.0.1:9042:9042 -d scylladb/scylla:6.1.2 \
  --overprovisioned 1 \
  --smp 1
```

The detached container starts before its CQL service is ready. Wait until a
query succeeds before running the Go program:

```console
until docker exec scylla cqlsh -e \
  "SELECT release_version FROM system.local" >/dev/null 2>&1; do
  sleep 1
done
```

## Connect and run a query

Create a cluster configuration, enable token-aware host selection, then create
a session:

```go
package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
		gocql.RoundRobinHostPolicy(),
	)

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	var releaseVersion string
	if err := session.Query(
		"SELECT release_version FROM system.local",
	).Scan(&releaseVersion); err != nil {
		log.Fatal(err)
	}

	fmt.Println("connected to ScyllaDB", releaseVersion)
}
```

Continue with [Connecting to the cluster](connecting/index.md) and
[Executing CQL statements](statements/index.md) before using the driver in a
production deployment.
