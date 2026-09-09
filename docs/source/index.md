# ScyllaDB gocql driver

ScyllaDB gocql is a shard-aware fork of the Apache Cassandra GoCQL driver. It
keeps the `github.com/gocql/gocql` package API and adds ScyllaDB-specific
features, including shard-aware routing and shard-aware ports.

## Other documentation

- [Source code and releases](https://github.com/scylladb/gocql)
- [ScyllaDB documentation](https://docs.scylladb.com/)

## Contents

- [Quick start](quick-start.md) — install the driver and run a first query.
- [Connecting to the cluster](connecting/index.md) — configure contact points,
  compression, authentication, TLS, and private networking.
- [Executing CQL statements](statements/index.md) — execute prepared, paged,
  batch, and lightweight-transaction statements.
- [Data types](data-types.md) — map CQL values to Go values.
- [Load balancing](load-balancing.md) — route requests to replicas, data
  centers, and shards.
- [Retry policy configuration](retry-policy.md) — control failed execution
  attempts and statement idempotence.
- [Speculative execution](speculative-execution.md) — start additional
  attempts for slow idempotent requests.

```{toctree}
:maxdepth: 2
:hidden:

quick-start
connecting/index
statements/index
data-types
load-balancing
retry-policy
speculative-execution
```
