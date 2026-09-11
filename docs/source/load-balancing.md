# Load balancing

Use a token-aware policy so requests go to replicas that own the queried
partition. In a multi-datacenter cluster, wrap a data-center-aware fallback
policy and use local consistency:

```go
cluster := gocql.NewCluster(hosts...)

fallback := gocql.RoundRobinHostPolicy()
if localDC != "" {
	fallback = gocql.DCAwareRoundRobinPolicy(localDC)
	cluster.Consistency = gocql.LocalQuorum
}

cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
```

Token-aware routing needs metadata for every partition-key component. Bind all
partition-key values instead of embedding them in the CQL string:

```go
// Routable: both partition-key components are bound values.
session.Query(
	"SELECT value FROM events WHERE account_id = ? AND bucket = ?",
	accountID,
	bucket,
)
```

`DCAwareRoundRobinPolicy` prioritizes the configured local data center.
`RackAwareRoundRobinPolicy` adds a local-rack preference inside that data
center. Both can be used as the fallback for `TokenAwareHostPolicy`.

When ScyllaDB advertises a supported, nonzero shard configuration, the driver
routes connections to shards and opens one connection per shard;
`ClusterConfig.NumConns` has no effect. If server-side shard-aware drivers are
disabled or valid sharding metadata is unavailable, the driver uses its normal
connection pool and honors `NumConns`.

See [Connecting to the cluster](connecting/index.md) for shard-aware port and
network requirements.
