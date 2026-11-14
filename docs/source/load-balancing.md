# Load balancing

Use a token-aware policy so requests go to replicas that own the queried
partition. `NewCluster` defaults to `LOCAL_QUORUM` and `LOCAL_SERIAL`, but its
default host selection policy is not datacenter-aware. These local consistency
levels apply in the coordinator's datacenter.

In a multi-datacenter cluster, set `localDC` to the same datacenter for all
clients whose reads and writes must share local quorums, or whose LWTs must
share local serial ordering. Wrap a datacenter-aware fallback policy and disable
cross-datacenter failover to keep coordinators in that datacenter:

```go
cluster := gocql.NewCluster(hosts...)

fallback := gocql.RoundRobinHostPolicy()
if localDC != "" {
	fallback = gocql.DCAwareRoundRobinPolicy(localDC, gocql.HostPolicyOptionDisableDCFailover)
}

cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
```

Without `HostPolicyOptionDisableDCFailover`, the datacenter-aware fallback can
select remote coordinators after exhausting local hosts. This changes which
datacenter the local consistency levels refer to. Disabling failover trades
cross-datacenter availability for keeping the local quorum and serial scope
fixed. Leave `NonLocalReplicasFallback` disabled as well when coordinators must
stay in that datacenter. An empty `localDC` in the example leaves routing
datacenter-unaware; use that only when a fixed datacenter is not required.

Applications needing cross-datacenter quorum or serial behavior can explicitly
choose `gocql.Quorum` and `gocql.Serial`; see
[Consistency defaults](statements/index.md) and
[Lightweight transactions](statements/lightweight-transactions.md).

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
