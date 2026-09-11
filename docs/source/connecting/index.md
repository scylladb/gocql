# Connecting to the cluster

Create one `Session` during application startup and share it across goroutines.
A session discovers cluster topology, maintains connection pools, and routes
concurrent requests. Creating a session per request wastes connections and
discovery work.

Supply several reachable contact points when possible:

```go
cluster := gocql.NewCluster(
	"192.0.2.10:9042",
	"192.0.2.11:9042",
	"192.0.2.12:9042",
)
cluster.Keyspace = "application"

session, err := cluster.CreateSession()
if err != nil {
	return err
}
defer session.Close()
```

Contact points bootstrap discovery; they are not the complete list of hosts
used by the session. Prefer node broadcast addresses or IP addresses. DNS names
that resolve to multiple addresses can make topology events difficult to match
to discovered hosts.

## Shard-aware port

ScyllaDB's shard-aware native transport port lets the driver establish the
correct per-shard connections with fewer attempts. The built-in host dialer
uses it automatically when advertised by the server.

If a custom `net.Dialer` is required, wrap it so the driver-selected source
port is honored:

```go
dialer := net.Dialer{
	Timeout: 5 * time.Second,
}
cluster.Dialer = &gocql.ScyllaShardAwareDialer{Dialer: dialer}
```

Custom `gocql.Dialer` implementations can read the requested source port with
`gocql.ScyllaGetSourcePort(ctx)`. If the port is already bound, return
`gocql.ErrScyllaSourcePortAlreadyInUse` or `syscall.EADDRINUSE` so the driver
can try another suitable port.

A custom `ClusterConfig.HostDialer` replaces the built-in host-level dialing
and fallback logic. It must implement `gocql.ShardDialer` to receive
shard-targeted calls, and its `DialShard` method is responsible for any fallback
after a shard-targeted dial fails.

Shard-aware ports require nodes configured with
`native_shard_aware_transport_port` for plaintext connections or
`native_shard_aware_transport_port_ssl` for TLS, a network path that preserves
source ports, and custom `gocql.Dialer` implementations that bind the selected
source port. The driver falls back to the regular native transport port when
using its built-in host dialer. Set
`ClusterConfig.DisableShardAwarePort` to `true` only when the advertised port
is unreachable and the network cannot be fixed.

## Reconnection policies

Reconnection policies restore failed host connections; they do not retry
queries. Configure steady-state and initial connection behavior separately:

```go
cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
	MaxRetries:      10,
	InitialInterval: time.Second,
	MaxInterval:     30 * time.Second,
}
cluster.InitialReconnectionPolicy = &gocql.ConstantReconnectionPolicy{
	MaxRetries: 5,
	Interval:   2 * time.Second,
}
```

`NoReconnectionPolicy` performs only the initial connection attempt. A custom
policy must return a positive maximum attempt count.

```{toctree}
:maxdepth: 1
:hidden:

compression
authentication
tls
../client-routes
```
