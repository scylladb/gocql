# Routing

Set `Profile.Routing` to a typed `RoutingPolicy`; update a ProfileHandle with its
validated Replace method. `NewRoutingPolicy(RoutingOptions{...})` configures
token awareness, datacenter/rack preference, remote fallback, replica shuffling,
leader-aware routing and latency ordering. Missing required locality is an
explicit validation error. `SingleTargetRouting` pins a typed host/shard target.

A custom RoutingPolicy implements Validate, Plan and Observe against immutable
MetadataSnapshot and RoutingInfo values. QueryPlan.Next lazily returns Target
values; it must not perform I/O or mutate inputs. Built-in routing keeps private
immutable indices and does not copy all hosts to choose its first target.

Config.Partitioners maps server names to TokenPartitioner implementations.
PartitionKey.ForEach streams canonical encoded components without gathering;
consume borrowed spans synchronously. Unknown partitioners disable token routing
with a diagnostic. Tablet placement and opaque version identities survive in
immutable snapshots; retained snapshots remain valid after refresh.

The optional hostpolicy subpackage adapts go-hostpool to the typed RoutingPolicy
and attempt observation seams.
