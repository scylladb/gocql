// Package redis provides a Redis-compatible API surface backed by ScyllaDB.
//
// It mirrors the go-redis call pattern so applications can migrate common
// key/value paths with minimal code changes. It is not a Redis server and it
// is not a drop-in replacement for one: Redis executes each command on a
// single thread, while this package maps commands onto a distributed store.
// Where a command cannot be made faithful, the difference is documented below
// rather than hidden.
//
// # Storage layout
//
// Four tables are used, named after Options.Table:
//
//	<table>       key -> type, value      strings and the key type registry
//	<table>_hash  (key, field) -> value   hashes
//	<table>_set   (key, member)           sets
//	<table>_list  (key, pos) -> value     lists
//
// The kv table doubles as the key namespace. String keys store their value
// there; hash, set and list keys store a type marker row. That is what allows
// Del, Exists, Keys and Scan to see keys of every type, and what makes
// WRONGTYPE detection possible. Set DisableKeyTypeRegistry to opt out, at the
// cost of both.
//
// With PartitionByBucket the primary keys gain a leading bucket column, so a
// bucket is one partition. Size buckets accordingly: a bucket that grows
// without bound becomes a hot partition.
//
// # Atomicity
//
// Single key mutations are enforced by the database, not by client side
// read-modify-write. SetNX is a conditional insert; Incr, Decr, Append, GetSet
// and GetDel run a compare-and-set loop; Del, HDel, SRem and list pops use
// conditional deletes so their counts stay correct under concurrency; HSet and
// SAdd use conditional inserts so "how many were new" is accurate.
//
// Conditional writes are lightweight transactions. They are correct but
// noticeably more expensive than plain writes, and they serialize per
// partition. Size expectations accordingly.
//
// # Command compatibility
//
// Atomic means the command is indivisible with respect to concurrent writers.
// TTL means an existing expiry survives the command.
//
//	Command                     Atomic  TTL kept  Notes
//	Set                         yes     KeepTTL   replaces a key of any type
//	SetEx                       yes     n/a       rejects expiration <= 0
//	SetNX                       yes     n/a       single conditional insert
//	Get, StrLen                 yes     yes       WRONGTYPE on non-string keys
//	GetSet                      yes     no        clears TTL, as Redis does
//	GetDel                      yes     n/a       conditional delete
//	Incr, IncrBy, Decr, DecrBy  yes     yes       errors on int64 overflow
//	Append                      yes     yes
//	MGet                        no      n/a       per key reads, nil on wrong type
//	MSet                        no*     no        *atomic with AtomicMSetByBucket
//	Del, Exists                 yes     n/a       per key, cascades to collections
//	Expire                      yes     n/a       expiration <= 0 deletes the key
//	TTL                         yes     n/a       -2 missing, -1 no expiry
//	Persist                     yes     n/a
//	Rename                      no      yes       string keys only
//	Copy                        no*     yes       *conditional without replace
//	HSet, HDel, HGet, HGetAll   per op  n/a       no TTL support for hashes
//	SAdd, SRem, SCard, ...      per op  n/a       SCard counts server side
//	LPush, RPush, LPop, RPop    per op  n/a       positions claimed with LWT
//	BLPop, BRPop                per op  n/a       polling, see below
//	Keys                        no      n/a       full scan, all key types
//	Scan                        no      n/a       server paging state cursor
//	Sort                        no      n/a       BY nosort only, no GET
//
// # Known differences from Redis
//
//   - Multi key commands are not atomic as a group. MSet is atomic only with
//     AtomicMSetByBucket, and only against other MSet calls in the same bucket:
//     Set, Del and Incr do not take the bucket guard.
//   - Expire, TTL and Persist apply to string keys. Hashes, sets and lists
//     report no expiry and reject Expire with ErrKeyTypeUnsupported.
//   - Rename and Copy support string keys.
//   - BLPop and BRPop poll with exponential backoff and jitter, because CQL has
//     no blocking primitive. A pop is atomic, so an element is delivered once,
//     but latency is bounded by the poll interval rather than by the write.
//   - Scan cursors are process local: they are handles for server paging state,
//     so they do not survive a restart and cannot be passed to another
//     instance. An unknown cursor returns ErrCursorUnknown.
//   - Keys is a full scan. Treat it as a maintenance tool.
//   - Key type enforcement uses a bounded local cache, so a type change made by
//     another process may not be noticed until the cache entry is evicted.
//
// # Operational notes
//
// Sessions created by this package use the token aware host policy, so each
// request is routed to a replica that owns the token. Bulk commands fan out
// bounded concurrent requests rather than issuing multi partition IN queries,
// which keeps coordinator load spread across the cluster.
//
// Schema creation happens once, on the first command, and is not latched on
// failure: a transient error is retried by the next call. It runs on a context
// detached from the caller so one cancelled request cannot leave the client
// permanently unusable. Set DisableAutoCreateTable to manage the schema
// yourself.
package redis
