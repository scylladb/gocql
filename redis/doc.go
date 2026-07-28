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
// A key and everything in it live in one partition of one table, named after
// Options.Table. Three side tables answer questions that partition cannot:
//
//	<table>         (key, kind, sub)      the key's meta row and its elements
//	<table>_index   (key)                 enumeration for Keys and Scan
//	<table>_expiry  (slot, bucket, key)   keys with an expiry, for Sweep
//	<table>_wakeup  (slot, bucket, key)   blocking pop notifications, optional
//
// The meta row sorts first in a key's partition and carries the key's type, its
// string value, a version, its element count, the two list bounds and its
// logical expiry. It exists exactly while the key exists, which is what makes
// EXISTS, TYPE and WRONGTYPE single reads with nothing cached and nothing
// inferred. The remaining rows are the key's elements: hash fields, set members
// and list positions, distinguished by the kind column.
//
// Co-location is what buys atomicity. A command asserts the key's type,
// mutates its elements and updates its count in one conditional batch against
// one partition, so there is no ordering to get wrong and no window in which a
// key is half a hash and half a string. It is also what makes a whole
// collection, or a key and all its elements, one round trip.
//
// The two enumeration tables are deliberately not authoritative. An index entry
// is written before the key it names, so a failed write leaves an entry
// pointing at nothing rather than a key no listing can see; enumeration
// verifies every candidate and deletes the entries that no longer resolve. The
// expiry index is a superset in the same way: a missing entry costs space until
// something reads the key, never correctness.
//
// With PartitionByBucket the primary key gains a leading bucket column, which
// routes a tenant's keys together while keeping one key per partition. With
// TransactionsByBucket the bucket becomes the whole partition key, so every key
// in a bucket shares one partition and a transaction can span them. Size a
// bucket by its total number of elements, not by its number of keys.
//
// # Atomicity
//
// Single key mutations are enforced by the database, not by client side
// read-modify-write. A key's version is the guard: a command reads it, computes
// the new state and applies it only while the row still carries that version.
// A concurrent writer to the same key makes the loser retry; unrelated keys
// never contend.
//
// Multi key atomicity is bounded by the partition, as it is in Redis Cluster.
// MSet uses a logged batch, which guarantees every mutation is applied but not
// that a reader sees them together. Multi, Watch and Exec give real
// transactions over the keys of one bucket when TransactionsByBucket is set:
// Watch pins the version of each key it names, and Exec applies the queued
// commands as one conditional batch whose conditions are those versions. If any
// of them moved, nothing is applied and Exec returns ErrTxAborted, which is
// EXEC returning nil after an invalidated WATCH.
//
// Conditional writes are lightweight transactions. They are correct but
// noticeably more expensive than plain writes, and they serialize per
// partition. Size expectations accordingly.
//
// # Expiry
//
// Expiry is a column on the meta row rather than only a cell TTL. That is what
// makes it conditionable and what lets it apply to hashes, sets and lists as
// well as strings, so Expire, TTL and Persist are O(1) for a collection instead
// of a rewrite of every element.
//
// The cost is that something has to reclaim the rows. Three things do: a read
// of an expired key deletes it in passing, a string also carries a cell TTL so
// the server reclaims it unprompted, and Sweep walks the expiry index for the
// keys nobody read again. Reads filter on the expiry column regardless, so a
// late sweep costs space and never correctness. Sweep is exported rather than
// automatic: a driver that starts its own background goroutines is a surprise.
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
//	MSet                        no*     no        *logged batch: applied, not isolated
//	Del, Exists                 yes     n/a       per key, elements go with the key
//	Expire, TTL, Persist        yes     n/a       every type; expiration <= 0 deletes
//	Rename                      no*     yes       *atomic with TransactionsByBucket
//	Copy                        no*     yes       *conditional without replace
//	Type                        yes     n/a       from the meta row
//	HSet, HDel                  yes     yes       exact created/removed counts
//	HGet, HExists, HLen         yes     n/a       HLen reads the recorded count
//	HGetAll                     yes     n/a       whole hash in one read
//	SAdd, SRem                  yes     yes       exact added/removed counts
//	SCard, SIsMember, SMembers  yes     n/a       SCard reads the recorded count
//	LPush, RPush, LPop, RPop    yes     yes       one batch per command
//	LLen, LRange                yes     n/a       LLen reads the recorded count
//	BLPop, BRPop                yes     n/a       wakeups plus a poll, see below
//	Multi, Watch, Exec          yes     yes       needs TransactionsByBucket
//	Keys                        no      n/a       full scan of the index
//	Scan                        no      n/a       server paging state cursor
//	Sort                        no      n/a       BY nosort only, no GET
//	Sweep                       n/a     n/a       reclaims expired keys
//
// # Limits
//
// Every command that could otherwise materialize an unbounded result, write an
// unbounded cell or build an unbounded batch has a ceiling, so one call cannot
// destabilize a coordinator or this process. Each is an Options field and each
// has a distinct error:
//
//	MaxValueSize        16MiB   per stored value          ErrValueTooLarge
//	MaxCollectionScan   100000  elements per read         ErrResultTooLarge
//	MaxKeysScan         —       keys per Keys call        ErrResultTooLarge
//	MaxBatchStatements  200     statements per command    ErrBatchTooLarge
//	MaxScanPageSize     10000   ceiling on Scan COUNT     silently clamped
//	MaxScanCursors      1024    live Scan cursors         oldest expire first
//
// A command that would exceed MaxBatchStatements is refused rather than split
// across batches, so a command that reported success was applied atomically.
//
// Keys are stored as text, so a key must be valid UTF-8 and at most 65535
// bytes. Hash fields, set members and values are binary safe.
//
// Username and Password are refused without TLSConfig unless
// AllowPlaintextCredentials is set, because credentials travel on the first
// frame of a connection.
//
// # Known differences from Redis
//
//   - Multi key atomicity needs co-location. Without TransactionsByBucket there
//     is no equivalent of MULTI/EXEC, and MSet is applied atomically but read
//     without isolation. This is the guarantee Redis Cluster offers, where a
//     multi-key command whose keys do not share a slot is refused; the bucket is
//     that slot.
//   - Watch pins a version when it is called, so the Redis order matters: watch
//     first, then read, then queue, then Exec. A value read before its key was
//     watched is not covered by anything. A queued write to a key nobody
//     watched is still guarded by the state Exec read, so a writer that lands
//     inside that one round trip aborts the transaction; watch the keys a
//     transaction writes if losing that race is not acceptable.
//   - Rename and Copy support string keys, and Rename is atomic only when both
//     keys share a partition. Otherwise it writes the destination and then
//     removes the source conditionally on what it read: a concurrent write to
//     the source is never discarded, and a conflict that outlasts CASMaxRetries
//     fails with ErrCASExhausted, rolling back its own write when the
//     destination did not exist beforehand.
//   - A contended single key mutation gives up after CASMaxRetries and returns
//     ErrCASExhausted. This is deliberate for pops: reporting an empty list
//     would tell a consumer to stop draining a queue that still holds work.
//     Retry, or treat it as "try again" the way a blocking pop does.
//   - BLPop and BRPop wake on a notification but poll underneath, because CQL
//     has no blocking primitive. A producer in the same process wakes a waiter
//     with no query at all; with EnableWakeupChannel a producer in another
//     process wakes it through one shared partition that a client with waiters
//     tails at a cost independent of how many keys or waiters there are.
//     Notifications are best effort, so a lost one delays a delivery and never
//     drops one; the poll is the correctness floor.
//   - Expiry is logical, so an expired key that nothing reads keeps its rows
//     until Sweep runs. It reads as absent throughout.
//   - There is no HSCAN or SSCAN. HGetAll, SMembers, LRange and Sort
//     materialize a whole collection under MaxCollectionScan, so a large
//     collection is all-or-error rather than paged.
//   - Scan cursors are process local: they are handles for server paging state,
//     so they do not survive a restart and cannot be passed to another
//     instance. An unknown cursor returns ErrCursorUnknown. A cursor is bound
//     to the bucket and the pattern that created it.
//   - Keys is a full scan of the enumeration index. Treat it as a maintenance
//     tool.
//   - A list is limited to 2^63 pushes on the same side over its lifetime,
//     after which it reports ErrListPositionExhausted and must be recreated.
//   - Sorted sets, pub/sub, streams and scripting are absent. Scripting in
//     particular has no equivalent: there is no way to run a caller's code next
//     to the data.
//
// # Operational notes
//
// Sessions created by this package use the token aware host policy, so each
// request is routed to a replica that owns the token. Bulk commands fan out
// bounded concurrent requests rather than issuing multi partition IN queries,
// which keeps coordinator load spread across the cluster.
//
// A conditional write is Paxos on one partition, and a key is one partition, so
// a single hot key serializes. Under tablets, where each table has its own
// mapping, co-location also means a key's elements no longer spread across
// shards the way separate tables let them: the ceiling for one key is one
// shard. Spread load across keys, not within one.
//
// A read that follows a conditional write at plain consistency may still return
// the prior value; read at serial consistency when a command's own write has to
// be visible immediately. Conditional writes are unaffected, since their
// condition is evaluated inside Paxos.
//
// Schema creation happens once, on the first command. A failure is remembered
// only for InitRetryCooldown, so a transient error does not make the client
// permanently unusable and concurrent requests do not pile DDL onto a cluster
// that just failed some. It runs on a context detached from the caller so one
// cancelled request cannot abort setup for everyone else. Set
// DisableAutoCreateTable to manage the schema yourself.
package redis
