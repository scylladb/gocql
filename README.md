<div align="center">

![Build Passing](https://github.com/scylladb/gocql/workflows/Build/badge.svg)
[![Read the Fork Driver Docs](https://img.shields.io/badge/Read_the_Docs-pkg_go-blue)](https://pkg.go.dev/github.com/scylladb/gocql#section-documentation)
[![Protocol Specs](https://img.shields.io/badge/Protocol_Specs-ScyllaDB_Docs-blue)](https://github.com/scylladb/scylladb/blob/master/docs/dev/protocol-extensions.md)

</div>

<h1 align="center">

Scylla Shard-Aware Fork of [apache/cassandra-gocql-driver](https://github.com/apache/cassandra-gocql-driver)

</h1>


<img src="./.github/assets/logo.svg" width="200" align="left" />

This is a fork of [apache/cassandra-gocql-driver](https://github.com/apache/cassandra-gocql-driver) package that we created at Scylla.
It contains extensions to tokenAwareHostPolicy supported by the Scylla 2.3 and onwards.
It allows driver to select a connection to a particular shard on a host based on the token.
This eliminates passing data between shards and significantly reduces latency.

There are open pull requests to merge the functionality to the upstream project:

* [gocql/gocql#1210](https://github.com/gocql/gocql/pull/1210)
* [gocql/gocql#1211](https://github.com/gocql/gocql/pull/1211).

It also provides support for shard aware ports, a faster way to connect to all shards, details available in [blogpost](https://www.scylladb.com/2021/04/27/connect-faster-to-scylla-with-a-shard-aware-port/).

---

### Table of Contents

- [1. Sunsetting Model](#1-sunsetting-model)
- [2. Installation](#2-installation)
- [3. Quick Start](#3-quick-start)
- [4. Data Types](#4-data-types)
- [5. Configuration](#5-configuration)
  - [5.1 Shard-aware port](#51-shard-aware-port)
  - [5.2 Client routes (PrivateLink)](#52-client-routes-privatelink)
  - [5.3 Iterator](#53-iterator)
- [6. Redis Compatibility Mode](#6-redis-compatibility-mode)
  - [6.1 Basic Usage (Set/Get/MGet/MSet/Del/Exists)](#61-basic-usage-setgetmgetmsetdelexists)
  - [6.1.1 Migration from go-redis](#611-migration-from-go-redis)
  - [6.2 SetNX / SetEx / GetSet / GetDel](#62-setnx--setex--getset--getdel)
  - [6.3 Counters (Incr/Decr)](#63-counters-incrdecr)
  - [6.4 Key Lifecycle (Expire/TTL/Persist)](#64-key-lifecycle-expireTTLpersist)
  - [6.5 Hash Commands (HSet/HGet/HDel/HExists/HGetAll)](#65-hash-commands-hsethgethdelhexistshgetall)
  - [6.6 String Helpers (Append/StrLen)](#66-string-helpers-appendstrlen)
  - [6.7 Rename / Copy](#67-rename--copy)
  - [6.8 Keys (full-scan)](#68-keys-full-scan)
  - [6.9 Set Commands (SAdd/SRem/SMembers/SIsMember/SCard)](#69-set-commands-saddsremsmemberssismemberscard)
  - [6.10 Blocking List Ops (BLPop/BRPop)](#610-blocking-list-ops-blpopbrpop)
  - [6.11 Scan (cursor iteration)](#611-scan-cursor-iteration)
  - [6.12 Sort](#612-sort)
  - [6.13 Transactions (Multi/Watch/Exec)](#613-transactions-multiwatchexec)
  - [6.14 Bucketed Clients](#614-bucketed-clients)
  - [6.15 Reclaiming Expired Keys (Sweep)](#615-reclaiming-expired-keys-sweep)
- [7. Contributing](#7-contributing)

## 1. Sunsetting Model

> [!WARNING]
> In general, the gocql team will focus on supporting the current and previous versions of Go. gocql may still work with older versions of Go, but official support for these versions will have been sunset.

## 2. Installation

This is a drop-in replacement to gocql, it reuses the `github.com/gocql/gocql` import path.

Add the following line to your project `go.mod` file.

```mod
replace github.com/gocql/gocql => github.com/scylladb/gocql latest
```

and run

```sh
go mod tidy
```

to evaluate `latest` to a concrete tag.

Your project now uses the Scylla driver fork, make sure you are using the `TokenAwareHostPolicy` to enable the shard-awareness, continue reading for details.

## 3. Quick Start

Spawn a ScyllaDB Instance using Docker Run command:

```sh
docker run --name node1 --network your-network -p "9042:9042" -d scylladb/scylla:6.1.2 \
	--overprovisioned 1 \
	--smp 1
```

Then, create a new connection using ScyllaDB GoCQL following the example below:

```go
package main

import (
    "fmt"
    "github.com/gocql/gocql"
)

func main() {
    var cluster = gocql.NewCluster("localhost:9042")

    var session, err = cluster.CreateSession()
    if err != nil {
        panic("Failed to connect to cluster")
    }

    defer session.Close()

    var query = session.Query("SELECT * FROM system.clients")

    if rows, err := query.Iter().SliceMap(); err == nil {
        for _, row := range rows {
            fmt.Printf("%v\n", row)
        }
    } else {
        panic("Query error: " + err.Error())
    }
}
```

`SliceMap()` consumes and closes the iterator before it returns.

## 4. Data Types

Here's an list of all CQL Types reflected in the GoCQL environment:

| ScyllaDB Type    | Go Type            |
| ---------------- | ------------------ |
| `ascii`          | `string`           |
| `bigint`         | `int64`            |
| `blob`           | `[]byte`           |
| `boolean`        | `bool`             |
| `date`           | `time.Time`        |
| `decimal`        | `inf.Dec`          |
| `double`         | `float64`          |
| `duration`       | `gocql.Duration`   |
| `float`          | `float32`          |
| `uuid`           | `gocql.UUID`       |
| `int`            | `int32`            |
| `inet`           | `string`           |
| `list<int>`      | `[]int32`          |
| `map<int, text>` | `map[int32]string` |
| `set<int>`       | `[]int32`          |
| `smallint`       | `int16`            |
| `text`           | `string`           |
| `time`           | `time.Duration`    |
| `timestamp`      | `time.Time`        |
| `timeuuid`       | `gocql.UUID`       |
| `tinyint`        | `int8`             |
| `varchar`        | `string`           |
| `varint`         | `int64`            |

## 5. Configuration

In order to make shard-awareness work, token aware host selection policy has to be enabled.
Please make sure that the gocql configuration has `PoolConfig.HostSelectionPolicy` properly set like in the example below.

__When working with a Scylla cluster, `PoolConfig.NumConns` option has no effect - the driver opens one connection for each shard and completely ignores this option.__

```go
c := gocql.NewCluster(hosts...)

// Enable token aware host selection policy, if using multi-dc cluster set a local DC.
fallback := gocql.RoundRobinHostPolicy()
if localDC != "" {
	fallback = gocql.DCAwareRoundRobinPolicy(localDC)
}
c.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

// If using multi-dc cluster use the "local" consistency levels.
if localDC != "" {
	c.Consistency = gocql.LocalQuorum
}

// When working with a Scylla cluster the driver always opens one connection per shard, so `NumConns` is ignored.
// c.NumConns = 4
```

### 5.1 Shard-aware port

This version of gocql supports a more robust method of establishing connection for each shard by using _shard aware port_ for native transport.
It greatly reduces time and the number of connections needed to establish a connection per shard in some cases - ex. when many clients connect at once, or when there are non-shard-aware clients connected to the same cluster.

If you are using a custom Dialer and if your nodes expose the shard-aware port, it is highly recommended to update it so that it uses a specific source port when connecting.

* If you are using a custom `net.Dialer`, you can make your dialer honor the source port by wrapping it in a `gocql.ScyllaShardAwareDialer`:

  ```go
  oldDialer := net.Dialer{...}
  clusterConfig.Dialer := &gocql.ScyllaShardAwareDialer{oldDialer}
  ```

* If you are using a custom type implementing `gocql.Dialer`, you can get the source port by using the `gocql.ScyllaGetSourcePort` function.
  An example:

  ```go
  func (d *myDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
      sourcePort := gocql.ScyllaGetSourcePort(ctx)
      localAddr, err := net.ResolveTCPAddr(network, fmt.Sprintf(":%d", sourcePort))
      if err != nil {
          return nil, err
      }
	  d := &net.Dialer{LocalAddr: localAddr}
	  return d.DialContext(ctx, network, addr)
  }
  ```

  The source port might be already bound by another connection on your system.
  In such case, you should return an appropriate error so that the driver can retry with a different port suitable for the shard it tries to connect to.

  * If you are using `net.Dialer.DialContext`, this function will return an error in case the source port is unavailable, and you can just return that error from your custom `Dialer`.
  * Otherwise, if you detect that the source port is unavailable, you can either return `gocql.ErrScyllaSourcePortAlreadyInUse` or `syscall.EADDRINUSE`.

For this feature to work correctly, you need to make sure the following conditions are met:

* Your cluster nodes are configured to listen on the shard-aware port (`native_shard_aware_transport_port` option),
* Your cluster nodes are not behind a NAT which changes source ports,
* If you have a custom Dialer, it connects from the correct source port (see the guide above).

The feature is designed to gracefully fall back to the using the non-shard-aware port when it detects that some of the above conditions are not met.
The driver will print a warning about misconfigured address translation if it detects it.
Issues with shard-aware port not being reachable are not reported in non-debug mode, because there is no way to detect it without false positives.

If you suspect that this feature is causing you problems, you can completely disable it by setting the `ClusterConfig.DisableShardAwarePort` flag to true.

### 5.2 Client routes (PrivateLink)

Scylla Cloud exposes a `system.client_routes` table that maps hosts to PrivateLink endpoints.
When configured, the driver can resolve and connect to the per-host PrivateLink address instead of using the public host IP.

Use `WithClientRoutes` to enable it and pass the connection IDs you receive from Scylla Cloud:

```go
cluster := gocql.NewCluster("private-link.dns.name")
cluster.WithOptions(
	gocql.WithClientRoutes(
		gocql.WithEndpoints(
			gocql.ClientRoutesEndpoint{ConnectionID: "your-connection-id"},
		),
	),
)
```

If you also want to seed the cluster with PrivateLink hostnames, provide `ConnectionAddr` values in the endpoints list.

### 5.3 Iterator

Paging is a way to parse large result sets in smaller chunks.
The driver provides an iterator to simplify this process.

Use `Query.Iter()` to obtain iterator:

```go
iter := session.Query("SELECT id, value FROM my_table WHERE id > 100 AND id < 10000").Iter()
var results []int

var id, value int
for !iter.Scan(&id, &value) {
	if id%2 == 0 {
		results = append(results, value)
	}
}

if err := iter.Close(); err != nil {
    // handle error
}
```

In case of range and `ALLOW FILTERING` queries server can send empty responses for some pages.
That is why you should never consider empty response as the end of the result set.
Always check `iter.Scan()` result to know if there are more results, or `Iter.LastPage()` to know if the last page was reached.

### 5.3 Compression

To control network costs and traffic, you can enable compression.

Use `ClusterConfig.Compressor` to enable compression (either Snappy or LZ4):

```go
...
import (
    ...
    "github.com/gocql/gocql"
    "github.com/gocql/gocql/lz4"
    ...
)

config := gocql.NewCluster("10.0.12.83", "10.0.13.04", "10.0.14.12")
config.Compressor = &gocql.SnappyCompressor{}
//or LZ4
config.Compressor = &lz4.LZ4Compressor{}
...
```

## 6. Redis Compatibility Mode

This repository now includes a Redis-compatible API surface in `github.com/gocql/gocql/redis`.

The goal is migration-friendly usage for common go-redis key/value paths. It is
**not** a Redis server and not a drop-in replacement for one. A key and all of
its elements live in one partition, so every single-key command is atomic and
enforced by the database; multi-key atomicity needs the keys to share a
partition, and blocking list ops and `SCAN` behave differently from Redis. Read
[6.0 Compatibility and guarantees](#60-compatibility-and-guarantees) before
migrating anything beyond plain `Set`/`Get`.

**String / KV commands:**

| Method | Signature |
|---|---|
| `Set` | `Set(ctx, key, value, expiration) *StatusCmd` |
| `SetEx` | `SetEx(ctx, key, value, expiration) *StatusCmd` |
| `SetNX` | `SetNX(ctx, key, value, expiration) *BoolCmd` |
| `Get` | `Get(ctx, key) *StringCmd` |
| `GetSet` | `GetSet(ctx, key, value) *StringCmd` |
| `GetDel` | `GetDel(ctx, key) *StringCmd` |
| `MSet` | `MSet(ctx, values...) *StatusCmd` |
| `MGet` | `MGet(ctx, keys...) *SliceCmd` |
| `Del` | `Del(ctx, keys...) *IntCmd` |
| `Exists` | `Exists(ctx, keys...) *IntCmd` |
| `Incr` | `Incr(ctx, key) *IntCmd` |
| `IncrBy` | `IncrBy(ctx, key, value) *IntCmd` |
| `Decr` | `Decr(ctx, key) *IntCmd` |
| `DecrBy` | `DecrBy(ctx, key, decrement) *IntCmd` |
| `Append` | `Append(ctx, key, value) *IntCmd` |
| `StrLen` | `StrLen(ctx, key) *IntCmd` |
| `Rename` | `Rename(ctx, key, newkey) *StatusCmd` |
| `Copy` | `Copy(ctx, source, destination, db, replace) *IntCmd` |
| `Keys` | `Keys(ctx, pattern) *StringSliceCmd` |
| `Scan` | `Scan(ctx, cursor, match, count) *ScanCmd` |
| `Sort` | `Sort(ctx, key, opt) *StringSliceCmd` |
| `Type` | `Type(ctx, key) *StatusCmd` |

**Key lifecycle commands:**

| Method | Signature |
|---|---|
| `Expire` | `Expire(ctx, key, expiration) *BoolCmd` |
| `TTL` | `TTL(ctx, key) *DurationCmd` |
| `Persist` | `Persist(ctx, key) *BoolCmd` |
| `Sweep` | `Sweep(ctx) *IntCmd` |

**Transaction commands** (require `TransactionsByBucket`, see [6.13](#613-transactions-multiwatchexec)):

| Method | Signature |
|---|---|
| `Multi` | `Multi() *Tx` |
| `Watch` | `Watch(ctx, keys...) *Tx` |
| `Tx.Watch` | `Watch(ctx, keys...) *Tx` |
| `Tx.Set` | `Set(key, value) *Tx` |
| `Tx.SetEx` | `SetEx(key, value, expiration) *Tx` |
| `Tx.IncrBy` | `IncrBy(key, delta) *Tx` |
| `Tx.Expire` | `Expire(key, expiration) *Tx` |
| `Tx.Del` | `Del(keys...) *Tx` |
| `Tx.Exec` | `Exec(ctx) error` |

**Hash commands:**

| Method | Signature |
|---|---|
| `HSet` | `HSet(ctx, key, values...) *IntCmd` |
| `HGet` | `HGet(ctx, key, field) *StringCmd` |
| `HExists` | `HExists(ctx, key, field) *BoolCmd` |
| `HDel` | `HDel(ctx, key, fields...) *IntCmd` |
| `HLen` | `HLen(ctx, key) *IntCmd` |
| `HGetAll` | `HGetAll(ctx, key) *MapStringStringCmd` |

**Set commands:**

| Method | Signature |
|---|---|
| `SAdd` | `SAdd(ctx, key, members...) *IntCmd` |
| `SRem` | `SRem(ctx, key, members...) *IntCmd` |
| `SMembers` | `SMembers(ctx, key) *StringSliceCmd` |
| `SIsMember` | `SIsMember(ctx, key, member) *BoolCmd` |
| `SCard` | `SCard(ctx, key) *IntCmd` |

**List commands:**

| Method | Signature |
|---|---|
| `LPush` | `LPush(ctx, key, values...) *IntCmd` |
| `RPush` | `RPush(ctx, key, values...) *IntCmd` |
| `LPop` | `LPop(ctx, key) *StringCmd` |
| `RPop` | `RPop(ctx, key) *StringCmd` |
| `LLen` | `LLen(ctx, key) *IntCmd` |
| `LRange` | `LRange(ctx, key, start, stop) *StringSliceCmd` |
| `BLPop` | `BLPop(ctx, timeout, keys...) *StringSliceCmd` |
| `BRPop` | `BRPop(ctx, timeout, keys...) *StringSliceCmd` |

**Command-style return types:**

- `StatusCmd` (`Val()`, `Result()`, `Err()`)
- `StringCmd` (`Val()`, `Result()`, `Err()`, `Bytes()`, `Int()`, `Int64()`)
- `IntCmd` (`Val()`, `Result()`, `Err()`)
- `SliceCmd` (`Val()`, `Result()`, `Err()`)
- `BoolCmd` (`Val()`, `Result()`, `Err()`)
- `DurationCmd` (`Val()`, `Result()`, `Err()`)
- `MapStringStringCmd` (`Val()`, `Result()`, `Err()`)
- `StringSliceCmd` (`Val()`, `Result()`, `Err()`)
- `ScanCmd` (`Val()`, `Cursor()`, `Result()`, `Err()`)

`Get` and `HGet` return `redis.Nil` when key/field is missing.

### 6.0 Compatibility and guarantees

**Atomic** means the command is indivisible with respect to concurrent writers.
**TTL kept** means an existing expiry survives the command.

| Command | Atomic | TTL kept | Notes |
|---|---|---|---|
| `Set` | yes | with `KeepTTL` | replaces a key of any type, elements included |
| `SetEx` | yes | n/a | rejects `expiration <= 0` |
| `SetNX` | yes | n/a | one conditional insert; safe for locking |
| `Get`, `StrLen` | yes | yes | `WRONGTYPE` on non-string keys |
| `GetSet` | yes | no | clears TTL, as Redis does |
| `GetDel` | yes | n/a | conditional delete; delivered once |
| `Incr`, `IncrBy`, `Decr`, `DecrBy` | yes | yes | errors on `int64` overflow |
| `Append` | yes | yes | |
| `MGet` | no | n/a | per-key reads; `nil` for wrong-type keys |
| `MSet` | isolated only within one bucket (`TransactionsByBucket`) | no | logged batch across partitions; a single-partition mutation within a bucket |
| `Del`, `Exists` | yes | n/a | one batch per key; elements go with the key |
| `Expire`, `Persist` | yes | n/a | every key type; `expiration <= 0` deletes (Redis 7) |
| `TTL` | yes | n/a | `-2` missing, `-1` no expiry |
| `Type` | yes | n/a | from the key's meta row |
| `Rename` | with `TransactionsByBucket` | yes | string keys; otherwise rolls back its own write |
| `Copy` | conditional without `replace` | yes | missing source returns `0`, not an error |
| `HSet`, `HDel`, `SAdd`, `SRem` | yes | yes | one batch; counts exact under concurrency |
| `HLen`, `SCard`, `LLen` | yes | n/a | recorded count, one read of the meta row |
| `HGet`, `HExists`, `SIsMember` | yes | n/a | meta row and named elements in one read |
| `HGetAll`, `SMembers`, `LRange` | yes | n/a | whole collection in one read |
| `LPush`, `RPush`, `LPop`, `RPop` | yes | yes | one batch; an element is delivered once |
| `BLPop`, `BRPop` | yes | n/a | wakeups plus a poll, see 6.10 |
| `Multi`, `Watch`, `Exec` | yes | yes | one bucket partition, see 6.13 |
| `Keys` | no | n/a | full scan of the index; maintenance tool |
| `Scan` | no | n/a | server paging-state cursor |
| `Sort` | no | n/a | `BY nosort` only, no `GET` |
| `Sweep` | n/a | n/a | reclaims expired keys, see 6.15 |

**Known differences from Redis**

- Multi-key atomicity needs co-location. Across partitions, `MSet` is a logged
  batch: every write is guaranteed to apply, but a reader can see one key
  updated and another not. When every key shares one bucket partition
  (`TransactionsByBucket`), that same `MSet` call becomes a single-partition
  batch — one mutation, so readers see every key change together — with no
  need to wrap it in a transaction. `Multi`/`Watch`/`Exec` adds what a plain
  batch cannot: a condition (`Watch`) or a mix of different command kinds.
  This is the same trade Redis Cluster makes with hash tags.
- `Watch` pins a version when it is called, so the Redis order matters: watch,
  read, queue, `Exec`. A value read before its key was watched is not covered.
  A queued write to a key nobody watched is still guarded by what `Exec` read,
  so a writer landing inside that one round trip aborts the transaction.
- `Rename` and `Copy` support string keys. `Rename` is atomic when both keys
  share a partition (`TransactionsByBucket`); otherwise it writes the
  destination and then removes the source conditionally on the value it read, so
  a concurrent write to the source is never silently discarded. A conflict that
  outlasts `CASMaxRetries` fails with `ErrCASExhausted` and rolls back its own
  write when the destination did not exist beforehand.
- A contended single-key mutation gives up after `CASMaxRetries` and returns
  `ErrCASExhausted`. For a pop this is deliberate: reporting an empty list would
  tell a consumer to stop draining a queue that still holds work. Retry, or
  treat it as "try again" the way `BLPop` does.
- `BLPop`/`BRPop` wake on a notification but poll underneath, because CQL has no
  blocking primitive. Notifications are best effort, so a lost one costs latency
  and never an element.
- Expiry is a column on the key, not only a cell TTL. It applies to every type,
  but an expired key that nothing reads keeps its rows until `Sweep` runs. It
  reads as absent throughout.
- There is no `HSCAN`/`SSCAN`. `HGetAll`, `SMembers`, `LRange` and `Sort`
  materialize a whole collection under `MaxCollectionScan`, so a large
  collection is all-or-error rather than paged.
- `Scan` cursors are handles for server paging state held in this process. They
  do not survive a restart, cannot be shared, and are bound to the bucket and
  pattern that created them; an unknown cursor returns `ErrCursorUnknown`.
- Conditional writes are lightweight transactions: correct, but more expensive
  than plain writes and serialized per partition. A hot key is one partition.
- A list allows 2^63 pushes on the same side over its lifetime, then reports
  `ErrListPositionExhausted`.
- Sorted sets, pub/sub, streams and scripting are absent.

**Limits**

Every command that could otherwise materialize an unbounded result, write an
unbounded cell or build an unbounded batch has a ceiling. Each is an `Options`
field:

| Option | Default | Bounds | Error |
|---|---|---|---|
| `MaxValueSize` | 16MiB | one stored value | `ErrValueTooLarge` |
| `MaxCollectionScan` | 100000 | elements per collection read | `ErrResultTooLarge` |
| `MaxKeysScan` | inherits above | keys per `Keys` call | `ErrResultTooLarge` |
| `MaxBatchStatements` | 200 | statements one command may batch | `ErrBatchTooLarge` |
| `MaxScanPageSize` | 10000 | ceiling on `Scan` `COUNT` | clamped silently |
| `MaxScanCursors` | 1024 | live `Scan` cursors | oldest expire first |

A command that would exceed `MaxBatchStatements` is refused rather than split
across batches, so a command that reported success was applied atomically.

Keys are stored as `text`: a key must be valid UTF-8 and at most 65535 bytes.
Hash fields, set members and values are binary safe.

`Username`/`Password` are refused without `TLSConfig` unless
`AllowPlaintextCredentials` is set, because credentials travel on the first frame
of a connection.

**Schema**

One table holds a key and its elements; three side tables answer what a key
partition cannot. All four are named after `Options.Table` (default
`redis_compat`):

```cql
CREATE TABLE redis_compat (
  key        text,
  kind       tinyint,   -- 0 meta, 1 hash field, 2 set member, 3 list position
  sub        blob,      -- field name, member, or encoded position
  value      blob,
  type       text,      -- string | hash | set | list, on the meta row
  version    bigint,    -- guard for every conditional write
  size       bigint,    -- element count, so HLen/SCard/LLen are one read
  head       bigint,    -- list bounds
  tail       bigint,
  expires_at timestamp,
  PRIMARY KEY ((key), kind, sub)
);
CREATE TABLE redis_compat_index  (key text PRIMARY KEY);
CREATE TABLE redis_compat_expiry (slot timestamp, bucket text, key text, PRIMARY KEY ((slot), bucket, key));
CREATE TABLE redis_compat_wakeup (slot timestamp, bucket text, key text, PRIMARY KEY ((slot), bucket, key));
```

The meta row (`kind = 0`) sorts first in a key's partition and *is* the key: it
exists exactly while the key exists, so `EXISTS`, `TYPE` and `WRONGTYPE` are
single reads with nothing cached and nothing inferred. The rest of the partition
is the key's elements.

Co-location is what buys atomicity: a command asserts the key's type, mutates
its elements and updates `size` in one conditional batch against one partition.
There is no window in which a key is half a hash and half a string, and an
emptied collection stops existing in the same batch that removes its last
element.

`redis_compat_index` backs `Keys` and `Scan`, which would otherwise return one
row per element. It is deliberately a superset: entries are written before the
keys they name, and enumeration verifies each candidate and deletes the ones
that no longer resolve. `redis_compat_expiry` lets `Sweep` find expired keys
without scanning the namespace, and `redis_compat_wakeup` carries blocking-pop
notifications when `EnableWakeupChannel` is set.

`PartitionByBucket` adds a leading `bucket` column to the partition key
(`PRIMARY KEY ((bucket, key), kind, sub)`), which routes a tenant's keys together
while keeping one key per partition. `TransactionsByBucket` makes the bucket the
whole partition key (`PRIMARY KEY ((bucket), key, kind, sub)`), which is what
lets a transaction span keys — and what makes the bucket the unit of contention,
so size it by its total number of elements rather than its number of keys. The
index is bucketed with it; the expiry and wakeup tables carry the bucket as a
column either way.

### 6.1 Basic Usage (Set/Get/MGet/MSet/Del/Exists)

```go
package main

import (
	"context"
	"fmt"
	"time"

	redis "github.com/gocql/gocql/redis"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addrs:    []string{"127.0.0.1:9042"},
		Keyspace: "app",
		// Optional. Base name for the tables; default redis_compat
		Table: "redis_compat",
	})
	defer rdb.Close()

	_ = rdb.Set(ctx, "k1", "v1", 0).Err()
	_ = rdb.MSet(ctx, "k2", "v2", "k3", "v3").Err()

	val, err := rdb.Get(ctx, "k1").Result()
	if err == redis.Nil {
		fmt.Println("k1 missing")
		return
	}
	if err != nil {
		panic(err)
	}
	fmt.Println("k1 =", val)

	values, _ := rdb.MGet(ctx, "k1", "k2", "missing").Result()
	fmt.Printf("mget: %#v\n", values) // []interface{}{"v1", "v2", nil}

	nExists, _ := rdb.Exists(ctx, "k1", "missing").Result()
	fmt.Println("exists count:", nExists)

	nDel, _ := rdb.Del(ctx, "k1", "k2", "missing").Result()
	fmt.Println("deleted count:", nDel)

	// Keep existing TTL semantics on overwrite:
	_ = rdb.Set(ctx, "k3", "v3-updated", redis.KeepTTL).Err()
	_ = rdb.Set(ctx, "k4", "v4", 10*time.Second).Err()
}
```

#### 6.1.1 Migration from go-redis

For common KV paths, migration is mostly import/config replacement.

```go
// before:
// import redis "github.com/redis/go-redis/v9"
// rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// after:
import redis "github.com/gocql/gocql/redis"

rdb := redis.NewClient(&redis.Options{
	Addrs:    []string{"127.0.0.1:9042"},
	Keyspace: "app",
	Table:    "redis_compat",
})
defer rdb.Close()
```

Callsites remain the same style:

```go
_ = rdb.Set(ctx, "k", "v", 0).Err()
v, err := rdb.Get(ctx, "k").Result()
if err == redis.Nil {
	// missing key
}
_ = rdb.MSet(ctx, "k1", "v1", "k2", "v2").Err()
arr, _ := rdb.MGet(ctx, "k1", "k2").Result()
n1, _ := rdb.Exists(ctx, "k1", "kX").Result()
n2, _ := rdb.Del(ctx, "k1", "k2").Result()
_, _, _, _ = v, arr, n1, n2
```

### 6.2 SetNX / SetEx / GetSet / GetDel

```go
// SetNX: set only if key does not exist (returns true if set).
ok, _ := rdb.SetNX(ctx, "lock", "token-123", 10*time.Second).Result()

// SetEx: alias for Set with mandatory TTL.
_ = rdb.SetEx(ctx, "session", "data", 30*time.Minute).Err()

// GetSet: atomically set new value, return old value.
old, err := rdb.GetSet(ctx, "counter", "0").Result()
if err == redis.Nil {
	// key didn't exist before
}

// GetDel: get value and delete key in one call.
val, err := rdb.GetDel(ctx, "one-time-token").Result()
if err == redis.Nil {
	// key didn't exist
}
```

### 6.3 Counters (Incr/Decr)

Keys are stored as string-encoded integers. Missing keys start at 0.

```go
n, _ := rdb.Incr(ctx, "visits").Result()       // 1
n, _ = rdb.IncrBy(ctx, "visits", 10).Result()  // 11
n, _ = rdb.Decr(ctx, "visits").Result()         // 10
n, _ = rdb.DecrBy(ctx, "visits", 3).Result()    // 7
```

Notes:
- Counters use a compare-and-set loop, so concurrent increments do not lose
  updates. Under heavy contention on one key the call may return
  `ErrCASExhausted`; raise `CASMaxRetries` if needed.
- An existing TTL on the key is preserved.
- Exceeding `int64` returns `ErrIncrOverflow`; a non-numeric value returns
  `ErrValueNotInteger`.

### 6.4 Key Lifecycle (Expire/TTL/Persist)

```go
// Set a TTL on an existing key.
ok, _ := rdb.Expire(ctx, "session", 5*time.Minute).Result()

// Read remaining TTL.
// Returns -2s if key missing, -1s if key has no TTL.
ttl, _ := rdb.TTL(ctx, "session").Result()

// Remove TTL (make key permanent).
ok, _ = rdb.Persist(ctx, "session").Result()
```

Notes:
- Expiry is a column on the key's meta row, so `Expire` and `Persist` are one
  guarded update and cost the same for a hash, set or list as for a string —
  there is no rewrite of the elements.
- A string also gets a cell TTL, so the server reclaims it unprompted. A
  collection is reclaimed by the first read after it expires, or by
  [`Sweep`](#615-reclaiming-expired-keys-sweep). Reads filter on the expiry
  either way, so an unswept key still reads as absent.
- `Expire` with a non-positive duration deletes the key, matching Redis 7.

### 6.5 Hash Commands (HSet/HGet/HDel/HExists/HGetAll)

A hash lives in its key's partition, one row per field. `HSet` and `HDel` read
the meta row together with the named fields and apply the writes and the new
field count as one conditional batch, so the returned count is exact under
concurrency and a hash that loses its last field stops existing.

```go
// HSet: set one or more fields. Returns count of new fields created.
n, _ := rdb.HSet(ctx, "user:1", "name", "Alice", "age", "30").Result()

// HGet: get single field.
name, err := rdb.HGet(ctx, "user:1", "name").Result()
if err == redis.Nil {
	// field missing
}

// HExists: check if field exists.
ok, _ := rdb.HExists(ctx, "user:1", "email").Result()

// HDel: remove fields. Returns count of fields removed.
n, _ = rdb.HDel(ctx, "user:1", "age").Result()

// HGetAll: get all fields as map[string]string.
all, _ := rdb.HGetAll(ctx, "user:1").Result()
fmt.Println(all) // map[name:Alice]
```

### 6.6 String Helpers (Append/StrLen)

```go
// Append appends to the value stored at key. Creates the key if missing.
// Returns new string length.
n, _ := rdb.Append(ctx, "greeting", "hello").Result() // 5
n, _ = rdb.Append(ctx, "greeting", " world").Result() // 11

// StrLen returns byte-length of the value. 0 if key missing.
slen, _ := rdb.StrLen(ctx, "greeting").Result() // 11
```

### 6.7 Rename / Copy

```go
// Rename moves src key to dst. Errors if src does not exist.
// The remaining TTL is carried over to dst.
// Atomic with TransactionsByBucket, where both keys share a partition: one
// conditional batch writes dst and removes src.
// Otherwise it writes dst, then deletes src conditionally on the value it read,
// so a concurrent write to src is never silently discarded; if that conflict
// cannot be resolved within CASMaxRetries the command fails with
// ErrCASExhausted and, when dst did not exist beforehand, rolls back its own
// write to dst.
_ = rdb.Rename(ctx, "old-key", "new-key").Err()

// Copy duplicates src to dst. Returns 1 if copied, 0 if dst exists and replace=false.
// destinationDB must be 0: there are no numbered databases here, and a
// non-zero value is refused rather than silently copying into the current
// namespace. Use Bucketed for a separate namespace instead.
copied, _ := rdb.Copy(ctx, "source", "dest", 0, true).Result() // 1
```

### 6.8 Keys (full-scan)

> **WARNING**: `Keys` scans the whole enumeration index and filters client-side.
> It is O(n) on the cluster and should **never** be used in production hot paths.
> Intended for debugging, tests, and migrations only.

```go
keys, _ := rdb.Keys(ctx, "user:*").Result()
// returns all keys matching the glob pattern

all, _ := rdb.Keys(ctx, "*").Result()
// returns every key in the bucket
```

Supports Redis glob patterns: `*`, `?`, `[abc]`, `[a-z]`.

The index is a superset of the keys that exist: an entry is written before the
key it names and outlives a key that was deleted or expired. Enumeration
therefore verifies each candidate before returning it, and deletes entries that
no longer resolve — so a namespace churned hard gets cheaper to enumerate the
more often it is enumerated. Results are capped by `MaxKeysScan`, which returns
`ErrResultTooLarge` rather than a truncated answer.

### 6.9 Set Commands (SAdd/SRem/SMembers/SIsMember/SCard)

A set lives in its key's partition, one row per member. Members are stored as a
blob, so they are binary safe, and `SCard` reads the recorded count rather than
counting rows.

```go
// SAdd: add members. Returns count of new members added.
added, _ := rdb.SAdd(ctx, "tags", "go", "scylla", "redis").Result() // 3

// SIsMember: check membership.
ok, _ := rdb.SIsMember(ctx, "tags", "go").Result() // true

// SMembers: get all members.
members, _ := rdb.SMembers(ctx, "tags").Result() // ["go", "scylla", "redis"]

// SCard: get set cardinality.
size, _ := rdb.SCard(ctx, "tags").Result() // 3

// SRem: remove members. Returns count of members removed.
removed, _ := rdb.SRem(ctx, "tags", "redis", "missing").Result() // 1
```

### 6.10 Blocking List Ops (BLPop/BRPop)

A list lives in its key's partition, one row per position, with the head and tail
bounds recorded on the meta row.

```go
// Push producers:
_, _ = rdb.LPush(ctx, "jobs", "job-2", "job-1").Result() // head insert
_, _ = rdb.RPush(ctx, "jobs", "job-3").Result()           // tail insert

// Wait up to 5s for value from left of list.
item, err := rdb.BLPop(ctx, 5*time.Second, "jobs", "jobs:backup").Result()
if err == redis.Nil {
	// timeout
}
// item == []string{key, value}

// Wait forever (until context canceled), pop from right.
item, err = rdb.BRPop(ctx, 0, "jobs").Result()
```

Non-blocking `LPop`, `RPop`, `LLen` and `LRange` are also available.

Notes:
- CQL has no blocking primitive, so a waiter is woken by a notification and
  polls underneath. A push in the same process wakes a local waiter
  immediately; with `EnableWakeupChannel` a push also writes a notification
  other processes read every `WakeupPollInterval` (default 20ms).
- The underlying poll uses exponential backoff with jitter
  (`BlockingPollInterval` to `BlockingPollMaxInterval`, 5ms to 250ms by
  default), so idle waiters do not query in lockstep and a missed notification
  costs latency rather than an element. With `EnableWakeupChannel` the poll is
  only a safety net, so its floor defaults to 250ms instead of 5ms.
- Each pop is one conditional batch: the element is removed, the length and the
  list bounds are updated together, so an element goes to exactly one consumer
  and a drained list stops existing.
- A pop that keeps losing the race fails with `ErrCASExhausted` rather than
  reporting the list empty, because "empty" tells a worker to stop.
- Returns `redis.Nil` on timeout with no item. A timeout of `0` waits until the
  context is cancelled.

### 6.11 Scan (cursor iteration)

```go
var (
	cursor uint64
	keys   []string
)
for {
	page, next, err := rdb.Scan(ctx, cursor, "user:*", 100).Result()
	if err != nil {
		panic(err)
	}
	keys = append(keys, page...)
	if next == 0 {
		break
	}
	cursor = next
}
```

Notes:
- Each call fetches exactly one server page; it does not re-scan the namespace.
- The cursor is a handle for the server's paging state, held in this process.
  It does not survive a restart and cannot be passed to another instance;
  an unknown or expired cursor returns `ErrCursorUnknown`.
- `MATCH` must stay the same for the whole iteration, and the cursor belongs to
  the bucket that created it.
- Paging reads the same enumeration index as `Keys`, with the same verification,
  so a key that was deleted mid-iteration is not returned.
- As in Redis, a page may be empty while the cursor is still non-zero.
  Iteration is finished when the returned cursor is `0`.

### 6.12 Sort

`Sort` supports practical subset of Redis sort options:
- `Alpha`
- `Order` (`ASC`/`DESC`)
- `Offset` + `Count`
- `By: "nosort"` (optional)

Current gaps:
- `BY` patterns (except `"nosort"`) not supported yet.
- `GET` patterns not supported yet.

```go
// Numeric ascending (default):
vals, _ := rdb.Sort(ctx, "scores", nil).Result()

// Alpha descending with limit:
vals, _ = rdb.Sort(ctx, "tags", &redis.Sort{
	Alpha:  true,
	Order:  "DESC",
	Offset: 0,
	Count:  10,
}).Result()
```

### 6.13 Transactions (Multi/Watch/Exec)

A transaction applies several keys' writes as one conditional batch. Since a
conditional batch is confined to one partition, the keys must share one — set
`TransactionsByBucket` and the bucket becomes the partition, so any keys in the
same bucket can be transacted together. This is Redis Cluster's rule with a
different name: co-locate the keys you need to write together.

```go
rdb := redis.NewClient(&redis.Options{
	Addrs:                []string{"127.0.0.1:9042"},
	Keyspace:             "app",
	TransactionsByBucket: true,
	Bucket:               "tenant-a",
})
defer rdb.Close()

// Move 10 units from one balance to another, aborting if either moved.
for {
	tx := rdb.Watch(ctx, "from", "to")

	from, err := rdb.Get(ctx, "from").Int64() // read after watching
	if err != nil {
		panic(err)
	}
	if from < 10 {
		break
	}

	err = tx.IncrBy("from", -10).IncrBy("to", 10).Exec(ctx)
	if errors.Is(err, redis.ErrTxAborted) {
		continue // someone else wrote a watched key; recompute
	}
	if err != nil {
		panic(err)
	}
	break
}
```

`Watch`, `Multi` and the queueing methods chain, and the first error is held
until `Exec` (or `Err`) returns it. `Multi()` queues without watching anything.
Queueable commands are `Set`, `SetEx`, `IncrBy`, `Expire` and `Del`; reads happen
on the client, outside the transaction, as they do in Redis.

**Semantics**

- `Watch` reads each key's version when you call it. `Exec` sends one condition
  per watched key, so any change since `Watch` — including a watched key that was
  absent and now exists — aborts everything and returns `ErrTxAborted`.
- The order matters and it is the Redis order: watch, read, queue, `Exec`. A
  value read before its key was watched is not covered by any condition.
- A key the transaction writes without watching is guarded by the state `Exec`
  itself read. The race window is one round trip instead of your think time, but
  it is not zero: watch what must not be lost.
- `ErrTxAborted` is not retried for you, exactly as `EXEC` returning nil is not.
  Only the caller knows whether the queued writes still make sense against the
  new state.
- `Exec` on an empty transaction returns `ErrTxEmpty`; a transaction needs
  `TransactionsByBucket` or it returns `ErrTxUnsupported`.
- A transaction is bounded by `MaxBatchStatements`, counting conditions and
  writes together, and belongs to the goroutine that built it.

### 6.14 Bucketed Clients

Two separate decisions are involved, made at two different times.

Whether to bucket at all is a schema decision, made once, in the `Options`
passed to `NewClient`: `PartitionByBucket` and `TransactionsByBucket` are read
there and baked into that table's `CREATE TABLE` and every statement the
client generates for as long as it runs. A table is either bucketed or it
isn't; there is no per-call override. Bucket routing is independent of
atomicity: enable `PartitionByBucket` to route a tenant's keys together while
keeping one key per partition, or `TransactionsByBucket` when you also want
`Multi`/`Watch`/`Exec` across them.

Which bucket to use is the per-call-site decision, and it is just a string:
`Options.Bucket` sets it once for the whole client (default `"default"`), or
call `Bucketed(bucket)` to get a lightweight client view bound to one bucket.
It shares the same session, schema state and caches as its parent, and keeps
the same API — deriving one is cheap and does not repeat schema setup, so it
is the natural way to route many tenants through one client. The package has
no notion of "user": mapping a tenant or user ID to a bucket name is the
caller's job, typically done once where a request is first attributed to a
tenant.

Key enumeration (`Keys`, `Scan`) and every key command are scoped to the view's
bucket, so the same key name in two buckets is two keys. `Sweep` is the
exception: it reclaims across buckets whichever view calls it. `Close` on a view
is a no-op — close the client returned by `NewClient`, which owns the session.

```go
base := redis.NewClient(&redis.Options{
	Addrs:             []string{"127.0.0.1:9042"},
	Keyspace:          "app",
	PartitionByBucket: true,
})
defer base.Close()

tenantA := base.Bucketed("tenant-a")
tenantB := base.Bucketed("tenant-b")

_ = tenantA.Set(ctx, "k1", "a1", 0).Err()
_ = tenantB.Set(ctx, "k1", "b1", 0).Err() // a different key
```

### 6.15 Reclaiming Expired Keys (Sweep)

An expired key reads as absent immediately, and the first read that touches it
deletes it. A key nobody reads again is the exception: its rows stay until
something removes them. `Sweep` is that something.

```go
// Run periodically, from one process or many. No background goroutine is
// started for you: the schedule is yours.
ticker := time.NewTicker(time.Minute)
for range ticker.C {
	reclaimed, err := rdb.Sweep(ctx).Result()
	...
}
```

`Sweep` returns how many keys it reclaimed. It reads the time-bucketed expiry
index rather than scanning the namespace, so the work is proportional to what
actually expired. Each call covers the slots since the previous one, and at most
`SweepLookback` (default 1h) on a first call or after a long gap — so run it more
often than that lookback, or the keys that expired before the window are left to
the next reader. It reclaims every bucket regardless of which view calls it, and
concurrent sweepers are safe.

Nothing depends on `Sweep` for correctness — an unswept key is invisible to every
command. It reclaims disk and keeps `Keys`/`Scan` from verifying rows that will
never resolve.

## 7. Contributing

If you have any interest to be contributing in this GoCQL Fork, please read the [CONTRIBUTING.md](CONTRIBUTING.md) before initialize any Issue or Pull Request.
