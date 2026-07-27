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
  - [6.13 Atomic MSet With LWT](#613-atomic-mset-with-lwt)
  - [6.14 Bucketed Clients](#614-bucketed-clients)
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
**not** a Redis server and not a drop-in replacement for one. Single-key
mutations are made atomic by the database (lightweight transactions), but
multi-key commands, blocking list ops and `SCAN` behave differently from Redis.
Read [6.0 Compatibility and guarantees](#60-compatibility-and-guarantees) before
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

**Key lifecycle commands:**

| Method | Signature |
|---|---|
| `Expire` | `Expire(ctx, key, expiration) *BoolCmd` |
| `TTL` | `TTL(ctx, key) *DurationCmd` |
| `Persist` | `Persist(ctx, key) *BoolCmd` |

**Hash commands:**

| Method | Signature |
|---|---|
| `HSet` | `HSet(ctx, key, values...) *IntCmd` |
| `HGet` | `HGet(ctx, key, field) *StringCmd` |
| `HExists` | `HExists(ctx, key, field) *BoolCmd` |
| `HDel` | `HDel(ctx, key, fields...) *IntCmd` |
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
| `Set` | yes | with `KeepTTL` | replaces a key of any type |
| `SetEx` | yes | n/a | rejects `expiration <= 0` |
| `SetNX` | yes | n/a | one conditional insert; safe for locking |
| `Get`, `StrLen` | yes | yes | `WRONGTYPE` on non-string keys |
| `GetSet` | yes | no | clears TTL, as Redis does |
| `GetDel` | yes | n/a | conditional delete; delivered once |
| `Incr`, `IncrBy`, `Decr`, `DecrBy` | yes | yes | errors on `int64` overflow |
| `Append` | yes | yes | |
| `MGet` | no | n/a | per-key reads; `nil` for wrong-type keys |
| `MSet` | only with `AtomicMSetByBucket` | no | see 6.13 |
| `Del`, `Exists` | per key | n/a | sees all key types, cascades to collections |
| `Expire` | yes | n/a | `expiration <= 0` deletes the key (Redis 7) |
| `TTL` | yes | n/a | `-2` missing, `-1` no expiry |
| `Persist` | yes | n/a | |
| `Rename` | no | yes | string keys only |
| `Copy` | conditional without `replace` | yes | missing source returns `0`, not an error |
| `HSet`, `HDel`, `SAdd`, `SRem` | per field/member | n/a | counts are exact under concurrency |
| `SCard` | yes | n/a | counted server-side |
| `LPush`, `RPush`, `LPop`, `RPop` | per element | n/a | positions claimed with LWT |
| `BLPop`, `BRPop` | per element | n/a | polling, see below |
| `Keys` | no | n/a | full scan; maintenance tool |
| `Scan` | no | n/a | server paging-state cursor |
| `Sort` | no | n/a | `BY nosort` only, no `GET` |

**Known differences from Redis**

- Multi-key commands are not atomic as a group. `MSet` is atomic only with
  `AtomicMSetByBucket`, and only against other `MSet` calls in the same bucket:
  `Set`, `Del` and `Incr` do not take the bucket guard.
- `Expire`/`TTL`/`Persist` apply to string keys. Collections report no expiry
  and `Expire` returns `ErrKeyTypeUnsupported`.
- `Rename` and `Copy` support string keys.
- `BLPop`/`BRPop` poll with exponential backoff and jitter, because CQL has no
  blocking primitive. Each pop is atomic (an element is delivered once), but
  latency is bounded by the poll interval.
- `Scan` cursors are handles for server paging state held in this process. They
  do not survive a restart and cannot be shared; an unknown cursor returns
  `ErrCursorUnknown`.
- Key-type enforcement uses a bounded local cache, so a type change made by
  another process may go unnoticed until the entry is evicted.
- Conditional writes are lightweight transactions: correct, but more expensive
  than plain writes and serialized per partition.

**Schema**

Four tables are created from `Options.Table` (default `redis_compat_kv`):

```cql
CREATE TABLE redis_compat_kv       (key text PRIMARY KEY, type text, value blob);
CREATE TABLE redis_compat_kv_hash  (key text, field text, value blob, PRIMARY KEY (key, field));
CREATE TABLE redis_compat_kv_set   (key text, member text, PRIMARY KEY (key, member));
CREATE TABLE redis_compat_kv_list  (key text, pos bigint, value blob, PRIMARY KEY (key, pos));
```

The kv table doubles as the key namespace: string keys store their value there,
collection keys store a type-marker row. That is what lets `Del`, `Exists`,
`Keys` and `Scan` see every key type and what makes `WRONGTYPE` possible. Set
`DisableKeyTypeRegistry` to opt out of both.

With `PartitionByBucket` (or `AtomicMSetByBucket`) each primary key gains a
leading `bucket` column, so one bucket is one partition — size buckets so they
do not become hot partitions.

> Upgrading from an earlier version of this package: the `type` column is added
> automatically with `ALTER TABLE`. If you run with `DisableAutoCreateTable`,
> add it yourself before upgrading.

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
		// Optional. Default table: redis_compat_kv
		Table: "redis_compat_kv",
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
	Table:    "redis_compat_kv",
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
- TTL is a property of the stored value in CQL, so `Expire` and `Persist`
  rewrite the value under a compare-and-set. A concurrent `Set` is detected and
  the operation retries instead of resurrecting the old value.
- `Expire` with a non-positive duration deletes the key, matching Redis 7.
- These commands apply to string keys. Hashes, sets and lists report no expiry
  and `Expire` returns `ErrKeyTypeUnsupported`.

### 6.5 Hash Commands (HSet/HGet/HDel/HExists/HGetAll)

Hashes are stored in a separate `<table>_hash` table with schema `(key, field, value)`.

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
// NOT atomic — reads value, writes to new key, deletes old key. TTL is not preserved.
_ = rdb.Rename(ctx, "old-key", "new-key").Err()

// Copy duplicates src to dst. Returns 1 if copied, 0 if dst exists and replace=false.
// db parameter is ignored (Scylla has no numbered databases).
copied, _ := rdb.Copy(ctx, "source", "dest", 0, true).Result() // 1
```

### 6.8 Keys (full-scan)

> **WARNING**: `Keys` performs a full table scan (`SELECT key FROM ...`) and filters client-side.
> It is O(n) on the cluster and should **never** be used in production hot paths.
> Intended for debugging, tests, and migrations only.

```go
keys, _ := rdb.Keys(ctx, "user:*").Result()
// returns all keys matching the glob pattern

all, _ := rdb.Keys(ctx, "*").Result()
// returns every key in the table
```

Supports Redis glob patterns: `*`, `?`, `[abc]`, `[a-z]`.

### 6.9 Set Commands (SAdd/SRem/SMembers/SIsMember/SCard)

Sets are stored in a separate `<table>_set` table with schema `(key, member)`.

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

Blocking list ops use a dedicated `<table>_list` table.

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

Non-blocking `LPop`, `RPop` and `LLen` are also available.

Notes:
- Polling-based implementation, not push-notify: CQL has no blocking primitive.
- Polling uses exponential backoff with jitter (`BlockingPollInterval` to
  `BlockingPollMaxInterval`, default 5ms to 250ms), so idle waiters do not
  query in lockstep. Latency is bounded by the poll interval, not the write.
- Each pop is a conditional delete, so an element is delivered to exactly one
  consumer even with many competing workers.
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
- `MATCH` must stay the same for the whole iteration.
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

### 6.13 Atomic MSet With LWT

`MSet` can be made atomic using Scylla/Cassandra LWT (CAS), scoped to a single bucket partition.

Enable it with:

- `AtomicMSetByBucket: true`
- `Bucket: "some-bucket"` (default `"default"`; `AtomicBucket` is a deprecated alias)
- optional retry controls:
  - `AtomicMSetMaxRetries` (default `16`)
  - `AtomicMSetRetryBackoff` (default `5ms`, jittered)
  - `AtomicMSetMaxPairs` (default `100`)

When enabled:

- schema uses the bucketed key model: `PRIMARY KEY ((bucket), key)`
- a reserved guard row inside the bucket's own kv partition tracks the CAS
  version, and `MSet` applies all writes plus the guard update as one
  conditional batch
- `MSet` retries CAS conflicts with jittered exponential backoff

> [!NOTE]
> The guard must live in the same partition as the values it protects: a
> conditional batch spanning two tables (for example a separate `<table>_guard`)
> is **rejected by the server**. Keeping it in the bucket partition is what makes
> the batch legal.

**Scope and cost of the guarantee**

- It is `MSet`-against-`MSet`, within one bucket. `Set`, `Del` and `Incr` do not
  take the guard and can interleave.
- The guard admits one writer per round, so a burst of N concurrent atomic
  `MSet` calls in a bucket needs a retry budget of about N. Raise
  `AtomicMSetMaxRetries` for hot buckets or spread writes across more buckets.
  Exhausting the budget returns an error wrapping `ErrCASExhausted`.
- A bucket is a single partition. Size buckets so they do not become hot
  partitions.

```go
rdb := redis.NewClient(&redis.Options{
	Addrs:                  []string{"127.0.0.1:9042"},
	Keyspace:               "app",
	Table:                  "redis_compat_kv_atomic",
	AtomicMSetByBucket:     true,
	Bucket:                 "tenant-a",
	AtomicMSetMaxRetries:   16,
	AtomicMSetRetryBackoff: 10 * time.Millisecond,
})
defer rdb.Close()

if err := rdb.MSet(ctx, "k1", "v1", "k2", "v2").Err(); err != nil {
	// CAS conflict after retries or query error
	panic(err)
}
```

> [!IMPORTANT]
> Atomic mode requires bucketed schema. If you already use a non-bucket table (`PRIMARY KEY (key)`), use a new table name or migrate schema before enabling `AtomicMSetByBucket`.

### 6.14 Bucketed Clients

For multi-tenant routing, use `Bucketed(bucket)` to get a lightweight client view bound to one bucket.
It shares the same session, schema state and caches, and keeps the same API.

Bucket routing is independent of atomicity: enable `PartitionByBucket` for
routing alone, or `AtomicMSetByBucket` when you also want atomic `MSet`.

Note that key enumeration (`Keys`, `Scan`) and `Del`/`Exists` are scoped to the
view's bucket. `Close` on a view is a no-op — close the client returned by
`NewClient`, which owns the session.

```go
base := redis.NewClient(&redis.Options{
	Addrs:              []string{"127.0.0.1:9042"},
	Keyspace:           "app",
	Table:              "redis_compat_kv_atomic",
	AtomicMSetByBucket: true,
})
defer base.Close()

tenantA := base.Bucketed("tenant-a")
tenantB := base.Bucketed("tenant-b")

_ = tenantA.MSet(ctx, "k1", "a1", "k2", "a2").Err()
_ = tenantB.Set(ctx, "k1", "b1", 0).Err()
```

## 7. Contributing

If you have any interest to be contributing in this GoCQL Fork, please read the [CONTRIBUTING.md](CONTRIBUTING.md) before initialize any Issue or Pull Request.
