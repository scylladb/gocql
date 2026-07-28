//go:build scylla_integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/gocql/gocql"
)

// Integration tests for the paths a fake cannot prove: real lightweight
// transactions, real conditional batches over a key partition, real clustering
// order for encoded list positions, and real server paging.
//
// Run with:
//
//	go test -tags scylla_integration ./redis/... -timeout 15m
//
// Set SCYLLA_HOSTS to point at a cluster. The tests create and drop their own
// keyspace.

func integrationHosts(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("SCYLLA_HOSTS")
	if raw == "" {
		raw = "127.0.0.1"
	}
	return strings.Split(raw, ",")
}

// integrationKeyspace creates a keyspace for one test and drops it afterwards.
// NetworkTopologyStrategy rather than SimpleStrategy: clusters with tablets
// enabled, the default from Scylla 2025 onward, reject SimpleStrategy.
func integrationKeyspace(t *testing.T) (hosts []string, keyspace string) {
	t.Helper()

	hosts = integrationHosts(t)
	keyspace = fmt.Sprintf("rediscompat_it_%d", time.Now().UnixNano()%1_000_000_000)

	admin := gocql.NewCluster(hosts...)
	admin.Timeout = 30 * time.Second
	session, err := gocql.NewSession(*admin)
	if err != nil {
		t.Skipf("no Scylla available at %v: %v", hosts, err)
	}
	defer session.Close()

	create := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "+
			"{'class':'NetworkTopologyStrategy','replication_factor':1}",
		keyspace)
	if err := session.Query(create).Exec(); err != nil {
		t.Fatalf("create keyspace: %v", err)
	}

	t.Cleanup(func() {
		drop := gocql.NewCluster(hosts...)
		drop.Timeout = 30 * time.Second
		if s, err := gocql.NewSession(*drop); err == nil {
			_ = s.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
			s.Close()
		}
	})
	return hosts, keyspace
}

func newClientOn(t *testing.T, hosts []string, keyspace string, mutate func(*Options)) *Client {
	t.Helper()

	opt := &Options{
		Addrs:             hosts,
		Keyspace:          keyspace,
		Table:             "kv",
		Timeout:           30 * time.Second,
		SerialConsistency: gocql.Serial,
	}
	if mutate != nil {
		mutate(opt)
	}

	client := NewClient(opt)
	if client.configErr != nil {
		t.Fatalf("NewClient: %v", client.configErr)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newIntegrationClient(t *testing.T, mutate func(*Options)) *Client {
	t.Helper()
	hosts, keyspace := integrationKeyspace(t)
	return newClientOn(t, hosts, keyspace, mutate)
}

func TestIntegrationSetNXIsExclusive(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	const workers = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			ok, err := client.SetNX(ctx, "lock", fmt.Sprintf("worker-%d", i), time.Minute).Result()
			if err != nil {
				t.Errorf("SetNX: %v", err)
				return
			}
			if ok {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("%d workers acquired the lock, want exactly 1", winners)
	}
}

func TestIntegrationConcurrentIncrIsExact(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) { o.CASMaxRetries = 50 })

	const workers, perWorker = 8, 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if err := client.Incr(ctx, "counter").Err(); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := client.Get(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != fmt.Sprint(workers*perWorker) {
		t.Fatalf("counter = %s, want %d", got, workers*perWorker)
	}
}

// The recorded element count is maintained by the same conditional batch that
// writes the elements, so it can only stay exact if the server rejects every
// batch whose version guard moved. Only a real Paxos round decides that.
func TestIntegrationCollectionCardinalityStaysExact(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) { o.CASMaxRetries = 60 })

	const workers, perWorker = 8, 10
	var wg sync.WaitGroup
	wg.Add(2 * workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				added, err := client.SAdd(ctx, "tags", fmt.Sprintf("m-%d-%d", w, i)).Result()
				if err != nil {
					t.Errorf("SAdd: %v", err)
					return
				}
				if added != 1 {
					t.Errorf("SAdd of a fresh member = %d, want 1", added)
					return
				}
			}
		}(w)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				created, err := client.HSet(ctx, "profile", fmt.Sprintf("f-%d-%d", w, i), "v").Result()
				if err != nil {
					t.Errorf("HSet: %v", err)
					return
				}
				if created != 1 {
					t.Errorf("HSet of a fresh field = %d, want 1", created)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	const want = workers * perWorker
	if got := client.SCard(ctx, "tags").Val(); got != want {
		t.Fatalf("SCard = %d, want %d", got, want)
	}
	if members := client.SMembers(ctx, "tags").Val(); len(members) != want {
		t.Fatalf("SMembers returned %d members, want %d", len(members), want)
	}
	if got := client.HLen(ctx, "profile").Val(); got != want {
		t.Fatalf("HLen = %d, want %d", got, want)
	}
	if all := client.HGetAll(ctx, "profile").Val(); len(all) != want {
		t.Fatalf("HGetAll returned %d fields, want %d", len(all), want)
	}

	// Re-adding what is already there must not move the count, and the server
	// has to reject the redundant writes rather than count them again.
	if added := client.SAdd(ctx, "tags", "m-0-0", "m-0-1").Val(); added != 0 {
		t.Fatalf("SAdd of existing members = %d, want 0", added)
	}
	if got := client.SCard(ctx, "tags").Val(); got != want {
		t.Fatalf("SCard after redundant adds = %d, want %d", got, want)
	}
}

// A key and its elements share a partition, so Del is one conditional batch
// with a slice delete in it. The server has to accept that shape, and it has to
// leave nothing behind.
func TestIntegrationDelCascadesInOneBatch(t *testing.T) {
	ctx := context.Background()
	hosts, keyspace := integrationKeyspace(t)
	client := newClientOn(t, hosts, keyspace, nil)

	fields := make([]any, 0, 80)
	for i := 0; i < 40; i++ {
		fields = append(fields, fmt.Sprintf("f%02d", i), "v")
	}
	if err := client.HSet(ctx, "profile", fields...).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Get(ctx, "profile").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Get on a hash key = %v, want WRONGTYPE", err)
	}

	removed, err := client.Del(ctx, "profile").Result()
	if err != nil {
		t.Fatalf("Del: %v", err)
	}
	if removed != 1 {
		t.Fatalf("Del = %d, want 1", removed)
	}
	if got := client.Exists(ctx, "profile").Val(); got != 0 {
		t.Fatalf("Exists after Del = %d, want 0", got)
	}
	if all := client.HGetAll(ctx, "profile").Val(); len(all) != 0 {
		t.Fatalf("hash rows survived Del: %v", all)
	}

	// Nothing may be left in the partition, not even a row the client no
	// longer reads: a leftover element would attach itself to the next key of
	// the same name.
	admin := gocql.NewCluster(hosts...)
	admin.Timeout = 30 * time.Second
	session, err := gocql.NewSession(*admin)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer session.Close()

	var rows int
	if err := session.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.kv WHERE key = 'profile'", keyspace)).
		Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("%d rows survived Del, want 0", rows)
	}
}

// SET replaces a key of any type. The element rows go in the same batch as the
// new value, so the key is never half a hash and half a string.
func TestIntegrationSetReplacesCollection(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.HSet(ctx, "k", "a", "1", "b", "2", "c", "3").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Set(ctx, "k", "now-a-string", 0).Err(); err != nil {
		t.Fatalf("Set over a hash: %v", err)
	}
	if got := client.Get(ctx, "k").Val(); got != "now-a-string" {
		t.Fatalf("Get = %q", got)
	}
	if err := client.HGetAll(ctx, "k").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HGetAll after Set = %v, want WRONGTYPE", err)
	}
	if got := client.Type(ctx, "k").Val(); got != "string" {
		t.Fatalf("Type = %q, want string", got)
	}
}

// List positions are signed and pushing left walks below zero, so the encoded
// clustering key has to compare in numeric order across the sign boundary.
// Bytewise blob comparison on the server is the thing being tested.
func TestIntegrationListOrderSurvivesNegativePositions(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.RPush(ctx, "queue", "e", "f", "g").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	// Two batches of left pushes so the head crosses zero and keeps going.
	if err := client.LPush(ctx, "queue", "d", "c").Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if err := client.LPush(ctx, "queue", "b", "a").Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}

	if got := client.LLen(ctx, "queue").Val(); got != 7 {
		t.Fatalf("LLen = %d, want 7", got)
	}
	if got := strings.Join(client.LRange(ctx, "queue", 0, -1).Val(), ""); got != "abcdefg" {
		t.Fatalf("LRange = %q, want abcdefg", got)
	}
	if got := strings.Join(client.LRange(ctx, "queue", 2, 4).Val(), ""); got != "cde" {
		t.Fatalf("LRange(2,4) = %q, want cde", got)
	}

	// Both ends have to be reachable: the head comes from an ascending slice
	// shared with the meta row, the tail from a descending one.
	if got := client.LPop(ctx, "queue").Val(); got != "a" {
		t.Fatalf("LPop = %q, want a", got)
	}
	if got := client.RPop(ctx, "queue").Val(); got != "g" {
		t.Fatalf("RPop = %q, want g", got)
	}

	var drained []string
	for i := 0; i < 5; i++ {
		value, err := client.LPop(ctx, "queue").Result()
		if err != nil {
			t.Fatalf("LPop: %v", err)
		}
		drained = append(drained, value)
	}
	if got := strings.Join(drained, ""); got != "bcdef" {
		t.Fatalf("drained %q, want bcdef", got)
	}
	if got := client.Exists(ctx, "queue").Val(); got != 0 {
		t.Fatalf("Exists on a drained list = %d, want 0", got)
	}
}

func TestIntegrationListPopDeliversOnce(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	const jobs = 30
	values := make([]any, jobs)
	for i := range values {
		values[i] = fmt.Sprintf("job-%d", i)
	}
	if err := client.RPush(ctx, "queue", values...).Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	var (
		mu       sync.Mutex
		consumed = map[string]int{}
		wg       sync.WaitGroup
	)
	wg.Add(4)
	for w := 0; w < 4; w++ {
		go func() {
			defer wg.Done()
			for {
				value, err := client.LPop(ctx, "queue").Result()
				if err == Nil {
					return
				}
				if errors.Is(err, ErrCASExhausted) {
					// Losing the race for the head says another consumer took
					// it, not that the queue is empty. A consumer retries.
					continue
				}
				if err != nil {
					t.Errorf("LPop: %v", err)
					return
				}
				mu.Lock()
				consumed[value]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(consumed) != jobs {
		t.Fatalf("consumed %d distinct jobs, want %d", len(consumed), jobs)
	}
	for value, count := range consumed {
		if count != 1 {
			t.Fatalf("%s was delivered %d times", value, count)
		}
	}
	if got := client.Exists(ctx, "queue").Val(); got != 0 {
		t.Fatalf("Exists on a fully drained queue = %d, want 0", got)
	}
}

// Expiry is a column on the meta row rather than only a cell TTL, which is what
// makes it apply to collections and what makes it conditionable. The remaining
// lifetime therefore has to survive a round trip through a timestamp column.
func TestIntegrationExpiryAppliesToEveryType(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.HSet(ctx, "profile", "name", "ada").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.SAdd(ctx, "tags", "x").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if err := client.RPush(ctx, "queue", "job").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	for _, key := range []string{"profile", "tags", "queue"} {
		ok, err := client.Expire(ctx, key, 2*time.Second).Result()
		if err != nil {
			t.Fatalf("Expire(%s): %v", key, err)
		}
		if !ok {
			t.Fatalf("Expire(%s) = false, want true", key)
		}
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("TTL(%s): %v", key, err)
		}
		if ttl <= 0 || ttl > 2*time.Second {
			t.Fatalf("TTL(%s) = %v, want a remainder inside 2s", key, ttl)
		}
	}

	// Persist is O(1) for a collection: it rewrites the expiry column, not the
	// elements.
	if err := client.Persist(ctx, "tags").Err(); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if ttl := client.TTL(ctx, "tags").Val(); ttl != -1*time.Second {
		t.Fatalf("TTL after Persist = %v, want -1s", ttl)
	}

	time.Sleep(2500 * time.Millisecond)

	if got := client.Exists(ctx, "profile", "queue").Val(); got != 0 {
		t.Fatalf("Exists after expiry = %d, want 0", got)
	}
	if all := client.HGetAll(ctx, "profile").Val(); len(all) != 0 {
		t.Fatalf("expired hash still returns %v", all)
	}
	if got := client.LLen(ctx, "queue").Val(); got != 0 {
		t.Fatalf("LLen of an expired list = %d, want 0", got)
	}
	if got := client.Exists(ctx, "tags").Val(); got != 1 {
		t.Fatalf("Exists of the persisted set = %d, want 1", got)
	}
	// The key namespace is reusable, and as another type.
	if err := client.Set(ctx, "profile", "v", 0).Err(); err != nil {
		t.Fatalf("Set over an expired hash: %v", err)
	}
}

// A string carries a cell TTL alongside its expiry so the server reclaims it
// unprompted, and every command that rewrites the value has to carry the
// remaining lifetime forward or the key silently becomes immortal.
func TestIntegrationTTLSurvivesRewrites(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	positive := func(t *testing.T, label, key string) {
		t.Helper()
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("%s TTL: %v", label, err)
		}
		if ttl <= 0 || ttl > time.Hour {
			t.Fatalf("%s TTL = %v, want a remainder inside the original hour", label, ttl)
		}
	}

	if err := client.Set(ctx, "counter", "1", time.Hour).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	positive(t, "after Set", "counter")

	if err := client.Incr(ctx, "counter").Err(); err != nil {
		t.Fatalf("Incr: %v", err)
	}
	positive(t, "after Incr", "counter")

	if err := client.Append(ctx, "counter", "9").Err(); err != nil {
		t.Fatalf("Append: %v", err)
	}
	positive(t, "after Append", "counter")

	if err := client.Set(ctx, "counter", "5", KeepTTL).Err(); err != nil {
		t.Fatalf("Set KeepTTL: %v", err)
	}
	positive(t, "after Set KeepTTL", "counter")

	if err := client.Rename(ctx, "counter", "moved").Err(); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	positive(t, "after Rename", "moved")
	if got := client.Exists(ctx, "counter").Val(); got != 0 {
		t.Fatalf("source survived Rename")
	}

	if err := client.Copy(ctx, "moved", "copied", 0, false).Err(); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	positive(t, "after Copy", "copied")

	const noExpiry = -1 * time.Second
	if err := client.Set(ctx, "forever", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ttl, err := client.TTL(ctx, "forever").Result(); err != nil || ttl != noExpiry {
		t.Fatalf("TTL of a persistent key = (%v, %v), want %v", ttl, err, noExpiry)
	}
	if err := client.Persist(ctx, "moved").Err(); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if ttl, err := client.TTL(ctx, "moved").Result(); err != nil || ttl != noExpiry {
		t.Fatalf("TTL after Persist = (%v, %v), want %v", ttl, err, noExpiry)
	}
	if ttl, err := client.TTL(ctx, "absent").Result(); err != nil || ttl != -2*time.Second {
		t.Fatalf("TTL of a missing key = (%v, %v), want -2s", ttl, err)
	}

	// GETSET clears the TTL, as Redis does, and the cell TTL has to go with it
	// or the value disappears while the key claims to be persistent.
	if err := client.Set(ctx, "token", "old", 2*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := client.GetSet(ctx, "token", "new").Result(); err != nil || got != "old" {
		t.Fatalf("GetSet = (%q, %v), want old", got, err)
	}
	if ttl := client.TTL(ctx, "token").Val(); ttl != noExpiry {
		t.Fatalf("TTL after GetSet = %v, want %v", ttl, noExpiry)
	}
	time.Sleep(3 * time.Second)
	if got, err := client.Get(ctx, "token").Result(); err != nil || got != "new" {
		t.Fatalf("Get after the original TTL would have fired = (%q, %v), want new", got, err)
	}
}

// Sweep reads the time bucketed expiry index, so it has to find keys nobody
// touched again without scanning the namespace.
func TestIntegrationSweepReclaimsExpiredKeys(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) { o.SweepLookback = 5 * time.Minute })

	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("short:%d", i), "v", time.Second).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := client.HSet(ctx, "hash:short", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Expire(ctx, "hash:short", time.Second).Err(); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if err := client.Set(ctx, "keeper", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	time.Sleep(2 * time.Second)

	reclaimed, err := client.Sweep(ctx).Result()
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if reclaimed < 6 {
		t.Fatalf("Sweep reclaimed %d keys, want at least 6", reclaimed)
	}
	if keys := client.Keys(ctx, "*").Val(); len(keys) != 1 || keys[0] != "keeper" {
		t.Fatalf("Keys after Sweep = %v, want only keeper", keys)
	}

	// A second sweep of the same slots must find nothing left to reclaim.
	again, err := client.Sweep(ctx).Result()
	if err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if again != 0 {
		t.Fatalf("second Sweep reclaimed %d keys, want 0", again)
	}
}

// With TransactionsByBucket every key of a bucket shares one partition, which
// is what lets Exec apply several keys as one conditional batch. The server
// decides whether that batch applies, so this is the test that matters.
func TestIntegrationTransactionIsAtomic(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) {
		o.TransactionsByBucket = true
		o.Bucket = "tenant-1"
	})

	if err := client.Set(ctx, "balance", "100", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "profile", "name", "ada").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	err := client.Multi().
		IncrBy("balance", -30).
		Set("audit", "withdrawal").
		SetEx("receipt", "r-1", time.Hour).
		Del("profile").
		Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if got := client.Get(ctx, "balance").Val(); got != "70" {
		t.Fatalf("balance = %q, want 70", got)
	}
	if got := client.Get(ctx, "audit").Val(); got != "withdrawal" {
		t.Fatalf("audit = %q", got)
	}
	if ttl := client.TTL(ctx, "receipt").Val(); ttl <= 0 {
		t.Fatalf("receipt TTL = %v, want positive", ttl)
	}
	if got := client.Exists(ctx, "profile").Val(); got != 0 {
		t.Fatalf("Exists(profile) after a queued Del = %d, want 0", got)
	}

	// A watched key that moves after it was watched has to abort the whole
	// batch, applying none of it.
	tx := client.Watch(ctx, "balance").Set("side-effect", "must-not-land")
	if err := client.Set(ctx, "balance", "999", 0).Err(); err != nil {
		t.Fatalf("interfering Set: %v", err)
	}
	if err := tx.Exec(ctx); !errors.Is(err, ErrTxAborted) {
		t.Fatalf("Exec after a watched key changed = %v, want ErrTxAborted", err)
	}
	if err := client.Get(ctx, "side-effect").Err(); !errors.Is(err, Nil) {
		t.Fatalf("aborted transaction still wrote its key: %v", err)
	}

	// Two transactions that watched the same key: the first to Exec applies,
	// the second is refused because its snapshot is stale.
	if err := client.Set(ctx, "seat", "free", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	first := client.Watch(ctx, "seat").Set("seat", "taken-by-a")
	second := client.Watch(ctx, "seat").Set("seat", "taken-by-b")
	if err := first.Exec(ctx); err != nil {
		t.Fatalf("first Exec: %v", err)
	}
	if err := second.Exec(ctx); !errors.Is(err, ErrTxAborted) {
		t.Fatalf("second Exec = %v, want ErrTxAborted", err)
	}
	if got := client.Get(ctx, "seat").Val(); got != "taken-by-a" {
		t.Fatalf("seat = %q, want taken-by-a", got)
	}

	// A watched key that does not exist yet is guarded on staying absent, and
	// the guard may not create it.
	claim := client.Watch(ctx, "claim").Set("claim", "mine")
	if err := client.Set(ctx, "claim", "theirs", 0).Err(); err != nil {
		t.Fatalf("interfering Set: %v", err)
	}
	if err := claim.Exec(ctx); !errors.Is(err, ErrTxAborted) {
		t.Fatalf("Exec after a watched key appeared = %v, want ErrTxAborted", err)
	}
	if got := client.Get(ctx, "claim").Val(); got != "theirs" {
		t.Fatalf("claim = %q, want theirs", got)
	}
	if err := client.Watch(ctx, "never").Set("ok", "1").Exec(ctx); err != nil {
		t.Fatalf("Exec with an untouched absent watch: %v", err)
	}
	if got := client.Exists(ctx, "never").Val(); got != 0 {
		t.Fatalf("the absence guard created the key it guards: Exists = %d", got)
	}
}

// Concurrent transactions on one key must not lose an update: every Exec that
// reported success has to be visible in the final value, and the rest have to
// have been refused rather than silently dropped.
func TestIntegrationConcurrentTransactionsDoNotLoseUpdates(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) {
		o.TransactionsByBucket = true
	})

	if err := client.Set(ctx, "balance", "0", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	const workers = 12
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		applied int
		aborted int
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			err := client.Watch(ctx, "balance").IncrBy("balance", 1).Exec(ctx)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				applied++
			case errors.Is(err, ErrTxAborted):
				aborted++
			default:
				t.Errorf("Exec: %v", err)
			}
		}()
	}
	wg.Wait()

	if applied == 0 {
		t.Fatal("no transaction applied")
	}
	if applied+aborted != workers {
		t.Fatalf("%d applied and %d aborted, want %d outcomes", applied, aborted, workers)
	}
	got, err := client.Get(ctx, "balance").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != fmt.Sprint(applied) {
		t.Fatalf("balance = %s, want %d: an applied transaction was lost", got, applied)
	}
}

func TestIntegrationTransactionsRequireGroupedLayout(t *testing.T) {
	client := newIntegrationClient(t, func(o *Options) { o.PartitionByBucket = true })
	if err := client.Multi().Set("k", "v").Exec(context.Background()); !errors.Is(err,
		ErrTxUnsupported) {
		t.Fatalf("Exec without TransactionsByBucket = %v, want ErrTxUnsupported", err)
	}
}

// Rename within one bucket partition is a single conditional batch, so the
// value is never visible under both names and never under neither.
func TestIntegrationGroupedRenameIsAtomic(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) {
		o.TransactionsByBucket = true
		o.Bucket = "tenant-1"
	})

	if err := client.Set(ctx, "src", "value", time.Hour).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "dst", "leftover", "1").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Rename(ctx, "src", "dst").Err(); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if got := client.Exists(ctx, "src").Val(); got != 0 {
		t.Fatalf("source survived Rename")
	}
	if got := client.Get(ctx, "dst").Val(); got != "value" {
		t.Fatalf("destination = %q, want value", got)
	}
	if ttl := client.TTL(ctx, "dst").Val(); ttl <= 0 {
		t.Fatalf("destination TTL = %v, want the source remainder", ttl)
	}
	if got := client.Type(ctx, "dst").Val(); got != "string" {
		t.Fatalf("destination Type = %q, want string", got)
	}
}

// Copy without replace must lose the race against an occupied destination
// without disturbing what is there. Whether the conditional insert applies is
// decided by the server's Paxos round.
func TestIntegrationCopyLeavesOccupiedDestination(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.HSet(ctx, "dst", "field", "keep").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Set(ctx, "src", "value", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	copied, err := client.Copy(ctx, "src", "dst", 0, false).Result()
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if copied != 0 {
		t.Fatalf("Copy = %d, want 0", copied)
	}
	if got := client.HGet(ctx, "dst", "field").Val(); got != "keep" {
		t.Fatalf("destination hash = %q, want it untouched", got)
	}

	copied, err = client.Copy(ctx, "src", "dst", 0, true).Result()
	if err != nil {
		t.Fatalf("Copy with replace: %v", err)
	}
	if copied != 1 {
		t.Fatalf("Copy with replace = %d, want 1", copied)
	}
	if got := client.Get(ctx, "dst").Val(); got != "value" {
		t.Fatalf("destination after replace = %q, want the copied value", got)
	}
}

// Claiming a key that does not exist is a conditional type change, so
// concurrent claimers of different types have to agree on exactly one.
func TestIntegrationTypeClaimIsExclusive(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	const writers = 12
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		wins  int
		fails int
	)
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			var err error
			if i%2 == 0 {
				err = client.SAdd(ctx, "k", fmt.Sprintf("m-%d", i)).Err()
			} else {
				err = client.HSet(ctx, "k", fmt.Sprintf("f-%d", i), "v").Err()
			}
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrWrongType), errors.Is(err, ErrCASExhausted):
				fails++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if wins == 0 {
		t.Fatal("no writer could claim the key")
	}
	if wins+fails != writers {
		t.Fatalf("%d wins and %d rejections, want %d outcomes", wins, fails, writers)
	}

	setErr := client.SMembers(ctx, "k").Err()
	hashErr := client.HGetAll(ctx, "k").Err()
	if (setErr == nil) == (hashErr == nil) {
		t.Fatalf("key answers as both types: SMembers=%v HGetAll=%v", setErr, hashErr)
	}
	if got := client.Exists(ctx, "k").Val(); got != 1 {
		t.Fatalf("Exists after the claim = %d, want 1", got)
	}
}

// Reading a key's meta row together with named elements is one multi-column IN
// query, and the mutation that follows is one batch. Both have server side
// limits, so they are exercised at a realistic width.
func TestIntegrationWideElementBatch(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	const fields = 60
	values := make([]any, 0, 2*fields)
	for i := 0; i < fields; i++ {
		values = append(values, fmt.Sprintf("f%02d", i), fmt.Sprintf("v%02d", i))
	}
	created, err := client.HSet(ctx, "wide", values...).Result()
	if err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if created != fields {
		t.Fatalf("HSet created %d fields, want %d", created, fields)
	}

	names := make([]string, 0, fields)
	for i := 0; i < fields; i++ {
		names = append(names, fmt.Sprintf("f%02d", i))
	}
	if got := client.HGet(ctx, "wide", "f42").Val(); got != "v42" {
		t.Fatalf("HGet = %q, want v42", got)
	}

	removed, err := client.HDel(ctx, "wide", names[:50]...).Result()
	if err != nil {
		t.Fatalf("HDel: %v", err)
	}
	if removed != 50 {
		t.Fatalf("HDel removed %d fields, want 50", removed)
	}
	if got := client.HLen(ctx, "wide").Val(); got != fields-50 {
		t.Fatalf("HLen = %d, want %d", got, fields-50)
	}

}

// A command that would need more statements than the ceiling allows is refused
// rather than split, so a command that succeeded was applied as one batch.
func TestIntegrationBatchCeilingRefusesWideCommands(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) { o.MaxBatchStatements = 8 })

	values := make([]any, 0, 40)
	for i := 0; i < 20; i++ {
		values = append(values, fmt.Sprintf("f%02d", i), "v")
	}
	if err := client.HSet(ctx, "wide", values...).Err(); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("HSet past MaxBatchStatements = %v, want ErrBatchTooLarge", err)
	}
	if got := client.Exists(ctx, "wide").Val(); got != 0 {
		t.Fatalf("a refused command wrote something: Exists = %d", got)
	}
	if err := client.HSet(ctx, "wide", values[:6]...).Err(); err != nil {
		t.Fatalf("HSet inside the ceiling: %v", err)
	}
	if got := client.HLen(ctx, "wide").Val(); got != 3 {
		t.Fatalf("HLen = %d, want 3", got)
	}
}

func TestIntegrationMSetAndMGet(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "logged batch across partitions"},
		{name: "unlogged batch in one partition", mutate: func(o *Options) {
			o.TransactionsByBucket = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newIntegrationClient(t, tc.mutate)
			if err := client.MSet(ctx, "a", "1", "b", "2", "c", "3").Err(); err != nil {
				t.Fatalf("MSet: %v", err)
			}
			values, err := client.MGet(ctx, "a", "b", "c", "missing").Result()
			if err != nil {
				t.Fatalf("MGet: %v", err)
			}
			if len(values) != 4 {
				t.Fatalf("MGet returned %d values", len(values))
			}
			for i, want := range []any{"1", "2", "3", nil} {
				if values[i] != want {
					t.Fatalf("MGet[%d] = %v, want %v", i, values[i], want)
				}
			}
			if keys := client.Keys(ctx, "*").Val(); len(keys) != 3 {
				t.Fatalf("Keys = %v, want 3 keys", keys)
			}
			// MSET clears any previous TTL, as Redis does.
			if err := client.Set(ctx, "a", "1", time.Hour).Err(); err != nil {
				t.Fatalf("Set: %v", err)
			}
			if err := client.MSet(ctx, "a", "2").Err(); err != nil {
				t.Fatalf("MSet: %v", err)
			}
			if ttl := client.TTL(ctx, "a").Val(); ttl != -1*time.Second {
				t.Fatalf("TTL after MSet = %v, want -1s", ttl)
			}
		})
	}
}

// Every layout clusters the table differently, and which restrictions a read
// may use follows from that. Running one command of each shape under all three
// is what catches a statement that is only valid in the layout it was written
// for.
func TestIntegrationEveryLayoutServesEveryCommand(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "key per partition"},
		{name: "bucket routed", mutate: func(o *Options) { o.PartitionByBucket = true }},
		{name: "bucket partition", mutate: func(o *Options) { o.TransactionsByBucket = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newIntegrationClient(t, tc.mutate)

			if err := client.Set(ctx, "s", "v", 0).Err(); err != nil {
				t.Fatalf("Set: %v", err)
			}
			if got := client.Get(ctx, "s").Val(); got != "v" {
				t.Fatalf("Get = %q", got)
			}

			// Hashes and sets read the meta row together with named elements,
			// which is the multi-column restriction the layout changes.
			if created := client.HSet(ctx, "h", "f1", "v1", "f2", "v2").Val(); created != 2 {
				t.Fatalf("HSet = %d, want 2", created)
			}
			if got := client.HGet(ctx, "h", "f2").Val(); got != "v2" {
				t.Fatalf("HGet = %q, want v2", got)
			}
			if got := client.HLen(ctx, "h").Val(); got != 2 {
				t.Fatalf("HLen = %d, want 2", got)
			}
			if removed := client.HDel(ctx, "h", "f1", "absent").Val(); removed != 1 {
				t.Fatalf("HDel = %d, want 1", removed)
			}
			if added := client.SAdd(ctx, "t", "a", "b").Val(); added != 2 {
				t.Fatalf("SAdd = %d, want 2", added)
			}
			if !client.SIsMember(ctx, "t", "b").Val() {
				t.Fatalf("SIsMember = false, want true")
			}
			if got := client.SCard(ctx, "t").Val(); got != 2 {
				t.Fatalf("SCard = %d, want 2", got)
			}

			// Both list ends: the head shares a slice with the meta row, the
			// tail needs a descending read of the last clustering column.
			if err := client.RPush(ctx, "q", "b", "c").Err(); err != nil {
				t.Fatalf("RPush: %v", err)
			}
			if err := client.LPush(ctx, "q", "a").Err(); err != nil {
				t.Fatalf("LPush: %v", err)
			}
			if got := strings.Join(client.LRange(ctx, "q", 0, -1).Val(), ""); got != "abc" {
				t.Fatalf("LRange = %q, want abc", got)
			}
			if got := client.LPop(ctx, "q").Val(); got != "a" {
				t.Fatalf("LPop = %q, want a", got)
			}
			if got := client.RPop(ctx, "q").Val(); got != "c" {
				t.Fatalf("RPop = %q, want c", got)
			}
			if got := client.LLen(ctx, "q").Val(); got != 1 {
				t.Fatalf("LLen = %d, want 1", got)
			}

			if err := client.Expire(ctx, "h", time.Hour).Err(); err != nil {
				t.Fatalf("Expire: %v", err)
			}
			if ttl := client.TTL(ctx, "h").Val(); ttl <= 0 {
				t.Fatalf("TTL = %v, want positive", ttl)
			}
			if keys := client.Keys(ctx, "*").Val(); len(keys) != 4 {
				t.Fatalf("Keys = %v, want 4", keys)
			}
			page, _, err := client.Scan(ctx, 0, "*", 100).Result()
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(page) != 4 {
				t.Fatalf("Scan = %v, want 4", page)
			}
			if removed := client.Del(ctx, "s", "h", "t", "q").Val(); removed != 4 {
				t.Fatalf("Del = %d, want 4", removed)
			}
			if keys := client.Keys(ctx, "*").Val(); len(keys) != 0 {
				t.Fatalf("Keys after Del = %v, want none", keys)
			}
		})
	}
}

func TestIntegrationScanPaging(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	const total = 120
	for i := 0; i < total; i++ {
		if err := client.Set(ctx, fmt.Sprintf("key:%03d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	seen := map[string]bool{}
	var cursor uint64
	for pages := 0; pages < 100; pages++ {
		page, next, err := client.Scan(ctx, cursor, "key:*", 25).Result()
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		for _, key := range page {
			if seen[key] {
				t.Fatalf("Scan returned %s twice", key)
			}
			seen[key] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if cursor != 0 {
		t.Fatal("Scan did not finish")
	}
	if len(seen) != total {
		t.Fatalf("Scan saw %d keys, want %d", len(seen), total)
	}
}

// Enumeration reads a side index that is deliberately a superset, so a key that
// expired or was never written has to be filtered by the verifying read and the
// stale entry removed as it is found.
func TestIntegrationEnumerationSelfHeals(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.Set(ctx, "live", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Set(ctx, "brief", "v", time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if keys := client.Keys(ctx, "*").Val(); len(keys) != 2 {
		t.Fatalf("Keys = %v, want 2", keys)
	}

	time.Sleep(1500 * time.Millisecond)

	if keys := client.Keys(ctx, "*").Val(); len(keys) != 1 || keys[0] != "live" {
		t.Fatalf("Keys after expiry = %v, want only live", keys)
	}
	page, _, err := client.Scan(ctx, 0, "*", 100).Result()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(page) != 1 || page[0] != "live" {
		t.Fatalf("Scan after expiry = %v, want only live", page)
	}
}

// A cursor holds the server's paging state for one bucket partition. Replaying
// it under another bucket would resume a different tenant's iteration.
func TestIntegrationScanCursorIsBucketScoped(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) {
		o.PartitionByBucket = true
		o.Bucket = "tenant-a"
	})
	other := client.Bucketed("tenant-b")

	for i := 0; i < 40; i++ {
		if err := client.Set(ctx, fmt.Sprintf("key:%03d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := other.Set(ctx, "own-key", "v", 0).Err(); err != nil {
		t.Fatalf("Set in the other bucket: %v", err)
	}

	page, cursor, err := client.Scan(ctx, 0, "key:*", 10).Result()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if cursor == 0 {
		t.Fatalf("first page held %d keys and finished the scan, cannot test isolation", len(page))
	}
	if _, _, err := other.Scan(ctx, cursor, "*", 10).Result(); !errors.Is(err, ErrCursorUnknown) {
		t.Fatalf("cursor replayed under another bucket = %v, want ErrCursorUnknown", err)
	}

	// The rejected replay must not have consumed the cursor, and each bucket
	// only ever sees its own keys.
	seen := map[string]bool{}
	for _, key := range page {
		seen[key] = true
	}
	for pages := 0; pages < 50 && cursor != 0; pages++ {
		next, cur, err := client.Scan(ctx, cursor, "key:*", 10).Result()
		if err != nil {
			t.Fatalf("Scan continuation: %v", err)
		}
		for _, key := range next {
			if seen[key] {
				t.Fatalf("Scan returned %s twice", key)
			}
			seen[key] = true
		}
		cursor = cur
	}
	if len(seen) != 40 {
		t.Fatalf("bucket scan saw %d keys, want 40", len(seen))
	}
	if keys := other.Keys(ctx, "*").Val(); len(keys) != 1 || keys[0] != "own-key" {
		t.Fatalf("other bucket saw %v, want only its own key", keys)
	}
}

// A blocked consumer in one process is woken by a producer in another through
// the shared wakeup partition. The poll interval is set far above the timeout
// this test allows, so a delivery inside it can only have come from the wakeup.
func TestIntegrationWakeupChannelWakesAnotherClient(t *testing.T) {
	ctx := context.Background()
	hosts, keyspace := integrationKeyspace(t)

	tune := func(o *Options) {
		o.EnableWakeupChannel = true
		o.WakeupPollInterval = 20 * time.Millisecond
		o.BlockingPollInterval = 30 * time.Second
		o.BlockingPollMaxInterval = 30 * time.Second
	}
	consumer := newClientOn(t, hosts, keyspace, tune)
	producer := newClientOn(t, hosts, keyspace, tune)

	// Create the schema before the consumer blocks, so the measured latency is
	// the wakeup path rather than DDL.
	if err := producer.Set(ctx, "warmup", "v", 0).Err(); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	type result struct {
		values  []string
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		values, err := consumer.BLPop(ctx, 25*time.Second, "queue").Result()
		done <- result{values, err, time.Since(start)}
	}()

	time.Sleep(500 * time.Millisecond)
	if err := producer.RPush(ctx, "queue", "job-1").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("BLPop: %v", got.err)
		}
		if len(got.values) != 2 || got.values[0] != "queue" || got.values[1] != "job-1" {
			t.Fatalf("BLPop = %v, want [queue job-1]", got.values)
		}
		if got.elapsed > 10*time.Second {
			t.Fatalf("BLPop took %v: the wakeup channel did not deliver, only the poll did", got.elapsed)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("BLPop never returned")
	}
}

// Without the wakeup channel the poll is the only mechanism, and it still has
// to deliver every element exactly once.
func TestIntegrationBlockingPopDrainsWithPollingOnly(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) {
		o.BlockingPollInterval = 10 * time.Millisecond
		o.CASMaxRetries = 40
	})

	const jobs = 12
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		consumed []string
	)
	wg.Add(3)
	for w := 0; w < 3; w++ {
		go func() {
			defer wg.Done()
			for {
				values, err := client.BLPop(ctx, 3*time.Second, "queue").Result()
				if errors.Is(err, Nil) {
					return
				}
				if err != nil {
					t.Errorf("BLPop: %v", err)
					return
				}
				mu.Lock()
				consumed = append(consumed, values[1])
				mu.Unlock()
			}
		}()
	}

	for i := 0; i < jobs; i++ {
		if err := client.RPush(ctx, "queue", fmt.Sprintf("job-%02d", i)).Err(); err != nil {
			t.Fatalf("RPush: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	sort.Strings(consumed)
	if len(consumed) != jobs {
		t.Fatalf("consumed %d jobs, want %d: %v", len(consumed), jobs, consumed)
	}
	for i, value := range consumed {
		if want := fmt.Sprintf("job-%02d", i); value != want {
			t.Fatalf("consumed[%d] = %q, want %q (a job was delivered twice)", i, value, want)
		}
	}
}

func TestIntegrationSortReadsCollections(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.RPush(ctx, "numbers", "10", "2", "33", "4").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	got, err := client.Sort(ctx, "numbers", &Sort{}).Result()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if strings.Join(got, ",") != "2,4,10,33" {
		t.Fatalf("Sort = %v, want numeric order", got)
	}

	got, err = client.Sort(ctx, "numbers", &Sort{Order: "DESC", Count: 2}).Result()
	if err != nil {
		t.Fatalf("Sort DESC: %v", err)
	}
	if strings.Join(got, ",") != "33,10" {
		t.Fatalf("Sort DESC = %v, want 33,10", got)
	}

	if err := client.SAdd(ctx, "words", "pear", "apple").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	got, err = client.Sort(ctx, "words", &Sort{Alpha: true}).Result()
	if err != nil {
		t.Fatalf("Sort Alpha: %v", err)
	}
	if strings.Join(got, ",") != "apple,pear" {
		t.Fatalf("Sort Alpha = %v", got)
	}
	if err := client.Set(ctx, "plain", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Sort(ctx, "plain", &Sort{Alpha: true}).Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Sort on a string = %v, want WRONGTYPE", err)
	}
}

// Values are binary safe and set members are stored as a blob clustering
// column, so both have to survive bytes a text column would reject.
func TestIntegrationBinarySafety(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	payload := []byte{0x00, 0xff, 0xfe, 'a', 0x0a}
	if err := client.Set(ctx, "blob", payload, 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := client.Get(ctx, "blob").Val(); got != string(payload) {
		t.Fatalf("Get = %q, want %q", got, payload)
	}
	if err := client.SAdd(ctx, "blobs", payload).Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if ok := client.SIsMember(ctx, "blobs", payload).Val(); !ok {
		t.Fatalf("SIsMember of a binary member = false, want true")
	}
	if members := client.SMembers(ctx, "blobs").Val(); len(members) != 1 || members[0] != string(payload) {
		t.Fatalf("SMembers = %q", members)
	}
	if err := client.HSet(ctx, "h", "field", payload).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if got := client.HGet(ctx, "h", "field").Val(); got != string(payload) {
		t.Fatalf("HGet = %q, want %q", got, payload)
	}
}
