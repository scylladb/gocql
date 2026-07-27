//go:build scylla_integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/gocql/gocql"
)

// Integration tests for the paths a fake cannot prove: real lightweight
// transactions, real conditional batches, real server paging.
//
// Run with:
//
//	go test -tags scylla_integration -run TestIntegration ./redis/... \
//	  -scylla.hosts=127.0.0.1
//
// or set SCYLLA_HOSTS. The tests create and drop their own keyspace.

func integrationHosts(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("SCYLLA_HOSTS")
	if raw == "" {
		raw = "127.0.0.1"
	}
	return strings.Split(raw, ",")
}

func newIntegrationClient(t *testing.T, mutate func(*Options)) *Client {
	t.Helper()

	hosts := integrationHosts(t)
	keyspace := fmt.Sprintf("rediscompat_it_%d", time.Now().UnixNano()%1_000_000)

	admin := gocql.NewCluster(hosts...)
	admin.Timeout = 20 * time.Second
	session, err := gocql.NewSession(*admin)
	if err != nil {
		t.Skipf("no Scylla available at %v: %v", hosts, err)
	}
	create := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
		keyspace)
	if err := session.Query(create).Exec(); err != nil {
		session.Close()
		t.Fatalf("create keyspace: %v", err)
	}
	session.Close()

	opt := &Options{
		Addrs:             hosts,
		Keyspace:          keyspace,
		Table:             "kv",
		Timeout:           20 * time.Second,
		SerialConsistency: gocql.Serial,
	}
	if mutate != nil {
		mutate(opt)
	}

	client := NewClient(opt)
	if client.configErr != nil {
		t.Fatalf("NewClient: %v", client.configErr)
	}

	t.Cleanup(func() {
		drop := gocql.NewCluster(hosts...)
		drop.Timeout = 20 * time.Second
		if s, err := gocql.NewSession(*drop); err == nil {
			_ = s.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
			s.Close()
		}
		_ = client.Close()
	})
	return client
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

// TestIntegrationAtomicMSetBatchIsAccepted is the regression test for the
// conditional batch: a batch spanning the kv table and a separate guard table
// is rejected by the server, which is why the guard row lives in the bucket
// partition of the kv table.
func TestIntegrationAtomicMSetBatchIsAccepted(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, func(o *Options) {
		o.AtomicMSetByBucket = true
		o.Bucket = "tenant-1"
	})

	if err := client.MSet(ctx, "a", "1", "b", "2", "c", "3").Err(); err != nil {
		t.Fatalf("atomic MSet: %v", err)
	}
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, err := client.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if got != want {
			t.Fatalf("Get(%s) = %q, want %q", key, got, want)
		}
	}

	// Concurrent atomic MSets must all land, each advancing the guard.
	const writers = 8
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			if err := client.MSet(ctx, key, "v").Err(); err != nil {
				t.Errorf("concurrent atomic MSet: %v", err)
			}
		}(i)
	}
	wg.Wait()
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

func TestIntegrationWrongTypeAndDelCascade(t *testing.T) {
	ctx := context.Background()
	client := newIntegrationClient(t, nil)

	if err := client.HSet(ctx, "profile", "name", "ada").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Get(ctx, "profile").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Get on hash key = %v, want WRONGTYPE", err)
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
}
