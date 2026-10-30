//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"
)

type v2TabletAttemptObserver struct {
	mu      sync.Mutex
	targets []Target
}

func (*v2TabletAttemptObserver) ObserveRequest(RequestEvent) {}
func (o *v2TabletAttemptObserver) ObserveAttempt(event AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if event.Err == nil {
		o.targets = append(o.targets, event.Target)
	}
}
func (o *v2TabletAttemptObserver) snapshot() []Target {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]Target(nil), o.targets...)
}

// Token-aware execution must converge on tablet host/shard targets without a
// server redirection hint, while retaining immutable public routing snapshots.
func TestTablets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := createV2Cluster()
	v2ConfigureRouting(t, &cluster, RoutingOptions{TokenAware: true})
	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()
	supported, err := v2TabletsSupported(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Skip("Tablets are not supported by this server")
	}
	table := v2SchemaRoutingName(t)
	if err := v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))`, table)); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 5; i++ {
		if _, err := v2Exec(ctx, session, fmt.Sprintf(`INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)`, table), IntValue(i), IntValue(i%5), IntValue(i%2)); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := Prepare(ctx, session, Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf(`SELECT pk, ck, v FROM %s WHERE pk = ?`, table), Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 5; i++ {
		bound, err := BindOwned(prepared, []Value{IntValue(i)})
		if err != nil {
			t.Fatal(err)
		}
		defer bound.Close()
		startTime, timeout, backoffDelay := time.Now(), 2*time.Second, 100*time.Millisecond
		success := false
		for attempt := 1; time.Since(startTime) < timeout; attempt++ {
			observer := new(v2TabletAttemptObserver)
			options := Options{Consistency: Option[Consistency]{Value: One, Set: true}, Observer: observer}
			// Warm the cache by visiting existing shard slots. With RF equal to the
			// cluster size and token-aligned tablet shards, every ordinary V1 request
			// can already reach an owner and legitimately receive no feedback.
			cached, _ := session.Metadata().Replicas(bound.Routing().Keyspace, table, bound.Routing().Token.Value)
			if len(cached) == 0 {
				hosts := session.Metadata().Hosts()
				if len(hosts) > 0 && hosts[0].Shards > 1 {
					options.HostID = Option[UUID]{Value: hosts[0].ID, Set: true}
					options.ShardID = Option[uint32]{Value: uint32(attempt) % hosts[0].Shards, Set: true}
				}
			}
			rows, err := Run(ctx, session, bound, options)
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				rows.Close()
				t.Fatalf("missing tablet row %d: %v", i, rows.Err())
			}
			row := rows.Row()
			if v2SchemaRoutingInt(t, row, "pk") != i || v2SchemaRoutingInt(t, row, "ck") != i%5 || v2SchemaRoutingInt(t, row, "v") != i%2 {
				rows.Close()
				t.Fatal("unexpected tablet row")
			}
			info := rows.Info()
			hinted := false
			for _, entry := range info.CustomPayload {
				if (entry.Key != "tablets-routing-v1" && entry.Key != "tablets-routing-v2") || !entry.Value.Valid {
					continue
				}
				hinted = true
				tablet, err := parseTypedTabletPayload(entry.Value.Value, bound.Routing().Keyspace, table, entry.Key == "tablets-routing-v2")
				if err != nil {
					rows.Close()
					t.Fatalf("failed to extract tablet information: %v", err)
				}
				t.Logf("Attempt %d: received tablet hint (replicas: %s), backing off %v", attempt, tablet.Replicas(), backoffDelay)
			}
			if rows.Next() {
				rows.Close()
				t.Fatal("unexpected additional tablet row")
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if !hinted {
				snapshot := session.Metadata()
				replicas, err := snapshot.Replicas(bound.Routing().Keyspace, table, bound.Routing().Token.Value)
				if err != nil {
					t.Fatal(err)
				}
				// V1 sends no hint when a cold request happens to hit an owning
				// shard. A cache miss is still warm-up, even without feedback.
				if len(replicas) == 0 {
					remaining := timeout - time.Since(startTime)
					if remaining > 0 {
						time.Sleep(min(backoffDelay, remaining))
					}
					continue
				}
				targets := observer.snapshot()
				if len(targets) == 0 {
					t.Fatal("tablet execution did not record an attempt")
				}
				target := targets[len(targets)-1]
				if target.HostID != info.HostID {
					t.Fatalf("attempt/coordinator mismatch: %v, %v", target, info.HostID)
				}
				if !slices.Contains(replicas, target) || !target.ShardID.Set {
					t.Fatalf("tablet execution target %v is not a shard-specific replica in %v", target, replicas)
				}
				success = true
				break
			}
			remaining := timeout - time.Since(startTime)
			if remaining <= 0 {
				break
			}
			time.Sleep(min(backoffDelay, remaining))
			backoffDelay *= 2
		}
		if !success {
			t.Fatalf("Timed out after %v (elapsed %v) waiting for tablets to stabilize (migrations still in progress)", timeout, time.Since(startTime))
		}
	}
	snapshot := session.Metadata()
	ranges, err := snapshot.TokenRanges(prepared.Info().Keyspace, table)
	if err != nil || len(ranges) == 0 {
		t.Fatalf("missing tablet ranges: %v", err)
	}
	for _, tokenRange := range ranges {
		for _, token := range []int64{tokenRange.End, tokenRange.Start, math.MinInt64, math.MaxInt64} {
			var expected []Target
			for _, candidate := range ranges {
				if token > candidate.Start && token <= candidate.End || candidate.Start >= candidate.End && (token > candidate.Start || token <= candidate.End) {
					expected = candidate.Replicas
					break
				}
			}
			actual, err := snapshot.Replicas(prepared.Info().Keyspace, table, token)
			if err != nil || !slices.Equal(actual, expected) {
				t.Fatalf("tablet boundary %d: got %v, want %v, error %v", token, actual, expected, err)
			}
		}
	}
	firstRange := ranges[0]
	expectedReplicas := append([]Target(nil), firstRange.Replicas...)
	ranges[0].Replicas[0].ShardID.Value++
	actual, err := snapshot.Replicas(prepared.Info().Keyspace, table, firstRange.End)
	if err != nil || !slices.Equal(actual, expectedReplicas) {
		t.Fatalf("mutating returned range changed snapshot: %v, %v", actual, err)
	}
	if err := session.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	actual, err = snapshot.Replicas(prepared.Info().Keyspace, table, firstRange.End)
	if err != nil || !slices.Equal(actual, expectedReplicas) {
		t.Fatalf("refresh changed retained snapshot: %v, %v", actual, err)
	}
}
