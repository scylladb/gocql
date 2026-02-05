//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Check if TokenAwareHostPolicy works correctly when using tablets
func TestTablets(t *testing.T) {
	if !isTabletsSupported() {
		t.Skip("Tablets are not supported by this server")
	}
	cluster := createCluster()

	fallback := RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck));
	`, "test_tablets")); err != nil {
		t.Fatalf("unable to create table: %v", err)
	}

	hosts := session.hostSource.getHostsList()

	hostAddresses := []string{}
	for _, host := range hosts {
		hostAddresses = append(hostAddresses, host.connectAddress.String())
	}

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		err := session.Query(`INSERT INTO test_tablets (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, i%2).WithContext(ctx).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := range 5 {
		startTime := time.Now()
		timeout := 2 * time.Second
		backoffDelay := 100 * time.Millisecond
		success := false

		for attempt := 1; time.Since(startTime) < timeout; attempt++ {
			iter := session.Query(`SELECT pk, ck, v FROM test_tablets WHERE pk = ?;`, i).WithContext(ctx).Consistency(One).Iter()

			payload := iter.GetCustomPayload()

			if err := iter.Close(); err != nil {
				t.Fatal(err)
			}

			if payload == nil || payload["tablets-routing-v1"] == nil {
				// Routing is working correctly
				success = true
				break
			}

			// Hint received, tablet migration may be in progress
			hint := payload["tablets-routing-v1"]
			tablet, err := unmarshalTabletHint(hint, 4, "", "")
			if err != nil {
				t.Fatalf("failed to extract tablet information: %s", err.Error())
			}
			t.Logf("Attempt %d: received tablet hint (replicas: %s) - tablet migration may be in progress, backing off %v", attempt, tablet.Replicas(), backoffDelay)

			// Backoff to allow tablet migration to complete, but do not exceed the overall timeout.
			remaining := timeout - time.Since(startTime)
			if remaining <= 0 {
				// Overall timeout reached; exit the retry loop and fail after the loop.
				break
			}
			sleepFor := backoffDelay
			if sleepFor > remaining {
				sleepFor = remaining
			}
			time.Sleep(sleepFor)
			backoffDelay *= 2 // Exponential backoff
		}

		if !success {
			elapsed := time.Since(startTime)
			t.Fatalf("Timed out after %v (elapsed %v) waiting for tablets to stabilize (migrations still in progress)", timeout, elapsed)
		}
	}
}
