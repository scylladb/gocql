//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"testing"
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

	for i := 0; i < 5; i++ {
		for attempt := 1; true; attempt++ {
			iter := session.Query(`SELECT pk, ck, v FROM test_tablets WHERE pk = ?;`, i).WithContext(ctx).Consistency(One).Iter()
			if payload := iter.GetCustomPayload(); payload != nil {
				if hint, ok := payload["tablets-routing-v1"]; ok {
					tablet, err := unmarshalTabletHint(hint, 4, "", "")
					if err != nil {
						t.Fatalf("failed to extract tablet information: %s", err.Error())
					}
					t.Logf("%s", tablet.Replicas())
					if attempt >= 3 {
						t.Fatalf("Tablet hint from the server should not be sent")
					}
				} else {
					break
				}
			} else {
				break
			}
			if err := iter.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
