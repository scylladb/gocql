//go:build integration && scylla
// +build integration,scylla

package gocql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSession_GetShardAwareRoutingInfo_Integration(t *testing.T) {
	const (
		keyspace = "gocql_scylla_shard_aware"
		table    = "test_column_metadata"
	)

	// prepare
	{
		cluster := createCluster(func(cc *ClusterConfig) {
			cc.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
		})

		session, err := cluster.CreateSession()
		if err != nil {
			t.Fatalf("failed to create session '%v'", err)
		}

		// best practice: add clean up
		t.Cleanup(func() {
			defer session.Close() // close session after tests

			// clear DB
			if err := createTable(session, `DROP KEYSPACE IF EXISTS `+keyspace); err != nil {
				t.Logf(fmt.Sprintf("unable to drop keyspace: %v", err))
			}
		})

		err = createTable(session, `DROP KEYSPACE IF EXISTS `+keyspace)
		if err != nil {
			t.Fatalf(fmt.Sprintf("unable to drop keyspace: %v", err))
		}

		err = createTable(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class': 'NetworkTopologyStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))
		if err != nil {
			t.Fatalf(fmt.Sprintf("unable to create keyspace: %v", err))
		}

		err = createTable(session, fmt.Sprintf("CREATE TABLE %s.%s (first_id int, second_id int, third_id int, PRIMARY KEY ((first_id, second_id)))", keyspace, table))
		if err != nil {
			t.Fatalf("failed to create table with error '%v'", err)
		}
	}

	cluster := createCluster(func(cc *ClusterConfig) {
		cc.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
		cc.Keyspace = keyspace
	})

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create session '%v'", err)
	}
	defer session.Close()

	info, err := session.GetShardAwareRoutingInfo(table, []string{"first_id", "second_id", "third_id"}, 1, 2, 3)
	if err != nil {
		t.Fatalf("failed to get shared aware routing info '%v'", err)
	}

	if info.Host == nil {
		t.Fatal("empty host info")
	}

	// composite key PC key (1,2)
	var (
		mask  = []byte{0, 4}
		delim = byte(0)
		// []byte{0, 0, 0, 1} == 1
		// []byte{0, 0, 0, 2} == 2
		want = append(append(append(append(append(mask, []byte{0, 0, 0, 1}...), delim), mask...), []byte{0, 0, 0, 2}...), delim)
	)

	if !reflect.DeepEqual(info.RoutingKey, want) {
		t.Fatalf("routing key want: '%v', got: '%v'", want, info.RoutingKey)
	}

	t.Logf("shard=%d, hostname=%s", info.Shard, info.Host.hostname)
}
