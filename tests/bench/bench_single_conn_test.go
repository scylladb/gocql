package bench_test

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer/recorder"
	"github.com/gocql/gocql/dialer/replayer"
)

func InitializeCluster() error {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		return fmt.Errorf("failed to create executor: %v", err)
	}
	defer executor.Close()

	keyspace := "single_conn_bench"

	err = executor.Exec(`DROP KEYSPACE IF EXISTS ` + keyspace)
	if err != nil {
		return fmt.Errorf("unable to drop keyspace: %v", err)
	}

	err = executor.Exec(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy','replication_factor' : 1}`, keyspace))

	if err != nil {
		return fmt.Errorf("unable to create keyspace: %v", err)
	}

	if err = executor.Exec(fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk));
	`, keyspace, "table1")); err != nil {
		return fmt.Errorf("unable to create table: %v", err)
	}
	return nil
}

func RecordSelectTraffic(size int, dir string) error {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = recorder.NewRecordDialer(dir)

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		return fmt.Errorf("failed to create executor: %v", err)
	}
	defer executor.Close()

	for i := 0; i < size; i++ {
		iter := executor.Iter(`SELECT v FROM single_conn_bench.table1 WHERE pk = ?;`, i)
		var name string
		for iter.Scan(&name) {
			if name[:4] != "Name" {
				return fmt.Errorf("got wrong value for name: %s", name)
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("failed to close iterator: %v", err)
		}
	}
	return nil
}

func RecordInsertTraffic(size int, dir string) error {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = recorder.NewRecordDialer(dir)

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		return fmt.Errorf("failed to create executor: %v", err)
	}
	defer executor.Close()

	for i := 0; i < size; i++ {
		err = executor.Exec(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i))
		if err != nil {
			return fmt.Errorf("failed to insert: %v", err)
		}
	}
	return nil
}

func BenchmarkSingleConnectionSelect(b *testing.B) {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = replayer.NewReplayDialer("rec_select")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		b.Fatalf("failed to create executor: %v", err)
	}
	defer executor.Close()

	b.Run("Select", func(b *testing.B) {
		for i := 0; i < 10; i++ {
			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
				for j := 0; j < b.N; j++ {
					_ = executor.Iter(`SELECT v FROM single_conn_bench.table1 WHERE pk = ?;`, i)
				}
			})
		}
	})
}

func BenchmarkSingleConnectionInsert(b *testing.B) {
	cluster := gocql.NewCluster("192.168.100.11")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = replayer.NewReplayDialer("rec_insert")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		b.Fatalf("failed to create executor: %v", err)
	}
	defer executor.Close()

	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < 10; i++ {
			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
				for j := 0; j < b.N; j++ {
					err = executor.Exec(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i))
					if err != nil {
						b.Fatalf("failed to insert: %v", err)
					}
				}
			})
		}
	})
}

// TestMain records the golden files the benchmarks above replay, under
// -update-golden. It needs a real node, and one reachable at the address the recording
// file names embed -- the replayer looks a recording up by the address the driver
// dialed, so 192.168.100.11 is not decoration.
//
// Nothing in CI can produce these files. A container on a bridge network of that subnet
// is the cheapest way to get one:
//
//	docker network create --subnet 192.168.100.0/24 --gateway 192.168.100.1 gocql-bench
//	docker run -d --name gocql-bench-scylla --network gocql-bench --ip 192.168.100.11 \
//	  scylladb/scylla:2026.2.4 --smp 1 --memory 1G --developer-mode 1 --overprovisioned 1
//	# wait for CQL, then from the repository root:
//	rm -f tests/bench/rec_select/* tests/bench/rec_insert/*
//	go test -C tests/bench -run '^$' -update-golden .
//	docker rm -f gocql-bench-scylla && docker network rm gocql-bench
//
// Clearing the directories first is not optional. ConnectionRecorder opens its files
// with O_APPEND, so a second run stacks another connection's frames on top of the
// first; the loader keys records by stream id and keeps whichever it read last, which
// makes the extra sessions invisible rather than harmless. The recordings were 23
// stacked sessions of the same 15 frames before this was written down.
//
// Regenerate whenever the driver changes what it puts on the wire during control
// connection setup -- a new control query, a different one, another handshake frame.
// TestBenchRecordingsMatchTheControlQuery in the root package guards the case that has
// already happened once.
func TestMain(m *testing.M) {
	update := flag.Bool("update-golden", false, "Update golden files")
	flag.Parse()
	if *update {
		err := InitializeCluster()
		if err != nil {
			fmt.Printf("failed to initialize cluster: %v\n", err)
			os.Exit(1)
		}
		err = RecordInsertTraffic(10, "rec_insert")
		if err != nil {
			fmt.Printf("failed to record insert traffic: %v\n", err)
			os.Exit(1)
		}
		err = RecordSelectTraffic(10, "rec_select")
		if err != nil {
			fmt.Printf("failed to record select traffic: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}
