package bench_test

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer/recorder"
)

func InitializeCluster() {
	cluster := gocql.NewCluster("127.0.0.2")
	cluster.Consistency = gocql.Quorum

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	keyspace := "single_conn_bench"

	err = executor.Exec(`DROP KEYSPACE IF EXISTS ` + keyspace)
	if err != nil {
		panic(fmt.Sprintf("unable to drop keyspace: %v", err))
	}

	err = executor.Exec(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy','replication_factor' : 1}`, keyspace))

	if err != nil {
		panic(fmt.Sprintf("unable to create keyspace: %v", err))
	}

	if err = executor.Exec(fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk));
	`, keyspace, "table1")); err != nil {
		panic(fmt.Sprintf("unable to create table: %v", err))
	}
}

func InsertData() {
	cluster := gocql.NewCluster("127.0.0.2")
	cluster.Consistency = gocql.Quorum

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	for i := 0; i < 100; i++ {
		err = executor.Exec(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i))
		if err != nil {
			panic(err)
		}
	}
}

func RecordSelectTraffic(size int) {
	cluster := gocql.NewCluster("127.0.0.2")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = recorder.NewRecordDialer("/home/sylwiaszunejko/gocql/recordings")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	for i := 0; i < size; i++ {
		iter := executor.Iter(`SELECT v FROM single_conn_bench.table1 WHERE pk = ?;`, i)
		var name string
		for iter.Scan(&name) {
			if name[:4] != "Name" {
				panic("Wrong value")
			}
		}
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}
}

func RecordInsertTraffic(size int) {
	cluster := gocql.NewCluster("127.0.0.2")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = recorder.NewRecordDialer("/home/sylwiaszunejko/gocql/recordings")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	executor, err := gocql.NewSingleHostQueryExecutor(cluster)
	if err != nil {
		panic(err)
	}
	defer executor.Close()

	for i := 0; i < size; i++ {
		err = executor.Exec(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i))

		if err != nil {
			panic(err)
		}
	}
}

// func BenchmarkSingleConnectionSelect(b *testing.B) {
// 	InitializeCluster()
// 	InsertData()
// 	RecordSelectTraffic(100)
// 	conn := gocql.NewMockConn()
// 	b.Run("SmallDataset", func(b *testing.B) {
// 		b.Run("Writes", func(b *testing.B) {
// 			fd, err := os.Open("../../recordings/127.0.0.2:19042-32768Writes")
// 			if err != nil {
// 				b.Fatal("failed to open replay file: %w", err)
// 			}
// 			defer fd.Close()

// 			scanner := bufio.NewScanner(fd)
// 			i := 0
// 			for scanner.Scan() {
// 				i = i + 1
// 				line := scanner.Text()
// 				b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
// 					for i := 0; i < b.N; i++ {
// 						fmt.Println(line)
// 						_, err := conn.Write([]byte(line))
// 						if err != nil {
// 							b.Fatal(err)
// 						}
// 					}
// 				})
// 			}
// 		})
// 		b.Run("Reads", func(b *testing.B) {
// 			fd, err := os.Open("../../recordings/127.0.0.2:19042-32768Reads")
// 			if err != nil {
// 				b.Fatal("failed to open replay file: %w", err)
// 			}
// 			defer fd.Close()

// 			scanner := bufio.NewScanner(fd)
// 			i := 0
// 			for scanner.Scan() {
// 				i = i + 1
// 				line := scanner.Text()
// 				b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
// 					for i := 0; i < b.N; i++ {
// 						fmt.Println(line)
// 						_, err := conn.Read([]byte(line))
// 						if err != nil {
// 							b.Fatal(err)
// 						}
// 					}
// 				})
// 			}
// 		})
// 	})
// 	// b.Run("MediumDataset", func(b *testing.B) {
// 	// 	RecordTraffic(10000)
// 	// })
// 	// b.Run("BigDataset", func(b *testing.B) {
// 	// 	RecordTraffic(100000)
// 	// })
// }

func BenchmarkSingleConnectionInsert(b *testing.B) {
	InitializeCluster()
	RecordInsertTraffic(100)
	// conn := gocql.NewMockConn()
	// b.Run("SmallDataset", func(b *testing.B) {
	// 	b.Run("Writes", func(b *testing.B) {
	// 		fd, err := os.Open("../../recordings/127.0.0.2:19042-32768Writes")
	// 		if err != nil {
	// 			b.Fatal("failed to open replay file: %w", err)
	// 		}
	// 		defer fd.Close()

	// 		scanner := bufio.NewScanner(fd)
	// 		i := 0
	// 		for scanner.Scan() {
	// 			i = i + 1
	// 			line := scanner.Text()
	// 			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
	// 				for i := 0; i < b.N; i++ {
	// 					fmt.Println(line)
	// 					_, err := conn.Write([]byte(line))
	// 					if err != nil {
	// 						b.Fatal(err)
	// 					}
	// 				}
	// 			})
	// 		}
	// 	})
	// 	b.Run("Reads", func(b *testing.B) {
	// 		fd, err := os.Open("../../recordings/127.0.0.2:19042-32768Reads")
	// 		if err != nil {
	// 			b.Fatal("failed to open replay file: %w", err)
	// 		}
	// 		defer fd.Close()

	// 		scanner := bufio.NewScanner(fd)
	// 		i := 0
	// 		for scanner.Scan() {
	// 			i = i + 1
	// 			line := scanner.Text()
	// 			b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
	// 				for i := 0; i < b.N; i++ {
	// 					fmt.Println(line)
	// 					_, err := conn.Read([]byte(line))
	// 					if err != nil {
	// 						b.Fatal(err)
	// 					}
	// 				}
	// 			})
	// 		}
	// 	})
	// })
	// b.Run("MediumDataset", func(b *testing.B) {
	// 	RecordTraffic(10000)
	// })
	// b.Run("BigDataset", func(b *testing.B) {
	// 	RecordTraffic(100000)
	// })
}
