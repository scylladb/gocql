package bench

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
)

func RecordTraffic(size int) {
	cluster := gocql.NewCluster("127.0.0.2")
	cluster.Consistency = gocql.Quorum

	cluster.Dialer = gocql.NewRecordDialer("/home/sylwiaszunejko/gocql/recordings")

	fallback := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	keyspace := "single_conn_bench"

	err = session.Query(`DROP KEYSPACE IF EXISTS ` + keyspace).Exec()
	if err != nil {
		panic(fmt.Sprintf("unable to drop keyspace: %v", err))
	}

	err = session.Query(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy','replication_factor' : 1}`, keyspace)).Exec()

	if err != nil {
		panic(fmt.Sprintf("unable to create keyspace: %v", err))
	}

	if err := session.Query(fmt.Sprintf(`CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk));
	`, keyspace, "table1")).Exec(); err != nil {
		panic(fmt.Sprintf("unable to create table: %v", err))
	}

	ctx := context.Background()

	for i := 0; i < size; i++ {
		err = session.Query(`INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);`, i, i%5, fmt.Sprintf("Name_%d", i)).WithContext(ctx).Exec()
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < size; i++ {
		var pk int
		var ck int
		var v string

		err = session.Query(`SELECT pk, ck, v FROM single_conn_bench.table1 WHERE pk = ?;`, i).WithContext(ctx).Consistency(gocql.One).Scan(&pk, &ck, &v)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSingleConnection(b *testing.B) {
	// RecordTraffic(100)
	conn := gocql.NewMockConn()
	b.Run("SmallDataset", func(b *testing.B) {
		b.Run("Writes", func(b *testing.B) {
			fd, err := os.Open("../../recordings/127.0.0.2:19042-32768Writes")
			if err != nil {
				b.Fatal("failed to open replay file: %w", err)
			}
			defer fd.Close()

			scanner := bufio.NewScanner(fd)
			i := 0
			for scanner.Scan() {
				i = i + 1
				line := scanner.Text()
				b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						fmt.Println(line)
						_, err := conn.Write([]byte(line))
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
		b.Run("Reads", func(b *testing.B) {
			fd, err := os.Open("../../recordings/127.0.0.2:19042-32768Reads")
			if err != nil {
				b.Fatal("failed to open replay file: %w", err)
			}
			defer fd.Close()

			scanner := bufio.NewScanner(fd)
			i := 0
			for scanner.Scan() {
				i = i + 1
				line := scanner.Text()
				b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						fmt.Println(line)
						_, err := conn.Read([]byte(line))
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	})
	// b.Run("MediumDataset", func(b *testing.B) {
	// 	RecordTraffic(10000)
	// })
	// b.Run("BigDataset", func(b *testing.B) {
	// 	RecordTraffic(100000)
	// })
}
