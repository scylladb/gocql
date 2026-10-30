/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bench_test

import (
	"context"
	"flag"
	"fmt"
	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/dialer/recorder"
	"github.com/scylladb/gocql/v2/dialer/replayer"
	"os"
	"strconv"
	"strings"
	"testing"
)

func benchConfig(d gocql.Dialer) gocql.Config {
	c := gocql.DefaultConfig("192.168.100.11")
	c.Pool.ConnectionsPerHost = 1
	c.Metadata.InitialDiscovery = false
	c.Dialer = d
	return c
}
func benchExec(s *gocql.Session, cql string) error {
	b, e := gocql.BindUnpreparedOwned(gocql.Statement[gocql.Unit, gocql.Unit]{CQL: cql, Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), gocql.Unit{})
	if e != nil {
		return e
	}
	defer b.Close()
	_, e = gocql.Exec(context.Background(), s, b, gocql.Options{})
	return e
}
func InitializeCluster() error {
	s, e := gocql.Connect(context.Background(), benchConfig(nil))
	if e != nil {
		return e
	}
	defer s.Close()
	for _, cql := range []string{"DROP KEYSPACE IF EXISTS single_conn_bench", "CREATE KEYSPACE single_conn_bench WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}", "CREATE TABLE single_conn_bench.table1(pk int PRIMARY KEY,ck int,v text)"} {
		if e := benchExec(s, cql); e != nil {
			return e
		}
	}
	return nil
}
func benchRead(s *gocql.Session, id int32) error {
	p, e := gocql.Prepare(context.Background(), s, gocql.Statement[int32, gocql.Text]{CQL: "SELECT v FROM single_conn_bench.table1 WHERE pk = ?;", Params: gocql.OneParam(gocql.Int32), Rows: gocql.OneColumn(gocql.TextCodec)}, gocql.PrepareOptions{})
	if e != nil {
		return e
	}
	b, e := gocql.BindOwned(p, id)
	if e != nil {
		return e
	}
	defer b.Close()
	rows, e := gocql.Run(context.Background(), s, b, gocql.Options{})
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		if !strings.HasPrefix(rows.Row().String(), "Name") {
			return fmt.Errorf("wrong recorded name")
		}
	}
	return rows.Err()
}

type benchInsert struct {
	PK, CK int32
	Value  string
}

func benchWrite(s *gocql.Session, id int32) error {
	p, e := gocql.Prepare(context.Background(), s, gocql.Statement[benchInsert, gocql.Unit]{CQL: "INSERT INTO single_conn_bench.table1 (pk, ck, v) VALUES (?, ?, ?);", Params: gocql.Params(gocql.Param("pk", gocql.Int32, func(v benchInsert) int32 { return v.PK }), gocql.Param("ck", gocql.Int32, func(v benchInsert) int32 { return v.CK }), gocql.Param("v", gocql.StringEncoder, func(v benchInsert) string { return v.Value })), Rows: gocql.NoRows}, gocql.PrepareOptions{})
	if e != nil {
		return e
	}
	b, e := gocql.BindOwned(p, benchInsert{id, id % 5, fmt.Sprintf("Name_%d", id)})
	if e != nil {
		return e
	}
	defer b.Close()
	_, e = gocql.Exec(context.Background(), s, b, gocql.Options{})
	return e
}
func RecordSelectTraffic(size int, dir string) error {
	s, e := gocql.Connect(context.Background(), benchConfig(recorder.NewRecordDialer(dir)))
	if e != nil {
		return e
	}
	defer s.Close()
	for i := 0; i < size; i++ {
		if e := benchRead(s, int32(i)); e != nil {
			return e
		}
	}
	return nil
}
func RecordInsertTraffic(size int, dir string) error {
	s, e := gocql.Connect(context.Background(), benchConfig(recorder.NewRecordDialer(dir)))
	if e != nil {
		return e
	}
	defer s.Close()
	for i := 0; i < size; i++ {
		if e := benchWrite(s, int32(i)); e != nil {
			return e
		}
	}
	return nil
}
func BenchmarkSingleConnectionSelect(b *testing.B) {
	s, e := gocql.Connect(context.Background(), benchConfig(replayer.NewReplayDialer("rec_select")))
	if e != nil {
		b.Fatal(e)
	}
	defer s.Close()
	for i := 0; i < 10; i++ {
		b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if e := benchRead(s, int32(i)); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
func BenchmarkSingleConnectionInsert(b *testing.B) {
	s, e := gocql.Connect(context.Background(), benchConfig(replayer.NewReplayDialer("rec_insert")))
	if e != nil {
		b.Fatal(e)
	}
	defer s.Close()
	for i := 0; i < 10; i++ {
		b.Run("Case"+strconv.Itoa(i), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if e := benchWrite(s, int32(i)); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
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
