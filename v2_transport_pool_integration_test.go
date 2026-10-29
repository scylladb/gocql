//go:build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestV2TransportPoolSizingLive(t *testing.T) {
	for _, mode := range []string{"host", "shard"} {
		t.Run(mode, func(t *testing.T) {
			config := createV2Cluster()
			config.KeepaliveInterval = 0
			if *flagRunSslTest {
				config.CoalesceWait = 100 * time.Microsecond
			}
			if mode == "host" {
				config.Pool.ConnectionsPerHost = 2
			} else {
				config.Pool.ConnectionsPerHost = 0
				config.Pool.ConnectionsPerShard = 2
			}
			var tlsCalls atomic.Int32
			if hook := config.TLSForHost; hook != nil {
				config.TLSForHost = func(ctx context.Context, host HostSnapshot) (*tls.Config, error) {
					tlsCalls.Add(1)
					return hook(ctx, host)
				}
			}
			s := createV2SessionFromConfig(config, t, false)
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for _, host := range s.hostSource.getHostsList() {
				pool, ok := s.pool.getPool(host)
				if !ok {
					t.Fatal("missing host pool")
				}
				p, ok := pool.connPicker.(*typedConnPicker)
				if !ok {
					t.Fatalf("v2 pool used legacy picker %T", pool.connPicker)
				}
				for {
					p.mu.RLock()
					count, missing := p.count(), max(0, p.target()-p.count())
					p.mu.RUnlock()
					if missing == 0 {
						t.Logf("mode=%s host=%s total=%d shards=%d", mode, host.hostUUID(), count, p.GetShardCount())
						break
					}
					select {
					case <-ctx.Done():
						t.Fatalf("pool target not reached mode=%s count=%d missing=%d", mode, count, missing)
					case <-time.After(time.Millisecond):
					}
				}
				p.mu.RLock()
				targeted := 0
				for shard, connections := range p.conns {
					if mode == "shard" && len(connections) != 2 {
						p.mu.RUnlock()
						t.Fatalf("shard%d has%d connections", shard, len(connections))
					}
					for _, conn := range connections {
						if conn.requestedShard.Set {
							targeted++
							if int(conn.requestedShard.Value) != conn.scyllaSupported.shard {
								p.mu.RUnlock()
								t.Fatal("wrong requested shard")
							}
						}
						if *flagRunSslTest {
							if _, ok := conn.r.(*connReader).conn.(*tls.Conn); !ok {
								p.mu.RUnlock()
								t.Fatal("TLS host path became plaintext")
							}
						}
					}
				}
				p.mu.RUnlock()
				if mode == "host" && targeted != 0 {
					t.Fatal("per-host override performed targeted fills")
				}
				if mode == "shard" && p.nrShards > 1 && config.Pool.ShardAwarePort && targeted == 0 {
					t.Fatal("shard mode never used source-port routing")
				}
				if pool.transportState() != poolReady {
					t.Fatal("full pool not ready")
				}
			}
			if *flagRunSslTest && tlsCalls.Load() == 0 {
				t.Fatal("per-host TLS hook not used")
			}
		})
	}
}

func TestV2ExclusiveTransportLive(t *testing.T) {
	config := createV2Cluster()
	config.Pool.ConnectionsPerHost = 1
	config.KeepaliveInterval = 0
	// v3/v4 frame compression is intentionally unsupported for streaming; this
	// lane explicitly negotiates raw, while v5 retains its raw/LZ4 selection.
	if config.Protocol < 5 {
		config.Compression = NoCompression
	}
	s := createV2SessionFromConfig(config, t, false)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn := codecTestMust(s.selectTypedConn(ctx, RoutingInfo{}, nil))
	table := fmt.Sprintf("step04_stream_%x", time.Now().UnixNano())
	qualified := "gocql_test." + table
	ddl := func(cql string) {
		b := codecTestMust(BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}))
		defer b.Close()
		if _, err := Exec(ctx, s, b, Options{connection: conn}); err != nil {
			t.Fatal(err)
		}
		if err := s.awaitV2SchemaAgreementOn(ctx, conn); err != nil {
			t.Fatal(err)
		}
	}
	ddl("CREATE TABLE " + qualified + "(id int PRIMARY KEY,value blob)")
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		b, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "DROP TABLE IF EXISTS " + qualified, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if err == nil {
			defer b.Close()
			_, err = Exec(cleanup, s, b, Options{})
		}
		if err != nil {
			t.Errorf("cleanup unique stream table: %v", err)
		}
	}()
	insert := codecTestMust(Prepare(ctx, s, Statement[Pair[int32, []byte], Unit]{CQL: "INSERT INTO " + qualified + "(id,value) VALUES(?,?)", Params: PairParams(1, OneParam(Int32), OneParam(BytesEncoder)), Rows: NoRows}, PrepareOptions{}))
	size := 400 << 10
	for i := 0; i < 4; i++ {
		payload := bytes.Repeat([]byte{byte(i + 1)}, size)
		bound := codecTestMust(BindOwned(insert, Pair[int32, []byte]{int32(i), payload}))
		_, err := Exec(ctx, s, bound, Options{})
		bound.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	pool, ok := s.pool.getPool(conn.host)
	if !ok {
		t.Fatal("pool missing")
	}
	r, err := pool.reserveStream(ctx, Target{HostID: conn.host.hostUUID()}, RoutingInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.close(false)
	decoder := StreamRow(func(*Pair[int32, []byte]) {}, StreamColumn("id", StreamInt32, func(r *Pair[int32, []byte]) *int32 { return &r.First }), StreamColumn("value", StreamBlob, func(r *Pair[int32, []byte]) *[]byte { return &r.Second }))
	request := &attemptRequest{statement: "SELECT id,value FROM " + qualified, consistency: Quorum, pageSize: 2}
	seen := map[int32]bool{}
	pages := 0
	for {
		response, err := r.exchange(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if err = response.installColumns(response.columns); err != nil {
			t.Fatal(err)
		}
		plan := codecTestMust(decoder.Compile(response.columns))
		dst := Pair[int32, []byte]{Second: make([]byte, 0, size)}
		for {
			ok, err := decodeStreamRow(response, plan, &dst)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if dst.First < 0 || dst.First >= 4 || seen[dst.First] || len(dst.Second) != size || !bytes.Equal(dst.Second, bytes.Repeat([]byte{byte(dst.First + 1)}, size)) {
				t.Fatal("invalid streamed live row", dst.First, len(dst.Second))
			}
			seen[dst.First] = true
		}
		next := response.metadata.pagingState
		response.releaseMetadata()
		if next == nil {
			break
		}
		request.pagingState = next
	}
	if len(seen) != 4 || pages < 2 {
		t.Fatal("live paging incomplete", len(seen), pages)
	}
	if r.units != nil {
		t.Logf("bounded validated-unit storage=%d bytes copies=%d", r.units.units.StorageBytes(), r.units.copies)
		if r.units.units.StorageBytes() > r.limits.MaxTransformBytes {
			t.Fatal("unit budget exceeded")
		}
	}
	if err = r.close(true); err != nil {
		t.Fatal(err)
	}
	// Early abandonment discards the reserved slot, and ordinary bounded pool
	// repair restores healthy shared execution without growing its target.
	r, err = pool.reserveStream(ctx, Target{HostID: conn.host.hostUUID()}, RoutingInfo{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := r.exchange(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	response.releaseMetadata()
	if err = r.close(false); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(15 * time.Second)
	for {
		pool.mu.RLock()
		count, missing := pool.connPicker.Size()
		pool.mu.RUnlock()
		if count == 1 && missing == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("pool did not recover reserved slot")
		case <-time.After(10 * time.Millisecond):
		}
	}
	query := codecTestMust(BindUnpreparedOwned(Statement[Unit, int32]{CQL: "SELECT id FROM " + qualified + " LIMIT 1", Params: NoParams, Rows: OneColumn(Int32)}, NewColumns(), Unit{}))
	defer query.Close()
	rows := codecTestMust(Run(ctx, s, query, Options{}))
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("shared execution after repair", rows.Err())
	}
	t.Logf("live streaming proto=%d TLS=%v compression=%d rows=%d pages=%d", config.Protocol, *flagRunSslTest, config.Compression, len(seen), pages)
}
