//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// Check defaults and explicit overrides through query construction, prepared
// execution and BATCH encoding, without needing a database.
func TestApplicationConsistencyDefaults(t *testing.T) {
	t.Parallel()

	const insert = "INSERT INTO ks.tbl (id) VALUES (1)"
	const selectStmt = "SELECT id FROM ks.tbl"
	for _, tc := range []struct {
		name             string
		clusterOverride  bool
		requestOverride  bool
		cons, serialCons Consistency
	}{
		{name: "defaults", cons: LocalQuorum, serialCons: LocalSerial},
		{name: "cluster overrides", clusterOverride: true, cons: Quorum, serialCons: Serial},
		{name: "omit serial consistency", clusterOverride: true, cons: Quorum},
		{name: "request overrides", requestOverride: true, cons: One, serialCons: Serial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newBatchTestServer(t, map[string]testPrepared{
				insert: {id: 42}, selectStmt: {id: 43},
			}, nil, nil)
			cfg := testCluster(protoVersion4, srv.Address)
			cfg.DefaultTimestamp = false
			if tc.clusterOverride {
				cfg.Consistency, cfg.SerialConsistency = tc.cons, tc.serialCons
			}
			session, err := cfg.CreateSession()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Close)

			type levels struct {
				cons, serial Consistency
				hasSerial    bool
			}
			want := levels{cons: tc.cons, serial: tc.serialCons, hasSerial: tc.serialCons != 0}
			received := make(chan levels, 2)
			srv.setExecuteHook(func(_ uint64, req *framer, stream int, resp *framer) bool {
				got := levels{cons: req.readConsistency()}
				flags := uint32(req.readByte())
				if flags&frm.FlagPageSize != 0 {
					req.readInt()
				}
				got.hasSerial = flags&frm.FlagWithSerialConsistency != 0
				if got.hasSerial {
					got.serial = req.readConsistency()
				}
				received <- got
				writeVoidResult(resp, stream)
				return true
			})
			for _, statement := range []string{selectStmt, insert} {
				q := session.Query(statement)
				if tc.requestOverride {
					q.Consistency(tc.cons).SerialConsistency(tc.serialCons)
				}
				if q.GetConsistency() != tc.cons || q.serialCons != tc.serialCons {
					t.Fatalf("query levels = %v/%v, want %v/%v", q.GetConsistency(), q.serialCons, tc.cons, tc.serialCons)
				}
				if err := q.Exec(); err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-received:
					if got != want {
						t.Fatalf("EXECUTE levels = %+v, want %+v", got, want)
					}
				default:
					t.Fatal("query succeeded without an EXECUTE request")
				}
				if got := srv.prepareCount(statement); got != 1 {
					t.Fatalf("PREPARE count = %d, want 1", got)
				}
			}

			batch := session.Batch(UnloggedBatch)
			if tc.requestOverride {
				batch.SetConsistency(tc.cons)
				batch.SerialConsistency(tc.serialCons)
			}
			batch.Query(insert)
			if err := batch.Exec(); err != nil {
				t.Fatal(err)
			}
			batches := srv.received()
			if len(batches) != 1 {
				t.Fatalf("BATCH count = %d, want 1", len(batches))
			}
			got := levels{cons: batches[0].cons, serial: batches[0].serialCons, hasSerial: batches[0].hasSerialCons}
			if got != want {
				t.Fatalf("BATCH levels = %+v, want %+v", got, want)
			}
		})
	}
}
