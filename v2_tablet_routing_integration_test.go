//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"
)

func TestStep06TabletRoutingLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	config := createV2Cluster()
	v2ConfigureRouting(t, &config, RoutingOptions{TokenAware: true, LeaderAware: true, ShuffleReplicas: true})
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	supported, err := v2TabletsSupported(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Skip("server does not provide tablets")
	}
	keyspace := testKeyspaceName(t, fmt.Sprintf("step06_%x", time.Now().UnixNano()))
	if err := v2CreateTable(ctx, s, "CREATE KEYSPACE "+keyspace+" WITH replication={'class':'NetworkTopologyStrategy','replication_factor':2} AND tablets={'enabled':true}"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := v2CreateTable(cleanup, s, "DROP KEYSPACE "+keyspace); err != nil {
			t.Error(err)
		}
	}()
	if err := v2CreateTable(ctx, s, "CREATE TABLE "+keyspace+".records (pk int PRIMARY KEY, v int)"); err != nil {
		t.Fatal(err)
	}
	ks, _ := s.Metadata().Keyspace(keyspace)
	if !ks.TabletBased || ks.Tables["records"].ID == "" {
		t.Fatal("server tablet/table UUID metadata absent", ks)
	}
	insert := codecTestMust(Prepare(ctx, s, Statement[int32, Unit]{CQL: "INSERT INTO " + keyspace + ".records(pk,v) VALUES(?,7)", Params: OneParam(Int32), Rows: NoRows}, PrepareOptions{}))
	read := codecTestMust(Prepare(ctx, s, Statement[int32, DynamicRow]{CQL: "SELECT pk,v FROM " + keyspace + ".records WHERE pk=?", Params: OneParam(Int32), Rows: DynamicRows}, PrepareOptions{}))
	feedback := 0
	for key := int32(0); key < 8; key++ {
		bound := codecTestMust(BindBorrowed(insert, key))
		result, err := Exec(ctx, s, bound, Options{})
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range result.CustomPayload {
			if entry.Key == "tablets-routing-v1" || entry.Key == "tablets-routing-v2" {
				feedback++
			}
		}
		bound.Close()
	}
	// Read the server's authoritative tablet table independently of feedback.
	id := codecTestMust(ParseUUID(ks.Tables["records"].ID))
	serverRows, err := v2Query(ctx, s, "SELECT last_token,replicas FROM system.tablets WHERE table_id=?", UUIDValue(id))
	if err != nil {
		t.Fatal(err)
	}
	var authoritative []TokenRange
	for serverRows.Next() {
		row := serverRows.Row()
		last := codecTestMust(codecTestMust(row.Get("last_token")).Signed())
		list := codecTestMust(codecTestMust(row.Get("replicas")).Elements())
		r := TokenRange{End: last}
		for list.Next() {
			tuple := codecTestMust(list.Value().Elements())
			if !tuple.Next() {
				t.Fatal("missing host")
			}
			host := codecTestMust(tuple.Value().UUID())
			if !tuple.Next() {
				t.Fatal("missing shard")
			}
			shard := codecTestMust(tuple.Value().Signed())
			r.Replicas = append(r.Replicas, Target{HostID: host, ShardID: Option[uint32]{Value: uint32(shard), Set: true}})
		}
		if err := list.Err(); err != nil {
			t.Fatal(err)
		}
		authoritative = append(authoritative, r)
	}
	if err := serverRows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("initial authoritative tablet read-node=%s", serverRows.Info().HostID)
	serverRows.Close()
	if len(authoritative) == 0 {
		t.Fatal("no authoritative tablet records")
	}
	slices.SortFunc(authoritative, func(a, b TokenRange) int { return comparePlacementToken(a.End, b.End) })
	for i := range authoritative {
		authoritative[i].Start = math.MinInt64
		if i > 0 {
			authoritative[i].Start = authoritative[i-1].End
		}
	}
	v2 := implCluster.InternalSessionGetConn(s).InternalFramers.InternalDefaults.InternalTabletsRoutingV2
	t.Logf("tablet v2 negotiated=%t, server table ID=%s, authoritative ranges=%d", v2, id, len(authoritative))
	for key := int32(0); key < 8; key++ {
		b := codecTestMust(BindBorrowed(read, key))
		token := b.Routing().Token
		if !token.Set {
			t.Fatal("prepared partition key did not produce token")
		}
		var expected []Target
		for _, r := range authoritative {
			if token.Value > r.Start && token.Value <= r.End {
				expected = r.Replicas
				break
			}
		}
		if len(expected) != 2 {
			t.Fatal("server replica count", expected)
		}
		// Tablet placement may move after the initial independent read. Require
		// one exact, non-transitioning authority on every node before and after
		// each query; the initial map is diagnostic, not permanent authority.
		verified := 0
		var latest step17TabletObservation
		for attempt := 0; attempt < 96 && verified < 8; attempt++ {
			before := step17TabletAuthority(t, ctx, s, id, token.Value)
			opts := Options{}
			if attempt < 3 {
				opts.HostID = Option[UUID]{Value: routing.SnapshotData(s.Metadata()).InternalHosts[attempt%len(routing.SnapshotData(s.Metadata()).InternalHosts)].ID, Set: true}
			}
			rows, err := Run(ctx, s, b, opts)
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				t.Fatal("missing data", rows.Err())
			}
			if v2SchemaRoutingInt(t, rows.Row(), "pk") != key || v2SchemaRoutingInt(t, rows.Row(), "v") != 7 {
				t.Fatal("bad row")
			}
			result := rows.Info()
			for _, entry := range result.CustomPayload {
				if entry.Key == "tablets-routing-v1" || entry.Key == "tablets-routing-v2" {
					feedback++
					hint, parseErr := parseTypedTabletPayload(entry.Value.Value, keyspace, "records", entry.Key == "tablets-routing-v2")
					if parseErr != nil {
						t.Fatal("actual tablet feedback", parseErr)
					}
					t.Logf("key=%d token=%d attempt=%d actual feedback replicas=%s", key, token.Value, attempt, hint.Replicas())
				}
			}
			if rows.Next() {
				t.Fatal("extra row")
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			after := step17TabletAuthority(t, ctx, s, id, token.Value)
			latest = after
			replicas, err := s.Metadata().Replicas(keyspace, "records", token.Value)
			if err != nil {
				t.Fatal(err)
			}
			stable := before.stable && after.stable && before.last == after.last && step17SameReplicaSet(before.replicas, after.replicas)
			cacheMatches := len(after.replicas) == 2 && step17SameReplicaSet(replicas, after.replicas)
			coordinatorMatches := false
			for _, replica := range after.replicas {
				coordinatorMatches = coordinatorMatches || replica.HostID == result.HostID
			}
			// Preserve all three cold host pins, including the nonreplica, before
			// requiring eight consecutive unpinned exact-coordinator assertions.
			if attempt >= 3 && stable && cacheMatches && coordinatorMatches {
				verified++
			} else {
				verified = 0
				t.Logf("tablet observation key=%d token=%d attempt=%d initial=%+v before=%+v after=%+v driver=%+v coordinator=%s generation=%d", key, token.Value, attempt, expected, before, after, replicas, result.HostID, s.Metadata().Generation())
			}
			if verified < 8 {
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
		if verified != 8 {
			t.Fatalf("key %d did not converge on stable authoritative replicas and eight routed coordinators within 96 total attempts: latest=%+v", key, latest)
		}
		b.Close()
	}
	if feedback == 0 {
		t.Fatal("cold cache did not receive real tablet feedback")
	}
	t.Logf("accepted live feedback responses=%d", feedback)
}

func TestStep06LeaderRoutingLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	config := createV2Cluster()
	v2ConfigureRouting(t, &config, RoutingOptions{TokenAware: true, LeaderAware: true, ShuffleReplicas: true})
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !implCluster.InternalSessionGetConn(s).InternalFramers.InternalDefaults.InternalTabletsRoutingV2 {
		t.Skip("server did not advertise TABLETS_ROUTING_V2_EXPERIMENTAL; source-accurate wire fixtures cover V2 separately")
	}
	ks := testKeyspaceName(t, fmt.Sprintf("step06_leader_%x", time.Now().UnixNano()))
	if err := v2CreateTable(ctx, s, "CREATE KEYSPACE "+ks+" WITH replication={'class':'NetworkTopologyStrategy','replication_factor':3} AND tablets={'enabled':true} AND consistency='global'"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := v2CreateTable(cleanup, s, "DROP KEYSPACE "+ks); err != nil {
			t.Error(err)
		}
	}()
	if err := v2CreateTable(ctx, s, "CREATE TABLE "+ks+".records(pk int PRIMARY KEY,v int)"); err != nil {
		t.Fatal(err)
	}
	meta, _ := s.Metadata().Keyspace(ks)
	if !meta.TabletBased || meta.ConsistencyMode != "global" {
		t.Fatal("global tablet metadata absent", meta)
	}
	if _, err := v2Exec(ctx, s, "INSERT INTO "+ks+".records(pk,v) VALUES(1,7)"); err != nil {
		t.Fatal(err)
	}
	p := codecTestMust(Prepare(ctx, s, Statement[int32, DynamicRow]{CQL: "SELECT v FROM " + ks + ".records WHERE pk=?", Params: OneParam(Int32), Rows: DynamicRows}, PrepareOptions{}))
	b := codecTestMust(BindBorrowed(p, int32(1)))
	defer b.Close()
	var leader UUID
	stable := 0
	feedback := 0
	for attempt := 0; attempt < 256 && stable < 16; attempt++ {
		rows, err := Run(ctx, s, b, Options{Consistency: Option[Consistency]{Value: LocalQuorum, Set: true}})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatal("missing strong row", rows.Err())
		}
		if v2SchemaRoutingInt(t, rows.Row(), "v") != 7 {
			t.Fatal("bad strong row")
		}
		result := rows.Info()
		moved := false
		for _, entry := range result.CustomPayload {
			if entry.Key != "tablets-routing-v2" || !entry.Value.Valid {
				continue
			}
			raw := entry.Value.Value.OwnedBytes()
			if len(raw) < 64 {
				t.Fatal("short v2 leader payload")
			}
			copy(leader[:], raw[40:56])
			feedback++
			moved = true
			stable = 0
		}
		for rows.Next() {
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !leader.IsEmpty() && !moved {
			if result.HostID != leader {
				t.Fatalf("strong read coordinator %s differs from original v2 leader %s", result.HostID, leader)
			}
			stable++
		}
		if moved {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	if feedback == 0 || stable != 16 {
		t.Fatal("V2 leader did not converge", feedback, stable)
	}
	t.Logf("independently decoded leader=%s, feedback=%d, stable LOCAL_QUORUM reads=%d", leader, feedback, stable)
}

// step17TabletAuthority brackets routed queries with independent server reads.
// During a transition or disagreement it supplies no correctness credit.
type step17TabletObservation struct {
	last      int64
	replicas  []Target
	readNodes []UUID
	stable    bool
}

func step17TabletAuthority(t *testing.T, ctx context.Context, s *Session, table UUID, token int64) step17TabletObservation {
	t.Helper()
	type parameters struct {
		Table UUID
		Token int64
	}
	statement := Statement[parameters, DynamicRow]{
		CQL:    "SELECT last_token,replicas,new_replicas,stage FROM system.tablets WHERE table_id=? AND last_token>=? LIMIT 1",
		Params: Params(Param("id", UUIDCodec, func(p parameters) UUID { return p.Table }), Param("token", Int64, func(p parameters) int64 { return p.Token })),
		Rows:   DynamicRows,
	}
	columns := NewColumns(ColumnSpec{Name: "id", Type: codecTestMust(ScalarType(TypeUUID))}, ColumnSpec{Name: "token", Type: codecTestMust(ScalarType(TypeBigInt))})
	bound := codecTestMust(BindUnpreparedOwned(statement, columns, parameters{table, token}))
	defer bound.Close()
	observation := step17TabletObservation{stable: true}
	for index, host := range s.Metadata().Hosts() {
		page := codecTestMust(RunUnpaged(ctx, s, bound, Options{HostID: Option[UUID]{Set: true, Value: host.ID}}))
		if page.Info().HostID != host.ID {
			t.Fatal("authoritative read lost exact host pin", page.Info().HostID, host.ID)
		}
		observation.readNodes = append(observation.readNodes, page.Info().HostID)
		if !page.Next() {
			t.Fatal("authoritative tablet absent", page.Err())
		}
		row := page.Row()
		last := codecTestMust(codecTestMust(row.Get("last_token")).Signed())
		iterator := codecTestMust(codecTestMust(row.Get("replicas")).Elements())
		var replicas []Target
		for iterator.Next() {
			tuple := codecTestMust(iterator.Value().Elements())
			if !tuple.Next() {
				t.Fatal("authoritative replica missing host", tuple.Err())
			}
			id := codecTestMust(tuple.Value().UUID())
			if !tuple.Next() {
				t.Fatal("authoritative replica missing shard", tuple.Err())
			}
			shard := codecTestMust(tuple.Value().Signed())
			if shard < 0 || shard > math.MaxUint32 {
				t.Fatal("invalid authoritative shard", shard)
			}
			if tuple.Next() || tuple.Err() != nil {
				t.Fatal("authoritative replica extra member", tuple.Err())
			}
			replicas = append(replicas, Target{HostID: id, ShardID: Option[uint32]{Set: true, Value: uint32(shard)}})
		}
		if iterator.Err() != nil {
			t.Fatal(iterator.Err())
		}
		if codecTestMust(row.Get("new_replicas")).State() != Null || codecTestMust(row.Get("stage")).State() != Null {
			observation.stable = false
		}
		if page.Next() || page.Err() != nil {
			t.Fatal("authoritative query count", page.Err())
		}
		if err := page.Close(); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			observation.last, observation.replicas = last, replicas
		} else if last != observation.last || !step17SameReplicaSet(replicas, observation.replicas) {
			observation.stable = false
		}
	}
	if len(observation.readNodes) == 0 {
		t.Fatal("no authoritative read nodes")
	}
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	return observation
}
