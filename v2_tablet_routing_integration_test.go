//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
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
	keyspace := fmt.Sprintf("step06_%x", time.Now().UnixNano())
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
	v2 := s.getConn().framers.defaults.tabletsRoutingV2
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
		learned := false
		for attempt := 0; attempt < 96; attempt++ {
			opts := Options{}
			if attempt < 3 {
				opts.HostID = Option[UUID]{Value: s.Metadata().hosts[attempt%len(s.Metadata().hosts)].ID, Set: true}
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
				}
			}
			if rows.Next() {
				t.Fatal("extra row")
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			replicas, err := s.Metadata().Replicas(keyspace, "records", token.Value)
			if err != nil {
				t.Fatal(err)
			}
			if len(replicas) == len(expected) {
				matches := true
				for _, r := range replicas {
					if !slices.Contains(expected, r) {
						matches = false
					}
				}
				if matches {
					learned = true
					break
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
		if !learned {
			t.Fatalf("key %d never learned authoritative replicas", key)
		}
		for i := 0; i < 8; i++ {
			rows, err := Run(ctx, s, b, Options{})
			if err != nil {
				t.Fatal(err)
			}
			result := rows.Info()
			found := false
			for _, r := range expected {
				if r.HostID == result.HostID {
					found = true
				}
			}
			if !found {
				t.Fatal("coordinator not in authoritative replica set", result.HostID, expected)
			}
			for rows.Next() {
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
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
	if !s.getConn().framers.defaults.tabletsRoutingV2 {
		t.Skip("server did not advertise TABLETS_ROUTING_V2_EXPERIMENTAL; source-accurate wire fixtures cover V2 separately")
	}
	ks := fmt.Sprintf("step06_leader_%x", time.Now().UnixNano())
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
