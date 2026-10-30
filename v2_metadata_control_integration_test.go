//go:build integration

package gocql

import implExecution "github.com/scylladb/gocql/v2/internal/execution"

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestStep05MetadataControlLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	config := createV2Cluster(func(c *Config) {
		c.Metadata.InitialDiscovery = false
		c.Pool.ConnectionsPerHost = 1
		c.Pool.ShardAwarePort = false
	})
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	identities := map[UUID]bool{}
	for _, host := range s.Metadata().Hosts() {
		if host.ID.IsEmpty() || identities[host.ID] {
			t.Fatal("invalid configured contact identities", s.Metadata().Hosts())
		}
		identities[host.ID] = true
	}
	if len(identities) != len(config.ContactPoints) {
		t.Fatal("configured contacts collapsed", s.Metadata().Hosts())
	}
	if err := s.AwaitSchemaAgreement(ctx); err != nil {
		t.Fatal(err)
	}
	keyspace := fmt.Sprintf("step05_%x", time.Now().UnixNano())
	sub, err := s.Subscribe(EventFilter{Kinds: []EventKind{SchemaEvent}, Keyspace: keyspace, Capacity: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	before := s.Metadata() // subscribe-before-snapshot, then discard <= generation
	conn, err := implCluster.InternalSessionSelectTypedConn(s, ctx, RoutingInfo{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(cql string) {
		t.Helper()
		b, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		if _, err := Exec(ctx, s, b, *func() *Options {
			opaqueStorageValue := new(Options)
			implExecution.OptionsData(opaqueStorageValue).InternalConnection = conn
			return opaqueStorageValue
		}()); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE KEYSPACE " + keyspace + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		b, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "DROP KEYSPACE IF EXISTS " + keyspace, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if err == nil {
			defer b.Close()
			_, err = Exec(cleanup, s, b, *func() *Options {
				opaqueStorageValuex := new(Options)
				implExecution.OptionsData(opaqueStorageValuex).InternalConnection = conn
				return opaqueStorageValuex
			}())
		}
		if err != nil {
			t.Error("cleanup unique keyspace", err)
		}
	}()
	exec("CREATE TYPE " + keyspace + ".record (value text)")
	exec("CREATE TABLE " + keyspace + ".records (id int PRIMARY KEY,value frozen<record>)")
	if err := s.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := s.Metadata()
	schema, ok := snapshot.Keyspace(keyspace)
	if !ok || schema.Tables["records"] == nil || schema.Types["record"] == nil {
		t.Fatal("typed explicit refresh omitted schema", schema)
	}
	if _, ok := before.Keyspace(keyspace); ok {
		t.Fatal("retained prior snapshot mutated")
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	seen := false
	for !seen {
		select {
		case event, ok := <-sub.Events():
			if !ok {
				t.Fatal("unexpected event overflow")
			}
			if event.Generation <= before.Generation() {
				continue
			}
			if event.Target == "TABLE" && event.Object == "records" && event.Change == "CREATED" {
				current, ok := s.Metadata().Keyspace(keyspace)
				if !ok || current.Tables["records"] == nil {
					t.Fatal("event preceded snapshot publication", event)
				}
				seen = true
			}
		case <-deadline.C:
			t.Fatal("schema event not delivered")
		}
	}
	exec("DROP TABLE " + keyspace + ".records")
	if err := s.RefreshMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if current, _ := s.Metadata().Keyspace(keyspace); current.Tables["records"] != nil {
		t.Fatal("dropped table resurrected")
	}
	if retained, _ := snapshot.Keyspace(keyspace); retained.Tables["records"] == nil {
		t.Fatal("retained complete snapshot changed after drop")
	}
}
