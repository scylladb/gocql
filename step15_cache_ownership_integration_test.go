//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Real socket/error/transform owners use the session cache only after every
// page alias is released. Session close drains idle storage without invalidating
// retained delivered pages, and a late lease cannot repopulate the closed cache.
func TestStep15NativeCacheOwnership(t *testing.T) {
	config := createV2Cluster(func(c *Config) {
		c.Pool.ConnectionsPerHost = 1
		c.KeepaliveInterval = 0
		c.Metadata.PeriodicRefresh = 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s, e := Connect(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ks := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
	created := false
	exec := func(cql string) {
		t.Helper()
		b := codecTestMust(BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Kind: OrdinaryStatement, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}))
		defer b.Close()
		if _, e := Exec(ctx, s, b, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	exec("CREATE KEYSPACE " + ks + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	created = true
	defer func() {
		if !created {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		cleanupSession, e := Connect(cleanup, config)
		if e != nil {
			t.Error(e)
			return
		}
		defer cleanupSession.Close()
		b, e := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "DROP KEYSPACE " + ks, Kind: OrdinaryStatement, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if e == nil {
			defer b.Close()
			_, e = Exec(cleanup, cleanupSession, b, Options{})
		}
		if e != nil {
			t.Error(e)
		}
	}()
	exec("CREATE TABLE " + ks + ".records(id int PRIMARY KEY,value blob)")
	insert := codecTestMust(Prepare(ctx, s, Statement[[]byte, Unit]{CQL: "INSERT INTO " + ks + ".records(id,value)VALUES(1,?)", Kind: OrdinaryStatement, Params: OneParam(BytesEncoder), Rows: NoRows}, PrepareOptions{}))
	write := func(value byte) {
		t.Helper()
		payload := bytes.Repeat([]byte{value}, 1<<20)
		bound := codecTestMust(BindBorrowed(insert, payload))
		_, e := Exec(ctx, s, bound, Options{})
		bound.Close()
		if e != nil {
			t.Fatal(e)
		}
		if e = bound.Wait(ctx); e != nil {
			t.Fatal(e)
		}
	}
	write(0x71)
	prepared := codecTestMust(Prepare(ctx, s, Statement[Unit, Bytes]{CQL: "SELECT value FROM " + ks + ".records WHERE id=1", Kind: OrdinaryStatement, Params: NoParams, Rows: OneColumn(BlobCodec)}, PrepareOptions{}))
	bound := codecTestMust(BindOwned(prepared, Unit{}))
	defer bound.Close()
	rows := codecTestMust(Run(ctx, s, bound, Options{Prefetch: Option[int]{Set: true, Value: 0}}))
	var old Bytes
	if !rows.NextInto(&old) {
		t.Fatal(rows.Err())
	}
	lease := codecTestMust(rows.Retain())
	defer lease.Close()
	owner := cqltype.BytesLease(old)
	if owner == nil {
		t.Fatal("missing physical owner")
	}
	rows.Close()
	firstAddress := uintptr(0)
	cqltype.BytesChain(old).Each(func(span wire.Span) error {
		if firstAddress == 0 && span.Len() > 0 {
			firstAddress = uintptr(unsafe.Pointer(unsafe.SliceData(span.Bytes())))
		}
		return nil
	})
	for generation := 0; generation < 8; generation++ {
		write(byte(0x72 + generation))
		freshRows := codecTestMust(Run(ctx, s, bound, Options{Prefetch: Option[int]{Set: true, Value: 0}}))
		var view Bytes
		if !freshRows.NextInto(&view) {
			t.Fatal(freshRows.Err())
		}
		cqltype.BytesChain(view).Each(func(span wire.Span) error {
			if span.Len() > 0 && uintptr(unsafe.Pointer(unsafe.SliceData(span.Bytes()))) == firstAddress {
				t.Fatal("live retained backing reused")
			}
			return nil
		})
		if got := codecTestMust(view.At(0)); got != byte(0x72+generation) {
			t.Fatal(got)
		}
		freshRows.Close()
		if got := codecTestMust(old.At(0)); got != 0x71 {
			t.Fatal("retained old page changed", got)
		}
	}
	clone := old.Clone()
	cache := implCluster.SessionData(s).InternalConnCfg.InternalReceiveCache
	if n, c := cache.Storage(); n > wire.ReceiveCacheMaxBytes || c > wire.ReceiveCacheMaxObjects {
		t.Fatal("idle cache bound", n, c)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if !owner.Alive() || codecTestMust(old.At(0)) != 0x71 {
		t.Fatal("session close invalidated delivered owner")
	}
	if n, c := cache.Storage(); n != 0 || c != 0 {
		t.Fatal("session cache did not drain", n, c)
	}
	lease.Close()
	if owner.Alive() {
		t.Fatal("owner final release did not finish")
	}
	if _, e = old.At(0); !errors.Is(e, ErrReleased) {
		t.Fatal("old backing remained accessible", e)
	}
	if n, c := cache.Storage(); n != 0 || c != 0 {
		t.Fatal("late lease repopulated closed cache", n, c)
	}
	if codecTestMust(clone.At(0)) != 0x71 {
		t.Fatal("Clone lost independent bytes")
	}
	t.Log("8 overwritten real responses preserve old retained backing; Close drains idle cache, late final lease cannot repopulate; Clone independent")
}
