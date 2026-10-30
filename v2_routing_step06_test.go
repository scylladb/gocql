// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import implExecution "github.com/scylladb/gocql/v2/internal/execution"

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/tablets"
)

func step06Hint(t testing.TB, first, last int64, version Option[uint64], replicas ...Target) tablets.TabletInfo {
	t.Helper()
	rs := make([]tablets.ReplicaInfo, len(replicas))
	for i, r := range replicas {
		rs[i] = tablets.NewReplicaInfo(tablets.HostUUID(r.HostID), int(r.ShardID.Value))
	}
	h, err := tablets.NewTabletInfo("ks", "table", first, last, rs)
	if err != nil {
		t.Fatal(err)
	}
	if version.Set {
		h = h.WithVersion(version.Value)
	}
	return h
}
func step06Payload(h tablets.TabletInfo) []byte {
	field := func(dst, b []byte) []byte { return append(binary.BigEndian.AppendUint32(dst, uint32(len(b))), b...) }
	var b []byte
	b = field(b, binary.BigEndian.AppendUint64(nil, uint64(h.FirstToken())))
	b = field(b, binary.BigEndian.AppendUint64(nil, uint64(h.LastToken())))
	list := binary.BigEndian.AppendUint32(nil, uint32(len(h.ReplicasUnsafe())))
	for _, r := range h.ReplicasUnsafe() {
		id := r.HostUUIDValue()
		tuple := field(nil, id[:])
		tuple = field(tuple, binary.BigEndian.AppendUint32(nil, uint32(r.ShardID())))
		list = field(list, tuple)
	}
	b = field(b, list)
	if v, ok := h.Version(); ok {
		b = field(b, binary.BigEndian.AppendUint64(nil, v))
	}
	return b
}
func TestStep06TabletPayloadAndWire(t *testing.T) {
	target := Target{HostID: UUID{15: 7}, ShardID: Option[uint32]{Value: math.MaxInt32, Set: true}}
	for _, v2 := range []bool{false, true} {
		h := step06Hint(t, math.MinInt64, math.MaxInt64, Option[uint64]{Value: 0xfedcba9876543210, Set: v2}, target)
		raw := step06Payload(h)
		for split := 0; split <= len(raw); split++ {
			chain := typedTestChain(t, raw[:split], raw[split:])
			parsed, err := parseTypedTabletPayload(cqltype.BytesView(chain, nil), "ks", "table", v2)
			if err != nil || !sameTabletObservation(&tabletRange{InternalSpan: TokenRange{Start: h.FirstToken(), End: h.LastToken(), Replicas: []Target{target}}, InternalVersion: Option[uint64]{Value: 0xfedcba9876543210, Set: v2}}, parsed) {
				t.Fatalf("v2=%t split=%d: %v", v2, split, err)
			}
		}
		for n := 0; n < len(raw); n++ {
			if _, err := parseTypedTabletPayload(BorrowBytes(raw[:n]), "ks", "table", v2); err == nil {
				t.Fatalf("accepted truncated prefix %d v2=%t", n, v2)
			}
		}
		for _, offset := range []int{4, 16} {
			bad := append([]byte(nil), raw...)
			copy(bad[offset:offset+8], raw[4:12])
			if _, err := parseTypedTabletPayload(BorrowBytes(bad), "ks", "table", v2); offset == 16 && err == nil {
				t.Fatal("accepted empty range")
			}
		}
		bad := append([]byte(nil), raw...)
		binary.BigEndian.PutUint32(bad[60:64], math.MaxUint32)
		if _, err := parseTypedTabletPayload(BorrowBytes(bad), "ks", "table", v2); err == nil {
			t.Fatal("accepted negative shard")
		}
	}
	for _, proto := range []byte{3, 4, 5} {
		for _, kind := range []string{"query", "execute", "batch", "prepare"} {
			c := &driverConn{InternalVersion: proto}
			c.InternalInitFramerCache()
			r := &attemptRequest{Statement: "SELECT * FROM ks.table", Consistency: One, TabletVersionBlock: 0xa7}
			switch kind {
			case "execute":
				r.PreparedID = []byte{1}
				r.ResultMetadataID = []byte{2}
			case "prepare":
				r.Prepare = true
			case "batch":
				r.Batch = &typedBatchRequest{Kind: LoggedBatch}
			}
			flatten := func(chain wire.Chain) []byte {
				raw := typedTestBytes(t, chain)
				if proto < 5 {
					return raw
				}
				reader := bytes.NewReader(raw)
				header, err := segment.ReadHeader(reader, false)
				if err != nil {
					t.Fatal(err)
				}
				p, err := segment.ReadOwnedPayload(reader, header, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer p.Release()
				return typedTestBytes(t, p.Chain())
			}
			base, err := c.InternalBuildTypedRequest(r, 23)
			if err != nil {
				t.Fatal(err)
			}
			plain := flatten(base)
			c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2 = true
			extended, err := c.InternalBuildTypedRequest(r, 23)
			if err != nil {
				t.Fatal(err)
			}
			got := flatten(extended)
			if kind == "execute" {
				if len(got) != len(plain)+1 || got[len(got)-1] != 0xa7 || !bytes.Equal(got[9:len(got)-1], plain[9:]) || binary.BigEndian.Uint32(got[5:9]) != binary.BigEndian.Uint32(plain[5:9])+1 {
					t.Fatalf("v%d execute block malformed", proto)
				}
			} else if !bytes.Equal(got, plain) {
				t.Fatalf("v%d %s gained version block", proto, kind)
			}
		}
	}
}

func step06Session(t testing.TB) *Session {
	t.Helper()
	s := func() *Session {
		opaqueStorageValue := new(Session)
		return opaqueStorageValue
	}()
	implCluster.SessionData(s).InternalMetadataDescriber = newMetadataDescriber(s)
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].Tables["table"].ID = "identity-1"
	routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = true
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(m).InternalKeyspaces["ks"])
	p := implCluster.InternalSessionPeripheral(s)
	p.InternalFeedbackOnce.Do(func() {})
	routing.SnapshotData(m).InternalPlacementSignal = &p.InternalPlacementEpoch
	routing.SnapshotData(m).InternalPlacements = placementPut(routing.SnapshotData(m).InternalPlacements, metadataTable{InternalKeyspace: "ks", InternalTable: "table"}, tabletPlacement{InternalIdentity: "identity-1"}, comparePlacementTable)
	p.InternalSnapshot.Store(m)
	t.Cleanup(func() {
		implCluster.InternalSessionCloseV2Peripheral(s)
		implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
	})
	return s
}
func step06Drain(s *Session) {
	p := implCluster.InternalSessionPeripheral(s)
	p.InternalFeedbackMu.Lock()
	updates := p.InternalFeedback
	p.InternalFeedback = make(map[v2TabletFeedbackKey]v2PendingTabletFeedback)
	resync := p.InternalFeedbackResync
	p.InternalFeedbackResync = false
	p.InternalFeedbackMu.Unlock()
	implCluster.InternalSessionPublishTabletFeedback(s, p, updates, resync)
}
func TestStep06PlacementFencesSharingAndOverflow(t *testing.T) {
	s := step06Session(t)
	p := implCluster.InternalSessionPeripheral(s)
	info := RoutingInfo{Keyspace: "ks", Table: "table"}
	first := s.Metadata()
	stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, first)
	h := step06Hint(t, 0, 100, Option[uint64]{Value: 7, Set: true}, Target{HostID: routing.SnapshotData(first).InternalHosts[0].ID, ShardID: Option[uint32]{Set: true}})
	implCluster.InternalSessionQueueTabletFeedback(s, h, stamp)
	step06Drain(s)
	a := s.Metadata()
	if a == first || routing.SnapshotData(a).InternalHostIndex != routing.SnapshotData(first).InternalHostIndex || routing.SnapshotData(a).InternalKeyspaces["ks"] != routing.SnapshotData(first).InternalKeyspaces["ks"] {
		t.Fatal("placement rebuilt host/schema graph")
	}
	if root, _ := routing.InternalMetadataSnapshotTabletRoot(first, metadataTable{InternalKeyspace: "ks", InternalTable: "table"}); root != nil {
		t.Fatal("mutated retained snapshot")
	}
	implCluster.InternalSessionQueueTabletFeedback(s, h, implCluster.InternalSessionCaptureTabletFeedback(s, info, a))
	step06Drain(s)
	if s.Metadata() != a {
		t.Fatal("duplicate advanced snapshot")
	}
	// A numerically smaller opaque version is a valid update. The stale request
	// captured before the first update cannot overwrite its overlapping range.
	moved := step06Hint(t, 0, 100, Option[uint64]{Value: 1, Set: true}, Target{HostID: routing.SnapshotData(first).InternalHosts[1].ID, ShardID: Option[uint32]{Value: 1, Set: true}})
	implCluster.InternalSessionQueueTabletFeedback(s, moved, stamp)
	step06Drain(s)
	if s.Metadata() != a {
		t.Fatal("stale response overwrote newer range")
	}
	implCluster.InternalSessionQueueTabletFeedback(s, moved, implCluster.InternalSessionCaptureTabletFeedback(s, info, a))
	step06Drain(s)
	b := s.Metadata()
	r, _ := routing.InternalMetadataSnapshotTabletRoot(b, metadataTable{InternalKeyspace: "ks", InternalTable: "table"})
	if n := tabletRangeFor(r, 1); n == nil || n.InternalValue.InternalVersion.Value != 1 {
		t.Fatal("ordered opaque versions numerically")
	}
	// Disjoint observations captured together merge, preserving the old record.
	left := implCluster.InternalSessionCaptureTabletFeedback(s, info, b)
	for _, bounds := range [][2]int64{{100, 200}, {200, 300}} {
		implCluster.InternalSessionQueueTabletFeedback(s, step06Hint(t, bounds[0], bounds[1], Option[uint64]{}, hTarget(3, 2)), left)
	}
	step06Drain(s)
	c := s.Metadata()
	newRoot, _ := routing.InternalMetadataSnapshotTabletRoot(c, metadataTable{InternalKeyspace: "ks", InternalTable: "table"})
	if tabletRangeFor(newRoot, 1).InternalValue != tabletRangeFor(r, 1).InternalValue || tabletRangeFor(newRoot, 150) == nil || tabletRangeFor(newRoot, 250) == nil {
		t.Fatal("disjoint ranges were copied or lost")
	}
	implCluster.
		// A split removes the old overlapping range entirely; adjacent ranges survive.
		InternalSessionQueueTabletFeedback(s, step06Hint(t, 10, 50, Option[uint64]{}, hTarget(2, 1)), implCluster.InternalSessionCaptureTabletFeedback(s, info, c))
	step06Drain(s)
	root, _ := routing.InternalMetadataSnapshotTabletRoot(s.Metadata(), metadataTable{InternalKeyspace: "ks", InternalTable: "table"})
	if tabletRangeFor(root, 1) != nil || tabletRangeFor(root, 51) != nil || tabletRangeFor(root, 150) == nil {
		t.Fatal("split retained old fragments or removed adjacent range")
	}
	beforeEpoch := s.Metadata()
	oldStamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, beforeEpoch)
	lazy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true})).Plan(RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 20, Set: true}}, beforeEpoch)

	p.InternalMu.Lock()
	p.InternalTableRevision[metadataTable{InternalKeyspace: "ks", InternalTable: "table"}]++
	p.InternalMu.Unlock()
	implCluster.InternalSessionQueueTabletFeedback(s, h, oldStamp)
	step06Drain(s)
	if s.Metadata() != beforeEpoch {
		t.Fatal("table epoch fence failed")
	}
	// Even before snapshot publication, a source drop/recreate rejects old replies.
	before := s.Metadata()
	sourceStamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, before)
	dropped := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGet()["ks"].Clone()
	delete(dropped.Tables, "table")
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", dropped)
	implCluster.InternalSessionQueueTabletFeedback(s, h, sourceStamp)
	step06Drain(s)
	if s.Metadata() != before {
		t.Fatal("dropped source table resurrected")
	}
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(before).InternalKeyspaces["ks"])
	ks := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalGet()["ks"]
	ks = ks.Clone()
	table := *ks.Tables["table"]
	table.ID = "identity-2"
	ks.Tables["table"] = &table
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", ks)
	implCluster.InternalSessionQueueTabletFeedback(s, h, sourceStamp)
	step06Drain(s)
	if s.Metadata() != before {
		t.Fatal("source identity fence failed")
	}
	implCluster.
		// Restore the known source, then saturation invalidates immediately, before
		// a worker can publish, with bounded queue ownership and later relearning.
		MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", routing.SnapshotData(before).InternalKeyspaces["ks"])
	for i := 0; i < 1025; i++ {
		implCluster.InternalSessionQueueTabletFeedback(s, step06Hint(t, int64(i*2), int64(i*2+1), Option[uint64]{}, hTarget(1, 0)), implCluster.InternalSessionCaptureTabletFeedback(s, info, before))
	}
	if target, ok := lazy.Next(); !ok || target.ShardID.Set {
		t.Fatal("preexisting plan used invalidated placement", target)
	}
	if replicas, _ := before.Replicas("ks", "table", 150); len(replicas) != 0 {
		t.Fatal("overflow retained known-stale mapping")
	}
	p.InternalFeedbackMu.Lock()
	if len(p.InternalFeedback) > 1024 {
		t.Fatal("unbounded queue")
	}
	for _, u := range p.InternalFeedback {
		if u.InternalStamp.InternalSnapshot != nil {
			t.Fatal("queue retained cluster snapshot")
		}
	}
	p.InternalFeedbackMu.Unlock()
	step06Drain(s)
	implCluster.InternalSessionQueueTabletFeedback(s, h, implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata()))
	step06Drain(s)
	if got, _ := s.Metadata().Replicas("ks", "table", 1); len(got) != 1 {
		t.Fatal("overflow could not relearn")
	}
}
func hTarget(id byte, shard uint32) Target {
	return Target{HostID: UUID{15: id}, ShardID: Option[uint32]{Value: shard, Set: true}}
}

func TestStep06LeaderLocalityLWTAndFallback(t *testing.T) {
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
	k := metadataTable{InternalKeyspace: "ks", InternalTable: "table"}
	set := func(targets ...Target) {
		h := step06Hint(t, 0, 100, Option[uint64]{Value: 9, Set: true}, targets...)
		r, _ := updateTabletRange(nil, h)
		routing.SnapshotData(m).InternalPlacements = placementPut(routing.SnapshotData(m).InternalPlacements, k, tabletPlacement{InternalRanges: r}, comparePlacementTable)
	}
	set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3))
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 1, Set: true}, Consistency: LocalQuorum, Conditional: true}
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, LeaderAware: true, Datacenter: "a", Rack: "r1", AllowRemote: true, ShuffleReplicas: true}))
	for i := 0; i < 10; i++ {
		plan := policy.Plan(info, m)
		first, ok := plan.Next()
		if !ok || first.HostID != routing.SnapshotData(m).InternalHosts[2].ID || !first.StrictShard || first.ShardID.Value != 1 {
			t.Fatal("eligible remote leader was not strict first", first)
		}
		if got := v2PlanIDs(plan); !bytes.Equal(got, []byte{1, 2, 4}) {
			t.Fatal("LWT locality order", got)
		}
	}
	for _, test := range []struct {
		name    string
		change  func()
		want    byte
		restore func()
	}{
		{"one", func() { info.Consistency = One }, 1, func() { info.Consistency = LocalQuorum }},
		{"local-one", func() { info.Consistency = LocalOne }, 1, func() { info.Consistency = LocalQuorum }},
		{"unknown-original-leader", func() { set(hTarget(99, 1), hTarget(2, 2), hTarget(1, 3)) }, 1, func() { set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3)) }},
		{"disconnected-leader", func() { routing.SnapshotData(m).InternalHosts[2].Connected = false }, 1, func() { routing.SnapshotData(m).InternalHosts[2].Connected = true }},
		{"disabled-leader", func() { routing.SnapshotData(m).InternalHosts[2].Enabled = false }, 1, func() { routing.SnapshotData(m).InternalHosts[2].Enabled = true }},
		{"invalid-leader-shard", func() { set(hTarget(3, 999), hTarget(2, 2), hTarget(1, 3)) }, 1, func() { set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.change()
			defer test.restore()
			first, _ := policy.Plan(info, m).Next()
			if first.HostID[15] != test.want {
				t.Fatal(first)
			}
		})
	}
	local := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, LeaderAware: true, Datacenter: "a", Rack: "r1"}))
	if ids := v2PlanIDs(local.Plan(info, m)); bytes.Contains(ids, []byte{3}) {
		t.Fatal("excluded remote leader admitted", ids)
	}
	// Explicit latency penalties apply to leaders and LWT without admitting hosts.
	p := policy.(*routingPolicy)
	p.InternalDecisions.Store(&routingDecisions{InternalSlow: map[UUID]time.Time{routing.SnapshotData(m).InternalHosts[2].ID: time.Now().Add(time.Hour)}})
	if got := v2PlanIDs(p.Plan(info, m)); !bytes.Equal(got, []byte{1, 2, 4, 3}) {
		t.Fatal("penalized leader order", got)
	}
	// Preserve distinct explicit shards, but suppress host-only fallback duplicates.
	p.InternalDecisions.Store(nil)
	set(hTarget(1, 0), hTarget(1, 1), hTarget(2, 2))
	ids := v2PlanIDs(p.Plan(info, m))
	if !bytes.Equal(ids, []byte{1, 1, 2, 4, 3}) {
		t.Fatal("shard destinations deduplicated by host", ids)
	}
}
func TestStep06ConcurrentPlansAndFeedback(t *testing.T) {
	s := step06Session(t)
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 1, Set: true}, Conditional: true}
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, ShuffleReplicas: true}))
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				m := s.Metadata()
				p := policy.Plan(info, m)
				_ = v2PlanIDs(p)
				_ = routing.InternalMetadataSnapshotTabletVersionBlock(m, info)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		implCluster.InternalSessionQueueTabletFeedback(s, step06Hint(t, 0, 100, Option[uint64]{Value: uint64(i), Set: true}, hTarget(byte(i%3+1), 0)), implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata()))
		step06Drain(s)
	}
	wg.Wait()
}
func TestStep06PickerStrictAndAdvisory(t *testing.T) {
	options := DefaultConfig("localhost").Pool
	makeConn := func(shard int) *driverConn {
		return &driverConn{InternalScyllaSupported: *func() *ScyllaConnectionFeatures {
			opaqueStorageValuexx := new(ScyllaConnectionFeatures)
			opaqueStorageValuexx.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
				opaqueStorageValuex := new(ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalNrShards = 2
				return opaqueStorageValuex
			}()
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexx).InternalShard = shard
			return opaqueStorageValuexx
		}(), InternalStreams: streams.New()}
	}
	a, b := makeConn(0), makeConn(1)
	p := codecTestMust(newTypedConnPicker(a, options))
	_ = p.Put(a)
	_ = p.Put(b)
	a.InternalExclusiveFlag.Store(true)
	if got := p.InternalPickTarget(Target{ShardID: Option[uint32]{Value: 0, Set: true}}, RoutingInfo{}); got != b {
		t.Fatal("advisory hint could not use free capacity")
	}
	if got := p.InternalPickTarget(Target{StrictShard: true, ShardID: Option[uint32]{Value: 0, Set: true}}, RoutingInfo{}); got != a {
		t.Fatal("strict target relaxed while reserved")
	}
	if got := p.InternalPickTarget(Target{StrictShard: true, ShardID: Option[uint32]{Value: math.MaxUint32, Set: true}}, RoutingInfo{}); got != nil {
		t.Fatal("invalid strict target relaxed")
	}
}

func FuzzStep06TabletPayload(f *testing.F) {
	h, _ := tablets.NewTabletInfo("ks", "table", 0, 100, []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{1}, 0)})
	f.Add(step06Payload(h.WithVersion(7)), true)
	f.Fuzz(func(t *testing.T, data []byte, v2 bool) {
		h, err := parseTypedTabletPayload(BorrowBytes(data), "ks", "table", v2)
		if err == nil {
			if h.FirstToken() >= h.LastToken() {
				t.Fatal("invalid range")
			}
			for _, r := range h.ReplicasUnsafe() {
				if r.ShardID() < 0 {
					t.Fatal("invalid shard")
				}
			}
		}
	})
}

func TestStep06NegotiationVersionAndFailedExecuteFeedback(t *testing.T) {
	for _, both := range []bool{false, true} {
		supported := map[string][]string{tabletsRoutingV1: {}}
		if both {
			supported[tabletsRoutingV2] = []string{}
		}
		exts := typedTabletExtensions(parseCQLProtocolExtensions(supported, 4, nopLogger{}), supported)
		c := &driverConn{InternalVersion: 4, InternalCqlProtoExts: exts}
		c.InternalInitFramerCache()
		if c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2 != both || c.InternalFramers.InternalDefaults.InternalTabletsRoutingV1 == both {
			t.Fatal("V2 did not take precedence")
		}
		for _, ext := range exts {
			if _, ok := ext.InternalSerialize()[tabletsRoutingV1]; ok && both {
				t.Fatal("both extensions negotiated")
			}
		}
	}
	s := step06Session(t)
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true, Value: 1}}
	h := step06Hint(t, 0, 100, Option[uint64]{Set: true, Value: 0xfedcba9876543210}, hTarget(1, 1))
	stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata())
	// applyTypedFeedbackStamped is deliberately before execute-error/UNPREPARED
	// interpretation; a response page can carry payload independently of rows.
	page := &responsePage{InternalResponseData: responseData{Payload: []PayloadEntry{{Key: "tablets-routing-v2", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes(step06Payload(h))}}}}}
	implCluster.InternalSessionApplyTypedFeedbackStamped(s, page, info, stamp, framerConfig{InternalTabletsRoutingV1: true, InternalTabletsRoutingV2: true})
	step06Drain(s)
	m := s.Metadata()
	for i := 0; i < 1024; i++ {
		b := routing.InternalMetadataSnapshotTabletVersionBlock(m, info)
		index := b >> 4
		if b&15 != byte(uint64(0xfedcba9876543210)>>uint(index*4)&15) {
			t.Fatal("wrong nibble direction", b)
		}
	}
	if b := routing.InternalMetadataSnapshotTabletVersionBlock(m, RoutingInfo{}); b != 0 {
		t.Fatal("absent identity/token hint is not zero")
	}
	// A malformed V2 body never falls back to the simultaneously supplied V1.
	page.Payload = append([]PayloadEntry{{Key: "tablets-routing-v1", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes(step06Payload(step06Hint(t, 100, 200, Option[uint64]{}, hTarget(2, 0))))}}}, page.Payload...)
	page.Payload[1].Value.Value = BorrowBytes([]byte{0})
	implCluster.InternalSessionApplyTypedFeedbackStamped(s, page, info, implCluster.InternalSessionCaptureTabletFeedback(s, info, m), framerConfig{InternalTabletsRoutingV1: true, InternalTabletsRoutingV2: true})
	step06Drain(s)
	if s.Metadata() != m {
		t.Fatal("malformed V2 fell back to V1")
	}
}
func TestStep06VnodeDistinctNodesAndRackBudget(t *testing.T) {
	hosts := []*HostInfo{func() *HostInfo {
		opaqueStorageValuexxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalHostId = UUID{15: 1}
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalDataCenter = "a"
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalRack = "r1"
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalTokens = []string{"0", "1"}
		return opaqueStorageValuexxx
	}(), func() *HostInfo {
		opaqueStorageValuexxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalHostId = UUID{15: 2}
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalDataCenter = "a"
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalRack = "r1"
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalTokens = []string{"2"}
		return opaqueStorageValuexxxx
	}(), func() *HostInfo {
		opaqueStorageValuexxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalHostId = UUID{15: 3}
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalDataCenter = "a"
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalRack = "r2"
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalTokens = []string{"3"}
		return opaqueStorageValuexxxxx
	}(), func() *HostInfo {
		opaqueStorageValuexxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalHostId = UUID{15: 4}
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalDataCenter = "a"
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalRack = "r3"
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalTokens = []string{"4"}
		return opaqueStorageValuexxxxxx
	}(), func() *HostInfo {
		opaqueStorageValuexxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalHostId = UUID{15: 5}
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalDataCenter = "b"
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalRack = "r1"
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalTokens = []string{"5"}
		return opaqueStorageValuexxxxxxx
	}(),
	}
	ring := codecTestMust(newTokenRing("Murmur3Partitioner", hosts))
	for _, test := range []struct {
		strategy string
		options  map[string]any
		want     []byte
	}{
		{"NetworkTopologyStrategy", map[string]any{"a": "4", "b": "1"}, []byte{1, 2, 3, 4, 5}},
		{"NetworkTopologyStrategy", map[string]any{"a": "3"}, []byte{1, 3, 4}},
		{"SimpleStrategy", map[string]any{"replication_factor": "999999999999999999"}, []byte{1, 2, 3, 4, 5}},
		{"SimpleStrategy", map[string]any{"replication_factor": "0"}, nil},
		{"UnknownStrategy", nil, []byte{1}},
	} {
		ranges, err := buildV2VnodeRanges(ring, &KeyspaceMetadata{StrategyClass: test.strategy, StrategyOptions: test.options})
		if err != nil {
			t.Fatal(err)
		}
		var ids []byte
		for _, r := range ranges[0].Replicas {
			ids = append(ids, r.HostID[15])
		}
		if !bytes.Equal(ids, test.want) {
			t.Fatal(test.strategy, ids, test.want)
		}
	}
}
func TestStep06AllDisconnectedAndMissingFollowers(t *testing.T) {
	m := v2TestMetadata()
	for i := range routing.SnapshotData(m).InternalHosts {
		routing.SnapshotData(m).InternalHosts[i].Connected = false
	}
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, Datacenter: "a"}))
	if ids := v2PlanIDs(policy.Plan(RoutingInfo{Kind: OrdinaryStatement}, m)); !bytes.Equal(ids, []byte{2, 4, 1}) {
		t.Fatal("enabled disconnected fallback", ids)
	}
	m = v2TestMetadata()
	routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
	h := step06Hint(t, 0, 100, Option[uint64]{Set: true}, hTarget(2, 1), hTarget(99, 2), hTarget(1, 1))
	r, _ := updateTabletRange(nil, h)
	routing.SnapshotData(m).InternalPlacements = placementPut(routing.SnapshotData(m).InternalPlacements, metadataTable{InternalKeyspace: "ks", InternalTable: "table"}, tabletPlacement{InternalRanges: r}, comparePlacementTable)
	leader := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, LeaderAware: true}))
	first, _ := leader.Plan(RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true, Value: 1}, Consistency: Quorum}, m).Next()
	if first.HostID[15] != 2 {
		t.Fatal("missing follower hid resolved leader")
	}
}

func TestStep06StrictStreamingReservationUsesExistingCapacity(t *testing.T) {
	s, conns, _ := runtimeTestSession(t, 1)
	c := conns[0]
	pool := implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)]
	config := DefaultConfig("localhost")
	config.Pool.ConnectionsPerHost = 1
	implCluster.SessionData(s).InternalConnCfg = &driverConnConfig{TypedConfig: &config}
	c.InternalCfg = implCluster.SessionData(s).InternalConnCfg
	pool.InternalServices = newPoolServices(s)
	pool.InternalRepairCtx, pool.InternalRepairCancel = context.WithCancel(c.InternalCtx)
	defer pool.InternalRepairCancel()
	pool.InternalStarted = true
	pool.InternalState = poolReady
	pool.InternalConnPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
	pool.InternalConnPicker.Put(c)
	c.InternalErrorHandler = pool
	target := Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost), ShardID: Option[uint32]{Value: 0, Set: true}, StrictShard: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := codecTestMust(pool.InternalReserveStream(ctx, target, RoutingInfo{}))
	done := make(chan *exclusiveConn, 1)
	errs := make(chan error, 1)
	go func() {
		next, err := pool.InternalReserveStream(ctx, target, RoutingInfo{})
		if err != nil {
			errs <- err
			return
		}
		done <- next
	}()
	select {
	case r := <-done:
		r.InternalClose(false)
		t.Fatal("strict reserved target created side capacity")
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(10 * time.Millisecond):
	}
	if pool.Size() != 1 {
		t.Fatal("reservation changed configured capacity")
	}
	if err := first.InternalClose(true); err != nil {
		t.Fatal(err)
	}
	select {
	case next := <-done:
		if next.InternalConn != c {
			t.Fatal("strict reservation switched connection slot")
		}
		next.InternalClose(true)
	case err := <-errs:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestStep06LatencyThresholdExpiryAndSamples(t *testing.T) {
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{Latency: LatencyOptions{Enabled: true, MinimumSamples: 2, RetryPeriod: time.Minute}})).(*routingPolicy)
	now := time.Now()
	for id, value := range map[byte]float64{1: float64(time.Millisecond), 2: float64(2 * time.Millisecond), 3: float64(3 * time.Millisecond), 4: float64(9 * time.Millisecond)} {
		samples := 2
		if id == 4 {
			samples = 1
		}
		policy.InternalLatencies.Store(UUID{15: id}, &routingLatency{InternalValue: value, InternalSamples: samples, InternalUpdated: now})
	}
	policy.Observe(AttemptEvent{Target: Target{HostID: UUID{15: 5}}, Started: now, Latency: time.Millisecond})
	decisions := policy.InternalDecisions.Load()
	if decisions == nil {
		t.Fatal("no decisions")
	}
	if _, ok := decisions.InternalSlow[UUID{15: 2}]; ok {
		t.Fatal("threshold equality excluded")
	}
	if _, ok := decisions.InternalSlow[UUID{15: 3}]; !ok {
		t.Fatal("strictly slow host retained")
	}
	if _, ok := decisions.InternalSlow[UUID{15: 4}]; ok {
		t.Fatal("insufficient samples excluded")
	}
	plan := &lazyRoutingPlan{InternalSlow: decisions, InternalNow: now.Add(2 * time.Minute)}
	if plan.InternalPenalty(Target{HostID: UUID{15: 3}}) {
		t.Fatal("penalty did not expire")
	}
	if !routingLatencySample(NewErrProtocol("bad response")) || routingLatencySample(context.Canceled) {
		t.Fatal("latency error classification")
	}
}

type step06CapturedPartitioner struct {
	calls    int
	expected []byte
}

func (p *step06CapturedPartitioner) Token(key PartitionKey) (int64, error) {
	p.calls++
	err := key.ForEach(func(b Bytes) error {
		return cqltype.EachBytes(b, func(s wire.Span) error {
			if len(s.Bytes()) != len(p.expected) || len(p.expected) > 0 && &s.Bytes()[0] != &p.expected[0] {
				return typeValidation("partitioner", "payload was reencoded or copied")
			}
			return nil
		})
	})
	return 77, err
}
func TestStep06BindingCapturesPartitionerAndBorrowedSpans(t *testing.T) {
	s := step06Session(t)
	payload := []byte{1, 2, 3}
	old := &step06CapturedPartitioner{expected: payload}
	newer := &step06CapturedPartitioner{expected: payload}
	peripheral := implCluster.InternalSessionPeripheral(s)
	peripheral.InternalMu.Lock()
	peripheral.InternalConfig.Partitioners = map[string]TokenPartitioner{"old": old, "new": newer}
	peripheral.InternalMu.Unlock()
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Name: "key", Type: typ, Keyspace: "ks", Table: "table"})
	encoder := OneParam(BlobCodec)
	params := codecTestMust(encoder.Compile(columns))
	state := &preparedState{InternalSession: s, InternalProtocol: 4, InternalParameters: columns, InternalPartitioner: "old", InternalInfo: PreparedInfo{Keyspace: "ks", Table: "table", PartitionKeyIndexes: []int{0}}}
	prepared := func() *Prepared[Bytes,
		Unit] {
		opaqueStorageValuexxxxxxxx := new(Prepared[Bytes,
			Unit])
		implExecution.PreparedData(opaqueStorageValuexxxxxxxx).InternalState = state
		implExecution.PreparedData(opaqueStorageValuexxxxxxxx).InternalParams = params
		implExecution.PreparedData(opaqueStorageValuexxxxxxxx).InternalRows = NoRows
		implExecution.PreparedData(opaqueStorageValuexxxxxxxx).
			// Later metadata selects a different partitioner. Existing preparation and
			// binding still consume the canonical original parameter spans exactly once.
			InternalPlans = &resultPlanCache[Unit]{}
		return opaqueStorageValuexxxxxxxx
	}()

	next := clonePlacementSnapshot(s.Metadata())
	routing.SnapshotData(next).InternalKeyspaces = map[string]*KeyspaceMetadata{"ks": cloneV2Keyspace(routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"])}
	routing.SnapshotData(next).InternalKeyspaces["ks"].Tables["table"].Options.Partitioner = "new"
	peripheral.InternalSnapshot.Store(next)
	b := codecTestMust(BindBorrowed(prepared, BorrowBytes(payload)))
	defer b.Close()
	if old.calls != 1 || newer.calls != 0 || !b.Routing().Token.Set || b.Routing().Token.Value != 77 {
		t.Fatal("binding refreshed captured partitioner", old.calls, newer.calls, b.Routing())
	}
}

func TestStep06FeedbackEnqueueDoesNotWaitForPublication(t *testing.T) {
	s := step06Session(t)
	p := implCluster.InternalSessionPeripheral(s)
	info := RoutingInfo{Keyspace: "ks", Table: "table"}
	stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, s.Metadata())
	h := step06Hint(t, 0, 100, Option[uint64]{}, hTarget(1, 0))
	p.InternalMu.Lock()
	done := make(chan struct{})
	go func() { implCluster.InternalSessionQueueTabletFeedback(s, h, stamp); close(done) }()
	select {
	case <-done:
		p.InternalMu.Unlock()
	case <-time.After(time.Second):
		p.InternalMu.Unlock()
		<-done
		t.Fatal("successful feedback enqueue waited for publication mutex")
	}
}
