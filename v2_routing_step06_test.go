// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
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
			parsed, err := parseTypedTabletPayload(Bytes{chain: chain}, "ks", "table", v2)
			if err != nil || !sameTabletObservation(&tabletRange{span: TokenRange{Start: h.FirstToken(), End: h.LastToken(), Replicas: []Target{target}}, version: Option[uint64]{Value: 0xfedcba9876543210, Set: v2}}, parsed) {
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
			c := &Conn{version: proto}
			c.initFramerCache()
			r := &attemptRequest{statement: "SELECT * FROM ks.table", consistency: One, tabletVersionBlock: 0xa7}
			switch kind {
			case "execute":
				r.preparedID = []byte{1}
				r.resultMetadataID = []byte{2}
			case "prepare":
				r.prepare = true
			case "batch":
				r.batch = &typedBatchRequest{kind: LoggedBatch}
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
			base, err := c.buildTypedRequest(r, 23)
			if err != nil {
				t.Fatal(err)
			}
			plain := flatten(base)
			c.framers.defaults.tabletsRoutingV2 = true
			extended, err := c.buildTypedRequest(r, 23)
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
	s := &Session{}
	s.metadataDescriber = newMetadataDescriber(s)
	m := v2TestMetadata()
	m.keyspaces["ks"].Tables["table"].ID = "identity-1"
	m.keyspaces["ks"].TabletBased = true
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", m.keyspaces["ks"])
	p := s.peripheral()
	p.feedbackOnce.Do(func() {})
	m.placementSignal = &p.placementEpoch
	m.placements = placementPut(m.placements, metadataTable{"ks", "table"}, tabletPlacement{identity: "identity-1"}, comparePlacementTable)
	p.snapshot.Store(m)
	t.Cleanup(func() { s.closeV2Peripheral(); s.metadataDescriber.metadata.tabletsMetadata.Close() })
	return s
}
func step06Drain(s *Session) {
	p := s.peripheral()
	p.feedbackMu.Lock()
	updates := p.feedback
	p.feedback = make(map[v2TabletFeedbackKey]v2PendingTabletFeedback)
	resync := p.feedbackResync
	p.feedbackResync = false
	p.feedbackMu.Unlock()
	s.publishTabletFeedback(p, updates, resync)
}
func TestStep06PlacementFencesSharingAndOverflow(t *testing.T) {
	s := step06Session(t)
	p := s.peripheral()
	info := RoutingInfo{Keyspace: "ks", Table: "table"}
	first := s.Metadata()
	stamp := s.captureTabletFeedback(info, first)
	h := step06Hint(t, 0, 100, Option[uint64]{Value: 7, Set: true}, Target{HostID: first.hosts[0].ID, ShardID: Option[uint32]{Set: true}})
	s.queueTabletFeedback(h, stamp)
	step06Drain(s)
	a := s.Metadata()
	if a == first || a.hostIndex != first.hostIndex || a.keyspaces["ks"] != first.keyspaces["ks"] {
		t.Fatal("placement rebuilt host/schema graph")
	}
	if root, _ := first.tabletRoot(metadataTable{"ks", "table"}); root != nil {
		t.Fatal("mutated retained snapshot")
	}
	s.queueTabletFeedback(h, s.captureTabletFeedback(info, a))
	step06Drain(s)
	if s.Metadata() != a {
		t.Fatal("duplicate advanced snapshot")
	}
	// A numerically smaller opaque version is a valid update. The stale request
	// captured before the first update cannot overwrite its overlapping range.
	moved := step06Hint(t, 0, 100, Option[uint64]{Value: 1, Set: true}, Target{HostID: first.hosts[1].ID, ShardID: Option[uint32]{Value: 1, Set: true}})
	s.queueTabletFeedback(moved, stamp)
	step06Drain(s)
	if s.Metadata() != a {
		t.Fatal("stale response overwrote newer range")
	}
	s.queueTabletFeedback(moved, s.captureTabletFeedback(info, a))
	step06Drain(s)
	b := s.Metadata()
	r, _ := b.tabletRoot(metadataTable{"ks", "table"})
	if n := tabletRangeFor(r, 1); n == nil || n.value.version.Value != 1 {
		t.Fatal("ordered opaque versions numerically")
	}
	// Disjoint observations captured together merge, preserving the old record.
	left := s.captureTabletFeedback(info, b)
	for _, bounds := range [][2]int64{{100, 200}, {200, 300}} {
		s.queueTabletFeedback(step06Hint(t, bounds[0], bounds[1], Option[uint64]{}, hTarget(3, 2)), left)
	}
	step06Drain(s)
	c := s.Metadata()
	newRoot, _ := c.tabletRoot(metadataTable{"ks", "table"})
	if tabletRangeFor(newRoot, 1).value != tabletRangeFor(r, 1).value || tabletRangeFor(newRoot, 150) == nil || tabletRangeFor(newRoot, 250) == nil {
		t.Fatal("disjoint ranges were copied or lost")
	}
	// A split removes the old overlapping range entirely; adjacent ranges survive.
	s.queueTabletFeedback(step06Hint(t, 10, 50, Option[uint64]{}, hTarget(2, 1)), s.captureTabletFeedback(info, c))
	step06Drain(s)
	root, _ := s.Metadata().tabletRoot(metadataTable{"ks", "table"})
	if tabletRangeFor(root, 1) != nil || tabletRangeFor(root, 51) != nil || tabletRangeFor(root, 150) == nil {
		t.Fatal("split retained old fragments or removed adjacent range")
	}
	beforeEpoch := s.Metadata()
	oldStamp := s.captureTabletFeedback(info, beforeEpoch)
	lazy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true})).Plan(RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 20, Set: true}}, beforeEpoch)

	p.mu.Lock()
	p.tableRevision[metadataTable{"ks", "table"}]++
	p.mu.Unlock()
	s.queueTabletFeedback(h, oldStamp)
	step06Drain(s)
	if s.Metadata() != beforeEpoch {
		t.Fatal("table epoch fence failed")
	}
	// Even before snapshot publication, a source drop/recreate rejects old replies.
	before := s.Metadata()
	sourceStamp := s.captureTabletFeedback(info, before)
	dropped := s.metadataDescriber.metadata.keyspaceMetadata.get()["ks"].Clone()
	delete(dropped.Tables, "table")
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", dropped)
	s.queueTabletFeedback(h, sourceStamp)
	step06Drain(s)
	if s.Metadata() != before {
		t.Fatal("dropped source table resurrected")
	}
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", before.keyspaces["ks"])
	ks := s.metadataDescriber.metadata.keyspaceMetadata.get()["ks"]
	ks = ks.Clone()
	table := *ks.Tables["table"]
	table.ID = "identity-2"
	ks.Tables["table"] = &table
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", ks)
	s.queueTabletFeedback(h, sourceStamp)
	step06Drain(s)
	if s.Metadata() != before {
		t.Fatal("source identity fence failed")
	}
	// Restore the known source, then saturation invalidates immediately, before
	// a worker can publish, with bounded queue ownership and later relearning.
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks", before.keyspaces["ks"])
	for i := 0; i < 1025; i++ {
		s.queueTabletFeedback(step06Hint(t, int64(i*2), int64(i*2+1), Option[uint64]{}, hTarget(1, 0)), s.captureTabletFeedback(info, before))
	}
	if target, ok := lazy.Next(); !ok || target.ShardID.Set {
		t.Fatal("preexisting plan used invalidated placement", target)
	}
	if replicas, _ := before.Replicas("ks", "table", 150); len(replicas) != 0 {
		t.Fatal("overflow retained known-stale mapping")
	}
	p.feedbackMu.Lock()
	if len(p.feedback) > 1024 {
		t.Fatal("unbounded queue")
	}
	for _, u := range p.feedback {
		if u.stamp.snapshot != nil {
			t.Fatal("queue retained cluster snapshot")
		}
	}
	p.feedbackMu.Unlock()
	step06Drain(s)
	s.queueTabletFeedback(h, s.captureTabletFeedback(info, s.Metadata()))
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
	m.keyspaces["ks"].ConsistencyMode = "global"
	k := metadataTable{"ks", "table"}
	set := func(targets ...Target) {
		h := step06Hint(t, 0, 100, Option[uint64]{Value: 9, Set: true}, targets...)
		r, _ := updateTabletRange(nil, h)
		m.placements = placementPut(m.placements, k, tabletPlacement{ranges: r}, comparePlacementTable)
	}
	set(hTarget(3, 1), hTarget(2, 2), hTarget(1, 3))
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 1, Set: true}, Consistency: LocalQuorum, Conditional: true}
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, LeaderAware: true, Datacenter: "a", Rack: "r1", AllowRemote: true, ShuffleReplicas: true}))
	for i := 0; i < 10; i++ {
		plan := policy.Plan(info, m)
		first, ok := plan.Next()
		if !ok || first.HostID != m.hosts[2].ID || !first.StrictShard || first.ShardID.Value != 1 {
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
		{"disconnected-leader", func() { m.hosts[2].Connected = false }, 1, func() { m.hosts[2].Connected = true }},
		{"disabled-leader", func() { m.hosts[2].Enabled = false }, 1, func() { m.hosts[2].Enabled = true }},
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
	p.decisions.Store(&routingDecisions{slow: map[UUID]time.Time{m.hosts[2].ID: time.Now().Add(time.Hour)}})
	if got := v2PlanIDs(p.Plan(info, m)); !bytes.Equal(got, []byte{1, 2, 4, 3}) {
		t.Fatal("penalized leader order", got)
	}
	// Preserve distinct explicit shards, but suppress host-only fallback duplicates.
	p.decisions.Store(nil)
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
				_ = m.tabletVersionBlock(info)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		s.queueTabletFeedback(step06Hint(t, 0, 100, Option[uint64]{Value: uint64(i), Set: true}, hTarget(byte(i%3+1), 0)), s.captureTabletFeedback(info, s.Metadata()))
		step06Drain(s)
	}
	wg.Wait()
}
func TestStep06PickerStrictAndAdvisory(t *testing.T) {
	options := DefaultConfig("localhost").Pool
	makeConn := func(shard int) *Conn {
		return &Conn{scyllaSupported: ScyllaConnectionFeatures{ScyllaHostFeatures: ScyllaHostFeatures{nrShards: 2}, shard: shard}, streams: streams.New()}
	}
	a, b := makeConn(0), makeConn(1)
	p := codecTestMust(newTypedConnPicker(a, options))
	_ = p.Put(a)
	_ = p.Put(b)
	a.exclusiveFlag.Store(true)
	if got := p.pickTarget(Target{ShardID: Option[uint32]{Value: 0, Set: true}}, RoutingInfo{}); got != b {
		t.Fatal("advisory hint could not use free capacity")
	}
	if got := p.pickTarget(Target{StrictShard: true, ShardID: Option[uint32]{Value: 0, Set: true}}, RoutingInfo{}); got != a {
		t.Fatal("strict target relaxed while reserved")
	}
	if got := p.pickTarget(Target{StrictShard: true, ShardID: Option[uint32]{Value: math.MaxUint32, Set: true}}, RoutingInfo{}); got != nil {
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
		c := &Conn{version: 4, cqlProtoExts: exts}
		c.initFramerCache()
		if c.framers.defaults.tabletsRoutingV2 != both || c.framers.defaults.tabletsRoutingV1 == both {
			t.Fatal("V2 did not take precedence")
		}
		for _, ext := range exts {
			if _, ok := ext.serialize()[tabletsRoutingV1]; ok && both {
				t.Fatal("both extensions negotiated")
			}
		}
	}
	s := step06Session(t)
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true, Value: 1}}
	h := step06Hint(t, 0, 100, Option[uint64]{Set: true, Value: 0xfedcba9876543210}, hTarget(1, 1))
	stamp := s.captureTabletFeedback(info, s.Metadata())
	// applyTypedFeedbackStamped is deliberately before execute-error/UNPREPARED
	// interpretation; a response page can carry payload independently of rows.
	page := &responsePage{payload: []PayloadEntry{{Key: "tablets-routing-v2", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes(step06Payload(h))}}}}
	s.applyTypedFeedbackStamped(page, info, stamp, framerConfig{tabletsRoutingV1: true, tabletsRoutingV2: true})
	step06Drain(s)
	m := s.Metadata()
	for i := 0; i < 1024; i++ {
		b := m.tabletVersionBlock(info)
		index := b >> 4
		if b&15 != byte(uint64(0xfedcba9876543210)>>uint(index*4)&15) {
			t.Fatal("wrong nibble direction", b)
		}
	}
	if b := m.tabletVersionBlock(RoutingInfo{}); b != 0 {
		t.Fatal("absent identity/token hint is not zero")
	}
	// A malformed V2 body never falls back to the simultaneously supplied V1.
	page.payload = append([]PayloadEntry{{Key: "tablets-routing-v1", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes(step06Payload(step06Hint(t, 100, 200, Option[uint64]{}, hTarget(2, 0))))}}}, page.payload...)
	page.payload[1].Value.Value = BorrowBytes([]byte{0})
	s.applyTypedFeedbackStamped(page, info, s.captureTabletFeedback(info, m), framerConfig{tabletsRoutingV1: true, tabletsRoutingV2: true})
	step06Drain(s)
	if s.Metadata() != m {
		t.Fatal("malformed V2 fell back to V1")
	}
}
func TestStep06VnodeDistinctNodesAndRackBudget(t *testing.T) {
	hosts := []*HostInfo{
		{hostId: UUID{15: 1}, dataCenter: "a", rack: "r1", tokens: []string{"0", "1"}},
		{hostId: UUID{15: 2}, dataCenter: "a", rack: "r1", tokens: []string{"2"}},
		{hostId: UUID{15: 3}, dataCenter: "a", rack: "r2", tokens: []string{"3"}},
		{hostId: UUID{15: 4}, dataCenter: "a", rack: "r3", tokens: []string{"4"}},
		{hostId: UUID{15: 5}, dataCenter: "b", rack: "r1", tokens: []string{"5"}},
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
	for i := range m.hosts {
		m.hosts[i].Connected = false
	}
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, Datacenter: "a"}))
	if ids := v2PlanIDs(policy.Plan(RoutingInfo{Kind: OrdinaryStatement}, m)); !bytes.Equal(ids, []byte{2, 4, 1}) {
		t.Fatal("enabled disconnected fallback", ids)
	}
	m = v2TestMetadata()
	m.keyspaces["ks"].ConsistencyMode = "global"
	h := step06Hint(t, 0, 100, Option[uint64]{Set: true}, hTarget(2, 1), hTarget(99, 2), hTarget(1, 1))
	r, _ := updateTabletRange(nil, h)
	m.placements = placementPut(m.placements, metadataTable{"ks", "table"}, tabletPlacement{ranges: r}, comparePlacementTable)
	leader := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, LeaderAware: true}))
	first, _ := leader.Plan(RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true, Value: 1}, Consistency: Quorum}, m).Next()
	if first.HostID[15] != 2 {
		t.Fatal("missing follower hid resolved leader")
	}
}

func TestStep06StrictStreamingReservationUsesExistingCapacity(t *testing.T) {
	s, conns, _ := runtimeTestSession(t, 1)
	c := conns[0]
	pool := s.pool.hostConnPools[c.host.hostUUID()]
	config := DefaultConfig("localhost")
	config.Pool.ConnectionsPerHost = 1
	s.connCfg = &ConnConfig{TypedConfig: &config}
	c.cfg = s.connCfg
	pool.session = s
	pool.repairCtx, pool.repairCancel = context.WithCancel(c.ctx)
	defer pool.repairCancel()
	pool.started = true
	pool.state = poolReady
	pool.connPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
	pool.connPicker.Put(c)
	c.errorHandler = pool
	target := Target{HostID: c.host.hostUUID(), ShardID: Option[uint32]{Value: 0, Set: true}, StrictShard: true}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := codecTestMust(pool.reserveStream(ctx, target, RoutingInfo{}))
	done := make(chan *exclusiveConn, 1)
	errs := make(chan error, 1)
	go func() {
		next, err := pool.reserveStream(ctx, target, RoutingInfo{})
		if err != nil {
			errs <- err
			return
		}
		done <- next
	}()
	select {
	case r := <-done:
		r.close(false)
		t.Fatal("strict reserved target created side capacity")
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(10 * time.Millisecond):
	}
	if pool.Size() != 1 {
		t.Fatal("reservation changed configured capacity")
	}
	if err := first.close(true); err != nil {
		t.Fatal(err)
	}
	select {
	case next := <-done:
		if next.conn != c {
			t.Fatal("strict reservation switched connection slot")
		}
		next.close(true)
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
		policy.latencies.Store(UUID{15: id}, &routingLatency{value: value, samples: samples, updated: now})
	}
	policy.Observe(AttemptEvent{Target: Target{HostID: UUID{15: 5}}, Started: now, Latency: time.Millisecond})
	decisions := policy.decisions.Load()
	if decisions == nil {
		t.Fatal("no decisions")
	}
	if _, ok := decisions.slow[UUID{15: 2}]; ok {
		t.Fatal("threshold equality excluded")
	}
	if _, ok := decisions.slow[UUID{15: 3}]; !ok {
		t.Fatal("strictly slow host retained")
	}
	if _, ok := decisions.slow[UUID{15: 4}]; ok {
		t.Fatal("insufficient samples excluded")
	}
	plan := &lazyRoutingPlan{slow: decisions, now: now.Add(2 * time.Minute)}
	if plan.penalty(Target{HostID: UUID{15: 3}}) {
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
		return b.each(func(s wire.Span) error {
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
	peripheral := s.peripheral()
	peripheral.mu.Lock()
	peripheral.config.Partitioners = map[string]TokenPartitioner{"old": old, "new": newer}
	peripheral.mu.Unlock()
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Name: "key", Type: typ, Keyspace: "ks", Table: "table"})
	encoder := OneParam(BlobCodec)
	params := codecTestMust(encoder.Compile(columns))
	state := &preparedState{session: s, protocol: 4, parameters: columns, partitioner: "old", info: PreparedInfo{Keyspace: "ks", Table: "table", PartitionKeyIndexes: []int{0}}}
	prepared := &Prepared[Bytes, Unit]{state: state, params: params, rows: NoRows, plans: &resultPlanCache[Unit]{}}
	// Later metadata selects a different partitioner. Existing preparation and
	// binding still consume the canonical original parameter spans exactly once.
	next := clonePlacementSnapshot(s.Metadata())
	next.keyspaces = map[string]*KeyspaceMetadata{"ks": cloneV2Keyspace(s.Metadata().keyspaces["ks"])}
	next.keyspaces["ks"].Tables["table"].Options.Partitioner = "new"
	peripheral.snapshot.Store(next)
	b := codecTestMust(BindBorrowed(prepared, BorrowBytes(payload)))
	defer b.Close()
	if old.calls != 1 || newer.calls != 0 || !b.Routing().Token.Set || b.Routing().Token.Value != 77 {
		t.Fatal("binding refreshed captured partitioner", old.calls, newer.calls, b.Routing())
	}
}

func TestStep06FeedbackEnqueueDoesNotWaitForPublication(t *testing.T) {
	s := step06Session(t)
	p := s.peripheral()
	info := RoutingInfo{Keyspace: "ks", Table: "table"}
	stamp := s.captureTabletFeedback(info, s.Metadata())
	h := step06Hint(t, 0, 100, Option[uint64]{}, hTarget(1, 0))
	p.mu.Lock()
	done := make(chan struct{})
	go func() { s.queueTabletFeedback(h, stamp); close(done) }()
	select {
	case <-done:
		p.mu.Unlock()
	case <-time.After(time.Second):
		p.mu.Unlock()
		<-done
		t.Fatal("successful feedback enqueue waited for publication mutex")
	}
}
