// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"math/rand/v2"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/tablets"
)

// Immutable AVL indices share every unaffected subtree. Neither an observation
// nor a routing plan may mutate a published node or replica slice.
type placementTree[K any, V any] struct {
	key         K
	value       V
	left, right *placementTree[K, V]
	height      int
}

func placementHeight[K, V any](n *placementTree[K, V]) int {
	if n == nil {
		return 0
	}
	return n.height
}
func placementNode[K, V any](key K, value V, l, r *placementTree[K, V]) *placementTree[K, V] {
	return &placementTree[K, V]{key: key, value: value, left: l, right: r, height: 1 + max(placementHeight(l), placementHeight(r))}
}
func placementBalance[K, V any](n *placementTree[K, V]) *placementTree[K, V] {
	if placementHeight(n.left) > placementHeight(n.right)+1 {
		l := n.left
		if placementHeight(l.right) > placementHeight(l.left) {
			r := l.right
			l = placementNode(r.key, r.value, placementNode(l.key, l.value, l.left, r.left), r.right)
		}
		return placementNode(l.key, l.value, l.left, placementNode(n.key, n.value, l.right, n.right))
	}
	if placementHeight(n.right) > placementHeight(n.left)+1 {
		r := n.right
		if placementHeight(r.left) > placementHeight(r.right) {
			l := r.left
			r = placementNode(l.key, l.value, l.left, placementNode(r.key, r.value, l.right, r.right))
		}
		return placementNode(r.key, r.value, placementNode(n.key, n.value, n.left, r.left), r.right)
	}
	return n
}
func placementGet[K, V any](n *placementTree[K, V], key K, cmp func(K, K) int) *placementTree[K, V] {
	for n != nil {
		c := cmp(key, n.key)
		if c == 0 {
			return n
		}
		if c < 0 {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil
}
func placementPut[K, V any](n *placementTree[K, V], key K, value V, cmp func(K, K) int) *placementTree[K, V] {
	if n == nil {
		return placementNode(key, value, nil, nil)
	}
	c := cmp(key, n.key)
	if c == 0 {
		return placementNode(key, value, n.left, n.right)
	}
	if c < 0 {
		return placementBalance(placementNode(n.key, n.value, placementPut(n.left, key, value, cmp), n.right))
	}
	return placementBalance(placementNode(n.key, n.value, n.left, placementPut(n.right, key, value, cmp)))
}
func placementDelete[K, V any](n *placementTree[K, V], key K, cmp func(K, K) int) *placementTree[K, V] {
	if n == nil {
		return nil
	}
	c := cmp(key, n.key)
	if c < 0 {
		return placementBalance(placementNode(n.key, n.value, placementDelete(n.left, key, cmp), n.right))
	}
	if c > 0 {
		return placementBalance(placementNode(n.key, n.value, n.left, placementDelete(n.right, key, cmp)))
	}
	if n.left == nil {
		return n.right
	}
	if n.right == nil {
		return n.left
	}
	next := n.right
	for next.left != nil {
		next = next.left
	}
	return placementBalance(placementNode(next.key, next.value, n.left, placementDelete(n.right, next.key, cmp)))
}
func placementEach[K, V any](n *placementTree[K, V], fn func(K, V)) {
	if n == nil {
		return
	}
	placementEach(n.left, fn)
	fn(n.key, n.value)
	placementEach(n.right, fn)
}
func comparePlacementToken(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func comparePlacementTable(a, b metadataTable) int {
	if a.keyspace < b.keyspace {
		return -1
	}
	if a.keyspace > b.keyspace {
		return 1
	}
	if a.table < b.table {
		return -1
	}
	if a.table > b.table {
		return 1
	}
	return 0
}

var tabletObservationID atomic.Uint64

type tabletRange struct {
	observation uint64
	span        TokenRange
	version     Option[uint64]
}
type tabletRangeTree = placementTree[int64, *tabletRange]
type tabletPlacement struct {
	ranges   *tabletRangeTree
	identity string
}
type tabletPlacementTree = placementTree[metadataTable, tabletPlacement]

func tabletRangeAfter(n *tabletRangeTree, token int64) *tabletRangeTree {
	var result *tabletRangeTree
	for n != nil {
		if n.key > token {
			result = n
			n = n.left
		} else {
			n = n.right
		}
	}
	return result
}
func tabletRangeFor(n *tabletRangeTree, token int64) *tabletRangeTree {
	var result *tabletRangeTree
	for n != nil {
		if n.key >= token {
			result = n
			n = n.left
		} else {
			n = n.right
		}
	}
	if result != nil && token > result.value.span.Start {
		return result
	}
	return nil
}
func sameTabletObservation(a *tabletRange, h tablets.TabletInfo) bool {
	v, ok := h.Version()
	if a.version.Set != ok || ok && a.version.Value != v || a.span.Start != h.FirstToken() || a.span.End != h.LastToken() {
		return false
	}
	r := h.ReplicasUnsafe()
	if len(r) != len(a.span.Replicas) {
		return false
	}
	for i, t := range a.span.Replicas {
		if t.HostID != UUID(r[i].HostUUIDValue()) || !t.ShardID.Set || t.ShardID.Value != uint32(r[i].ShardID()) {
			return false
		}
	}
	return true
}
func updateTabletRange(root *tabletRangeTree, h tablets.TabletInfo) (*tabletRangeTree, bool) {
	first, last := h.FirstToken(), h.LastToken()
	n := tabletRangeAfter(root, first)
	if n != nil && sameTabletObservation(n.value, h) {
		return root, false
	}
	for n != nil && n.value.span.Start < last {
		root = placementDelete(root, n.key, comparePlacementToken)
		n = tabletRangeAfter(root, first)
	}
	r := &tabletRange{observation: tabletObservationID.Add(1), span: TokenRange{Start: first, End: last, Replicas: make([]Target, len(h.ReplicasUnsafe()))}}
	for i, replica := range h.ReplicasUnsafe() {
		r.span.Replicas[i] = Target{HostID: UUID(replica.HostUUIDValue()), ShardID: Option[uint32]{Value: uint32(replica.ShardID()), Set: true}}
	}
	r.version.Value, r.version.Set = h.Version()
	return placementPut(root, last, r, comparePlacementToken), true
}

func (m *MetadataSnapshot) tableIdentity(k metadataTable) string {
	if m == nil {
		return ""
	}
	ks := m.keyspaces[k.keyspace]
	if ks == nil {
		return ""
	}
	if t := ks.Tables[k.table]; t != nil {
		return t.ID
	}
	if v := ks.Views[k.table]; v != nil {
		return v.ID
	}
	return ""
}
func (m *MetadataSnapshot) tabletRoot(k metadataTable) (*tabletRangeTree, bool) {
	if m == nil {
		return nil, false
	}
	if n := placementGet(m.placements, k, comparePlacementTable); n != nil {
		if m.placementSignal != nil && m.placementEpoch != m.placementSignal.Load() {
			return nil, true
		}
		return n.value.ranges, true
	}
	return nil, false
}
func (m *MetadataSnapshot) tabletVersionBlock(info RoutingInfo) byte {
	if info.Table == "" || info.Keyspace == "" || !info.Token.Set {
		return 0
	}
	if root, ok := m.tabletRoot(metadataTable{info.Keyspace, info.Table}); ok {
		if n := tabletRangeFor(root, info.Token.Value); n != nil && n.value.version.Set {
			index := rand.Uint32N(16)
			return byte(index<<4) | byte(n.value.version.Value>>(index*4)&15)
		}
	}
	return byte(rand.Uint32())
}

// placementReplicas is a private borrowed view; public accessors detach it.
func (m *MetadataSnapshot) placementReplicas(keyspace, table string, token int64) ([]Target, *tabletRangeTree) {
	if m == nil {
		return nil, nil
	}
	if m.placementSignal != nil && m.placementEpoch != m.placementSignal.Load() {
		return nil, nil
	}
	if root, ok := m.tabletRoot(metadataTable{keyspace, table}); ok {
		if n := tabletRangeFor(root, token); n != nil {
			return n.value.span.Replicas, n
		}
		return nil, nil
	}
	ranges, tablet, err := m.lookupRanges(keyspace, table)
	if err != nil || len(ranges) == 0 {
		return nil, nil
	}
	return m.legacyPlacementReplicas(ranges, tablet, token), nil
}
func (m *MetadataSnapshot) legacyPlacementReplicas(ranges []TokenRange, tablet bool, token int64) []Target {
	lo, hi := 0, len(ranges)
	for lo < hi {
		mid := (lo + hi) / 2
		if ranges[mid].End < token {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if tablet {
		if lo == len(ranges) || token <= ranges[lo].Start {
			return nil
		}
	} else if lo == len(ranges) {
		lo = 0
	}
	return ranges[lo].Replicas
}

type tabletFeedbackStamp struct {
	snapshot             *MetadataSnapshot
	valid                bool
	epoch, schema, table uint64
	identity             string
}

func (s *Session) captureTabletFeedback(info RoutingInfo, m *MetadataSnapshot) tabletFeedbackStamp {
	p := s.peripheral()
	if p == nil {
		return tabletFeedbackStamp{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := metadataTable{info.Keyspace, info.Table}
	return tabletFeedbackStamp{valid: s.tabletSchemaCurrent(k, m.tableIdentity(k)), snapshot: m, epoch: p.placementEpoch.Load(), schema: p.schemaEpoch[k.keyspace], table: p.tableRevision[k], identity: m.tableIdentity(k)}
}
func clonePlacementSnapshot(m *MetadataSnapshot) *MetadataSnapshot {
	return &MetadataSnapshot{generation: m.generation + 1, hosts: m.hosts, hostIndex: m.hostIndex, keyspaces: m.keyspaces, schemaSources: m.schemaSources, ranges: m.ranges, tablets: m.tablets, partitioner: m.partitioner, diagnostics: m.diagnostics, placements: m.placements, placementEpoch: m.placementEpoch, placementSignal: m.placementSignal}
}

// Schema/topology refresh is a cold path. It reconciles object identities and
// removed replicas without constructing a new schema graph for tablet feedback.
func reconcileTabletPlacement(old, current *MetadataSnapshot) *tabletPlacementTree {
	var root *tabletPlacementTree
	if old != nil {
		root = old.placements
	}
	known := make(map[UUID]bool, len(current.hosts))
	for _, h := range current.hosts {
		known[h.ID] = true
	}
	placementEach(root, func(k metadataTable, t tabletPlacement) {
		if _, _, err := current.lookupRanges(k.keyspace, k.table); err != nil || current.tableIdentity(k) != t.identity {
			root = placementDelete(root, k, comparePlacementTable)
			return
		}
		ranges := t.ranges
		previousKS := old.keyspaces[k.keyspace]
		currentKS := current.keyspaces[k.keyspace]
		if previousKS != nil && currentKS != nil {
			if previousKS.StrategyClass != currentKS.StrategyClass || !compareInterfaceMaps(previousKS.StrategyOptions, currentKS.StrategyOptions) || previousKS.ConsistencyMode != currentKS.ConsistencyMode {
				ranges = nil
			}
			if a, b := previousKS.Tables[k.table], currentKS.Tables[k.table]; a != nil && b != nil && !a.Equals(b) {
				ranges = nil
			}
		}
		if old.placementEpoch != current.placementEpoch {
			ranges = nil
		} else {
			placementEach(ranges, func(end int64, r *tabletRange) {
				for _, target := range r.span.Replicas {
					if !known[target.HostID] {
						ranges = placementDelete(ranges, end, comparePlacementToken)
						break
					}
				}
			})
		}
		if ranges != t.ranges {
			t.ranges = ranges
			root = placementPut(root, k, t, comparePlacementTable)
		}
	})
	for k, ranges := range current.tablets {
		if placementGet(root, k, comparePlacementTable) != nil {
			continue
		}
		if _, _, err := current.lookupRanges(k.keyspace, k.table); err != nil {
			continue
		}
		t := tabletPlacement{identity: current.tableIdentity(k)}
		for _, r := range ranges {
			if r.Start < r.End {
				v := &tabletRange{observation: tabletObservationID.Add(1), span: r}
				t.ranges = placementPut(t.ranges, r.End, v, comparePlacementToken)
			}
		}
		root = placementPut(root, k, t, comparePlacementTable)
	}
	for name, ks := range current.keyspaces {
		if ks.TabletBased {
			for table := range ks.Tables {
				k := metadataTable{name, table}
				if placementGet(root, k, comparePlacementTable) == nil {
					root = placementPut(root, k, tabletPlacement{identity: current.tableIdentity(k)}, comparePlacementTable)
				}
			}
			for table := range ks.Views {
				k := metadataTable{name, table}
				if placementGet(root, k, comparePlacementTable) == nil {
					root = placementPut(root, k, tabletPlacement{identity: current.tableIdentity(k)}, comparePlacementTable)
				}
			}
		}
	}
	return root
}

type v2TabletFeedbackKey struct {
	keyspace, table string
	end             int64
}
type v2PendingTabletFeedback struct {
	hint        tablets.TabletInfo
	stamp       tabletFeedbackStamp
	observation tabletRangeObservation
}

// applyV2TabletHint is retained for internal legacy callers/tests. Actual EXECUTE
// responses supply the stamp captured before sending, never a receipt-time stamp.
func (s *Session) applyV2TabletHint(h tablets.TabletInfo) {
	info := RoutingInfo{Keyspace: h.KeyspaceName(), Table: h.TableName()}
	s.queueTabletFeedback(h, s.captureTabletFeedback(info, s.Metadata()))
}
func (s *Session) queueTabletFeedback(h tablets.TabletInfo, stamp tabletFeedbackStamp) {
	p := s.peripheral()
	if p == nil || stamp.snapshot == nil || !stamp.valid {
		return
	}
	k := metadataTable{h.KeyspaceName(), h.TableName()}
	if _, _, err := stamp.snapshot.lookupRanges(k.keyspace, k.table); err != nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	before, _ := stamp.snapshot.tabletRoot(k)
	observation := observeTabletRanges(before, h.FirstToken(), h.LastToken())
	stamp.snapshot = nil // the queue never retains a cluster/schema snapshot or page.
	p.feedbackMu.Lock()
	key := v2TabletFeedbackKey{k.keyspace, k.table, h.LastToken()}
	p.knownTabletTables[k] = true
	if len(p.feedback) >= 1024 {
		if _, exists := p.feedback[key]; !exists {
			clear(p.feedback)
			p.feedbackResync = true
			p.placementEpoch.Add(1)
		}
	}
	// Keep the earliest pending observation for an overlapping identity/range.
	// Later requests can relearn after it publishes; receipt order is not freshness.
	if prior, ok := p.feedback[key]; !ok || prior.stamp.epoch != stamp.epoch || prior.stamp.schema != stamp.schema || prior.stamp.table != stamp.table || prior.stamp.identity != stamp.identity {
		p.feedback[key] = v2PendingTabletFeedback{hint: h, stamp: stamp, observation: observation}
	}
	p.feedbackMu.Unlock()
	p.feedbackOnce.Do(func() { p.startWorker(func() { s.runV2TabletFeedback(p) }) })
	select {
	case p.feedbackWake <- struct{}{}:
	default:
	}
}
func (s *Session) publishTabletFeedback(p *v2PeripheralState, updates map[v2TabletFeedbackKey]v2PendingTabletFeedback, resync bool) {
	var known []metadataTable
	if resync {
		p.feedbackMu.Lock()
		for k := range p.knownTabletTables {
			known = append(known, k)
		}
		p.feedbackMu.Unlock()
	}
	p.publishMu.Lock()
	p.mu.Lock()
	old := p.snapshot.Load()
	if old == nil || p.closed {
		p.mu.Unlock()
		p.publishMu.Unlock()
		return
	}
	next := old
	ensureNext := func() {
		if next == old {
			next = clonePlacementSnapshot(old)
			next.placementSignal = &p.placementEpoch
		}
	}
	epoch := p.placementEpoch.Load()
	if old.placementEpoch != epoch {
		ensureNext()
		next.placements = nil
		next.placementEpoch = epoch
		// Invalidated mappings remain explicitly tablet-based, including tables
		// learned by feedback before metadata advertised tablets.
		placementEach(old.placements, func(k metadataTable, t tabletPlacement) {
			t.ranges = nil
			next.placements = placementPut(next.placements, k, t, comparePlacementTable)
		})
	}
	if resync {
		ensureNext()
		next.diagnostics = make([]error, 0, len(old.diagnostics)+1)
		for _, diagnostic := range old.diagnostics {
			if v, ok := diagnostic.(*ValidationError); ok && v.Field == "tablet placement" {
				continue
			}
			next.diagnostics = append(next.diagnostics, diagnostic)
		}
		next.diagnostics = append(next.diagnostics, typeValidation("tablet placement", "feedback queue saturated; mappings invalidated"))
		for _, k := range known {
			if _, _, err := old.lookupRanges(k.keyspace, k.table); err == nil && placementGet(next.placements, k, comparePlacementTable) == nil {
				next.placements = placementPut(next.placements, k, tabletPlacement{identity: old.tableIdentity(k)}, comparePlacementTable)
			}
		}
	}
	for _, u := range updates {
		h, stamp := u.hint, u.stamp
		k := metadataTable{h.KeyspaceName(), h.TableName()}
		if !s.tabletSchemaCurrent(k, stamp.identity) || stamp.epoch != epoch || stamp.schema != p.schemaEpoch[k.keyspace] || stamp.table != p.tableRevision[k] || p.schemaDropped[k.keyspace] || stamp.identity != old.tableIdentity(k) {
			continue
		}
		if _, _, err := old.lookupRanges(k.keyspace, k.table); err != nil {
			continue
		}
		var current *tabletRangeTree
		t := tabletPlacement{identity: stamp.identity}
		if n := placementGet(next.placements, k, comparePlacementTable); n != nil {
			t = n.value
			current = t.ranges
		}
		// Exact duplicates do not replace nodes, generations, or events.
		n := tabletRangeAfter(current, h.FirstToken())
		if n != nil && sameTabletObservation(n.value, h) {
			continue
		}
		if u.observation != observeTabletRanges(current, h.FirstToken(), h.LastToken()) {
			continue
		}
		var changed bool
		t.ranges, changed = updateTabletRange(current, h)
		if changed {
			ensureNext()
			next.placements = placementPut(next.placements, k, t, comparePlacementTable)
		}
	}
	changed := next != old
	if changed {
		p.snapshot.Store(next)
	}
	p.mu.Unlock()
	p.publishMu.Unlock()
	if resync {
		s.publishV2LocalEvent(Event{Kind: OverflowEvent, Generation: next.generation, Change: "TABLET_RESYNC"})
	}
}
func (s *Session) runV2TabletFeedback(p *v2PeripheralState) {
	for {
		select {
		case <-p.done:
			return
		case <-p.feedbackWake:
		}
		p.feedbackMu.Lock()
		pending := p.feedback
		p.feedback = make(map[v2TabletFeedbackKey]v2PendingTabletFeedback)
		resync := p.feedbackResync
		p.feedbackResync = false
		p.feedbackMu.Unlock()
		s.publishTabletFeedback(p, pending, resync)
	}
}

func (s *Session) capturedRoutingPartitioner(name string) TokenPartitioner {
	p := s.peripheral()
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config.Partitioners[name]
}

// Monotonically unique observation IDs provide a bounded exact interval fence:
// every inserted overlapping record raises maxID; removals without an insertion
// reduce count. Published records never change IDs. Unchanged AVL ancestors can
// be rebuilt without changing a range's identity.
type tabletRangeObservation struct {
	maxID uint64
	count uint64
}

func observeTabletRanges(root *tabletRangeTree, first, last int64) tabletRangeObservation {
	var result tabletRangeObservation
	var visit func(*tabletRangeTree)
	visit = func(n *tabletRangeTree) {
		if n == nil {
			return
		}
		if n.key <= first {
			visit(n.right)
			return
		}
		if n.value.span.Start >= last {
			visit(n.left)
			return
		}
		visit(n.left)
		result.maxID = max(result.maxID, n.value.observation)
		result.count++
		visit(n.right)
	}
	visit(root)
	return result
}

// Caller holds peripheral.mu. Source identity is checked as well as the public
// snapshot: a drop can already be fenced while its snapshot build is pending.
func (s *Session) tabletSchemaCurrent(k metadataTable, identity string) bool {
	if s.metadataDescriber == nil || s.metadataDescriber.metadata == nil {
		return false
	}
	source := s.metadataDescriber.metadata.keyspaceMetadata.keyspaceMap.Load()
	if source == nil {
		return false
	}
	ks := (*source)[k.keyspace]
	if ks == nil {
		return false
	}
	if _, invalid := ks.tablesInvalidated[k.table]; invalid {
		return false
	}
	if t := ks.Tables[k.table]; t != nil {
		return t.ID == identity
	}
	if v := ks.Views[k.table]; v != nil {
		return v.ID == identity
	}
	return false
}
