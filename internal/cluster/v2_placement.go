// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"github.com/scylladb/gocql/v2/tablets"
)

func InternalSessionCaptureTabletFeedback(s *Session, info RoutingInfo, m *MetadataSnapshot) tabletFeedbackStamp {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return tabletFeedbackStamp{}
	}
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	k := metadataTable{InternalKeyspace: info.Keyspace, InternalTable: info.Table}
	return tabletFeedbackStamp{InternalValid: s.tabletSchemaCurrent(k, routing.InternalMetadataSnapshotTableIdentity(m, k)), InternalSnapshot: m, InternalEpoch: p.InternalPlacementEpoch.Load(), InternalSchema: p.InternalSchemaEpoch[k.InternalKeyspace], InternalTable: p.InternalTableRevision[k], InternalIdentity: routing.InternalMetadataSnapshotTableIdentity(m, k)}
}

type InternalV2TabletFeedbackKey struct {
	InternalKeyspace, InternalTable string
	InternalEnd                     int64
}
type InternalV2PendingTabletFeedback struct {
	InternalHint        tablets.TabletInfo
	InternalStamp       tabletFeedbackStamp
	InternalObservation tabletRangeObservation
}

// applyV2TabletHint is retained for internal legacy callers/tests. Actual EXECUTE
// responses supply the stamp captured before sending, never a receipt-time stamp.
func InternalSessionApplyV2TabletHint(s *Session, h tablets.TabletInfo) {
	info := RoutingInfo{Keyspace: h.KeyspaceName(), Table: h.TableName()}
	InternalSessionQueueTabletFeedback(s, h, InternalSessionCaptureTabletFeedback(s, info, s.Metadata()))
}
func InternalSessionQueueTabletFeedback(s *Session, h tablets.TabletInfo, stamp tabletFeedbackStamp) {
	p := InternalSessionPeripheral(s)
	if p == nil || stamp.InternalSnapshot == nil || !stamp.InternalValid {
		return
	}
	k := metadataTable{InternalKeyspace: h.KeyspaceName(), InternalTable: h.TableName()}
	if _, _, err := routing.InternalMetadataSnapshotLookupRanges(stamp.InternalSnapshot, k.InternalKeyspace, k.InternalTable); err != nil {
		return
	}
	select {
	case <-p.InternalDone:
		return
	default:
	}
	before, _ := routing.InternalMetadataSnapshotTabletRoot(stamp.InternalSnapshot, k)
	observation := observeTabletRanges(before, h.FirstToken(), h.LastToken())
	stamp.InternalSnapshot = nil // the queue never retains a cluster/schema snapshot or page.
	p.InternalFeedbackMu.Lock()
	key := InternalV2TabletFeedbackKey{k.InternalKeyspace, k.InternalTable, h.LastToken()}
	p.knownTabletTables[k] = true
	if len(p.InternalFeedback) >= 1024 {
		if _, exists := p.InternalFeedback[key]; !exists {
			clear(p.InternalFeedback)
			p.InternalFeedbackResync = true
			p.InternalPlacementEpoch.Add(1)
		}
	}
	// Keep the earliest pending observation for an overlapping identity/range.
	// Later requests can relearn after it publishes; receipt order is not freshness.
	if prior, ok := p.InternalFeedback[key]; !ok || prior.InternalStamp.InternalEpoch != stamp.InternalEpoch || prior.InternalStamp.InternalSchema != stamp.InternalSchema || prior.InternalStamp.InternalTable != stamp.InternalTable || prior.InternalStamp.InternalIdentity != stamp.InternalIdentity {
		p.InternalFeedback[key] = InternalV2PendingTabletFeedback{InternalHint: h, InternalStamp: stamp, InternalObservation: observation}
	}
	p.InternalFeedbackMu.Unlock()
	p.InternalFeedbackOnce.Do(func() { p.InternalStartWorker(func() { InternalSessionRunV2TabletFeedback(s, p) }) })
	select {
	case p.feedbackWake <- struct{}{}:
	default:
	}
}
func InternalSessionPublishTabletFeedback(s *Session, p *InternalV2PeripheralState, updates map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback, resync bool) {
	var known []metadataTable
	if resync {
		p.InternalFeedbackMu.Lock()
		for k := range p.knownTabletTables {
			known = append(known, k)
		}
		p.InternalFeedbackMu.Unlock()
	}
	p.publishMu.Lock()
	p.InternalMu.Lock()
	old := p.InternalSnapshot.Load()
	if old == nil || p.closed {
		p.InternalMu.Unlock()
		p.publishMu.Unlock()
		return
	}
	next := old
	ensureNext := func() {
		if next == old {
			next = clonePlacementSnapshot(old)
			routing.SnapshotData(next).InternalPlacementSignal = &p.InternalPlacementEpoch
		}
	}
	epoch := p.InternalPlacementEpoch.Load()
	if routing.SnapshotData(old).InternalPlacementEpoch != epoch {
		ensureNext()
		routing.SnapshotData(next).InternalPlacements = nil
		routing.SnapshotData(next).InternalPlacementEpoch = epoch
		// Invalidated mappings remain explicitly tablet-based, including tables
		// learned by feedback before metadata advertised tablets.
		placementEach(routing.SnapshotData(old).InternalPlacements, func(k metadataTable, t tabletPlacement) {
			t.InternalRanges = nil
			routing.SnapshotData(next).InternalPlacements = placementPut(routing.SnapshotData(next).InternalPlacements, k, t, comparePlacementTable)
		})
	}
	if resync {
		ensureNext()
		routing.SnapshotData(next).InternalDiagnostics = make([]error, 0, len(routing.SnapshotData(old).InternalDiagnostics)+1)
		for _, diagnostic := range routing.SnapshotData(old).InternalDiagnostics {
			if v, ok := diagnostic.(*ValidationError); ok && v.Field == "tablet placement" {
				continue
			}
			routing.SnapshotData(next).InternalDiagnostics = append(routing.SnapshotData(next).InternalDiagnostics, diagnostic)
		}
		routing.SnapshotData(next).InternalDiagnostics = append(routing.SnapshotData(next).InternalDiagnostics, typeValidation("tablet placement", "feedback queue saturated; mappings invalidated"))
		for _, k := range known {
			if _, _, err := routing.InternalMetadataSnapshotLookupRanges(old, k.InternalKeyspace, k.InternalTable); err == nil && placementGet(routing.SnapshotData(next).InternalPlacements, k, comparePlacementTable) == nil {
				routing.SnapshotData(next).InternalPlacements = placementPut(routing.SnapshotData(next).InternalPlacements, k, tabletPlacement{InternalIdentity: routing.InternalMetadataSnapshotTableIdentity(old, k)}, comparePlacementTable)
			}
		}
	}
	for _, u := range updates {
		h, stamp := u.InternalHint, u.InternalStamp
		k := metadataTable{InternalKeyspace: h.KeyspaceName(), InternalTable: h.TableName()}
		if !s.tabletSchemaCurrent(k, stamp.InternalIdentity) || stamp.InternalEpoch != epoch || stamp.InternalSchema != p.InternalSchemaEpoch[k.InternalKeyspace] || stamp.InternalTable != p.InternalTableRevision[k] || p.schemaDropped[k.InternalKeyspace] || stamp.InternalIdentity != routing.InternalMetadataSnapshotTableIdentity(old, k) {
			continue
		}
		if _, _, err := routing.InternalMetadataSnapshotLookupRanges(old, k.InternalKeyspace, k.InternalTable); err != nil {
			continue
		}
		var current *tabletRangeTree
		t := tabletPlacement{InternalIdentity: stamp.InternalIdentity}
		if n := placementGet(routing.SnapshotData(next).InternalPlacements, k, comparePlacementTable); n != nil {
			t = n.InternalValue
			current = t.InternalRanges
		}
		// Exact duplicates do not replace nodes, generations, or events.
		n := tabletRangeAfter(current, h.FirstToken())
		if n != nil && sameTabletObservation(n.InternalValue, h) {
			continue
		}
		if u.InternalObservation != observeTabletRanges(current, h.FirstToken(), h.LastToken()) {
			continue
		}
		var changed bool
		t.InternalRanges, changed = updateTabletRange(current, h)
		if changed {
			ensureNext()
			routing.SnapshotData(next).InternalPlacements = placementPut(routing.SnapshotData(next).InternalPlacements, k, t, comparePlacementTable)
		}
	}
	changed := next != old
	if changed {
		p.InternalSnapshot.Store(next)
	}
	p.InternalMu.Unlock()
	p.publishMu.Unlock()
	if resync {
		InternalSessionPublishV2LocalEvent(s, Event{Kind: OverflowEvent, Generation: routing.SnapshotData(next).InternalGeneration, Change: "TABLET_RESYNC"})
	}
}
func InternalSessionRunV2TabletFeedback(s *Session, p *InternalV2PeripheralState) {
	for {
		select {
		case <-p.InternalDone:
			return
		case <-p.feedbackWake:
		}
		p.InternalFeedbackMu.Lock()
		pending := p.InternalFeedback
		p.InternalFeedback = make(map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback)
		resync := p.InternalFeedbackResync
		p.InternalFeedbackResync = false
		p.InternalFeedbackMu.Unlock()
		InternalSessionPublishTabletFeedback(s, p, pending, resync)
	}
}

func InternalSessionCapturedRoutingPartitioner(s *Session, name string) TokenPartitioner {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return nil
	}
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	return p.InternalConfig.Partitioners[name]
}

// Caller holds peripheral.mu. Source identity is checked as well as the public
// snapshot: a drop can already be fenced while its snapshot build is pending.
func (s *Session) tabletSchemaCurrent(k metadataTable, identity string) bool {
	if s.privateStorage.InternalMetadataDescriber == nil || s.privateStorage.InternalMetadataDescriber.InternalMetadata == nil {
		return false
	}
	source := s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load()
	if source == nil {
		return false
	}
	ks := (*source)[k.InternalKeyspace]
	if ks == nil {
		return false
	}
	if _, invalid := implMetadata.KeyspaceData(ks).InternalTablesInvalidated[k.InternalTable]; invalid {
		return false
	}
	if t := ks.Tables[k.InternalTable]; t != nil {
		return t.ID == identity
	}
	if v := ks.Views[k.InternalTable]; v != nil {
		return v.ID == identity
	}
	return false
}
