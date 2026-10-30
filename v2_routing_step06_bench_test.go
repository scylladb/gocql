// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

var step06BenchmarkTarget Target

func BenchmarkStep06FirstTarget(b *testing.B) {
	for _, count := range []int{3, 30, 300} {
		for _, kind := range []string{"ordinary", "lwt", "leader", "latency", "pinned"} {
			b.Run(fmt.Sprintf("hosts%d/%s", count, kind), func(b *testing.B) {
				m := v2TestMetadata()
				m.hosts = make([]HostSnapshot, count)
				for i := range m.hosts {
					h := v2TestHost(0, "a", "r1")
					binary.BigEndian.PutUint64(h.ID[8:], uint64(i+1))
					m.hosts[i] = h
				}
				m.hostIndex = buildRoutingHostIndex(m.hosts)
				h := step06Hint(b, 0, 100, Option[uint64]{Value: 7, Set: true}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
				root, _ := updateTabletRange(nil, h)
				m.placements = placementPut(m.placements, metadataTable{"ks", "table"}, tabletPlacement{ranges: root}, comparePlacementTable)
				info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 1, Set: true}, Consistency: Quorum}
				options := RoutingOptions{TokenAware: true, Datacenter: "a", Rack: "r1", ShuffleReplicas: true, LeaderAware: kind == "leader"}
				if kind == "lwt" {
					info.Conditional = true
				}
				if kind == "leader" {
					m.keyspaces["ks"].ConsistencyMode = "global"
				}
				if kind == "latency" {
					options.Latency.Enabled = true
				}
				if kind == "pinned" {
					info.HostID = Option[UUID]{Value: m.hosts[0].ID, Set: true}
				}
				p := codecTestMust(NewRoutingPolicy(options))
				if kind == "latency" {
					p.(*routingPolicy).decisions.Store(&routingDecisions{slow: map[UUID]time.Time{m.hosts[2].ID: time.Now().Add(time.Hour)}})
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := p.Validate(m); err != nil {
						b.Fatal(err)
					}
					plan := p.Plan(info, m)
					target, ok := plan.Next()
					if !ok {
						b.Fatal("no target")
					}
					step06BenchmarkTarget = target
				}
			})
		}
	}
}
func BenchmarkStep06PlacementUpdate(b *testing.B) {
	for _, tables := range []int{1, 1024} {
		for _, ranges := range []int{1, 1024, 16384} {
			for _, duplicate := range []bool{false, true} {
				b.Run(fmt.Sprintf("tables%d/ranges%d/duplicate%t", tables, ranges, duplicate), func(b *testing.B) {
					s := step06Session(b)
					p := s.peripheral()
					m := s.Metadata()
					var root *tabletRangeTree
					for i := 0; i < ranges; i++ {
						root, _ = updateTabletRange(root, step06Hint(b, int64(i*2), int64(i*2+1), Option[uint64]{Value: uint64(i), Set: true}, hTarget(1, 0)))
					}
					k := metadataTable{"ks", "table"}
					m.placements = placementPut(m.placements, k, tabletPlacement{identity: "identity-1", ranges: root}, comparePlacementTable)
					for i := 1; i < tables; i++ {
						other := metadataTable{"ks", fmt.Sprintf("other%d", i)}
						m.placements = placementPut(m.placements, other, tabletPlacement{ranges: root}, comparePlacementTable)
					}
					info := RoutingInfo{Keyspace: "ks", Table: "table"}
					h := step06Hint(b, 0, 1, Option[uint64]{Value: 0, Set: true}, hTarget(1, 0))
					updates := make(map[v2TabletFeedbackKey]v2PendingTabletFeedback, 1)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						current := p.snapshot.Load()
						stamp := s.captureTabletFeedback(info, current)
						r, _ := current.tabletRoot(k)
						stamp.snapshot = nil
						if !duplicate {
							h = h.WithVersion(uint64(i + 1))
						}
						updates[v2TabletFeedbackKey{"ks", "table", 1}] = v2PendingTabletFeedback{hint: h, stamp: stamp, observation: observeTabletRanges(r, 0, 1)}
						s.publishTabletFeedback(p, updates, false)
					}
				})
			}
		}
	}
}
func BenchmarkStep06FeedbackQueue(b *testing.B) {
	for _, ranges := range []int{1, 1024} {
		b.Run(fmt.Sprintf("hints%d", ranges), func(b *testing.B) {
			s := step06Session(b)
			info := RoutingInfo{Keyspace: "ks", Table: "table"}
			stamp := s.captureTabletFeedback(info, s.Metadata())
			hints := make([]v2PendingTabletFeedback, ranges)
			for i := range hints {
				hints[i].hint = step06Hint(b, int64(i*2), int64(i*2+1), Option[uint64]{}, hTarget(1, 0))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.queueTabletFeedback(hints[i%ranges].hint, stamp)
			}
		})
	}
}
