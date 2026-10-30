// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"fmt"
	"strconv"
	"strings"
)

// Cold-path replica precomputation uses the same distinct-node/rack-budget
// traversal for every factor, then retains its global ring order for LWT.
func InternalBuildV2VnodeRanges(ring *tokenRing, ks *KeyspaceMetadata) ([]TokenRange, error) {
	hosts := make(map[UUID]*HostInfo)
	dcHosts := make(map[string]int)
	dcRacks := make(map[string]map[string]bool)
	for _, entry := range ring.InternalTokens {
		h := entry.InternalHost
		id := implEndpoint.InternalHostInfoHostUUID(h)
		if hosts[id] != nil {
			continue
		}
		hosts[id] = h
		dc := h.DataCenter()
		dcHosts[dc]++
		if dcRacks[dc] == nil {
			dcRacks[dc] = make(map[string]bool)
		}
		dcRacks[dc][h.Rack()] = true
	}
	factor := func(value any, cap int) (int, error) {
		n, err := strconv.ParseUint(fmt.Sprint(value), 10, 64)
		if err != nil {
			return 0, typeValidation("replication factor", "invalid value %v", value)
		}
		if n > uint64(cap) {
			return cap, nil
		}
		return int(n), nil
	}
	nts := strings.HasSuffix(ks.StrategyClass, "NetworkTopologyStrategy")
	rf := 1
	dcs := make(map[string]int)
	var err error
	switch {
	case nts:
		for dc, value := range ks.StrategyOptions {
			if dc == "class" {
				continue
			}
			dcs[dc], err = factor(value, dcHosts[dc])
			if err != nil {
				return nil, err
			}
		}
	case strings.HasSuffix(ks.StrategyClass, "SimpleStrategy"):
		rf, err = factor(ks.StrategyOptions["replication_factor"], len(hosts))
		if err != nil {
			return nil, err
		}
	case strings.HasSuffix(ks.StrategyClass, "EverywhereStrategy"):
		rf = len(hosts)
	}
	out := make([]TokenRange, len(ring.InternalTokens))
	for i, entry := range ring.InternalTokens {
		token, ok := entry.InternalToken.(int64Token)
		if !ok {
			return nil, typeValidation("replica placement", "non-int64 token")
		}
		out[i].End = int64(token)
		seen := make(map[UUID]bool)
		counts := make(map[string]int)
		racks := make(map[string]map[string]bool)
		repeats := make(map[string]int)
		if nts {
			for dc, rf := range dcs {
				repeats[dc] = max(rf-len(dcRacks[dc]), 0)
			}
		}
		for j := range ring.InternalTokens {
			h := ring.InternalTokens[(i+j)%len(ring.InternalTokens)].InternalHost
			id := implEndpoint.InternalHostInfoHostUUID(h)
			if seen[id] {
				continue
			}
			seen[id] = true
			if nts {
				dc, rack := h.DataCenter(), h.Rack()
				if counts[dc] >= dcs[dc] {
					continue
				}
				if racks[dc] == nil {
					racks[dc] = make(map[string]bool)
				}
				if racks[dc][rack] {
					if repeats[dc] == 0 {
						continue
					}
					repeats[dc]--
				}
				racks[dc][rack] = true
				counts[dc]++
			} else if len(out[i].Replicas) >= rf {
				break
			}
			out[i].Replicas = append(out[i].Replicas, Target{HostID: id})
		}
	}
	for i := range out {
		out[i].Start = out[(i+len(out)-1)%len(out)].End
	}
	return out, nil
}
