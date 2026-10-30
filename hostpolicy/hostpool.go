/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package hostpolicy provides an optional health-weighted routing policy.
package hostpolicy

import (
	"fmt"
	"github.com/hailocab/go-hostpool"
	"github.com/scylladb/gocql/v2"
	"sync"
)

// HostPool adapts go-hostpool to the typed routing and attempt-observation API.
// Each instance belongs to one session. Metadata generations update membership;
// attempts report their outcome through RoutingPolicy.Observe.
func HostPool(hp hostpool.HostPool) gocql.RoutingPolicy {
	return &hostPoolHostPolicy{hp: hp}
}

type hostPoolHostPolicy struct {
	mu          sync.Mutex
	hp          hostpool.HostPool
	generation  uint64
	initialized bool
	hosts       []gocql.HostSnapshot
	byAddress   map[string]gocql.HostSnapshot
	responses   map[gocql.UUID]hostpool.HostPoolResponse
}

func (p *hostPoolHostPolicy) Validate(_ *gocql.MetadataSnapshot) error {
	if p.hp == nil {
		return &gocql.ValidationError{Field: "hostpool", Err: fmt.Errorf("nil host pool")}
	}
	return nil
}
func (p *hostPoolHostPolicy) setHosts(hosts []gocql.HostSnapshot) {
	addresses := make([]string, 0, len(hosts))
	p.hosts = nil
	p.byAddress = make(map[string]gocql.HostSnapshot, len(hosts))
	p.responses = make(map[gocql.UUID]hostpool.HostPoolResponse, len(hosts))
	for _, h := range hosts {
		if h.Enabled && h.Up {
			address := h.Address.String()
			addresses = append(addresses, address)
			p.byAddress[address] = h
			p.hosts = append(p.hosts, h)
		}
	}
	p.hp.SetHosts(addresses)
}
func (p *hostPoolHostPolicy) Plan(info gocql.RoutingInfo, m *gocql.MetadataSnapshot) gocql.QueryPlan {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m != nil && (!p.initialized || p.generation != m.Generation()) {
		p.setHosts(m.Hosts())
		p.generation = m.Generation()
		p.initialized = true
	}
	return &hostPoolPlan{policy: p, hosts: p.hosts, byAddress: p.byAddress, info: info, seen: make(map[gocql.UUID]bool)}
}
func (p *hostPoolHostPolicy) Observe(event gocql.AttemptEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r := p.responses[event.Target.HostID]; r != nil && !event.Cancelled {
		r.Mark(event.Err)
	}
}

type hostPoolPlan struct {
	policy    *hostPoolHostPolicy
	hosts     []gocql.HostSnapshot
	byAddress map[string]gocql.HostSnapshot
	info      gocql.RoutingInfo
	seen      map[gocql.UUID]bool
	fallback  int
}

func (p *hostPoolPlan) Next() (gocql.Target, bool) {
	policy := p.policy
	policy.mu.Lock()
	defer policy.mu.Unlock()
	emit := func(h gocql.HostSnapshot) (gocql.Target, bool) {
		if p.seen[h.ID] || p.info.HostID.Set && p.info.HostID.Value != h.ID {
			return gocql.Target{}, false
		}
		p.seen[h.ID] = true
		return gocql.Target{HostID: h.ID, ShardID: p.info.ShardID, StrictShard: p.info.ShardID.Set}, true
	}
	for n := 0; n < 2*len(p.hosts); n++ {
		r := policy.hp.Get()
		h, ok := p.byAddress[r.Host()]
		if !ok {
			continue
		}
		if t, ok := emit(h); ok {
			policy.responses[h.ID] = r
			return t, true
		}
	}
	for p.fallback < len(p.hosts) {
		h := p.hosts[p.fallback]
		p.fallback++
		if t, ok := emit(h); ok {
			return t, true
		}
	}
	return gocql.Target{}, false
}
