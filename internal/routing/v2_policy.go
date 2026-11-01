// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"context"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/internal/murmur"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/wire"
	// PartitionKey is the canonical encoded component stream. It never owns a
	// second serialization of the bound parameters.
)

type PartitionKey struct{ state PartitionKeyState }

func (k PartitionKey) Len() int64 { return PartitionKeyData(&k).InternalChain.Len() }
func (k PartitionKey) ForEach(fn func(Bytes) error) error {
	if fn == nil {
		return typeValidation("partition key", "nil visitor")
	}
	return PartitionKeyData(&k).InternalChain.Each(func(s wire.Span) error {
		c, e := wire.NewChain(s)
		if e != nil {
			return e
		}
		return fn(cqltype.BytesView(c, nil))
	})
}

type TokenPartitioner interface {
	Token(PartitionKey) (int64, error)
}

var Murmur3TokenPartitioner TokenPartitioner = murmur3StreamingPartitioner{}
var CDCTokenPartitioner TokenPartitioner = cdcStreamingPartitioner{}

func InternalNewPartitionKey(cells []encodedCell, indexes []int) (PartitionKey, bool, error) {
	if len(indexes) == 0 {
		return *NewPartitionKey(PartitionKeyState{}), false, nil
	}
	missing := false
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.State == Null || c.State == Unset {
			missing = true
		} else if c.State != Present {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 && c.Body.Len() > 65535 {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "composite component exceeds 65535 bytes")
		}
	}
	if missing {
		return *NewPartitionKey(PartitionKeyState{}), false, nil
	}
	b := wire.NewBuilder(256)
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.State == Null || c.State == Unset {
			return *NewPartitionKey(PartitionKeyState{}), false, nil
		}
		if c.State != Present {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 {
			if c.Body.Len() > 65535 {
				return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "composite component exceeds 65535 bytes")
			}
			if e := b.Uint16(uint16(c.Body.Len())); e != nil {
				return *NewPartitionKey(PartitionKeyState{}), false, e
			}
		}
		if e := c.Body.Each(b.Borrow); e != nil {
			return *NewPartitionKey(PartitionKeyState{}), false, e
		}
		if len(indexes) > 1 {
			if e := b.Uint8(0); e != nil {
				return *NewPartitionKey(PartitionKeyState{}), false, e
			}
		}
	}
	c, e := b.Freeze()
	return *NewPartitionKey(PartitionKeyState{InternalChain: c}), e == nil, e
}
func InternalComputeRoutingToken(cells []encodedCell, indexes []int, p TokenPartitioner) (Option[int64], error) {
	if _, builtin := p.(murmur3StreamingPartitioner); builtin {
		for _, index := range indexes {
			if index >= 0 && index < len(cells) && cells[index].State != Present && cells[index].State != Null && cells[index].State != Unset {
				return Option[int64]{}, typeValidation("partition key", "invalid cell state")
			}
		}
		token, err := InternalRoutingToken(cells, indexes)
		if token.Set && token.Value == math.MinInt64 {
			token.Value = math.MaxInt64
		}
		return token, err
	}
	k, ok, e := InternalNewPartitionKey(cells, indexes)
	if e != nil || !ok || p == nil {
		return Option[int64]{}, e
	}
	v, e := p.Token(k)
	return Option[int64]{Value: v, Set: e == nil}, e
}

type cdcStreamingPartitioner struct{}

func (cdcStreamingPartitioner) Token(k PartitionKey) (int64, error) {
	if k.Len() < 8 {
		return math.MinInt64, nil
	}
	var n uint64
	remaining := 8
	e := PartitionKeyData(&k).InternalChain.Each(func(s wire.Span) error {
		for _, b := range s.Bytes() {
			if remaining == 0 {
				break
			}
			n = n<<8 | uint64(b)
			remaining--
		}
		return nil
	})
	v := int64(n)
	if v == math.MinInt64 {
		v = math.MaxInt64
	}
	return v, e
}

type murmur3StreamingPartitioner struct{}

func (murmur3StreamingPartitioner) Token(k PartitionKey) (int64, error) {
	var hash murmur.Stream
	err := PartitionKeyData(&k).InternalChain.Each(func(s wire.Span) error { _, err := hash.Write(s.Bytes()); return err })
	token := hash.Sum64()
	if token == math.MinInt64 {
		token = math.MaxInt64
	}
	return token, err
}

type RoutingOptions struct {
	TokenAware                                bool
	Datacenter, Rack                          string
	AllowRemote, ShuffleReplicas, LeaderAware bool
	Latency                                   LatencyOptions
}
type LatencyOptions struct {
	Enabled                            bool
	ExclusionThreshold                 float64
	MinimumSamples                     int
	Scale, RetryPeriod, UpdateInterval time.Duration
}
type InternalRoutingLatency struct {
	mu              sync.Mutex
	InternalValue   float64
	InternalSamples int
	InternalUpdated time.Time
}
type InternalRoutingDecisions struct{ InternalSlow map[UUID]time.Time }
type InternalRoutingPolicy struct {
	InternalOptions   RoutingOptions
	seed              uint64
	round             atomic.Uint64
	InternalLatencies sync.Map
	InternalDecisions atomic.Pointer[InternalRoutingDecisions]
	updateAfter       atomic.Int64
	minimumBits       atomic.Uint64
}

func NewRoutingPolicy(o RoutingOptions) (RoutingPolicy, error) {
	if o.Rack != "" && o.Datacenter == "" {
		return nil, typeValidation("routing rack", "requires a datacenter")
	}
	if o.Latency.Enabled {
		if o.Latency.ExclusionThreshold == 0 {
			o.Latency.ExclusionThreshold = 2
		}
		if o.Latency.MinimumSamples == 0 {
			o.Latency.MinimumSamples = 50
		}
		if o.Latency.Scale == 0 {
			o.Latency.Scale = 100 * time.Millisecond
		}
		if o.Latency.RetryPeriod == 0 {
			o.Latency.RetryPeriod = 10 * time.Second
		}
		if o.Latency.UpdateInterval == 0 {
			o.Latency.UpdateInterval = time.Second
		}
		if o.Latency.ExclusionThreshold < 1 || math.IsNaN(o.Latency.ExclusionThreshold) || math.IsInf(o.Latency.ExclusionThreshold, 0) || o.Latency.MinimumSamples < 1 || o.Latency.Scale <= 0 || o.Latency.RetryPeriod <= 0 || o.Latency.UpdateInterval <= 0 {
			return nil, typeValidation("latency routing", "invalid configuration")
		}
	}
	return &InternalRoutingPolicy{InternalOptions: o, seed: rand.Uint64()}, nil
}
func (p *InternalRoutingPolicy) Validate(m *MetadataSnapshot) error {
	if m == nil {
		return typeValidation("routing", "missing metadata")
	}
	if p.InternalOptions.Datacenter == "" {
		return nil
	}
	idx := m.routingIndex()
	dc := idx.enabledDC[p.InternalOptions.Datacenter] > 0
	rack := idx.enabledRack[routingLocality{p.InternalOptions.Datacenter, p.InternalOptions.Rack}] > 0
	if !dc {
		return typeValidation("routing datacenter", "%q has no eligible hosts", p.InternalOptions.Datacenter)
	}
	if p.InternalOptions.Rack != "" && !rack {
		return typeValidation("routing rack", "%q has no eligible hosts", p.InternalOptions.Rack)
	}
	return nil
}

type InternalTargetPlan struct {
	InternalTargets []Target
	next            int
}

func (p *InternalTargetPlan) Next() (Target, bool) {
	if p == nil || p.next >= len(p.InternalTargets) {
		return Target{}, false
	}
	t := p.InternalTargets[p.next]
	p.next++
	return t, true
}

// Host indices and locality slices are built once on topology publication.
type routingLocality struct{ dc, rack string }
type InternalRoutingHostIndex struct {
	enabledDC   map[string]int
	enabledRack map[routingLocality]int
	byID        map[UUID]int
	all         []int
	dc          map[string][]int
	rack        map[routingLocality][]int
}

func InternalBuildRoutingHostIndex(hosts []HostSnapshot) *InternalRoutingHostIndex {
	x := &InternalRoutingHostIndex{enabledDC: make(map[string]int), enabledRack: make(map[routingLocality]int), byID: make(map[UUID]int, len(hosts)), dc: make(map[string][]int), rack: make(map[routingLocality][]int)}
	for i, h := range hosts {
		perfAdd(2, 1)
		x.byID[h.ID] = i
		x.all = append(x.all, i)
		x.dc[h.Datacenter] = append(x.dc[h.Datacenter], i)
		k := routingLocality{h.Datacenter, h.Rack}
		if h.Enabled {
			x.enabledDC[h.Datacenter]++
			x.enabledRack[k]++
		}
		x.rack[k] = append(x.rack[k], i)
	}
	return x
}
func (m *MetadataSnapshot) routingIndex() *InternalRoutingHostIndex {
	if SnapshotData(m).InternalHostIndex != nil {
		return SnapshotData(m).InternalHostIndex
	}
	return InternalBuildRoutingHostIndex(SnapshotData(m).InternalHosts)
}

type InternalLazyRoutingPlan struct {
	policy                     *InternalRoutingPolicy
	metadata                   *MetadataSnapshot
	index                      *InternalRoutingHostIndex
	info                       RoutingInfo
	replicas                   []Target
	leader                     Option[Target]
	permutation                []int
	smallPermutation           [16]int
	emitted                    []Target
	smallEmitted               [8]Target
	groups                     [3][]int
	stage, cursor, latencyPass int
	rotation                   int
	InternalSlow               *InternalRoutingDecisions
	InternalNow                time.Time
	pinned                     bool
}

func (p *InternalRoutingPolicy) Plan(info RoutingInfo, m *MetadataSnapshot) QueryPlan {
	info.Conditional = info.Conditional || info.Kind != OrdinaryStatement
	if m == nil {
		return &InternalTargetPlan{}
	}
	plan := &InternalLazyRoutingPlan{policy: p, metadata: m, index: m.routingIndex(), info: info, InternalNow: time.Now(), InternalSlow: p.InternalDecisions.Load()}
	plan.emitted = plan.smallEmitted[:0]
	if info.HostID.Set {
		plan.pinned = true
		return plan
	}
	o := p.InternalOptions
	if o.TokenAware && info.Token.Set && info.Keyspace != "" && info.Table != "" {
		plan.replicas, _ = m.placementReplicas(info.Keyspace, info.Table, info.Token.Value)
	}
	conditional := info.Conditional || info.Consistency == Serial || info.Consistency == LocalSerial
	plan.permutation = plan.smallPermutation[:0]
	if len(plan.replicas) > len(plan.smallPermutation) {
		plan.permutation = make([]int, 0, len(plan.replicas))
	}
	for i := range plan.replicas {
		plan.permutation = append(plan.permutation, i)
	}
	if !conditional && len(plan.permutation) > 1 {
		seed := p.seed
		if o.ShuffleReplicas {
			seed = rand.Uint64()
		}
		rng := rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
		for i := len(plan.permutation) - 1; i > 0; i-- {
			bound := uint64(i + 1)
			threshold := -bound % bound
			n := rng.Uint64()
			for n < threshold {
				n = rng.Uint64()
			}
			j := int(n % bound)
			plan.permutation[i], plan.permutation[j] = plan.permutation[j], plan.permutation[i]
		}
	}
	if o.LeaderAware && o.TokenAware && info.Token.Set && info.Consistency != One && info.Consistency != LocalOne {
		if ks := SnapshotData(m).InternalKeyspaces[info.Keyspace]; ks != nil && ks.ConsistencyMode == "global" {
			_, r := m.placementReplicas(info.Keyspace, info.Table, info.Token.Value)
			if r != nil && r.InternalValue.InternalVersion.Set && len(r.InternalValue.InternalSpan.Replicas) > 0 {
				plan.leader = Option[Target]{Value: r.InternalValue.InternalSpan.Replicas[0], Set: true}
			}
		}
	}
	if o.Datacenter == "" {
		plan.groups[1] = plan.index.all
	} else {
		plan.groups[1] = plan.index.dc[o.Datacenter]
		if o.Rack != "" {
			plan.groups[0] = plan.index.rack[routingLocality{o.Datacenter, o.Rack}]
		}
		if o.AllowRemote {
			plan.groups[2] = plan.index.all
		}
	}
	if !conditional {
		plan.rotation = int(p.round.Add(1) & uint64(^uint(0)>>1))
	}
	return plan
}
func (p *InternalLazyRoutingPlan) host(t Target) (HostSnapshot, bool) {
	perfAdd(0, 1)
	i, ok := p.index.byID[t.HostID]
	if !ok {
		return HostSnapshot{}, false
	}
	return SnapshotData(p.metadata).InternalHosts[i], true
}
func (p *InternalLazyRoutingPlan) eligible(h HostSnapshot) bool {
	return h.Enabled && (p.policy.InternalOptions.Datacenter == "" || h.Datacenter == p.policy.InternalOptions.Datacenter || p.policy.InternalOptions.AllowRemote)
}
func (p *InternalLazyRoutingPlan) InternalPenalty(t Target) bool {
	if p.InternalSlow == nil {
		return false
	}
	until, ok := p.InternalSlow.InternalSlow[t.HostID]
	return ok && p.InternalNow.Before(until)
}
func targetsDuplicate(a, b Target) bool {
	return a.HostID == b.HostID && (!a.ShardID.Set || !b.ShardID.Set || a.ShardID.Value == b.ShardID.Value)
}
func (p *InternalLazyRoutingPlan) accept(t Target) (Target, bool) {
	if p.InternalPenalty(t) != (p.latencyPass == 1) {
		return Target{}, false
	}
	for _, e := range p.emitted {
		if targetsDuplicate(t, e) {
			return Target{}, false
		}
	}
	p.emitted = append(p.emitted, t)
	return t, true
}
func (p *InternalLazyRoutingPlan) Next() (Target, bool) {
	if !p.pinned && p.metadata != nil && SnapshotData(p.metadata).InternalPlacementSignal != nil && SnapshotData(p.metadata).InternalPlacementEpoch != SnapshotData(p.metadata).InternalPlacementSignal.Load() {
		p.replicas = nil
		p.permutation = nil
		p.leader = Option[Target]{}
		if p.stage <= 3 {
			p.stage = 4
			p.cursor = 0
		}
	}
	if p.pinned {
		if p.stage > 0 {
			return Target{}, false
		}
		p.stage++
		t := Target{HostID: p.info.HostID.Value, ShardID: p.info.ShardID, StrictShard: p.info.ShardID.Set}
		if h, ok := p.host(t); ok && h.Enabled {
			return t, true
		}
		return Target{}, false
	}
	for {
		if p.stage == 0 {
			p.stage++
			if p.leader.Set {
				t := p.leader.Value
				if h, ok := p.host(t); ok && p.eligible(h) && h.Connected && h.Up && (!t.ShardID.Set || h.Shards > 0 && t.ShardID.Value < h.Shards) {
					t.StrictShard = t.ShardID.Set
					if t, ok := p.accept(t); ok {
						return t, true
					}
				}
			}
		}
		// Connected replicas: rack, DC, then permitted remote replicas. Remote
		// replicas precede local nonreplicas.
		for p.stage >= 1 && p.stage <= 3 {
			for p.cursor < len(p.permutation) {
				t := p.replicas[p.permutation[p.cursor]]
				p.cursor++
				h, ok := p.host(t)
				if !ok || !p.eligible(h) || !h.Connected || !h.Up {
					continue
				}
				o := p.policy.InternalOptions
				local := o.Datacenter == "" || h.Datacenter == o.Datacenter
				tier := 3
				if local {
					tier = 2
					if o.Rack != "" && h.Rack == o.Rack {
						tier = 1
					}
				}
				if tier != p.stage {
					continue
				}
				if t, ok := p.accept(t); ok {
					return t, true
				}
			}
			p.cursor = 0
			p.stage++
		}
		// Shared immutable fallback indices are traversed only on demand.
		for p.stage >= 4 && p.stage <= 8 {
			group := p.stage - 4
			connected := true
			if p.stage >= 7 {
				group = p.stage - 6
				connected = false
			}
			ids := p.groups[group]
			for p.cursor < len(ids) {
				i := (p.cursor + p.rotation) % len(ids)
				p.cursor++
				perfAdd(1, 1)
				h := SnapshotData(p.metadata).InternalHosts[ids[i]]
				if !p.eligible(h) || connected && (!h.Connected || !h.Up) {
					continue
				}
				if group == 2 && p.policy.InternalOptions.Datacenter != "" && h.Datacenter == p.policy.InternalOptions.Datacenter {
					continue
				}
				if t, ok := p.accept(Target{HostID: h.ID}); ok {
					return t, true
				}
			}
			p.cursor = 0
			p.stage++
		}
		if p.latencyPass == 0 && p.InternalSlow != nil {
			p.latencyPass = 1
			p.stage = 0
			p.cursor = 0
			continue
		}
		return Target{}, false
	}
}
func (p *InternalRoutingPolicy) Observe(e AttemptEvent) {
	if !p.InternalOptions.Latency.Enabled || e.Cancelled || !InternalRoutingLatencySample(e.Err) || e.Latency <= 0 {
		return
	}
	actual, found := p.InternalLatencies.Load(e.Target.HostID)
	if !found {
		actual, _ = p.InternalLatencies.LoadOrStore(e.Target.HostID, &InternalRoutingLatency{})
	}
	v := actual.(*InternalRoutingLatency)
	now := e.Started.Add(e.Latency)
	if e.Started.IsZero() {
		now = time.Now()
	}
	v.mu.Lock()
	weight := 1.0
	if v.InternalSamples > 0 {
		elapsed := max(now.Sub(v.InternalUpdated), 0)
		weight = 1 - math.Exp(-float64(elapsed)/float64(p.InternalOptions.Latency.Scale))
	}
	v.InternalValue += weight * (float64(e.Latency) - v.InternalValue)
	v.InternalSamples++
	v.InternalUpdated = now
	value, samples := v.InternalValue, v.InternalSamples
	v.mu.Unlock()
	if samples >= p.InternalOptions.Latency.MinimumSamples {
		for {
			bits := p.minimumBits.Load()
			if bits != 0 && value >= math.Float64frombits(bits) {
				break
			}
			if p.minimumBits.CompareAndSwap(bits, math.Float64bits(value)) {
				p.updateAfter.Store(0)
				break
			}
		}
	}
	due := p.updateAfter.Load()
	clock := time.Now()
	if clock.UnixNano() < due || !p.updateAfter.CompareAndSwap(due, clock.Add(p.InternalOptions.Latency.UpdateInterval).UnixNano()) {
		return
	}
	minimum := math.Inf(1)
	p.InternalLatencies.Range(func(_, value any) bool {
		v := value.(*InternalRoutingLatency)
		v.mu.Lock()
		if v.InternalSamples >= p.InternalOptions.Latency.MinimumSamples && v.InternalValue > 0 {
			minimum = min(minimum, v.InternalValue)
		}
		v.mu.Unlock()
		return true
	})
	decisions := &InternalRoutingDecisions{InternalSlow: make(map[UUID]time.Time)}
	p.InternalLatencies.Range(func(key, value any) bool {
		v := value.(*InternalRoutingLatency)
		v.mu.Lock()
		if v.InternalSamples >= p.InternalOptions.Latency.MinimumSamples && v.InternalValue > minimum*p.InternalOptions.Latency.ExclusionThreshold {
			decisions.InternalSlow[key.(UUID)] = v.InternalUpdated.Add(p.InternalOptions.Latency.RetryPeriod)
		}
		v.mu.Unlock()
		return true
	})
	p.InternalDecisions.Store(decisions)
}

type singleTargetRouting struct {
	target Target
	strict bool
}

func SingleTargetRouting(t Target, strictShard bool) RoutingPolicy {
	return &singleTargetRouting{target: t, strict: strictShard}
}
func (p *singleTargetRouting) Validate(m *MetadataSnapshot) error {
	if (p.strict || p.target.StrictShard) && !p.target.ShardID.Set {
		return typeValidation("routing shard", "strict target requires a shard ID")
	}
	if m != nil {
		if i, ok := m.routingIndex().byID[p.target.HostID]; ok {
			h := SnapshotData(m).InternalHosts[i]
			if h.Enabled {
				if (p.strict || p.target.StrictShard) && p.target.ShardID.Set && (h.Shards == 0 || p.target.ShardID.Value >= h.Shards) {
					return typeValidation("routing shard", "unavailable shard")
				}
				return nil
			}
		}
	}
	return typeValidation("routing host", "host is not eligible")
}
func (p *singleTargetRouting) Plan(_ RoutingInfo, m *MetadataSnapshot) QueryPlan {
	if m != nil {
		if i, ok := m.routingIndex().byID[p.target.HostID]; ok {
			h := SnapshotData(m).InternalHosts[i]
			if h.Enabled {
				t := p.target
				t.StrictShard = p.strict || t.StrictShard
				if t.ShardID.Set && (h.Shards == 0 || t.ShardID.Value >= h.Shards) {
					if t.StrictShard {
						return &InternalTargetPlan{}
					}
					t.ShardID = Option[uint32]{}
				}
				return &InternalTargetPlan{InternalTargets: []Target{t}}
			}
		}
	}
	return &InternalTargetPlan{}
}
func (*singleTargetRouting) Observe(AttemptEvent) {}

func InternalRoutingLatencySample(err error) bool {
	if err == nil {
		return true
	}
	var server RequestError
	if errors.As(err, &server) {
		switch server.GetCode() {
		case ErrCodeUnavailable, ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeUnprepared:
			return false
		}
		var rate *RequestErrRateLimitReached
		return !errors.As(err, &rate)
	}
	// Local validation/serialization and broken transport are not response latency.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, runtimeerrors.ErrNoConnections) || errors.Is(err, runtimeerrors.ErrConnectionClosed) {
		return false
	}
	var validation *ValidationError
	var network net.Error
	if errors.As(err, &validation) || errors.As(err, &network) {
		return false
	}
	return true
}

type PartitionKeyState struct {
	InternalChain wire.Chain
}
