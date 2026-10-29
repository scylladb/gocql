package gocql

import (
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// PartitionKey is the canonical encoded component stream. It never owns a
// second serialization of the bound parameters.
type PartitionKey struct{ chain wire.Chain }

func (k PartitionKey) Len() int64 { return k.chain.Len() }
func (k PartitionKey) ForEach(fn func(Bytes) error) error {
	if fn == nil {
		return typeValidation("partition key", "nil visitor")
	}
	return k.chain.Each(func(s wire.Span) error {
		c, e := wire.NewChain(s)
		if e != nil {
			return e
		}
		return fn(Bytes{chain: c})
	})
}

type TokenPartitioner interface {
	Token(PartitionKey) (int64, error)
}

var Murmur3TokenPartitioner TokenPartitioner = murmur3StreamingPartitioner{}
var CDCTokenPartitioner TokenPartitioner = cdcStreamingPartitioner{}

func newPartitionKey(cells []encodedCell, indexes []int) (PartitionKey, bool, error) {
	if len(indexes) == 0 {
		return PartitionKey{}, false, nil
	}
	missing := false
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return PartitionKey{}, false, typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.state == Null || c.state == Unset {
			missing = true
		} else if c.state != Present {
			return PartitionKey{}, false, typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 && c.body.Len() > 65535 {
			return PartitionKey{}, false, typeValidation("partition key", "composite component exceeds 65535 bytes")
		}
	}
	if missing {
		return PartitionKey{}, false, nil
	}
	b := wire.NewBuilder(256)
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return PartitionKey{}, false, typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.state == Null || c.state == Unset {
			return PartitionKey{}, false, nil
		}
		if c.state != Present {
			return PartitionKey{}, false, typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 {
			if c.body.Len() > 65535 {
				return PartitionKey{}, false, typeValidation("partition key", "composite component exceeds 65535 bytes")
			}
			if e := b.Uint16(uint16(c.body.Len())); e != nil {
				return PartitionKey{}, false, e
			}
		}
		if e := c.body.Each(b.Borrow); e != nil {
			return PartitionKey{}, false, e
		}
		if len(indexes) > 1 {
			if e := b.Uint8(0); e != nil {
				return PartitionKey{}, false, e
			}
		}
	}
	c, e := b.Freeze()
	return PartitionKey{chain: c}, e == nil, e
}
func computeRoutingToken(cells []encodedCell, indexes []int, p TokenPartitioner) (Option[int64], error) {
	if _, builtin := p.(murmur3StreamingPartitioner); builtin {
		for _, index := range indexes {
			if index >= 0 && index < len(cells) && cells[index].state != Present && cells[index].state != Null && cells[index].state != Unset {
				return Option[int64]{}, typeValidation("partition key", "invalid cell state")
			}
		}
		token, err := routingToken(cells, indexes)
		if token.Set && token.Value == math.MinInt64 {
			token.Value = math.MaxInt64
		}
		return token, err
	}
	k, ok, e := newPartitionKey(cells, indexes)
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
	e := k.chain.Each(func(s wire.Span) error {
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
	err := k.chain.Each(func(s wire.Span) error { _, err := hash.Write(s.Bytes()); return err })
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
type routingLatency struct {
	value              float64
	samples            int
	updated, lastProbe time.Time
}
type routingPolicy struct {
	options   RoutingOptions
	round     atomic.Uint64
	mu        sync.Mutex
	latencies map[UUID]routingLatency
	minimum   float64
	minimumAt time.Time
}

func NewRoutingPolicy(o RoutingOptions) (RoutingPolicy, error) {
	if o.LeaderAware {
		return nil, typeValidation("leader-aware routing", "requires tablet v2 leader metadata, which is not implemented")
	}
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
	return &routingPolicy{options: o, latencies: make(map[UUID]routingLatency)}, nil
}
func (p *routingPolicy) Validate(m *MetadataSnapshot) error {
	if m == nil {
		return typeValidation("routing", "missing metadata")
	}
	if p.options.Datacenter == "" {
		return nil
	}
	dc, rack := false, false
	for _, h := range m.hosts {
		if !h.Enabled {
			continue
		}
		if h.Datacenter == p.options.Datacenter {
			dc = true
			if h.Rack == p.options.Rack {
				rack = true
			}
		}
	}
	if !dc {
		return typeValidation("routing datacenter", "%q has no eligible hosts", p.options.Datacenter)
	}
	if p.options.Rack != "" && !rack {
		return typeValidation("routing rack", "%q has no eligible hosts", p.options.Rack)
	}
	return nil
}

type targetPlan struct {
	targets []Target
	next    int
}

func (p *targetPlan) Next() (Target, bool) {
	if p == nil || p.next >= len(p.targets) {
		return Target{}, false
	}
	t := p.targets[p.next]
	p.next++
	return t, true
}
func (p *routingPolicy) Plan(info RoutingInfo, m *MetadataSnapshot) QueryPlan {
	plan := &targetPlan{}
	if m == nil {
		return plan
	}
	eligible := make(map[UUID]HostSnapshot, len(m.hosts))
	for _, h := range m.hosts {
		if h.Enabled && h.Up {
			eligible[h.ID] = h
		}
	}
	if info.HostID.Set {
		if h, ok := eligible[info.HostID.Value]; ok {
			shard := info.ShardID
			if shard.Set && h.Shards > 0 && shard.Value >= h.Shards {
				shard = Option[uint32]{}
			}
			plan.targets = []Target{{HostID: h.ID, ShardID: shard}}
		}
		return plan
	}
	conditional := info.Conditional || info.Consistency == Serial || info.Consistency == LocalSerial
	var replicas []Target
	if p.options.TokenAware && info.Token.Set {
		replicas, _ = m.Replicas(info.Keyspace, info.Table, info.Token.Value)
	}
	if !conditional && p.options.ShuffleReplicas {
		rand.Shuffle(len(replicas), func(i, j int) { replicas[i], replicas[j] = replicas[j], replicas[i] })
	}
	seen := make(map[UUID]bool)
	groups := [6][]Target{}
	tier := func(h HostSnapshot, replica bool) int {
		local := p.options.Datacenter == "" || h.Datacenter == p.options.Datacenter
		if !local && !p.options.AllowRemote {
			return -1
		}
		if !local {
			if replica {
				return 4
			}
			return 5
		}
		if p.options.Rack != "" && h.Rack != p.options.Rack {
			if replica {
				return 1
			}
			return 3
		}
		if replica {
			return 0
		}
		return 2
	}
	for _, t := range replicas {
		h, ok := eligible[t.HostID]
		if !ok || seen[t.HostID] {
			continue
		}
		n := tier(h, true)
		if n < 0 {
			continue
		}
		groups[n] = append(groups[n], t)
		seen[t.HostID] = true
	}
	var fallback []HostSnapshot
	for _, h := range m.hosts {
		if _, ok := eligible[h.ID]; ok && !seen[h.ID] {
			fallback = append(fallback, h)
		}
	}
	if len(fallback) > 0 && !conditional {
		n := int(p.round.Add(1)-1) % len(fallback)
		fallback = append(fallback[n:], fallback[:n]...)
	}
	for _, h := range fallback {
		n := tier(h, false)
		if n >= 0 {
			groups[n] = append(groups[n], Target{HostID: h.ID})
		}
	}
	for _, g := range groups {
		plan.targets = append(plan.targets, g...)
	}
	if p.options.Latency.Enabled && !conditional {
		plan.targets = p.orderByLatency(plan.targets)
	}
	return plan
}
func (p *routingPolicy) Observe(e AttemptEvent) {
	if !p.options.Latency.Enabled || e.Cancelled || e.Err != nil || e.Latency < 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.latencies[e.Target.HostID]
	now := e.Started.Add(e.Latency)
	if now.IsZero() {
		now = time.Now()
	}
	weight := 1.0
	if v.samples > 0 {
		elapsed := now.Sub(v.updated)
		if elapsed < 0 {
			elapsed = 0
		}
		weight = 1 - math.Exp(-float64(elapsed)/float64(p.options.Latency.Scale))
	}
	v.value += weight * (float64(e.Latency) - v.value)
	v.samples++
	v.updated = now
	p.latencies[e.Target.HostID] = v
}
func (p *routingPolicy) orderByLatency(targets []Target) []Target {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	minimum := p.minimum
	if p.minimumAt.IsZero() || now.Sub(p.minimumAt) >= p.options.Latency.UpdateInterval {
		minimum = math.Inf(1)
		for _, t := range targets {
			v := p.latencies[t.HostID]
			if v.samples >= p.options.Latency.MinimumSamples && v.value < minimum {
				minimum = v.value
			}
		}
		p.minimum, p.minimumAt = minimum, now
	}
	if math.IsInf(minimum, 1) {
		return targets
	}
	slow := make(map[UUID]bool)
	for _, t := range targets {
		v := p.latencies[t.HostID]
		if v.samples >= p.options.Latency.MinimumSamples && v.value > minimum*p.options.Latency.ExclusionThreshold {
			if now.Sub(v.lastProbe) >= p.options.Latency.RetryPeriod {
				v.lastProbe = now
				p.latencies[t.HostID] = v
			} else {
				slow[t.HostID] = true
			}
		}
	}
	sort.SliceStable(targets, func(i, j int) bool { return !slow[targets[i].HostID] && slow[targets[j].HostID] })
	return targets
}

type singleTargetRouting struct {
	target Target
	strict bool
}

func SingleTargetRouting(t Target, strictShard bool) RoutingPolicy {
	return &singleTargetRouting{target: t, strict: strictShard}
}
func (p *singleTargetRouting) Validate(m *MetadataSnapshot) error {
	if m != nil {
		for _, h := range m.hosts {
			if h.ID == p.target.HostID && h.Enabled {
				if p.strict && p.target.ShardID.Set && (h.Shards == 0 || p.target.ShardID.Value >= h.Shards) {
					return typeValidation("routing shard", "unavailable shard")
				}
				return nil
			}
		}
	}
	return typeValidation("routing host", "host is not eligible")
}
func (p *singleTargetRouting) Plan(_ RoutingInfo, m *MetadataSnapshot) QueryPlan {
	plan := &targetPlan{}
	if m == nil {
		return plan
	}
	for _, h := range m.hosts {
		if h.ID == p.target.HostID && h.Enabled && h.Up {
			t := p.target
			if t.ShardID.Set && (h.Shards == 0 || t.ShardID.Value >= h.Shards) {
				if p.strict {
					return plan
				}
				t.ShardID = Option[uint32]{}
			}
			plan.targets = []Target{t}
			break
		}
	}
	return plan
}
func (*singleTargetRouting) Observe(AttemptEvent) {}
