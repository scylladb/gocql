package gocql

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"bytes"
	"math"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2StreamingPartitioners(t *testing.T) {
	for length := 0; length < 96; length++ {
		input := make([]byte, length)
		for i := range input {
			input[i] = byte(i*131 + 177)
		}
		expected := murmur.Murmur3H1(input)
		if expected == math.MinInt64 {
			expected = math.MaxInt64
		}
		for split := 0; split <= length; split++ {
			chain, err := wire.NewChain(wire.Borrow(input[:split]), wire.Borrow(input[split:]))
			if err != nil {
				t.Fatal(err)
			}
			got, err := Murmur3TokenPartitioner.Token(*routing.NewPartitionKey(routing.PartitionKeyState{InternalChain: chain}))
			if err != nil || got != expected {
				t.Fatalf("length=%d split=%d: got %d,%v want %d", length, split, got, err, expected)
			}
		}
	}
	for _, test := range []struct {
		input    []byte
		expected int64
	}{{[]byte{1, 2}, math.MinInt64}, {[]byte{0x80, 0, 0, 0, 0, 0, 0, 0}, math.MaxInt64}, {[]byte{0, 0, 0, 0, 0, 0, 0, 3, 9}, 3}} {
		var spans []wire.Span
		for i := range test.input {
			spans = append(spans, wire.Borrow(test.input[i:i+1]))
		}
		chain, _ := wire.NewChain(spans...)
		got, err := CDCTokenPartitioner.Token(*routing.NewPartitionKey(routing.PartitionKeyState{InternalChain: chain}))
		if err != nil || got != test.expected {
			t.Fatalf("CDC=%d,%v want %d", got, err, test.expected)
		}
	}
}
func TestV2PartitionKeyCanonicalStream(t *testing.T) {
	first := []byte{1, 2}
	second := []byte{3}
	a, _ := wire.NewChain(wire.Borrow(first))
	b, _ := wire.NewChain(wire.Borrow(second))
	cells := []encodedCell{{State: Present, Body: a}, {State: Present, Body: b}}
	key, complete, err := newPartitionKey(cells, []int{1, 0})
	if err != nil || !complete {
		t.Fatal(complete, err)
	}
	var output bytes.Buffer
	err = key.ForEach(func(b Bytes) error { _, err := b.WriteTo(&output); return err })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), []byte{0, 1, 3, 0, 0, 2, 1, 2, 0}) {
		t.Fatal(output.Bytes())
	}
	// Input payload spans are reused; the caller's borrowing contract is real.
	first[0] = 9
	output.Reset()
	_ = key.ForEach(func(b Bytes) error { _, err := b.WriteTo(&output); return err })
	if output.Bytes()[6] != 9 {
		t.Fatal("partition payload copied")
	}
	cells[0].State = Null
	if _, ok, err := newPartitionKey(cells, []int{0, 1}); err != nil || ok {
		t.Fatal("null key must disable token routing")
	}
	if _, _, err := newPartitionKey(cells, []int{3}); err == nil {
		t.Fatal("invalid key index accepted")
	}
	large, _ := wire.NewChain(wire.Borrow(make([]byte, 65536)))
	cells[0] = encodedCell{State: Present, Body: large}
	if _, _, err := newPartitionKey(cells, []int{0, 1}); err == nil {
		t.Fatal("oversized composite accepted")
	}
}
func v2TestHost(id byte, dc, rack string) HostSnapshot {
	var uuid UUID
	uuid[15] = id
	return HostSnapshot{ID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *MetadataSnapshot {
	hosts := []HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return routing.NewMetadataSnapshot(routing.SnapshotState{InternalHostIndex: buildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*KeyspaceMetadata{"ks": {Tables: map[string]*TableMetadata{"table": {Name: "table"}}}}, InternalRanges: map[string][]TokenRange{"ks": {{Start: 0, End: 0, Replicas: []Target{{HostID: hosts[2].ID}, {HostID: hosts[1].ID}, {HostID: hosts[0].ID}}}}}, InternalTablets: map[metadataTable][]TokenRange{}})
}
func v2PlanIDs(p QueryPlan) []byte {
	var result []byte
	for {
		v, ok := p.Next()
		if !ok {
			return result
		}
		result = append(result, v.HostID[15])
	}
}
func TestV2RoutingOrderAndIndependentPlans(t *testing.T) {
	m := v2TestMetadata()
	policy, err := NewRoutingPolicy(RoutingOptions{TokenAware: true, Datacenter: "a", Rack: "r1", AllowRemote: true, ShuffleReplicas: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Validate(m); err != nil {
		t.Fatal(err)
	}
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true, Value: 1}, Conditional: true}
	a, b := policy.Plan(info, m), policy.Plan(info, m)
	want := []byte{1, 2, 3, 4}
	if got := v2PlanIDs(a); !bytes.Equal(got, want) {
		t.Fatalf("plan=%v want %v", got, want)
	}
	if got := v2PlanIDs(b); !bytes.Equal(got, want) {
		t.Fatalf("second plan=%v", got)
	}
	for i := 0; i < 20; i++ {
		info.Conditional = false
		info.Consistency = Serial
		if got := v2PlanIDs(policy.Plan(info, m)); !bytes.Equal(got, want) {
			t.Fatal("SERIAL order changed", got)
		}
	}
	routing.SnapshotData(m).InternalHosts[0].Enabled = false
	if got := v2PlanIDs(policy.Plan(info, m)); bytes.Contains(got, []byte{1}) {
		t.Fatal("disabled host in plan")
	}
	bad, _ := NewRoutingPolicy(RoutingOptions{Datacenter: "missing"})
	if err := bad.Validate(m); err == nil {
		t.Fatal("missing DC validated")
	}
}
func TestV2RoutingTabletMissAndStrictShard(t *testing.T) {
	m := v2TestMetadata()
	routing.SnapshotData(m).InternalTablets[metadataTable{InternalKeyspace: "ks", InternalTable: "table"}] = []TokenRange{{Start: 10, End: 20, Replicas: []Target{{HostID: routing.SnapshotData(m).InternalHosts[1].ID, ShardID: Option[uint32]{Value: 2, Set: true}}}}}
	if got, err := m.Replicas("ks", "table", 30); err != nil || len(got) != 0 {
		t.Fatal("tablet miss fell back to vnode", got, err)
	}
	single := SingleTargetRouting(Target{HostID: routing.SnapshotData(m).InternalHosts[0].ID, ShardID: Option[uint32]{Value: 9, Set: true}}, true)
	if err := single.Validate(m); err == nil {
		t.Fatal("invalid strict shard accepted")
	}
	if _, ok := single.Plan(RoutingInfo{}, m).Next(); ok {
		t.Fatal("strict shard fell back")
	}
	single = SingleTargetRouting(Target{HostID: routing.SnapshotData(m).InternalHosts[0].ID, ShardID: Option[uint32]{Value: 9, Set: true}}, false)
	target, ok := single.Plan(RoutingInfo{}, m).Next()
	if !ok || target.ShardID.Set {
		t.Fatal("hint fallback failed", target, ok)
	}
}
func TestV2LatencyRoutingDeprioritizesAndRetries(t *testing.T) {
	policy, _ := NewRoutingPolicy(RoutingOptions{Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: time.Hour}})
	p := policy.(*routingPolicy)
	m := v2TestMetadata()
	for i, h := range routing.SnapshotData(m).InternalHosts {
		latency := time.Millisecond
		if i == 0 {
			latency = time.Second
		}
		p.Observe(AttemptEvent{Target: Target{HostID: h.ID}, Started: time.Now(), Latency: latency})
	}
	_ = p.Plan(RoutingInfo{}, m)
	ids := v2PlanIDs(p.Plan(RoutingInfo{}, m))
	if ids[len(ids)-1] != 1 {
		t.Fatal("slow host was not deferred", ids)
	}
}
