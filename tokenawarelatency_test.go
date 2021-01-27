package gocql

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"
)

func TestTokenAwareLatencyHost_Latency(t *testing.T) {
	t.Run("no data", func(t *testing.T) {
		h := tokenAwareLatencyHost{
			current: newBigWriterBucket(),
		}
		value, ok := h.latency()
		if ok {
			t.Errorf("expected to get no data, got %v %v", value, ok)
		}
		h.decay()
		value, ok = h.latency()
		if ok {
			t.Errorf("expected to get no data, got %v %v", value, ok)
		}
	})
	t.Run("single bucket", func(t *testing.T) {
		h := tokenAwareLatencyHost{
			current: newBigWriterBucket(),
		}
		h.recordLatency(12 * time.Millisecond)
		h.recordLatency(16 * time.Millisecond)
		value, ok := h.latency()
		if !ok || value != 14*time.Millisecond {
			t.Errorf("expected to get average, got %v %v", value, ok)
		}
		// decay doesn't change the value if we don't have new data.
		h.decay()
		value, ok = h.latency()
		if !ok || value != 14*time.Millisecond {
			t.Errorf("expected to get average, got %v %v", value, ok)
		}
		// but the data is discarded after the period elapses.
		for i := 0; i < bucketCount-1; i++ {
			h.decay()
		}
		value, ok = h.latency()
		if ok {
			t.Errorf("expected to get no data, got %v %v", value, ok)
		}
	})
	t.Run("multiple buckets", func(t *testing.T) {
		h := tokenAwareLatencyHost{
			current: newBigWriterBucket(),
		}
		// Newest bucket has weight bucketCount (64), oldest bucket has weight 1.
		// We first log latency and then decay it, so that it has 1/4 of initial weight.
		// Since we are subtracting weight with each decay, we need to call decay 3/4*bucketCount times.
		h.recordLatency(10 * time.Millisecond)
		for i := 0; i < bucketCount*3/4; i++ {
			h.decay()
		}
		// We record latency to the newest bucket again, so this one has full bucketCount weight.
		h.recordLatency(15 * time.Millisecond)
		value, ok := h.latency()
		// Weighted average of 10 with weight 1/4*bucketCount and 15 with weight bucketCount is
		// 10 * 1/4 * bucketCount + 15 * bucketCount / (1/4 * bucketCount + bucketCount) = (10 * 1/4 + 15) / (5/4) = 14
		if !ok || value != 14*time.Millisecond {
			t.Errorf("expected to get weighted average, got %v %v", value, ok)
		}
	})
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAwareLatency_NetworkStrategy_NoLatencyData(t *testing.T) {
	const keyspace = "keyspace"
	policy, err := NewTokenAwareLatencyHostPolicy(TokenAwareLatencyHostPolicyOptions{
		ExplorationPortion:      0.1,
		DecayPeriod:             1 * time.Minute,
		LocalDatacenter:         "local",
		RemoteDatacenterPenalty: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	policy.explorationPortion = 0 // override this to have deterministic test
	policy.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "00", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "01", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "02", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "03", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "04", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "05", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "06", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "07", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "08", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "09", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	policy.AddHosts(hosts[:])

	policy.SetPartitioner("OrderedPartitioner")

	policy.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   2,
				"remote1": 2,
				"remote2": 2,
			},
		}, nil
	}
	replicasUpdateChan := make(chan struct{})
	policy.afterReplicasUpdated = func(keyspaceName string) {
		if keyspaceName == keyspace {
			close(replicasUpdateChan)
		}
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})
	<-replicasUpdateChan

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", tokenRingReplicas{
		{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3], hosts[4], hosts[5]}},
		{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4], hosts[5], hosts[6]}},
		{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5], hosts[6], hosts[7]}},
		{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6], hosts[7], hosts[8]}},
		{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7], hosts[8], hosts[9]}},
		{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8], hosts[9], hosts[10]}},
		{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9], hosts[10], hosts[11]}},
		{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10], hosts[11], hosts[0]}},
		{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11], hosts[0], hosts[1]}},
		{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0], hosts[1], hosts[2]}},
		{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1], hosts[2], hosts[3]}},
		{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2], hosts[3], hosts[4]}},
	}, policy.keyspaces[keyspace].replicas)
	if len(policy.keyspaces) != 1 {
		t.Fatalf("expected only single keyspace, got %v", policy.keyspaces)
	}

	// now the token ring is configured
	query.RoutingKey([]byte("18"))
	iter = policy.Pick(query)

	selectedHostList := iterToSlice(iter)
	if len(selectedHostList) != 12 {
		t.Fatalf("expected 12 hosts, got %v", selectedHostList)
	}
	sort.Strings(selectedHostList[2:6])  // order of non-local replicas does not matter
	sort.Strings(selectedHostList[6:12]) // order of other hosts does not matter
	expected := []string{"04", "07", "03", "05", "06", "08", "00", "01", "02", "09", "10", "11"}
	assertDeepEqual(t, "selected hosts", expected, selectedHostList)
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAwareLatency_NetworkStrategy_WithLatencyData(t *testing.T) {
	const keyspace = "keyspace"
	policy, err := NewTokenAwareLatencyHostPolicy(TokenAwareLatencyHostPolicyOptions{
		ExplorationPortion:      0.1,
		DecayPeriod:             1 * time.Minute,
		LocalDatacenter:         "local",
		RemoteDatacenterPenalty: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	policy.explorationPortion = 0 // override this to have deterministic test
	policy.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return nil, errors.New("not initialized")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "00", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "01", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "02", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "03", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "04", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "05", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "06", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "07", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "08", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "09", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	policy.AddHosts(hosts[:])

	policy.SetPartitioner("OrderedPartitioner")

	policy.getKeyspaceMetadata = func(keyspaceName string) (*KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]interface{}{
				"class":   "NetworkTopologyStrategy",
				"local":   2,
				"remote1": 2,
				"remote2": 2,
			},
		}, nil
	}
	replicasUpdateChan := make(chan struct{})
	policy.afterReplicasUpdated = func(keyspaceName string) {
		if keyspaceName == keyspace {
			close(replicasUpdateChan)
		}
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})
	<-replicasUpdateChan

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", tokenRingReplicas{
		{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3], hosts[4], hosts[5]}},
		{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4], hosts[5], hosts[6]}},
		{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5], hosts[6], hosts[7]}},
		{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6], hosts[7], hosts[8]}},
		{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7], hosts[8], hosts[9]}},
		{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8], hosts[9], hosts[10]}},
		{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9], hosts[10], hosts[11]}},
		{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10], hosts[11], hosts[0]}},
		{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11], hosts[0], hosts[1]}},
		{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0], hosts[1], hosts[2]}},
		{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1], hosts[2], hosts[3]}},
		{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2], hosts[3], hosts[4]}},
	}, policy.keyspaces[keyspace].replicas)
	if len(policy.keyspaces) != 1 {
		t.Fatalf("expected only single keyspace, got %v", policy.keyspaces)
	}

	policy.hosts["01"].recordLatency(1 * time.Millisecond)
	policy.hosts["05"].recordLatency(7 * time.Millisecond)
	policy.hosts["07"].recordLatency(8 * time.Millisecond)
	policy.hosts["04"].recordLatency(10 * time.Millisecond)
	policy.hosts["06"].recordLatency(11 * time.Millisecond)
	policy.hosts["03"].recordLatency(15 * time.Millisecond)
	// 08 does not have latency data, as a replica it should be before 01

	// now the token ring is configured
	query.RoutingKey([]byte("18"))
	iter = policy.Pick(query)

	selectedHostList := iterToSlice(iter)
	if len(selectedHostList) != 12 {
		t.Fatalf("expected 12 hosts, got %v", selectedHostList)
	}
	sort.Strings(selectedHostList[7:12]) // order of other hosts does not matter
	expected := []string{"05", "07", "04", "06", "03", "08", "01", "00", "02", "09", "10", "11"}
	assertDeepEqual(t, "selected hosts", expected, selectedHostList)
}

func iterToSlice(iter NextHost) []string {
	var out []string
	for {
		host := iter()
		if host == nil {
			break
		}
		var hostID string
		if host.Info() != nil {
			hostID = host.Info().HostID()
		}
		out = append(out, hostID)
	}
	return out
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAwareLatency_NetworkStrategy_SlowKeyspaceMetadata(t *testing.T) {
	const keyspace = "keyspace"
	policy, err := NewTokenAwareLatencyHostPolicy(TokenAwareLatencyHostPolicyOptions{
		ExplorationPortion:      0.1,
		DecayPeriod:             1 * time.Minute,
		LocalDatacenter:         "local",
		RemoteDatacenterPenalty: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	keyspaceMetadataChan := make(chan struct{})
	defer close(keyspaceMetadataChan)
	policy.explorationPortion = 0 // override this to have deterministic test
	policy.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		<-keyspaceMetadataChan // blocks until end of test
		return nil, errors.New("some error")
	}

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		{hostId: "00", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "01", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "02", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "03", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "04", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "05", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "06", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "07", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "08", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "09", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
	}
	policy.AddHosts(hosts[:])

	policy.SetPartitioner("OrderedPartitioner")

	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// now the token ring is still not configured.
	query.RoutingKey([]byte("18"))
	iter = policy.Pick(query)

	selectedHostList := iterToSlice(iter)
	if len(selectedHostList) != 12 {
		t.Fatalf("expected 12 hosts, got %v", selectedHostList)
	}
	sort.Strings(selectedHostList[1:12]) // order of other hosts does not matter
	expected := []string{"03", "00", "01", "02", "04", "05", "06", "07", "08", "09", "10", "11"}
	assertDeepEqual(t, "selected hosts", expected, selectedHostList)
}
