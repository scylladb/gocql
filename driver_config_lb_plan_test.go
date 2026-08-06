//go:build unit
// +build unit

package gocql

import (
	"errors"
	"net"
	"testing"
)

// TestFallbackToNonPreferredNodesMatchesQueryPlan pins the reported flag
// against the plan the policy really produces, rather than against a reading
// of the configuration.
//
// The flag answers "may a request reach a node outside the reported
// node-preference?", and two independent things decide it: whether the
// fallback confines requests at all, and whether NonLocalReplicasFallback
// makes Pick serve remote replicas out of its own buckets before consulting
// the fallback. Deriving the expectation from an actual query plan is the only
// way to keep the report honest about their combination.
func TestFallbackToNonPreferredNodesMatchesQueryPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fallback HostSelectionPolicy
		opts     []func(*tokenAwareHostPolicy)
	}{
		{name: "dc-aware", fallback: DCAwareRoundRobinPolicy("local")},
		{
			name:     "dc-aware, failover disabled",
			fallback: DCAwareRoundRobinPolicy("local", HostPolicyOptionDisableDCFailover),
		},
		{
			name:     "dc-aware, failover disabled, non-local replicas allowed",
			fallback: DCAwareRoundRobinPolicy("local", HostPolicyOptionDisableDCFailover),
			opts:     []func(*tokenAwareHostPolicy){NonLocalReplicasFallback()},
		},
		{name: "rack-aware", fallback: RackAwareRoundRobinPolicy("local", "rack1")},
		{
			name:     "rack-aware, failover disabled",
			fallback: RackAwareRoundRobinPolicy("local", "rack1", HostPolicyOptionDisableDCFailover),
		},
		{name: "round-robin", fallback: RoundRobinHostPolicy()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := TokenAwareHostPolicy(tt.fallback, append(tt.opts, DontShuffleReplicas())...)
			tap := policy.(*tokenAwareHostPolicy)

			plan := queryPlanLocations(t, policy, tap)
			preference := buildNodeLocationPreferenceReport(tap)
			got := buildLoadBalancingPolicyReport(tap).(loadBalancingTokenAwareReport).FallbackToNonPreferredNodes

			if preference == nil {
				// Nothing is preferred, so nothing can be escaped and the plan
				// cannot answer the question. What must still hold is that a
				// policy constraining nothing does not claim to confine.
				if !got {
					t.Errorf("reported fallback-to-non-preferred-nodes = false with no node-preference to confine requests to")
				}
				return
			}

			// Does the plan actually reach a node the reported preference
			// excludes?
			planEscapes := false
			for _, loc := range plan {
				if !preferenceCovers(preference, loc) {
					planEscapes = true
					break
				}
			}
			if got != planEscapes {
				t.Errorf("reported fallback-to-non-preferred-nodes = %v, but the query plan %v against preference %#v reaches outside it = %v",
					got, plan, preference, planEscapes)
			}
		})
	}
}

// hostLocation is a host's datacenter and rack as the plan saw them.
type hostLocation struct{ dc, rack string }

func (l hostLocation) String() string { return l.dc + "/" + l.rack }

// preferenceCovers reports whether loc is inside the reported preference. A
// report with no preference covers everything, since it constrains nothing.
func preferenceCovers(preference any, loc hostLocation) bool {
	switch p := preference.(type) {
	case nodeLocationDCReport:
		return loc.dc == p.LocalDC
	case nodeLocationRackReport:
		return loc.dc == p.LocalDC && loc.rack == p.LocalRack
	default:
		return true
	}
}

// queryPlanLocations drives the policy over a small ring spanning two
// datacenters and two racks, and returns every host the plan yields.
func queryPlanLocations(t *testing.T, policy HostSelectionPolicy, tap *tokenAwareHostPolicy) []hostLocation {
	t.Helper()

	const keyspace = "ks"
	tap.getKeyspaceName = func() string { return keyspace }
	tap.getKeyspaceMetadata = func(string) (*KeyspaceMetadata, error) { return nil, errors.New("not initialized") }

	hosts := []*HostInfo{
		{hostId: tUUID(0), connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "local", rack: "rack1", state: NodeUp},
		{hostId: tUUID(1), connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local", rack: "rack2", state: NodeUp},
		{hostId: tUUID(2), connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote", rack: "rack9", state: NodeUp},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")
	tap.getKeyspaceMetadata = func(string) (*KeyspaceMetadata, error) {
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "NetworkTopologyStrategy",
			StrategyOptions: map[string]any{
				"class": "NetworkTopologyStrategy", "local": 2, "remote": 1,
			},
		}, nil
	}
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	query := &Query{routingInfo: &queryRoutingInfo{}}
	query.getKeyspace = func() string { return keyspace }
	query.RoutingKey([]byte("05"))

	var plan []hostLocation
	iter := policy.Pick(query)
	for selected := iter(); selected != nil; selected = iter() {
		plan = append(plan, hostLocation{dc: selected.Info().DataCenter(), rack: selected.Info().Rack()})
	}
	if len(plan) == 0 {
		t.Fatal("expected the policy to yield a query plan")
	}
	return plan
}
