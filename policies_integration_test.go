//go:build integration
// +build integration

package gocql

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestDCValidationTokenAware(t *testing.T) {
	t.Parallel()
	testV2InvalidDatacenter(t, RoutingOptions{TokenAware: true, Datacenter: "WRONG_DC"})
}

func TestDCValidationDCAware(t *testing.T) {
	t.Parallel()
	testV2InvalidDatacenter(t, RoutingOptions{Datacenter: "WRONG_DC"})
}

func TestDCValidationRackAware(t *testing.T) {
	t.Parallel()
	testV2InvalidDatacenter(t, RoutingOptions{Datacenter: "WRONG_DC", Rack: "RACK"})
}

func testV2InvalidDatacenter(t *testing.T, options RoutingOptions) {
	t.Helper()
	cluster := createV2Cluster()
	v2ConfigureRouting(t, &cluster, options)
	session, err := Connect(context.Background(), cluster)
	if session != nil {
		session.Close()
	}
	if err == nil {
		t.Fatal("Connect was expected to fail with an unknown datacenter")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected datacenter validation error, got %T: %v", err, err)
	}
}

func TestTokenAwareHostPolicy(t *testing.T) {
	t.Parallel()
	for _, configuredKeyspace := range []bool{true, false} {
		name := "no-keyspace"
		if configuredKeyspace {
			name = "keyspace"
		}
		t.Run(name, func(t *testing.T) {
			bootstrap := createV2Session(t)
			defer bootstrap.Close()
			keyspace := v2SchemaRoutingName(t)
			v2CreateRoutingKeyspace(t, bootstrap, keyspace, 1)
			defer cleanup(t, bootstrap, keyspace)
			cluster := createV2Cluster()
			v2ConfigureRouting(t, &cluster, RoutingOptions{TokenAware: true})
			if configuredKeyspace {
				cluster.Keyspace = keyspace
			}
			testIfPolicyInitializedProperly(t, cluster, keyspace)
		})
	}
}

func testIfPolicyInitializedProperly(t *testing.T, cluster Config, keyspace string) {
	t.Helper()
	ctx := context.Background()
	session, err := Connect(ctx, cluster)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer session.Close()
	metadata := session.Metadata()
	hosts := metadata.Hosts()
	if len(hosts) == 0 {
		t.Fatal("token-aware metadata has no hosts")
	}
	tokens := 0
	for _, host := range hosts {
		tokens += len(host.Tokens)
	}
	if tokens == 0 {
		t.Fatal("token-aware metadata has no tokens")
	}
	local, err := v2ReadOne(ctx, session, "SELECT partitioner FROM system.local")
	if err != nil {
		t.Fatal(err)
	}
	partitionerValue, err := local.Get("partitioner")
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := partitionerValue.Text()
	if err != nil || partitioner.String() == "" {
		t.Fatalf("missing partitioner: %v", err)
	}
	ranges, err := metadata.TokenRanges(keyspace, "")
	if err != nil || len(ranges) == 0 {
		t.Fatalf("token-aware metadata has no keyspace ranges: %v", err)
	}
	for _, tokenRange := range ranges {
		if len(tokenRange.Replicas) == 0 {
			t.Fatal("token-aware metadata has a range without replicas")
		}
	}
	table := v2SchemaRoutingName(t)
	if err := v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE %s.%s (pk int PRIMARY KEY, v int)", keyspace, table)); err != nil {
		t.Fatal(err)
	}
	if _, err := v2Exec(ctx, session, fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, table), IntValue(7), IntValue(9)); err != nil {
		t.Fatal(err)
	}
	prepared, err := Prepare(ctx, session, Statement[[]Value, DynamicRow]{CQL: fmt.Sprintf("SELECT pk, v FROM %s.%s WHERE pk = ?", keyspace, table), Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindOwned(prepared, []Value{IntValue(7)})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	routing := bound.Routing()
	if !routing.Token.Set {
		t.Fatal("prepared partition key has no routing token")
	}
	replicas, err := session.Metadata().Replicas(keyspace, table, routing.Token.Value)
	if err != nil || len(replicas) == 0 {
		t.Fatalf("missing bound token replicas: %v", err)
	}
	for _, options := range []RoutingOptions{
		{TokenAware: true},
		{TokenAware: true, Datacenter: hosts[0].Datacenter},
		{TokenAware: true, Datacenter: hosts[0].Datacenter, Rack: hosts[0].Rack},
	} {
		policy, err := NewRoutingPolicy(options)
		if err != nil {
			t.Fatal(err)
		}
		profile := cluster.DefaultProfile.Snapshot()
		profile.Routing = policy
		handle, err := NewProfile(profile)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := Run(ctx, session, bound, Options{Profile: handle, Consistency: Option[Consistency]{Value: One, Set: true}})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			rows.Close()
			t.Fatalf("missing routed row: %v", rows.Err())
		}
		if v2SchemaRoutingInt(t, rows.Row(), "pk") != 7 || v2SchemaRoutingInt(t, rows.Row(), "v") != 9 {
			rows.Close()
			t.Fatal("unexpected routed row")
		}
		coordinator := rows.Info().HostID
		if rows.Next() {
			rows.Close()
			t.Fatal("unexpected additional routed row")
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, replica := range replicas {
			found = found || replica.HostID == coordinator
		}
		if !found {
			t.Fatalf("coordinator %s is not a token replica: %v", coordinator, replicas)
		}
		for _, host := range session.Metadata().Hosts() {
			if host.ID == coordinator && options.Datacenter != "" && host.Datacenter != options.Datacenter {
				t.Fatalf("coordinator is outside configured datacenter: %+v", host)
			}
		}
	}
}

// A test-only metadata input models down/removal events while retaining actual
// query admission and routing. Session metadata snapshots are immutable, so this
// fault injection cannot change shared HostInfo objects used by other sessions.
type v2UnavailableHostsRouting struct {
	RoutingPolicy
	oneHost bool
}

func (p v2UnavailableHostsRouting) Plan(info RoutingInfo, metadata *MetadataSnapshot) QueryPlan {
	snapshot := *metadata
	snapshot.hosts = metadata.Hosts()
	for i := range snapshot.hosts {
		snapshot.hosts[i].Up = false
	}
	if p.oneHost && len(snapshot.hosts) > 1 {
		snapshot.hosts = snapshot.hosts[:1]
	}
	return p.RoutingPolicy.Plan(info, &snapshot)
}

// Keep the all-hosts-down disruption test serialized as in the original suite.
//
//nolint:paralleltest
func TestNoHangAllHostsDown(t *testing.T) {
	session := createV2Session(t)
	defer session.Close()
	hosts := session.Metadata().Hosts()
	if len(hosts) == 0 {
		t.Fatal("no hosts")
	}
	dc, rack := hosts[0].Datacenter, hosts[0].Rack
	policies := []RoutingOptions{
		{Datacenter: dc, AllowRemote: true},
		{Datacenter: dc},
		{TokenAware: true, Datacenter: dc, AllowRemote: true},
		{TokenAware: true, Datacenter: dc},
		{Datacenter: dc, Rack: rack, AllowRemote: true},
		{Datacenter: dc, Rack: rack},
		{TokenAware: true, Datacenter: dc, Rack: rack, AllowRemote: true},
		{TokenAware: true, Datacenter: dc, Rack: rack},
		{},
	}
	bound, err := BindUnpreparedOwned(Statement[Unit, DynamicRow]{CQL: "SELECT host_id FROM system.local", Params: NoParams, Rows: DynamicRows}, NewColumns(), Unit{})
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	for i, options := range policies {
		policy, err := NewRoutingPolicy(options)
		if err != nil {
			t.Fatal(err)
		}
		for _, oneHost := range []bool{false, true} {
			profile := createV2Cluster().DefaultProfile.Snapshot()
			profile.Routing = v2UnavailableHostsRouting{RoutingPolicy: policy, oneHost: oneHost}
			handle, err := NewProfile(profile)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			rows, queryErr := Run(ctx, session, bound, Options{Profile: handle})
			if rows != nil {
				queryErr = rows.Close()
			}
			if ctx.Err() != nil {
				t.Errorf("policy %d oneHost=%t hung when every host was down: %v", i, oneHost, queryErr)
			}
			if queryErr == nil {
				t.Errorf("policy %d oneHost=%t unexpectedly executed with every host down", i, oneHost)
			}
			cancel()
		}
	}
}
