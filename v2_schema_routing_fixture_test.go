//go:build integration

package gocql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func v2SchemaRoutingName(t testing.TB) string {
	t.Helper()
	return testKeyspaceName(t, strings.ReplaceAll(MustRandomUUID().String(), "-", ""))
}

func v2ConfigureRouting(t testing.TB, config *Config, options RoutingOptions) RoutingPolicy {
	t.Helper()
	policy, err := NewRoutingPolicy(options)
	if err != nil {
		t.Fatal(err)
	}
	profile := config.DefaultProfile.Snapshot()
	profile.Routing = policy
	if err := config.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	return policy
}

func v2CreateRoutingKeyspace(t testing.TB, session *Session, keyspace string, replicas int) {
	t.Helper()
	ctx := context.Background()
	cql := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '%d'}", keyspace, replicas)
	supported, err := v2TabletsSupported(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		cql += " AND tablets = {'enabled': false}"
	}
	if err := v2CreateTable(ctx, session, cql); err != nil {
		t.Fatal(err)
	}
}

func v2SchemaRoutingExec(ctx context.Context, session *Session, cql string, options Options, values ...Value) (Result, error) {
	statement := Statement[[]Value, Unit]{CQL: cql, Params: DynamicParams, Rows: NoRows}
	prepared, err := Prepare(ctx, session, statement, PrepareOptions{})
	if err != nil {
		return Result{}, err
	}
	bound, err := BindOwned(prepared, values)
	if err != nil {
		return Result{}, err
	}
	defer bound.Close()
	return Exec(ctx, session, bound, options)
}

func v2SchemaRoutingInt(t testing.TB, row DynamicRow, name string) int32 {
	t.Helper()
	value, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	result, err := value.Signed()
	if err != nil {
		t.Fatal(err)
	}
	return int32(result)
}
