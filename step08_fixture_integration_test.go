//go:build integration

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// The runner always invokes cleanup after its test subprocess, even on failure.
// A random run prefix is required; default/manual suites cannot trigger it.
func TestStep08Cleanup(t *testing.T) {
	if os.Getenv("GOCQL_FIXTURE_CLEANUP") != "1" {
		t.Skip("cleanup runs only in the native runner's explicit post-package subprocess")
	}
	step08CleanupNamespaces(t, nil)
}

// A nil allowlist cleans the whole owned run after its test process joins. A
// non-nil allowlist is used mid-package only for exact killed/joined children.
func step08CleanupNamespaces(t *testing.T, allowed []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		t.Skip("requires an owned fixture run")
	}
	owns := func(name string) bool { return name == prefix || strings.HasPrefix(name, prefix+"_") }
	selected := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		if !owns(name) {
			t.Fatalf("cleanup namespace outside owned run: %q", name)
		}
		selected[name] = true
	}
	matches := func(name string) bool { return owns(name) && (allowed == nil || selected[name]) }
	config := createV2Cluster(func(c *Config) {
		c.Metadata.SchemaEvents = false
		c.Metadata.TopologyEvents = false
		c.Metadata.StatusEvents = false
		c.Metadata.AutomaticAgreement = false
		c.Metadata.RefreshAfterAgreement = false
	})
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		value, err := rows.Row().At(0)
		if err != nil {
			t.Fatal(err)
		}
		name, err := value.Text()
		if err != nil {
			t.Fatal(err)
		}
		if matches(name.String()) {
			names = append(names, name.String())
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err := v2Exec(ctx, s, "DROP KEYSPACE "+(`"`+strings.ReplaceAll(name, `"`, `""`)+`"`)); err != nil {
			t.Fatal(err)
		}
		t.Log("DROPPED", name)
	}
	if err := s.AwaitSchemaAgreement(ctx); err != nil {
		t.Fatal(err)
	}
	remaining, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces")
	if err != nil {
		t.Fatal(err)
	}
	defer remaining.Close()
	for remaining.Next() {
		value, err := remaining.Row().At(0)
		if err != nil {
			t.Fatal(err)
		}
		name, err := value.Text()
		if err != nil {
			t.Fatal(err)
		}
		if matches(name.String()) {
			t.Errorf("owned namespace survived cleanup: %s", name.String())
		}
	}
	if err := remaining.Close(); err != nil {
		t.Fatal(err)
	}

}

func step08CheckAggregate(t *testing.T, s *Session, keyspace string) {
	t.Helper()
	table := keyspace + ".fixture_values"
	if err := v2CreateTable(context.Background(), s, "CREATE TABLE "+table+" (id int PRIMARY KEY, value double)"); err != nil {
		t.Fatal(err)
	}
	for id, value := range []float64{2, 4, 9} {
		if _, err := v2Exec(context.Background(), s, "INSERT INTO "+table+" (id,value) VALUES (?,?)", IntValue(int32(id)), DoubleValue(value)); err != nil {
			t.Fatal(err)
		}
	}
	row, err := v2ReadOne(context.Background(), s, fmt.Sprintf("SELECT %s.average(value) AS average FROM %s", keyspace, table))
	if err != nil {
		t.Fatal(err)
	}
	value, err := row.At(0)
	if err != nil {
		t.Fatal(err)
	}
	average, err := value.Float()
	if err != nil || average != 5 {
		t.Fatalf("equivalent average = %v, %v", average, err)
	}
	if _, err := v2Exec(context.Background(), s, "DROP TABLE "+table); err != nil {
		t.Fatal(err)
	}
}

// Explicit lifecycle probe used only by the harness's failure/cancellation gate.
// It provisions a real owned namespace before announcing the injection point.
func TestStep08HarnessLifecycle(t *testing.T) {
	action := os.Getenv("GOCQL_FIXTURE_ACTION")
	if action == "" {
		t.Skip("explicit harness failure/cancellation probe")
	}
	s := createV2Session(t)
	defer s.Close()
	if marker := os.Getenv("GOCQL_FIXTURE_MARKER"); marker != "" {
		if err := os.WriteFile(marker, []byte(implCluster.SessionData(s).InternalCfg.Keyspace), 0600); err != nil {
			t.Fatal(err)
		}
	}
	switch action {
	case "fail":
		t.Fatal("intentional harness failure after provisioning")
	case "cancel":
		select {}
	default:
		t.Fatal("unknown lifecycle injection")
	}
}

func TestStep08FixtureIdentity(t *testing.T) {
	s, err := Connect(context.Background(), createV2Cluster())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	query := "SELECT release_version FROM system.local"
	if *flagDistribution == "scylla" {
		query = "SELECT version FROM system.versions WHERE key='local'"
	}
	row, err := v2ReadOne(context.Background(), s, query)
	if err != nil {
		t.Fatal(err)
	}
	value, err := row.At(0)
	if err != nil {
		t.Fatal(err)
	}
	version, err := value.Text()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ACTUAL_SERVER_VERSION", version.String(), "PROTOCOL", implCluster.SessionData(s).InternalCfg.ProtoVersion)
	if !strings.HasPrefix(version.String(), strings.TrimPrefix(flagCassVersion.String(), "v")) {
		t.Fatalf("unexpected server version %q", version.String())
	}
}

func TestStep08ClientCertificateEnforced(t *testing.T) {
	if !*flagRunSslTest || *flagDistribution != "scylla" {
		t.Skip("requires Scylla mutual-TLS fixture")
	}
	accepted, err := Connect(context.Background(), createV2Cluster())
	if err != nil {
		t.Fatal("valid client certificate rejected", err)
	}
	accepted.Close()
	config := createV2Cluster(func(c *Config) { c.TLS = c.TLS.Clone(); c.TLS.Certificates = nil })
	rejected, err := Connect(context.Background(), config)
	if err == nil {
		rejected.Close()
		t.Fatal("mutual-TLS server accepted missing client certificate")
	}
	t.Log("MISSING_CLIENT_CERT_REJECTED", err)
}
