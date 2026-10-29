//go:build integration
// +build integration

// Copyright (C) 2017 ScyllaDB

package gocql

import (
	"context"
	"errors"
	"flag"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

// cassandraOverride returns override when running against Cassandra and it's set, else base.
func cassandraOverride(base, override string) string {
	if override != "" && *flagDistribution == "cassandra" {
		return override
	}
	return base
}

func TestRecreateSchema(t *testing.T) {
	t.Parallel()

	failsOnOldScylla := false
	if *flagDistribution == "scylla" && flagCassVersion.Before(2024, 0, 0) {
		failsOnOldScylla = true
	}

	session := createV2SessionFromConfig(createV2Cluster(), t, true)
	defer session.Close()

	getStmtFromCluster := isDescribeKeyspaceSupported(t, session)
	tabletsAutoEnabled := v2SchemaRoutingTabletsAutoEnabled(t, session)

	tcs := []struct {
		Name            string
		FixedKeyspace   string // original keyspace name used in .cql files
		FailWithTablets bool
		Input           string
		CassandraInput  string // overrides Input on Cassandra, when its CQL syntax requirements differ
		Golden          string
		CassandraGolden string // overrides Golden on Cassandra, when the rendered dump differs
	}{
		{
			Name:          "Keyspace",
			FixedKeyspace: "gocqlx_keyspace",
			Input:         "testdata/recreate/keyspace.cql",
			Golden:        "testdata/recreate/keyspace_golden.cql",
		},
		{
			Name:          "Table",
			FixedKeyspace: "gocqlx_table",
			Input:         "testdata/recreate/table.cql",
			Golden:        "testdata/recreate/table_golden.cql",
		},
		{
			Name:            "Materialized Views",
			FixedKeyspace:   "gocqlx_mv",
			FailWithTablets: failsOnOldScylla,
			Input:           "testdata/recreate/materialized_views.cql",
			// Cassandra requires every MV primary key column not in the SELECT list
			// to be explicitly restricted with IS NOT NULL; Scylla does not.
			CassandraInput: "testdata/recreate/materialized_views_cassandra.cql",
			Golden:         "testdata/recreate/materialized_views_golden.cql",
			// Cassandra's rendered dump includes the extra IS NOT NULL restriction above.
			CassandraGolden: "testdata/recreate/materialized_views_golden_cassandra.cql",
		},
		{
			Name:            "Index",
			FixedKeyspace:   "gocqlx_idx",
			FailWithTablets: failsOnOldScylla,
			Input:           "testdata/recreate/index.cql",
			Golden:          "testdata/recreate/index_golden.cql",
		},
		{
			Name:            "Secondary Index",
			FixedKeyspace:   "gocqlx_sec_idx",
			FailWithTablets: failsOnOldScylla,
			Input:           "testdata/recreate/secondary_index.cql",
			// Cassandra's CQL grammar doesn't accept Scylla's multi-column
			// index target syntax "((location), name)"; use a single-column index.
			CassandraInput:  "testdata/recreate/secondary_index_cassandra.cql",
			Golden:          "testdata/recreate/secondary_index_golden.cql",
			CassandraGolden: "testdata/recreate/secondary_index_cassandra_golden.cql",
		},
		{
			Name:          "UDT",
			FixedKeyspace: "gocqlx_udt",
			Input:         "testdata/recreate/udt.cql",
			Golden:        "testdata/recreate/udt_golden.cql",
		},
		{
			Name:          "Aggregates",
			FixedKeyspace: "gocqlx_aggregates",
			Input:         "testdata/recreate/aggregates.cql",
			Golden:        "testdata/recreate/aggregates_golden.cql",
		},
	}

	for i := range tcs {
		test := tcs[i]
		t.Run(test.Name, func(t *testing.T) {
			if test.Name == "UDT" && *flagDistribution == "scylla" && flagCassVersion.Major == 2024 && flagCassVersion.Minor == 1 {
				t.Skip("Doesn't work properly on Scylla 2024.1 due to https://github.com/scylladb/scylladb/issues/26761")
			}

			// Generate a unique keyspace name to avoid collisions under parallel execution.
			// Replace the fixed keyspace name in CQL input/golden files with the unique name.
			ks := v2SchemaRoutingName(t)
			cleanup(t, session, ks)

			input := cassandraOverride(test.Input, test.CassandraInput)
			in, err := os.ReadFile(input)
			if err != nil {
				t.Fatal(err)
			}

			// Substitute the fixed keyspace name in the CQL input with the unique name.
			inStr := strings.ReplaceAll(string(in), test.FixedKeyspace, ks)

			queries := trimQueries(strings.Split(inStr, ";"))
			for _, q := range queries {
				_, err = v2Exec(context.Background(), session, q)
				if err != nil {
					break
				}
			}

			if tabletsAutoEnabled && test.FailWithTablets {
				if err == nil {
					t.Errorf("did not get expected error or tablets")
				} else if strings.Contains(err.Error(), "not supported") && strings.Contains(err.Error(), "tablets") {
					return
				} else {
					t.Fatal("query failed with unexpected error", err)
				}
			} else if err != nil {
				t.Fatal("invalid input query", err)
			}

			if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := session.RefreshMetadata(context.Background()); err != nil {
				t.Fatal(err)
			}
			km, ok := session.Metadata().Keyspace(ks)
			if !ok {
				t.Fatal("keyspace missing from typed metadata snapshot:", ks)
			}
			dump, err := km.ToCQL()
			if err != nil {
				t.Fatal("recreate schema", err)
			}

			dump = trimSchema(dump)

			// Normalize the dump back to the fixed keyspace name for comparison with golden files.
			dump = strings.ReplaceAll(dump, ks, test.FixedKeyspace)

			var golden []byte
			if getStmtFromCluster {
				golden, err = getV2CreateStatements(context.Background(), session, ks)
				if err != nil {
					t.Fatal(err)
				}
				golden = []byte(trimSchema(string(golden)))
				// Normalize from the cluster's unique keyspace name back to fixed name.
				golden = []byte(strings.ReplaceAll(string(golden), ks, test.FixedKeyspace))
			} else {
				goldenPath := cassandraOverride(test.Golden, test.CassandraGolden)
				if *updateGolden {
					if err := os.WriteFile(goldenPath, []byte(dump), 0644); err != nil {
						t.Fatal(err)
					}
				}
				golden, err = os.ReadFile(goldenPath)
				if err != nil {
					t.Fatal(err)
				}
				golden = []byte(trimSchema(string(golden)))
			}

			goldenQueries := trimQueries(sortQueries(strings.Split(string(golden), ";")))
			dumpQueries := trimQueries(sortQueries(strings.Split(dump, ";")))

			if len(goldenQueries) != len(dumpQueries) {
				t.Fatalf("Expected len(dumpQueries) to be %d, got %d", len(goldenQueries), len(dumpQueries))
			}
			// Compare with golden
			for i, dq := range dumpQueries {
				gq := goldenQueries[i]

				if diff := cmp.Diff(gq, dq); diff != "" {
					t.Errorf("dumpQueries[%d] diff\n%s", i, diff)
				}
			}

			// Exec dumped queries to check if they are CQL-correct.
			// Substitute the fixed keyspace name back to the unique name for execution.
			cleanup(t, session, ks)

			for _, q := range trimQueries(strings.Split(strings.ReplaceAll(dump, test.FixedKeyspace, ks), ";")) {
				if _, err := v2Exec(context.Background(), session, q); err != nil {
					t.Fatal("invalid dump query", q, err)
				}
			}

			// Check if new dump is the same as previous
			err = session.AwaitSchemaAgreement(context.Background())
			if err != nil {
				t.Fatal("failed to await for schema agreement", err)
			}
			err = session.RefreshMetadata(context.Background())
			if err != nil {
				t.Fatal("failed to read schema for keyspace", err)
			}
			km, ok = session.Metadata().Keyspace(ks)
			if !ok {
				t.Fatal("recreated keyspace missing from typed metadata snapshot:", ks)
			}
			secondDump, err := km.ToCQL()
			if err != nil {
				t.Fatal("recreate schema", err)
			}

			secondDump = trimSchema(secondDump)
			// Normalize the second dump back to fixed keyspace name for comparison.
			secondDump = strings.ReplaceAll(secondDump, ks, test.FixedKeyspace)

			secondDumpQueries := trimQueries(sortQueries(strings.Split(secondDump, ";")))

			if !cmp.Equal(secondDumpQueries, dumpQueries) {
				t.Errorf("first dump and second one differs: %s", cmp.Diff(secondDumpQueries, dumpQueries))
			}
		})
	}
}

func isDescribeKeyspaceSupported(t *testing.T, s *Session) bool {
	t.Helper()

	rows, err := v2Query(context.Background(), s, `DESCRIBE KEYSPACE system WITH INTERNALS`)
	if err == nil {
		err = rows.Close()
	}
	if err != nil {
		var server RequestError
		if errors.As(err, &server) && server.GetCode() == ErrCodeSyntax {
			// DESCRIBE KEYSPACE is not supported on older versions of Cassandra and Scylla
			// For such case schema statement is going to be recreated on the client side
			return false
		}
		t.Fatalf("error querying keyspace schema: %v", err)
	}
	return true
}

func cleanup(t *testing.T, session *Session, keyspace string) {
	if err := v2CreateTable(context.Background(), session, `DROP KEYSPACE IF EXISTS `+keyspace); err != nil {
		t.Fatalf("unable to drop keyspace: %v", err)
	}
}

func sortQueries(in []string) []string {
	q := trimQueries(in)
	sort.Strings(q)
	return q
}

func trimQueries(in []string) []string {
	queries := in[:0]
	for _, q := range in {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		if len(q) != 0 {
			queries = append(queries, q)
		}
	}
	return queries
}

var schemaVersion = regexp.MustCompile(` WITH ID = [0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}[ \t\n]+AND`)

func trimSchema(s string) string {
	// Remove temporary items from the scheme, in particular schema version:
	// ) WITH ID = cf0364d0-3b85-11ef-b79d-80a2ee1928c0
	return strings.ReplaceAll(schemaVersion.ReplaceAllString(s, " WITH"), "\n\n", "\n")
}

// Probe server-default tablet behavior in an isolated keyspace using typed rows.
func v2SchemaRoutingTabletsAutoEnabled(t *testing.T, session *Session) bool {
	t.Helper()
	supported, err := v2TabletsSupported(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		return false
	}
	keyspace := v2SchemaRoutingName(t)
	v2CreateRoutingKeyspace(t, session, keyspace, 1)
	defer cleanup(t, session, keyspace)
	statements, err := getV2CreateStatements(context.Background(), session, keyspace)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(strings.ToLower(string(statements)), "and tablets")
}
