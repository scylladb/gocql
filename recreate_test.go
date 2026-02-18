//go:build integration
// +build integration

// Copyright (C) 2017 ScyllaDB

package gocql

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	frm "github.com/gocql/gocql/internal/frame"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files")

func TestRecreateSchema(t *testing.T) {
	failsOnOldScylla := false
	if *flagDistribution == "scylla" && flagCassVersion.Before(2024, 0, 0) {
		failsOnOldScylla = true
	}

	session := createSessionFromClusterTabletsDisabled(createCluster(), t)
	defer session.Close()

	getStmtFromCluster := isDescribeKeyspaceSupported(t, session)
	tabletsAutoEnabled := isTabletsSupported() && isTabletsAutoEnabled()

	tcs := []struct {
		Name            string
		Keyspace        string
		FailWithTablets bool
		Input           string
		Golden          string
	}{
		{
			Name:     "Keyspace",
			Keyspace: "gocqlx_keyspace",
			Input:    "testdata/recreate/keyspace.cql",
			Golden:   "testdata/recreate/keyspace_golden.cql",
		},
		{
			Name:     "Table",
			Keyspace: "gocqlx_table",
			Input:    "testdata/recreate/table.cql",
			Golden:   "testdata/recreate/table_golden.cql",
		},
		{
			Name:            "Materialized Views",
			Keyspace:        "gocqlx_mv",
			FailWithTablets: failsOnOldScylla,
			Input:           "testdata/recreate/materialized_views.cql",
			Golden:          "testdata/recreate/materialized_views_golden.cql",
		},
		{
			Name:            "Index",
			Keyspace:        "gocqlx_idx",
			FailWithTablets: failsOnOldScylla,
			Input:           "testdata/recreate/index.cql",
			Golden:          "testdata/recreate/index_golden.cql",
		},
		{
			Name:            "Secondary Index",
			Keyspace:        "gocqlx_sec_idx",
			FailWithTablets: failsOnOldScylla,
			Input:           "testdata/recreate/secondary_index.cql",
			Golden:          "testdata/recreate/secondary_index_golden.cql",
		},
		{
			Name:     "UDT",
			Keyspace: "gocqlx_udt",
			Input:    "testdata/recreate/udt.cql",
			Golden:   "testdata/recreate/udt_golden.cql",
		},
		{
			Name:     "Aggregates",
			Keyspace: "gocqlx_aggregates",
			Input:    "testdata/recreate/aggregates.cql",
			Golden:   "testdata/recreate/aggregates_golden.cql",
		},
	}

	for i := range tcs {
		test := tcs[i]
		t.Run(test.Name, func(t *testing.T) {
			if test.Name == "UDT" && *flagDistribution == "scylla" && flagCassVersion.Major == 2024 && flagCassVersion.Minor == 1 {
				t.Skip("Doesn't work properly on Scylla 2024.1 due to https://github.com/scylladb/scylladb/issues/26761")
			}
			cleanup(t, session, test.Keyspace)

			in, err := os.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}

			queries := trimQueries(strings.Split(string(in), ";"))
			for _, q := range queries {
				qr := session.Query(q, nil)
				err = qr.Exec()
				if err != nil {
					break
				}
				qr.Release()
			}

			err = session.AwaitSchemaAgreement(context.Background())
			if err != nil {
				t.Fatal("failed to await for schema agreement", err)
			}
			err = session.metadataDescriber.refreshSchema(test.Keyspace)
			if err != nil {
				t.Fatal("failed to read schema for keyspace", err)
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

			km, err := session.KeyspaceMetadata(test.Keyspace)
			if err != nil {
				t.Fatal("dump schema", err)
			}
			dump, err := km.ToCQL()
			if err != nil {
				t.Fatal("recreate schema", err)
			}

			dump = trimSchema(dump)

			var golden []byte
			if getStmtFromCluster {
				golden, err = getCreateStatements(session, test.Keyspace)
				if err != nil {
					t.Fatal(err)
				}
				golden = []byte(trimSchema(string(golden)))
			} else {
				if *updateGolden {
					if err := os.WriteFile(test.Golden, []byte(dump), 0644); err != nil {
						t.Fatal(err)
					}
				}
				golden, err = os.ReadFile(test.Golden)
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

			// Exec dumped queries to check if they are CQL-correct
			cleanup(t, session, test.Keyspace)
			session.metadataDescriber.clearSchema(test.Keyspace)

			for _, q := range trimQueries(strings.Split(dump, ";")) {
				qr := session.Query(q, nil)
				if err := qr.Exec(); err != nil {
					t.Fatal("invalid dump query", q, err)
				}
				qr.Release()
			}

			// Check if new dump is the same as previous
			err = session.AwaitSchemaAgreement(context.Background())
			if err != nil {
				t.Fatal("failed to await for schema agreement", err)
			}
			err = session.metadataDescriber.refreshSchema(test.Keyspace)
			if err != nil {
				t.Fatal("failed to read schema for keyspace", err)
			}
			km, err = session.KeyspaceMetadata(test.Keyspace)
			if err != nil {
				t.Fatal("dump schema", err)
			}
			secondDump, err := km.ToCQL()
			if err != nil {
				t.Fatal("recreate schema", err)
			}

			secondDump = trimSchema(secondDump)

			secondDumpQueries := trimQueries(sortQueries(strings.Split(secondDump, ";")))

			if !cmp.Equal(secondDumpQueries, dumpQueries) {
				t.Errorf("first dump and second one differs: %s", cmp.Diff(secondDumpQueries, dumpQueries))
			}
		})
	}
}

func isDescribeKeyspaceSupported(t *testing.T, s *Session) bool {
	t.Helper()

	err := s.control.query(fmt.Sprintf(`DESCRIBE KEYSPACE system WITH INTERNALS`)).Close()
	if err != nil {
		if errFrame, ok := err.(frm.ErrorFrame); ok && errFrame.Code == ErrCodeSyntax {
			// DESCRIBE KEYSPACE is not supported on older versions of Cassandra and Scylla
			// For such case schema statement is going to be recreated on the client side
			return false
		}
		t.Fatalf("error querying keyspace schema: %v", err)
	}
	return true
}

func TestScyllaEncryptionOptionsUnmarshaller(t *testing.T) {
	const (
		input  = "testdata/recreate/scylla_encryption_options.bin"
		golden = "testdata/recreate/scylla_encryption_options_golden.json"
	)

	inputBuf, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}

	goldenBuf, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}

	goldenOpts := &scyllaEncryptionOptions{}
	if err := json.Unmarshal(goldenBuf, goldenOpts); err != nil {
		t.Fatal(err)
	}

	opts := &scyllaEncryptionOptions{}
	if err := opts.UnmarshalBinary(inputBuf); err != nil {
		t.Error(err)
	}

	if !cmp.Equal(goldenOpts, opts) {
		t.Error(cmp.Diff(goldenOpts, opts))
	}

}

func cleanup(t *testing.T, session *Session, keyspace string) {
	qr := session.Query(`DROP KEYSPACE IF EXISTS ` + keyspace)
	if err := qr.Exec(); err != nil {
		t.Fatalf("unable to drop keyspace: %v", err)
	}
	qr.Release()
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
