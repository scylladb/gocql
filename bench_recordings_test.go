//go:build unit

package gocql

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// benchRecordings are the captured connections checked into tests/bench, which the
// replay benchmarks in that nested module run against.
var benchRecordings = []string{
	"rec_insert/192.168.100.11:9042-0Writes",
	"rec_select/192.168.100.11:9042-0Writes",
}

// usingTimeoutClause matches the clause Conn.systemRequestStatement appends to a
// system request. It is ScyllaDB-only and derived from a configured timeout rather than
// from a constant, so the recorded text carries whatever the recording node's
// connection had in force.
var usingTimeoutClause = regexp.MustCompile(`^ USING TIMEOUT [0-9]+ms$`)

// TestBenchRecordingsMatchTheControlQuery pins that the checked-in recordings still
// answer the query the control connection actually sends.
//
// A recording is only useful while the driver keeps asking what it asked when the
// bytes were captured, and nothing about editing conn.go says otherwise. This is not a
// hypothetical: the recordings were captured in October 2024 against a driver that sent
// `SELECT * FROM system.local WHERE key='local'`, and b6a9682 ("perf: use explicit
// column lists in system.local/peers queries") replaced that with qrySystemLocal in
// April 2026 without regenerating them. They went stale that day.
//
// Nothing caught it for four months. The replay benchmarks would have -- an unmatched
// request panics ConnectionReplayer.Write -- but the workflow that runs them is gated
// behind the run-benchmark-tests label, and dialer.GetFrameHash was hashing three
// constant zero bytes for every QUERY frame (scylladb/gocql#1000), so the stale request
// matched the fresh one anyway. Replay even stayed correct by accident, because the
// recorded `SELECT *` response is a superset and hostInfoFromMap reads columns by name.
//
// This test is the always-run guard for that, and it lives in this package because
// qrySystemLocal is unexported here.
func TestBenchRecordingsMatchTheControlQuery(t *testing.T) {
	for _, name := range benchRecordings {
		t.Run(name, func(t *testing.T) {
			queries := recordedQueryTexts(t, filepath.Join("tests", "bench", name))
			if len(queries) == 0 {
				t.Fatal("the recording holds no QUERY frame, so it cannot serve the control connection's system.local lookup")
			}

			for _, query := range queries {
				suffix, ok := strings.CutPrefix(query, qrySystemLocal)
				if !ok {
					t.Errorf("recorded query\n\t%q\nis not the control connection's system.local lookup\n\t%q\nRegenerate the recordings: see the recipe above TestMain in tests/bench/bench_single_conn_test.go.", query, qrySystemLocal)
					continue
				}
				if suffix != "" && !usingTimeoutClause.MatchString(suffix) {
					t.Errorf("recorded query carries an unexpected trailer %q; only Conn.systemRequestStatement's USING TIMEOUT clause belongs there", suffix)
				}
			}
		})
	}
}

// recordedQueryTexts returns the query text of every QUERY frame in a recording's
// requests file. A record is one JSON object per line holding one base64 frame.
func recordedQueryTexts(t *testing.T, path string) []string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the recording: %v", err)
	}

	var queries []string
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record struct {
			Data string `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("record %d does not decode: %v", i, err)
		}
		frame, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			t.Fatalf("record %d does not base64-decode: %v", i, err)
		}

		// A v3+ request header is version, flags, a 2-byte stream id, the opcode and
		// the 4-byte body length. A QUERY body opens with the query as a [long string].
		if len(frame) < 13 || frm.Op(frame[4]) != frm.OpQuery {
			continue
		}
		length := int(frame[9])<<24 | int(frame[10])<<16 | int(frame[11])<<8 | int(frame[12])
		if length < 0 || 13+length > len(frame) {
			t.Fatalf("record %d: a QUERY's query text runs past the frame", i)
		}
		queries = append(queries, string(frame[13:13+length]))
	}
	return queries
}
