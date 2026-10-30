//go:build unit

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	metadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	"testing"

	"github.com/scylladb/gocql/v2/dialer"
	frm "github.com/scylladb/gocql/v2/internal/frame"

	"os"
	"path/filepath"
	"strings"
)

var benchRecordings = []string{
	"rec_insert/connection-0Writes",
	"rec_select/connection-0Writes",
}

// benchControlQuery and benchControlParams pin typed control startup exactly.
// The original pre-migration captures and their failed operational replay remain
// source-bound historical evidence in step10; these fixtures run current code.
// The capture's negotiated Scylla metadata timeout is 30s. Unlike legacy
// querySystem it is independent of connect and client metadata timeout budgets.
// The always-run operational replay additionally guards the complete startup.
func benchControlQuery() string { return qrySystemLocal + " USING TIMEOUT 30000ms" }

func benchControlParams(t *testing.T) []byte {
	t.Helper()
	sentinel := errors.New("capture encoded control request")
	var request *model.AttemptRequest
	_, err := metadata.ReadRows(context.Background(), 4, DefaultConfig().Metadata.RequestTimeout,
		func(_ context.Context, r *model.AttemptRequest) (*model.ResponseData, error) {
			request = r
			return nil, sentinel
		},
		metadata.Query[Unit, Unit]{CQL: benchControlQuery(), Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
	if !errors.Is(err, sentinel) || request == nil {
		t.Fatalf("capture canonical metadata request: %v", err)
	}
	frame, err := (protocol.Settings{Protocol: 4, MaxFrameBytes: DefaultConfig().Limits.MaxFrameBytes, MaxDecompressedBytes: DefaultConfig().Limits.MaxDecompressedBytes}).BuildTypedRequest(request, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, frame.Len())
	if _, err := frame.CopyTo(raw); err != nil {
		t.Fatal(err)
	}
	_, tail, ok := recordedQueryText(t, 0, raw)
	if !ok {
		t.Fatal("canonical metadata request is not QUERY")
	}
	return tail
}

const opcodeOffset = 4

// recordedIsQuery reports whether a frame announces itself a QUERY, whether or not
// recordedQueryText can go on to read its text.
func recordedIsQuery(frame []byte) bool {
	return len(frame) > opcodeOffset && frm.Op(frame[opcodeOffset]) == frm.OpQuery
}

// recordedQueryText returns a QUERY frame's query text, the parameter bytes behind it,
// and whether the frame is one this guard can read.
//
// The offsets are dialer's own: a v3+ request header is version, flags, a 2-byte stream
// id, the opcode and the 4-byte body length, and a QUERY body opens with the query as a
// [long string].
//
// A QUERY carrying a custom payload is reported as unreadable rather than as not a
// QUERY -- recordedIsQuery still recognises it, so a caller can tell the two apart. The
// payload is a [bytes map] sitting between the header and the query text, so the text
// has no fixed offset in such a frame: GetFrameHash walks the map to find it, and that
// walk is unexported here. The control connection never sets one, so the frame this
// guard looks for never carries a payload; reading it at the fixed offset would take the
// map's entry count for the text length and fail on a perfectly healthy recording.
//
// A frame too short to carry an opcode is not a QUERY, because nothing in it says it is.
// One that announces itself a QUERY and then stops early is a damaged record, and says
// so rather than being skipped.
func recordedQueryText(t *testing.T, i int, frame []byte) (string, []byte, bool) {
	t.Helper()

	const longStringLen = 4

	textStart := dialer.FrameHeaderLen + longStringLen
	if !recordedIsQuery(frame) {
		return "", nil, false
	}
	if frame[1]&frm.FlagCustomPayload == frm.FlagCustomPayload {
		return "", nil, false
	}
	if len(frame) < textStart {
		t.Fatalf("record %d: a QUERY stops inside the header its query text follows", i)
	}

	// Read the length signed, as dialer.addLongString does and for its reason: a
	// negative value is a damaged field rather than a 4 GB one. int is 64 bits here,
	// so reading it unsigned would put 0xFFFFFFFF at 4294967295 and report a plain
	// overrun instead of naming the malformed length.
	size := frame[dialer.FrameHeaderLen:textStart]
	length := int(int32(uint32(size[0])<<24 | uint32(size[1])<<16 | uint32(size[2])<<8 | uint32(size[3])))
	if length < 0 {
		t.Fatalf("record %d: a QUERY's query text announces a negative length (%d)", i, length)
	}
	// Compared against the space left rather than added to textStart, for the reason
	// dialer.fits documents: textStart+length overflows a 32-bit int at the largest
	// length a [long string] can encode, and the negative sum passes every bound.
	if length > len(frame)-textStart {
		t.Fatalf("record %d: a QUERY's query text runs past the frame", i)
	}
	return string(frame[textStart : textStart+length]), frame[textStart+length:], true
}

func readRecording(t *testing.T, name string) []dialer.Record {
	t.Helper()

	dir := filepath.Join("tests", "bench")

	path := filepath.Join(dir, name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the recording: %v", err)
	}

	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		t.Fatalf("the recording at %s holds no records", path)
	}

	var records []dialer.Record
	for i, line := range strings.Split(trimmed, "\n") {
		var record dialer.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("record %d does not decode: %v", i, err)
		}
		records = append(records, record)
	}
	return records
}

// Current typed-control captures also run through TestSingleConnectionReplay.
func TestBenchCurrentControlCapture(t *testing.T) {
	const regenerate = "\nRegenerate the recordings: see the recipe above TestMain in tests/bench/bench_single_conn_test.go."

	want := benchControlQuery()
	wantParams := benchControlParams(t)

	for _, name := range benchRecordings {
		t.Run(name, func(t *testing.T) {
			var (
				texts   []string
				params  []byte
				found   bool
				skipped int
			)
			for i, record := range readRecording(t, name) {
				text, tail, ok := recordedQueryText(t, i, record.Data)
				if !ok {
					if recordedIsQuery(record.Data) {
						skipped++
					}
					continue
				}
				texts = append(texts, text)
				if text == want && !found {
					params, found = tail, true
				}
			}

			if !found {
				switch {
				case skipped > 0:
					// Not a stale recording: the frames are there and this parser
					// cannot read them. Reporting "no QUERY frame" here would send
					// the reader off to regenerate a recording that is fine.
					t.Fatalf("all %d of the recording's QUERY frames carry a custom payload, which this guard does not read past", skipped)
				case len(texts) == 0:
					t.Fatal("the recording holds no QUERY frame, so it cannot serve the control connection's system.local lookup")
				default:
					t.Fatalf("no recorded query is the one the control connection sends\n\twant %q\n\tgot  %q"+regenerate, want, texts)
				}
			}

			// The statement matches. The parameters behind it are inside the hash as
			// well, so the recording is only current if they match too.
			if !bytes.Equal(params, wantParams) {
				t.Fatalf("recorded control query parameters differ from current typed metadata execution\nwant % x\ngot  % x%s", wantParams, params, regenerate)
			}
		})
	}
}
