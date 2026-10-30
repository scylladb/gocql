//go:build unit

package gocql

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/scylladb/gocql/v2/dialer"
	frm "github.com/scylladb/gocql/v2/internal/frame"

	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var benchRecordings = []string{
	"rec_insert/192.168.100.11:9042-0Writes",
	"rec_select/192.168.100.11:9042-0Writes",
}

// benchControlQuery is the statement the control connection sends on a recorded
// connection: qrySystemLocal with the ScyllaDB-only USING TIMEOUT clause appended. The
// bench module records with a bare gocql.newClusterSettings() against a ScyllaDB node, so the
// clause comes from newClusterSettings's default ConnectTimeout -- and it is rendered here by
// the production code rather than restated, so a change to that default, or to the
// rendering, fails this test instead of slipping past a pattern.
//
// The timeout sits inside the query text, and therefore inside the frame hash the
// replayer matches on. A guard accepting any number would let a changed default leave the
// recordings stale exactly the way b6a9682 did, and leave it to the label-gated replay
// lane to notice -- which is the gap this test exists to close.
//
// ConnectTimeout is the right source because of an ordering: controlConn.setupConn is
// what issues qrySystemLocal, and its callers reach finalizeConnection only afterwards --
// which republishes the clause from MetadataSchemaRequestTimeout (conn.go:347). Both
// default to 60s, so a recording cannot tell the two apart; if the control query ever
// moves after finalizeConnection, this guard keeps passing and the recordings go stale.
func benchControlQuery() string {
	// setSystemRequestTimeout only stores into an atomic and reads isScyllaConn, so
	// nothing else of a driverConn has to be real here.
	c := &driverConn{InternalScyllaSupported: ScyllaConnectionFeatures{
		ScyllaHostFeatures: *func() *ScyllaHostFeatures {
			opaqueStorageValue := new(ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalIsScylla = true
			return opaqueStorageValue
		}(),
	}}
	c.InternalSetSystemRequestTimeout(newClusterSettings().ConnectTimeout)
	stmt, _ := c.InternalSystemRequestStatement(qrySystemLocal)
	return stmt
}

// benchControlParams is the <query_parameters> block the control connection's QUERY
// carries, up to but not including the default timestamp.
//
// The statement is not the whole of what the recordings turn on. The hash the replayer
// matches a request by covers the parameters behind the text too -- the range stops only
// at the default timestamp, which is time.Now() at send -- so a changed page size or a
// default timestamp switched off re-stales the recordings exactly the way b6a9682 did,
// and comparing statements alone would not notice.
//
// Rendered from the driver's own values, for the same reason benchControlQuery is: a
// restated 0x24 would go on matching the recording after the flags it stands for stopped
// being sent, which is the failure this guard exists to make loud.
//
// querySystem sends Consistency(One) and sets disableSkipMetadata, and leaves the cluster
// defaults otherwise -- so the flags are the page size and the default timestamp, and the
// absent skip-metadata bit is itself part of what is pinned here.
func benchControlParams() []byte {
	cluster := newClusterSettings()

	var flags uint32
	if cluster.PageSize > 0 {
		flags |= frm.FlagPageSize
	}
	if cluster.DefaultTimestamp {
		flags |= frm.FlagDefaultTimestamp
	}

	params := []byte{byte(One >> 8), byte(One), byte(flags)}
	if cluster.PageSize > 0 {
		params = append(params, byte(cluster.PageSize>>24), byte(cluster.PageSize>>16),
			byte(cluster.PageSize>>8), byte(cluster.PageSize))
	}
	return params
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
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s is a nested module and is not present in this tree", dir)
	}

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

// Historical capture integrity only: these captures require regeneration for
// typed control traffic in step13. Canonical runtime coverage uses live peers.
func TestBenchHistoricalControlCapture(t *testing.T) {
	const regenerate = "\nRegenerate the recordings: see the recipe above TestMain in tests/bench/bench_single_conn_test.go."

	want := benchControlQuery()
	wantParams := benchControlParams()

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
			const timestampLen = 8
			if len(params) != len(wantParams)+timestampLen {
				t.Fatalf("the recorded control query carries %d parameter bytes, want %d: %d up to the default timestamp, then the timestamp's %d"+regenerate,
					len(params), len(wantParams)+timestampLen, len(wantParams), timestampLen)
			}
			if got := params[:len(wantParams)]; !bytes.Equal(got, wantParams) {
				t.Errorf("the recorded control query's parameters are not the ones the driver now sends\n\twant % x\n\tgot  % x\n(the consistency, the query-flags byte, then the page size)"+regenerate, wantParams, got)
			}
		})
	}
}

// readRecording returns every record in a checked-in recording. A record is one JSON
// object per line, the shape dialer.Record marshals.
//
// tests/bench is a nested Go module, so the parent module's published zip omits the
// directory entirely and it is simply absent when these tests run from the module cache
// rather than a checkout. That is not a stale recording, so skip. A file missing from a
// directory that does exist is a real problem and still fails.
