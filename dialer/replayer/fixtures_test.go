package replayer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/gocql/gocql/dialer"
)

// fixtureRecordings are the recordings checked into tests/bench, which the replay
// benchmarks run against.
//
// They can only be regenerated with `go test -C tests/bench -update-golden` against a
// live node at the address the recorder hardcodes, so nothing in ordinary CI can
// rebuild them — which is exactly why the invariants they depend on are worth asserting
// from the always-run unit lane rather than only from the benchmark job, which is gated
// behind a label.
var fixtureRecordings = []string{
	"rec_insert/192.168.100.11:9042-0",
	"rec_select/192.168.100.11:9042-0",
}

// TestCheckedInRecordingsStillLoad pins the two properties the replay benchmarks
// depend on, against the actual bytes they replay.
//
// Neither is about a particular hash value. The hash function may change — a recording
// stores raw frames and is rehashed at load time with the same function applied to the
// live request, so both sides move together. What it may not do is stop telling the
// recording's own requests apart: the replayer scans for the first matching hash, so a
// collision serves the wrong response, silently. The scan runs over a slice sorted by
// stream id, so it is at least wrong the same way every run.
func TestCheckedInRecordingsStillLoad(t *testing.T) {
	for _, name := range fixtureRecordings {
		t.Run(name, func(t *testing.T) {
			base := benchRecordingBase(t, name)

			frames, proto, err := loadResponseFramesFromFiles(base+"Reads", base+"Writes")
			if err != nil {
				t.Fatalf("loading the recording: %v", err)
			}
			if len(frames) == 0 {
				t.Fatal("the recording paired no requests with responses")
			}
			if proto == 0 {
				t.Error("no protocol version was read from the recording")
			}

			seen := make(map[int64]int, len(frames))
			for i, frame := range frames {
				if first, dup := seen[frame.Hash]; dup {
					t.Errorf("frames %d and %d hash alike (%d), so the replayer would serve whichever it reached first",
						first, i, frame.Hash)
					continue
				}
				seen[frame.Hash] = i

				// materialise refuses any record that is not one whole frame, and a
				// short one cannot be refused later: the driver reads a frame with
				// io.ReadFull and ConnectionReplayer's SetReadDeadline is a no-op, so
				// it blocks mid-frame forever. An empty response is only the loudest
				// case of that.
				if !wholeFrame(frame.Response) {
					t.Errorf("frame %d holds %d bytes, which is not one whole CQL frame", i, len(frame.Response))
				}
			}
		})
	}
}

// benchRecordingBase returns the path prefix the two halves of a checked-in recording
// share, and skips when tests/bench is absent.
//
// tests/bench is a nested Go module, so the parent module's published zip omits the
// directory entirely and it is simply not there when these tests run from the module
// cache rather than a checkout. That is not a stale recording, so skip. A file missing
// from a directory that does exist is a real problem and still fails, below.
func benchRecordingBase(t *testing.T, name string) string {
	t.Helper()

	dir := filepath.Join("..", "..", "tests", "bench")
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s is a nested module and is not present in this tree", dir)
	}
	return filepath.Join(dir, name)
}

// readAllRecords returns every record in one half of a recording, in the order it was
// written. A record is one JSON object per line, the shape dialer.Record marshals.
//
// Deliberately not loadFramesFromFile, which the replay path uses: that keys by stream
// id, so a recording stacking more than one session keeps only the last record per id,
// and it skips a line that does not decode with a warning on stderr. Both are right for
// a replay and wrong for a guard -- a record that goes missing is exactly the one worth
// reporting. The bufio.Reader is copied from it for the reason its own comment gives: a
// frame is base64-inflated by 4/3 in JSON, so bufio.Scanner's default cap is reachable.
func readAllRecords(t *testing.T, path string) []dialer.Record {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening the recording: %v", err)
	}
	defer file.Close()

	var records []dialer.Record
	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			var record dialer.Record
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatalf("record %d in %s does not decode: %v", len(records), path, err)
			}
			records = append(records, record)
		}
		if readErr != nil {
			// A final record without its newline is still a record; io.EOF ends the
			// file either way.
			if errors.Is(readErr, io.EOF) {
				break
			}
			t.Fatalf("reading %s: %v", path, readErr)
		}
	}
	if len(records) == 0 {
		t.Fatalf("the recording at %s holds no records", path)
	}
	return records
}

// TestCheckedInRecordingsCarryTheirProtocolStamp pins that the recordings were produced
// by this tree's recorder, and that one directory is one connection at one version.
//
// FrameWriter stamps Record.Proto on every record it writes and has since the field
// existed, so an unstamped record was captured by an older build. Replay tolerates that
// -- loadResponseFramesFromFiles falls back to a response frame's own version byte --
// which is exactly why it needs saying out loud: without this, a regeneration run from a
// stale checkout silently produces recordings the checked-in recipe cannot reproduce,
// and Record.Proto goes back to being exercised by no fixture at all.
//
// The stamps are compared against each other, not against their own frames. FrameWriter
// derives the stamp by handing the frame to FrameProtoVersion, so re-reading byte 0 with
// that same function could only ever agree: it would assert nothing. What is not
// self-evident is that the records agree among themselves. The recorder appends to these
// files (see NewConnectionRecorder), so a directory can hold one session's Reads beside
// another's Writes, and that is the mixed state worth catching.
//
// loadResponseFramesFromFiles refuses such a disagreement too, but only among the Writes
// records -- it never reads a response record's stamp at all. So the Reads half is this
// test's own to cover, and comparing both halves in one pass is what makes them answer
// to each other.
func TestCheckedInRecordingsCarryTheirProtocolStamp(t *testing.T) {
	for _, name := range fixtureRecordings {
		t.Run(name, func(t *testing.T) {
			base := benchRecordingBase(t, name)

			// The first stamped record a directory yields, and where it came from, so
			// a disagreement can name both sides.
			var (
				proto byte
				from  string
			)
			for _, half := range []string{"Reads", "Writes"} {
				for i, record := range readAllRecords(t, base+half) {
					where := fmt.Sprintf("%s record %d", half, i)
					if record.Proto == 0 {
						t.Errorf("%s carries no proto stamp, so the recording predates this tree's recorder; regenerate it (see the recipe above TestMain in tests/bench/bench_single_conn_test.go)", where)
						continue
					}
					if proto == 0 {
						proto, from = record.Proto, where
						continue
					}
					if record.Proto != proto {
						t.Errorf("%s is stamped proto %d but %s is stamped proto %d; a recording is one connection at one version", where, record.Proto, from, proto)
					}
				}
			}
		})
	}
}
