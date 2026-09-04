package replayer

import (
	"path/filepath"
	"testing"
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
// collision serves the wrong response, silently, and with `map` iteration deciding
// which one.
func TestCheckedInRecordingsStillLoad(t *testing.T) {
	for _, name := range fixtureRecordings {
		t.Run(name, func(t *testing.T) {
			base := filepath.Join("..", "..", "tests", "bench", name)

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
