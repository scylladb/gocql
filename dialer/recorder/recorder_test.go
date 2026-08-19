package recorder

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gocql/gocql/dialer"
)

// stubConn is a net.Conn whose Read serves a fixed byte stream and whose Write
// accepts everything, so the recorder's own behaviour is all a test observes.
type stubConn struct {
	readData []byte
}

func (c *stubConn) Read(b []byte) (int, error) {
	if len(c.readData) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.readData)
	c.readData = c.readData[n:]
	return n, nil
}

func (c *stubConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *stubConn) Close() error                     { return nil }
func (c *stubConn) LocalAddr() net.Addr              { return nil }
func (c *stubConn) RemoteAddr() net.Addr             { return nil }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

// optionsFrame builds a body-less OPTIONS frame with the given version byte.
func optionsFrame(version byte) []byte {
	return []byte{version, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}
}

// startupFrame builds a protocol v4 STARTUP request whose [string map] carries the
// SCYLLA_USE_METADATA_ID opt-in — the frame the recorder has to see whole for the
// latch to fire. Its body is long enough to split a Write anywhere in it.
func startupFrame() []byte {
	opts := [][2]string{
		{"CQL_VERSION", "3.0.0"},
		{"SCYLLA_USE_METADATA_ID", ""},
	}
	body := []byte{0x00, byte(len(opts))}
	for _, kv := range opts {
		for _, s := range kv {
			body = append(body, byte(len(s)>>8), byte(len(s)))
			body = append(body, s...)
		}
	}
	header := []byte{
		0x04,       // version v4 (request)
		0x00,       // header flags
		0x00, 0x2A, // stream id
		0x01,                              // opStartup
		0x00, 0x00, 0x00, byte(len(body)), // body length
	}
	return append(header, body...)
}

// recordedFrames decodes the newline-delimited records the recorder wrote.
func recordedFrames(t *testing.T, fname string) []dialer.Record {
	t.Helper()

	data, err := os.ReadFile(fname)
	if err != nil {
		t.Fatalf("reading %s: %v", fname, err)
	}

	var records []dialer.Record
	for _, line := range bytes.Split(bytes.TrimRight(data, "\n"), []byte{'\n'}) {
		var record dialer.Record
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decoding %s: %v", fname, err)
		}
		records = append(records, record)
	}
	return records
}

// TestConnectionRecorderReassemblesSplitFrames pins that a frame delivered over
// several calls is recorded once and whole.
//
// The metadata-ID latch is what makes this visible: it looks for the opt-in key in
// a completed STARTUP, so a record cut short at an arbitrary byte misses it, every
// later EXECUTE is stamped UseMetadataID=false, and replay fails as silent hash
// mismatches rather than as an error. The recorder used to restart its record on
// every call, so any frame that did not arrive in one piece was truncated.
func TestConnectionRecorderReassemblesSplitFrames(t *testing.T) {
	startup := startupFrame()

	for _, tc := range []struct {
		name  string
		split int
	}{
		// Before the 9-byte header is complete the declared length is unknown, so
		// this exercises the path that returns without deciding anything.
		{"split inside the header", 5},
		{"split inside the body", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fname := filepath.Join(t.TempDir(), "conn")
			rec, err := NewConnectionRecorder(fname, &stubConn{}, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			for _, chunk := range [][]byte{startup[:tc.split], startup[tc.split:], optionsFrame(0x04)} {
				if _, err := rec.Write(chunk); err != nil {
					t.Fatalf("Write: %v", err)
				}
			}

			records := recordedFrames(t, fname+"Writes")
			if len(records) != 2 {
				t.Fatalf("recorded %d frames, want 2 (the split STARTUP and the OPTIONS)", len(records))
			}

			if !bytes.Equal(records[0].Data, startup) {
				t.Errorf("STARTUP recorded as % X, want % X", records[0].Data, startup)
			}
			if want := int(startup[2])<<8 | int(startup[3]); records[0].StreamID != want {
				t.Errorf("STARTUP stream id = %d, want %d", records[0].StreamID, want)
			}
			if !bytes.Equal(records[1].Data, optionsFrame(0x04)) {
				t.Errorf("OPTIONS recorded as % X, want % X", records[1].Data, optionsFrame(0x04))
			}

			// The point of recording the frame whole: both the STARTUP itself and
			// every frame after it carry the negotiated state.
			for i, record := range records {
				if !record.UseMetadataID {
					t.Errorf("record %d: UseMetadataID = false, want true (the opt-in was in the STARTUP)", i)
				}
			}
		})
	}
}

// TestConnectionRecorderSplitsCoalescedFrames pins the other half of the framing:
// a call carrying more than one frame is recorded as the frames it holds, not as
// one blob leaving its successor to start mid-frame.
//
// The read side is where this happens for real. Conn wraps the dialed connection in
// a bufio.Reader (conn.go), which fills with reads of up to its buffer size, so two
// responses that arrive together reach the recorder in a single call.
func TestConnectionRecorderSplitsCoalescedFrames(t *testing.T) {
	t.Run("two whole frames in one read", func(t *testing.T) {
		first, second := optionsFrame(0x84), optionsFrame(0x84)
		second[3] = 0x02 // a different stream id, so the two records are telling apart

		fname := filepath.Join(t.TempDir(), "conn")
		rec, err := NewConnectionRecorder(fname, &stubConn{readData: append(append([]byte{}, first...), second...)}, nil)
		if err != nil {
			t.Fatalf("NewConnectionRecorder: %v", err)
		}
		defer rec.Close()

		if _, err := rec.Read(make([]byte, 4096)); err != nil {
			t.Fatalf("Read: %v", err)
		}

		records := recordedFrames(t, fname+"Reads")
		if len(records) != 2 {
			t.Fatalf("recorded %d frames, want 2", len(records))
		}
		for i, want := range [][]byte{first, second} {
			if !bytes.Equal(records[i].Data, want) {
				t.Errorf("record %d = % X, want % X", i, records[i].Data, want)
			}
			if wantID := int(want[2])<<8 | int(want[3]); records[i].StreamID != wantID {
				t.Errorf("record %d: stream id = %d, want %d", i, records[i].StreamID, wantID)
			}
		}
	})

	t.Run("a frame and part of the next", func(t *testing.T) {
		startup, options := startupFrame(), optionsFrame(0x04)

		fname := filepath.Join(t.TempDir(), "conn")
		rec, err := NewConnectionRecorder(fname, &stubConn{}, nil)
		if err != nil {
			t.Fatalf("NewConnectionRecorder: %v", err)
		}
		defer rec.Close()

		// The boundary falls inside the OPTIONS header, so the second frame is picked
		// up mid-header rather than at a clean start.
		for _, chunk := range [][]byte{append(append([]byte{}, startup...), options[:4]...), options[4:]} {
			if _, err := rec.Write(chunk); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}

		records := recordedFrames(t, fname+"Writes")
		if len(records) != 2 {
			t.Fatalf("recorded %d frames, want 2", len(records))
		}
		if !bytes.Equal(records[0].Data, startup) {
			t.Errorf("STARTUP recorded as % X, want % X", records[0].Data, startup)
		}
		if !bytes.Equal(records[1].Data, options) {
			t.Errorf("OPTIONS recorded as % X, want % X", records[1].Data, options)
		}
		// The latch fired on the STARTUP even though the call did not end with it.
		for i, record := range records {
			if !record.UseMetadataID {
				t.Errorf("record %d: UseMetadataID = false, want true", i)
			}
		}
	})
}

// TestConnectionRecorderRecordsUnsegmentedProtoV5 pins that a v5 connection is
// recorded rather than refused.
//
// It used to be refused outright, because the recorder sliced the byte stream on fixed
// CQL header offsets and a v5 transport segment has none of them. The handshake is
// still unsegmented on v5, so these frames go through the plain path; what changed is
// that reaching them is no longer an error.
func TestConnectionRecorderRecordsUnsegmentedProtoV5(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "conn")
	rec, err := NewConnectionRecorder(fname, &stubConn{readData: optionsFrame(0x85)}, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	if _, err := rec.Write(optionsFrame(0x05)); err != nil {
		t.Fatalf("Write(v5 frame) error = %v, want nil", err)
	}
	buf := make([]byte, 64)
	if _, err := rec.Read(buf); err != nil {
		t.Fatalf("Read(v5 response) error = %v, want nil", err)
	}

	for _, suffix := range []string{"Writes", "Reads"} {
		got := recordedFrames(t, fname+suffix)
		if len(got) != 1 {
			t.Fatalf("%s: recorded %d frames, want 1", suffix, len(got))
		}
	}
}

// TestConnectionRecorderAcceptsProtoV4 pins that the rejection is scoped to
// v5+: a v4 frame (with and without the direction bit) is recorded normally.
func TestConnectionRecorderAcceptsProtoV4(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "conn")
	rec, err := NewConnectionRecorder(fname, &stubConn{readData: optionsFrame(0x84)}, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	if _, err := rec.Write(optionsFrame(0x04)); err != nil {
		t.Fatalf("Write(v4 frame) error = %v, want nil", err)
	}
	buf := make([]byte, 64)
	if _, err := rec.Read(buf); err != nil {
		t.Fatalf("Read(v4 response) error = %v, want nil", err)
	}

	for _, suffix := range []string{"Writes", "Reads"} {
		data, err := os.ReadFile(fname + suffix)
		if err != nil {
			t.Fatalf("reading the %s record file: %v", suffix, err)
		}
		if len(data) == 0 {
			t.Errorf("the v4 frame was not recorded to the %s file", suffix)
		}
	}
}
