package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/dialer"
)

// TestConnectionRecorderFailKeepsOneCause pins that fail reports the latched failure
// rather than its argument. Read and Write hand what fail returns straight back to the
// driver while Close ranks the latched value first, so returning the argument would have
// the driver's error handler and the Close caller naming different causes for the same
// recording -- and the later one is always the symptom, never the cause.
func TestConnectionRecorderFailKeepsOneCause(t *testing.T) {
	var c ConnectionRecorder

	first := errors.New("the segment payload failed its CRC32")
	later := errors.New("write tcp: broken pipe")

	if got := c.fail(first); !errors.Is(got, first) {
		t.Fatalf("fail(first) = %v, want the first failure back", got)
	}
	if got := c.fail(later); !errors.Is(got, first) {
		t.Errorf("fail(later) = %v, want the latched %v", got, first)
	}
	if got := c.failed(); !errors.Is(got, first) {
		t.Errorf("failed() = %v, want the latched %v", got, first)
	}
}

// stubConn is a net.Conn whose Read serves a fixed byte stream and whose Write
// accepts everything, so the recorder's own behaviour is all a test observes.
type stubConn struct {
	readData []byte
	written  []byte
	writeErr error
	closeErr error
	closed   bool
	// closes counts every Close, so a test can tell one teardown from two.
	closes int
}

func (c *stubConn) Read(b []byte) (int, error) {
	if len(c.readData) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.readData)
	c.readData = c.readData[n:]
	return n, nil
}

func (c *stubConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.written = append(c.written, b...)
	return len(b), nil
}
func (c *stubConn) Close() error                     { c.closes++; c.closed = true; return c.closeErr }
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

	// An empty file is a recording of nothing, which is a frame count the caller can
	// assert on. Splitting it yields one zero-length line that json.Unmarshal refuses,
	// so this used to fail here as "unexpected end of JSON input" instead.
	if len(data) == 0 {
		return nil
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

// TestConnectionRecorderPropagatesEOF pins that a server-closed connection reports
// io.EOF. The recorder must record on EOF too -- a final read can carry data -- but
// it used to return the recording step's error in its place, which is nil, so a dead
// connection read as (0, nil) forever: the driver reads through io.ReadFull, which
// loops while err is nil, and spun at full speed instead of tearing the
// connection down.
func TestConnectionRecorderPropagatesEOF(t *testing.T) {
	response := optionsFrame(0x84)
	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{readData: response}, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	buf := make([]byte, len(response))
	if n, err := rec.Read(buf); err != nil || n != len(response) {
		t.Fatalf("Read = (%d, %v), want (%d, nil)", n, err, len(response))
	}
	if _, err := rec.Read(buf); err != io.EOF {
		t.Fatalf("Read at end of stream = %v, want io.EOF", err)
	}
}

// timeoutErr is a net.Error reporting a timeout -- the shape connReader.Read treats as
// resumable when the read that hit it still delivered bytes.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// scriptedRead is one result a scriptedConn hands back: some bytes, and the error
// returned with them.
type scriptedRead struct {
	data []byte
	err  error
}

// scriptedConn serves a fixed sequence of Read results, so a test can hand the recorder
// the (n > 0, err) pair a real socket is allowed to return.
type scriptedConn struct {
	stubConn
	reads []scriptedRead
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	next := c.reads[0]
	c.reads = c.reads[1:]
	return copy(b, next.data), next.err
}

// TestConnectionRecorderRecordsAPartialReadThatErrored pins that bytes delivered
// alongside a non-EOF error still reach the recording.
//
// A net.Conn may return n > 0 together with an error, and in this driver that is
// routine rather than terminal: connReader.Read arms a deadline per attempt and resumes
// a read that timed out while still making progress, up to maxReadAttempts times, so a
// large frame body over a slow link arrives in pieces on a connection that carries
// straight on. Returning before recording those bytes dropped them from the recording
// and left the read decoder mid-frame at a stale offset, so every frame after them was
// assembled from the wrong bytes -- and nothing was latched, so neither the driver nor
// Close ever said so.
func TestConnectionRecorderRecordsAPartialReadThatErrored(t *testing.T) {
	response := optionsFrame(0x84)
	const split = 4 // mid-header, so a dropped prefix mis-frames everything after it

	conn := &scriptedConn{reads: []scriptedRead{
		{data: response[:split], err: timeoutErr{}},
		{data: response[split:]},
	}}

	dir := t.TempDir()
	rec, err := NewConnectionRecorder(filepath.Join(dir, "conn"), conn, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}

	buf := make([]byte, len(response))
	n, err := rec.Read(buf)
	if n != split {
		t.Fatalf("first Read = %d bytes, want %d", n, split)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("first Read error = %v, want a timeout the driver would resume", err)
	}

	if n, err := rec.Read(buf); err != nil || n != len(response)-split {
		t.Fatalf("second Read = (%d, %v), want (%d, nil)", n, err, len(response)-split)
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	records := recordedFrames(t, filepath.Join(dir, "connReads"))
	if len(records) != 1 {
		t.Fatalf("recorded %d frames, want 1", len(records))
	}
	if !bytes.Equal(records[0].Data, response) {
		t.Errorf("recorded frame = %x, want %x", records[0].Data, response)
	}
}

// endlessConn serves one whole frame per Read until it is closed.
//
// It records being closed in the embedded stubConn's own field rather than adding a
// second one that shadows it: a test reaching for stubConn.closed -- which is what
// TestConnectionRecorderCloseClosesEverything asserts on -- would otherwise read a
// field this type never sets and pass for the wrong reason. Both accesses are under
// mu because Close here races the read goroutine, which the unsynchronised
// stubConn.Close this one overrides does not have to consider.
type endlessConn struct {
	stubConn
	frame []byte
	mu    sync.Mutex
}

func (c *endlessConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stubConn.closed {
		return 0, net.ErrClosed
	}
	return copy(b, c.frame), nil
}

func (c *endlessConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stubConn.closed = true
	return nil
}

// TestConnectionRecorderCloseIsSafeWhileReading pins that Close may run while the read
// goroutine is still inside Read. Worth running under -race, which is what catches it.
//
// gocql closes a connection from whichever goroutine noticed it was finished while
// serve() can still be in Read, so a net.Conn has to tolerate the overlap. Close used to
// shut the recording files before the socket and then read decoder state that Feed was
// concurrently appending to: the in-flight frames were lost to an "file already closed"
// that named nothing, and the truncation verdict came out differently depending on which
// goroutine got there first.
func TestConnectionRecorderCloseIsSafeWhileReading(t *testing.T) {
	response := optionsFrame(0x84)
	conn := &endlessConn{frame: response}

	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, len(response))
		for {
			if _, err := rec.Read(buf); err != nil {
				return
			}
		}
	}()

	// Long enough for the reader to be somewhere inside Read.
	time.Sleep(time.Millisecond)

	if err := rec.Close(); err != nil {
		t.Errorf("Close while reading: %v", err)
	}
	<-done

	// Spelled through the embedded struct on purpose: that is the field a helper
	// written against stubConn reads, and endlessConn used to declare a second one
	// that shadowed it, so its Close left this one false with nothing to notice.
	if !conn.stubConn.closed {
		t.Error("Close left the wrapped connection open")
	}
}

// TestConnectionRecorderCloseReportsATruncatedStream pins that a stream ending in
// the middle of a frame is reported at Close. On disk that recording looks complete,
// and at load time the loss surfaces only as an unpaired stream or an unmatched
// hash, with nothing pointing at the session that was cut short.
func TestConnectionRecorderCloseReportsATruncatedStream(t *testing.T) {
	startup := startupFrame()

	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{}, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}

	if _, err := rec.Write(startup[:len(startup)-3]); err != nil {
		t.Fatalf("Write: %v", err)
	}

	err = rec.Close()
	if err == nil {
		t.Fatal("closing a connection mid-frame reported nothing")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("error %q does not say the recording is truncated", err)
	}
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

// startupFrameV2 builds a protocol v2 STARTUP: the handshake the driver opens with, in
// the layout that predates the 2-byte stream id, so the opcode sits at index 3 and the
// body at index 8 rather than 4 and 9.
func startupFrameV2() []byte {
	body := []byte{
		0x00, 0x01, // one option
		0x00, 0x0B, 'C', 'Q', 'L', '_', 'V', 'E', 'R', 'S', 'I', 'O', 'N',
		0x00, 0x05, '3', '.', '0', '.', '0',
	}
	header := []byte{
		0x02,                              // version v2 (request)
		0x00,                              // header flags
		0x00,                              // stream id (1 byte)
		0x01,                              // opStartup
		0x00, 0x00, 0x00, byte(len(body)), // body length
	}
	return append(header, body...)
}

// optionsFrameV2 builds a body-less protocol v2 OPTIONS: eight bytes that are nothing
// but a v1/v2 header, and therefore a complete frame one byte short of the v3+ header
// the splitter slices on.
//
// This is the shape that used to slip the floor entirely, because the check ran only
// once nine bytes had arrived. It is also the shape the driver sends most often --
// controlConn.heartBeat sends an OPTIONS every 30 seconds, and OPTIONS carries no body.
func optionsFrameV2() []byte {
	return []byte{
		0x02,                   // version v2 (request)
		0x00,                   // header flags
		0x00,                   // stream id (1 byte)
		0x05,                   // opOptions
		0x00, 0x00, 0x00, 0x00, // body length
	}
}

// TestConnectionRecorderRefusesPreV3 is the other side of AcceptsProtoV4: a connection
// a caller pinned to protocol v1 or v2 is refused by name, and nothing reaches the
// recording.
//
// What used to happen was worse than an error. Every offset here reads the v3+ header,
// so a v2 stream was sliced a byte late, and the write succeeded -- the file filled
// with records whose frames were not frames, and whose stream ids came from the middle
// of a body. Nothing said so until a replay of them failed to match anything.
//
// Both a bodyful frame and a bodyless one, because Write gates the socket on the
// recording: a frame the splitter fails to refuse is forwarded to the server and
// recorded nowhere. The bodyless case is the one that got that far.
func TestConnectionRecorderRefusesPreV3(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{name: "a bodyful STARTUP", frame: startupFrameV2()},
		{name: "a bodyless OPTIONS", frame: optionsFrameV2()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fname := filepath.Join(t.TempDir(), "conn")
			conn := &stubConn{}
			rec, err := NewConnectionRecorder(fname, conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			defer rec.Close()

			n, err := rec.Write(tc.frame)
			if err == nil {
				t.Fatal("a protocol v2 connection was recorded")
			}
			if !strings.Contains(err.Error(), "protocol v2") {
				t.Errorf("error does not name the version: %v", err)
			}
			if n != 0 {
				t.Errorf("Write reported %d bytes written, want 0", n)
			}
			if len(conn.written) != 0 {
				t.Errorf("%d bytes reached the server from a refused connection", len(conn.written))
			}
			if got := recordedFrames(t, fname+"Writes"); len(got) != 0 {
				t.Errorf("recorded %d frames from a refused connection, want 0", len(got))
			}
		})
	}
}

// TestConnectionRecorderCloseClosesEverything pins that one failing close does not
// strand the rest. Close used to return at the first error, leaving the other
// recording file and the socket itself open -- under the driver's redial loop, one
// descriptor and one connection per attempt, which is the leak NewConnectionRecorder
// was fixed for on its setup path.
func TestConnectionRecorderCloseClosesEverything(t *testing.T) {
	conn := &stubConn{}
	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	recorder := rec.(*ConnectionRecorder)

	// Closing the Writes file underneath is what a full disk or an I/O error looks
	// like from Close: the first close it makes reports one.
	if err := recorder.fd_writes.Close(); err != nil {
		t.Fatalf("closing the Writes file: %v", err)
	}

	if err := rec.Close(); err == nil {
		t.Error("Close reported success although the Writes file was already closed")
	}
	if !conn.closed {
		t.Error("Close left the wrapped connection open")
	}
	if err := recorder.fd_reads.Close(); err == nil {
		t.Error("Close left the Reads file open")
	}
}

// TestConnectionRecorderCloseReportsEveryFailure pins that the close that failed first
// does not silence the ones after it. Close kept only the first file error and then
// dropped the socket's entirely -- and the socket's is the one closeWithError hands to
// errorHandler.HandleError, so a Writes file that would not close hid the connection
// failure the driver was about to be told about.
func TestConnectionRecorderCloseReportsEveryFailure(t *testing.T) {
	sockErr := errors.New("closing the socket")
	conn := &stubConn{closeErr: sockErr}
	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	recorder := rec.(*ConnectionRecorder)

	if err := recorder.fd_writes.Close(); err != nil {
		t.Fatalf("closing the Writes file: %v", err)
	}

	closeErr := rec.Close()
	if closeErr == nil {
		t.Fatal("Close reported success although the Writes file was already closed")
	}
	if !strings.Contains(closeErr.Error(), "Writes file") {
		t.Errorf("Close reported %v, which does not say which file it was", closeErr)
	}
	if !errors.Is(closeErr, sockErr) {
		t.Errorf("Close reported %v, which drops the socket's own failure", closeErr)
	}
}

// TestConnectionRecorderCloseIsIdempotent pins that a second Close runs no second
// teardown and reports what the first one did. Closing an already-closed everything
// turned a healthy recording's nil into os.ErrClosed for each file joined with the
// socket's net.ErrClosed -- and on the driver route Conn.closeWithError hands that to
// errorHandler.HandleError, so a complete recording was reported as the reason the
// connection ended. An explicit Close alongside a deferred one is the ordinary shape.
func TestConnectionRecorderCloseIsIdempotent(t *testing.T) {
	startup := startupFrame()

	for _, tc := range []struct {
		name  string
		write []byte
		want  string
	}{
		{name: "a complete recording", write: startup},
		{name: "a truncated recording", write: startup[:len(startup)-3], want: "truncated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &stubConn{}
			rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
			if err != nil {
				t.Fatalf("NewConnectionRecorder: %v", err)
			}
			if _, err := rec.Write(tc.write); err != nil {
				t.Fatalf("Write: %v", err)
			}

			first := rec.Close()
			if tc.want == "" && first != nil {
				t.Fatalf("Close reported %v for a complete recording", first)
			}
			if tc.want != "" && (first == nil || !strings.Contains(first.Error(), tc.want)) {
				t.Fatalf("Close reported %v, want one mentioning %q", first, tc.want)
			}

			if second := rec.Close(); second != first {
				t.Errorf("the second Close reported %v, want the first one's %v", second, first)
			}
			if conn.closes != 1 {
				t.Errorf("the socket was closed %d times, want once", conn.closes)
			}
		})
	}
}

// TestRecordDialerClosesTheSocketWhenRecordingCannotStart pins that a dial whose
// recording cannot be opened does not strand the connection it has already made.
//
// NewConnectionRecorder returns (nil, err) for anything os.OpenFile refuses -- a
// missing recording directory, a read-only volume, a full disk, EMFILE -- and hands
// the caller no net.Conn to close. The driver answers a failed dial by redialing, so
// a socket left behind here is one stranded pair per attempt.
func TestRecordDialerClosesTheSocketWhenRecordingCannotStart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	d := NewRecordDialer(filepath.Join(t.TempDir(), "no-such-directory"))
	if conn, err := d.DialContext(context.Background(), "tcp", ln.Addr().String()); err == nil {
		conn.Close()
		t.Fatal("DialContext succeeded although the recording directory does not exist")
	}

	server := <-accepted
	if server == nil {
		t.Fatal("the dial never reached the listener")
	}
	defer server.Close()

	// The far end reading EOF is the only thing that can be observed about a socket
	// the dialer no longer hands out. Deadlined so a leak fails this test rather than
	// hanging it until the package timeout.
	if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("server-side read = %v, want io.EOF; the dialed socket was left open", err)
	}
}

// TestRecorderLatchesASendFailure pins that a request recorded but not sent ends the
// recording rather than being left behind in it.
//
// Recording happens before the send, so that record is already on disk and cannot be
// recalled. The loader keys records by stream id and keeps the last one, and the
// driver reuses stream ids, so left to run the connection would hand the loader an
// unsent request paired with an earlier exchange's response -- served on replay as a
// plausible answer to a question nobody asked.
func TestRecorderLatchesASendFailure(t *testing.T) {
	sendErr := errors.New("connection reset by peer")
	conn := &stubConn{readData: optionsFrame(0x84), writeErr: sendErr}
	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	if _, err := rec.Write(startupFrame()); !errors.Is(err, sendErr) {
		t.Fatalf("Write error = %v, want %v", err, sendErr)
	}

	// The connection healing afterwards is what the latch is for: recording resumes
	// only if nothing remembers that a request went unsent.
	conn.writeErr = nil
	if _, err := rec.Write(optionsFrame(0x04)); !errors.Is(err, sendErr) {
		t.Errorf("Write after a failed send = %v, want the latched %v", err, sendErr)
	}
	if _, err := rec.Read(make([]byte, 64)); !errors.Is(err, sendErr) {
		t.Errorf("Read after a failed send = %v, want the latched %v", err, sendErr)
	}
}

// TestRecorderHandsOnNothingItCouldNotRecord pins the read path's half of the
// contract TestRecorderSendsNothingItCouldNotRecord pins for writes: a frame that
// could not be recorded does not reach the driver either. It used to return the bytes
// alongside the error, and a bufio.Reader hands those on before it reports the error
// -- so the driver parsed and acted on frames the recording does not hold, and the
// Reads and Writes files disagreed about what crossed the wire.
func TestRecorderHandsOnNothingItCouldNotRecord(t *testing.T) {
	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{readData: optionsFrame(0x84)}, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	recorder := rec.(*ConnectionRecorder)

	// Closing the Reads file underneath is what a full disk looks like from record:
	// the frame decodes, and writing it out is what fails.
	if err := recorder.fd_reads.Close(); err != nil {
		t.Fatalf("closing the Reads file: %v", err)
	}

	n, err := rec.Read(make([]byte, 64))
	if err == nil {
		t.Fatal("a frame that could not be recorded was handed to the driver")
	}
	if n != 0 {
		t.Errorf("Read = (%d, %v), want (0, err)", n, err)
	}
	if _, err := rec.Write(optionsFrame(0x04)); err == nil {
		t.Error("the recording failure was not latched")
	}
}

// closeRacingConn is a net.Conn whose Write is still in flight when Close arrives and
// fails the way a real socket does once that close has landed.
//
// Close releases the write and then waits for it to have decided what to do with the
// error, so the ordering the test is about is pinned rather than raced: the recorder
// must have marked itself closed before the socket goes, not after.
type closeRacingConn struct {
	stubConn
	inWrite  chan struct{}
	release  chan struct{}
	observed chan struct{}
}

func (c *closeRacingConn) Write([]byte) (int, error) {
	close(c.inWrite)
	<-c.release
	return 0, net.ErrClosed
}

func (c *closeRacingConn) Close() error {
	close(c.release)
	<-c.observed
	return c.stubConn.Close()
}

// TestConnectionRecorderCloseDoesNotLatchItsOwnTeardown pins that the close's own
// casualties are not reported as the reason the recording ended.
//
// gocql closes a connection from whichever goroutine noticed it was done, while the
// write coalescer may still be inside WriteTo, so a graceful close routinely fails an
// in-flight write with net.ErrClosed. Latched, that outranks everything else Close has
// to say -- and closeWithError hands Close's error to errorHandler.HandleError, so a
// complete recording reported its own teardown as a connection failure.
func TestConnectionRecorderCloseDoesNotLatchItsOwnTeardown(t *testing.T) {
	conn := &closeRacingConn{
		inWrite:  make(chan struct{}),
		release:  make(chan struct{}),
		observed: make(chan struct{}),
	}

	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}

	writeErr := make(chan error, 1)
	go func() {
		_, err := rec.Write(startupFrame())
		close(conn.observed)
		writeErr <- err
	}()

	<-conn.inWrite
	if err := rec.Close(); err != nil {
		t.Errorf("Close reported %v; nothing was wrong with the recording", err)
	}

	// The caller still learns its write failed. Only the latch is skipped.
	if err := <-writeErr; !errors.Is(err, net.ErrClosed) {
		t.Errorf("Write returned %v, want the socket error to reach the caller", err)
	}
}
