package recorder

import (
	"bytes"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gocql/gocql/dialer"
	"github.com/gocql/gocql/dialer/replayer"
	"github.com/gocql/gocql/internal/segment"
)

// passthroughCompressor is a no-op compressor that honours the append contract. A real
// lz4 lives in a separate Go module; what these tests need is the compressed segment
// *layout*, not compression.
type passthroughCompressor struct{}

func (passthroughCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (passthroughCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// v5Frame builds a protocol v5 frame with the given opcode, direction and body.
func v5Frame(op byte, response bool, streamID int, body []byte) []byte {
	version := byte(0x05)
	if response {
		version = 0x85
	}
	frame := []byte{
		version, 0x00,
		byte(streamID >> 8), byte(streamID & 0xFF),
		op,
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}
	return append(frame, body...)
}

// startupV5 builds a v5 STARTUP, optionally naming a compression algorithm, which is
// what tells the dialers the segment header layout.
func startupV5(compression string) []byte {
	opts := [][2]string{{"CQL_VERSION", "3.0.0"}}
	if compression != "" {
		opts = append(opts, [2]string{"COMPRESSION", compression})
	}

	var body []byte
	body = append(body, byte(len(opts)>>8), byte(len(opts)))
	for _, kv := range opts {
		for _, s := range kv {
			body = append(body, byte(len(s)>>8), byte(len(s)))
			body = append(body, s...)
		}
	}
	return v5Frame(0x01, false, 0x0040, body)
}

// exchange is one request/response pair of a simulated connection.
type exchange struct {
	request  []byte
	response []byte
	// segmented reports whether these frames are on the wire as transport segments,
	// which for the handshake frames they are not.
	segmented bool
}

// v5Conversation is a v5 connection from OPTIONS through to a couple of queries,
// including authentication — which is where the framing boundary is easy to get wrong,
// because AUTH_RESPONSE is already segmented.
func v5Conversation(compression string) []exchange {
	big := bytes.Repeat([]byte{0x5A}, 2*segment.MaxPayloadSize+11)

	return []exchange{
		// Unsegmented handshake.
		{request: v5Frame(0x05, false, 0x0001, nil), response: v5Frame(0x06, true, 0x0001, nil)},
		{request: startupV5(compression), response: v5Frame(0x03, true, 0x0040, []byte{0x00, 0x04, 'n', 'o', 'n', 'e'})},

		// Segmented from AUTH_RESPONSE onward.
		{request: v5Frame(0x0F, false, 0x0080, []byte{0x00, 0x00, 0x00, 0x02, 0x01, 0x02}), response: v5Frame(0x10, true, 0x0080, nil), segmented: true},
		{request: v5Frame(0x09, false, 0x00C0, []byte{0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'}), response: v5Frame(0x08, true, 0x00C0, []byte{0x00, 0x00, 0x00, 0x01}), segmented: true},

		// An EXECUTE whose response is far larger than one segment can carry, so the
		// chain path runs.
		{request: v5Frame(0x0A, false, 0x0100, []byte{
			0x00, 0x02, 'h', 'i', // preparedID, [short bytes]
			0x00, 0x00, // resultMetadataID, empty [short bytes]
			0x00, 0x01, // consistency ONE
			0x00, 0x00, 0x00, 0x00, // v5 flags, none set
		}), response: v5Frame(0x08, true, 0x0100, big), segmented: true},
	}
}

// onWire renders one frame as the bytes that would cross the socket for it.
func onWire(t *testing.T, frame []byte, segmented bool, comp dialer.SegmentCompressor) []byte {
	t.Helper()

	if !segmented {
		return frame
	}
	out, err := dialer.AppendSegmented(nil, frame, comp)
	if err != nil {
		t.Fatalf("encoding a frame: %v", err)
	}
	return out
}

// wire renders one direction of a conversation as the whole byte stream, which is what
// the recorder's stub connection hands back.
func wire(t *testing.T, conv []exchange, comp dialer.SegmentCompressor, responses bool) []byte {
	t.Helper()

	var out []byte
	for _, ex := range conv {
		frame := ex.request
		if responses {
			frame = ex.response
		}
		out = append(out, onWire(t, frame, ex.segmented, comp)...)
	}
	return out
}

// readExactly drains n bytes from conn in small chunks, so a recorder has to reassemble
// segments across reads rather than being handed each one whole.
func readExactly(t *testing.T, conn net.Conn, n int) {
	t.Helper()

	buf := make([]byte, 37)
	for read := 0; read < n; {
		got, err := conn.Read(buf[:min(len(buf), n-read)])
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		read += got
	}
}

// TestProtocolV5RoundTrip records a v5 connection and replays it, which is the whole
// point of the segment support: the recorder is handed transport segments and has to
// store the frames inside them, and the replayer has to put them back on the wire in
// the framing the driver expects.
//
// It cannot be done against a real cluster. controlConn.discoverProtocol pins
// negotiation to v4 and ScyllaDB does not speak v5, so `go test -C tests/bench
// -update-golden` can never produce a v5 recording. This drives both dialers in
// process instead.
func TestProtocolV5RoundTrip(t *testing.T) {
	for _, cc := range []struct {
		name        string
		compression string
		comp        dialer.SegmentCompressor
	}{
		{name: "uncompressed", compression: ""},
		{name: "compressed", compression: "lz4", comp: passthroughCompressor{}},
	} {
		t.Run(cc.name, func(t *testing.T) {
			conv := v5Conversation(cc.compression)
			fname := filepath.Join(t.TempDir(), "conn")

			recordConversation(t, fname, conv, cc.comp)

			// The recording must hold bare frames, decompressed and unwrapped, so the
			// format does not depend on the protocol version.
			for _, side := range []struct {
				suffix string
				want   func(exchange) []byte
			}{
				{suffix: "Writes", want: func(e exchange) []byte { return e.request }},
				{suffix: "Reads", want: func(e exchange) []byte { return e.response }},
			} {
				got := recordedFrames(t, fname+side.suffix)
				if len(got) != len(conv) {
					t.Fatalf("%s: recorded %d frames, want %d", side.suffix, len(got), len(conv))
				}
				for i, ex := range conv {
					if !bytes.Equal(got[i].Data, side.want(ex)) {
						t.Errorf("%s: frame %d was not recorded as a bare CQL frame", side.suffix, i)
					}
				}
			}

			replayConversation(t, fname, conv, cc.comp)
		})
	}
}

// queryV5 builds a protocol v5 QUERY carrying a page size and a default timestamp,
// the way querySystem sends one: a [long string] query text, the consistency, the
// 4-byte v5 flags field, and then the fields those flags announce.
func queryV5(streamID int, query string, timestamp int64) []byte {
	body := []byte{byte(len(query) >> 24), byte(len(query) >> 16), byte(len(query) >> 8), byte(len(query))}
	body = append(body, query...)
	body = append(body, 0x00, 0x01)             // consistency ONE
	body = append(body, 0x00, 0x00, 0x00, 0x24) // flags: page size | default timestamp
	body = append(body, 0x00, 0x00, 0x13, 0x88) // page size 5000
	for shift := 56; shift >= 0; shift -= 8 {
		body = append(body, byte(timestamp>>uint(shift)))
	}
	return v5Frame(0x07, false, streamID, body)
}

// TestProtocolV5QueryReplaysWithAFreshTimestamp is the round trip a real driver
// performs and TestProtocolV5RoundTrip cannot: it replays a request that is not
// byte-identical to the recorded one.
//
// The default timestamp is time.Now() at send (framer.writeQueryParams), so the QUERY
// the driver puts on the wire during replay differs from the recorded one in those
// eight bytes and in nothing else. The hash therefore has to cover the query text --
// so the two statements are told apart -- while leaving the timestamp out.
//
// Before scylladb/gocql#1000 a v5 QUERY could satisfy neither. The parameter walk was
// handed the body start, read the text's length and first two characters as the 4-byte
// flags field, failed a bounds check on the nonsense that produced, and fell back to
// hashing the whole frame -- timestamp included. Replaying either query below panicked
// with "unable to find a response to replay".
func TestProtocolV5QueryReplaysWithAFreshTimestamp(t *testing.T) {
	const (
		local = "SELECT * FROM system.local WHERE key='local'"
		peers = "SELECT peer, data_center FROM system.peers"
	)

	// The handshake, unsegmented; then two distinct queries, segmented. Separate
	// stream ids because a recording pairs requests with responses by stream id.
	handshake := []exchange{
		{request: v5Frame(0x05, false, 0x0001, nil), response: v5Frame(0x06, true, 0x0001, nil)},
		{request: startupV5(""), response: v5Frame(0x02, true, 0x0040, nil)},
	}
	queries := func(timestamp int64) []exchange {
		return []exchange{
			{request: queryV5(0x0080, local, timestamp), response: v5Frame(0x08, true, 0x0080, []byte{0x00, 0x00, 0x00, 0x01}), segmented: true},
			{request: queryV5(0x00C0, peers, timestamp), response: v5Frame(0x08, true, 0x00C0, []byte{0x00, 0x00, 0x00, 0x02}), segmented: true},
		}
	}

	fname := filepath.Join(t.TempDir(), "conn")
	recordConversation(t, fname, append(append([]exchange{}, handshake...), queries(111)...), nil)

	// Replay the same two statements, stamped with a different timestamp. Each must
	// come back with its own recorded response, not the other one's.
	replayConversation(t, fname, append(append([]exchange{}, handshake...), queries(999999)...), nil)
}

// recordConversation records conv through a ConnectionRecorder fed the bytes a socket
// would have carried for it, and closes the recorder.
func recordConversation(t *testing.T, fname string, conv []exchange, comp dialer.SegmentCompressor) {
	t.Helper()

	rec, err := NewConnectionRecorder(fname, &stubConn{readData: wire(t, conv, comp, true)}, comp)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}

	// Request, response, request — in that order. The framing switch is announced by
	// a response and applies to the requests after it, so recording every request up
	// front would never see it.
	for i, ex := range conv {
		if _, err := rec.Write(onWire(t, ex.request, ex.segmented, comp)); err != nil {
			t.Fatalf("exchange %d: recording the request: %v", i, err)
		}
		readExactly(t, rec, len(onWire(t, ex.response, ex.segmented, comp)))
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("closing the recorder: %v", err)
	}
}

// replayConversation replays conv against the recording at fname, and checks every
// response comes back framed the way the driver expects.
func replayConversation(t *testing.T, fname string, conv []exchange, comp dialer.SegmentCompressor) {
	t.Helper()

	rep, err := replayer.NewConnectionReplayer(fname, comp)
	if err != nil {
		t.Fatalf("NewConnectionReplayer: %v", err)
	}
	defer func() {
		if err := rep.Close(); err != nil {
			t.Errorf("closing the replayer: %v", err)
		}
	}()

	for i, ex := range conv {
		if _, err := rep.Write(onWire(t, ex.request, ex.segmented, comp)); err != nil {
			t.Fatalf("exchange %d: replaying the request: %v", i, err)
		}

		want, err := expectedResponseWire(ex, comp)
		if err != nil {
			t.Fatalf("exchange %d: %v", i, err)
		}

		got := make([]byte, 0, len(want))
		chunk := make([]byte, 53)
		for len(got) < len(want) {
			n, err := rep.Read(chunk)
			if err != nil {
				t.Fatalf("exchange %d: reading the response: %v", i, err)
			}
			got = append(got, chunk[:n]...)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("exchange %d: replayed % X, want % X", i, got, want)
		}
	}
}

// expectedResponseWire is what the replayer should put on the wire for one exchange.
func expectedResponseWire(ex exchange, comp dialer.SegmentCompressor) ([]byte, error) {
	if !ex.segmented {
		return ex.response, nil
	}
	return dialer.AppendSegmented(nil, ex.response, comp)
}

// TestProtocolV5RoundTripNeedsACompressor pins that a compressed v5 connection with no
// compressor supplied says so, rather than decoding the stream with the wrong segment
// header size and reporting corrupt frames.
func TestProtocolV5RoundTripNeedsACompressor(t *testing.T) {
	conv := v5Conversation("lz4")
	fname := filepath.Join(t.TempDir(), "conn")

	rec, err := NewConnectionRecorder(fname, &stubConn{readData: wire(t, conv, passthroughCompressor{}, true)}, nil)
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	// The handshake goes through unsegmented; the first segmented write is where the
	// missing compressor is discovered.
	for i, ex := range conv[:2] {
		if _, err := rec.Write(ex.request); err != nil {
			t.Fatalf("exchange %d: %v", i, err)
		}
		readExactly(t, rec, len(ex.response))
	}

	_, err = rec.Write(onWire(t, conv[2].request, true, passthroughCompressor{}))
	if err == nil {
		t.Fatal("a compressed v5 connection was recorded without a compressor")
	}
	if !errors.Is(err, dialer.ErrSegmentCompressorRequired) {
		t.Errorf("error = %v, want dialer.ErrSegmentCompressorRequired", err)
	}
}

// TestRecorderSendsNothingItCouldNotRecord pins the write path's order: the frame
// is recorded before it is sent, so a request the recording refuses -- here a
// pre-v5 compressed STARTUP -- never reaches the server, Write reports (0, err)
// rather than success with a trailing error the write coalescer would swallow, and
// the failure is latched so both directions return it from then on: the connection
// comes down at the failure instead of past it, recording a lie.
func TestRecorderSendsNothingItCouldNotRecord(t *testing.T) {
	startup := startupV5("lz4")
	startup[0] = 0x04

	conn := &stubConn{}
	rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), conn, passthroughCompressor{})
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	n, err := rec.Write(startup)
	if err == nil {
		t.Fatal("a compressed protocol v4 connection was recorded")
	}
	if n != 0 {
		t.Errorf("Write = %d bytes, want 0", n)
	}
	if len(conn.written) != 0 {
		t.Errorf("%d bytes reached the connection after the recording refused the frame", len(conn.written))
	}

	if _, err := rec.Write(optionsFrame(0x04)); err == nil {
		t.Error("a Write after the recording failed was accepted")
	}
	if _, err := rec.Read(make([]byte, 8)); err == nil {
		t.Error("a Read after the recording failed was accepted")
	}
}

// TestRecorderRefusesPreV5Compression pins that a compressed connection below protocol
// v5 is refused where the cause is still visible.
//
// Below v5 a negotiated compressor compresses frame bodies, not transport segments, so
// nothing here would fail: the recorder would write the frames out with compressed
// bodies and look successful. The cost lands on whoever replays that recording, as a
// panic from GetFrameHash finding no match -- because a compressed body carries the
// encoded default timestamp, so the hash differs on every run -- with nothing in it
// pointing at compression.
func TestRecorderRefusesPreV5Compression(t *testing.T) {
	// A v4 STARTUP naming lz4: same body as the v5 one, one version byte apart.
	startup := startupV5("lz4")
	startup[0] = 0x04

	fname := filepath.Join(t.TempDir(), "conn")
	rec, err := NewConnectionRecorder(fname, &stubConn{}, passthroughCompressor{})
	if err != nil {
		t.Fatalf("NewConnectionRecorder: %v", err)
	}
	defer rec.Close()

	_, err = rec.Write(startup)
	if err == nil {
		t.Fatal("a compressed protocol v4 connection was recorded")
	}
	for _, want := range []string{"lz4", "v4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
