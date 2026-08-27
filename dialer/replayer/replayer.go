package replayer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer"
)

func NewReplayDialer(dir string) *ReplayDialer {
	return &ReplayDialer{
		dir: dir,
	}
}

type ReplayDialer struct {
	dir string
	net.Dialer
}

func (d *ReplayDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	sourcePort := gocql.ScyllaGetSourcePort(ctx)
	return NewConnectionReplayer(path.Join(d.dir, fmt.Sprintf("%s-%d", addr, sourcePort)))
}

func NewConnectionReplayer(fname string) (net.Conn, error) {
	frames, err := loadResponseFramesFromFiles(fname+"Reads", fname+"Writes")
	if err != nil {
		return nil, err
	}
	return &ConnectionReplayer{frames: frames, frameIdsToReplay: []int{}, streamIdsToReplay: []int{}, gotRequest: make(chan struct{}, 1)}, nil
}

type ConnectionReplayer struct {
	// failed is the failure that ended this connection, if one did. Only materialise
	// raises one, and nothing it refuses is repairable by trying again -- while frameIdx
	// is not advanced past a record that failed, so without this Read would fetch that
	// same record on every call and hand the driver an unbounded stream of errors:
	// identical to read, and distinct as values, one fresh fmt.Errorf per call.
	//
	// A plain field rather than an atomic, and read by nothing outside Read. Write runs
	// on another goroutine and deliberately does not consult it. The recorder gates both
	// its directions, but it holds its latch in an atomic.Pointer because it had to; this
	// connection's cross-goroutine state -- frameIdsToReplay, streamIdsToReplay, closed
	// and the gotRequest handshake around them -- is being replaced wholesale by a mutex
	// and a sync.Cond in scylladb/gocql#1020, and a fourth shared field here would be one
	// more thing for that change to unpick. Gating Write would also cost the guarantee
	// dialer.FrameSplitter.Feed rests on: that this type's Write is the one Feed caller
	// with no failure gate of its own.
	//
	// It leads the struct only to keep the pointer-bearing fields contiguous, which the
	// fieldalignment vet check requires -- an error is two words of pointers.
	failed error

	gotRequest        chan struct{}
	frames            []*FrameRecorded
	frameIdsToReplay  []int
	streamIdsToReplay []int
	// outgoing is the response currently being served: a copy of the recorded frame
	// with its stream id patched, handed out across as many Read calls as it takes.
	// Materialising it once is what makes the stream id right regardless of how the
	// caller's buffer is sized.
	outgoing []byte
	// splitter turns the bytes the driver writes into whole CQL frames. The write
	// path happens to deliver exactly one frame per Write today — there is no
	// bufio.Writer on it, and although write coalescing is on by default,
	// net.Buffers.WriteTo only uses writev when the writer implements the unexported
	// buffersWriter, which this type does not, because it holds its net.Conn as a
	// named field rather than embedding it. That is an implementation detail of the
	// standard library plus a struct-layout choice, not a contract: embedding
	// net.Conn here would silently start coalescing several frames into one Write.
	// The exported Conn.Write passes arbitrary bytes regardless. So the assumption
	// is not relied on.
	splitter    dialer.FrameSplitter
	outgoingPos int
	frameIdx    int
	closed      bool
	// useMetadataID latches once the STARTUP request on this connection opts into
	// SCYLLA_USE_METADATA_ID, matching how the recorder stamped the frames so live
	// and load-time hashes agree (see GetFrameHash / Record.UseMetadataID).
	useMetadataID bool
}

// fail latches err as the failure that ended this connection and returns it. Only the
// first is kept, as in dialer.FrameSplitter.fail: a caller can never be handed a second
// error, so a later Read reporting the same value is what says the connection was latched
// rather than retried.
func (c *ConnectionReplayer) fail(err error) error {
	if c.failed == nil {
		c.failed = err
	}
	return c.failed
}

func (c *ConnectionReplayer) frameStreamID() int {
	return c.streamIdsToReplay[c.frameIdx]
}

func (c *ConnectionReplayer) getPendingFrame() *FrameRecorded {
	if c.frameIdx < 0 || c.frameIdx >= len(c.frameIdsToReplay) {
		return nil
	}
	frameId := c.frameIdsToReplay[c.frameIdx]
	if frameId < 0 || frameId >= len(c.frames) {
		return nil
	}
	return c.frames[frameId]
}

func (c *ConnectionReplayer) pushStreamIDToReplay(b []byte, idx int) {
	if b[0] > 0x02 {
		c.streamIdsToReplay = append(c.streamIdsToReplay, int(b[2])<<8|int(b[3]))
	} else {
		c.streamIdsToReplay = append(c.streamIdsToReplay, int(b[2]))
	}
	c.frameIdsToReplay = append(c.frameIdsToReplay, idx)

	select {
	case c.gotRequest <- struct{}{}:
	default:
	}
}

// headerLen is the v3+ CQL frame header: version, flags, a 2-byte stream id, the
// opcode and the 4-byte body length.
//
// A local constant: the dialer package keeps its own copy unexported. Fixed at the v3+
// layout like the rest of this pipeline -- the frame splitter feeding the recorder
// slices on the same nine bytes, so a v1/v2 stream never reaches a recording intact
// anyway (scylladb/gocql#1022).
const headerLen = 9

// maxRetainedResponse bounds the buffer kept between responses.
//
// It is reused so that serving a response costs no allocation, but a recording may hold
// one of any size the driver's own frame limit allows, and reusing that for the life of
// the connection would pin it there. So an outlier is handed back instead, the way the
// dialer's frame splitter hands back an outsized frame. Its constant is unexported,
// hence this one.
const maxRetainedResponse = 64 << 10

// wholeFrame reports whether b is exactly one CQL frame: a full v3+ header followed by
// the body length that header declares, no more and no less. More would mean two frames
// in one record, which the recorder cannot write -- it records what its decoder hands
// it, one whole frame at a time.
func wholeFrame(b []byte) bool {
	if len(b) < headerLen {
		return false
	}
	declared := int64(b[5])<<24 | int64(b[6])<<16 | int64(b[7])<<8 | int64(b[8])
	return int64(len(b))-headerLen == declared
}

func replaceFrameStreamID(b []byte, stream int) {
	if b[0] > 0x02 {
		b[2] = byte(stream >> 8)
		b[3] = byte(stream)
	} else {
		b[2] = byte(stream)
	}
}

// Read serves the recorded response to the request most recently matched, across as
// many calls as the caller's buffer needs.
//
// A latched failure ends it. materialise fails on a record no amount of retrying repairs,
// and it fails before frameIdx moves, so the branch below would fetch that same record
// again on the next call and the driver -- which answers a read error by tearing the
// connection down and redialling -- would be handed the same failure for as long as it
// kept reading. It is reported ahead of the zero-length shortcut for the same reason
// dialer.Decoder.Feed reports one for an empty buffer: (0, nil) is the answer that reads
// as a healthy connection with nothing ready yet. io.EOF after Close is deliberately not
// latched -- it is not a failure, and it is already idempotent.
func (c *ConnectionReplayer) Read(b []byte) (n int, err error) {
	if c.failed != nil {
		return 0, c.failed
	}
	if len(b) == 0 {
		return 0, nil
	}

	if c.outgoingPos == len(c.outgoing) {
		// The response served last has drained, so an outsized buffer goes back before
		// the next one is copied into it, and before the wait below rather than after
		// it: a connection parked for the next request should not be holding it. See
		// maxRetainedResponse.
		if cap(c.outgoing) > maxRetainedResponse {
			c.outgoing, c.outgoingPos = nil, 0
		}

		frame := c.getPendingFrame()
		for frame == nil {
			<-c.gotRequest
			frame = c.getPendingFrame()
		}
		if c.Closed() {
			return 0, io.EOF
		}
		if err := c.materialise(frame); err != nil {
			return 0, c.fail(err)
		}
		c.frameIdx = c.frameIdx + 1
	}

	n = copy(b, c.outgoing[c.outgoingPos:])
	c.outgoingPos = c.outgoingPos + n
	return n, nil
}

// materialise prepares the bytes for one recorded response, reading the stream id to
// patch in from the request that matched it.
//
// The frame is copied first. FrameRecorded is shared and served once per benchmark
// iteration, so patching it in place would rewrite the recording's own copy with the
// stream id of whichever request arrived first and leave every later iteration
// depending on that.
//
// This used to patch the caller's buffer instead, once per response and only when the
// whole response fitted, so a response larger than the buffer was replayed with the
// stream id it was recorded with. Unreachable with the checked-in recordings, whose
// largest response is well under the driver's 4 KiB read buffer, which is why it went
// unnoticed.
func (c *ConnectionReplayer) materialise(frame *FrameRecorded) error {
	// A record that is not one whole frame cannot be served at all, and the driver
	// cannot be left to reject it. It reads a frame with io.ReadFull twice -- the nine
	// header bytes, then exactly the body length that header declares -- and this
	// connection's SetReadDeadline does nothing, so anything short of a whole frame
	// leaves the driver blocked mid-frame with no deadline to end it, while Read parks
	// waiting for a request whose response has already been counted as delivered. A
	// record holding no bytes at all is the same failure one step earlier: Read copies
	// nothing into a buffer with room in it and returns (0, nil).
	//
	// If another goroutine's request does arrive, the outcome is worse than the hang:
	// the next response's bytes are served as the remainder of this one.
	//
	// Only a damaged recording produces either. loadFramesFromFile skips lines that do
	// not decode, but `{"data":null}` decodes fine, and a recording is a file on disk
	// that can be truncated mid-frame.
	if !wholeFrame(frame.Response) {
		return fmt.Errorf("gocql/dialer: recording holds %d bytes for stream %d, which is not one whole CQL frame", len(frame.Response), c.frameStreamID())
	}

	c.outgoing = append(c.outgoing[:0], frame.Response...)
	c.outgoingPos = 0

	replaceFrameStreamID(c.outgoing, c.frameStreamID())
	return nil
}

func (c *ConnectionReplayer) Write(b []byte) (n int, err error) {
	if err := c.splitter.Feed(b, c.matchRequest); err != nil {
		return 0, err
	}
	return len(b), nil
}

// matchRequest finds the recorded response for one request frame and queues it.
func (c *ConnectionReplayer) matchRequest(frame []byte) error {
	// A request frame's first byte is its protocol version, and the driver's
	// handshake frames are never segment-framed — so a v5+ connection is
	// rejected here during the handshake. Past it, v5 switches to transport
	// segments, which this replayer can neither hash for matching nor patch
	// stream ids into without breaking the segment CRCs.
	if dialer.FrameIsProtoV5OrNewer(frame) {
		return dialer.ErrProtoV5NotSupported
	}

	if !c.useMetadataID && dialer.StartupNegotiatesMetadataID(frame) {
		c.useMetadataID = true
	}
	writeHash := dialer.GetFrameHash(frame, c.useMetadataID)

	for i, q := range c.frames {
		if q.Hash == writeHash {
			c.pushStreamIDToReplay(frame, i)
			return nil
		}
	}

	// Deliberately a panic rather than a returned error. A request with no recorded
	// response means the recording does not match the driver that is replaying it,
	// which no amount of retrying fixes — and returning an error here would be
	// handled as a connection failure and retried or reported far from the cause.
	// The replay benchmarks depend on this being loud: it is what makes them a
	// regression test for the frame hashing rather than a timing measurement.
	panic(fmt.Errorf("unable to find a response to replay"))
}

func (c *ConnectionReplayer) Close() error {
	close(c.gotRequest)
	c.closed = true
	return nil
}

func (c *ConnectionReplayer) Closed() bool {
	return c.closed
}

type MockAddr struct {
	network string
	address string
}

func (m *MockAddr) Network() string {
	return m.network
}

func (m *MockAddr) String() string {
	return m.address
}

func (c ConnectionReplayer) LocalAddr() net.Addr {
	return &MockAddr{
		network: "tcp",
		address: "10.0.0.1:54321",
	}
}

func (c ConnectionReplayer) RemoteAddr() net.Addr {
	return &MockAddr{
		network: "tcp",
		address: "192.168.1.100:12345",
	}
}

func (c ConnectionReplayer) SetDeadline(t time.Time) error {
	return nil
}

func (c ConnectionReplayer) SetReadDeadline(t time.Time) error {
	return nil
}

func (c ConnectionReplayer) SetWriteDeadline(t time.Time) error {
	return nil
}

func loadFramesFromFile(filename string) (map[int]dialer.Record, error) {
	records := make(map[int]dialer.Record)

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	// Read the records with a plain bufio.Reader rather than a bufio.Scanner: a
	// record holds one whole frame, which encoding/json base64-inflates by 4/3, so
	// any frame over ~48 KiB exceeds the scanner's default bufio.MaxScanTokenSize
	// and fails the load. A frame's length is the peer's to choose (up to the
	// driver's own frm.MaxFrameSize), so no fixed line cap is the right one; ReadBytes
	// grows to the line in front of it and nothing larger.
	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			var record dialer.Record
			// A recording is a debugging artefact and can be truncated or edited, so
			// a record that does not decode is reported and skipped rather than
			// failing the whole file — the frames around it still replay.
			if err := json.Unmarshal(line, &record); err != nil {
				fmt.Printf("Error decoding JSON in %s: %s\n", filename, err)
			} else {
				records[record.StreamID] = record
			}
		}
		if readErr != nil {
			// A final record without its newline is still a record; io.EOF ends the
			// file either way.
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error reading file %s: %w", filename, readErr)
		}
	}
	return records, nil
}

func loadResponseFramesFromFiles(read_file, write_file string) ([]*FrameRecorded, error) {
	read_records, err := loadFramesFromFile(read_file)
	if err != nil {
		return nil, err
	}
	write_records, err := loadFramesFromFile(write_file)
	if err != nil {
		return nil, err
	}

	var frames = []*FrameRecorded{}
	for streamID, record1 := range read_records {
		if record2, exists := write_records[streamID]; exists {
			frames = append(frames, &FrameRecorded{Response: record1.Data, Hash: dialer.GetFrameHash(record2.Data, record2.UseMetadataID)})
		}
	}
	return frames, nil
}

type FrameRecorded struct {
	Response []byte
	Hash     int64
}
