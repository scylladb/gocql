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
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer"
)

// Option configures a ReplayDialer.
type Option func(*ReplayDialer)

// WithSegmentCompressor supplies the compressor a protocol v5 connection's transport
// segments are compressed with.
//
// It has to be supplied rather than derived: the only implementation, lz4, lives in a
// separate Go module, so neither the driver nor this package can construct one. Pass
// the same compressor the ClusterConfig being replayed against uses -- the layout
// follows what the replaying driver negotiates, not what was recorded, because
// recordings hold bare frames.
//
// Its name is checked against the algorithm the STARTUP names, when it reports one, and
// a compressed connection below protocol v5 is refused outright: there compression
// applies to frame bodies rather than transport segments, which this package does not
// implement, and left alone it would surface as a replay whose hashes never match.
func WithSegmentCompressor(comp dialer.SegmentCompressor) Option {
	return func(d *ReplayDialer) { d.comp = comp }
}

func NewReplayDialer(dir string, opts ...Option) *ReplayDialer {
	d := &ReplayDialer{dir: dir}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

type ReplayDialer struct {
	dir  string
	comp dialer.SegmentCompressor
	net.Dialer
}

func (d *ReplayDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	sourcePort := gocql.ScyllaGetSourcePort(ctx)
	return NewConnectionReplayer(path.Join(d.dir, fmt.Sprintf("%s-%d", addr, sourcePort)), d.comp)
}

// NewConnectionReplayer answers requests from the recording at fname.
//
// comp may be nil; see WithSegmentCompressor for when it is needed.
func NewConnectionReplayer(fname string, comp dialer.SegmentCompressor) (net.Conn, error) {
	frames, proto, err := loadResponseFramesFromFiles(fname+"Reads", fname+"Writes")
	if err != nil {
		return nil, err
	}
	// Refused here rather than in the loader, which reports the protocol version off a
	// recording that pairs nothing on purpose -- a version check that disarms itself
	// exactly when the recording is damaged is not a check. A replayer over zero frames
	// is a different matter: the dial succeeds and the first request then panics with
	// nothing naming the recording, which is what a mistyped directory, or a source port
	// this recording never saw, produces.
	if len(frames) == 0 {
		return nil, fmt.Errorf("gocql/dialer: %sReads and %sWrites pair no requests with responses; there is nothing to replay", fname, fname)
	}
	framing := dialer.NewFraming(comp)
	return &ConnectionReplayer{
		frames:            frames,
		recordedProto:     proto,
		frameIdsToReplay:  []int{},
		streamIdsToReplay: []int{},
		gotRequest:        make(chan struct{}, 1),
		framing:           framing,
		requests:          framing.NewDecoder(),
	}, nil
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
	// framing tracks how this connection's bytes are framed, and is shared by the
	// request decoder and the response encoder: the handshake fact that switches them
	// is carried by a response but applies to requests too.
	framing *dialer.Framing
	// requests turns the bytes the driver writes into whole CQL frames, following the
	// connection across the switch to transport segments.
	//
	// The write path happens to deliver exactly one frame per Write today -- there is
	// no bufio.Writer on it, and although write coalescing is on by default,
	// net.Buffers.WriteTo only uses writev when the writer implements the unexported
	// buffersWriter, which this type does not, because it holds its net.Conn as a
	// named field rather than embedding it. That is an implementation detail of the
	// standard library plus a struct-layout choice, not a contract: embedding
	// net.Conn here would silently start coalescing several frames into one Write.
	// The exported Conn.Write passes arbitrary bytes regardless. So the assumption is
	// not relied on.
	requests *dialer.Decoder
	// patched is the recorded response with its stream id rewritten, before framing.
	patched []byte
	// outgoing is what is actually served: patched, wrapped in whatever framing the
	// connection has reached, handed out across as many Read calls as it takes.
	// Materialising it once is what makes the stream id right regardless of how the
	// caller's buffer is sized.
	outgoing      []byte
	outgoingPos   int
	frameIdx      int
	recordedProto byte
	closed        bool
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

// A stream id is two bytes at frame[2:4] on every protocol this package reads. It used
// to be one byte on v1/v2, and both functions below forked on the version to find out
// -- dialer.FrameSplitter now refuses a connection under v3 before either can see it.
func (c *ConnectionReplayer) pushStreamIDToReplay(b []byte, idx int) {
	c.streamIdsToReplay = append(c.streamIdsToReplay, int(b[2])<<8|int(b[3]))
	c.frameIdsToReplay = append(c.frameIdsToReplay, idx)

	select {
	case c.gotRequest <- struct{}{}:
	default:
	}
}

// maxRetainedResponse bounds the buffers kept between responses.
//
// They are reused so that serving a response costs no allocation, but a recording may
// hold one of any size the driver's own frame limit allows, and reusing that for the
// life of the connection would pin it twice over -- once patched, once encoded. So an
// outlier is handed back instead, the way the dialer's frame splitter hands back an
// outsized frame. Its constant is unexported, hence this one.
const maxRetainedResponse = 64 << 10

// wholeFrame reports whether b is exactly one CQL frame: a full v3+ header followed by
// the body length that header declares, no more and no less. More would mean two frames
// in one record, which the recorder cannot write -- it records what its decoder hands
// it, one whole frame at a time.
func wholeFrame(b []byte) bool {
	declared, ok := dialer.FrameBodyLen(b)
	return ok && len(b)-dialer.FrameHeaderLen == declared
}

func replaceFrameStreamID(b []byte, stream int) {
	b[2] = byte(stream >> 8)
	b[3] = byte(stream)
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
		// the next one is encoded into it, and before the wait below rather than after
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
//
// On failure it leaves nothing servable behind: outgoingPos == len(outgoing), so Read
// finds the branch it would take anyway rather than a buffer half describing a response
// that was never encoded.
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

	c.patched = append(c.patched[:0], frame.Response...)
	replaceFrameStreamID(c.patched, c.frameStreamID())

	// Encode before observing. The frame that flips the framing latch -- READY or
	// AUTHENTICATE -- is itself unsegmented; the switch applies to what comes after
	// it. Observing first would wrap the very frame that announces the change.
	//
	// A failure empties outgoing rather than leaving it as it was. EncodeResponse encodes
	// straight into the buffer it is handed, and AppendSegmented documents that the
	// compressed layout can fail after writing into it -- segment.AppendCompressed
	// reserves the segment header in dst before calling a compressor that may then error.
	// Left alone, outgoing would keep the previous response's length over bytes that are
	// no longer that response. Nothing serves them today, because outgoingPos ==
	// len(outgoing) on the way in here and Read latches the failure on the way out, but
	// that is two invariants elsewhere holding up one line here.
	out, err := c.framing.EncodeResponse(c.outgoing[:0], c.patched)
	if err != nil {
		c.outgoing, c.outgoingPos = c.outgoing[:0], 0
		return err
	}
	c.outgoing = out
	c.outgoingPos = 0

	c.framing.ObserveResponse(c.patched)

	// patched has no reader until the next response, so an outsized one is handed back
	// here rather than pinned for the life of the connection. outgoing is released in
	// Read instead, once the caller has drained it.
	if cap(c.patched) > maxRetainedResponse {
		c.patched = nil
	}
	return nil
}

func (c *ConnectionReplayer) Write(b []byte) (n int, err error) {
	if err := c.requests.Feed(b, c.matchRequest); err != nil {
		return 0, err
	}
	return len(b), nil
}

// matchRequest finds the recorded response for one request frame and queues it.
func (c *ConnectionReplayer) matchRequest(frame []byte) error {
	// A recording holds bare frames, so nothing about it announces which protocol
	// version produced them; replaying a v5 recording against a v4 driver would
	// otherwise serve it responses whose version byte it rejects deep inside
	// readHeader, far from the cause.
	if live := dialer.FrameProtoVersion(frame); c.recordedProto != 0 && live != c.recordedProto {
		return fmt.Errorf("gocql/dialer: recording is protocol v%d but the driver is replaying it at protocol v%d", c.recordedProto, live)
	}

	// Reads the COMPRESSION option, which decides the segment header layout for the
	// rest of the connection, and refuses the negotiated compressions a recording
	// cannot represent -- body compression below v5, or an algorithm the supplied
	// compressor does not implement. Reported before the hash is taken, because on a
	// pre-v5 compressed connection the hash is exactly what goes wrong, and the panic
	// it leads to names nothing.
	if err := c.framing.ObserveRequest(frame); err != nil {
		return err
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
				// Stderr, not stdout: this is a library, and a caller whose own output
				// is the point should not find a warning about a recording file in the
				// middle of it. It is still reported rather than dropped -- it is the
				// only sign that a record went missing from the pairing below.
				fmt.Fprintf(os.Stderr, "Error decoding JSON in %s: %s\n", filename, err)
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

// loadResponseFramesFromFiles pairs each recorded response with the request that
// produced it, and reports the protocol version the recording was made at.
func loadResponseFramesFromFiles(read_file, write_file string) ([]*FrameRecorded, byte, error) {
	read_records, err := loadFramesFromFile(read_file)
	if err != nil {
		return nil, 0, err
	}
	write_records, err := loadFramesFromFile(write_file)
	if err != nil {
		return nil, 0, err
	}

	var (
		frames []*FrameRecorded
		proto  byte
	)

	// Pair by stream id in sorted order, not in map order. matchRequest scans frames
	// for the first hash that matches, so were two ever to collide, this slice's order
	// would decide which response the colliding request is served.
	//
	// No checked-in recording holds such a pair -- TestCheckedInRecordingsStillLoad
	// asserts the hashes are distinct -- and the shapes that used to collide are fixed
	// rather than merely rare. This is not a live defect being worked around: it is
	// that a 64-bit hash of caller-supplied bytes cannot be promised never to collide,
	// and ranging a map made the choice afresh on every run of the same binary, which
	// is not something a replay benchmark or a fixture failure can be reproduced from.
	paired := make([]int, 0, len(read_records))
	for streamID := range read_records {
		if _, exists := write_records[streamID]; exists {
			paired = append(paired, streamID)
		}
	}
	slices.Sort(paired)

	// The protocol version comes from the records that will actually be replayed, and
	// only from them. The recorder appends every connection to one file (see
	// NewConnectionRecorder), so a record the last session did not overwrite is a
	// leftover from an earlier one -- and a leftover that negotiated a different
	// version, a downgrade probe say, would otherwise refuse a recording that replays
	// perfectly well.
	//
	// Nothing pairing at all is the exception: there the whole Writes file is used
	// instead, because a version check that disarms itself exactly when the recording
	// is damaged is not a check.
	//
	// Every request on a connection shares one version, so any record answers -- take
	// each record's stamp where it has one and its frame's own version byte where it
	// does not, for recordings that predate the field. Per record, not per file: a
	// single stamped record among unstamped leftovers would otherwise answer for all of
	// them and the rest would never be looked at, which is exactly the file a directory
	// reused across a recorder upgrade holds. Records that disagree are refused rather
	// than resolved: settling it here would settle it by iteration order, arming or
	// disarming the check in matchRequest from run to run.
	source := paired
	if len(source) == 0 {
		source = make([]int, 0, len(write_records))
		for streamID := range write_records {
			source = append(source, streamID)
		}
		slices.Sort(source)
	}

	agree := func(have, seen byte) (byte, error) {
		if have != 0 && have != seen {
			return 0, fmt.Errorf("gocql/dialer: %s holds records from protocol v%d and protocol v%d; a recording is one connection at one version", write_file, have, seen)
		}
		return seen, nil
	}
	for _, streamID := range source {
		record := write_records[streamID]
		seen := record.Proto
		if seen == 0 {
			seen = dialer.FrameProtoVersion(record.Data)
		}
		// 0 at this point is a record whose data did not survive the file, which says
		// nothing about the version rather than disagreeing with it.
		if seen == 0 {
			continue
		}
		if proto, err = agree(proto, seen); err != nil {
			return nil, 0, err
		}
	}

	for _, streamID := range paired {
		request := write_records[streamID]
		frames = append(frames, &FrameRecorded{
			Response: read_records[streamID].Data,
			Hash:     dialer.GetFrameHash(request.Data, request.UseMetadataID),
		})
	}
	return frames, proto, nil
}

type FrameRecorded struct {
	Response []byte
	Hash     int64
}
