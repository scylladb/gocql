package recorder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer"
)

// Option configures a RecordDialer.
type Option func(*RecordDialer)

// WithSegmentCompressor supplies the compressor a protocol v5 connection's transport
// segments are compressed with.
//
// It has to be supplied rather than derived: the only implementation, lz4, lives in a
// separate Go module, so neither the driver nor this package can construct one. Pass
// the same compressor the ClusterConfig uses. Recording or replaying a v5 connection
// that negotiated compression without one fails with
// dialer.ErrSegmentCompressorRequired rather than decoding the stream with the wrong
// segment header size.
//
// It is ignored on a connection that did not negotiate compression. Compression below
// protocol v5 is not ignored but refused: there it compresses frame bodies rather than
// transport segments, which is a different thing this package does not implement, and
// left alone it would surface as a recording whose hashes never match on replay.
func WithSegmentCompressor(comp dialer.SegmentCompressor) Option {
	return func(d *RecordDialer) { d.comp = comp }
}

func NewRecordDialer(dir string, opts ...Option) *RecordDialer {
	d := &RecordDialer{dir: dir}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

type RecordDialer struct {
	dir  string
	comp dialer.SegmentCompressor
	net.Dialer
}

func (d *RecordDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	sourcePort := gocql.ScyllaGetSourcePort(ctx)
	dialerWithLocalAddr := d.Dialer
	dialerWithLocalAddr.LocalAddr, err = net.ResolveTCPAddr(network, fmt.Sprintf(":%d", sourcePort))
	if err != nil {
		return nil, err
	}

	conn, err = dialerWithLocalAddr.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	rec, err := NewConnectionRecorder(path.Join(d.dir, fmt.Sprintf("%s-%d", addr, sourcePort)), conn, d.comp)
	if err != nil {
		// Nothing else will close conn: the recorder never took ownership of it, and
		// the caller is handed a nil net.Conn alongside the error. Left open it strands
		// the socket and its peer on the server, and the driver answers a failed dial
		// by redialing -- so that is one stranded pair per attempt, for as long as the
		// recording directory stays unwritable. The same leak the constructor below
		// closes on its own file descriptors, and Close on all three of its handles.
		//
		// The close error is dropped rather than joined: err is what explains this
		// dial, and there is no recording here for a close to be truthful about.
		conn.Close()
		return nil, err
	}
	return rec, nil
}

// NewConnectionRecorder wraps conn, recording every frame in both directions.
//
// comp may be nil; see WithSegmentCompressor for when it is needed.
//
// # One recording directory per run
//
// The files are opened O_APPEND under a name derived from the address and the source
// port, and RecordDialer takes that port from the context -- which is 0 unless
// shard-awareness is on. Every connection to a host therefore appends to the same pair
// of files, and a directory reused across runs keeps accumulating. The loader survives
// that only because it keys records by stream id and keeps the last one for each: the
// recordings checked into tests/bench hold 345 records from 23 stacked sessions, and
// load as the 15 that survive. Anything the last session did not overwrite is a record
// from an older one, paired with whatever response now shares its stream id.
//
// So a recording directory has to be per run. Making the file name unique per
// connection instead would need the replayer to stop deriving the same name from the
// same two values, which is a different design than the one here.
func NewConnectionRecorder(fname string, conn net.Conn, comp dialer.SegmentCompressor) (net.Conn, error) {
	fd_writes, err := os.OpenFile(fname+"Writes", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	fd_reads, err2 := os.OpenFile(fname+"Reads", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err2 != nil {
		fd_writes.Close()
		return nil, err2
	}
	// Both directions share one Framing: the handshake fact that switches them to
	// transport segments is carried by a response, and applies to requests too.
	framing := dialer.NewFraming(comp)
	return &ConnectionRecorder{
		fd_writes:    fd_writes,
		fd_reads:     fd_reads,
		orig:         conn,
		read_record:  FrameWriter{framing: framing, decoder: framing.NewDecoder(), response: true},
		write_record: FrameWriter{framing: framing, decoder: framing.NewDecoder()},
	}, nil
}

// FrameWriter records the CQL frames of one direction of a connection, one JSON
// object per line.
//
// Frame boundaries come from a dialer.Decoder, which follows the connection across the
// handshake: bare CQL frames to begin with, transport segments once a protocol v5
// connection switches. Recordings hold bare frames either way, so the format does not
// depend on the protocol version and a v5 recording is stored unwrapped and
// decompressed.
type FrameWriter struct {
	framing *dialer.Framing
	decoder *dialer.Decoder
	// response reports whether this direction carries responses. It decides which of
	// the two handshake facts this direction can observe.
	response bool
	// useMetadataID latches once a STARTUP frame on this connection opts into the
	// SCYLLA_USE_METADATA_ID extension, so every subsequent recorded frame is
	// stamped with the negotiated state (the driver only sends the opt-in when the
	// server advertised it, so its presence means the extension is active).
	//
	// Deliberately per-direction rather than on Framing: only requests carry a
	// STARTUP, so sharing it would start stamping the flag into the Reads recording
	// as well. Nothing reads it from there, and the checked-in recordings do not
	// carry the field at all.
	useMetadataID bool
}

// Write records the frames in b[:n].
func (f *FrameWriter) Write(b []byte, n int, file *os.File) error {
	return f.decoder.Feed(b[:n], func(frame []byte) error {
		return f.record(frame, file)
	})
}

// record appends one complete frame to the recording.
func (f *FrameWriter) record(frame []byte, file *os.File) error {
	if f.response {
		// Flips the framing latch when the handshake reaches READY or AUTHENTICATE,
		// so both directions decode what follows as transport segments. The frame
		// carrying that news is itself unsegmented, and has already been decoded as
		// such by the time this runs.
		f.framing.ObserveResponse(frame)
	} else {
		// Reads the COMPRESSION option, which decides the segment header layout, and
		// refuses the negotiated compressions a recording cannot represent -- body
		// compression below v5, or an algorithm the supplied compressor does not
		// implement. Both are fatal to the recording rather than to the connection, but
		// there is nothing useful to write once either is true, so it stops here.
		if err := f.framing.ObserveRequest(frame); err != nil {
			return err
		}

		// The latch reads a whole STARTUP, which is the reason frames are assembled
		// before being recorded rather than each write being appended as it arrives.
		// Missing the opt-in here would stamp every later EXECUTE false and turn
		// replay into silent hash mismatches rather than an error.
		if !f.useMetadataID && dialer.StartupNegotiatesMetadataID(frame) {
			f.useMetadataID = true
		}
	}

	record := dialer.Record{
		Data:          frame,
		StreamID:      int(frame[2])<<8 | int(frame[3]),
		UseMetadataID: f.useMetadataID,
		Proto:         dialer.FrameProtoVersion(frame),
	}

	jsonData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to encode JSON record: %w", err)
	}
	if _, err := file.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to record: %w", err)
	}
	return nil
}

type ConnectionRecorder struct {
	fd_writes *os.File
	fd_reads  *os.File
	orig      net.Conn
	// verdict is what Close reported the first time it ran, returned by every later
	// call; see Close.
	verdict error
	// fatal latches the first recording failure, so both directions fail from then
	// on: there is nothing useful to record past it, and a connection that carries
	// on writes a recording with a silent hole. Atomic because reads and writes run
	// on different goroutines (the driver's serve loop and the query's).
	fatal        atomic.Pointer[error]
	read_record  FrameWriter
	write_record FrameWriter
	// mu serialises everything that touches a recording: the two FrameWriters above,
	// the files under them, and closed. A net.Conn may be closed while its reader is
	// still in flight -- gocql closes from whichever goroutine finished with the
	// connection while serve() sits in Read -- so Close would otherwise pull the files
	// out from under a recording write, and read decoder state that Feed was still
	// appending to.
	//
	// It sits after the FrameWriters, not next to fatal where it reads better: it holds
	// no pointers, and the fieldalignment vet check counts every byte before the last
	// pointer as one the GC has to scan. Splitting the pointer-bearing fields with it
	// costs 8 such bytes and fails make check.
	mu sync.Mutex
	// closeOnce runs the teardown once, whatever a caller does; see Close.
	closeOnce sync.Once
	// closed is set by Close, under mu, before it closes the socket. Recording stops
	// there: the truncation verdict is about to be taken, so a later write would
	// invalidate the answer already given and would land on files that are closing. It
	// is also how Write knows that a send failure from this point on is the close
	// itself rather than anything about the recording.
	closed bool
}

// fail latches err as the recording's terminal failure and returns the latched failure:
// err itself when it won the race, the earlier one otherwise.
//
// Returning the latched value rather than the argument is what keeps one recording to one
// cause. Read and Write hand this straight back to the driver and Close ranks the same
// value first, so a write that fails after the read goroutine has already latched -- the
// ordinary way a broken recording unwinds -- reports why the recording ended rather than
// the symptom that followed it.
func (c *ConnectionRecorder) fail(err error) error {
	c.fatal.CompareAndSwap(nil, &err)
	return c.failed()
}

// isClosed reports whether Close has begun.
func (c *ConnectionRecorder) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// failed returns the latched failure, if any.
func (c *ConnectionRecorder) failed() error {
	if p := c.fatal.Load(); p != nil {
		return *p
	}
	return nil
}

// recordRead appends the frames in b[:n] to the Reads recording.
//
// It is a no-op once Close has run: the file is gone by then, and the truncation
// verdict has already been taken.
func (c *ConnectionRecorder) recordRead(b []byte, n int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.read_record.Write(b, n, c.fd_reads)
}

// recordWrite appends the frames in b to the Writes recording. See recordRead.
func (c *ConnectionRecorder) recordWrite(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.write_record.Write(b, len(b), c.fd_writes)
}

func (c *ConnectionRecorder) Read(b []byte) (n int, err error) {
	if err := c.failed(); err != nil {
		return 0, err
	}

	n, err = c.orig.Read(b)

	// Whatever arrived is recorded, whatever came back with it. A net.Conn may return
	// n > 0 alongside an error, and in this driver that is routine rather than
	// terminal: connReader.Read resumes a read that timed out while still making
	// progress, up to maxReadAttempts times, so a large body over a slow link arrives
	// in pieces on a connection that carries straight on. Returning before recording
	// those bytes left them out of the recording *and* the read decoder mid-frame at a
	// stale offset, so every frame after them was assembled from the wrong bytes --
	// silently, because nothing was latched and the driver was never told.
	//
	// This subsumes the io.EOF case that used to be spelled out here: a conn between
	// this one and the socket is free to wrap its io.EOF, and an identity test against
	// it took the early return, dropping the frames the last read carried.
	if n > 0 {
		if recErr := c.recordRead(b, n); recErr != nil {
			// None of it is handed on, the way an unrecordable write sends nothing. The
			// failure is terminal and latched, and these are bytes the driver would act
			// on that no replay of the recording can reproduce.
			return 0, c.fail(recErr)
		}
	}
	// err reaches the caller untouched, io.EOF included: swallowed, a server-closed
	// connection reads as (0, nil) forever, which the driver's io.ReadFull treats as
	// no progress and spins on at full speed, never noticing the connection died. It
	// is deliberately not latched either -- the read it ended may yet be resumed.
	return n, err
}

// Write records the frames in b, then forwards them to the wrapped connection.
//
// Recording first is load-bearing. The write decoder has to consume the STARTUP
// before the server can answer READY, because that answer -- observed by the read
// goroutine -- flips the shared framing latch, and a write decoder still short of
// the STARTUP would then misparse it as a transport segment (see Framing). It also
// means a refused STARTUP (ObserveRequest) is refused before anything reaches the
// server, and that a failure to record sends nothing and returns (0, err): the
// driver's write coalescer attributes success by bytes written, so bytes-then-error
// would be reported upstream as a clean write and the recording would be silently
// short.
//
// The cost is that a request recorded here can still fail to send, and that record
// cannot be recalled. It does not simply go unpaired: the loader keys records by
// stream id and keeps the last one for each, and the driver reuses stream ids, so an
// unsent request displaces whatever was recorded on that stream before it and is
// paired at load time with that earlier exchange's response. So a send failure is
// latched like a recording failure -- the recording ends where it stopped being true,
// rather than growing more records around a hole. Once Close has begun it is not: see
// below.
func (c *ConnectionRecorder) Write(b []byte) (n int, err error) {
	if err := c.failed(); err != nil {
		return 0, err
	}

	if err := c.recordWrite(b); err != nil {
		return 0, c.fail(err)
	}

	n, err = c.orig.Write(b)
	// Not once Close has begun. gocql closes a connection from whichever goroutine
	// noticed it was done, while the write coalescer may still be inside WriteTo, so a
	// perfectly ordinary close fails an in-flight write with net.ErrClosed. Latching
	// that would outrank the recording's real verdict in Close -- and on a graceful
	// close Conn.closeWithError hands Close's error to errorHandler.HandleError, so a
	// complete recording would report its own teardown as the reason it ended.
	if err != nil && !c.isClosed() {
		c.fail(err)
	}
	return n, err
}

// Close closes the wrapped connection and both recording files, and reports what went
// wrong with the recording.
//
// It is idempotent: the teardown runs once and every later call reports the same verdict.
// A net.Conn is closed more than once routinely -- an explicit Close alongside a deferred
// one is the ordinary shape -- and re-running the body below turned a healthy recording's
// nil into os.ErrClosed for each file plus net.ErrClosed for the socket, which on the
// driver route is the string Conn.closeWithError hands to errorHandler.HandleError. So a
// complete recording reported its own second close as the reason it was broken.
//
// sync.Once rather than the closed flag, which is already set before the files are
// touched: a second caller arriving mid-teardown waits for the verdict instead of reading
// one that is not built yet.
func (c *ConnectionRecorder) Close() error {
	c.closeOnce.Do(func() { c.verdict = c.close() })
	return c.verdict
}

// close is Close's one-shot teardown.
//
// The socket is closed before the files, and closed is set before either. gocql closes a
// connection from whichever goroutine noticed it was done while serve() may still be
// inside Read, so closing the files first pulled them out from under a recording write
// that was already in flight: its frames were lost and it latched a "file already closed"
// naming nothing. Closing the socket first unblocks that read so it can finish. Marking
// closed ahead of both is what stops the write that same close interrupts from latching
// its net.ErrClosed as the recording's verdict; see Write.
//
// Every close is then attempted whatever the ones before it did, and every failure is
// reported. Returning at the first would leave the other recording file and the socket
// open, and under the driver's redial loop that is one descriptor and one connection
// stranded per attempt -- the same leak NewConnectionRecorder had on its setup path.
// Keeping only the first was that same mistake one step further in: a Writes file that
// would not close discarded the Reads file's failure and the socket's alike, and the
// socket's is the one closeWithError hands to errorHandler.HandleError.
//
// A read that had already returned from the socket but not yet reached recordRead is
// dropped rather than raced: it finds closed set and gives up. Those are bytes from a
// connection being torn down, and the alternative is a verdict below that depends on
// which goroutine got there first.
func (c *ConnectionRecorder) close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	connErr := c.orig.Close()

	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	if err := c.fd_writes.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close the Writes file: %w", err))
	}
	if err := c.fd_reads.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close the Reads file: %w", err))
	}
	if connErr != nil {
		errs = append(errs, connErr)
	}
	closeErr := errors.Join(errs...)

	// Ordered by what it tells the caller. A latched failure is why the recording
	// stopped being true, so it outranks both of the others: reporting a truncated
	// stream instead named the symptom of the very failure that caused it, and left
	// the cause -- the one thing that says which write or which read went wrong --
	// unreported on every path out of here.
	if err := c.failed(); err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// A stream that ends inside a frame or an unfinished segment chain produced a
	// recording that looks complete but is not, and at load time the loss surfaces
	// only as an unpaired stream or an unmatched hash, with nothing pointing at this
	// session.
	if c.read_record.decoder.Pending() || c.write_record.decoder.Pending() {
		return fmt.Errorf("gocql/dialer: the connection closed in the middle of a frame or segment chain; the recording is truncated")
	}
	return nil
}

func (c *ConnectionRecorder) LocalAddr() net.Addr {
	return c.orig.LocalAddr()
}

func (c *ConnectionRecorder) RemoteAddr() net.Addr {
	return c.orig.RemoteAddr()
}

func (c *ConnectionRecorder) SetDeadline(t time.Time) error {
	return c.orig.SetDeadline(t)
}

func (c *ConnectionRecorder) SetReadDeadline(t time.Time) error {
	return c.orig.SetReadDeadline(t)
}

func (c *ConnectionRecorder) SetWriteDeadline(t time.Time) error {
	return c.orig.SetWriteDeadline(t)
}
