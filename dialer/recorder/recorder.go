package recorder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path"
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
	fmt.Println("Dial Context Record Dialer")
	sourcePort := gocql.ScyllaGetSourcePort(ctx)
	fmt.Println("Source port: ", sourcePort)
	dialerWithLocalAddr := d.Dialer
	dialerWithLocalAddr.LocalAddr, err = net.ResolveTCPAddr(network, fmt.Sprintf(":%d", sourcePort))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	conn, err = dialerWithLocalAddr.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	return NewConnectionRecorder(path.Join(d.dir, fmt.Sprintf("%s-%d", addr, sourcePort)), conn, d.comp)
}

// NewConnectionRecorder wraps conn, recording every frame in both directions.
//
// comp may be nil; see WithSegmentCompressor for when it is needed.
func NewConnectionRecorder(fname string, conn net.Conn, comp dialer.SegmentCompressor) (net.Conn, error) {
	fd_writes, err := os.OpenFile(fname+"Writes", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	fd_reads, err2 := os.OpenFile(fname+"Reads", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err2 != nil {
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
	fd_writes    *os.File
	fd_reads     *os.File
	orig         net.Conn
	read_record  FrameWriter
	write_record FrameWriter
}

func (c *ConnectionRecorder) Read(b []byte) (n int, err error) {
	n, err = c.orig.Read(b)
	if err != nil && err != io.EOF {
		return n, err
	}

	return n, c.read_record.Write(b, n, c.fd_reads)
}

func (c *ConnectionRecorder) Write(b []byte) (n int, err error) {
	n, err = c.orig.Write(b)
	if err != nil {
		return n, err
	}

	return n, c.write_record.Write(b, n, c.fd_writes)
}

func (c ConnectionRecorder) Close() error {
	if err := c.fd_writes.Close(); err != nil {
		return fmt.Errorf("failed to close the file: %w", err)
	}
	if err := c.fd_reads.Close(); err != nil {
		return fmt.Errorf("failed to close the file: %w", err)
	}
	return c.orig.Close()
}

func (c ConnectionRecorder) LocalAddr() net.Addr {
	return c.orig.LocalAddr()
}

func (c ConnectionRecorder) RemoteAddr() net.Addr {
	return c.orig.RemoteAddr()
}

func (c ConnectionRecorder) SetDeadline(t time.Time) error {
	return c.orig.SetDeadline(t)
}

func (c ConnectionRecorder) SetReadDeadline(t time.Time) error {
	return c.orig.SetReadDeadline(t)
}

func (c ConnectionRecorder) SetWriteDeadline(t time.Time) error {
	return c.orig.SetWriteDeadline(t)
}
