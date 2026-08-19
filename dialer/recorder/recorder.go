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

func NewRecordDialer(dir string) *RecordDialer {
	return &RecordDialer{
		dir: dir,
	}
}

type RecordDialer struct {
	dir string
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

	return NewConnectionRecorder(path.Join(d.dir, fmt.Sprintf("%s-%d", addr, sourcePort)), conn)
}

func NewConnectionRecorder(fname string, conn net.Conn) (net.Conn, error) {
	fd_writes, err := os.OpenFile(fname+"Writes", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	fd_reads, err2 := os.OpenFile(fname+"Reads", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err2 != nil {
		return nil, err2
	}
	return &ConnectionRecorder{fd_writes: fd_writes, fd_reads: fd_reads, orig: conn}, nil
}

// FrameWriter records the CQL frames it is fed, one JSON object per line.
//
// The frame boundaries come from dialer.FrameSplitter, which both this and the
// replayer use: a read can carry several frames or end in the middle of one, and
// either shape has to come back out of the recording as the frames that went in.
type FrameWriter struct {
	splitter dialer.FrameSplitter
	// useMetadataID latches once a STARTUP frame on this connection opts into the
	// SCYLLA_USE_METADATA_ID extension, so every subsequent recorded frame is
	// stamped with the negotiated state (the driver only sends the opt-in when
	// the server advertised it, so its presence means the extension is active).
	useMetadataID bool
}

// Write records the frames in b[:n].
func (f *FrameWriter) Write(b []byte, n int, file *os.File) error {
	return f.splitter.Feed(b[:n], func(frame []byte) error {
		return f.record(frame, file)
	})
}

// record appends one complete frame to the recording.
func (f *FrameWriter) record(frame []byte, file *os.File) error {
	// A frame's first byte is its protocol version, and the driver's handshake
	// frames are never segment-framed — so on a v5+ connection this fires on the
	// first frame of the handshake, before any transport segment reaches the
	// fixed-offset frame slicing and is recorded as garbage.
	if dialer.FrameIsProtoV5OrNewer(frame) {
		return dialer.ErrProtoV5NotSupported
	}

	// The latch reads a whole STARTUP, which is the reason frames are assembled
	// before being recorded rather than each write being appended as it arrives.
	// Missing the opt-in here would stamp every later EXECUTE false and turn replay
	// into silent hash mismatches rather than an error.
	if !f.useMetadataID && dialer.StartupNegotiatesMetadataID(frame) {
		f.useMetadataID = true
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
