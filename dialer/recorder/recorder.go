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

// headerLen is the CQL frame header the recorder slices on: version, flags, a
// 2-byte stream id, the opcode and the 4-byte body length. It is fixed, as it has
// always been here. Negotiation lands on v4 (discoverProtocol) and v5+ is refused
// outright (dialer.ErrProtoV5NotSupported), so the 1-byte stream id of v1/v2 only
// appears if a caller pins ProtoVersion to one of them — which these dialers have
// never handled, here or in the offset math they share with dialer.GetFrameHash.
const headerLen = 9

type FrameWriter struct {
	record dialer.Record
	// bodyLeft counts the body bytes still owed to the frame in record, and is
	// meaningful only once that frame's header is complete — until then the
	// declared length has not been read.
	bodyLeft int
	// useMetadataID latches once a STARTUP frame on this connection opts into the
	// SCYLLA_USE_METADATA_ID extension, so every subsequent recorded frame is
	// stamped with the negotiated state (the driver only sends the opt-in when
	// the server advertised it, so its presence means the extension is active).
	useMetadataID bool
}

// Write records the frames in b[:n]. Neither side of a connection delivers one
// frame per call: the driver's read path fills a bufio.Reader, so a read can carry
// several frames or end in the middle of one, and either shape has to come back out
// of the recording as the frames that went in.
func (f *FrameWriter) Write(b []byte, n int, file *os.File) error {
	for rest := b[:n]; len(rest) > 0; {
		taken, err := f.consume(rest, file)
		if err != nil {
			return err
		}
		rest = rest[taken:]
	}
	return nil
}

// consume appends the prefix of b belonging to the frame in progress and, once that
// frame is whole, records it and resets for the next one. It returns how many bytes
// it took, which is all of b unless b runs on into the following frame.
func (f *FrameWriter) consume(b []byte, file *os.File) (int, error) {
	// While the header is short the body length is still unknown, so take only what
	// completes the header: anything past it may belong to the next frame.
	headerShort := len(f.record.Data) < headerLen
	want := f.bodyLeft
	if headerShort {
		want = headerLen - len(f.record.Data)
	}

	taken := min(want, len(b))
	f.record.Data = append(f.record.Data, b[:taken]...)

	if headerShort {
		// A frame's first byte is its protocol version, and the driver's handshake
		// frames are never segment-framed — so on a v5+ connection this fires during
		// the handshake, before any transport segment reaches the fixed-offset frame
		// slicing here and is recorded as garbage.
		if dialer.FrameIsProtoV5OrNewer(f.record.Data) {
			return taken, dialer.ErrProtoV5NotSupported
		}
		if len(f.record.Data) < headerLen {
			return taken, nil
		}
		f.bodyLeft = int(f.record.Data[5])<<24 | int(f.record.Data[6])<<16 | int(f.record.Data[7])<<8 | int(f.record.Data[8])
		f.record.StreamID = int(f.record.Data[2])<<8 | int(f.record.Data[3])
	} else {
		f.bodyLeft -= taken
	}

	if f.bodyLeft > 0 {
		return taken, nil
	}
	return taken, f.flush(file)
}

// flush writes the completed frame in record and starts a fresh one.
func (f *FrameWriter) flush(file *os.File) error {
	// The latch reads a whole STARTUP, which is the reason the frame is assembled
	// before it is recorded rather than each call being written as it arrives.
	// Missing the opt-in here would stamp every later EXECUTE false and turn replay
	// into silent hash mismatches rather than an error.
	if !f.useMetadataID && dialer.StartupNegotiatesMetadataID(f.record.Data) {
		f.useMetadataID = true
	}
	f.record.UseMetadataID = f.useMetadataID

	jsonData, err := json.Marshal(f.record)
	if err != nil {
		return fmt.Errorf("failed to encode JSON record: %w", err)
	}
	if _, err := file.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to record: %w", err)
	}

	f.record = dialer.Record{}
	f.bodyLeft = 0
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
