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

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/dialer"
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
	return &ConnectionRecorder{fd_writes: fd_writes, fd_reads: fd_reads, orig: conn, write_record: FrameWriter{new: true}, read_record: FrameWriter{new: true}}, nil
}

type FrameWriter struct {
	new       bool
	to_record int
	record    dialer.Record
}

func (f *FrameWriter) Write(b []byte, n int, file *os.File) (err error) {
	if f.new {
		f.to_record = -1
		f.record = dialer.Record{}
	}

	recorded_ealier := len(f.record.Data)
	f.record.Data = append(f.record.Data, b[:n]...)

	if f.to_record == -1 && len(f.record.Data) >= 9 {
		f.to_record = 9 + int(f.record.Data[5+0])<<24 | int(f.record.Data[6])<<16 | int(f.record.Data[7])<<8 | int(f.record.Data[8]) - recorded_ealier
		f.record.StreamID = int(f.record.Data[2])<<8 | int(f.record.Data[3])
	} else if f.to_record == -1 {
		return err
	}

	f.to_record = f.to_record - n
	if f.to_record <= 0 {
		f.new = true
		// Write JSON record to file
		jsonData, marshalErr := json.Marshal(f.record)
		if marshalErr != nil {
			return fmt.Errorf("failed to encode JSON record: %w", marshalErr)
		}
		_, writeErr := file.Write(append(jsonData, '\n'))
		if writeErr != nil {
			return fmt.Errorf("failed to record: %w", writeErr)
		}
	}
	return err
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
