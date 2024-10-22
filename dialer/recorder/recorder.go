package recorder

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"time"

	"github.com/gocql/gocql"
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
	// if sourcePort == 0 {
	// 	return d.Dialer.DialContext(ctx, network, addr)
	// }
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
	fmt.Println("New recorder: ", fname)
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

type ConnectionRecorder struct {
	fd_writes *os.File
	fd_reads  *os.File
	orig      net.Conn
}

func (c ConnectionRecorder) Read(b []byte) (n int, err error) {
	n, err = c.orig.Read(b)
	if err != nil && err != io.EOF {
		return n, err
	}

	_, writeErr := c.fd_reads.Write(b[:n])
	if writeErr != nil {
		return n, fmt.Errorf("failed to record read: %w", writeErr)
	}
	_, writeErr = c.fd_reads.Write([]byte("\n"))
	if writeErr != nil {
		return n, fmt.Errorf("failed to record read: %w", writeErr)
	}

	return n, err
}

func (c ConnectionRecorder) Write(b []byte) (n int, err error) {
	_, writeErr := c.fd_writes.Write(b)
	if writeErr != nil {
		return n, fmt.Errorf("failed to record write: %w", writeErr)
	}
	_, writeErr = c.fd_writes.Write([]byte("\n"))
	if writeErr != nil {
		return n, fmt.Errorf("failed to record write: %w", writeErr)
	}
	return c.orig.Write(b)
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
