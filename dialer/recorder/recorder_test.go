package recorder

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gocql/gocql/dialer"
)

// stubConn is a net.Conn whose Read serves a fixed byte stream and whose Write
// accepts everything, so the recorder's own behaviour is all a test observes.
type stubConn struct {
	readData []byte
}

func (c *stubConn) Read(b []byte) (int, error) {
	if len(c.readData) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.readData)
	c.readData = c.readData[n:]
	return n, nil
}

func (c *stubConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *stubConn) Close() error                     { return nil }
func (c *stubConn) LocalAddr() net.Addr              { return nil }
func (c *stubConn) RemoteAddr() net.Addr             { return nil }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

// optionsFrame builds a body-less OPTIONS frame with the given version byte.
func optionsFrame(version byte) []byte {
	return []byte{version, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}
}

// TestConnectionRecorderRejectsProtoV5 pins the rejection path dkropachev asked
// for: on v5+ the byte stream carries transport segments after the handshake,
// which the recorder's fixed-offset frame slicing would record as garbage. The
// version byte of the handshake frames is genuine (they are never segmented),
// so both directions fail there, before any segment flows.
func TestConnectionRecorderRejectsProtoV5(t *testing.T) {
	t.Run("write side", func(t *testing.T) {
		rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{})
		if err != nil {
			t.Fatalf("NewConnectionRecorder: %v", err)
		}
		defer rec.Close()

		if _, err := rec.Write(optionsFrame(0x05)); !errors.Is(err, dialer.ErrProtoV5NotSupported) {
			t.Fatalf("Write(v5 frame) error = %v, want ErrProtoV5NotSupported", err)
		}
	})

	t.Run("read side", func(t *testing.T) {
		rec, err := NewConnectionRecorder(filepath.Join(t.TempDir(), "conn"), &stubConn{readData: optionsFrame(0x85)})
		if err != nil {
			t.Fatalf("NewConnectionRecorder: %v", err)
		}
		defer rec.Close()

		buf := make([]byte, 64)
		if _, err := rec.Read(buf); !errors.Is(err, dialer.ErrProtoV5NotSupported) {
			t.Fatalf("Read(v5 response) error = %v, want ErrProtoV5NotSupported", err)
		}
	})
}

// TestConnectionRecorderAcceptsProtoV4 pins that the rejection is scoped to
// v5+: a v4 frame (with and without the direction bit) is recorded normally.
func TestConnectionRecorderAcceptsProtoV4(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "conn")
	rec, err := NewConnectionRecorder(fname, &stubConn{readData: optionsFrame(0x84)})
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
