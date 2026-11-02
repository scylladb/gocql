// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type aheadConn struct {
	net.Conn
	data      []byte
	chunk     int
	err       error
	armErr    error
	reads     int
	largest   int
	deadlines int
}

func (c *aheadConn) Read(p []byte) (int, error) {
	c.reads++
	c.largest = max(c.largest, len(p))
	if c.chunk > 0 {
		p = p[:min(len(p), c.chunk)]
	}
	n := copy(p, c.data)
	c.data = c.data[n:]
	if len(c.data) == 0 {
		return n, c.err
	}
	return n, nil
}

func (c *aheadConn) SetReadDeadline(time.Time) error { c.deadlines++; return c.armErr }

func TestNativeReadAheadBoundaryAndReuse(t *testing.T) {
	for _, limit := range []int{1, 7, 128, 4096, 32 << 10} {
		data := bytes.Repeat([]byte("b"), 32<<10)
		conn := &aheadConn{data: append(bytes.Clone(data), []byte("next-header")...)}
		raw := &InternalConnReader{InternalConn: conn}
		var ahead nativeStreamReadAhead
		ahead.begin(raw, int64(len(data)), limit)
		var got []byte
		var scalar [4]byte
		for len(got) < len(data) {
			n, err := ahead.Read(scalar[:])
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			got = append(got, scalar[:n]...)
		}
		if !bytes.Equal(got, data) || string(conn.data) != "next-header" {
			t.Fatal("body read crossed frame boundary")
		}
		if cap(ahead.buf) > min(limit, 4<<10) || conn.largest > min(limit, 4<<10) && limit >= 4 {
			t.Fatalf("limit %d: retained %d largest %d", limit, cap(ahead.buf), conn.largest)
		}
		if limit >= 4096 && conn.reads > 16 {
			t.Fatalf("scalar reads not amortized: %d", conn.reads)
		}
		if conn.deadlines != conn.reads {
			t.Fatal("missing read deadline")
		}
		backing := ahead.buf
		ahead.begin(raw, int64(len(conn.data)), limit)
		if _, err := io.ReadAll(&ahead); err != nil {
			t.Fatal(err)
		}
		if len(backing) > 0 && &backing[0] != &ahead.buf[0] {
			t.Fatal("scratch not reused within reservation")
		}
	}
}

func TestNativeReadAheadReturnsAvailableProgress(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	raw := &InternalConnReader{InternalConn: client}
	raw.SetTimeout(time.Second)
	var ahead nativeStreamReadAhead
	ahead.begin(raw, 4096, 4096)
	done := make(chan error, 1)
	go func() {
		_, err := server.Write([]byte{7})
		done <- err
	}()
	var out [1]byte
	if n, err := ahead.Read(out[:]); n != 1 || err != nil || out[0] != 7 {
		t.Fatalf("single available byte stalled or failed: %d %v", n, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNativeReadAheadReleasedBeforeSharedReuse(t *testing.T) {
	conn := &aheadConn{data: []byte("body")}
	raw := &InternalConnReader{InternalConn: conn}
	c := &Conn{InternalCtx: context.Background(), InternalR: raw, InternalStreams: streams.New()}
	stream, _ := c.InternalStreams.GetStream()
	c.InternalExclusiveFlag.Store(true)
	r := &InternalExclusiveConn{InternalConn: c, InternalComplete: true, InternalStream: stream, InternalDone: make(chan struct{})}
	r.readAhead.begin(raw, 4, 4096)
	if got, err := io.ReadAll(&r.readAhead); err != nil || string(got) != "body" || cap(r.readAhead.buf) > 4 {
		t.Fatal("body-sized cap", string(got), err, cap(r.readAhead.buf))
	}
	if err := r.InternalClose(true); err != nil {
		t.Fatal(err)
	}
	if r.readAhead.buf != nil || r.readAhead.source != nil || c.InternalExclusiveFlag.Load() || c.InternalStreams.InUse() != 0 {
		t.Fatal("reservation retained scratch or exclusive ownership after close")
	}
}

func TestNativeReadAheadPartialErrorsAndArmingFailure(t *testing.T) {
	boom := errors.New("read failed")
	for _, terminal := range []error{io.EOF, boom} {
		conn := &aheadConn{data: []byte("abc"), err: terminal}
		var ahead nativeStreamReadAhead
		ahead.begin(&InternalConnReader{InternalConn: conn}, 10, 4096)
		for index, char := range []byte("abc") {
			var p [1]byte
			n, err := ahead.Read(p[:])
			if n != 1 || p[0] != char || index < 2 && err != nil || index == 2 && err != terminal {
				t.Fatal(index, n, err)
			}
		}
		if _, err := ahead.Read(make([]byte, 1)); err != terminal || conn.reads != 1 {
			t.Fatal("lost sticky error", err, conn.reads)
		}
	}
	conn := &aheadConn{data: []byte("abc"), armErr: boom}
	var ahead nativeStreamReadAhead
	ahead.begin(&InternalConnReader{InternalConn: conn}, 3, 3)
	if n, err := ahead.Read(make([]byte, 1)); n != 0 || !errors.Is(err, InternalErrArmReadDeadline) || !errors.Is(err, boom) || conn.reads != 0 {
		t.Fatal(n, err, conn.reads)
	}
}

func TestNativeReadAheadFrameValidationPrecedesBodyRead(t *testing.T) {
	for _, version := range []byte{3, 4} {
		for _, invalid := range []string{"version", "length", "flag", "opcode"} {
			header := []byte{version | 0x80, 0, 0, 1, byte(frm.OpResult), 0, 0, 0, 8}
			switch invalid {
			case "version":
				header[0] = version
			case "length":
				header[5] = 0x7f
			case "flag":
				header[1] = 0x40
			case "opcode":
				header[4] = 0xff
			}
			conn := &aheadConn{data: append(header, []byte("bodydata")...)}
			reservation := &InternalExclusiveConn{InternalConn: &Conn{InternalVersion: version, InternalR: &InternalConnReader{InternalConn: conn}}, InternalLimits: StreamLimits{ReadAheadBytes: 4096}}
			if _, _, err := reservation.readStreamEnvelope(context.Background(), wire.StreamLimits{MaxBytes: 16}); err == nil || string(conn.data) != "bodydata" || len(reservation.readAhead.buf) != 0 {
				t.Fatal(version, invalid, err)
			}
		}
	}
}

func TestExclusiveIOEntryPreservesWinningCancellation(t *testing.T) {
	for _, gate := range []string{"closing", "closed", "connection"} {
		for _, cause := range []error{nil, context.Canceled, context.DeadlineExceeded} {
			ctx, cancel := context.WithCancel(context.Background())
			if gate == "connection" {
				cancel()
			}
			r := &InternalExclusiveConn{InternalConn: &Conn{InternalCtx: ctx}, terminalCause: cause}
			switch gate {
			case "closing":
				r.InternalClosing.Store(true)
			case "closed":
				r.closed = true
			}
			called := false
			err := r.InternalWithIO(context.Background(), func() error { called = true; return nil })
			cancel()
			want := cause
			if want == nil {
				want = ErrClosed
			}
			if called || err != want {
				t.Fatalf("%s cause=%v: called=%v err=%v, want %v", gate, cause, called, err, want)
			}
		}
	}
	// An explicit manual close keeps its terminal error even if the supplied
	// page context is also canceled; only a recorded winning cause takes priority.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &InternalExclusiveConn{}
	r.InternalClosing.Store(true)
	if err := r.InternalWithIO(ctx, func() error { return nil }); err != ErrClosed {
		t.Fatal("manual close precedence changed", err)
	}
}

func BenchmarkNativeReadAheadScalars(b *testing.B) {
	data := make([]byte, 128<<10)
	conn := &aheadConn{}
	var ahead nativeStreamReadAhead
	raw := &InternalConnReader{InternalConn: conn}
	var out [4]byte
	b.ReportAllocs()
	for range b.N {
		conn.data = data
		ahead.begin(raw, int64(len(data)), 4096)
		for range len(data) / 4 {
			if _, err := ahead.Read(out[:]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(conn.reads)/float64(b.N), "reads/op")
}
