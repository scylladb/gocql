// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/testkit"
)

// This source retains only ROWS metadata and counters. It generates every cell
// into the receiver's requested range; neither a response fixture nor output
// proportional to a whole page exists on the producing side.
type step14RowsSource struct {
	prefix                  []byte
	rows, size, row, offset int
	length                  [4]byte
}

func step14Rows(version byte, rows, size int) *step14RowsSource {
	p := testkit.BlobRows(version, 0, []byte{})
	p = p[:len(p)-4]
	binary.BigEndian.PutUint32(p[5:9], uint32(len(p)-9+rows*(4+size)))
	binary.BigEndian.PutUint32(p[len(p)-4:], uint32(rows))
	s := &step14RowsSource{prefix: p, rows: rows, size: size}
	binary.BigEndian.PutUint32(s.length[:], uint32(size))
	return s
}
func (s *step14RowsSource) Read(p []byte) (int, error) {
	p = p[:min(len(p), 16381)] // intentionally cross scalar/unit boundaries
	if len(s.prefix) > 0 {
		n := copy(p, s.prefix)
		s.prefix = s.prefix[n:]
		return n, nil
	}
	if s.row == s.rows {
		return 0, io.EOF
	}
	if s.offset < 4 {
		n := copy(p, s.length[s.offset:])
		s.offset += n
		return n, nil
	}
	n := min(len(p), s.size-(s.offset-4))
	for i := 0; i < n; i++ {
		p[i] = 0x71
	}
	s.offset += n
	if s.offset == s.size+4 {
		s.row++
		s.offset = 0
	}
	return n, nil
}

type step14GeneratedConn struct {
	*step04MemoryConn
	input io.Reader
}

func (c *step14GeneratedConn) Read(p []byte) (int, error) { return c.input.Read(p) }

// Every current streaming mode receives the same generated results. This
// measures byte-storage capacity while decoding, including inside skip reads,
// and samples it again while a slow caller is idle. It is not an allocation or
// throughput benchmark: fixed structs/plans and TLS's own record buffers are
// separate from the bounded driver byte-storage claim.
func TestStep14StreamingStorageMatrix(t *testing.T) {
	for _, mode := range step08Modes {
		t.Run(mode.name, func(t *testing.T) {
			for _, work := range []struct {
				name       string
				rows, size int
				skip, slow bool
			}{
				{"cell256K", 1, 256 << 10, false, false}, {"cell8M", 1, 8 << 20, false, false},
				{"page8M", 128, 64 << 10, false, false}, {"skip8M", 1, 8 << 20, true, false},
				{"slowpage8M", 128, 64 << 10, false, true},
			} {
				t.Run(work.name, func(t *testing.T) {
					source := step14Rows(mode.version, work.rows, work.size)
					// v5's independent producer retains just one bounded source unit.
					var input io.Reader = source
					if mode.version == 5 {
						input = &step14UnitSource{input: source, compressed: mode.compression != nil}
					}
					memory := &step14GeneratedConn{step04MemoryConn: &step04MemoryConn{}, input: input}
					var socket net.Conn = memory
					var peer net.Conn
					done := make(chan error, 1)
					if mode.tls {
						a, b := net.Pipe()
						cert, conf := step04TLS(t)
						client := tls.Client(a, conf)
						server := tls.Server(b, &tls.Config{Certificates: []tls.Certificate{cert}})
						go func() { done <- server.Handshake() }()
						if err := client.Handshake(); err != nil {
							t.Fatal(err)
						}
						if err := <-done; err != nil {
							t.Fatal(err)
						}
						socket = &step08TLSConn{Conn: client, raw: a}
						peer = &step08TLSConn{Conn: server, raw: b}
					}
					r := step04MemoryReservation(mode.version, mode.compression, socket)
					defer r.InternalClose(false)
					if peer != nil {
						defer peer.Close()
						go func() {
							typedTestReadRequest(t, r.InternalConn, peer)
							_, err := io.Copy(peer, input)
							done <- err
						}()
					}
					response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT generated", Consistency: One})
					if err != nil {
						t.Fatal(err)
					}
					defer response.InternalReleaseMetadata()
					if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
						t.Fatal(err)
					}
					var peak int64
					sample := func() {
						n := response.InternalWorkingStorageBytes()
						peak = max(peak, n)
						if n > 320<<10 {
							t.Fatalf("unbounded driver byte storage: %d", n)
						}
					}
					// Sampling the actual source Read catches simultaneous scratch during skip,
					// not merely the smaller storage retained after decode returns.
					r.InternalConn.InternalR.(*connReader).InternalConn = &step14ObservedConn{Conn: socket, observe: sample}
					sample()
					if work.skip {
						plan := codecTestMust(StreamRow(func(*Unit) {}).Compile(response.InternalColumns))
						var dst Unit
						for i := 0; i < work.rows; i++ {
							if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil {
								t.Fatal(ok, err)
							}
							sample()
						}
						if ok, err := decodeStreamRow(response, plan, &dst); ok || err != nil {
							t.Fatal(ok, err)
						}
					} else {
						plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
						dst := make([]byte, 0, work.size)
						backing := unsafe.SliceData(dst[:cap(dst)])
						for i := 0; i < work.rows; i++ {
							if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil {
								t.Fatal(ok, err)
							}
							if len(dst) != work.size || unsafe.SliceData(dst) != backing || bytes.Count(dst, []byte{0x71}) != len(dst) {
								t.Fatal("incorrect final output/backing")
							}
							sample()
							if work.slow {
								time.Sleep(time.Millisecond)
								sample()
							}
						}
						if ok, err := decodeStreamRow(response, plan, &dst); ok || err != nil {
							t.Fatal(ok, err)
						}
					}
					if peer != nil {
						if err := <-done; err != nil {
							t.Fatal(err)
						}
					}
					copies := int64(0)
					if r.InternalUnits != nil {
						copies = r.InternalUnits.InternalCopies
					}
					t.Logf("mode=%s rows=%d cell=%d skipped=%v slow=%v peak_driver_byte_storage=%d validated_unit_to_parser_bytes=%d source_unit_payload<=130000 source_unit_encoded<=131071 final_output=%d", mode.name, work.rows, work.size, work.skip, work.slow, peak, copies, func() int {
						if work.skip {
							return 0
						}
						return work.size
					}())
				})
			}
		})
	}
}

type step14ObservedConn struct {
	net.Conn
	observe func()
}

func (s *step14ObservedConn) Read(p []byte) (int, error) { s.observe(); return s.Conn.Read(p) }

type step14UnitSource struct {
	input      io.Reader
	compressed bool
	pending    []byte
}

func (s *step14UnitSource) Read(p []byte) (int, error) {
	if len(s.pending) == 0 {
		data := make([]byte, 130000)
		n, err := io.ReadFull(s.input, data)
		if n == 0 {
			return 0, err
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		s.pending = testkit.Unit(data[:n], s.compressed, false)
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// Each fuzz input chooses a real final-write interruption schedule. Close must
// join that write, preserve exact output, retire admission and reject all later
// destination access. Bounded seed/capacity/row sizes prevent allocation abuse.
func FuzzStep14StreamingCancellationLifetime(f *testing.F) {
	for i := byte(0); i < 8; i++ {
		f.Add(i, byte(0), []byte("final"))
	}
	f.Fuzz(func(t *testing.T, which, action byte, payload []byte) {
		if len(payload) > 256 {
			return
		}
		if len(payload) == 0 {
			payload = []byte("x")
		}
		mode := step08Modes[int(which)%len(step08Modes)]
		s, c, peer := step08StreamSession(t, mode)
		done := make(chan struct{})
		go func() {
			defer close(done)
			stream := typedTestReadRequest(t, c, peer)
			_, _ = peer.Write(step08Packet(mode, testkit.BlobRows(mode.version, uint16(stream), payload)))
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bound := runtimeQuery(t)
		defer bound.Close()
		decoder := &step08FinalAccess{entered: make(chan struct{}), leave: make(chan struct{})}
		var release sync.Once
		unblock := func() { release.Do(func() { close(decoder.leave) }) }
		defer unblock()
		cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Incremental: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { unblock(); cursor.Close() }()
		dst := make([]byte, 0, len(payload))
		next := make(chan struct{})
		go func() { cursor.NextInto(&dst); close(next) }()
		step14Await(t, decoder.entered)
		closed := make(chan struct{})
		go func() {
			if action%2 == 0 {
				cancel()
			}
			cursor.Close()
			close(closed)
		}()
		select {
		case <-closed:
			t.Fatal("Close returned during final destination write")
		default:
		}
		unblock()
		step14Await(t, next)
		step14Await(t, closed)
		step14Await(t, done)
		expected := append([]byte(nil), payload...)
		expected[0] = 'F'
		if decoder.writes.Load() != 1 || !bytes.Equal(dst, expected) {
			t.Fatal("final write not joined")
		}
		if cursor.NextInto(&dst) || !bytes.Equal(dst, expected) {
			t.Fatal("late destination access")
		}
		bound.Close()
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := bound.Wait(wait); err != nil {
			t.Fatal(fmt.Errorf("admission not retired: %w", err))
		}
	})
}

func step14Await(t testing.TB, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded lifetime operation did not join")
	}
}

type step14DecoderError struct{ cause error }

func (d step14DecoderError) Compile(Columns) (StreamRowPlan[[]byte], error) { return d, nil }
func (d step14DecoderError) Reset(dst *[]byte)                              { *dst = (*dst)[:0] }
func (d step14DecoderError) DecodeInto(r *StreamRowReader, dst *[]byte) error {
	p, err := StreamBlob.Compile(codecTestMust(ScalarType(TypeBlob)))
	if err != nil {
		return err
	}
	if err = ReadStreamColumn(r, p, dst); err != nil {
		return err
	}
	return d.cause
}
func TestStep14CustomDecoderErrorAfterWrite(t *testing.T) {
	for _, mode := range step08Modes {
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := step08StreamSession(t, mode)
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(step08Packet(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("partial"))))
			}()
			cause := fmt.Errorf("custom decoder terminal failure")
			bound := runtimeQuery(t)
			defer bound.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cursor, err := RunStream(ctx, s, bound, step14DecoderError{cause}, StreamOptions{Incremental: true, Options: Options{Idempotent: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			dst := make([]byte, 0, 16)
			if cursor.NextInto(&dst) || !errors.Is(cursor.Err(), cause) {
				t.Fatal("custom decoder error published row", cursor.Err())
			}
			if string(dst) != "partial" || !cursor.Info().OutputStarted || cursor.Info().DeliveredRows != 0 || cursor.Info().Result.Attempts != 1 {
				t.Fatal("custom decoder replay/progress", dst, cursor.Info())
			}
			if cursor.NextInto(&dst) || string(dst) != "partial" || !errors.Is(cursor.Close(), cause) {
				t.Fatal("terminal custom failure changed destination/error")
			}
			bound.Close()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			step14Await(t, done)
		})
	}
}
