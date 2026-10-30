/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
	lz4 "github.com/scylladb/gocql/v2/internal/wirecompress"
)

type step04MemoryConn struct {
	source                 *bytes.Reader
	readCalls              int
	directBytes, otherBulk int64
	start, end             uintptr
	closed                 bool
}

func (c *step04MemoryConn) Read(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	c.readCalls++
	if len(p) > 64 {
		address := uintptr(unsafe.Pointer(&p[0]))
		if address >= c.start && address+uintptr(len(p)) <= c.end {
			c.directBytes += int64(len(p))
		} else {
			c.otherBulk += int64(len(p))
		}
	}
	return c.source.Read(p)
}
func (c *step04MemoryConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
func (c *step04MemoryConn) Close() error { c.closed = true; return nil }
func (c *step04MemoryConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 49152}
}
func (c *step04MemoryConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9042}
}
func (c *step04MemoryConn) SetDeadline(time.Time) error      { return nil }
func (c *step04MemoryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *step04MemoryConn) SetWriteDeadline(time.Time) error { return nil }
func step04MemoryReservation(version byte, compressor Compressor, source *step04MemoryConn) *exclusiveConn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &driverConn{InternalVersion: version, InternalCtx: ctx, InternalCancel: cancel, InternalR: &connReader{InternalConn: source}, InternalStreams: streams.New(), InternalLogger: nopLogger{}, InternalErrorHandler: connErrorHandlerFn(func(*driverConn, error, bool) {}), InternalCompressor: compressor}
	c.InternalW = &deadlineContextWriter{InternalW: source, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	if version == 5 && compressor != nil {
		c.InternalSegCompressor = compressor.(segment.Compressor)
	}
	c.InternalInitFramerCache()
	stream, ok := c.InternalStreams.GetStream()
	if !ok || stream != 0 {
		panic("synthetic stream allocation failed")
	}
	c.InternalExclusiveFlag.Store(true)
	r := &exclusiveConn{InternalConn: c, InternalCtx: ctx, InternalComplete: true, InternalStream: stream, InternalDone: make(chan struct{})}
	r.InternalInitStream(StreamLimits{})
	return r
}
func step04EncodedResponse(version byte, compressed bool, payload []byte) []byte {
	body := typedTestRowsBody(payload)
	frame := []byte{version | 0x80, 0, 0, 0, 8}
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
	frame = append(frame, body...)
	if version < 5 {
		return frame
	}
	var out []byte
	for at := 0; at < len(frame); {
		end := min(at+segment.MaxPayloadSize, len(frame))
		chain, _ := wire.NewChain(wire.Borrow(frame[at:end]))
		packet, err := segment.EncodeSpans(chain, len(frame) <= segment.MaxPayloadSize, compressed)
		if err != nil {
			panic(err)
		}
		start := len(out)
		out = append(out, make([]byte, packet.Len())...)
		packet.CopyTo(out[start:])
		at = end
	}
	return out
}
func TestV2StreamingTransportReadTargetsAndWorkingMemory(t *testing.T) {
	for _, mode := range []struct {
		version    byte
		compressor Compressor
	}{{4, nil}, {5, nil}, {5, lz4.LZ4Compressor{}}} {
		var storage int64
		for _, size := range []int{400 << 10, 8 << 20} {
			payload := make([]byte, size)
			rand.New(rand.NewSource(42)).Read(payload)
			encoded := step04EncodedResponse(mode.version, mode.compressor != nil, payload)
			source := &step04MemoryConn{source: bytes.NewReader(encoded)}
			r := step04MemoryReservation(mode.version, mode.compressor, source)
			dst := make([]byte, 0, size)
			source.start = uintptr(unsafe.Pointer(&dst[:cap(dst)][0]))
			source.end = source.start + uintptr(size)
			response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT value", Consistency: One})
			if err != nil {
				t.Fatal(err)
			}
			if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
				t.Fatal(err)
			}
			plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(response.InternalColumns))
			if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil || !bytes.Equal(dst, payload) {
				t.Fatal(ok, err)
			}
			if mode.version == 4 {
				if source.directBytes != int64(size) || source.otherBulk != 0 {
					t.Fatal("raw payload did not target final ranges", source.directBytes, source.otherBulk)
				}
			} else {
				now := r.InternalUnits.InternalUnits.StorageBytes()
				if now > r.InternalLimits.MaxTransformBytes || storage != 0 && now != storage {
					t.Fatal("working memory grew with response bytes", storage, now)
				}
				storage = now
			}
			if response.InternalBudget.Used >= 16<<10 {
				t.Fatal("metadata allocations grew with cell", response.InternalBudget.Used)
			}
			response.InternalReleaseMetadata()
			r.InternalClose(true)
		}
	}
}
func BenchmarkV2ExclusiveTransport(b *testing.B) {
	for _, mode := range []struct {
		name       string
		version    byte
		compressor Compressor
	}{{"raw4", 4, nil}, {"raw5", 5, nil}, {"lz4v5", 5, lz4.LZ4Compressor{}}} {
		for _, size := range []int{1024, 400 << 10, 8 << 20} {
			b.Run(fmt.Sprintf("%s/%d", mode.name, size), func(b *testing.B) {
				payload := bytes.Repeat([]byte("stream-payload"), size/14+1)[:size]
				encoded := step04EncodedResponse(mode.version, mode.compressor != nil, payload)
				source := &step04MemoryConn{source: bytes.NewReader(encoded)}
				r := step04MemoryReservation(mode.version, mode.compressor, source)
				defer r.InternalClose(true)
				dst := make([]byte, 0, size)
				request := &attemptRequest{Statement: "SELECT value", Consistency: One}
				var plan StreamRowPlan[[]byte]
				run := func() {
					source.source.Reset(encoded)
					source.closed = false
					source.readCalls = 0
					source.directBytes = 0
					source.otherBulk = 0
					if r.InternalUnits != nil {
						r.InternalUnits.InternalData = nil
						r.InternalUnits.InternalOffset = 0
						r.InternalUnits.InternalFrameActive = false
						r.InternalUnits.InternalStarted = false
						r.InternalUnits.InternalCopies = 0
					}
					response, err := r.InternalExchange(context.Background(), request)
					if err != nil {
						b.Fatal(err)
					}
					if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
						b.Fatal(err)
					}
					if plan == nil {
						plan, err = StreamOneColumn(StreamBlob).Compile(response.InternalColumns)
						if err != nil {
							b.Fatal(err)
						}
					}
					if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil {
						b.Fatal(ok, err)
					}
					response.InternalReleaseMetadata()
				}
				run()
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.StopTimer()
				b.ReportMetric(float64(source.readCalls), "reads/op")
				if r.InternalUnits != nil {
					b.ReportMetric(float64(r.InternalUnits.InternalUnits.StorageBytes()), "transform-B")
					b.ReportMetric(float64(r.InternalUnits.InternalCopies), "unit-copy-B/op")
				}
			})
		}
	}
}
func FuzzV2ExclusiveIncrementalFraming(f *testing.F) {
	f.Add(byte(4), step04EncodedResponse(4, false, []byte("value")))
	f.Add(byte(5), step04EncodedResponse(5, false, []byte("value")))
	f.Add(byte(5), []byte{0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, version byte, input []byte) {
		if len(input) > 8192 {
			t.Skip()
		}
		version = 3 + version%3
		source := &step04MemoryConn{source: bytes.NewReader(input)}
		r := step04MemoryReservation(version, nil, source)
		defer r.InternalClose(false)
		r.InternalLimits.MaxMetadataBytes = 2048
		r.InternalMetadataBudget.SetLimit(2048)
		response, err := r.InternalExchange(context.Background(), &attemptRequest{Statement: "SELECT", Consistency: One})
		if err != nil {
			if response != nil {
				response.InternalReleaseMetadata()
			}
			return
		}
		defer response.InternalReleaseMetadata()
		if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
			return
		}
		plan, err := StreamRow(func(*Unit) {}).Compile(response.InternalColumns)
		if err != nil {
			return
		}
		var dst Unit
		for i := 0; i < 16; i++ {
			ok, err := decodeStreamRow(response, plan, &dst)
			if err != nil || !ok {
				return
			}
		}
	})
}

func BenchmarkV2ExclusiveRowsScaling(b *testing.B) {
	for _, count := range []int{1, 32, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			body := typedTestRowsBody([]byte{42})
			binary.BigEndian.PutUint32(body[len(body)-9:], uint32(count))
			cell := []byte{0, 0, 0, 1, 42}
			for i := 1; i < count; i++ {
				body = append(body, cell...)
			}
			encoded := []byte{0x84, 0, 0, 0, 8}
			encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(body)))
			encoded = append(encoded, body...)
			source := &step04MemoryConn{source: bytes.NewReader(encoded)}
			r := step04MemoryReservation(4, nil, source)
			defer r.InternalClose(true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var plan StreamRowPlan[[]byte]
			dst := make([]byte, 0, 1)
			run := func() {
				source.source.Reset(encoded)
				response, err := r.InternalExchange(ctx, &attemptRequest{Statement: "SELECT value", Consistency: One})
				if err != nil {
					b.Fatal(err)
				}
				if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
					b.Fatal(err)
				}
				if plan == nil {
					plan, err = StreamOneColumn(StreamBlob).Compile(response.InternalColumns)
					if err != nil {
						b.Fatal(err)
					}
				}
				n := 0
				for {
					ok, err := decodeStreamRow(response, plan, &dst)
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						break
					}
					n++
				}
				if n != count {
					b.Fatal(n, count)
				}
				response.InternalReleaseMetadata()
			}
			run()
			b.ReportAllocs()
			b.ReportMetric(float64(count), "rows/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
		})
	}
}

type step04WireCounter struct {
	net.Conn
	reads, writes, readBytes, writeBytes atomic.Int64
}

func (c *step04WireCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.reads.Add(1)
	c.readBytes.Add(int64(n))
	return n, err
}
func (c *step04WireCounter) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.writes.Add(1)
	c.writeBytes.Add(int64(n))
	return n, err
}
func (c *step04WireCounter) reset() {
	c.reads.Store(0)
	c.writes.Store(0)
	c.readBytes.Store(0)
	c.writeBytes.Store(0)
}
func step04BenchTLS(b testing.TB) (*tls.Conn, *tls.Conn, *step04WireCounter, func()) {
	b.Helper()
	rawClient, rawServer := net.Pipe()
	wire := &step04WireCounter{Conn: rawClient}
	cert, config := step04TLS(b)
	client := tls.Client(wire, config)
	server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}, SessionTicketsDisabled: true})
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	if err := client.Handshake(); err != nil {
		b.Fatal(err)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	return client, server, wire, func() { rawClient.Close(); rawServer.Close() }
}
func BenchmarkV2TLSWriteAmplification(b *testing.B) {
	for _, size := range []int{64 << 10, 400 << 10} {
		for _, fragments := range []int{1, 16, 1024} {
			b.Run(fmt.Sprintf("bytes=%d/fragments=%d", size, fragments), func(b *testing.B) {
				client, server, wireCounter, cleanup := step04BenchTLS(b)
				defer cleanup()
				payload := bytes.Repeat([]byte("x"), size)
				parts := make([][]byte, 0, fragments)
				spans := make([]wire.Span, 0, fragments)
				for offset := 0; offset < size; {
					end := min(offset+max(1, size/fragments), size)
					part := payload[offset:end]
					parts = append(parts, part)
					spans = append(spans, wire.Borrow(part))
					offset = end
				}
				chain, err := wire.NewChain(spans...)
				if err != nil {
					b.Fatal(err)
				}
				recorder := &typedTLSRecorder{conn: client, expected: parts}
				writer := &deadlineContextWriter{InternalW: recorder, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
				warm := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					_, err := io.CopyN(io.Discard, server, int64(size))
					if err != nil {
						done <- err
						close(warm)
						return
					}
					close(warm)
					_, err = io.CopyN(io.Discard, server, int64(size)*int64(b.N))
					done <- err
				}()
				run := func() {
					recorder.next = 0
					if _, err := writer.InternalWriteSpans(context.Background(), chain); err != nil {
						b.Fatal(err)
					}
					if recorder.next != len(parts) {
						b.Fatal("span accounting")
					}
				}
				run()
				<-warm
				wireCounter.reset()
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.StopTimer()
				if err := <-done; err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(wireCounter.writes.Load())/float64(b.N), "tls-writes/op")
				b.ReportMetric(float64(wireCounter.writeBytes.Load())/float64(b.N), "tls-wire-B/op")
				b.ReportMetric(float64(len(parts)), "plaintext-spans/op")
				b.ReportMetric(0, "driver-gather-B/op")
			})
		}
	}
}

type step04PlaintextCounter struct {
	net.Conn
	reads, outputBytes int64
	start, end         uintptr
}

func (c *step04PlaintextCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.reads++
	if n > 0 {
		address := uintptr(unsafe.Pointer(&p[0]))
		if address >= c.start && address+uintptr(n) <= c.end {
			c.outputBytes += int64(n)
		} else if n > 64 {
			return n, fmt.Errorf("driver gathered TLS plaintext instead of final output")
		}
	}
	return n, err
}
func BenchmarkV2TLSReceiveAmplification(b *testing.B) {
	for _, size := range []int{4096, 400 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			client, server, wireCounter, cleanup := step04BenchTLS(b)
			defer cleanup()
			encoded := step04EncodedResponse(4, false, bytes.Repeat([]byte("z"), size))
			plain := &step04PlaintextCounter{Conn: client}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := &driverConn{InternalVersion: 4, InternalCtx: ctx, InternalCancel: cancel, InternalR: &connReader{InternalConn: plain}, InternalStreams: streams.New(), InternalLogger: nopLogger{}, InternalErrorHandler: connErrorHandlerFn(func(*driverConn, error, bool) {})}
			c.InternalW = &deadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			c.InternalInitFramerCache()
			stream, _ := c.InternalStreams.GetStream()
			c.InternalExclusiveFlag.Store(true)
			r := &exclusiveConn{InternalConn: c, InternalCtx: ctx, InternalStream: stream, InternalComplete: true, InternalDone: make(chan struct{})}
			r.InternalInitStream(StreamLimits{})
			defer r.InternalClose(true)
			warm := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				var header [9]byte
				for i := 0; i <= b.N; i++ {
					if _, err := io.ReadFull(server, header[:]); err != nil {
						done <- err
						return
					}
					if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
						done <- err
						return
					}
					if _, err := server.Write(encoded); err != nil {
						done <- err
						return
					}
					if i == 0 {
						close(warm)
					}
				}
				done <- nil
			}()
			dst := make([]byte, 0, size)
			plain.start = uintptr(unsafe.Pointer(&dst[:cap(dst)][0]))
			plain.end = plain.start + uintptr(size)
			var plan StreamRowPlan[[]byte]
			run := func() {
				response, err := r.InternalExchange(ctx, &attemptRequest{Statement: "SELECT value", Consistency: One})
				if err != nil {
					b.Fatal(err)
				}
				if err = response.InternalInstallColumns(response.InternalColumns); err != nil {
					b.Fatal(err)
				}
				if plan == nil {
					plan, err = StreamOneColumn(StreamBlob).Compile(response.InternalColumns)
					if err != nil {
						b.Fatal(err)
					}
				}
				if ok, err := decodeStreamRow(response, plan, &dst); !ok || err != nil {
					b.Fatal(ok, err)
				}
				response.InternalReleaseMetadata()
			}
			run()
			<-warm
			wireCounter.reset()
			plain.reads = 0
			plain.outputBytes = 0
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
			b.StopTimer()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(wireCounter.reads.Load())/float64(b.N), "net-reads/op")
			b.ReportMetric(float64(wireCounter.readBytes.Load())/float64(b.N), "tls-wire-read-B/op")
			b.ReportMetric(float64(plain.reads)/float64(b.N), "plaintext-reads/op")
			b.ReportMetric(float64(plain.outputBytes)/float64(b.N), "tls-output-handoff-B/op")
			b.ReportMetric(0, "driver-gather-B/op")
		})
	}
}
