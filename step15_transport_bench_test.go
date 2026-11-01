// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Uses real loopback sockets and the actual coalescer; no wrapper hides writev.
// OS syscall counts are collected in separate strace runs of the same cases.
func BenchmarkStep15SocketWrites(b *testing.B) {
	for _, tlsOn := range []bool{false, true} {
		for _, count := range []int{8, 1024, 4096} {
			for _, mixed := range []bool{false, true} {
				b.Run(fmt.Sprintf("tls%t/spans%d/mixed%t", tlsOn, count, mixed), func(b *testing.B) {
					listener, e := net.Listen("tcp", "127.0.0.1:0")
					if e != nil {
						b.Fatal(e)
					}
					defer listener.Close()
					accepted := make(chan net.Conn, 1)
					go func() {
						conn, e := listener.Accept()
						if e == nil {
							accepted <- conn
						}
					}()
					client, e := net.Dial("tcp", listener.Addr().String())
					if e != nil {
						b.Fatal(e)
					}
					server := <-accepted
					defer client.Close()
					defer server.Close()
					if tlsOn {
						cert, conf := step04TLS(b)
						a := tls.Client(client, conf)
						z := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{cert}})
						done := make(chan error, 1)
						go func() { done <- z.Handshake() }()
						if e = a.Handshake(); e != nil {
							b.Fatal(e)
						}
						if e = <-done; e != nil {
							b.Fatal(e)
						}
						client, server = a, z
					}
					spans := make([]wire.Span, 0, count+1)
					for i := 0; i < count; i++ {
						spans = append(spans, wire.Borrow([]byte{byte(i)}))
					}
					if mixed {
						spans = append(spans, wire.Borrow(make([]byte, 500<<10)))
					}
					chain := codecTestMust(wire.NewChain(spans...))
					quit := make(chan struct{})
					writer := newWriteCoalescer(client, 0, time.Microsecond, quit)
					defer close(quit)
					done := make(chan error, 1)
					go func() { _, e := io.CopyN(io.Discard, server, int64(b.N)*chain.Len()); done <- e }()
					b.SetBytes(chain.Len())
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, e := writer.InternalWriteSpans(context.Background(), chain); e != nil {
							b.Fatal(e)
						}
					}
					b.StopTimer()
					if e := <-done; e != nil {
						b.Fatal(e)
					}
					b.ReportMetric(float64(len(spans)), "spans/op")
				})
			}
		}
	}
}
