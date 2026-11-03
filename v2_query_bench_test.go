// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"time"
)

// These diagnostics isolate native paging and decoding from application SHA-256.
// Canonical comparison observations always retain full payload hashing.
func BenchmarkReusableQueryDecoding(b *testing.B) {
	for _, size := range []struct {
		name        string
		rows, bytes int
	}{{"point", 1, 1024}, {"partition", 128, 4096}} {
		for _, stream := range []bool{false, true} {
			name := size.name + "/buffered"
			if stream {
				name = size.name + "/stream"
			}
			b.Run(name, func(b *testing.B) {
				s, conns, peers := runtimeTestSession(b, 1)
				value := strings.Repeat("x", size.bytes)
				var bodies [][]byte
				if size.rows == 1 {
					bodies = [][]byte{runtimeRowsBody(nil, value)}
				} else {
					values := make([]string, 32)
					for i := range values {
						values[i] = value
					}
					for page := 1; page <= 4; page++ {
						bodies = append(bodies, runtimeRowsBody([]byte{byte(page)}, values...))
					}
					bodies = append(bodies, runtimeRowsBody(nil))
				}
				served := make(chan error, 1)
				go func() {
					var header [9]byte
					for range b.N {
						for _, body := range bodies {
							if _, err := io.ReadFull(peers[0], header[:]); err != nil {
								served <- err
								return
							}
							if _, err := io.CopyN(io.Discard, peers[0], int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
								served <- err
								return
							}
							header[0], header[1], header[4] = 0x84, 0, 8
							binary.BigEndian.PutUint32(header[5:], uint32(len(body)))
							if _, err := peers[0].Write(header[:]); err != nil {
								served <- err
								return
							}
							if _, err := peers[0].Write(body); err != nil {
								served <- err
								return
							}
						}
					}
					served <- nil
				}()
				options := Options{PageSize: Option[int32]{Set: true, Value: 32}, Prefetch: Option[int]{Set: true, Value: 0}, Timeout: Option[time.Duration]{Set: true, Value: 10 * time.Second}}
				statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
				var query *Query[Unit, []byte]
				var streaming *StreamQuery[Unit, []byte]
				if stream {
					streaming = codecTestMust(NewUnpreparedStreamQuery(s, statement, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{Options: options}))
					defer streaming.Close()
				} else {
					query = codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), options))
					defer query.Close()
				}
				dst := make([]byte, 0, size.bytes)
				b.ReportAllocs()
				b.SetBytes(int64(size.rows * size.bytes))
				b.ResetTimer()
				for range b.N {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					count := 0
					if stream {
						cursor, err := streaming.Run(ctx, Unit{})
						if err != nil {
							b.Fatal(err)
						}
						for cursor.NextInto(&dst) {
							count++
							if len(dst) != size.bytes || dst[0] != 'x' || dst[len(dst)-1] != 'x' {
								b.Fatal("bad diagnostic payload")
							}
						}
						if err := cursor.Err(); err != nil {
							b.Fatal(err)
						}
						if err := cursor.Close(); err != nil {
							b.Fatal(err)
						}
						if err := streaming.Wait(ctx); err != nil {
							b.Fatal(err)
						}
					} else {
						cursor, err := query.Run(ctx, Unit{})
						if err != nil {
							b.Fatal(err)
						}
						for cursor.NextInto(&dst) {
							count++
							if len(dst) != size.bytes || dst[0] != 'x' || dst[len(dst)-1] != 'x' {
								b.Fatal("bad diagnostic payload")
							}
						}
						if err := cursor.Err(); err != nil {
							b.Fatal(err)
						}
						if err := cursor.Close(); err != nil {
							b.Fatal(err)
						}
						if err := query.Wait(ctx); err != nil {
							b.Fatal(err)
						}
					}
					cancel()
					if count != size.rows {
						b.Fatal("incomplete diagnostic partition", count)
					}
				}
				b.StopTimer()
				if err := <-served; err != nil {
					b.Fatal(err)
				}
				_ = conns
			})
		}
	}
}
