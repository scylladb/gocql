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
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// Final caller storage and immutable peer response fixtures are outside the
// measured loop. Measurements include net.Pipe/peer scheduling and complete
// logical API setup; they are not database/socket throughput numbers.
func BenchmarkStep07ExecutionStorage(b *testing.B) {
	for _, shape := range []struct{ rows, cell int }{{1, 1024}, {100, 1024}, {1000, 1024}, {1, 400 << 10}, {1, 8 << 20}} {
		for _, mode := range []string{"row", "into", "stream"} {
			b.Run(fmt.Sprintf("%s/rows=%d/cell=%d", mode, shape.rows, shape.cell), func(b *testing.B) {
				s, cs, peers := runtimeTestSession(b, 1)
				c, peer := cs[0], peers[0]
				values := make([]string, shape.rows)
				for i := range values {
					values[i] = strings.Repeat("x", shape.cell)
				}
				body := runtimeRowsBody(nil, values...)
				done := make(chan struct{})
				go func() {
					defer close(done)
					for i := 0; i < b.N; i++ {
						id := typedTestReadRequest(b, c, peer)
						header := typedTestResponse(b, 4, id, nil)
						binary.BigEndian.PutUint32(header[5:], uint32(len(body)))
						if _, err := peer.Write(header); err != nil {
							b.Error(err)
							return
						}
						if _, err := peer.Write(body); err != nil {
							b.Error(err)
							return
						}
					}
				}()
				bound := runtimeQuery(b)
				dst := make([]byte, 0, shape.cell)
				b.SetBytes(int64(shape.rows * shape.cell))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode == "stream" {
						cursor, err := RunStream(context.Background(), s, bound, StreamOneColumn(StreamBlob), StreamOptions{})
						if err != nil {
							b.Fatal(err)
						}
						count := 0
						for cursor.NextInto(&dst) {
							count++
						}
						if cursor.Err() != nil || count != shape.rows {
							b.Fatal(count, cursor.Err())
						}
						cursor.Close()
					} else {
						rows, err := Run(context.Background(), s, bound, Options{Prefetch: Option[int]{Set: true, Value: 0}})
						if err != nil {
							b.Fatal(err)
						}
						count := 0
						var view Bytes
						for {
							ok := false
							if mode == "row" {
								ok = rows.Next()
								if ok {
									view = rows.Row()
								}
							} else {
								ok = rows.NextInto(&view)
							}
							if !ok {
								break
							}
							if view.Len() != int64(shape.cell) {
								b.Fatal("size")
							}
							count++
						}
						if rows.Err() != nil || count != shape.rows {
							b.Fatal(count, rows.Err())
						}
						rows.Close()
					}
				}
				b.StopTimer()
				bound.Close()
				bound.Wait(context.Background())
				<-done
				b.ReportMetric(float64(shape.cell), "caller-capacity-B")
				if mode == "stream" {
					b.ReportMetric(0, "driver-payload-gather-B/op")
				} else {
					b.ReportMetric(float64(shape.rows*shape.cell), "retained-page-payload-B/op")
				}
			})
		}
	}
}

func BenchmarkStep07SharedAdmission(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16} {
		b.Run(fmt.Sprint(parallelism), func(b *testing.B) {
			s := &Session{}
			bound := runtimeQuery(b)
			b.ReportAllocs()
			old := runtime.GOMAXPROCS(parallelism)
			defer runtime.GOMAXPROCS(old)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ctx, cancel, request, err := admitExecution(context.Background(), s, bound, Options{})
					if err != nil {
						b.Error(err)
						return
					}
					_ = ctx
					request.logical.finish(Result{}, nil)
					cancel()
					request.logical.releaseInput()
				}
			})
			b.StopTimer()
			bound.Close()
			bound.Wait(context.Background())
		})
	}
}
