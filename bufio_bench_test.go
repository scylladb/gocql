//go:build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bufio"
	"fmt"
	"io"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// countingReader wraps an io.Reader and counts the number of Read() calls.
// On a real TCP socket, each Read() corresponds to a syscall (~100-200ns overhead).
type countingReader struct {
	r     io.Reader
	calls int
}

func (cr *countingReader) Read(p []byte) (int, error) {
	cr.calls++
	return cr.r.Read(p)
}

// repeatingFrameReader streams the same frame bytes repeatedly without
// materializing all of them up front. This avoids allocating b.N*frameSize
// bytes (potentially gigabytes for large frames / high b.N) just to feed the
// benchmark input.
type repeatingFrameReader struct {
	frame []byte
	n     int // remaining whole frames to emit
	off   int // offset within the current frame
}

func (rr *repeatingFrameReader) Read(p []byte) (int, error) {
	if rr.n == 0 && rr.off == 0 {
		return 0, io.EOF
	}
	read := 0
	for read < len(p) && rr.n > 0 {
		copied := copy(p[read:], rr.frame[rr.off:])
		read += copied
		rr.off += copied
		if rr.off == len(rr.frame) {
			rr.off = 0
			rr.n--
		}
	}
	return read, nil
}

// BenchmarkBufioSizeReadCalls demonstrates how larger bufio.Reader sizes reduce
// the number of underlying Read() calls per CQL frame. On real TCP sockets, each
// Read() is a syscall costing ~100-200ns in kernel context switch overhead.
// The "reads/frame" metric shows the direct syscall reduction.
func BenchmarkBufioSizeReadCalls(b *testing.B) {
	for _, frameSize := range []int{4096, 32768, 131072} {
		for _, bufSize := range []int{4096, 16384, 32768, 65536} {
			name := fmt.Sprintf("frame=%dKB/bufio=%dKB", frameSize/1024, bufSize/1024)
			b.Run(name, func(b *testing.B) {
				body := make([]byte, frameSize)
				header := make([]byte, headSize)
				header[0] = byte(protoVersion4) | 0x80
				header[4] = byte(frm.OpResult)
				header[5] = byte(frameSize >> 24)
				header[6] = byte(frameSize >> 16)
				header[7] = byte(frameSize >> 8)
				header[8] = byte(frameSize)
				fullFrame := append(header, body...)

				// Stream the frame repeatedly instead of allocating
				// b.N*frameSize bytes up front.
				cr := &countingReader{r: &repeatingFrameReader{frame: fullFrame, n: b.N}}
				r := bufio.NewReaderSize(cr, bufSize)
				headerBuf := make([]byte, headSize)
				f := newFramer(nil, protoVersion4)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					head, err := readHeader(r, headerBuf)
					if err != nil {
						b.Fatal(err)
					}
					if err := f.readFrame(r, &head); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(cr.calls)/float64(b.N), "reads/frame")
			})
		}
	}
}
