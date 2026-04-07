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
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// BenchmarkFramerFinishNoCompressor measures frame serialization without any
// compressor set (baseline). This is the cheapest path through finish().
func BenchmarkFramerFinishNoCompressor(b *testing.B) {
	for _, bodySize := range []int{64, 512, 4096, 48000} {
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)

			for i := 0; i < b.N; i++ {
				f := newFramer(nil, protoVersion4, compressionOpts{})
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFramerFinishCompressAll measures frame serialization with
// compression always applied (threshold=0, the backward-compatible default).
func BenchmarkFramerFinishCompressAll(b *testing.B) {
	comp := SnappyCompressor{}
	for _, bodySize := range []int{64, 512, 4096, 48000} {
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)

			for i := 0; i < b.N; i++ {
				f := newFramer(comp, protoVersion4, compressionOpts{})
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFramerFinishThresholdSkip measures frame serialization when the
// compressor is configured but the body is below the compression threshold,
// so compression is skipped. This shows the cost of the threshold check +
// flag clearing compared to the no-compressor baseline.
func BenchmarkFramerFinishThresholdSkip(b *testing.B) {
	comp := SnappyCompressor{}
	// Threshold is larger than all body sizes → compression always skipped.
	opts := compressionOpts{threshold: 100000}
	for _, bodySize := range []int{64, 512, 4096, 48000} {
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)

			for i := 0; i < b.N; i++ {
				f := newFramer(comp, protoVersion4, opts)
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFramerFinishNeverCompress measures frame serialization with the
// never-compress sentinel (threshold=-1). This should be comparable to the
// threshold-skip path.
func BenchmarkFramerFinishNeverCompress(b *testing.B) {
	comp := SnappyCompressor{}
	opts := compressionOpts{threshold: neverCompressSize}
	for _, bodySize := range []int{64, 512, 4096, 48000} {
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)

			for i := 0; i < b.N; i++ {
				f := newFramer(comp, protoVersion4, opts)
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFramerFinishThresholdCompress measures frame serialization when
// the body exceeds the threshold and compression is applied.
func BenchmarkFramerFinishThresholdCompress(b *testing.B) {
	comp := SnappyCompressor{}
	// Threshold at 256 bytes, bodies are all above that.
	opts := compressionOpts{threshold: 256}
	for _, bodySize := range []int{512, 4096, 48000} {
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)

			for i := 0; i < b.N; i++ {
				f := newFramer(comp, protoVersion4, opts)
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFramerFinishRatioKeep measures frame serialization where
// compression is applied and the ratio check passes (compressible data).
// This shows the overhead of the ratio check on the happy path.
func BenchmarkFramerFinishRatioKeep(b *testing.B) {
	comp := SnappyCompressor{}
	opts := compressionOpts{minSavingsPct: 15}
	// All-zero body compresses extremely well → ratio check passes.
	for _, bodySize := range []int{512, 4096, 48000} {
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)

			for i := 0; i < b.N; i++ {
				f := newFramer(comp, protoVersion4, opts)
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFramerFinishRatioDiscard measures frame serialization where
// compression is applied but the ratio check fails (incompressible data),
// causing the compressed output to be discarded. This measures the cost of
// the "wasted compression" path.
func BenchmarkFramerFinishRatioDiscard(b *testing.B) {
	comp := SnappyCompressor{}
	opts := compressionOpts{minSavingsPct: 15}
	for _, bodySize := range []int{512, 4096, 48000} {
		// Pre-generate random body once per sub-benchmark.
		b.Run(fmt.Sprintf("body=%d", bodySize), func(b *testing.B) {
			b.ReportAllocs()
			body := make([]byte, bodySize)
			if _, err := rand.Read(body); err != nil {
				b.Fatal(err)
			}

			for i := 0; i < b.N; i++ {
				f := newFramer(comp, protoVersion4, opts)
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.buf = append(f.buf, body...)
				if err := f.finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkQueryFrameSerialization measures full QUERY frame serialization
// (writeQueryFrame → finish) with different compression configurations.
func BenchmarkQueryFrameSerialization(b *testing.B) {
	shortStmt := "SELECT * FROM ks.tbl WHERE id = ?"
	// Build a ~4KB statement to simulate larger queries.
	longStmt := "INSERT INTO ks.tbl (id, data) VALUES (?, '" + strings.Repeat("x", 4000) + "')"

	cases := []struct {
		name      string
		comp      Compressor
		statement string
		opts      compressionOpts
	}{
		{"no_compression/short", nil, shortStmt, compressionOpts{}},
		{"compress_all/short", SnappyCompressor{}, shortStmt, compressionOpts{}},
		{"threshold_skip/short", SnappyCompressor{}, shortStmt, compressionOpts{threshold: 8192}},
		{"no_compression/long", nil, longStmt, compressionOpts{}},
		{"compress_all/long", SnappyCompressor{}, longStmt, compressionOpts{}},
		{"threshold_skip/long", SnappyCompressor{}, longStmt, compressionOpts{threshold: 8192}},
		{"threshold_compress/long", SnappyCompressor{}, longStmt, compressionOpts{threshold: 1024}},
		{"ratio_keep/long", SnappyCompressor{}, longStmt, compressionOpts{minSavingsPct: 10}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			frame := &writeQueryFrame{
				statement: tc.statement,
				params:    queryParams{consistency: One},
			}

			for i := 0; i < b.N; i++ {
				f := newFramer(tc.comp, protoVersion4, tc.opts)
				if err := frame.buildFrame(f, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExecuteFrameSerialization measures EXECUTE frame serialization with
// various compression configurations.
func BenchmarkExecuteFrameSerialization(b *testing.B) {
	preparedID := []byte("prepared_test_id_1234567890")
	typ := NativeType{proto: protoVersion4, typ: TypeInt}
	val, _ := Marshal(typ, 42)

	cases := []struct {
		comp Compressor
		name string
		opts compressionOpts
	}{
		{nil, "no_compression", compressionOpts{}},
		{SnappyCompressor{}, "compress_all", compressionOpts{}},
		{SnappyCompressor{}, "threshold_skip", compressionOpts{threshold: 8192}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			frame := &writeExecuteFrame{
				preparedID: preparedID,
				params: queryParams{
					consistency: One,
					values:      []queryValues{{value: val}},
				},
			}

			for i := 0; i < b.N; i++ {
				f := newFramer(tc.comp, protoVersion4, tc.opts)
				if err := frame.buildFrame(f, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBatchFrameCompressionThreshold measures BATCH frame serialization
// which produces larger payloads, comparing compression strategies.
func BenchmarkBatchFrameCompressionThreshold(b *testing.B) {
	colCount := 2
	typ := NativeType{proto: protoVersion4, typ: TypeInt}

	makeBatch := func(numStatements int) *writeBatchFrame {
		frame := &writeBatchFrame{
			typ:              LoggedBatch,
			statements:       make([]batchStatment, numStatements),
			consistency:      Quorum,
			defaultTimestamp: true,
		}
		for j := 0; j < numStatements; j++ {
			bs := &frame.statements[j]
			bs.preparedID = []byte(fmt.Sprintf("prepared_%d", j%5))
			bs.values = make([]queryValues, colCount)
			for k := 0; k < colCount; k++ {
				val, _ := Marshal(typ, j+k)
				bs.values[k] = queryValues{value: val}
			}
		}
		return frame
	}

	for _, size := range []int{10, 100} {
		cases := []struct {
			comp Compressor
			name string
			opts compressionOpts
		}{
			{nil, "no_compression", compressionOpts{}},
			{SnappyCompressor{}, "compress_all", compressionOpts{}},
			{SnappyCompressor{}, "threshold_skip", compressionOpts{threshold: 100000}},
			{SnappyCompressor{}, "threshold_compress", compressionOpts{threshold: 64}},
			{SnappyCompressor{}, "never_compress", compressionOpts{threshold: neverCompressSize}},
			{SnappyCompressor{}, "ratio_keep", compressionOpts{minSavingsPct: 10}},
		}

		for _, tc := range cases {
			b.Run(fmt.Sprintf("entries=%d/%s", size, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				frame := makeBatch(size)

				for i := 0; i < b.N; i++ {
					f := newFramer(tc.comp, protoVersion4, tc.opts)
					if err := frame.buildFrame(f, 1); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkResolveCompressionThreshold measures the cost of resolving the
// per-connection compression threshold, which runs once per connection setup.
func BenchmarkResolveCompressionThreshold(b *testing.B) {
	localRackHost := &HostInfo{dataCenter: "dc1", rack: "rack1"}
	remoteDCHost := &HostInfo{dataCenter: "dc2", rack: "rack1"}

	cases := []struct {
		policy HostSelectionPolicy
		host   *HostInfo
		name   string
		cp     CompressionPolicy
	}{
		{
			name:   "zero_policy/rack_aware",
			policy: RackAwareRoundRobinPolicy("dc1", "rack1"),
			host:   localRackHost,
			cp:     CompressionPolicy{},
		},
		{
			name:   "local_rack/rack_aware",
			policy: RackAwareRoundRobinPolicy("dc1", "rack1"),
			host:   localRackHost,
			cp: CompressionPolicy{
				MinCompressLocalSize:  neverCompressSize,
				MinCompressRemoteSize: 1024,
				Scope:                 CompressNonLocalRack,
			},
		},
		{
			name:   "remote_dc/rack_aware",
			policy: RackAwareRoundRobinPolicy("dc1", "rack1"),
			host:   remoteDCHost,
			cp: CompressionPolicy{
				MinCompressLocalSize:  neverCompressSize,
				MinCompressRemoteSize: 1024,
				Scope:                 CompressNonLocalDC,
			},
		},
		{
			name:   "remote_dc/dc_aware",
			policy: DCAwareRoundRobinPolicy("dc1"),
			host:   remoteDCHost,
			cp: CompressionPolicy{
				MinCompressLocalSize:  5000,
				MinCompressRemoteSize: 500,
				Scope:                 CompressNonLocalDC,
			},
		},
		{
			name:   "roundrobin_no_tierer",
			policy: RoundRobinHostPolicy(),
			host:   remoteDCHost,
			cp: CompressionPolicy{
				MinCompressLocalSize:  4096,
				MinCompressRemoteSize: 256,
				Scope:                 CompressNonLocalRack,
			},
		},
	}

	for _, tc := range cases {
		tc.policy.AddHost(localRackHost)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = resolveCompressionThreshold(tc.cp, tc.policy, tc.host)
			}
		})
	}
}
