//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStep15CompressionCopyBoundary(t *testing.T) {
	for _, codec := range codecs {
		for _, kind := range []string{"repetitive", "random", "mixed"} {
			for _, count := range []int{1, 16, 4096} {
				t.Run(fmt.Sprintf("%s/%s/spans%d", codec.name, kind, count), func(t *testing.T) {
					data := bytes.Repeat([]byte{'x'}, 500<<10)
					if kind == "random" {
						rand.New(rand.NewSource(15)).Read(data)
					}
					if kind == "mixed" {
						rand.New(rand.NewSource(15)).Read(data[len(data)/2:])
					}
					chain := splitChain(t, data, max(1, len(data)/count))
					dst := make([]byte, 0, len(data)+len(data)/6+64)
					base := unsafe.SliceData(dst[:cap(dst)])
					wire.ResetPerfCounters()
					encoded, e := codec.append(dst, chain)
					if e != nil {
						t.Fatal(e)
					}
					c := wire.ReadPerfCounters()
					if c.CopyBytes != 0 || unsafe.SliceData(encoded) != base {
						t.Fatal("gather or output backing replacement", c)
					}
					if kind == "repetitive" && len(encoded) > len(data)/4 {
						t.Fatal("ineffective compression")
					}
					t.Logf("input=%d output=%d ratio=%.5f transformed_input=%d source_gather_bytes=%d output_to_frame_copy=0", len(data), len(encoded), float64(len(encoded))/float64(len(data)), len(data), c.CopyBytes)
					checkRoundTrip(t, codec, chain, data)
				})
			}
		}
	}
}
