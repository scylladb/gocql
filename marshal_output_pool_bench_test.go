//go:build all || unit
// +build all unit

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

import "testing"

// These benchmarks isolate marshalOutputPool's own contribution, separate
// from the type-specialized fast-path encoding it sits on top of. Each pair
// marshals the same []int32 list via marshalListInt32 (which calls
// getMarshalOutput): "Pooled" returns the buffer via putMarshalOutput before
// the next iteration, as conn.go does after the framer copies it; "Unpooled"
// never returns it, so getMarshalOutput's pool is permanently empty and every
// call falls through to a fresh make([]byte, size) — the byte-for-byte
// behavior this file's marshalOutputPool replaces.
//
// n=1 and n=10 approximate typical query parameters; n=1000 and n=11000
// approximate the large-collection-cell motivation cited in PR #910
// (~11K-element cells).

func benchmarkMarshalListInt32Sizes(b *testing.B, release bool) {
	sizes := []int{1, 10, 1000, 11000}
	for _, n := range sizes {
		input := make([]int32, n)
		for i := range input {
			input[i] = int32(i)
		}
		b.Run(sizeName(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf, err := marshalListInt32(input)
				if err != nil {
					b.Fatal(err)
				}
				if release {
					putMarshalOutput(buf)
				}
			}
		})
	}
}

func BenchmarkMarshalListInt32PooledRelease(b *testing.B) {
	benchmarkMarshalListInt32Sizes(b, true)
}

func BenchmarkMarshalListInt32NoRelease(b *testing.B) {
	benchmarkMarshalListInt32Sizes(b, false)
}

func benchmarkMarshalListInt32SizesParallel(b *testing.B, release bool) {
	sizes := []int{1, 10, 1000, 11000}
	for _, n := range sizes {
		input := make([]int32, n)
		for i := range input {
			input[i] = int32(i)
		}
		b.Run(sizeName(n), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf, err := marshalListInt32(input)
					if err != nil {
						b.Fatal(err)
					}
					if release {
						putMarshalOutput(buf)
					}
				}
			})
		})
	}
}

// BenchmarkMarshalListInt32PooledReleaseParallel exercises marshalOutputPool
// under concurrent load (sync.Pool's actual design target — its per-P local
// cache only pays off when multiple goroutines contend for buffers).
func BenchmarkMarshalListInt32PooledReleaseParallel(b *testing.B) {
	benchmarkMarshalListInt32SizesParallel(b, true)
}

func BenchmarkMarshalListInt32NoReleaseParallel(b *testing.B) {
	benchmarkMarshalListInt32SizesParallel(b, false)
}

func sizeName(n int) string {
	switch n {
	case 1:
		return "n_1"
	case 10:
		return "n_10"
	case 1000:
		return "n_1000"
	case 11000:
		return "n_11000"
	default:
		return "n_other"
	}
}
