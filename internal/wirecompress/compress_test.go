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

package wirecompress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/snappy"

	"github.com/pierrec/lz4/v4"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type blockCodec struct {
	name   string
	append func([]byte, wire.Chain) ([]byte, error)
	decode func([]byte, int) ([]byte, error)
}

var codecs = []blockCodec{
	{name: "lz4", append: AppendLZ4, decode: func(encoded []byte, size int) ([]byte, error) {
		decoded := make([]byte, size)
		n, err := lz4.UncompressBlock(encoded, decoded)
		if err != nil {
			return nil, err
		}
		if n != size {
			return nil, fmt.Errorf("decoded %d bytes, want %d", n, size)
		}
		return decoded, nil
	}},
	{name: "snappy", append: AppendSnappy, decode: func(encoded []byte, size int) ([]byte, error) {
		// DecodeStrict rejects S2 repeat-offset extensions that the ordinary
		// klauspost decoder deliberately accepts in addition to Snappy.
		return snappy.DecodeStrict(nil, encoded)
	}},
}

func splitChain(t testing.TB, data []byte, step int) wire.Chain {
	t.Helper()
	var spans []wire.Span
	for len(data) != 0 {
		n := min(len(data), step)
		spans = append(spans, wire.Borrow(data[:n]))
		data = data[n:]
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

func checkRoundTrip(t testing.TB, codec blockCodec, chain wire.Chain, want []byte) []byte {
	t.Helper()
	prefix := []byte{0xfa, 0xce, 0xb0, 0x0c}
	dst := append(make([]byte, 0, len(want)+len(want)/6+64), prefix...)
	encoded, err := codec.append(dst, chain)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded[:len(prefix)], prefix) {
		t.Fatalf("%s changed destination prefix", codec.name)
	}
	encoded = encoded[len(prefix):]
	got, err := codec.decode(encoded, len(want))
	if err != nil {
		t.Fatalf("%s decoder: %v; input length %d", codec.name, err, len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s round trip mismatch: got %d bytes, want %d", codec.name, len(got), len(want))
	}
	return encoded
}

func TestEmpty(t *testing.T) {
	for _, codec := range codecs {
		t.Run(codec.name, func(t *testing.T) {
			for _, data := range [][]byte{nil, {}, make([]byte, 0, 16)} {
				chain, err := wire.NewChain(wire.Borrow(data), wire.Borrow(nil))
				if err != nil {
					t.Fatal(err)
				}
				encoded := checkRoundTrip(t, codec, chain, nil)
				if !bytes.Equal(encoded, []byte{0}) {
					t.Fatalf("empty block = %x, want 00", encoded)
				}
			}
		})
	}
}

func TestGrowingOutput(t *testing.T) {
	data := make([]byte, 256<<10)
	_, _ = rand.New(rand.NewSource(531)).Read(data)
	chain := splitChain(t, data, 117)
	for _, codec := range codecs {
		for _, prefix := range [][]byte{nil, {1, 2, 3}} {
			dst := bytes.Clone(prefix)
			encoded, err := codec.append(dst, chain)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded[:len(prefix)], prefix) {
				t.Fatal("growing output changed prefix")
			}
			got, err := codec.decode(encoded[len(prefix):], len(data))
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("%s growing-output round trip: %v", codec.name, err)
			}
		}
	}
}

func TestBoundaryLengths(t *testing.T) {
	lengths := []int{255, 256, 269, 270, 271, 65535, 65536, 65537, 131072}
	for n := 1; n <= 80; n++ {
		lengths = append(lengths, n)
	}
	rng := rand.New(rand.NewSource(73))
	for _, n := range lengths {
		random := make([]byte, n)
		_, _ = rng.Read(random)
		for _, data := range [][]byte{random, bytes.Repeat([]byte{'r'}, n)} {
			for _, step := range []int{1, 3, 17, 8191} {
				chain := splitChain(t, data, step)
				for _, codec := range codecs {
					checkRoundTrip(t, codec, chain, data)
				}
			}
		}
	}
}

func TestRandomSplitChains(t *testing.T) {
	rng := rand.New(rand.NewSource(918273))
	for iteration := 0; iteration < 150; iteration++ {
		data := make([]byte, rng.Intn(64<<10))
		_, _ = rng.Read(data)
		// Insert repeated regions with varied (including overlapping) offsets.
		for i := 1; i < len(data); {
			n := min(len(data)-i, 1+rng.Intn(1024))
			if rng.Intn(2) == 0 {
				offset := 1 + rng.Intn(i)
				for j := 0; j < n; j++ {
					data[i+j] = data[i+j-offset]
				}
			}
			i += n
		}
		original := bytes.Clone(data)
		var spans []wire.Span
		for pos := 0; pos < len(data); {
			n := min(len(data)-pos, 1+rng.Intn(513))
			if rng.Intn(4) == 0 {
				spans = append(spans, wire.BorrowString(string(data[pos:pos+n])))
			} else {
				spans = append(spans, wire.Borrow(data[pos:pos+n]))
			}
			pos += n
		}
		chain, err := wire.NewChain(spans...)
		if err != nil {
			t.Fatal(err)
		}
		for _, codec := range codecs {
			checkRoundTrip(t, codec, chain, original)
		}
		if !bytes.Equal(data, original) {
			t.Fatal("compressor mutated borrowed source")
		}
	}
}

func TestCrossSpanMatchCompression(t *testing.T) {
	pattern := []byte("a match crosses every borrowed boundary;")
	data := bytes.Repeat(pattern, 8192)
	for _, step := range []int{1, 3, len(pattern) - 1, 65533} {
		chain := splitChain(t, data, step)
		for _, codec := range codecs {
			t.Run(fmt.Sprintf("%s/span-%d", codec.name, step), func(t *testing.T) {
				encoded := checkRoundTrip(t, codec, chain, data)
				if len(encoded) >= len(data)/10 {
					t.Fatalf("repetitive input did not compress: %d -> %d", len(data), len(encoded))
				}
			})
		}
	}
}

func TestSnappyLongOffset(t *testing.T) {
	// Avoid incidental matches and ensure the reference is outside LZ4's
	// 16-bit window. Hash collisions may lose some matches; a 16 KiB repeated
	// suffix leaves many opportunities for a four-byte copy instruction.
	rng := rand.New(rand.NewSource(991))
	data := make([]byte, 90000)
	_, _ = rng.Read(data)
	data = append(data, data[:16000]...)
	chain := splitChain(t, data, 1117)
	encoded := checkRoundTrip(t, codecs[1], chain, data)
	_, prefix := binary.Uvarint(encoded)
	longOffset := false
	for p := prefix; p < len(encoded); {
		tag := encoded[p]
		p++
		switch tag & 3 {
		case 0:
			n := int(tag >> 2)
			if n < 60 {
				n++
			} else {
				width := n - 59
				n = 0
				for i := 0; i < width; i++ {
					n |= int(encoded[p+i]) << (8 * i)
				}
				p += width
				n++
			}
			p += n
		case 1:
			p++
		case 2:
			p += 2
		case 3:
			if binary.LittleEndian.Uint32(encoded[p:]) > 65535 {
				longOffset = true
			}
			p += 4
		}
	}
	if !longOffset {
		t.Fatal("encoder did not produce a match beyond the 64 KiB window")
	}
	if len(encoded) >= len(data)-8000 {
		t.Fatalf("long-offset repeated region did not compress: %d -> %d", len(data), len(encoded))
	}
}

func TestLZ4BlockTermination(t *testing.T) {
	for n := 0; n < 300; n++ {
		data := bytes.Repeat([]byte{'a'}, n)
		encoded := checkRoundTrip(t, codecs[0], splitChain(t, data, 3), data)
		position, decoded, lastMatch := 0, 0, -1
		for position < len(encoded) {
			token := encoded[position]
			position++
			literalLen := int(token >> 4)
			if literalLen == 15 {
				for {
					next := int(encoded[position])
					position++
					literalLen += next
					if next != 255 {
						break
					}
				}
			}
			position += literalLen
			decoded += literalLen
			if position == len(encoded) {
				if token&15 != 0 || literalLen < min(5, n) {
					t.Fatalf("n=%d: invalid final literal sequence: token=%x length=%d", n, token, literalLen)
				}
				break
			}
			offset := int(binary.LittleEndian.Uint16(encoded[position:]))
			position += 2
			if offset == 0 || offset > decoded {
				t.Fatalf("n=%d: invalid match offset %d at %d", n, offset, decoded)
			}
			lastMatch = decoded
			matchLen := int(token&15) + 4
			if matchLen == 19 {
				for {
					next := int(encoded[position])
					position++
					matchLen += next
					if next != 255 {
						break
					}
				}
			}
			decoded += matchLen
		}
		if lastMatch >= 0 && lastMatch > n-12 {
			t.Fatalf("n=%d: last match starts too late: %d", n, lastMatch)
		}
	}
}

func TestSlicedChain(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 1000)
	chain := splitChain(t, data, 7)
	chain, err := chain.Slice(3, int64(len(data)-11))
	if err != nil {
		t.Fatal(err)
	}
	for _, codec := range codecs {
		checkRoundTrip(t, codec, chain, data[3:len(data)-8])
	}
}

func TestSizeBounds(t *testing.T) {
	for _, divisor := range []uint64{6, 255} {
		for _, test := range []struct {
			dst int
			src int64
		}{
			{0, -1}, {0, int64(math.MaxUint32) + 1},
			{-1, 0}, {math.MaxInt, 0}, {math.MaxInt - 31, 0},
			{math.MaxInt - 64, 100},
		} {
			if _, err := checkSize(test.dst, test.src, divisor); !errors.Is(err, ErrTooLarge) {
				t.Fatalf("checkSize(%d, %d, %d) = %v", test.dst, test.src, divisor, err)
			}
		}
		if _, err := checkSize(0, 1000, divisor); err != nil {
			t.Fatal(err)
		}
	}
	// Repeated borrowed descriptors express an oversized input without a
	// multi-gigabyte payload allocation. Validation precedes indexing/output.
	span := wire.Borrow(make([]byte, 1<<20))
	spans := make([]wire.Span, 4096)
	for i := range spans {
		spans[i] = span
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	for _, codec := range codecs {
		dst := []byte{1, 2, 3}
		got, err := codec.append(dst, chain)
		if !errors.Is(err, ErrTooLarge) || !bytes.Equal(got, dst) || &got[0] != &dst[0] {
			t.Fatalf("%s oversized input: output %x, error %v", codec.name, got, err)
		}
	}
}

func TestConcurrentBorrowedInput(t *testing.T) {
	data := bytes.Repeat([]byte("concurrent borrowed input"), 4096)
	chain := splitChain(t, data, 389)
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			for _, codec := range codecs {
				checkRoundTrip(t, codec, chain, data)
			}
		})
	}
}

func TestPreallocatedOutputAllocations(t *testing.T) {
	// Fixed span count and output capacity isolate input handling allocations.
	// Increasing payload size must not allocate a gather buffer. Code inspection
	// additionally establishes that all source reads are borrowed slice/scalar
	// accesses, and appendRange writes only final encoded literal positions.
	for _, codec := range codecs {
		for _, size := range []int{1 << 10, 1 << 20} {
			for _, spans := range []int{1, 8, 16} {
				data := bytes.Repeat([]byte{'x'}, size)
				chain := splitChain(t, data, size/spans)
				dst := make([]byte, 0, size+size/6+64)
				allocs := testing.AllocsPerRun(10, func() {
					if _, err := codec.append(dst, chain); err != nil {
						panic(err)
					}
				})
				want := 0.0
				if spans > 8 {
					want = 1 // Descriptor index only, independent of byte length.
				}
				if allocs != want {
					t.Fatalf("%s/%d bytes/%d spans allocated %g times, want %g", codec.name, size, spans, allocs, want)
				}
			}
		}
	}
}

func BenchmarkSpanCompression(b *testing.B) {
	for _, codec := range codecs {
		for _, size := range []int{1 << 10, 1 << 20} {
			for _, spans := range []int{1, 16, 1024} {
				for _, random := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/%dB/%dspans/random=%v", codec.name, size, spans, random), func(b *testing.B) {
						data := bytes.Repeat([]byte{'x'}, size)
						if random {
							_, _ = rand.New(rand.NewSource(17)).Read(data)
						}
						chain := splitChain(b, data, size/spans)
						dst := make([]byte, 0, size+size/6+64)
						b.SetBytes(int64(size))
						b.ReportAllocs()
						b.ResetTimer()
						var encoded []byte
						for i := 0; i < b.N; i++ {
							var err error
							if encoded, err = codec.append(dst, chain); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						b.ReportMetric(float64(len(encoded)), "transformed-output-B/op")
						b.ReportMetric(float64(size), "transform-input-B/op")
						b.ReportMetric(float64(len(encoded))/float64(size), "compression-ratio")
					})
				}
			}
		}
	}
}

func FuzzSpanCompression(f *testing.F) {
	f.Add([]byte{}, uint16(1))
	f.Add([]byte("borrowed spans borrowed spans borrowed spans"), uint16(3))
	f.Add(bytes.Repeat([]byte{'x'}, 65537), uint16(257))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint16) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		chain := splitChain(t, data, int(chunk)+1)
		for _, codec := range codecs {
			checkRoundTrip(t, codec, chain, data)
		}
	})
}
