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

package segment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type shortUnitReader struct {
	source io.Reader
	chunk  int
	read   int
}

func (r *shortUnitReader) Read(p []byte) (int, error) {
	n, e := r.source.Read(p[:min(len(p), r.chunk)])
	r.read += n
	return n, e
}
func encodedUnit(t testing.TB, payload []byte, compressed, self bool) []byte {
	t.Helper()
	c, _ := wire.NewChain(wire.Borrow(payload))
	encoded, e := EncodeSpans(c, self, compressed)
	if e != nil {
		t.Fatal(e)
	}
	out := make([]byte, encoded.Len())
	if _, e = encoded.CopyTo(out); e != nil {
		t.Fatal(e)
	}
	return out
}
func TestUnitReaderSplitIntegrityAndReuse(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		var comp Compressor
		if compressed {
			comp = independentLZ4{}
		}
		payload := bytes.Repeat([]byte("bounded validated unit "), 1000)
		encoded := encodedUnit(t, payload, compressed, false)
		for chunk := 1; chunk <= 16; chunk++ {
			input := &shortUnitReader{source: bytes.NewReader(bytes.Repeat(encoded, 3)), chunk: chunk}
			r, e := NewUnitReader(context.Background(), input, comp, UnitLimits{})
			if e != nil {
				t.Fatal(e)
			}
			var backing *byte
			for i := 0; i < 3; i++ {
				h, out, e := r.Next()
				if e != nil {
					t.Fatal(e)
				}
				if h.IsSelfContained || !bytes.Equal(out, payload) {
					t.Fatal("bad unit")
				}
				if i == 0 {
					backing = &out[0]
				} else if &out[0] != backing {
					t.Fatal("unit storage was not reused")
				}
				if r.StorageBytes() > DefaultUnitStorageBytes {
					t.Fatal("storage grew", r.StorageBytes())
				}
			}
			r.Close()
			if _, out, e := r.Next(); out != nil || !errors.Is(e, io.ErrClosedPipe) {
				t.Fatal(e)
			}
		}
	}
}
func TestUnitReaderRejectsBeforePayloadRead(t *testing.T) {
	payload := bytes.Repeat([]byte("compression"), 1000)
	for _, compressed := range []bool{false, true} {
		var comp Compressor
		if compressed {
			comp = independentLZ4{}
		}
		encoded := encodedUnit(t, payload, compressed, true)
		for _, limits := range []UnitLimits{{MaxStorageBytes: 16}, {MaxDecodedBytes: 2}} {
			input := &shortUnitReader{source: bytes.NewReader(encoded), chunk: 1}
			r, e := NewUnitReader(context.Background(), input, comp, limits)
			if e != nil {
				t.Fatal(e)
			}
			if _, out, e := r.Next(); out != nil || !errors.Is(e, ErrUnitLimit) {
				t.Fatal(e)
			}
			if input.read != HeaderSize(compressed) || cap(r.storage) != 0 {
				t.Fatal("invalid length read/allocated payload", input.read, cap(r.storage))
			}
		}
	}
}
func TestUnitReaderCorruptionAndTruncation(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		var comp Compressor
		if compressed {
			comp = independentLZ4{}
		}
		encoded := encodedUnit(t, []byte("CRC verified before publication"), compressed, true)
		for at := 0; at < len(encoded); at++ {
			corrupted := bytes.Clone(encoded)
			corrupted[at] ^= 0x80
			r, _ := NewUnitReader(context.Background(), bytes.NewReader(corrupted), comp, UnitLimits{})
			if _, out, e := r.Next(); e == nil || out != nil {
				t.Fatalf("corrupt byte %d accepted", at)
			}
		}
		for cut := 0; cut < len(encoded); cut++ {
			r, _ := NewUnitReader(context.Background(), bytes.NewReader(encoded[:cut]), comp, UnitLimits{})
			if _, out, e := r.Next(); e == nil || out != nil {
				t.Fatalf("cut %d accepted", cut)
			}
		}
	}
}
func TestSegmentPayloadRejectsForgedHeaders(t *testing.T) {
	headers := []Header{{PayloadLen: -1}, {PayloadLen: MaxPayloadSize + 1}, {PayloadLen: 1, UncompressedLen: -1}, {PayloadLen: 1, UncompressedLen: MaxPayloadSize + 1}, {PayloadLen: 1, UncompressedLen: 4}}
	for _, h := range headers {
		input := &shortUnitReader{source: bytes.NewReader(nil), chunk: 1}
		if _, e := ReadOwnedPayload(input, h, nil); !errors.Is(e, ErrInvalidHeader) {
			t.Fatal(h, e)
		}
		if _, e := ReadUncompressedPayload(input, h, new(Scratch)); !errors.Is(e, ErrInvalidHeader) {
			t.Fatal(h, e)
		}
		if _, e := ReadCompressedPayload(input, h, nil, new(Scratch)); !errors.Is(e, ErrInvalidHeader) {
			t.Fatal(h, e)
		}
		if input.read != 0 {
			t.Fatal("forged header read payload")
		}
	}
	if _, e := ReadUncompressedPayload(bytes.NewReader(nil), Header{}, nil); !errors.Is(e, ErrInvalidHeader) {
		t.Fatal(e)
	}
}

type wrongStorageDecoder struct{}

func (wrongStorageDecoder) AppendCompressed(dst, src []byte) ([]byte, error) { panic("unused") }
func (wrongStorageDecoder) AppendDecompressed(dst, src []byte, n uint32) ([]byte, error) {
	out, e := (independentLZ4{}).AppendDecompressed(nil, src, n)
	return out, e
}
func TestUnitReaderDecoderCannotReplaceStorage(t *testing.T) {
	encoded := encodedUnit(t, bytes.Repeat([]byte("x"), 1000), true, true)
	r, _ := NewUnitReader(context.Background(), bytes.NewReader(encoded), wrongStorageDecoder{}, UnitLimits{})
	if _, out, e := r.Next(); out != nil || !errors.Is(e, ErrDecoderStorage) {
		t.Fatal(e)
	}
}
func TestUnitReaderCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	input := &shortUnitReader{source: bytes.NewReader(nil), chunk: 1}
	r, _ := NewUnitReader(ctx, input, nil, UnitLimits{})
	if _, out, e := r.Next(); out != nil || !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if input.read != 0 {
		t.Fatal("read after cancellation")
	}
}
func FuzzValidatedUnits(f *testing.F) {
	f.Add([]byte("split and validate"), uint8(1), false)
	f.Add(bytes.Repeat([]byte("long pattern"), 100), uint8(3), true)
	f.Fuzz(func(t *testing.T, payload []byte, chunk uint8, compressed bool) {
		if len(payload) > MaxPayloadSize {
			t.Skip()
		}
		encoded := encodedUnit(t, payload, compressed, false)
		var comp Compressor
		if compressed {
			comp = independentLZ4{}
		}
		source := &shortUnitReader{source: bytes.NewReader(encoded), chunk: int(chunk) + 1}
		r, _ := NewUnitReader(context.Background(), source, comp, UnitLimits{})
		_, out, e := r.Next()
		if e != nil || !bytes.Equal(out, payload) {
			t.Fatalf("round trip: %v", e)
		}
		if len(encoded) > HeaderSize(compressed)+4 {
			encoded[HeaderSize(compressed)] ^= 1
			r, _ = NewUnitReader(context.Background(), bytes.NewReader(encoded), comp, UnitLimits{})
			if _, out, e := r.Next(); e == nil || out != nil {
				t.Fatal("corrupt payload published")
			}
		}
	})
}

type cyclicUnitReader struct {
	unit   []byte
	offset int
}

func (r *cyclicUnitReader) Read(p []byte) (int, error) {
	if r.offset == len(r.unit) {
		r.offset = 0
	}
	n := copy(p, r.unit[r.offset:])
	r.offset += n
	return n, nil
}
func BenchmarkValidatedUnits(b *testing.B) {
	for _, compressed := range []bool{false, true} {
		for _, size := range []int{32, 4096, MaxPayloadSize} {
			b.Run(fmt.Sprintf("compressed=%v/%d", compressed, size), func(b *testing.B) {
				encoded := encodedUnit(b, bytes.Repeat([]byte("x"), size), compressed, true)
				var comp Compressor
				if compressed {
					comp = independentLZ4{}
				}
				source := &cyclicUnitReader{unit: encoded}
				r, _ := NewUnitReader(context.Background(), source, comp, UnitLimits{})
				if _, _, e := r.Next(); e != nil {
					b.Fatal(e)
				}
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, out, e := r.Next(); e != nil || len(out) != size {
						b.Fatal(e)
					}
				}
			})
		}
	}
}
func TestUnitReaderMaximumDecodedAndMixedUnits(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), MaxPayloadSize)
	compressed := encodedUnit(t, payload, true, false)
	// Independently inspect the declared expansion and require the maximum.
	h, e := ReadHeader(bytes.NewReader(compressed), true)
	if e != nil || h.UncompressedLen != MaxPayloadSize {
		t.Fatal(h, e)
	}
	raw := make([]byte, MaxPayloadSize)
	for i := range raw {
		raw[i] = byte(i*71 + i/257)
	}
	// Force raw storage using the independent contiguous segment codec.
	rawEncoded, e := AppendCompressed(nil, raw, false, storedUnitCodec{})
	if e != nil {
		t.Fatal(e)
	}
	source := bytes.NewReader(append(compressed, rawEncoded...))
	r, _ := NewUnitReader(context.Background(), source, independentLZ4{}, UnitLimits{})
	for _, want := range [][]byte{payload, raw} {
		if _, out, e := r.Next(); e != nil || !bytes.Equal(out, want) {
			t.Fatal(e)
		}
	}
	if r.StorageBytes() != DefaultUnitStorageBytes {
		t.Fatal(r.StorageBytes())
	}
}

type storedUnitCodec struct{}

func (storedUnitCodec) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}
func (storedUnitCodec) AppendDecompressed(dst, src []byte, n uint32) ([]byte, error) { panic("unused") }
