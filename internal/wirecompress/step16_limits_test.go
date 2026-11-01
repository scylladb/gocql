// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestStep16ConfiguredFrameDecodeBounds(t *testing.T) {
	for _, compressor := range []interface {
		Name() string
		Encode([]byte) ([]byte, error)
		DecodedLen([]byte) (int, error)
	}{LZ4Compressor{}, SnappyCompressor{}} {
		t.Run(compressor.Name(), func(t *testing.T) {
			decoder, ok := FrameDecoderWithLimit(compressor, math.MaxInt32)
			if !ok {
				t.Fatal("missing bounded decoder")
			}
			for _, length := range []uint64{256<<20 + 1, 512 << 20, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32} {
				var encoded []byte
				if compressor.Name() == "lz4" {
					encoded = binary.BigEndian.AppendUint32(nil, uint32(length))
				} else {
					encoded = binary.AppendUvarint(nil, length)
				}
				n, err := decoder.DecodedLen(encoded)
				if length <= math.MaxInt32 && (err != nil || uint64(n) != length) {
					t.Fatal(length, n, err)
				}
				if length > math.MaxInt32 && err == nil {
					t.Fatal("invalid signed length accepted", length)
				}
				if _, err := compressor.DecodedLen(encoded); err == nil {
					t.Fatal("standalone default ceiling bypassed")
				}
			}
			input := bytes.Repeat([]byte("bounded transform"), 32)
			encoded, err := compressor.Encode(input)
			if err != nil {
				t.Fatal(err)
			}
			bounded, _ := FrameDecoderWithLimit(compressor, int64(len(input)))
			dst := make([]byte, 0, len(input))
			out, err := bounded.DecodeTo(dst, encoded)
			if err != nil || !bytes.Equal(out, input) || &out[0] != &dst[:cap(dst)][0] {
				t.Fatal("final output/backing", err)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				decoder, _ := FrameDecoderWithLimit(compressor, 512<<20)
				out, err = decoder.DecodeTo(dst, encoded)
			}); allocations != 0 || err != nil {
				t.Fatal("configured decoder wrapper allocated", allocations, err)
			}
			reject, _ := FrameDecoderWithLimit(compressor, int64(len(input)-1))
			if _, err := reject.DecodeTo(dst, encoded); err == nil {
				t.Fatal("output allocated/decoded above configured bound")
			}
		})
	}
}

type step16AliasingDecoder struct{}

func (step16AliasingDecoder) Name() string                        { return "lz4" }
func (step16AliasingDecoder) Decode(input []byte) ([]byte, error) { return input, nil }

func TestStep16CustomFrameDecoderRetainsFallback(t *testing.T) {
	if _, ok := FrameDecoderWithLimit(step16AliasingDecoder{}, math.MaxInt32); ok {
		t.Fatal("custom aliasing decoder treated as a built-in")
	}
}
