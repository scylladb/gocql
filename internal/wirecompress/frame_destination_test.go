// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStep15DecodeToFinalArena(t *testing.T) {
	for _, codec := range []interface {
		Name() string
		Encode([]byte) ([]byte, error)
		Decode([]byte) ([]byte, error)
		DecodedLen([]byte) (int, error)
		DecodeTo([]byte, []byte) ([]byte, error)
	}{LZ4Compressor{}, SnappyCompressor{}} {
		for _, n := range []int{0, 32, 4096, 8 << 20} {
			t.Run(fmt.Sprintf("%s/%d", codec.Name(), n), func(t *testing.T) {
				input := bytes.Repeat([]byte{0x71}, n)
				encoded, e := codec.Encode(input)
				if e != nil {
					t.Fatal(e)
				}
				dst := make([]byte, 0, n)
				out, e := codec.DecodeTo(dst, encoded)
				if e != nil || !bytes.Equal(out, input) {
					t.Fatal(e)
				}
				if n > 0 && &out[0] != &dst[:cap(dst)][0] {
					t.Fatal("decompressed output copied/replaced")
				}
				ordinary, e := codec.Decode(encoded)
				if e != nil || !bytes.Equal(ordinary, out) {
					t.Fatal("ordinary decode drift", e)
				}
				for prefix := 0; prefix < min(16, len(encoded)); prefix++ {
					_, _ = codec.DecodeTo(dst, encoded[:prefix])
				}
			})
		}
	}
	if _, e := (LZ4Compressor{}).DecodeTo(make([]byte, 0, 16), []byte{0x7f, 0xff, 0xff, 0xff}); e == nil {
		t.Fatal("oversized LZ4")
	}
	if _, e := (SnappyCompressor{}).DecodeTo(make([]byte, 0, 16), []byte{0xff, 0xff, 0xff, 0xff, 0x0f}); e == nil {
		t.Fatal("oversized Snappy")
	}
}
