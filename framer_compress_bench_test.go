//go:build unit
// +build unit

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package gocql

import (
	"bytes"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// TestFramerFinishCompressEncodeInto verifies that framer.finish() using the
// EncodeInto path (compressorBuf != nil) produces a frame whose compressed body
// decodes back to the original body, and that the framer's compressBuf is
// reused across successive finishes without corrupting output.
func TestFramerFinishCompressEncodeInto(t *testing.T) {
	t.Parallel()

	comp := SnappyCompressor{}
	cb, _ := any(comp).(CompressorWithBuffer)
	if cb == nil {
		t.Fatal("SnappyCompressor does not implement CompressorWithBuffer")
	}

	f := &framer{
		compressor:    comp,
		compressorBuf: cb,
		proto:         protoVersion4,
		flags:         frm.FlagCompress,
	}

	bodies := [][]byte{
		bytes.Repeat([]byte("hello world "), 4),
		bytes.Repeat([]byte("x"), 1),
		bytes.Repeat([]byte("abcdefgh"), 512),
		bytes.Repeat([]byte{0}, 2048),
	}

	for i, body := range bodies {
		f.buf = f.buf[:0]
		f.writeHeader(f.flags, frm.OpQuery, 1)
		f.buf = append(f.buf, body...)
		if err := f.finish(); err != nil {
			t.Fatalf("body %d: finish failed: %v", i, err)
		}
		// Body after the 9-byte header is the compressed payload.
		compressed := f.buf[headSize:]
		decoded, err := comp.Decode(compressed)
		if err != nil {
			t.Fatalf("body %d: decode of finished frame failed: %v", i, err)
		}
		if !bytes.Equal(decoded, body) {
			t.Fatalf("body %d: finish+decode round-trip mismatch (got %d bytes, want %d)", i, len(decoded), len(body))
		}
		// Length field must equal the compressed body length.
		gotLen := int(f.buf[5])<<24 | int(f.buf[6])<<16 | int(f.buf[7])<<8 | int(f.buf[8])
		if gotLen != len(compressed) {
			t.Fatalf("body %d: frame length header %d != compressed len %d", i, gotLen, len(compressed))
		}
	}
}

// BenchmarkFramerFinishCompress measures the write-path finish() compression
// step. With a compressor implementing CompressorWithBuffer (SnappyCompressor),
// the framer reuses compressBuf instead of allocating a new output buffer per
// frame. The "Encode" sub-benchmark forces the legacy Compressor.Encode path
// (compressorBuf == nil) for an apples-to-apples baseline comparison.
func BenchmarkFramerFinishCompress(b *testing.B) {
	body := make([]byte, 512)
	for i := range body {
		body[i] = byte(i % 251)
	}

	comp := SnappyCompressor{}
	cb, _ := any(comp).(CompressorWithBuffer)

	run := func(b *testing.B, f *framer) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.buf = f.buf[:0]
			f.writeHeader(f.flags, frm.OpQuery, 1)
			f.buf = append(f.buf, body...)
			if err := f.finish(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("Encode", func(b *testing.B) {
		// compressorBuf == nil -> legacy Compressor.Encode path (allocates).
		run(b, &framer{compressor: comp, proto: protoVersion4, flags: frm.FlagCompress})
	})

	b.Run("EncodeInto", func(b *testing.B) {
		// compressorBuf != nil -> reuse compressBuf (no per-frame allocation).
		run(b, &framer{compressor: comp, compressorBuf: cb, proto: protoVersion4, flags: frm.FlagCompress})
	})
}
