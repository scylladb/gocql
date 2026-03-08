//go:build unit
// +build unit

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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// benchLogger discards output to avoid skewing benchmark results.
var benchLogger = log.New(io.Discard, "", 0)

func TestFuzzBugs(t *testing.T) {
	t.Parallel()

	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := [][]byte{
		[]byte("00000\xa0000"),
		[]byte("\x8000\x0e\x00\x00\x00\x000"),
		[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
		[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
		[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
			"\x00\n0000000000\x00\x14000000" +
			"00000000000000\x00\x020000" +
			"\x00\a000000000\x00\x050000000" +
			"\xff0000000000000000000" +
			"0000000"),
		[]byte("\x82\xe600\x00\x00\x00\x000"),
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.Version))
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	t.Parallel()

	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 3)

	framer.writeHeader(0, frm.OpStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	t.Parallel()

	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{0x03, 0x00, 0x00, 0x00, byte(frm.OpReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 3)

	head := frm.FrameHeader{
		Version: protoVersion3,
		Op:      frm.OpReady,
		Length:  r.Len() - 9,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, 9))
	if err != nil {
		t.Fatal(err)
	}
	if head.Op != frm.OpReady {
		t.Fatalf("expected to get header %v got %v", frm.OpReady, head.Op)
	}
}

func TestParseEventFrame_ClientRoutesChanged(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	fr.header = &frm.FrameHeader{Version: protoVersion4}
	fr.writeString("CLIENT_ROUTES_CHANGE")
	fr.writeString("UPDATED")
	fr.writeStringList([]string{"c1", ""})
	fr.writeStringList([]string{})

	frame := fr.parseEventFrame()
	evt, ok := frame.(*frm.ClientRoutesChanged)
	if !ok {
		t.Fatalf("expected ClientRoutesChanged frame, got %T", frame)
	}
	if evt.ChangeType != "UPDATED" {
		t.Fatalf("ChangeType = %v, want UPDATED", evt.ChangeType)
	}
	if len(evt.ConnectionIDs) != 2 || evt.ConnectionIDs[1] != "" {
		t.Fatalf("ConnectionIDs = %v, want [c1 \"\"]", evt.ConnectionIDs)
	}
	if len(evt.HostIDs) != 0 {
		t.Fatalf("HostIDs = %v, want empty", evt.HostIDs)
	}
}

func TestFramerResetClearsAllFields(t *testing.T) {
	t.Parallel()

	f := newFramer(nil, protoVersion4)
	// Populate every mutable field
	f.header = &frm.FrameHeader{Version: protoVersion4}
	f.customPayload = map[string][]byte{"key": {1, 2, 3}}
	f.traceID = make([]byte, 16)
	f.flagLWT = 42
	f.rateLimitingErrorCode = 7
	f.tabletsRoutingV1 = true
	f.proto = protoVersion4
	f.flags = frm.FlagTracing | frm.FlagCompress

	// Grow buf beyond readBuffer to simulate a serialized frame
	f.writeHeader(f.flags, frm.OpQuery, 1)
	f.writeBytes(make([]byte, 512))

	f.reset()

	if f.compres != nil {
		t.Error("compres should be nil after reset")
	}
	if f.header != nil {
		t.Error("header should be nil after reset")
	}
	if f.customPayload != nil {
		t.Error("customPayload should be nil after reset")
	}
	if f.traceID != nil {
		t.Error("traceID should be nil after reset")
	}
	if f.flagLWT != 0 {
		t.Errorf("flagLWT should be 0 after reset, got %d", f.flagLWT)
	}
	if f.rateLimitingErrorCode != 0 {
		t.Errorf("rateLimitingErrorCode should be 0 after reset, got %d", f.rateLimitingErrorCode)
	}
	if f.proto != 0 {
		t.Errorf("proto should be 0 after reset, got %d", f.proto)
	}
	if f.flags != 0 {
		t.Errorf("flags should be 0 after reset, got %x", f.flags)
	}
	if f.tabletsRoutingV1 {
		t.Error("tabletsRoutingV1 should be false after reset")
	}
	if len(f.buf) != 0 {
		t.Errorf("buf should have length 0 after reset, got %d", len(f.buf))
	}
	if f.readBuffer == nil {
		t.Error("readBuffer should not be nil after reset")
	}
}

func TestFramerResetPreservesNormalBuffer(t *testing.T) {
	t.Parallel()

	f := newFramer(nil, protoVersion4)
	// Grow the buffer to something larger than default but under the cap
	f.writeHeader(0, frm.OpQuery, 1)
	f.writeBytes(make([]byte, 4096))

	grownCap := cap(f.buf)
	if grownCap <= defaultBufSize {
		t.Fatalf("expected buf to have grown beyond %d, got cap=%d", defaultBufSize, grownCap)
	}

	f.reset()

	// readBuffer should retain the grown capacity
	if cap(f.readBuffer) != grownCap {
		t.Errorf("expected readBuffer cap to be preserved at %d, got %d", grownCap, cap(f.readBuffer))
	}
	// buf should point to readBuffer
	if cap(f.buf) != cap(f.readBuffer) {
		t.Errorf("buf and readBuffer should share backing array after reset")
	}
}

func TestFramerResetDiscardsOversizedBuffer(t *testing.T) {
	t.Parallel()

	f := newFramer(nil, protoVersion4)
	// Replace readBuffer with an oversized one
	oversized := make([]byte, maxPooledBufSize+1)
	f.readBuffer = oversized
	f.buf = oversized[:0]

	f.reset()

	if cap(f.readBuffer) > maxPooledBufSize {
		t.Errorf("expected readBuffer to be replaced (cap=%d), got cap=%d", defaultBufSize, cap(f.readBuffer))
	}
	if cap(f.readBuffer) != defaultBufSize {
		t.Errorf("expected readBuffer cap=%d after discard, got %d", defaultBufSize, cap(f.readBuffer))
	}
}

func TestGetPutWriteFramerRoundTrip(t *testing.T) {
	t.Parallel()

	logger := log.New(os.Stderr, "", log.LstdFlags)

	f := getWriteFramer(nil, protoVersion4, nil, logger)
	if f == nil {
		t.Fatal("getWriteFramer returned nil")
	}
	if f.proto != protoVersion4&protoVersionMask {
		t.Errorf("expected proto=%d, got %d", protoVersion4&protoVersionMask, f.proto)
	}

	// Use it: write a header and some data
	f.writeHeader(f.flags, frm.OpQuery, 1)
	f.writeBytes(make([]byte, 256))

	grownCap := cap(f.buf)

	// Return to pool
	putWriteFramer(f)

	// Get another framer — should reuse the pooled one (or get a new one; either is valid)
	f2 := getWriteFramer(nil, protoVersion3, nil, logger)
	if f2 == nil {
		t.Fatal("second getWriteFramer returned nil")
	}
	if f2.proto != protoVersion3&protoVersionMask {
		t.Errorf("expected proto=%d, got %d", protoVersion3&protoVersionMask, f2.proto)
	}
	if len(f2.buf) != 0 {
		t.Errorf("expected buf to be empty, got len=%d", len(f2.buf))
	}

	// If we got the same framer back, the buffer should retain its capacity
	if f == f2 && cap(f2.readBuffer) != grownCap {
		t.Errorf("expected reused framer to retain buffer capacity %d, got %d", grownCap, cap(f2.readBuffer))
	}

	putWriteFramer(f2)
}

func TestWriteFramerPoolConcurrency(t *testing.T) {
	t.Parallel()

	logger := log.New(os.Stderr, "", log.LstdFlags)

	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				f := getWriteFramer(nil, protoVersion4, nil, logger)

				// Simulate building a frame
				f.writeHeader(f.flags, frm.OpQuery, 1)
				f.writeBytes(make([]byte, 64))

				// Verify basic invariants before returning
				if f.proto != protoVersion4&protoVersionMask {
					t.Errorf("unexpected proto version: %d", f.proto)
				}
				if len(f.buf) == 0 {
					t.Error("buf should not be empty after writing")
				}

				putWriteFramer(f)
			}
		}()
	}

	wg.Wait()
}

func TestWriteFramerBuiltFrameIntegrity(t *testing.T) {
	t.Parallel()

	logger := log.New(os.Stderr, "", log.LstdFlags)

	// Get a framer, build a real frame, verify bytes match what newFramerWithExts produces
	pooled := getWriteFramer(nil, protoVersion4, nil, logger)
	pooled.writeHeader(pooled.flags, frm.OpQuery, 5)
	pooled.writeString("SELECT 1")
	if err := pooled.finish(); err != nil {
		t.Fatal(err)
	}
	pooledBuf := make([]byte, len(pooled.buf))
	copy(pooledBuf, pooled.buf)
	putWriteFramer(pooled)

	fresh := newFramerWithExts(nil, protoVersion4, nil, logger)
	fresh.writeHeader(fresh.flags, frm.OpQuery, 5)
	fresh.writeString("SELECT 1")
	if err := fresh.finish(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(pooledBuf, fresh.buf) {
		t.Errorf("pooled framer produced different bytes than fresh framer:\npooled: %x\nfresh:  %x", pooledBuf, fresh.buf)
	}
}

// buildTypicalFrame simulates building a typical CQL query frame.
func buildTypicalFrame(f *framer) {
	f.writeHeader(f.flags, frm.OpQuery, 1)
	f.writeString("SELECT key, value FROM my_keyspace.my_table WHERE key = ?")
	// Simulate query parameters (consistency + values)
	f.writeShort(0x0001) // ONE consistency
	f.writeByte(0x01)    // flags: values present
	f.writeShort(1)      // 1 value
	f.writeBytes([]byte("some-partition-key-value-here"))
	_ = f.finish()
}

// BenchmarkFramerNewAlloc benchmarks the old path: allocate a new framer per frame.
func BenchmarkFramerNewAlloc(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		f := newFramerWithExts(nil, protoVersion4, nil, benchLogger)
		buildTypicalFrame(f)
		// Frame is written to wire; framer becomes garbage
	}
}

// BenchmarkFramerPooled benchmarks the new path: get from pool, use, return.
func BenchmarkFramerPooled(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		f := getWriteFramer(nil, protoVersion4, nil, benchLogger)
		buildTypicalFrame(f)
		putWriteFramer(f)
	}
}

// BenchmarkFramerNewAllocParallel benchmarks concurrent new-alloc framers.
func BenchmarkFramerNewAllocParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f := newFramerWithExts(nil, protoVersion4, nil, benchLogger)
			buildTypicalFrame(f)
		}
	})
}

// BenchmarkFramerPooledParallel benchmarks concurrent pooled framers.
func BenchmarkFramerPooledParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f := getWriteFramer(nil, protoVersion4, nil, benchLogger)
			buildTypicalFrame(f)
			putWriteFramer(f)
		}
	})
}
