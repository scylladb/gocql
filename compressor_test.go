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

package gocql_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/klauspost/compress/s2"

	"github.com/gocql/gocql"
)

type frameExample struct {
	Name     string
	Frame    []byte
	FilePath string
}

var frameExamples = struct {
	Requests  []frameExample
	Responses []frameExample
}{
	Requests: []frameExample{
		{
			Name:     "Small query request",
			FilePath: "testdata/frames/small_query_request.bin",
		},
		{
			Name:     "Medium query request",
			FilePath: "testdata/frames/medium_query_request.bin",
		},
		{
			Name:     "Big query request",
			FilePath: "testdata/frames/big_query_request.bin",
		},
		{
			Name:     "Prepare statement request",
			FilePath: "testdata/frames/prepare_statement_request.bin",
		},
	},
	Responses: []frameExample{
		{
			Name:     "Small query response",
			FilePath: "testdata/frames/small_query_response.bin",
		},
		{
			Name:     "Medium query response",
			FilePath: "testdata/frames/medium_query_response.bin",
		},
		{
			Name:     "Big query response",
			FilePath: "testdata/frames/big_query_response.bin",
		},
		{
			Name:     "Prepare statement response",
			FilePath: "testdata/frames/prepare_statement_response.bin",
		},
	},
}

func TestSnappyCompressor(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		c := gocql.SnappyCompressor{}
		if c.Name() != "snappy" {
			t.Fatalf("expected name to be 'snappy', got %v", c.Name())
		}

		str := "My Test String"
		//Test Encoding with S2 library, Snappy compatible encoding.
		expected := s2.EncodeSnappy(nil, []byte(str))
		if res, err := c.Encode([]byte(str)); err != nil {
			t.Fatalf("failed to encode '%v' with error %v", str, err)
		} else if bytes.Compare(expected, res) != 0 {
			t.Fatal("failed to match the expected encoded value with the result encoded value.")
		}

		val, err := c.Encode([]byte(str))
		if err != nil {
			t.Fatalf("failed to encode '%v' with error '%v'", str, err)
		}

		//Test Decoding with S2 library, Snappy compatible encoding.
		if expected, err := s2.Decode(nil, val); err != nil {
			t.Fatalf("failed to decode '%v' with error %v", val, err)
		} else if res, err := c.Decode(val); err != nil {
			t.Fatalf("failed to decode '%v' with error %v", val, err)
		} else if bytes.Compare(expected, res) != 0 {
			t.Fatal("failed to match the expected decoded value with the result decoded value.")
		}
	})

	t.Run("frame-examples", func(t *testing.T) {
		c := gocql.SnappyCompressor{}

		t.Run("Encode", func(t *testing.T) {
			for id := range frameExamples.Requests {
				frame := frameExamples.Requests[id]
				t.Run(frame.Name, func(t *testing.T) {
					t.Parallel()

					encoded, err := c.Encode(frame.Frame)
					if err != nil {
						t.Fatalf("failed to encode frame %s", frame.Name)
					}
					decoded, err := c.Decode(encoded)
					if err != nil {
						t.Fatalf("failed to decode frame %s", frame.Name)
					}

					if bytes.Compare(decoded, frame.Frame) != 0 {
						t.Fatalf("failed to match the decoded value with the original value")
					}
					t.Logf("Compression rate %f", float64(len(encoded))/float64(len(frame.Frame)))
				})
			}
		})

		t.Run("Decode", func(t *testing.T) {
			for id := range frameExamples.Responses {
				frame := frameExamples.Responses[id]
				t.Run(frame.Name, func(t *testing.T) {
					t.Parallel()

					decoded, err := c.Decode(frame.Frame)
					if err != nil {
						t.Fatalf("failed to decode frame %s", frame.Name)
					}

					if len(decoded) == 0 {
						t.Fatalf("frame was decoded to empty slice")
					}
				})
			}
		})
	})
}

// TestSnappyCompressorDecodeInto verifies the zero-alloc DecodeInto path:
// it must produce the same output as Decode, correctly reuse a caller-supplied
// buffer when capacity is sufficient, grow it when not, and not corrupt the
// previous result across reuse (the critical buffer-lifetime guarantee).
func TestSnappyCompressorDecodeInto(t *testing.T) {
	t.Parallel()

	c := gocql.SnappyCompressor{}

	t.Run("equivalence-and-reuse", func(t *testing.T) {
		for id := range frameExamples.Responses {
			frame := frameExamples.Responses[id]
			t.Run(frame.Name, func(t *testing.T) {
				want, err := c.Decode(frame.Frame)
				if err != nil {
					t.Fatalf("Decode failed: %v", err)
				}

				// First call with nil dst.
				got, err := c.DecodeInto(frame.Frame, nil)
				if err != nil {
					t.Fatalf("DecodeInto(nil) failed: %v", err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("DecodeInto(nil) mismatch")
				}

				// Reuse the returned buffer; result must match again and the
				// data from the previous call must not be silently aliased into
				// something unexpected (we copy want before reuse).
				prev := append([]byte(nil), got...)
				got2, err := c.DecodeInto(frame.Frame, got[:0])
				if err != nil {
					t.Fatalf("DecodeInto(reuse) failed: %v", err)
				}
				if !bytes.Equal(got2, want) {
					t.Fatalf("DecodeInto(reuse) mismatch")
				}
				if !bytes.Equal(prev, want) {
					t.Fatalf("prior decoded copy was corrupted by reuse")
				}
			})
		}
	})

	t.Run("grows-undersized-buffer", func(t *testing.T) {
		// frameExamples is keyed by name; pick the big response to force growth.
		var big []byte
		for id := range frameExamples.Responses {
			if frameExamples.Responses[id].Name == "Big query response" {
				big = frameExamples.Responses[id].Frame
			}
		}
		if big == nil {
			t.Skip("big query response fixture not available")
		}
		want, err := c.Decode(big)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		// Intentionally tiny dst; DecodeInto must grow/reallocate.
		dst := make([]byte, 0, 1)
		got, err := c.DecodeInto(big, dst)
		if err != nil {
			t.Fatalf("DecodeInto failed: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("DecodeInto with undersized dst produced wrong output")
		}
	})

	t.Run("interface-assertion", func(t *testing.T) {
		// SnappyCompressor must satisfy the optional CompressorWithBuffer
		// interface so the framer's zero-alloc path is exercised.
		var comp gocql.Compressor = gocql.SnappyCompressor{}
		if _, ok := comp.(gocql.CompressorWithBuffer); !ok {
			t.Fatal("SnappyCompressor does not implement CompressorWithBuffer")
		}
	})
}

// TestSnappyCompressorEncodeInto verifies the zero-alloc EncodeInto path: it
// must produce output that round-trips through Decode identically to Encode,
// correctly reuse a caller-supplied buffer when capacity is sufficient, grow it
// when not, and not corrupt the previous result across reuse.
func TestSnappyCompressorEncodeInto(t *testing.T) {
	t.Parallel()

	c := gocql.SnappyCompressor{}

	t.Run("equivalence-and-reuse", func(t *testing.T) {
		var dst []byte // reused across all fixtures
		for id := range frameExamples.Requests {
			frame := frameExamples.Requests[id]
			t.Run(frame.Name, func(t *testing.T) {
				want, err := c.Encode(frame.Frame)
				if err != nil {
					t.Fatalf("Encode failed: %v", err)
				}

				got, err := c.EncodeInto(frame.Frame, dst)
				if err != nil {
					t.Fatalf("EncodeInto failed: %v", err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("EncodeInto output differs from Encode output")
				}

				// Round-trip through Decode.
				dec, err := c.Decode(got)
				if err != nil {
					t.Fatalf("Decode of EncodeInto output failed: %v", err)
				}
				if !bytes.Equal(dec, frame.Frame) {
					t.Fatalf("EncodeInto round-trip mismatch")
				}

				dst = got // reuse next iteration
			})
		}
	})

	t.Run("grows-undersized-buffer", func(t *testing.T) {
		var big []byte
		for id := range frameExamples.Requests {
			if len(frameExamples.Requests[id].Frame) > len(big) {
				big = frameExamples.Requests[id].Frame
			}
		}
		if big == nil {
			t.Skip("no request fixtures available")
		}
		want, err := c.Encode(big)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		// Intentionally tiny dst; EncodeInto must grow/reallocate.
		got, err := c.EncodeInto(big, make([]byte, 0, 1))
		if err != nil {
			t.Fatalf("EncodeInto failed: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("EncodeInto with undersized dst produced wrong output")
		}
	})

	t.Run("interface-assertion", func(t *testing.T) {
		var comp gocql.Compressor = gocql.SnappyCompressor{}
		if _, ok := comp.(gocql.CompressorWithBuffer); !ok {
			t.Fatal("SnappyCompressor does not implement CompressorWithBuffer (EncodeInto)")
		}
	})
}

func BenchmarkSnappyCompressor(b *testing.B) {
	c := gocql.SnappyCompressor{}
	b.Run("Decode", func(b *testing.B) {
		for _, frame := range frameExamples.Responses {
			b.Run(frame.Name, func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					_, _ = c.Decode(frame.Frame)
				}
			})
		}
	})

	b.Run("Encode", func(b *testing.B) {
		for _, frame := range frameExamples.Requests {
			b.Run(frame.Name, func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					_, _ = c.Encode(frame.Frame)
				}
			})
		}
	})
}

func init() {
	var err error
	for id, def := range frameExamples.Requests {
		frameExamples.Requests[id].Frame, err = os.ReadFile(def.FilePath)
		if err != nil {
			panic("can't read file " + def.FilePath)
		}
	}
	for id, def := range frameExamples.Responses {
		frameExamples.Responses[id].Frame, err = os.ReadFile(def.FilePath)
		if err != nil {
			panic("can't read file " + def.FilePath)
		}
	}
}
