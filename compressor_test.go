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
		if res, err := c.AppendCompressedWithLength(nil, []byte(str)); err != nil {
			t.Fatalf("failed to encode '%v' with error %v", str, err)
		} else if bytes.Compare(expected, res) != 0 {
			t.Fatal("failed to match the expected encoded value with the result encoded value.")
		}

		val, err := c.AppendCompressedWithLength(nil, []byte(str))
		if err != nil {
			t.Fatalf("failed to encode '%v' with error '%v'", str, err)
		}

		//Test Decoding with S2 library, Snappy compatible encoding.
		if expected, err := s2.Decode(nil, val); err != nil {
			t.Fatalf("failed to decode '%v' with error %v", val, err)
		} else if res, err := c.AppendDecompressedWithLength(nil, val); err != nil {
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

					encoded, err := c.AppendCompressedWithLength(nil, frame.Frame)
					if err != nil {
						t.Fatalf("failed to encode frame %s", frame.Name)
					}
					decoded, err := c.AppendDecompressedWithLength(nil, encoded)
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

					decoded, err := c.AppendDecompressedWithLength(nil, frame.Frame)
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

func BenchmarkSnappyCompressor(b *testing.B) {
	c := gocql.SnappyCompressor{}
	b.Run("Decode", func(b *testing.B) {
		for _, frame := range frameExamples.Responses {
			b.Run(frame.Name, func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					_, _ = c.AppendDecompressedWithLength(nil, frame.Frame)
				}
			})
		}
	})

	b.Run("Encode", func(b *testing.B) {
		for _, frame := range frameExamples.Requests {
			b.Run(frame.Name, func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					_, _ = c.AppendCompressedWithLength(nil, frame.Frame)
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
