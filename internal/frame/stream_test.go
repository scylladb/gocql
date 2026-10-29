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

package frame

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func streamHeader(version byte, flags byte, stream int16, op Op, size uint32) []byte {
	b := []byte{version, flags, 0, 0, byte(op)}
	binary.BigEndian.PutUint16(b[2:4], uint16(stream))
	return binary.BigEndian.AppendUint32(b, size)
}

type envelopeSource struct {
	data                []byte
	offset, chunk, gate int
}

func (s *envelopeSource) Read(p []byte) (int, error) {
	if s.offset >= s.gate {
		return 0, errors.New("read beyond supplied first row")
	}
	n := min(len(p), s.chunk, s.gate-s.offset)
	copy(p[:n], s.data[s.offset:s.offset+n])
	s.offset += n
	return n, nil
}
func TestStreamingEnvelopeReturnsBeforeBodyAndNextRow(t *testing.T) {
	// The source declares two cells but only supplies the first until its caller
	// advances the gate. Envelope parsing must not request even that first body.
	body := binary.BigEndian.AppendUint32(nil, 3)
	body = append(body, "one"...)
	body = binary.BigEndian.AppendUint32(body, 3)
	body = append(body, "two"...)
	for _, version := range []byte{3, 4, 5} {
		for chunk := 1; chunk <= 9; chunk++ {
			data := append(streamHeader(version|0x80, 0, 2, OpResult, uint32(len(body))), body...)
			source := &envelopeSource{data: data, chunk: chunk, gate: HeaderSize + 7}
			head, s, e := ReadStreamEnvelope(context.Background(), source, version, wire.StreamLimits{})
			if e != nil || head.Length != len(body) || source.offset != HeaderSize {
				t.Fatalf("header %v bytes=%d: %v", head, source.offset, e)
			}
			var dst [3]byte
			if e = s.Cell(false, func(_ wire.CellState, c *wire.Stream) error { return c.ReadFull(dst[:]) }); e != nil || string(dst[:]) != "one" {
				t.Fatal(e)
			}
			if source.offset != source.gate || s.Remaining() != 7 {
				t.Fatal("prefetched second row")
			}
			source.gate = len(data)
			if e = s.Cell(false, func(_ wire.CellState, c *wire.Stream) error { return c.ReadFull(dst[:]) }); e != nil || string(dst[:]) != "two" {
				t.Fatal(e)
			}
			if e = s.RequireEnd(); e != nil {
				t.Fatal(e)
			}
		}
	}
}
func TestStreamingEnvelopeMalformedHeader(t *testing.T) {
	for _, tc := range []struct {
		name    string
		header  []byte
		version byte
		limits  wire.StreamLimits
		want    error
	}{
		{"request", streamHeader(4, 0, 0, OpResult, 0), 4, wire.StreamLimits{}, nil},
		{"mismatch", streamHeader(0x83, 0, 0, OpResult, 0), 4, wire.StreamLimits{}, nil},
		{"negative", streamHeader(0x84, 0, 0, OpResult, math.MaxUint32), 4, wire.StreamLimits{}, nil},
		{"too-big", streamHeader(0x84, 0, 0, OpResult, 100), 4, wire.StreamLimits{MaxBytes: 99}, wire.ErrLimit},
		{"flags", streamHeader(0x84, 0x80, 0, OpResult, 0), 4, wire.StreamLimits{}, nil},
		{"v3-warning", streamHeader(0x83, FlagWarning, 0, OpResult, 0), 3, wire.StreamLimits{}, nil},
		{"compression", streamHeader(0x84, FlagCompress, 0, OpResult, 0), 4, wire.StreamLimits{}, ErrStreamFrameCompression},
		{"opcode", streamHeader(0x84, 0, 0, 0xff, 0), 4, wire.StreamLimits{}, nil},
		{"negative-stream", streamHeader(0x84, 0, -1, OpResult, 0), 4, wire.StreamLimits{}, nil},
		{"event-stream", streamHeader(0x84, 0, 0, OpEvent, 0), 4, wire.StreamLimits{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.header)
			_, s, e := ReadStreamEnvelope(context.Background(), r, tc.version, tc.limits)
			if e == nil || s != nil {
				t.Fatal("accepted", e)
			}
			if tc.want != nil && !errors.Is(e, tc.want) {
				t.Fatal(e)
			}
			if r.Len() != 0 {
				t.Fatal("unexpected header read")
			}
		})
	}
}
func TestStreamEnvelopeShortAndCanceledHeader(t *testing.T) {
	valid := streamHeader(0x84, 0, -1, OpEvent, 0)
	for cut := 0; cut < HeaderSize; cut++ {
		if _, _, e := ReadStreamEnvelope(context.Background(), bytes.NewReader(valid[:cut]), 4, wire.StreamLimits{}); e == nil {
			t.Fatal("short header accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := bytes.NewReader(valid)
	if _, _, e := ReadStreamEnvelope(ctx, r, 4, wire.StreamLimits{}); !errors.Is(e, context.Canceled) || r.Len() != len(valid) {
		t.Fatal(e)
	}
	if _, _, e := ReadStreamEnvelope(context.Background(), r, 4, wire.StreamLimits{MaxBytes: -1}); !errors.Is(e, wire.ErrLimit) || r.Len() != len(valid) {
		t.Fatal(e)
	}
	if _, s, e := ReadStreamEnvelope(context.Background(), bytes.NewReader(valid), 4, wire.StreamLimits{}); e != nil || s.RequireEnd() != nil {
		t.Fatal(e)
	}
	if _, e := DecodeHeader([]byte{1}); !errors.Is(e, io.ErrUnexpectedEOF) {
		t.Fatal(e)
	}
}
func FuzzStreamEnvelope(f *testing.F) {
	f.Add(streamHeader(0x84, 0, 0, OpResult, 0))
	f.Add(streamHeader(0x85, FlagWarning, -1, OpEvent, 100))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip()
		}
		_, s, e := ReadStreamEnvelope(context.Background(), bytes.NewReader(data), 4, wire.StreamLimits{MaxBytes: 1 << 16})
		if e == nil {
			_ = s.Skip()
		}
	})
}
