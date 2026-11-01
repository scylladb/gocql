// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"errors"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestStep16HandshakeFrameBudgets(t *testing.T) {
	for _, compressor := range []Compressor{nil, wirecompress.LZ4Compressor{}, wirecompress.SnappyCompressor{}} {
		body := bytes.Repeat([]byte{1}, 512)
		data := body
		flags := byte(0)
		if compressor != nil {
			var err error
			data, err = compressor.Encode(body)
			if err != nil {
				t.Fatal(err)
			}
			flags = frm.FlagCompress
		}
		for _, budget := range []int64{511, 512} {
			reader := NewFramer(compressor, 4)
			reader.MaxFrameBytes, reader.MaxDecompressedBytes = 1024, budget
			err := reader.ReadFrame(bytes.NewReader(data), &frm.FrameHeader{Length: len(data), Flags: flags})
			if budget == 511 && err == nil {
				t.Fatal("handshake decode exceeded budget")
			}
			if budget == 512 && (err != nil || !bytes.Equal(reader.Buf, body)) {
				t.Fatal("handshake decode", err)
			}
		}
	}
	for _, budget := range []int64{3, 4} {
		writer := NewFramer(nil, 4)
		writer.MaxFrameBytes = budget
		writer.WriteHeader(0, frm.OpOptions, 0)
		writer.Buf = append(writer.Buf, 1, 2, 3, 4)
		err := writer.Finish()
		if budget == 3 && !errors.Is(err, ErrFrameTooBig) || budget == 4 && err != nil {
			t.Fatal("handshake write bound", budget, err)
		}
	}
}
