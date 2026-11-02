// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestStep16StreamingLargeEnvelopeAdmission(t *testing.T) {
	for _, declared := range []uint32{256<<20 + 1, math.MaxInt32} {
		s, conn, peer := step07StreamSession(t, 4, nil)
		conn.InternalCfg.TypedLimits.MaxFrameBytes = int64(declared)
		conn.InternalCfg.TypedLimits.MaxDecompressedBytes = int64(declared)
		done := make(chan struct{})
		go func() {
			defer close(done)
			stream := typedTestReadRequest(t, conn, peer)
			frame := typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "x"))
			binary.BigEndian.PutUint32(frame[5:9], declared)
			// Send only the metadata/row count. Admission must accept the high
			// envelope without gathering or allocating the advertised body.
			if _, err := peer.Write(frame[:len(frame)-5]); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		bound := runtimeQuery(t)
		cursor, err := RunStream(ctx, s, bound, StreamOneColumn(StreamBlob), StreamOptions{Incremental: true})
		if err != nil {
			t.Fatal("configured envelope rejected", declared, err)
		}
		if cursor.Info().Pages != 1 || cursor.Info().OutputStarted {
			t.Fatal("payload read before destination")
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
		bound.Close()
		if err := bound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done
	}
}
