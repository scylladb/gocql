// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type step15Writer interface {
	InternalWriteContext(context.Context, []byte) (int, error)
	InternalSetWriteTimeout(time.Duration)
}
type step15CanonicalWriter struct {
	step15Writer
	payload []byte
	bytes   atomic.Int64
}

func (w *step15CanonicalWriter) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if e := chain.Each(func(s wire.Span) error {
		p := s.Bytes()
		if len(p) == len(w.payload) && bytes.Equal(p, w.payload) {
			if unsafe.SliceData(p) != unsafe.SliceData(w.payload) {
				return fmt.Errorf("canonical payload was copied")
			}
			w.bytes.Add(int64(len(p)))
		}
		return nil
	}); e != nil {
		return 0, e
	}
	return w.step15Writer.(interface {
		InternalWriteSpans(context.Context, wire.Chain) (int64, error)
	}).InternalWriteSpans(ctx, chain)
}
func TestStep15CanonicalAssembly(t *testing.T) {
	for _, size := range []int{0, 32, 4096, 500 << 10} {
		for _, proto := range []byte{3, 4, 5} {
			for _, owned := range []bool{false, true} {
				t.Run(fmt.Sprintf("bytes%d/v%d/owned%t", size, proto, owned), func(t *testing.T) {
					typ := codecTestMust(ScalarType(TypeBlob))
					cols := NewColumns(ColumnSpec{Name: "v", Type: typ})
					calls := 0
					input := bytes.Repeat([]byte{0x71}, size)
					statement := Statement[[]byte, Unit]{Kind: OrdinaryStatement, CQL: "INSERT INTO ks.tbl(v)VALUES(?)", Params: OneParam[[]byte](countingBlobEncoder{calls: &calls}), Rows: NoRows}
					var bound *Bound[Unit]
					if owned {
						bound = codecTestMust(BindUnpreparedOwned(statement, cols, input))
					} else {
						bound = codecTestMust(BindUnpreparedBorrowed(statement, cols, input))
					}
					defer bound.Close()
					canonical := implExecution.BoundData(bound).InternalRequest.InternalValues
					c := &driverConn{InternalVersion: proto}
					c.InternalInitFramerCache()
					for _, batch := range []bool{false, true} {
						for attempt := 0; attempt < 4; attempt++ {
							request := &attemptRequest{Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1, Consistency: One}
							if batch {
								request.Batch = &typedBatchRequest{Kind: LoggedBatch, Items: []typedBatchItem{{Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1}, {Statement: statement.CQL, Values: canonical.Framed, ValueCount: 1}}}
							}
							frame := codecTestMust(c.InternalBuildTypedRequest(request, attempt))
							retained := int64(0)
							frame.Each(func(s wire.Span) error {
								p := s.Bytes()
								if len(p) == 0 {
									return nil
								}
								start := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
								canonical.Cells[0].Body.Each(func(original wire.Span) error {
									o := original.Bytes()
									if len(o) > 0 {
										lo := uintptr(unsafe.Pointer(unsafe.SliceData(o)))
										if start >= lo && start+uintptr(len(p)) <= lo+uintptr(len(o)) {
											retained += int64(len(p))
										}
									}
									return nil
								})
								return nil
							})
							want := int64(size)
							if batch {
								want *= 2
							}
							if retained != want {
								t.Fatalf("attempt=%d batch=%t canonical bytes=%d want=%d", attempt, batch, retained, want)
							}
						}
					}
					if calls != 1 {
						t.Fatal("secondary encode", calls)
					}
					t.Logf("encoder_calls=1 attempt_envelopes=8 payload_backing_reused bytes=%d", size)
				})
			}
		}
	}
}
