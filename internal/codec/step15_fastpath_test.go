// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStep15ContiguousScalarSafety(t *testing.T) {
	uuidType := CodecTestMust(cqltype.ScalarType(cqltype.TypeUUID))
	timeType := CodecTestMust(cqltype.ScalarType(cqltype.TypeTimeUUID))
	textType := CodecTestMust(cqltype.ScalarType(cqltype.TypeText))
	data := []byte{0, 1, 2, 3, 4, 5, 0x10, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	for split := 0; split <= len(data); split++ {
		chain := CodecTestMust(wire.NewChain(wire.Borrow(data[:split]), wire.Borrow(data[split:])))
		lease := wire.NewLease(chain, nil)
		r := NewValueReader(uuidType, cqltype.Present, chain, lease)
		got := CodecTestMust(UUIDCodec.Decode(r))
		if !bytes.Equal(got[:], data) {
			t.Fatal("UUID result")
		}
		if _, e := TimeUUIDCodec.Decode(NewValueReader(timeType, cqltype.Present, chain, lease)); e != nil {
			t.Fatal(e)
		}
		lease.Release()
		if _, e := UUIDCodec.Decode(r); !errors.Is(e, cqltype.ErrReleased) {
			t.Fatal("expired UUID backing accessed", e)
		}
	}
	for width := 0; width <= 17; width++ {
		if width == 16 {
			continue
		}
		raw := make([]byte, width)
		if _, e := UUIDCodec.Decode(NewValueReader(uuidType, cqltype.Present, CodecTestMust(wire.NewChain(wire.Borrow(raw))), nil)); e == nil {
			t.Fatal("UUID width", width)
		}
	}
	for _, state := range []cqltype.CellState{cqltype.Null, cqltype.Unset, cqltype.Empty} {
		if _, e := UUIDCodec.Decode(NewValueReader(uuidType, state, wire.Chain{}, nil)); e == nil {
			t.Fatal("UUID state", state)
		}
	}
	if _, e := UUIDCodec.Decode(NewValueReader(textType, cqltype.Present, CodecTestMust(wire.NewChain(wire.Borrow(data))), nil)); e == nil {
		t.Fatal("wrong UUID metadata")
	}
	invalidVersion := append([]byte(nil), data...)
	invalidVersion[6] = 0x40
	if _, e := TimeUUIDCodec.Decode(NewValueReader(timeType, cqltype.Present, CodecTestMust(wire.NewChain(wire.Borrow(invalidVersion))), nil)); e == nil {
		t.Fatal("TimeUUID version bypass")
	}
	for _, input := range [][]byte{[]byte("valid-🙂"), {0xc0, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xe2, 0x82}} {
		for split := 0; split <= len(input); split++ {
			chain := CodecTestMust(wire.NewChain(wire.Borrow(input[:split]), wire.Borrow(input[split:])))
			lease := wire.NewLease(chain, nil)
			r := NewValueReader(textType, cqltype.Present, chain, lease)
			v, e := TextCodec.Decode(r)
			if input[0] == 'v' {
				if e != nil {
					t.Fatal(e)
				}
				cqltype.EachBytes(v.Bytes(), func(s wire.Span) error {
					p := s.Bytes()
					match := false
					chain.Each(func(original wire.Span) error {
						if len(p) > 0 && len(original.Bytes()) > 0 && &p[0] == &original.Bytes()[0] {
							match = true
						}
						return nil
					})
					if !match {
						t.Fatal("text flattened")
					}
					return nil
				})
			} else if e == nil {
				t.Fatalf("invalid UTF8 split%d", split)
			}
			lease.Release()
			if _, e := TextCodec.Decode(r); !errors.Is(e, cqltype.ErrReleased) {
				t.Fatal("expired text", e)
			}
		}
	}
	for _, duration := range []cqltype.Duration{{}, {Months: math.MaxInt32, Days: math.MaxInt32, Nanoseconds: math.MaxInt64}, {Months: math.MinInt32, Days: math.MinInt32, Nanoseconds: math.MinInt64}} {
		typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeDuration))
		encoded := CodecTestMust(EncodeValueOwned(typ, DurationCodec, duration))
		if got := CodecTestMust(DecodeValue(DurationCodec, encoded)); got != duration {
			t.Fatal(got, duration)
		}
	}
	typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeInet))
	for _, address := range []string{"127.0.0.1", "2001:db8::1"} {
		v := netip.MustParseAddr(address)
		encoded := CodecTestMust(EncodeValueOwned(typ, InetCodec, v))
		if got := CodecTestMust(DecodeValue(InetCodec, encoded)); got != v {
			t.Fatal(got, v)
		}
	}
}

func TestStep15DynamicCopiedRowsIndexAndClone(t *testing.T) {
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	specs := make([]cqltype.ColumnSpec, 128)
	values := make([]Value, 128)
	for i := range specs {
		specs[i] = cqltype.ColumnSpec{Name: fmt.Sprint("v", i), Type: blob}
		values[i] = BlobValue([]byte{byte(i)})
	}
	cols := cqltype.NewColumns(specs...)
	chain := dynamicTestRowBody(t, cols, values)
	lease := wire.NewLease(chain, nil)
	plan := CodecTestMust(DynamicRows.Compile(cols))
	row := CodecTestMust(plan.Decode(NewRowReader(cols, chain, lease)))
	copyRow := row
	if row.physical == nil || row.physical != copyRow.physical || row.physical.cells != nil {
		t.Fatal("eager or unshared physical index")
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for j := 0; j < 32; j++ {
				value, e := copyRow.Get("v127")
				if e != nil {
					t.Error(e)
					return
				}
				b, e := value.Bytes()
				if e != nil {
					t.Error(e)
					return
				}
				if got, e := b.At(0); e != nil || got != 127 {
					t.Error(got, e)
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	if len(row.physical.cells) != 128 {
		t.Fatal("index not published once")
	}
	clone := row.Clone()
	if clone.physical == row.physical || clone.physical.cells != nil {
		t.Fatal("Clone retained mutable row index")
	}
	lease.Release()
	if _, e := row.At(127); !errors.Is(e, cqltype.ErrReleased) {
		t.Fatal("cached index bypassed lease", e)
	}
	if _, e := copyRow.At(127); !errors.Is(e, cqltype.ErrReleased) {
		t.Fatal("copy cached index bypassed lease", e)
	}
	value := CodecTestMust(clone.Get("v127"))
	owned := CodecTestMust(value.Bytes())
	if got := CodecTestMust(owned.At(0)); got != 127 {
		t.Fatal(got)
	}
}
