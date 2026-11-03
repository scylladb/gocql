// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
	"io"
	"math"
	"net/netip"
	"reflect"
	"testing"
)

func compareBufferedScalar[T any](t *testing.T, kind cqltype.Type, decoder StreamDecoder[T], bodies [][]byte, seed func() T) {
	t.Helper()
	typ := rowTestType(t, kind)
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	plan := CodecTestMust(StreamOneColumn(decoder).Compile(columns))
	for _, body := range bodies {
		for _, fragment := range []int{1, 3, 4096} {
			frame := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
			frame = append(frame, body...)
			var values [2]T
			var results [2]error
			for mode := 0; mode < 2; mode++ {
				var source io.Reader = bytes.NewReader(frame)
				if mode == 1 {
					var spans []wire.Span
					for offset := 0; offset < len(frame); offset += fragment {
						spans = append(spans, wire.Borrow(frame[offset:min(len(frame), offset+fragment)]))
					}
					chain := CodecTestMust(wire.NewChain(spans...))
					source = wire.NewCursor(chain)
				}
				scope := CodecTestMust(wire.NewStream(context.Background(), source, int64(len(frame)), wire.StreamLimits{}))
				reader := CodecTestMust(NewStreamRowReader(columns, scope))
				values[mode] = seed()
				plan.Reset(&values[mode])
				results[mode] = plan.DecodeInto(reader, &values[mode])
				if results[mode] != nil && scope.Err() == nil {
					t.Fatalf("rejected cell left envelope usable in mode %d: %v", mode, results[mode])
				}
				if results[mode] == nil {
					results[mode] = scope.RequireEnd()
				}
			}
			if (results[0] == nil) != (results[1] == nil) {
				t.Fatalf("%s buffered/generic error differs: %v / %v", kind, results[0], results[1])
			}
			if results[0] != nil {
				var a, b *cqltype.ValidationError
				if errors.As(results[0], &a) != errors.As(results[1], &b) || errors.Is(results[0], io.ErrUnexpectedEOF) != errors.Is(results[1], io.ErrUnexpectedEOF) {
					t.Fatalf("error class changed: %v / %v", results[0], results[1])
				}
			}
			equal := reflect.DeepEqual(values[0], values[1])
			if !equal {
				a, b := reflect.ValueOf(values[0]), reflect.ValueOf(values[1])
				if a.Kind() == reflect.Float32 || a.Kind() == reflect.Float64 {
					equal = math.Float64bits(a.Float()) == math.Float64bits(b.Float())
				}
			}
			if results[0] == nil && !equal {
				t.Fatalf("%s buffered/generic values differ: %#v / %#v", kind, values[0], values[1])
			}
		}
	}
}
func TestBufferedBuiltInScalarPathsMatchIncrementalChecks(t *testing.T) {
	compareBufferedScalar(t, cqltype.TypeBoolean, StreamBool, [][]byte{{0}, {1}, {2}, {}, {1, 0}}, func() bool { return true })
	compareBufferedScalar(t, cqltype.TypeTinyInt, StreamInt8, [][]byte{{0xff}, {0x7f}, {}}, func() int8 { return 17 })
	compareBufferedScalar(t, cqltype.TypeSmallInt, StreamInt16, [][]byte{{0xff, 0xfe}, {0x7f, 0xff}, {0}}, func() int16 { return 17 })
	compareBufferedScalar(t, cqltype.TypeInt, StreamInt32, [][]byte{{0xff, 0xff, 0xff, 0xfe}, {0, 0, 0, 41}, {0}}, func() int32 { return 17 })
	compareBufferedScalar(t, cqltype.TypeBigInt, StreamInt64, [][]byte{{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, {0}}, func() int64 { return 17 })
	compareBufferedScalar(t, cqltype.TypeTimestamp, StreamTimestamp, [][]byte{{0, 0, 0, 0, 0, 0, 0, 42}, {0}}, func() Timestamp { return 17 })
	compareBufferedScalar(t, cqltype.TypeCounter, StreamCounter, [][]byte{{0, 0, 0, 0, 0, 0, 0, 42}, {0}}, func() Counter { return 17 })
	f32 := binary.BigEndian.AppendUint32(nil, math.Float32bits(3.25))
	compareBufferedScalar(t, cqltype.TypeFloat, StreamFloat32, [][]byte{f32, binary.BigEndian.AppendUint32(nil, 0x7fc00001), binary.BigEndian.AppendUint32(nil, 0x80000000), {0}}, func() float32 { return 1 })
	f64 := binary.BigEndian.AppendUint64(nil, math.Float64bits(3.25))
	compareBufferedScalar(t, cqltype.TypeDouble, StreamFloat64, [][]byte{f64, binary.BigEndian.AppendUint64(nil, 0x7ff8000000000001), binary.BigEndian.AppendUint64(nil, 0x8000000000000000), {0}}, func() float64 { return 1 })
	compareBufferedScalar(t, cqltype.TypeDate, StreamDate, [][]byte{{0x80, 0, 0, 0}, {0}}, func() Date { return 17 })
	compareBufferedScalar(t, cqltype.TypeTime, StreamTime, [][]byte{binary.BigEndian.AppendUint64(nil, 42), binary.BigEndian.AppendUint64(nil, ^uint64(0))}, func() TimeOfDay { return 17 })
	uuid := make([]byte, 16)
	uuid[6] = 0x10
	compareBufferedScalar(t, cqltype.TypeUUID, StreamUUID, [][]byte{uuid, {0}}, func() cqltype.UUID { return cqltype.UUID{} })
	compareBufferedScalar(t, cqltype.TypeTimeUUID, StreamTimeUUID, [][]byte{uuid, make([]byte, 16)}, func() TimeUUID { return TimeUUID{} })
	compareBufferedScalar(t, cqltype.TypeDuration, StreamDuration, [][]byte{{0, 0, 0}, {2, 4, 6}, {0xff}, {1, 2, 0}}, func() cqltype.Duration { return cqltype.Duration{} })
	compareBufferedScalar(t, cqltype.TypeInet, StreamInet, [][]byte{{127, 0, 0, 1}, {0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, {1, 2, 3, 4, 5}, {}}, func() netip.Addr { return netip.Addr{} })
	for _, entry := range []struct {
		kind    cqltype.Type
		decoder StreamDecoder[[]byte]
		bodies  [][]byte
	}{{cqltype.TypeBlob, StreamBlob, [][]byte{[]byte("payload"), {}}}, {cqltype.TypeText, StreamText, [][]byte{[]byte("€"), {0xe2, 0x82}}}, {cqltype.TypeAscii, StreamASCII, [][]byte{[]byte("ascii"), {0x80}}}, {cqltype.TypeVarint, StreamVarint, [][]byte{{0xff}, {}}}} {
		compareBufferedScalar(t, entry.kind, entry.decoder, entry.bodies, func() []byte { return make([]byte, 0, 32) })
	}
	compareBufferedScalar(t, cqltype.TypeDecimal, StreamDecimal, [][]byte{{0, 0, 0, 2, 42}, {0, 0, 0, 2}}, func() StreamDecimalValue { return StreamDecimalValue{Unscaled: make([]byte, 0, 32)} })
	compareBufferedScalar(t, cqltype.TypeInt, StreamEmpty(StreamInt32), [][]byte{{}, {0, 0, 0, 4}}, func() cqltype.EmptyValueOf[int32] { return cqltype.EmptyValueOf[int32]{} })
}
func TestBufferedScalarNullBoundsCapacityAndMetadata(t *testing.T) {
	typ := rowTestType(t, cqltype.TypeInt)
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	for _, frame := range [][]byte{{0xff, 0xff, 0xff, 0xff}, {0xff, 0xff, 0xff, 0xfe}, {0, 0, 0, 8, 0, 0, 0, 1}} {
		scope := CodecTestMust(wire.NewStream(context.Background(), wire.NewCursor(FoundationChain(t, frame)), int64(len(frame)), wire.StreamLimits{}))
		reader := CodecTestMust(NewStreamRowReader(columns, scope))
		plan := CodecTestMust(StreamNullable(StreamInt32).Compile(typ))
		var value cqltype.Nullable[int32]
		err := ReadStreamColumn(reader, plan, &value)
		if bytes.Equal(frame, []byte{0xff, 0xff, 0xff, 0xff}) {
			if err != nil || value.Valid {
				t.Fatal(err, value)
			}
		} else if err == nil {
			t.Fatal("accepted malformed buffered cell")
		}
	}
	blob := rowTestType(t, cqltype.TypeBlob)
	columns = cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: blob})
	frame := []byte{0, 0, 0, 4, 1, 2, 3, 4}
	scope := CodecTestMust(wire.NewStream(context.Background(), wire.NewCursor(FoundationChain(t, frame)), 8, wire.StreamLimits{}))
	reader := CodecTestMust(NewStreamRowReader(columns, scope))
	plan := CodecTestMust(StreamBlob.Compile(blob))
	value := make([]byte, 0, 2)
	var capacity *StreamCapacityError
	if err := ReadStreamColumn(reader, plan, &value); !errors.As(err, &capacity) || len(value) != 0 {
		t.Fatal("capacity preflight", err, value)
	}
	if !errors.As(scope.RequireEnd(), &capacity) {
		t.Fatal("capacity failure did not poison the envelope")
	}
	scope = CodecTestMust(wire.NewStream(context.Background(), wire.NewCursor(FoundationChain(t, frame)), 8, wire.StreamLimits{}))
	reader = CodecTestMust(NewStreamRowReader(columns, scope))
	integer := CodecTestMust(StreamInt32.Compile(typ))
	destination := int32(99)
	var mismatch *cqltype.ValidationError
	if err := ReadStreamColumn(reader, integer, &destination); !errors.As(err, &mismatch) || destination != 99 {
		t.Fatal("metadata mismatch changed the destination or escaped validation", err, destination)
	}
	if !errors.As(scope.RequireEnd(), &mismatch) {
		t.Fatal("metadata mismatch left the envelope usable")
	}
}

func TestStreamValidationCacheRequiresIdentityAndLimits(t *testing.T) {
	typ := rowTestType(t, cqltype.TypeInt)
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	newScope := func(limits wire.StreamLimits) *wire.Stream {
		return CodecTestMust(wire.NewStream(context.Background(), wire.NewCursor(wire.Chain{}), 0, limits))
	}
	scope := newScope(wire.StreamLimits{})
	reader := CodecTestMust(NewStreamRowReader(columns, scope))
	cache := CaptureStreamValidation(reader)
	if cache == nil || cache.RetainedBytes() > StreamValidationCacheMaxBytes || !cache.Matches(columns, scope.Limits()) {
		t.Fatal("cache was not bounded and reusable")
	}
	equal := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	if cache.Matches(equal, scope.Limits()) {
		t.Fatal("validation cache accepted a different metadata owner")
	}
	limits := scope.Limits()
	limits.MaxDepth++
	if cache.Matches(columns, limits) {
		t.Fatal("validation cache ignored changed limits")
	}
	next := CodecTestMust(NewStreamRowReaderUsingValidation(columns, newScope(wire.StreamLimits{}), cache))
	if next.shared == reader.shared || &next.validations[0] != &reader.validations[0] {
		t.Fatal("cache reused mutable state or recompiled immutable plans")
	}
	large := make([]cqltype.ColumnSpec, 512)
	for i := range large {
		large[i] = cqltype.ColumnSpec{Name: "value", Type: typ}
	}
	oversize := CodecTestMust(NewStreamRowReader(cqltype.NewColumns(large...), newScope(wire.StreamLimits{})))
	if CaptureStreamValidation(oversize) != nil {
		t.Fatal("oversize metadata retained in cache")
	}
}

type opaqueStreamCacheValue[T any] struct{ StreamValuePlan[T] }
type opaqueStreamCacheDecoder[T any] struct{ StreamDecoder[T] }

func (d opaqueStreamCacheDecoder[T]) Compile(typ cqltype.DataType) (StreamValuePlan[T], error) {
	plan, err := d.StreamDecoder.Compile(typ)
	return opaqueStreamCacheValue[T]{plan}, err
}

func TestOnlyBoundedBuiltInStreamPlansAreCached(t *testing.T) {
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, cqltype.TypeInt)})
	builtin := CodecTestMust(StreamOneColumn(StreamInt32).Compile(columns))
	if !CacheableStreamRowPlan(builtin) {
		t.Fatal("built-in scalar plan was not cacheable")
	}
	opaque := CodecTestMust(StreamOneColumn[int32](opaqueStreamCacheDecoder[int32]{StreamInt32}).Compile(columns))
	if CacheableStreamRowPlan(opaque) {
		t.Fatal("opaque custom value plan entered the bounded cache")
	}
	type row struct{ Value int32 }
	for index, decoder := range []StreamDecoder[int32]{StreamInt32, opaqueStreamCacheDecoder[int32]{StreamInt32}} {
		plan := CodecTestMust(StreamRow(func(*row) {}, StreamColumn("value", decoder, func(v *row) *int32 { return &v.Value })).Compile(columns))
		if CacheableStreamRowPlan(plan) != (index == 0) {
			t.Fatal("field plan cacheability did not follow its value codec")
		}
	}
}
