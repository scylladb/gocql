package gocql

import (
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

var containerIntSink int32
var containerSequenceSink Sequence[int32]

func BenchmarkV2ContainerIteration(b *testing.B) {
	for _, count := range []int{1, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			typ, _ := ListType(rowTestType(b, TypeInt))
			values := make([]int32, count)
			encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), values)
			if err != nil {
				b.Fatal(err)
			}
			sequence, err := DecodeValue(ListDecoder(Int32), encoded)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(count) * 8)
			b.ResetTimer()
			for b.Loop() {
				it := sequence.Iterator()
				for it.Next() {
					containerIntSink = it.Value()
				}
				if it.Err() != nil {
					b.Fatal(it.Err())
				}
			}
		})
	}
}

func BenchmarkV2ContainerSegmentedIteration(b *testing.B) {
	typ, _ := ListType(rowTestType(b, TypeInt))
	encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, 1024))
	if err != nil {
		b.Fatal(err)
	}
	data := encoded.Bytes().OwnedBytes()
	spans := make([]wire.Span, len(data))
	for i := range data {
		spans[i] = wire.Borrow(data[i : i+1])
	}
	chain, _ := wire.NewChain(spans...)
	sequence, err := ListDecoder(Int32).Decode(newValueReader(typ, Present, chain, nil))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		it := sequence.Iterator()
		for it.Next() {
			containerIntSink = it.Value()
		}
		if it.Err() != nil {
			b.Fatal(it.Err())
		}
	}
}

func BenchmarkV2ContainerCompiledDecode(b *testing.B) {
	typ, _ := ListType(rowTestType(b, TypeInt))
	encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, 1024))
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := compileValueDecoder(ListDecoder(Int32), typ)
	if err != nil {
		b.Fatal(err)
	}
	reader := newValueReader(typ, Present, encoded.body.chain, nil)
	b.ReportAllocs()
	b.SetBytes(reader.body.Len())
	b.ResetTimer()
	for b.Loop() {
		sequence, err := decoder.Decode(reader)
		if err != nil {
			b.Fatal(err)
		}
		containerSequenceSink = sequence
	}
}

func BenchmarkV2ContainerTupleIteration(b *testing.B) {
	tuple, _ := TupleType(rowTestType(b, TypeInt))
	typ, _ := ListType(tuple)
	encoded, err := EncodeValueBorrowed(typ, ListEncoder(TupleEncoder(OneParam(Int32))), make([]int32, 1024))
	if err != nil {
		b.Fatal(err)
	}
	sequence, err := DecodeValue(ListDecoder(TupleDecoder(OneColumn(Int32))), encoded)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		it := sequence.Iterator()
		for it.Next() {
			containerIntSink = it.Value()
		}
		if it.Err() != nil {
			b.Fatal(it.Err())
		}
	}
}

func TestV2ContainerIterationAllocationsDoNotScale(t *testing.T) {
	typ, _ := ListType(rowTestType(t, TypeInt))
	allocationCount := func(count int) float64 {
		encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, count))
		if err != nil {
			t.Fatal(err)
		}
		sequence, err := DecodeValue(ListDecoder(Int32), encoded)
		if err != nil {
			t.Fatal(err)
		}
		return testing.AllocsPerRun(50, func() {
			it := sequence.Iterator()
			for it.Next() {
				containerIntSink = it.Value()
			}
			if it.Err() != nil {
				t.Fatal(it.Err())
			}
		})
	}
	small, large := allocationCount(1), allocationCount(1024)
	if large > small {
		t.Fatalf("iteration allocations scale with elements: one=%g, 1024=%g", small, large)
	}
}

func TestV2ContainerSegmentedIterationAllocationsDoNotScale(t *testing.T) {
	typ, _ := ListType(rowTestType(t, TypeInt))
	allocationCount := func(count int) float64 {
		encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, count))
		if err != nil {
			t.Fatal(err)
		}
		data := encoded.Bytes().OwnedBytes()
		spans := make([]wire.Span, len(data))
		for index := range data {
			spans[index] = wire.Borrow(data[index : index+1])
		}
		chain, _ := wire.NewChain(spans...)
		sequence, err := ListDecoder(Int32).Decode(newValueReader(typ, Present, chain, nil))
		if err != nil {
			t.Fatal(err)
		}
		return testing.AllocsPerRun(30, func() {
			it := sequence.Iterator()
			for it.Next() {
				containerIntSink = it.Value()
			}
			if it.Err() != nil {
				t.Fatal(it.Err())
			}
		})
	}
	small, large := allocationCount(1), allocationCount(1024)
	if large > small {
		t.Fatalf("cross-span allocations scale with elements: one=%g, 1024=%g", small, large)
	}
}
