package gocql

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func rowTestType(t testing.TB, kind Type) DataType {
	t.Helper()
	v, err := ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func encodeTestParams[P any](t testing.TB, columns Columns, codec ParamEncoder[P], value P) wire.Chain {
	t.Helper()
	plan, err := codec.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	builder := wire.NewBuilder(8)
	w := newParamWriter(builder, columns, true)
	if err := plan.Encode(w, value); err != nil {
		t.Fatal(err)
	}
	if err := finishParamWriter(w); err != nil {
		t.Fatal(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

func TestV2RowsTypedMappingAndDetachedClone(t *testing.T) {
	type input struct {
		Key  int32
		Body []byte
	}
	type output struct {
		Key  int32
		Body Bytes
	}
	columns := NewColumns(
		ColumnSpec{Name: "body", Type: rowTestType(t, TypeBlob)},
		ColumnSpec{Name: "key", Type: rowTestType(t, TypeInt)},
	)
	params := Params(
		Param("key", Int32, func(v input) int32 { return v.Key }),
		Param("body", BytesEncoder, func(v input) []byte { return v.Body }),
	)
	payload := []byte("borrowed payload")
	chain := encodeTestParams(t, columns, params, input{Key: 42, Body: payload})
	lease := wire.NewLease(chain, nil)
	rows := Row(
		Column("key", Int32, func(v *output) *int32 { return &v.Key }),
		Column("body", BlobCodec, func(v *output) *Bytes { return &v.Body }),
	)
	plan, err := rows.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	reader := newRowReader(columns, chain, lease)
	row, err := plan.Decode(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := finishRowReader(reader); err != nil {
		t.Fatal(err)
	}
	if err := reader.cursor.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	if row.Key != 42 || !bytes.Equal(row.Body.OwnedBytes(), payload) {
		t.Fatalf("wrong row: %+v", row)
	}
	clone := plan.Clone(row)
	lease.Release()
	if _, err := row.Body.At(0); !errors.Is(err, ErrReleased) {
		t.Fatalf("expired row: %v", err)
	}
	payload[0] = 'X'
	if got := string(clone.Body.OwnedBytes()); got != "borrowed payload" {
		t.Fatalf("clone retained borrowed input: %q", got)
	}
}

func TestV2RowsRepeatedMarkersAndAmbiguousAliases(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "key", Type: rowTestType(t, TypeInt)}, ColumnSpec{Name: "key", Type: rowTestType(t, TypeInt)})
	chain := encodeTestParams(t, columns, Params(Param("key", Int32, func(v int32) int32 { return v })), int32(9))
	if _, err := Row(Column("key", Int32, func(v *int32) *int32 { return v })).Compile(columns); err == nil {
		t.Fatal("named duplicate aliases must require positional mapping")
	}
	plan, err := PairRows(1, OneColumn(Int32), OneColumn(Int32)).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	result, err := plan.Decode(newRowReader(columns, chain, nil))
	if err != nil || result.First != 9 || result.Second != 9 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := Params(ParamAt(0, Int32, func(v int32) int32 { return v })).Compile(columns); err == nil {
		t.Fatal("unaccounted parameter accepted")
	}
	if _, err := PairRows(-1, OneColumn(Int32), OneColumn(Int32)).Compile(columns); err == nil {
		t.Fatal("negative split accepted")
	}
}

type failingRowEncoder struct{}

func (failingRowEncoder) CheckType(DataType) error { return nil }
func (failingRowEncoder) Encode(w *ValueWriter, value int32) error {
	if err := w.Uint32(uint32(value)); err != nil {
		return err
	}
	return fmt.Errorf("test encoder failure")
}

func TestV2ParamsEncodingFailureRollsBackWholePlan(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "a", Type: rowTestType(t, TypeInt)}, ColumnSpec{Name: "b", Type: rowTestType(t, TypeInt)})
	codec := PairParams(1, OneParam(Int32), OneParam[int32](failingRowEncoder{}))
	plan, err := codec.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	builder := wire.NewBuilder(4)
	if err := builder.Uint8(0xff); err != nil {
		t.Fatal(err)
	}
	w := newParamWriter(builder, columns, true)
	if err := plan.Encode(w, Pair[int32, int32]{First: 3, Second: 4}); err == nil {
		t.Fatal("expected encoder failure")
	}
	if builder.Len() != 1 || w.index != 0 {
		t.Fatalf("partial parameters escaped: %d bytes, %d parameters", builder.Len(), w.index)
	}
}

func TestV2RowTruncatedIgnoredCellIsError(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "unused", Type: rowTestType(t, TypeBlob)})
	plan, err := Row[Unit](IgnoreColumn[Unit]("unused")).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{{0, 0, 0}, {0, 0, 0, 8, 1}, {255, 255, 255, 254}} {
		chain, _ := wire.NewChain(wire.Borrow(malformed))
		if _, err := plan.Decode(newRowReader(columns, chain, nil)); err == nil {
			t.Fatalf("accepted malformed ignored cell: %x", malformed)
		}
	}
}

func TestV2RowCompiledPlanConcurrentAndMetadataSpecific(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "v", Type: rowTestType(t, TypeInt)})
	codec := OneColumn(Int32)
	plan, err := codec.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	changed := NewColumns(ColumnSpec{Name: "v", Type: rowTestType(t, TypeText)})
	if _, err := codec.Compile(changed); err == nil {
		t.Fatal("accepted incompatible changed result type")
	}
	chain := encodeTestParams(t, columns, OneParam(Int32), int32(123))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				value, err := plan.Decode(newRowReader(columns, chain, nil))
				if err != nil || value != 123 {
					t.Errorf("value=%d err=%v", value, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestV2OwnedParametersDetachOnlyBorrowedPayload(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "blob", Type: rowTestType(t, TypeBlob)})
	plan, err := OneParam(BytesEncoder).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("original")
	builder := wire.NewBuilder(8)
	w := newParamWriter(builder, columns, true)
	w.ownBorrowed = true
	if err := plan.Encode(w, payload); err != nil {
		t.Fatal(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	value, err := ReadColumn(newRowReader(columns, chain, nil), BlobCodec)
	if err != nil || string(value.OwnedBytes()) != "original" {
		t.Fatalf("value=%q err=%v", value.OwnedBytes(), err)
	}
}

func BenchmarkV2RowScalarPlan(b *testing.B) {
	columns := NewColumns(ColumnSpec{Name: "v", Type: rowTestType(b, TypeInt)})
	plan, err := OneColumn(Int32).Compile(columns)
	if err != nil {
		b.Fatal(err)
	}
	chain := encodeTestParams(b, columns, OneParam(Int32), int32(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := plan.Decode(newRowReader(columns, chain, nil))
		if err != nil {
			b.Fatal(err)
		}
	}
}
