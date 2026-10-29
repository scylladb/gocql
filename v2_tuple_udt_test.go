package gocql

import (
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2TuplePhysicalFramingAndNullableSuffix(t *testing.T) {
	type input struct {
		ID   int32
		Name Nullable[string]
	}
	type output struct {
		ID   int32
		Name Nullable[Text]
	}
	integer, text := rowTestType(t, TypeInt), rowTestType(t, TypeText)
	tuple, _ := TupleType(integer, text)
	encoder := TupleEncoder(Params(
		ParamAt(0, Int32, func(v input) int32 { return v.ID }),
		ParamAt(1, NullableEncoder(StringEncoder), func(v input) Nullable[string] { return v.Name }),
	))
	decoder := TupleDecoder(Row(
		ColumnAt(0, Int32, func(v *output) *int32 { return &v.ID }),
		ColumnAt(1, NullableDecoder(TextCodec), func(v *output) *Nullable[Text] { return &v.Name }),
	))
	encoded := containerGolden(t, tuple, encoder, input{ID: 7}, "0000000400000007ffffffff")
	decoded, err := DecodeValue(decoder, encoded)
	if err != nil || decoded.ID != 7 || decoded.Name.Valid {
		t.Fatalf("tuple=%+v err=%v", decoded, err)
	}
	short, _ := hex.DecodeString("0000000400000007")
	decoded, err = DecodeValue(decoder, containerValue(t, tuple, short))
	if err != nil || decoded.ID != 7 || decoded.Name.Valid {
		t.Fatalf("short tuple=%+v err=%v", decoded, err)
	}
	nonnullable := TupleDecoder(PairRows(1, OneColumn(Int32), OneColumn(TextCodec)))
	if _, err := DecodeValue(nonnullable, containerValue(t, tuple, short)); err == nil {
		t.Fatal("absent suffix became nonnullable zero value")
	}
	for _, bad := range []string{"000000040000000700", "0000000400000007fffffffe", "0000000400000007ffffffff00"} {
		body, _ := hex.DecodeString(bad)
		if _, err := DecodeValue(decoder, containerValue(t, tuple, body)); err == nil {
			t.Fatalf("malformed tuple accepted: %s", bad)
		}
	}
	if err := TupleDecoder(OneColumn(Int32)).CheckType(tuple); err == nil {
		t.Fatal("tuple arity mismatch accepted")
	}
	list, _ := ListType(tuple)
	listEncoded := containerGolden(t, list, ListEncoder(encoder), []input{{ID: 7}}, "000000010000000c0000000400000007ffffffff")
	sequence, err := DecodeValue(ListDecoder(decoder), listEncoded)
	if err != nil {
		t.Fatal(err)
	}
	it := sequence.Iterator()
	if !it.Next() || it.Value().ID != 7 || it.Value().Name.Valid {
		t.Fatalf("nested tuple: %v", it.Err())
	}
	// The tuple stays one column even when adjacent to an ordinary scalar.
	columns := NewColumns(ColumnSpec{Name: "record", Type: tuple}, ColumnSpec{Name: "tail", Type: integer})
	chain := encodeTestParams(t, columns, PairParams(1, OneParam(encoder), OneParam(Int32)), Pair[input, int32]{First: input{ID: 7}, Second: 9})
	plan, err := PairRows(1, OneColumn(decoder), OneColumn(Int32)).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	row, err := plan.Decode(newRowReader(columns, chain, nil))
	if err != nil || row.First.ID != 7 || row.Second != 9 {
		t.Fatalf("physical tuple row=%+v err=%v", row, err)
	}
}

func TestV2UDTNamedEvolutionOptions(t *testing.T) {
	type record struct {
		ID      int32
		Body    Text
		Missing int32
	}
	integer, text := rowTestType(t, TypeInt), rowTestType(t, TypeText)
	typ, _ := UDTType("ks", "record", ColumnSpec{Name: "body", Type: text}, ColumnSpec{Name: "extra", Type: integer}, ColumnSpec{Name: "id", Type: integer})
	params := Params(
		Param("id", Int32, func(v record) int32 { return v.ID }),
		Param("body", TextCodec, func(v record) Text { return v.Body }),
	)
	rows := Row(
		Column("id", Int32, func(v *record) *int32 { return &v.ID }),
		Column("body", TextCodec, func(v *record) *Text { return &v.Body }),
	)
	if err := UDTEncoder(params, UDTOptions{Keyspace: "ks", Name: "record"}).CheckType(typ); err == nil {
		t.Fatal("extra database field accepted without option")
	}
	if err := UDTDecoder(rows, UDTOptions{Keyspace: "ks", Name: "record"}).CheckType(typ); err == nil {
		t.Fatal("extra database field accepted without option")
	}
	options := UDTOptions{Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true}
	encoder, decoder := UDTEncoder(params, options), UDTDecoder(rows, options)
	encoded := containerGolden(t, typ, encoder, record{ID: 7, Body: codecTestMust(BorrowText("x"))}, "0000000178ffffffff0000000400000007")
	decoded, err := DecodeValue(decoder, encoded)
	if err != nil || decoded.ID != 7 || decoded.Body.String() != "x" {
		t.Fatalf("UDT=%+v err=%v", decoded, err)
	}
	missingRows := Row(
		Column("id", Int32, func(v *record) *int32 { return &v.ID }),
		Column("body", TextCodec, func(v *record) *Text { return &v.Body }),
		Column("gone", Int32, func(v *record) *int32 { return &v.Missing }),
	)
	if err := UDTDecoder(missingRows, options).CheckType(typ); err == nil {
		t.Fatal("unknown application field accepted")
	}
	options.AllowMissingFields = true
	decoded, err = DecodeValue(UDTDecoder(missingRows, options), encoded)
	if err != nil || decoded.Missing != 0 || decoded.ID != 7 {
		t.Fatalf("missing application field=%+v err=%v", decoded, err)
	}
	missingParams := Params(
		Param("id", Int32, func(v record) int32 { return v.ID }),
		Param("body", TextCodec, func(v record) Text { return v.Body }),
		Param("gone", Int32, func(v record) int32 { return v.Missing }),
	)
	containerGolden(t, typ, UDTEncoder(missingParams, options), record{ID: 7, Body: codecTestMust(BorrowText("x")), Missing: 42}, "0000000178ffffffff0000000400000007")
	// The compiled identity includes the keyspace and UDT name, not just fields.
	bound, err := compileValueDecoder(decoder, typ)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := UDTType("other", "record", typ.Fields().columns...)
	if _, err := bound.Decode(newValueReader(other, Present, encoded.body.chain, nil)); err == nil {
		t.Fatal("compiled UDT accepted different identity")
	}
}

func TestV2UDTMissingMetadataDiffersFromNull(t *testing.T) {
	type record struct {
		ID   int32
		Body Text
	}
	typ, _ := UDTType("ks", "record", ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)}, ColumnSpec{Name: "body", Type: rowTestType(t, TypeText)})
	rows := Row(
		Column("id", Int32, func(v *record) *int32 { return &v.ID }),
		Column("body", TextCodec, func(v *record) *Text { return &v.Body }),
	)
	short, _ := hex.DecodeString("0000000400000007")
	for _, options := range []UDTOptions{{Keyspace: "ks", Name: "record"}, {Keyspace: "ks", Name: "record", AllowMissingFields: true}, {Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true, AllowMissingFields: true}} {
		if _, err := DecodeValue(UDTDecoder(rows, options), containerValue(t, typ, short)); err == nil {
			t.Fatal("missing serialized field became zero through AllowMissingFields")
		}
	}
	type nullableRecord struct {
		ID   int32
		Body Nullable[Text]
	}
	nullableRows := Row(
		Column("id", Int32, func(v *nullableRecord) *int32 { return &v.ID }),
		Column("body", NullableDecoder(TextCodec), func(v *nullableRecord) *Nullable[Text] { return &v.Body }),
	)
	got, err := DecodeValue(UDTDecoder(nullableRows, UDTOptions{Keyspace: "ks", Name: "record"}), containerValue(t, typ, short))
	if err != nil || got.ID != 7 || got.Body.Valid {
		t.Fatalf("nullable suffix=%+v err=%v", got, err)
	}
}

func TestV2UDTIgnoredSuffixStillValidatesFraming(t *testing.T) {
	typ, _ := UDTType("ks", "record", ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)}, ColumnSpec{Name: "ignored", Type: rowTestType(t, TypeBlob)})
	decoder := UDTDecoder(Row(Column("id", Int32, func(v *int32) *int32 { return v })), UDTOptions{Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true})
	for _, bad := range []string{"000000040000000700", "00000004000000070000000401", "0000000400000007fffffffe", "0000000400000007ffffffff00"} {
		body, _ := hex.DecodeString(bad)
		if _, err := DecodeValue(decoder, containerValue(t, typ, body)); err == nil {
			t.Fatalf("malformed ignored suffix accepted: %s", bad)
		}
	}
}

func TestV2UDTOptionsPropagateThroughComposition(t *testing.T) {
	integer := rowTestType(t, TypeInt)
	typ, _ := UDTType("ks", "record", ColumnSpec{Name: "id", Type: integer}, ColumnSpec{Name: "extra", Type: integer})
	options := UDTOptions{Keyspace: "ks", Name: "record", AllowExtraDatabaseFields: true}
	params := MapParams(Params(Param("id", Int32, func(v int32) int32 { return v })), func(v int64) int32 { return int32(v) })
	rows := MapRows(Row(Column("id", Int32, func(v *int32) *int32 { return v })), func(v int32) int64 { return int64(v) }, func(v int64) int64 { return v })
	encoded := containerGolden(t, typ, UDTEncoder(params, options), int64(7), "0000000400000007ffffffff")
	decoded, err := DecodeValue(UDTDecoder(rows, options), encoded)
	if err != nil || decoded != 7 {
		t.Fatalf("mapped UDT=%v err=%v", decoded, err)
	}
	positional, err := DecodeValue(UDTDecoder(OneColumn(Int32), options), encoded)
	if err != nil || positional != 7 {
		t.Fatalf("single positional UDT=%v err=%v", positional, err)
	}
}

func TestV2RecordCloneDetachesNestedBorrowing(t *testing.T) {
	text := rowTestType(t, TypeText)
	list, _ := ListType(text)
	tuple, _ := TupleType(list)
	encoder := TupleEncoder(OneParam(ListEncoder(StringEncoder)))
	decoder := TupleDecoder(OneColumn(ListDecoder(TextCodec)))
	encoded, err := EncodeValueBorrowed(tuple, encoder, []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	lease := wire.NewLease(encoded.body.chain, nil)
	encoded.body.lease = lease
	value, err := DecodeValue(decoder, encoded)
	if err != nil {
		t.Fatal(err)
	}
	clone := decoder.Clone(value)
	lease.Release()
	if it := value.Iterator(); it.Next() || !errors.Is(it.Err(), ErrReleased) {
		t.Fatal("released nested view survived")
	}
	values, err := Collect(clone.Iterator())
	if err != nil || values[1].String() != "second" {
		t.Fatalf("nested clone: %v", err)
	}
}

func TestV2TupleConcurrentCompiledPlansCheckOnce(t *testing.T) {
	var checks atomic.Int64
	integer := rowTestType(t, TypeInt)
	tuple, _ := TupleType(integer)
	columns := NewColumns(ColumnSpec{Name: "record", Type: tuple})
	decoder := TupleDecoder(OneColumn(containerCheckedInt{checks: &checks}))
	plan, err := OneColumn(decoder).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	chain := encodeTestParams(t, columns, OneParam(TupleEncoder(OneParam(Int32))), int32(9))
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 30 {
				got, err := plan.Decode(newRowReader(columns, chain, nil))
				if err != nil || got != 9 {
					t.Errorf("got=%v err=%v", got, err)
					return
				}
			}
		}()
	}
	group.Wait()
	if checks.Load() != 1 {
		t.Fatalf("tuple child metadata checked %d times", checks.Load())
	}
}
