//go:build unit
// +build unit

package gocql

import (
	"testing"
)

// decodeAll runs dec's per-column decoders over data (raw column bytes,
// produced by Marshal) into dest, failing the test on any decode error.
func decodeAll(t *testing.T, dec *compiledRowDecoder, data [][]byte, dest []any) {
	t.Helper()
	for i := range dest {
		if err := dec.decoders[i](data[i], dest[i]); err != nil {
			t.Fatalf("column %d: unexpected decode error: %v", i, err)
		}
	}
}

// TestResolveRowDecoderHitsAndMisses is the decoder-side analogue
// of TestGetOrCompileParamEncoderCachedHitsAndMisses: verifies the
// per-statement decoder cache (preparedStatment.jitDecoder) returns the same
// compiled decoder on a warm hit, and correctly invalidates when the Scan
// destination-type shape changes, rather than silently reusing a decoder
// compiled for different types. Unlike a pointer-identity-only check, this
// also decodes real wire bytes through the cached decoder and asserts the
// actual decoded values, so a decoder that happened to share a pointer but
// misdecode would still be caught.
func TestResolveRowDecoderHitsAndMisses(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
	}

	idBytes, err := Marshal(columns[0].TypeInfo, int32(42))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	nameBytes, err := Marshal(columns[1].TypeInfo, "hello")
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	rowData := [][]byte{idBytes, nameBytes}

	stmt := &preparedStatment{}

	var id1 int32
	var name1 string
	dec1 := resolveRowDecoder(stmt, columns, []any{&id1, &name1})
	if dec1 == nil {
		t.Fatal("expected non-nil decoder")
	}
	decodeAll(t, dec1.dec, rowData, []any{&id1, &name1})
	if id1 != 42 || name1 != "hello" {
		t.Fatalf("cold decode: got id=%d name=%q, want id=42 name=%q", id1, name1, "hello")
	}

	// Same shape, different (fresh) destination variables: must return the
	// identical cached *compiledRowDecoder (pointer equality), proving this
	// went through the fast path rather than recompiling — and must still
	// decode correctly through that cached pointer.
	var id2 int32
	var name2 string
	dec2 := resolveRowDecoder(stmt, columns, []any{&id2, &name2})
	if dec1.dec != dec2.dec {
		t.Fatal("expected the cached decoder pointer to be reused for the same destination-type shape")
	}
	decodeAll(t, dec2.dec, rowData, []any{&id2, &name2})
	if id2 != 42 || name2 != "hello" {
		t.Fatalf("warm-hit decode: got id=%d name=%q, want id=42 name=%q", id2, name2, "hello")
	}

	// Different shape (int64 instead of int32 for column 0): must NOT reuse
	// dec1/dec2 — that would silently misdecode into the wrong-sized int.
	int64Bytes, err := Marshal(columns[0].TypeInfo, int64(42))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	var id3 int64
	var name3 string
	dec3 := resolveRowDecoder(stmt, columns, []any{&id3, &name3})
	if dec3.dec == dec1.dec {
		t.Fatal("expected a different decoder after the destination-type shape changed")
	}
	decodeAll(t, dec3.dec, [][]byte{int64Bytes, nameBytes}, []any{&id3, &name3})
	if id3 != 42 || name3 != "hello" {
		t.Fatalf("post-invalidation decode: got id=%d name=%q, want id=42 name=%q", id3, name3, "hello")
	}
}

// TestResolveRowDecoderNilStmt verifies the nil-stmt fallback
// (non-prepared queries have no preparedStatment to cache against) still
// works and matches the uncached path's behavior.
func TestResolveRowDecoderNilStmt(t *testing.T) {
	columns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}}}
	var id int32

	dec := resolveRowDecoder(nil, columns, []any{&id})
	if dec == nil {
		t.Fatal("expected non-nil decoder")
	}
}

// TestResolveRowDecoderReusesAcrossFreshIdenticalSchemaSlices
// verifies the cache hits across independently-allocated but identically-shaped
// []ColumnInfo slices: parseResultMetadata allocates a fresh slice per response
// under the default config, so an identity check would miss every time.
func TestResolveRowDecoderReusesAcrossFreshIdenticalSchemaSlices(t *testing.T) {
	firstResponseColumns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}
	secondResponseColumns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}

	stmt := &preparedStatment{}

	var id1 int32
	dec1 := resolveRowDecoder(stmt, firstResponseColumns, []any{&id1})

	var id2 int32
	dec2 := resolveRowDecoder(stmt, secondResponseColumns, []any{&id2})

	if dec1.dec != dec2.dec {
		t.Fatal("expected the stmt-local cache to hit across independently-allocated, identically-shaped columns slices")
	}
}

// TestResolveRowDecoderInvalidatesOnColumnTypeChange verifies the fix for a
// metadata-refresh race: a *preparedStatment left stale by a lost
// updateMetadataIfSame race (conn.go) must not have its decoder applied to
// bytes for a different column schema. destTypes are deliberately identical
// across both calls, so only the columns check can catch this.
func TestResolveRowDecoderInvalidatesOnColumnTypeChange(t *testing.T) {
	oldColumns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}}}
	newColumns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}}}

	oldBytes, err := Marshal(oldColumns[0].TypeInfo, int32(7))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	newBytes, err := Marshal(newColumns[0].TypeInfo, int64(1<<40))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}

	stmt := &preparedStatment{}

	var id1 int64
	decodeAll(t, resolveRowDecoder(stmt, oldColumns, []any{&id1}).dec, [][]byte{oldBytes}, []any{&id1})
	if id1 != 7 {
		t.Fatalf("got %d, want 7", id1)
	}

	// Same dest type, new column type: reusing the int-compiled decoder here
	// would read 4 bytes of an 8-byte value.
	var id2 int64
	decodeAll(t, resolveRowDecoder(stmt, newColumns, []any{&id2}).dec, [][]byte{newBytes}, []any{&id2})
	if id2 != 1<<40 {
		t.Fatalf("got %d, want %d", id2, int64(1<<40))
	}

	cached := stmt.jitDecoder.Load()
	if cached == nil || !columnsEqual(cached.columns, newColumns) {
		t.Fatal("expected the cache entry to hold the most recent columns, not the first")
	}
}

// TestResolveRowDecoderInvalidatesOnNestedElementTypeChange covers a schema
// change the previous top-level-type-code signature could not see: the outer
// type code (list) is unchanged, only the element type differs. The compiled
// fallback decoder captures the element TypeInfo, so reusing it would misread
// the new element width.
func TestResolveRowDecoderInvalidatesOnNestedElementTypeChange(t *testing.T) {
	listOfInt := []ColumnInfo{{Name: "vals", TypeInfo: CollectionType{
		NativeType: NativeType{typ: TypeList, proto: 4},
		Elem:       NativeType{typ: TypeInt, proto: 4},
	}}}
	listOfBigInt := []ColumnInfo{{Name: "vals", TypeInfo: CollectionType{
		NativeType: NativeType{typ: TypeList, proto: 4},
		Elem:       NativeType{typ: TypeBigInt, proto: 4},
	}}}

	if columnsEqual(listOfInt, listOfBigInt) {
		t.Fatal("list<int> and list<bigint> must not compare equal")
	}

	bigIntBytes, err := Marshal(listOfBigInt[0].TypeInfo, []int64{1 << 40})
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}

	stmt := &preparedStatment{}

	var vals1 []int64
	resolveRowDecoder(stmt, listOfInt, []any{&vals1})

	// Same dest type, nested element type changed underneath it.
	var vals2 []int64
	decodeAll(t, resolveRowDecoder(stmt, listOfBigInt, []any{&vals2}).dec, [][]byte{bigIntBytes}, []any{&vals2})
	if len(vals2) != 1 || vals2[0] != 1<<40 {
		t.Fatalf("got %v, want [%d]", vals2, int64(1<<40))
	}
}

// TestColumnsEqualDistinguishesPreviouslyCollidingShapes locks in two layouts
// that the earlier columnsSignature hash mapped to the same value (its
// multiplier, 31, was smaller than the CQL type-code range), which would have
// let a stale decoder survive a change between them.
func TestColumnsEqualDistinguishesPreviouslyCollidingShapes(t *testing.T) {
	asciiThenList := []ColumnInfo{
		{Name: "a", TypeInfo: NativeType{typ: TypeAscii, proto: 4}},
		{Name: "b", TypeInfo: CollectionType{
			NativeType: NativeType{typ: TypeList, proto: 4},
			Elem:       NativeType{typ: TypeVarchar, proto: 4},
		}},
	}
	bigIntThenAscii := []ColumnInfo{
		{Name: "a", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "b", TypeInfo: NativeType{typ: TypeAscii, proto: 4}},
	}

	if columnsEqual(asciiThenList, bigIntThenAscii) {
		t.Fatal("distinct column layouts must not compare equal")
	}
}

// TestTypeInfoEqual covers the nested TypeInfo kinds columnsEqual recurses
// into, each differing only below the top-level type code.
func TestTypeInfoEqual(t *testing.T) {
	native := func(typ Type) NativeType { return NativeType{typ: typ, proto: 4} }
	list := func(elem TypeInfo) CollectionType {
		return CollectionType{NativeType: native(TypeList), Elem: elem}
	}
	udt := func(fieldType TypeInfo) UDTTypeInfo {
		return UDTTypeInfo{
			NativeType: native(TypeUDT), KeySpace: "ks", Name: "u",
			Elements: []UDTField{{Name: "f", Type: fieldType}},
		}
	}

	tests := []struct {
		name string
		a, b TypeInfo
		want bool
	}{
		{"same native", native(TypeInt), native(TypeInt), true},
		{"different native", native(TypeInt), native(TypeBigInt), false},
		{"same list", list(native(TypeInt)), list(native(TypeInt)), true},
		{"different list elem", list(native(TypeInt)), list(native(TypeBigInt)), false},
		{
			"different map key",
			CollectionType{NativeType: native(TypeMap), Key: native(TypeInt), Elem: native(TypeVarchar)},
			CollectionType{NativeType: native(TypeMap), Key: native(TypeBigInt), Elem: native(TypeVarchar)},
			false,
		},
		{
			"different tuple elem",
			TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{native(TypeInt)}},
			TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{native(TypeBigInt)}},
			false,
		},
		{
			"different tuple arity",
			TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{native(TypeInt)}},
			TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{native(TypeInt), native(TypeInt)}},
			false,
		},
		{"same udt", udt(native(TypeInt)), udt(native(TypeInt)), true},
		{"different udt field type", udt(native(TypeInt)), udt(native(TypeBigInt)), false},
		{
			"different vector dimensions",
			VectorType{NativeType: native(TypeCustom), SubType: native(TypeFloat), Dimensions: 3},
			VectorType{NativeType: native(TypeCustom), SubType: native(TypeFloat), Dimensions: 4},
			false,
		},
		{"native vs collection", native(TypeList), list(native(TypeInt)), false},
		{"both nil", nil, nil, true},
		{"one nil", nil, native(TypeInt), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := typeInfoEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("typeInfoEqual = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnsureRowDecoderForInvalidatesOnPageTurn verifies a page turn that
// changes the schema drops the decoder compiled for the previous page, even
// though the caller's destination types are unchanged — the common case, since
// the same Scan call site is reused for every row.
func TestEnsureRowDecoderForInvalidatesOnPageTurn(t *testing.T) {
	oldColumns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}}}
	newColumns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}}}

	idBytes, err := Marshal(oldColumns[0].TypeInfo, int32(7))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	newIDBytes, err := Marshal(newColumns[0].TypeInfo, int64(7))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}

	iter := &Iter{meta: resultMetadata{columns: oldColumns}}

	var id1 int32
	iter.ensureRowDecoderFor([]any{&id1})
	decodeAll(t, iter.rowDecoder.dec, [][]byte{idBytes}, []any{&id1})
	if id1 != 7 {
		t.Fatalf("got %d, want 7", id1)
	}

	// The page turn fetchNextPage performs when RESULT_METADATA_CHANGED lands
	// on a page boundary.
	iter.copyPageData(&Iter{meta: resultMetadata{columns: newColumns}})
	if iter.rowDecoder != nil {
		t.Fatal("expected the page turn to drop the decoder compiled for the previous schema")
	}

	var id2 int64
	iter.ensureRowDecoderFor([]any{&id2})
	decodeAll(t, iter.rowDecoder.dec, [][]byte{newIDBytes}, []any{&id2})
	if id2 != 7 {
		t.Fatalf("got %d, want 7", id2)
	}
}

// BenchmarkEnsureRowDecoderForWarmRow measures the per-row revalidation on the
// Scan hot path: every row of every Iter pays it.
func BenchmarkEnsureRowDecoderForWarmRow(b *testing.B) {
	columns := []ColumnInfo{
		{Name: "title", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "body", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "views", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "protected", TypeInfo: NativeType{typ: TypeBoolean, proto: 4}},
		{Name: "tags", TypeInfo: CollectionType{NativeType: NativeType{typ: TypeSet, proto: 4}, Elem: NativeType{typ: TypeVarchar, proto: 4}}},
	}
	var title, body string
	var views int64
	var protected bool
	var tags []string
	dest := []any{&title, &body, &views, &protected, &tags}

	iter := &Iter{meta: resultMetadata{columns: columns}, preparedStmt: &preparedStatment{}}
	iter.ensureRowDecoderFor(dest) // warm, as it is after the first row

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.ensureRowDecoderFor(dest)
	}
}

// BenchmarkGetOrCompileRowDecoder_ColdVsCachedHit is the decoder-side
// analogue of BenchmarkGetOrCompileParamEncoder_ColdVsCachedHit.
func BenchmarkGetOrCompileRowDecoder_ColdVsCachedHit(b *testing.B) {
	columns := []ColumnInfo{
		{Name: "title", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "body", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "views", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "protected", TypeInfo: NativeType{typ: TypeBoolean, proto: 4}},
		{Name: "tags", TypeInfo: CollectionType{NativeType: NativeType{typ: TypeSet, proto: 4}, Elem: NativeType{typ: TypeVarchar, proto: 4}}},
	}
	var title, body string
	var views int64
	var protected bool
	var tags []string
	dest := []any{&title, &body, &views, &protected, &tags}

	b.Run("Uncached_PerCallKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = getOrCompileRowDecoder(columns, dest)
		}
	})

	b.Run("Cached_PerStatement_WarmHit", func(b *testing.B) {
		stmt := &preparedStatment{}
		resolveRowDecoder(stmt, columns, dest) // warm the cache
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = resolveRowDecoder(stmt, columns, dest)
		}
	})
}
