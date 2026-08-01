//go:build unit
// +build unit

package gocql

import (
	"testing"
)

// TestGetOrCompileRowDecoderCachedHitsAndMisses is the decoder-side analogue
// of TestGetOrCompileParamEncoderCachedHitsAndMisses: verifies the
// per-statement decoder cache (preparedStatment.jitDecoder) returns the same
// compiled decoder on a warm hit, and correctly invalidates when the Scan
// destination-type shape changes, rather than silently reusing a decoder
// compiled for different types.
func TestGetOrCompileRowDecoderCachedHitsAndMisses(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
	}

	stmt := &preparedStatment{}

	var id1 int32
	var name1 string
	dec1 := getOrCompileRowDecoderCached(stmt, columns, []any{&id1, &name1})
	if dec1 == nil {
		t.Fatal("expected non-nil decoder")
	}

	// Same shape, different (fresh) destination variables: must return the
	// identical cached *compiledRowDecoder (pointer equality), proving this
	// went through the fast path rather than recompiling.
	var id2 int32
	var name2 string
	dec2 := getOrCompileRowDecoderCached(stmt, columns, []any{&id2, &name2})
	if dec1 != dec2 {
		t.Fatal("expected the cached decoder pointer to be reused for the same destination-type shape")
	}

	// Different shape (int64 instead of int32 for column 0): must NOT reuse
	// dec1/dec2 — that would silently misdecode into the wrong-sized int.
	var id3 int64
	var name3 string
	dec3 := getOrCompileRowDecoderCached(stmt, columns, []any{&id3, &name3})
	if dec3 == dec1 {
		t.Fatal("expected a different decoder after the destination-type shape changed")
	}
}

// TestGetOrCompileRowDecoderCachedNilStmt verifies the nil-stmt fallback
// (non-prepared queries have no preparedStatment to cache against) still
// works and matches the uncached path's behavior.
func TestGetOrCompileRowDecoderCachedNilStmt(t *testing.T) {
	columns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}}}
	var id int32

	dec := getOrCompileRowDecoderCached(nil, columns, []any{&id})
	if dec == nil {
		t.Fatal("expected non-nil decoder")
	}
}

// TestGetOrCompileRowDecoderCachedReusesAcrossFreshIdenticalSchemaSlices
// verifies the cache hits across two independently-allocated but
// identically-shaped []ColumnInfo slices, not just across the same slice.
// This matters because, under this driver's actual default configuration
// (DisableSkipMetadata's default of true means skip_metadata is never
// requested), the server sends full column metadata on every response, so
// parseResultMetadata allocates a brand-new []ColumnInfo on every call
// regardless of whether the schema changed. A cache-validity check based on
// the columns slice's backing-array identity would fail every single time
// under that default and silently defeat this entire cache; columnsSignature
// (a count + top-level-CQL-type hash, not a pointer) is what avoids that.
func TestGetOrCompileRowDecoderCachedReusesAcrossFreshIdenticalSchemaSlices(t *testing.T) {
	firstResponseColumns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}
	secondResponseColumns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}

	stmt := &preparedStatment{}

	var id1 int32
	dec1 := getOrCompileRowDecoderCached(stmt, firstResponseColumns, []any{&id1})

	var id2 int32
	dec2 := getOrCompileRowDecoderCached(stmt, secondResponseColumns, []any{&id2})

	if dec1 != dec2 {
		t.Fatal("expected the stmt-local cache to hit across independently-allocated, identically-shaped columns slices")
	}
}

// TestGetOrCompileRowDecoderCachedInvalidatesOnColumnsShapeChange verifies
// the fix for a metadata-refresh race: even when destTypes are unchanged, a
// cache hit must not be served once the columns' actual shape (count or any
// column's top-level CQL type) changes from what the cached decoder was
// compiled for — see cachedJITDecoder.columnsSig. Without this check, a
// *preparedStatment left stale by a lost updateMetadataIfSame race (conn.go)
// could have its decoder applied to bytes for a different, incompatible
// column schema.
func TestGetOrCompileRowDecoderCachedInvalidatesOnColumnsShapeChange(t *testing.T) {
	oldColumns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
	}
	// A genuine schema change: same column count, different top-level CQL type.
	newColumns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
	}

	stmt := &preparedStatment{}

	var id1 int32
	_ = getOrCompileRowDecoderCached(stmt, oldColumns, []any{&id1})

	var id2 int64
	_ = getOrCompileRowDecoderCached(stmt, newColumns, []any{&id2})

	cached := stmt.jitDecoder.Load()
	if cached == nil {
		t.Fatal("expected a cached decoder entry after the second call")
	}
	if cached.columnsSig != columnsSignature(newColumns) {
		t.Fatal("expected the cache entry to be keyed on the most recent columns' shape, not the first one")
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
		getOrCompileRowDecoderCached(stmt, columns, dest) // warm the cache
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = getOrCompileRowDecoderCached(stmt, columns, dest)
		}
	})
}
