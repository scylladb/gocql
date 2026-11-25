//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"testing"
	"time"
)

// resetEncoderCache empties the process-wide encoder cache now and on cleanup.
// Tests using it must not call t.Parallel.
func resetEncoderCache(t *testing.T) {
	t.Helper()
	clearCompiledCache(&encoderCache, &encoderCacheCount)
	t.Cleanup(func() { clearCompiledCache(&encoderCache, &encoderCacheCount) })
}

// TestCompiledEncoderCacheIsBounded checks the encoder cache stops growing at
// maxCompiledCacheEntries.
func TestCompiledEncoderCacheIsBounded(t *testing.T) {
	resetEncoderCache(t)

	values := []any{int64(1)}
	for i := 0; i < maxCompiledCacheEntries+100; i++ {
		// Distinct type codes give distinct keys.
		columns := []ColumnInfo{{Name: "c", TypeInfo: NativeType{typ: Type(i), proto: 4}}}
		if enc := getOrCompileParamEncoder(columns, values); enc == nil {
			t.Fatal("expected an encoder even once the cache is full")
		}
	}

	if got := encoderCacheCount.Load(); got > maxCompiledCacheEntries {
		t.Fatalf("cache grew to %d entries, past the %d bound", got, maxCompiledCacheEntries)
	}
	var live int
	encoderCache.Range(func(_, _ any) bool { live++; return true })
	if live > maxCompiledCacheEntries {
		t.Fatalf("cache holds %d entries, past the %d bound", live, maxCompiledCacheEntries)
	}
}

// TestGetOrCompileParamEncoderCachedHitsAndMisses verifies the per-statement
// cache matches the uncached path cold and warm, and is invalidated by a shape change.
func TestGetOrCompileParamEncoderCachedHitsAndMisses(t *testing.T) {
	resetEncoderCache(t)
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
	}

	stmt := stmtFor(columns)

	valuesA := []any{int32(1), "a"}
	enc1 := getOrCompileParamEncoderCached(stmt, valuesA)
	if enc1 == nil {
		t.Fatal("expected non-nil encoder")
	}

	// Same shape again: must return the identical cached *compiledParamEncoder
	// (pointer equality), proving this went through the fast path rather than
	// recompiling.
	valuesA2 := []any{int32(99), "different value, same types"}
	enc2 := getOrCompileParamEncoderCached(stmt, valuesA2)
	if enc1 != enc2 {
		t.Fatal("expected the cached encoder pointer to be reused for the same argument-type shape")
	}

	// Verify it still encodes correctly for the new values.
	b, err := enc2.encoders[0](int32(99))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, err := Marshal(columns[0].TypeInfo, int32(99))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	if !bytes.Equal(b, want) {
		t.Fatalf("cached encoder produced wrong bytes: got %v, want %v", b, want)
	}

	// Different shape (int64 instead of int32 for column 0): must NOT reuse
	// enc1/enc2 — that would silently miscompile the int64 value with an
	// encoder built for int32.
	valuesB := []any{int64(1), "a"}
	enc3 := getOrCompileParamEncoderCached(stmt, valuesB)
	if enc3 == enc1 {
		t.Fatal("expected a different encoder after the argument-type shape changed")
	}
	b3, err := enc3.encoders[0](int64(1))
	if err != nil {
		t.Fatalf("unexpected error encoding int64: %v", err)
	}
	want3, err := Marshal(columns[0].TypeInfo, int64(1))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	if !bytes.Equal(b3, want3) {
		t.Fatalf("post-invalidation encoder produced wrong bytes: got %v, want %v", b3, want3)
	}

	// Switching back resolves via the process-wide cache and must return the
	// exact same *compiledParamEncoder as enc1/enc2.
	enc4 := getOrCompileParamEncoderCached(stmt, valuesA)
	if enc4 != enc1 {
		t.Fatal("expected the process-wide cache to return the original compiled encoder")
	}
	b4, err := enc4.encoders[0](int32(7))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want4, err := Marshal(columns[0].TypeInfo, int32(7))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	if !bytes.Equal(b4, want4) {
		t.Fatalf("re-resolved encoder produced wrong bytes: got %v, want %v", b4, want4)
	}
}

// TestGetOrCompileParamEncoderCachedNilStmt verifies the nil-stmt fallback
// (used when there's no prepared statement to cache against) still works.
func TestGetOrCompileParamEncoderCachedNilStmt(t *testing.T) {
	columns := []ColumnInfo{{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}}}
	values := []any{int32(42)}

	enc := getOrCompileParamEncoderCached(&preparedStatment{request: preparedMetadata{resultMetadata: resultMetadata{columns: columns}}}, values)
	b, err := enc.encoders[0](int32(42))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, err := Marshal(columns[0].TypeInfo, int32(42))
	if err != nil {
		t.Fatalf("unexpected error from reference Marshal: %v", err)
	}
	if !bytes.Equal(b, want) {
		t.Fatalf("got %v, want %v", b, want)
	}
}

// BenchmarkGetOrCompileParamEncoder_ColdVsCachedHit contrasts per-call key
// computation with the per-statement cached hit, on scalar plus collection columns.
func BenchmarkGetOrCompileParamEncoder_ColdVsCachedHit(b *testing.B) {
	columns := []ColumnInfo{
		{Name: "title", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "body", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
		{Name: "views", TypeInfo: NativeType{typ: TypeBigInt, proto: 4}},
		{Name: "protected", TypeInfo: NativeType{typ: TypeBoolean, proto: 4}},
		{Name: "modified", TypeInfo: NativeType{typ: TypeTimestamp, proto: 4}},
		{Name: "tags", TypeInfo: CollectionType{NativeType: NativeType{typ: TypeSet, proto: 4}, Elem: NativeType{typ: TypeVarchar, proto: 4}}},
	}
	values := []any{
		"Frontpage",
		"Welcome to this wiki page!",
		int64(0),
		false,
		time.Now(),
		[]string{"start", "important", "test"},
	}

	b.Run("Uncached_PerCallKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = getOrCompileParamEncoder(columns, values)
		}
	})

	b.Run("Cached_PerStatement_WarmHit", func(b *testing.B) {
		stmt := stmtFor(columns)
		getOrCompileParamEncoderCached(stmt, values) // warm the cache
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = getOrCompileParamEncoderCached(stmt, values)
		}
	})
}
