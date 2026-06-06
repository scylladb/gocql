//go:build unit
// +build unit

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package gocql

import (
	"bytes"
	"testing"
)

// TestCreateRoutingKeyCaching verifies that createRoutingKeyCaching produces the
// same routing key as createRoutingKey and that the per-column cache it populates
// contains bytes identical to a direct Marshal of each partition-key value.
func TestCreateRoutingKeyCaching(t *testing.T) {
	t.Parallel()

	tInt := NewNativeType(4, TypeInt)
	tText := NewNativeType(4, TypeVarchar)

	t.Run("single", func(t *testing.T) {
		t.Parallel()
		rki := &routingKeyInfo{indexes: []int{0}, types: []TypeInfo{tInt}}
		values := []any{int32(12345)}

		want, err := createRoutingKey(rki, values)
		if err != nil {
			t.Fatal(err)
		}

		var cache []pkMarshalEntry
		got, err := createRoutingKeyCaching(rki, values, &cache)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("routing key mismatch: want %x got %x", want, got)
		}
		if len(cache) != 1 {
			t.Fatalf("expected 1 cache entry, got %d", len(cache))
		}
		if cache[0].index != 0 {
			t.Fatalf("expected cache index 0, got %d", cache[0].index)
		}
		marshalled, _ := Marshal(tInt, int32(12345))
		if !bytes.Equal(cache[0].value, marshalled) {
			t.Fatalf("cache value %x != marshalled %x", cache[0].value, marshalled)
		}
	})

	t.Run("composite", func(t *testing.T) {
		t.Parallel()
		// Partition key columns are at value indexes 0 and 2.
		rki := &routingKeyInfo{indexes: []int{0, 2}, types: []TypeInfo{tInt, tText}}
		values := []any{int32(7), "ignored", "partkey"}

		want, err := createRoutingKey(rki, values)
		if err != nil {
			t.Fatal(err)
		}
		var cache []pkMarshalEntry
		got, err := createRoutingKeyCaching(rki, values, &cache)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("routing key mismatch: want %x got %x", want, got)
		}
		if len(cache) != 2 {
			t.Fatalf("expected 2 cache entries, got %d", len(cache))
		}
		// Verify each cached entry equals a direct marshal of its value.
		m0, _ := Marshal(tInt, int32(7))
		m2, _ := Marshal(tText, "partkey")
		idxToVal := map[int][]byte{}
		for _, e := range cache {
			idxToVal[e.index] = e.value
		}
		if !bytes.Equal(idxToVal[0], m0) {
			t.Fatalf("cache[idx0] %x != %x", idxToVal[0], m0)
		}
		if !bytes.Equal(idxToVal[2], m2) {
			t.Fatalf("cache[idx2] %x != %x", idxToVal[2], m2)
		}
	})

	t.Run("named-not-cached", func(t *testing.T) {
		t.Parallel()
		// A *namedValue PK column cannot be marshalled directly as its CQL type
		// (the frame builder unwraps it first). createRoutingKey already errors
		// in this case, so token-aware routing falls back; the important
		// property here is that no stale/incorrect entry is left in the cache.
		rki := &routingKeyInfo{indexes: []int{0}, types: []TypeInfo{tInt}}
		nv := &namedValue{name: "pk", value: int32(5)}
		values := []any{nv}

		// Baseline: createRoutingKey errors on the named value.
		_, errBase := createRoutingKey(rki, values)

		var cache []pkMarshalEntry
		_, errCache := createRoutingKeyCaching(rki, values, &cache)

		// Behaviour must match the non-caching variant exactly.
		if (errBase == nil) != (errCache == nil) {
			t.Fatalf("error behaviour diverged: base=%v cache=%v", errBase, errCache)
		}
		if len(cache) != 0 {
			t.Fatalf("named value should not be cached, got %d entries", len(cache))
		}
	})
}

// TestPKMarshalCacheLookup verifies the per-query cache lookup helper, including
// the type-validation guard that prevents reusing bytes marshalled with a
// different type.
func TestPKMarshalCacheLookup(t *testing.T) {
	t.Parallel()
	tInt := NewNativeType(4, TypeInt)
	tBig := NewNativeType(4, TypeBigInt)
	q := &Query{
		pkMarshalCache: []pkMarshalEntry{
			{index: 2, typ: tInt, value: []byte{0xAA}},
			{index: 5, typ: tInt, value: []byte{0xBB}},
		},
	}
	if v, ok := q.lookupPKMarshalCache(2, tInt); !ok || !bytes.Equal(v, []byte{0xAA}) {
		t.Fatalf("index 2 lookup failed: v=%x ok=%v", v, ok)
	}
	if v, ok := q.lookupPKMarshalCache(5, tInt); !ok || !bytes.Equal(v, []byte{0xBB}) {
		t.Fatalf("index 5 lookup failed: v=%x ok=%v", v, ok)
	}
	if _, ok := q.lookupPKMarshalCache(3, tInt); ok {
		t.Fatal("index 3 should not be found")
	}
	// Type mismatch must be a miss (would otherwise inject wrong-typed bytes).
	if _, ok := q.lookupPKMarshalCache(2, tBig); ok {
		t.Fatal("type mismatch must not reuse cached bytes")
	}
	// Non-NativeType want must be a miss.
	if _, ok := q.lookupPKMarshalCache(2, TupleTypeInfo{}); ok {
		t.Fatal("non-NativeType want must not reuse cached bytes")
	}
}

// TestPKMarshalCacheInvalidation verifies that the partition-key marshalling
// cache is discarded when the query is rebound (Bind) or given an explicit
// routing key (RoutingKey), so executeQuery can never reuse stale bytes that
// were marshalled from a previous set of values.
func TestPKMarshalCacheInvalidation(t *testing.T) {
	t.Parallel()

	t.Run("Bind clears cache", func(t *testing.T) {
		t.Parallel()
		q := &Query{
			values:         []any{int32(1)},
			pkMarshalCache: []pkMarshalEntry{{index: 0, value: []byte{0x01}}},
		}
		q.Bind(int32(2))
		if len(q.pkMarshalCache) != 0 {
			t.Fatalf("Bind must clear pkMarshalCache, got %d entries", len(q.pkMarshalCache))
		}
		if _, ok := q.lookupPKMarshalCache(0, NewNativeType(4, TypeInt)); ok {
			t.Fatal("stale cache entry survived Bind")
		}
	})

	t.Run("RoutingKey clears cache", func(t *testing.T) {
		t.Parallel()
		q := &Query{
			values:         []any{int32(1)},
			pkMarshalCache: []pkMarshalEntry{{index: 0, value: []byte{0x01}}},
		}
		q.RoutingKey([]byte{0xFF})
		if len(q.pkMarshalCache) != 0 {
			t.Fatalf("RoutingKey must clear pkMarshalCache, got %d entries", len(q.pkMarshalCache))
		}
	})
}
