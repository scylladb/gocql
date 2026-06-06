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

import "testing"

// BenchmarkCreateRoutingKey measures the cost of marshalling partition-key
// columns to compute the routing token. This runs on every token-aware query
// via tokenAwareHostPolicy.Pick -> GetRoutingKey -> createRoutingKey.
func BenchmarkCreateRoutingKey(b *testing.B) {
	ti := NewNativeType(4, TypeInt)

	b.Run("single", func(b *testing.B) {
		rki := &routingKeyInfo{indexes: []int{0}, types: []TypeInfo{ti}}
		values := []any{int32(12345)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := createRoutingKey(rki, values); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("composite", func(b *testing.B) {
		rki := &routingKeyInfo{
			indexes: []int{0, 1, 2},
			types:   []TypeInfo{ti, NewNativeType(4, TypeVarchar), ti},
		}
		values := []any{int32(1), "partition", int32(2)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := createRoutingKey(rki, values); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRoutingKeyMarshalReuse contrasts the pre-optimization behavior
// (marshal the partition-key value once for the routing token and again for the
// request frame) with the optimized behavior (marshal once for the token, then
// reuse the cached bytes for the frame). It approximates the per-query
// write-path saving for a single-int-PK table.
func BenchmarkRoutingKeyMarshalReuse(b *testing.B) {
	ti := NewNativeType(4, TypeInt)
	rki := &routingKeyInfo{indexes: []int{0}, types: []TypeInfo{ti}}
	values := []any{int32(987654)}

	b.Run("marshal-twice", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rk, err := createRoutingKey(rki, values)
			if err != nil {
				b.Fatal(err)
			}
			_ = rk
			var qv queryValues
			if err := marshalQueryValue(ti, values[0], &qv); err != nil {
				b.Fatal(err)
			}
			_ = qv
		}
	})

	b.Run("marshal-once-reuse", func(b *testing.B) {
		var cache []pkMarshalEntry
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache = cache[:0]
			rk, err := createRoutingKeyCaching(rki, values, &cache)
			if err != nil {
				b.Fatal(err)
			}
			_ = rk
			var qv queryValues
			qv.value = cache[0].value // frame reuses cached bytes, no second Marshal
			_ = qv
		}
	})
}
