/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gocql

// jit_decoder.go: compiled row decoder that removes per-row type switches and
// reflection from Scan. Named types, Unmarshalers, **T and complex CQL types
// fall back to the generic Unmarshal path.

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql/serialization/bigint"
	"github.com/gocql/gocql/serialization/blob"
	"github.com/gocql/gocql/serialization/boolean"
	"github.com/gocql/gocql/serialization/counter"
	"github.com/gocql/gocql/serialization/cqlint"
	"github.com/gocql/gocql/serialization/double"
	"github.com/gocql/gocql/serialization/float"
	"github.com/gocql/gocql/serialization/inet"
	"github.com/gocql/gocql/serialization/smallint"
	"github.com/gocql/gocql/serialization/text"
	"github.com/gocql/gocql/serialization/timestamp"
	"github.com/gocql/gocql/serialization/timeuuid"
	"github.com/gocql/gocql/serialization/tinyint"
	"github.com/gocql/gocql/serialization/uuid"
	"github.com/gocql/gocql/serialization/varchar"
)

// columnDecoder decodes one CQL column into a specific Go destination type;
// resolved once per (CQL type, Go type) pair.
type columnDecoder func(data []byte, dest any) error

// compiledRowDecoder is a pre-compiled decoder for a specific row shape:
// a fixed sequence of (CQL column type, Go destination type) pairs.
// It is built lazily on the first Scan call and cached for reuse.
type compiledRowDecoder struct {
	// decoders holds one decoder per column; nil for a tuple column, which
	// scanColumn expands into one destination per element.
	decoders []columnDecoder
}

// decoderCache is a process-wide cache of compiled row decoders.
// It uses sync.Map for lock-free reads on the hot path.
var (
	decoderCache      sync.Map // map[string]*compiledRowDecoder
	decoderCacheCount atomic.Int64
)

// maxCompiledCacheEntries bounds the process-wide decoder and encoder caches.
// On reaching it the cache is cleared (not sealed): sealing would lock out
// steady-state shapes after a startup burst; an LRU would need a lock.
const maxCompiledCacheEntries = 1024

// clearCompiledCache empties a compiled-artifact cache that has reached the
// bound. Concurrent callers may briefly over- or under-count; the caches are
// advisory, so a lost entry or an extra one only costs a recompile.
func clearCompiledCache(cache *sync.Map, count *atomic.Int64) {
	cache.Range(func(k, _ any) bool {
		cache.Delete(k)
		return true
	})
	count.Store(0)
}

// compileRowDecoder builds a compiledRowDecoder for the columns and per-column
// destination types, falling back to Unmarshal where no fast path exists.
func compileRowDecoder(columns []ColumnInfo, colTypes []reflect.Type) *compiledRowDecoder {
	decoders := make([]columnDecoder, len(columns))
	for i, col := range columns {
		if _, ok := col.TypeInfo.(TupleTypeInfo); !ok {
			decoders[i] = compileColumnDecoder(col.TypeInfo, colTypes[i])
		}
	}
	return &compiledRowDecoder{decoders: decoders}
}

// columnDestTypes maps per-destination types to per-column ones. A tuple column
// consumes one destination per element and maps to nil, as it has no decoder.
// The caller guarantees len(destTypes) == the row's actualColCount.
func columnDestTypes(columns []ColumnInfo, destTypes []reflect.Type) []reflect.Type {
	colTypes := make([]reflect.Type, len(columns))
	k := 0
	for i, col := range columns {
		if tuple, ok := col.TypeInfo.(TupleTypeInfo); ok {
			k += len(tuple.Elems)
			continue
		}
		colTypes[i] = destTypes[k]
		k++
	}
	return colTypes
}

// makeDecoderCacheKey builds a unique string key from column metadata and dest types.
func makeDecoderCacheKey(columns []ColumnInfo, destTypes []reflect.Type) string {
	// 11 bytes per column: 1 version + 2 CQL type + 8 Go type pointer;
	// complex types append their TypeInfo to disambiguate.
	buf := make([]byte, 0, len(columns)*11)
	for i, col := range columns {
		// Encode protocol version as 1 byte (affects Unmarshal behavior).
		buf = append(buf, col.TypeInfo.Version())
		// Encode CQL type as 2 bytes.
		cqlType := col.TypeInfo.Type()
		buf = append(buf, byte(cqlType>>8), byte(cqlType))
		// Encode Go type as its unique pointer value (8 bytes).
		// For nil dest types, use 8 zero bytes as a sentinel.
		if destTypes[i] == nil {
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
		} else {
			ptr := reflect.ValueOf(destTypes[i]).Pointer()
			buf = append(buf,
				byte(ptr>>56), byte(ptr>>48), byte(ptr>>40), byte(ptr>>32),
				byte(ptr>>24), byte(ptr>>16), byte(ptr>>8), byte(ptr))
		}
		// list<int> and list<bigint> share a type code, so complex types add their full structure.
		switch cqlType {
		case TypeList, TypeSet, TypeMap, TypeTuple, TypeUDT, TypeCustom:
			buf = appendTypeInfoKey(buf, col.TypeInfo)
		}
	}
	return string(buf)
}

// getOrCompileRowDecoder returns a cached compiled decoder or builds one.
func getOrCompileRowDecoder(columns []ColumnInfo, dest []any) *compiledRowDecoder {
	destTypes := make([]reflect.Type, len(dest))
	for i, d := range dest {
		if d == nil {
			destTypes[i] = nil
		} else {
			destTypes[i] = reflect.TypeOf(d)
		}
	}

	colTypes := columnDestTypes(columns, destTypes)
	key := makeDecoderCacheKey(columns, colTypes)
	if cached, ok := decoderCache.Load(key); ok {
		return cached.(*compiledRowDecoder)
	}

	dec := compileRowDecoder(columns, colTypes)
	if decoderCacheCount.Load() >= maxCompiledCacheEntries {
		clearCompiledCache(&decoderCache, &decoderCacheCount)
	}
	actual, loaded := decoderCache.LoadOrStore(key, dec)
	if !loaded {
		decoderCacheCount.Add(1)
	}
	return actual.(*compiledRowDecoder)
}

// appendTypeInfoKey serializes t structurally, length-prefixing every string and
// count, so distinct types cannot collide the way their String() text can.
func appendTypeInfoKey(buf []byte, t TypeInfo) []byte {
	appendU32 := func(buf []byte, n int) []byte {
		return append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	appendStr := func(buf []byte, s string) []byte {
		return append(appendU32(buf, len(s)), s...)
	}
	appendNative := func(buf []byte, nt NativeType) []byte {
		buf = append(buf, nt.proto, byte(nt.typ>>8), byte(nt.typ))
		return appendStr(buf, nt.custom)
	}
	switch tt := t.(type) {
	case nil:
		return append(buf, 0)
	case NativeType:
		return appendNative(append(buf, 1), tt)
	case CollectionType:
		buf = appendNative(append(buf, 2), tt.NativeType)
		return appendTypeInfoKey(appendTypeInfoKey(buf, tt.Key), tt.Elem)
	case VectorType:
		buf = appendU32(appendNative(append(buf, 3), tt.NativeType), tt.Dimensions)
		return appendTypeInfoKey(buf, tt.SubType)
	case TupleTypeInfo:
		buf = appendU32(appendNative(append(buf, 4), tt.NativeType), len(tt.Elems))
		for _, e := range tt.Elems {
			buf = appendTypeInfoKey(buf, e)
		}
		return buf
	case UDTTypeInfo:
		buf = appendNative(append(buf, 5), tt.NativeType)
		buf = appendU32(appendStr(appendStr(buf, tt.KeySpace), tt.Name), len(tt.Elements))
		for _, f := range tt.Elements {
			buf = appendTypeInfoKey(appendStr(buf, f.Name), f.Type)
		}
		return buf
	}
	// shortcut: a foreign TypeInfo implementation keys on its %T and text, which may collide; add a case if one appears.
	return appendStr(appendStr(append(buf, 6), fmt.Sprintf("%T", t)), fmt.Sprint(t))
}

// columnsEqual reports whether two column lists decode identically: same count
// and same CQL type per position. Column names do not affect decoding.
func columnsEqual(a, b []ColumnInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !typeInfoEqual(a[i].TypeInfo, b[i].TypeInfo) {
			return false
		}
	}
	return true
}

// typeInfoEqual reports whether two TypeInfos describe the same CQL type,
// nested types included. NativeType is compared as a struct to avoid interface calls.
func typeInfoEqual(a, b TypeInfo) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	switch at := a.(type) {
	case NativeType:
		bt, ok := b.(NativeType)
		return ok && at == bt
	case CollectionType:
		bt, ok := b.(CollectionType)
		return ok && at.NativeType == bt.NativeType &&
			typeInfoEqual(at.Key, bt.Key) && typeInfoEqual(at.Elem, bt.Elem)
	case VectorType:
		bt, ok := b.(VectorType)
		return ok && at.NativeType == bt.NativeType && at.Dimensions == bt.Dimensions &&
			typeInfoEqual(at.SubType, bt.SubType)
	case TupleTypeInfo:
		bt, ok := b.(TupleTypeInfo)
		if !ok || at.NativeType != bt.NativeType || len(at.Elems) != len(bt.Elems) {
			return false
		}
		for i := range at.Elems {
			if !typeInfoEqual(at.Elems[i], bt.Elems[i]) {
				return false
			}
		}
		return true
	case UDTTypeInfo:
		bt, ok := b.(UDTTypeInfo)
		if !ok || at.NativeType != bt.NativeType || at.KeySpace != bt.KeySpace ||
			at.Name != bt.Name || len(at.Elements) != len(bt.Elements) {
			return false
		}
		for i := range at.Elements {
			if at.Elements[i].Name != bt.Elements[i].Name ||
				!typeInfoEqual(at.Elements[i].Type, bt.Elements[i].Type) {
				return false
			}
		}
		return true
	}

	// Unknown TypeInfo implementation: recompile rather than risk a wrong reuse.
	return false
}

// compileColumnDecoder selects the decoder for a (CQL type, Go type) pair, else falls back to Unmarshal.
// Compares exact reflect.Types, not Kind: *MyInt would fail the dest.(*int32) assertion.
func compileColumnDecoder(info TypeInfo, destType reflect.Type) columnDecoder {
	if destType == nil {
		return decodeSkip
	}

	cqlType := info.Type()

	// Unwrap pointer: destType is reflect.Type of the value passed to Scan,
	// which is already a pointer (e.g., *int, *string). We need the elem type.
	if destType.Kind() != reflect.Ptr {
		return decodeFallback(info)
	}
	elemType := destType.Elem()
	// A defined pointer type (type P *int32) is not *int32, which the fast paths assert.
	if destType != reflect.PointerTo(elemType) {
		return decodeFallback(info)
	}

	// Check for Unmarshaler interface — must always take priority.
	if destType.Implements(unmarshalerType) {
		return decodeFallback(info)
	}

	// Check for pointer-to-pointer (nullable) — fall back for now.
	if elemType.Kind() == reflect.Ptr {
		return decodeFallback(info)
	}

	switch cqlType {
	// TypeAscii is deliberately absent: ascii.DecString/DecBytes reject bytes
	// above 127, which no fast path here enforces, so it falls back.
	case TypeVarchar:
		if elemType == stringType {
			return decodeVarcharToString
		}
		if elemType == bytesType {
			return decodeVarcharToBytes
		}

	case TypeText:
		if elemType == stringType {
			return decodeTextToString
		}
		if elemType == bytesType {
			return decodeTextToBytes
		}

	case TypeBlob:
		if elemType == bytesType {
			return decodeBlobToBytes
		}
		if elemType == stringType {
			return decodeBlobToString
		}

	case TypeInt:
		switch elemType {
		case intType:
			return decodeIntToInt
		case int32Type:
			return decodeIntToInt32
		case int64Type:
			return decodeIntToInt64
		}

	case TypeBigInt:
		switch elemType {
		case int64Type:
			return decodeBigIntToInt64
		case intType:
			return decodeBigIntToInt
		}

	case TypeCounter:
		switch elemType {
		case int64Type:
			return decodeCounterToInt64
		case intType:
			return decodeCounterToInt
		}

	case TypeSmallInt:
		switch elemType {
		case int16Type:
			return decodeSmallIntToInt16
		case intType:
			return decodeSmallIntToInt
		}

	case TypeTinyInt:
		if elemType == int8Type {
			return decodeTinyIntToInt8
		}

	case TypeBoolean:
		if elemType == boolType {
			return decodeBoolToBool
		}

	case TypeFloat:
		if elemType == float32Type {
			return decodeFloatToFloat32
		}

	case TypeDouble:
		if elemType == float64Type {
			return decodeDoubleToFloat64
		}

	case TypeTimestamp:
		if elemType == timeType {
			return decodeTimestampToTime
		}
		if elemType == int64Type {
			return decodeTimestampToInt64
		}

	case TypeUUID:
		if elemType == uuidType {
			return decodeUUIDToUUID
		}
		if elemType == stringType {
			return decodeUUIDToString
		}

	case TypeTimeUUID:
		if elemType == uuidType {
			return decodeTimeUUIDToUUID
		}
		if elemType == stringType {
			return decodeTimeUUIDToString
		}

	case TypeInet:
		if elemType == ipType {
			return decodeInetToIP
		}
		if elemType == stringType {
			return decodeInetToString
		}
	}

	// No fast path — fall back to generic Unmarshal.
	return decodeFallback(info)
}

// Sentinel reflect.Types for common destination types.
var (
	unmarshalerType = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
	timeType        = reflect.TypeOf(time.Time{})
	uuidType        = reflect.TypeOf(UUID{})
	ipType          = reflect.TypeOf(net.IP{})

	// Exact types: named types share a Kind, and the decoders' type assertions would panic.
	stringType  = reflect.TypeOf("")
	boolType    = reflect.TypeOf(false)
	intType     = reflect.TypeOf(int(0))
	int8Type    = reflect.TypeOf(int8(0))
	int16Type   = reflect.TypeOf(int16(0))
	int32Type   = reflect.TypeOf(int32(0))
	int64Type   = reflect.TypeOf(int64(0))
	float32Type = reflect.TypeOf(float32(0))
	float64Type = reflect.TypeOf(float64(0))
	bytesType   = reflect.TypeOf([]byte(nil))
)

// --- Fast-path decoders ---
// Each wraps the helper Unmarshal uses; hot ones inline the well-formed case (helpers don't inline, ~14%/row).

func decErr(err error) error {
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func decodeSkip(_ []byte, _ any) error {
	return nil
}

func decodeFallback(info TypeInfo) columnDecoder {
	return func(data []byte, dest any) error {
		return Unmarshal(info, data, dest)
	}
}

func decodeVarcharToString(data []byte, dest any) error {
	if p := dest.(*string); p != nil {
		*p = string(data)
		return nil
	}
	return decErr(varchar.DecString(data, dest.(*string)))
}

func decodeVarcharToBytes(data []byte, dest any) error {
	return decErr(varchar.DecBytes(data, dest.(*[]byte)))
}

func decodeTextToString(data []byte, dest any) error {
	if p := dest.(*string); p != nil {
		*p = string(data)
		return nil
	}
	return decErr(text.DecString(data, dest.(*string)))
}

func decodeTextToBytes(data []byte, dest any) error {
	return decErr(text.DecBytes(data, dest.(*[]byte)))
}

func decodeBlobToString(data []byte, dest any) error {
	if p := dest.(*string); p != nil {
		*p = string(data)
		return nil
	}
	return decErr(blob.DecString(data, dest.(*string)))
}

func decodeBlobToBytes(data []byte, dest any) error {
	return decErr(blob.DecBytes(data, dest.(*[]byte)))
}

func decodeIntToInt32(data []byte, dest any) error {
	if p := dest.(*int32); p != nil && len(data) == 4 {
		*p = int32(binary.BigEndian.Uint32(data))
		return nil
	}
	return decErr(cqlint.DecInt32(data, dest.(*int32)))
}

func decodeIntToInt(data []byte, dest any) error {
	if p := dest.(*int); p != nil && len(data) == 4 {
		*p = int(int32(binary.BigEndian.Uint32(data)))
		return nil
	}
	return decErr(cqlint.DecInt(data, dest.(*int)))
}

func decodeIntToInt64(data []byte, dest any) error {
	if p := dest.(*int64); p != nil && len(data) == 4 {
		*p = int64(int32(binary.BigEndian.Uint32(data)))
		return nil
	}
	return decErr(cqlint.DecInt64(data, dest.(*int64)))
}

func decodeBigIntToInt64(data []byte, dest any) error {
	if p := dest.(*int64); p != nil && len(data) == 8 {
		*p = int64(binary.BigEndian.Uint64(data))
		return nil
	}
	return decErr(bigint.DecInt64(data, dest.(*int64)))
}

func decodeBigIntToInt(data []byte, dest any) error {
	if p := dest.(*int); p != nil && len(data) == 8 {
		*p = int(int64(binary.BigEndian.Uint64(data)))
		return nil
	}
	return decErr(bigint.DecInt(data, dest.(*int)))
}

func decodeCounterToInt64(data []byte, dest any) error {
	if p := dest.(*int64); p != nil && len(data) == 8 {
		*p = int64(binary.BigEndian.Uint64(data))
		return nil
	}
	return decErr(counter.DecInt64(data, dest.(*int64)))
}

func decodeCounterToInt(data []byte, dest any) error {
	if p := dest.(*int); p != nil && len(data) == 8 {
		*p = int(int64(binary.BigEndian.Uint64(data)))
		return nil
	}
	return decErr(counter.DecInt(data, dest.(*int)))
}

func decodeSmallIntToInt16(data []byte, dest any) error {
	if p := dest.(*int16); p != nil && len(data) == 2 {
		*p = int16(binary.BigEndian.Uint16(data))
		return nil
	}
	return decErr(smallint.DecInt16(data, dest.(*int16)))
}

func decodeSmallIntToInt(data []byte, dest any) error {
	if p := dest.(*int); p != nil && len(data) == 2 {
		*p = int(int16(binary.BigEndian.Uint16(data)))
		return nil
	}
	return decErr(smallint.DecInt(data, dest.(*int)))
}

func decodeTinyIntToInt8(data []byte, dest any) error {
	if p := dest.(*int8); p != nil && len(data) == 1 {
		*p = int8(data[0])
		return nil
	}
	return decErr(tinyint.DecInt8(data, dest.(*int8)))
}

func decodeBoolToBool(data []byte, dest any) error {
	if p := dest.(*bool); p != nil && len(data) == 1 {
		*p = data[0] != 0
		return nil
	}
	return decErr(boolean.DecBool(data, dest.(*bool)))
}

func decodeFloatToFloat32(data []byte, dest any) error {
	if p := dest.(*float32); p != nil && len(data) == 4 {
		*p = math.Float32frombits(binary.BigEndian.Uint32(data))
		return nil
	}
	return decErr(float.DecFloat32(data, dest.(*float32)))
}

func decodeDoubleToFloat64(data []byte, dest any) error {
	if p := dest.(*float64); p != nil && len(data) == 8 {
		*p = math.Float64frombits(binary.BigEndian.Uint64(data))
		return nil
	}
	return decErr(double.DecFloat64(data, dest.(*float64)))
}

func decodeTimestampToTime(data []byte, dest any) error {
	if p := dest.(*time.Time); p != nil && len(data) == 8 {
		ms := int64(binary.BigEndian.Uint64(data))
		*p = time.Unix(ms/1e3, (ms%1e3)*1e6).UTC()
		return nil
	}
	return decErr(timestamp.DecTime(data, dest.(*time.Time)))
}

func decodeTimestampToInt64(data []byte, dest any) error {
	if p := dest.(*int64); p != nil && len(data) == 8 {
		*p = int64(binary.BigEndian.Uint64(data))
		return nil
	}
	return decErr(timestamp.DecInt64(data, dest.(*int64)))
}

func decodeUUIDToUUID(data []byte, dest any) error {
	if p := dest.(*UUID); p != nil && len(data) == 16 {
		copy(p[:], data)
		return nil
	}
	return decErr(uuid.DecArray(data, (*[16]byte)(dest.(*UUID))))
}

func decodeUUIDToString(data []byte, dest any) error {
	return decErr(uuid.DecString(data, dest.(*string)))
}

func decodeTimeUUIDToUUID(data []byte, dest any) error {
	if p := dest.(*UUID); p != nil && len(data) == 16 {
		copy(p[:], data)
		return nil
	}
	return decErr(timeuuid.DecArray(data, (*[16]byte)(dest.(*UUID))))
}

func decodeTimeUUIDToString(data []byte, dest any) error {
	return decErr(timeuuid.DecString(data, dest.(*string)))
}

func decodeInetToIP(data []byte, dest any) error {
	return decErr(inet.DecNetIP(data, dest.(*net.IP)))
}

func decodeInetToString(data []byte, dest any) error {
	return decErr(inet.DecString(data, dest.(*string)))
}
