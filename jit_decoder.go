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

// jit_decoder.go implements a "JIT-compiled" row decoder that eliminates
// per-row type switches and reflection from the Scan hot path.
//
// On the first Scan/ScanInto call, the (CQL column type, Go destination type)
// pairs are inspected once to select a direct decode function for each column.
// These are cached globally (by schema+type signature) and per-Iter, so
// subsequent rows pay only the cost of a direct function call per column.
//
// Supported fast-path types:
//   - Integers: int, int8, int16, int32, int64
//   - Floats: float32, float64
//   - Bool, string, []byte, time.Time, UUID, net.IP
//
// Named types (type MyInt int32), Unmarshaler implementations, nullable
// pointers (**T), and complex CQL types (collections, UDTs, tuples) all
// fall back transparently to the generic Unmarshal path.

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"reflect"
	"sync"
	"time"
)

// columnDecoder is a fast-path function that decodes a single CQL column
// value from raw bytes into a specific Go destination type. It is resolved
// once (at "compile" time) from the (CQL type, Go type) pair and reused
// for every subsequent row, eliminating per-row type switches and reflection.
type columnDecoder func(data []byte, dest any) error

// compiledRowDecoder is a pre-compiled decoder for a specific row shape:
// a fixed sequence of (CQL column type, Go destination type) pairs.
// It is built lazily on the first Scan call and cached for reuse.
type compiledRowDecoder struct {
	decoders []columnDecoder
}

// decoderCache is a process-wide cache of compiled row decoders.
// It uses sync.Map for lock-free reads on the hot path.
var decoderCache sync.Map // map[string]*compiledRowDecoder

// compileRowDecoder builds a compiledRowDecoder for the given column metadata
// and destination types. For each column, it selects the fastest available
// decoder for the (CQL type, Go type) pair. If no fast path exists, it falls
// back to the generic Unmarshal function.
func compileRowDecoder(columns []ColumnInfo, destTypes []reflect.Type) *compiledRowDecoder {
	decoders := make([]columnDecoder, len(columns))
	for i, col := range columns {
		decoders[i] = compileColumnDecoder(col.TypeInfo, destTypes[i])
	}
	return &compiledRowDecoder{decoders: decoders}
}

// makeDecoderCacheKey builds a unique string key from column metadata and dest types.
func makeDecoderCacheKey(columns []ColumnInfo, destTypes []reflect.Type) string {
	// 11 bytes per column: 1 for protocol version + 2 for CQL type + 8 for Go type pointer.
	// For complex types (collections, UDTs, tuples), we append the TypeInfo
	// string representation to disambiguate different schemas that share
	// the same top-level CQL type code.
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
		// For complex types, the top-level CQL type code is insufficient
		// to distinguish different schemas (e.g. list<int> vs list<bigint>).
		// Append the full TypeInfo string, length-prefixed so this
		// variable-length text's boundary with the next column's fixed-width
		// record is unambiguous — otherwise two different column sequences
		// could serialize to the same key bytes whenever one column's type
		// text happens to read as a shifted concatenation of another
		// layout's bytes.
		switch cqlType {
		case TypeList, TypeSet, TypeMap, TypeTuple, TypeUDT, TypeCustom:
			s := fmt.Sprint(col.TypeInfo)
			n := uint32(len(s))
			buf = append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			buf = append(buf, s...)
		}
	}
	return string(buf)
}

// getOrCompileRowDecoder returns a cached compiled decoder or builds one.
func getOrCompileRowDecoder(columns []ColumnInfo, dest []any) *compiledRowDecoder {
	destTypes := destTypesOf(dest)
	return getOrCompileRowDecoderFromTypes(columns, destTypes)
}

// destTypesOf extracts each destination's reflect.Type (nil for a nil dest).
func destTypesOf(dest []any) []reflect.Type {
	destTypes := make([]reflect.Type, len(dest))
	for i, d := range dest {
		if d == nil {
			destTypes[i] = nil
		} else {
			destTypes[i] = reflect.TypeOf(d)
		}
	}
	return destTypes
}

// getOrCompileRowDecoderFromTypes is the shared slow path: build (or find in
// the process-wide cache) the compiledRowDecoder for this exact (columns,
// destTypes) shape.
func getOrCompileRowDecoderFromTypes(columns []ColumnInfo, destTypes []reflect.Type) *compiledRowDecoder {
	key := makeDecoderCacheKey(columns, destTypes)
	if cached, ok := decoderCache.Load(key); ok {
		return cached.(*compiledRowDecoder)
	}

	dec := compileRowDecoder(columns, destTypes)
	actual, _ := decoderCache.LoadOrStore(key, dec)
	return actual.(*compiledRowDecoder)
}

// destTypesEqualToValues reports whether each value in dest has the same
// reflect.Type (nil for a nil dest) as the corresponding entry in destTypes,
// without allocating a new []reflect.Type to compare against.
func destTypesEqualToValues(dest []any, destTypes []reflect.Type) bool {
	if len(dest) != len(destTypes) {
		return false
	}
	for i, d := range dest {
		var t reflect.Type
		if d != nil {
			t = reflect.TypeOf(d)
		}
		if t != destTypes[i] {
			return false
		}
	}
	return true
}

// cachedJITDecoder pairs a compiledRowDecoder with the exact destTypes shape
// it was compiled for, so a cache hit can be confirmed with a cheap
// comparison against the current Scan call's dest slice instead of
// recomputing a string key.
//
// columnsSig additionally pins a cheap signature of the columns the decoder
// was compiled against (see getOrCompileRowDecoderCached and
// columnsSignature): a cache hit must never be served across two different
// metadata generations for the same *preparedStatment, since the whole point
// of caching here is to skip re-deriving destTypes, not to skip
// re-validating that the columns are the ones this decoder was actually
// built for.
type cachedJITDecoder struct {
	dec        *compiledRowDecoder
	destTypes  []reflect.Type
	columnsSig uint64
}

// getOrCompileRowDecoderCached is getOrCompileRowDecoder, but backed by a
// single-entry cache on stmt (see preparedStatment.jitDecoder) in addition to
// the process-wide sync.Map: a prepared statement is scanned into the same
// call-site destination types on essentially every repeat execution in real
// usage (e.g. a hot query loop reusing the same *WikiPage locals), so after
// the first row this turns decoder resolution into one atomic pointer load
// plus two cheap comparisons — no key string and no sync.Map lookup at all.
//
// Why columns must also be validated, not just destTypes: a *preparedStatment
// can legitimately be shared across a schema change. When the server reports
// RESULT_METADATA_CHANGED (see conn.go's newMetadataID handling), a losing
// goroutine in a concurrent-execution race can end up with iter.preparedStmt
// still pointing at the pre-change statement while iter.meta.columns already
// reflects the new schema — the two are populated independently and are not
// atomically swapped together. If the stale statement already has a warm
// jitDecoder from before the change (the common case — the whole premise of
// this cache is that the same statement is scanned into the same Go types
// repeatedly), validating destTypes alone would let a decoder compiled for
// the OLD column types be silently applied to bytes for the NEW schema:
// wrong-sized reads, misinterpreted values, or a panic, with no error in the
// best case.
//
// columnsSignature hashes column count and each column's top-level CQL type
// code rather than comparing the columns slice's backing-array identity: with
// DisableSkipMetadata's default of true, skip_metadata is never requested, so
// the server sends full column metadata on every response and
// parseResultMetadata (frame.go) allocates a brand-new []ColumnInfo on every
// single call regardless of whether the schema changed — an identity check
// would fail on every call and silently defeat this entire cache under the
// driver's default configuration. The signature is stable across repeated
// identical-schema responses (the common case) while still changing whenever
// a column is added/removed/reordered/retyped at the top level (the case
// this mechanism protects against). It does not distinguish two schemas that
// differ only in a nested element type behind an unchanged outer type (e.g.
// list<int> vs list<bigint>) — accepted as a bounded, narrow trade-off in
// exchange for the signature being computable with no allocation, versus a
// full structural key that would reintroduce this cache's entire allocation
// cost.
//
// stmt may be nil — e.g. non-prepared queries have no preparedStatment to
// cache against — in which case this falls back to the uncached path,
// identical to getOrCompileRowDecoder. That still costs a fresh compile per
// Iter (Iter.rowDecoder itself caches within one Iter's rows), same as
// before this cache existed.
func getOrCompileRowDecoderCached(stmt *preparedStatment, columns []ColumnInfo, dest []any) *compiledRowDecoder {
	if stmt == nil {
		return getOrCompileRowDecoder(columns, dest)
	}

	columnsSig := columnsSignature(columns)
	if cached := stmt.jitDecoder.Load(); cached != nil && cached.columnsSig == columnsSig &&
		destTypesEqualToValues(dest, cached.destTypes) {
		return cached.dec
	}

	destTypes := destTypesOf(dest)
	dec := getOrCompileRowDecoderFromTypes(columns, destTypes)
	// Last writer wins; a lost race just means a future call redoes this
	// resolution instead of caching it, which is always correct and rare.
	stmt.jitDecoder.Store(&cachedJITDecoder{destTypes: destTypes, columnsSig: columnsSig, dec: dec})
	return dec
}

// columnsSignature computes a cheap, allocation-free hash of columns' shape:
// column count plus each column's top-level CQL type code. See
// getOrCompileRowDecoderCached for what this does and does not distinguish.
func columnsSignature(columns []ColumnInfo) uint64 {
	sig := uint64(len(columns))
	for i := range columns {
		sig = sig*31 + uint64(columns[i].TypeInfo.Type())
	}
	return sig
}

// compileColumnDecoder selects the optimal decoder for a (CQL type, Go type) pair.
// For common pairs it returns a direct function; otherwise it falls back to Unmarshal.
// IMPORTANT: We compare against exact reflect.Types (not Kind) to avoid panics
// with named types (e.g. type MyInt int32 would match Kind==Int32 but
// dest.(*int32) assertion would fail on *MyInt).
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

	// Check for Unmarshaler interface — must always take priority.
	if destType.Implements(unmarshalerType) {
		return decodeFallback(info)
	}

	// Check for pointer-to-pointer (nullable) — fall back for now.
	if elemType.Kind() == reflect.Ptr {
		return decodeFallback(info)
	}

	switch cqlType {
	case TypeVarchar, TypeText, TypeAscii:
		if elemType == stringType {
			return decodeVarcharToString
		}
		if elemType == bytesType {
			return decodeVarcharToBytes
		}

	case TypeBlob:
		if elemType == bytesType {
			return decodeBlobToBytes
		}
		if elemType == stringType {
			return decodeVarcharToString
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

	case TypeBigInt, TypeCounter:
		switch elemType {
		case int64Type:
			return decodeBigIntToInt64
		case intType:
			return decodeBigIntToInt
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
			return decodeBigIntToInt64 // timestamp is millis as int64
		}

	case TypeUUID, TypeTimeUUID:
		if elemType == uuidType {
			return decodeUUIDToUUID
		}
		if elemType == stringType {
			return decodeUUIDToString
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

	// Exact primitive types for fast-path matching.
	// Named types (e.g. type MyInt int32) have the same Kind but different
	// reflect.Type, so we must compare against these exact types to avoid
	// type assertion panics in the decoders.
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

func decodeSkip(_ []byte, _ any) error {
	return nil
}

func decodeFallback(info TypeInfo) columnDecoder {
	return func(data []byte, dest any) error {
		return Unmarshal(info, data, dest)
	}
}

func decodeVarcharToString(data []byte, dest any) error {
	p := dest.(*string)
	if len(data) == 0 {
		*p = ""
		return nil
	}
	*p = string(data)
	return nil
}

func decodeVarcharToBytes(data []byte, dest any) error {
	p := dest.(*[]byte)
	if data == nil {
		*p = nil
		return nil
	}
	if len(data) == 0 {
		*p = make([]byte, 0)
		return nil
	}
	// Copy (to avoid retaining the framer's buffer) by reusing *p's existing
	// backing array when it already has enough capacity, matching
	// varchar.DecBytes's allocation-avoidance on a hot Scan loop that reuses
	// the same destination slice across rows — only grows/allocates when
	// *p's capacity is insufficient.
	*p = append((*p)[:0], data...)
	return nil
}

func decodeBlobToBytes(data []byte, dest any) error {
	return decodeVarcharToBytes(data, dest)
}

func decodeIntToInt32(data []byte, dest any) error {
	p := dest.(*int32)
	switch len(data) {
	case 0:
		*p = 0
	case 4:
		*p = int32(binary.BigEndian.Uint32(data))
	default:
		return unmarshalErrorf("unmarshal int: expected 0 or 4 bytes, got %d", len(data))
	}
	return nil
}

func decodeIntToInt(data []byte, dest any) error {
	p := dest.(*int)
	switch len(data) {
	case 0:
		*p = 0
	case 4:
		*p = int(int32(binary.BigEndian.Uint32(data)))
	default:
		return unmarshalErrorf("unmarshal int: expected 0 or 4 bytes, got %d", len(data))
	}
	return nil
}

func decodeIntToInt64(data []byte, dest any) error {
	p := dest.(*int64)
	switch len(data) {
	case 0:
		*p = 0
	case 4:
		*p = int64(int32(binary.BigEndian.Uint32(data)))
	default:
		return unmarshalErrorf("unmarshal int: expected 0 or 4 bytes, got %d", len(data))
	}
	return nil
}

func decodeBigIntToInt64(data []byte, dest any) error {
	p := dest.(*int64)
	switch len(data) {
	case 0:
		*p = 0
	case 8:
		*p = int64(binary.BigEndian.Uint64(data))
	default:
		return unmarshalErrorf("unmarshal bigint: expected 0 or 8 bytes, got %d", len(data))
	}
	return nil
}

func decodeBigIntToInt(data []byte, dest any) error {
	p := dest.(*int)
	switch len(data) {
	case 0:
		*p = 0
	case 8:
		*p = int(int64(binary.BigEndian.Uint64(data)))
	default:
		return unmarshalErrorf("unmarshal bigint: expected 0 or 8 bytes, got %d", len(data))
	}
	return nil
}

func decodeSmallIntToInt16(data []byte, dest any) error {
	p := dest.(*int16)
	switch len(data) {
	case 0:
		*p = 0
	case 2:
		*p = int16(binary.BigEndian.Uint16(data))
	default:
		return unmarshalErrorf("unmarshal smallint: expected 0 or 2 bytes, got %d", len(data))
	}
	return nil
}

func decodeSmallIntToInt(data []byte, dest any) error {
	p := dest.(*int)
	switch len(data) {
	case 0:
		*p = 0
	case 2:
		*p = int(int16(binary.BigEndian.Uint16(data)))
	default:
		return unmarshalErrorf("unmarshal smallint: expected 0 or 2 bytes, got %d", len(data))
	}
	return nil
}

func decodeTinyIntToInt8(data []byte, dest any) error {
	p := dest.(*int8)
	switch len(data) {
	case 0:
		*p = 0
	case 1:
		*p = int8(data[0])
	default:
		return unmarshalErrorf("unmarshal tinyint: expected 0 or 1 bytes, got %d", len(data))
	}
	return nil
}

func decodeBoolToBool(data []byte, dest any) error {
	p := dest.(*bool)
	switch len(data) {
	case 0:
		*p = false
	case 1:
		*p = data[0] != 0
	default:
		return unmarshalErrorf("unmarshal boolean: expected 0 or 1 bytes, got %d", len(data))
	}
	return nil
}

func decodeFloatToFloat32(data []byte, dest any) error {
	p := dest.(*float32)
	switch len(data) {
	case 0:
		*p = 0
	case 4:
		*p = math.Float32frombits(binary.BigEndian.Uint32(data))
	default:
		return unmarshalErrorf("unmarshal float: expected 0 or 4 bytes, got %d", len(data))
	}
	return nil
}

func decodeDoubleToFloat64(data []byte, dest any) error {
	p := dest.(*float64)
	switch len(data) {
	case 0:
		*p = 0
	case 8:
		*p = math.Float64frombits(binary.BigEndian.Uint64(data))
	default:
		return unmarshalErrorf("unmarshal double: expected 0 or 8 bytes, got %d", len(data))
	}
	return nil
}

func decodeTimestampToTime(data []byte, dest any) error {
	p := dest.(*time.Time)
	switch len(data) {
	case 0:
		*p = time.Time{}
	case 8:
		msec := int64(binary.BigEndian.Uint64(data))
		*p = time.Unix(msec/1e3, (msec%1e3)*1e6).UTC()
	default:
		return unmarshalErrorf("unmarshal timestamp: expected 0 or 8 bytes, got %d", len(data))
	}
	return nil
}

func decodeUUIDToUUID(data []byte, dest any) error {
	p := dest.(*UUID)
	switch len(data) {
	case 0:
		*p = UUID{}
	case 16:
		copy(p[:], data)
	default:
		return unmarshalErrorf("unmarshal uuid: expected 0 or 16 bytes, got %d", len(data))
	}
	return nil
}

func decodeUUIDToString(data []byte, dest any) error {
	p := dest.(*string)
	switch len(data) {
	case 0:
		*p = ""
	case 16:
		var u UUID
		copy(u[:], data)
		*p = u.String()
	default:
		return unmarshalErrorf("unmarshal uuid: expected 0 or 16 bytes, got %d", len(data))
	}
	return nil
}

func decodeInetToIP(data []byte, dest any) error {
	p := dest.(*net.IP)
	switch len(data) {
	case 0:
		if data == nil {
			*p = nil
		} else {
			*p = make(net.IP, 0)
		}
	case 4:
		buf := make([]byte, 4)
		copy(buf, data)
		*p = net.IP(buf)
	case 16:
		buf := make([]byte, 16)
		copy(buf, data)
		ip := net.IP(buf)
		if v4 := ip.To4(); v4 != nil {
			*p = v4
		} else {
			*p = ip
		}
	default:
		return unmarshalErrorf("unmarshal inet: expected 0, 4 or 16 bytes, got %d", len(data))
	}
	return nil
}

func decodeInetToString(data []byte, dest any) error {
	p := dest.(*string)
	switch len(data) {
	case 0:
		if data == nil {
			*p = ""
		} else {
			*p = "0.0.0.0"
		}
	case 4, 16:
		*p = net.IP(data).String()
	default:
		return unmarshalErrorf("unmarshal inet: expected 0, 4 or 16 bytes, got %d", len(data))
	}
	return nil
}
