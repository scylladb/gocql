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
		// Append the full TypeInfo string to prevent cache collisions.
		switch cqlType {
		case TypeList, TypeSet, TypeMap, TypeTuple, TypeUDT, TypeCustom:
			buf = append(buf, fmt.Sprint(col.TypeInfo)...)
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

	key := makeDecoderCacheKey(columns, destTypes)
	if cached, ok := decoderCache.Load(key); ok {
		return cached.(*compiledRowDecoder)
	}

	dec := compileRowDecoder(columns, destTypes)
	actual, _ := decoderCache.LoadOrStore(key, dec)
	return actual.(*compiledRowDecoder)
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
	// Copy to avoid retaining framer buffer.
	buf := make([]byte, len(data))
	copy(buf, data)
	*p = buf
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
