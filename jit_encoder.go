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

// jit_encoder.go implements a "JIT-compiled" parameter encoder that eliminates
// per-call type switches and reflection from the Marshal hot path.
//
// On the first call, the (CQL column type, Go source type) pairs are inspected
// once to select a direct encode function for each parameter. These are cached
// globally (by schema+type signature) via sync.Map, so subsequent executions
// of the same prepared statement with the same Go types pay only the cost of
// a direct function call per parameter.
//
// Supported fast-path types:
//   - Integers: int, int8, int16, int32, int64
//   - Floats: float32, float64
//   - Bool, string, []byte, time.Time, UUID, net.IP
//
// Named types, Marshaler implementations, and complex CQL types all
// fall back transparently to the generic Marshal path.

import (
	"fmt"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql/serialization/bigint"
	"github.com/gocql/gocql/serialization/blob"
	"github.com/gocql/gocql/serialization/boolean"
	"github.com/gocql/gocql/serialization/cqlint"
	"github.com/gocql/gocql/serialization/double"
	"github.com/gocql/gocql/serialization/float"
	"github.com/gocql/gocql/serialization/inet"
	"github.com/gocql/gocql/serialization/smallint"
	"github.com/gocql/gocql/serialization/timestamp"
	"github.com/gocql/gocql/serialization/tinyint"
	"github.com/gocql/gocql/serialization/uuid"
	"github.com/gocql/gocql/serialization/varchar"
)

// columnEncoder is a fast-path function that encodes a single Go value
// into CQL wire bytes. It is resolved once (at "compile" time) from the
// (CQL type, Go type) pair and reused for every subsequent execution.
type columnEncoder func(value any) ([]byte, error)

// compiledParamEncoder is a pre-compiled encoder for a specific parameter shape:
// a fixed sequence of (CQL column type, Go source type) pairs.
type compiledParamEncoder struct {
	encoders []columnEncoder
}

// encodeInto runs the compiled encoders into dst. Like Marshal, it turns a
// runtime panic from a concurrently mutated value into an error (#1160).
func (e *compiledParamEncoder) encodeInto(columns []ColumnInfo, values []any, dst []queryValues) (err error) {
	i := 0
	defer func() {
		if r := recover(); r != nil {
			re, ok := r.(runtime.Error)
			if !ok {
				panic(r)
			}
			err = MarshalError{msg: fmt.Sprintf("gocql: failed to marshal %s value (%T)", columns[i].TypeInfo, values[i]), cause: re}
		}
	}()
	for ; i < len(values); i++ {
		val, encErr := e.encoders[i](values[i])
		if encErr != nil {
			return encErr
		}
		dst[i].value = val
	}
	return nil
}

// encoderCache is a process-wide cache of compiled param encoders.
// Bounded by maxCompiledCacheEntries; see jit_decoder.go for the rationale.
var (
	encoderCache      sync.Map // map[string]*compiledParamEncoder
	encoderCacheCount atomic.Int64
)

// compileParamEncoder builds a compiledParamEncoder for the given column metadata
// and source value types.
func compileParamEncoder(columns []ColumnInfo, srcTypes []reflect.Type) *compiledParamEncoder {
	encoders := make([]columnEncoder, len(columns))
	for i, col := range columns {
		encoders[i] = compileColumnEncoder(col.TypeInfo, srcTypes[i])
	}
	return &compiledParamEncoder{encoders: encoders}
}

// makeEncoderCacheKey builds a unique string key from column metadata and source types.
func makeEncoderCacheKey(columns []ColumnInfo, srcTypes []reflect.Type) string {
	// 11 bytes per column: 1 for protocol version + 2 for CQL type + 8 for Go type pointer.
	buf := make([]byte, 0, len(columns)*11)
	for i, col := range columns {
		buf = append(buf, col.TypeInfo.Version())
		cqlType := col.TypeInfo.Type()
		buf = append(buf, byte(cqlType>>8), byte(cqlType))
		if srcTypes[i] == nil {
			buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
		} else {
			ptr := reflect.ValueOf(srcTypes[i]).Pointer()
			buf = append(buf,
				byte(ptr>>56), byte(ptr>>48), byte(ptr>>40), byte(ptr>>32),
				byte(ptr>>24), byte(ptr>>16), byte(ptr>>8), byte(ptr))
		}
		switch cqlType {
		case TypeList, TypeSet, TypeMap, TypeTuple, TypeUDT, TypeCustom:
			buf = appendTypeInfoKey(buf, col.TypeInfo)
		}
	}
	return string(buf)
}

// getOrCompileParamEncoder returns a cached compiled encoder or builds one.
func getOrCompileParamEncoder(columns []ColumnInfo, values []any) *compiledParamEncoder {
	srcTypes := srcTypesOf(values)
	return getOrCompileParamEncoderFromTypes(columns, srcTypes)
}

// srcTypesOf extracts each value's reflect.Type (nil for a nil value).
func srcTypesOf(values []any) []reflect.Type {
	srcTypes := make([]reflect.Type, len(values))
	for i, v := range values {
		if v == nil {
			srcTypes[i] = nil
		} else {
			srcTypes[i] = reflect.TypeOf(v)
		}
	}
	return srcTypes
}

// getOrCompileParamEncoderFromTypes is the shared slow path: build (or find
// in the process-wide cache) the compiledParamEncoder for this exact
// (columns, srcTypes) shape. Both getOrCompileParamEncoder and a
// getOrCompileParamEncoderCached miss go through here.
func getOrCompileParamEncoderFromTypes(columns []ColumnInfo, srcTypes []reflect.Type) *compiledParamEncoder {
	key := makeEncoderCacheKey(columns, srcTypes)
	if cached, ok := encoderCache.Load(key); ok {
		return cached.(*compiledParamEncoder)
	}

	enc := compileParamEncoder(columns, srcTypes)
	if encoderCacheCount.Load() >= maxCompiledCacheEntries {
		clearCompiledCache(&encoderCache, &encoderCacheCount)
	}
	actual, loaded := encoderCache.LoadOrStore(key, enc)
	if !loaded {
		encoderCacheCount.Add(1)
	}
	return actual.(*compiledParamEncoder)
}

// valuesMatchSrcTypes reports whether each value's reflect.TypeOf (nil for a
// nil value) equals the corresponding entry in srcTypes, without allocating
// a new []reflect.Type to compare against.
func valuesMatchSrcTypes(values []any, srcTypes []reflect.Type) bool {
	if len(values) != len(srcTypes) {
		return false
	}
	for i, v := range values {
		var t reflect.Type
		if v != nil {
			t = reflect.TypeOf(v)
		}
		if t != srcTypes[i] {
			return false
		}
	}
	return true
}

// getOrCompileParamEncoderCached is getOrCompileParamEncoder, but backed by a
// single-entry cache on stmt (see preparedStatment.jitEncoder) in addition to
// the process-wide sync.Map: a prepared statement is executed with the same
// call-site argument types on essentially every repeat execution in real
// usage, so after the first call this turns encoder resolution into one
// atomic pointer load plus a comparison against the existing values slice —
// no new []reflect.Type, no key string, and no sync.Map lookup at all.
//
// The columns come from stmt itself rather than from the caller, so a cached
// encoder cannot be served for a different schema than it was compiled for.
// (resolveRowDecoder has to compare columns at runtime because its columns
// arrive from the Iter's own metadata, which a page turn can replace
// independently of the statement; here the two cannot diverge.) A
// preparedStatment's metadata is immutable once built — a metadata change
// allocates a new one — so stmt.request.columns is stable for the cache entry.
//
// The cache holds one entry, which assumes a statement is executed with one
// argument shape. A call site alternating shapes for the same statement — say
// a value and an untyped nil for the same optional parameter — misses every
// time and pays a resolution plus a store into a process-wide atomic. That is
// still cheaper than the reflective Marshal it replaces, so it degrades to no
// worse than not caching, but it does not speed such a caller up.
func getOrCompileParamEncoderCached(stmt *preparedStatment, values []any) *compiledParamEncoder {
	if cached := stmt.jitEncoder.Load(); cached != nil && valuesMatchSrcTypes(values, cached.srcTypes) {
		return cached.enc
	}

	srcTypes := srcTypesOf(values)
	enc := getOrCompileParamEncoderFromTypes(stmt.request.columns, srcTypes)
	// Last writer wins; a lost race just means a future call redoes this
	// resolution instead of caching it, which is always correct and rare.
	stmt.jitEncoder.Store(&cachedJITEncoder{srcTypes: srcTypes, enc: enc})
	return enc
}

// compileColumnEncoder selects the optimal encoder for a (CQL type, Go type) pair.
func compileColumnEncoder(info TypeInfo, srcType reflect.Type) columnEncoder {
	cqlType := info.Type()

	if srcType == nil {
		return compileNilEncoder(info, cqlType)
	}

	// Check for Marshaler interface — must always take priority.
	if srcType.Implements(marshalerType) {
		return encodeFallback(info)
	}

	// Handle pointer types: *T where T is a primitive.
	if srcType.Kind() == reflect.Ptr {
		elemType := srcType.Elem()
		// A defined pointer type (type P *int32) is not *int32, which the fast paths assert.
		if srcType != reflect.PointerTo(elemType) {
			return encodeFallback(info)
		}
		return compilePointerEncoder(info, cqlType, elemType)
	}

	// Non-pointer value types.
	switch cqlType {
	case TypeVarchar, TypeText, TypeAscii:
		if srcType == stringType {
			return encodeStringToVarchar
		}
		if srcType == bytesType {
			return encodeBytesToVarchar
		}

	case TypeBlob:
		if srcType == bytesType {
			return encodeBytesToVarchar
		}
		if srcType == stringType {
			return encodeStringToVarchar
		}

	case TypeInt:
		switch srcType {
		case int32Type:
			return encodeInt32ToInt
		case intType:
			return encodeIntToInt
		case int64Type:
			return encodeInt64ToInt
		case int16Type:
			return encodeInt16ToInt
		case int8Type:
			return encodeInt8ToInt
		}

	case TypeBigInt, TypeCounter:
		switch srcType {
		case int64Type:
			return encodeInt64ToBigInt
		case intType:
			return encodeIntToBigInt
		}

	case TypeSmallInt:
		switch srcType {
		case int16Type:
			return encodeInt16ToSmallInt
		case intType:
			return encodeIntToSmallInt
		}

	case TypeTinyInt:
		if srcType == int8Type {
			return encodeInt8ToTinyInt
		}

	case TypeBoolean:
		if srcType == boolType {
			return encodeBoolToBool
		}

	case TypeFloat:
		if srcType == float32Type {
			return encodeFloat32ToFloat
		}

	case TypeDouble:
		if srcType == float64Type {
			return encodeFloat64ToDouble
		}

	case TypeTimestamp:
		if srcType == timeType {
			return encodeTimeToTimestamp
		}
		if srcType == int64Type {
			return encodeInt64ToTimestamp
		}

	case TypeUUID, TypeTimeUUID:
		if srcType == uuidType {
			return encodeUUIDToUUID
		}

	case TypeInet:
		if srcType == ipType {
			return encodeIPToInet
		}
		if srcType == stringType {
			return encodeStringToInet
		}
	}

	return encodeFallback(info)
}

// compilePointerEncoder handles *T → CQL type encoding.
func compilePointerEncoder(info TypeInfo, cqlType Type, elemType reflect.Type) columnEncoder {
	switch cqlType {
	case TypeVarchar, TypeText, TypeAscii:
		if elemType == stringType {
			return encodePtrStringToVarchar
		}

	case TypeBlob:
		if elemType == bytesType {
			return encodePtrBytesToBlob
		}

	case TypeInt:
		switch elemType {
		case int32Type:
			return encodePtrInt32ToInt
		case intType:
			return encodePtrIntToInt
		}

	case TypeBigInt, TypeCounter:
		if elemType == int64Type {
			return encodePtrInt64ToBigInt
		}

	case TypeBoolean:
		if elemType == boolType {
			return encodePtrBoolToBool
		}

	case TypeFloat:
		if elemType == float32Type {
			return encodePtrFloat32ToFloat
		}

	case TypeDouble:
		if elemType == float64Type {
			return encodePtrFloat64ToDouble
		}

	case TypeTimestamp:
		if elemType == timeType {
			return encodePtrTimeToTimestamp
		}

	case TypeUUID, TypeTimeUUID:
		if elemType == uuidType {
			return encodePtrUUIDToUUID
		}

	case TypeSmallInt:
		if elemType == int16Type {
			return encodePtrInt16ToSmallInt
		}

	case TypeTinyInt:
		if elemType == int8Type {
			return encodePtrInt8ToTinyInt
		}

	case TypeInet:
		if elemType == ipType {
			return encodePtrIPToInet
		}
		if elemType == stringType {
			return encodePtrStringToInet
		}
	}

	return encodeFallback(info)
}

// Sentinel reflect.Type for Marshaler interface.
var marshalerType = reflect.TypeOf((*Marshaler)(nil)).Elem()

// --- Fast-path encoders ---
//
// Each one skips Marshal's type switches by calling the serialization
// package's typed helper directly, and wraps errors exactly as Marshal does.

func encErr(b []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return b, nil
}

// compileNilEncoder picks the encoder for an untyped nil value. Marshal
// accepts nil only for native and collection columns, encoding it as CQL NULL;
// it rejects nil for UDT and custom columns, so those defer to it for the
// error. A nil bound to a tuple column makes Marshal panic (marshalTuple
// reflects on a zero Value) — report it instead of propagating that.
func compileNilEncoder(info TypeInfo, cqlType Type) columnEncoder {
	switch cqlType {
	case TypeUDT, TypeCustom:
		return encodeFallback(info)
	case TypeTuple:
		return func(any) ([]byte, error) {
			return nil, marshalErrorf("can not marshal nil into %s", info)
		}
	}
	return encodeNil
}

func encodeNil(_ any) ([]byte, error) {
	return nil, nil
}

func encodeFallback(info TypeInfo) columnEncoder {
	return func(value any) ([]byte, error) {
		return Marshal(info, value)
	}
}

func encodeStringToVarchar(value any) ([]byte, error) {
	return encErr(varchar.EncString(value.(string)))
}

func encodeBytesToVarchar(value any) ([]byte, error) {
	return encErr(varchar.EncBytes(value.([]byte)))
}

func encodeInt32ToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncInt32(value.(int32)))
}

func encodeIntToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncInt(value.(int)))
}

func encodeInt64ToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncInt64(value.(int64)))
}

func encodeInt16ToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncInt16(value.(int16)))
}

func encodeInt8ToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncInt8(value.(int8)))
}

func encodeInt64ToBigInt(value any) ([]byte, error) {
	return encErr(bigint.EncInt64(value.(int64)))
}

func encodeIntToBigInt(value any) ([]byte, error) {
	return encErr(bigint.EncInt(value.(int)))
}

func encodeInt16ToSmallInt(value any) ([]byte, error) {
	return encErr(smallint.EncInt16(value.(int16)))
}

func encodeIntToSmallInt(value any) ([]byte, error) {
	return encErr(smallint.EncInt(value.(int)))
}

func encodeInt8ToTinyInt(value any) ([]byte, error) {
	return encErr(tinyint.EncInt8(value.(int8)))
}

func encodeBoolToBool(value any) ([]byte, error) {
	return encErr(boolean.EncBool(value.(bool)))
}

func encodeFloat32ToFloat(value any) ([]byte, error) {
	return encErr(float.EncFloat32(value.(float32)))
}

func encodeFloat64ToDouble(value any) ([]byte, error) {
	return encErr(double.EncFloat64(value.(float64)))
}

func encodeTimeToTimestamp(value any) ([]byte, error) {
	return encErr(timestamp.EncTime(value.(time.Time)))
}

func encodeInt64ToTimestamp(value any) ([]byte, error) {
	return encErr(timestamp.EncInt64(value.(int64)))
}

func encodeUUIDToUUID(value any) ([]byte, error) {
	return encErr(uuid.EncArray(value.(UUID)))
}

func encodeIPToInet(value any) ([]byte, error) {
	return encErr(inet.EncNetIP(value.(net.IP)))
}

func encodeStringToInet(value any) ([]byte, error) {
	return encErr(inet.EncString(value.(string)))
}

// --- Pointer-type encoders ---

func encodePtrStringToVarchar(value any) ([]byte, error) {
	return encErr(varchar.EncStringR(value.(*string)))
}

func encodePtrBytesToBlob(value any) ([]byte, error) {
	return encErr(blob.EncBytesR(value.(*[]byte)))
}

func encodePtrInt32ToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncInt32R(value.(*int32)))
}

func encodePtrIntToInt(value any) ([]byte, error) {
	return encErr(cqlint.EncIntR(value.(*int)))
}

func encodePtrInt64ToBigInt(value any) ([]byte, error) {
	return encErr(bigint.EncInt64R(value.(*int64)))
}

func encodePtrBoolToBool(value any) ([]byte, error) {
	return encErr(boolean.EncBoolR(value.(*bool)))
}

func encodePtrFloat32ToFloat(value any) ([]byte, error) {
	return encErr(float.EncFloat32R(value.(*float32)))
}

func encodePtrFloat64ToDouble(value any) ([]byte, error) {
	return encErr(double.EncFloat64R(value.(*float64)))
}

func encodePtrTimeToTimestamp(value any) ([]byte, error) {
	return encErr(timestamp.EncTimeR(value.(*time.Time)))
}

func encodePtrUUIDToUUID(value any) ([]byte, error) {
	return encErr(uuid.EncArrayR((*[16]byte)(value.(*UUID))))
}

func encodePtrInt16ToSmallInt(value any) ([]byte, error) {
	return encErr(smallint.EncInt16R(value.(*int16)))
}

func encodePtrInt8ToTinyInt(value any) ([]byte, error) {
	return encErr(tinyint.EncInt8R(value.(*int8)))
}

func encodePtrIPToInet(value any) ([]byte, error) {
	return encErr(inet.EncNetIPr(value.(*net.IP)))
}

func encodePtrStringToInet(value any) ([]byte, error) {
	return encErr(inet.EncStringR(value.(*string)))
}
