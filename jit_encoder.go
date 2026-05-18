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

// jit_encoder.go: compiled parameter encoder that removes per-call type
// switches and reflection from Marshal. Named types, Marshalers and complex
// CQL types fall back to the generic Marshal path.

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
	srcTypes := make([]reflect.Type, len(values))
	for i, v := range values {
		if v == nil {
			srcTypes[i] = nil
		} else {
			srcTypes[i] = reflect.TypeOf(v)
		}
	}

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
// Each calls the serialization helper directly and wraps errors as Marshal does.

func encErr(b []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return b, nil
}

// compileNilEncoder picks the encoder for an untyped nil: CQL NULL for native
// and collection columns; UDT, custom and tuple defer to Marshal, which rejects it.
func compileNilEncoder(info TypeInfo, cqlType Type) columnEncoder {
	switch cqlType {
	case TypeUDT, TypeCustom, TypeTuple:
		return encodeFallback(info)
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
	p := value.(*UUID)
	if p == nil {
		return nil, nil
	}
	// The encoded slice aliases the caller's UUID until the frame is built, like other pointer encoders.
	return encErr(uuid.EncArrayR((*[16]byte)(p)))
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
