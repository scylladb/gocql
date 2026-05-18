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
	"math"
	"net"
	"reflect"
	"sync"
	"time"
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

// encoderCache is a process-wide cache of compiled param encoders.
var encoderCache sync.Map // map[string]*compiledParamEncoder

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
			buf = append(buf, fmt.Sprint(col.TypeInfo)...)
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
	actual, _ := encoderCache.LoadOrStore(key, enc)
	return actual.(*compiledParamEncoder)
}

// compileColumnEncoder selects the optimal encoder for a (CQL type, Go type) pair.
func compileColumnEncoder(info TypeInfo, srcType reflect.Type) columnEncoder {
	if srcType == nil {
		return encodeNil
	}

	cqlType := info.Type()

	// Check for Marshaler interface — must always take priority.
	if srcType.Implements(marshalerType) {
		return encodeFallback(info)
	}

	// Handle pointer types: *T where T is a primitive.
	if srcType.Kind() == reflect.Ptr {
		elemType := srcType.Elem()
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
			return encodeInt64ToBigInt // timestamp is millis as int64
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

func encodeNil(_ any) ([]byte, error) {
	return nil, nil
}

func encodeFallback(info TypeInfo) columnEncoder {
	return func(value any) ([]byte, error) {
		return Marshal(info, value)
	}
}

func encodeStringToVarchar(value any) ([]byte, error) {
	v := value.(string)
	if v == "" {
		return make([]byte, 0), nil
	}
	return []byte(v), nil
}

func encodeBytesToVarchar(value any) ([]byte, error) {
	v := value.([]byte)
	return v, nil
}

func encodeInt32ToInt(value any) ([]byte, error) {
	v := value.(int32)
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

func encodeIntToInt(value any) ([]byte, error) {
	v := value.(int)
	if v > math.MaxInt32 || v < math.MinInt32 {
		return nil, marshalErrorf("marshal int: value %d out of range", v)
	}
	i := int32(v)
	return []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}, nil
}

func encodeInt64ToInt(value any) ([]byte, error) {
	v := value.(int64)
	if v > math.MaxInt32 || v < math.MinInt32 {
		return nil, marshalErrorf("marshal int: value %d out of range", v)
	}
	i := int32(v)
	return []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}, nil
}

func encodeInt16ToInt(value any) ([]byte, error) {
	v := int32(value.(int16))
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

func encodeInt8ToInt(value any) ([]byte, error) {
	v := int32(value.(int8))
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

func encodeInt64ToBigInt(value any) ([]byte, error) {
	v := value.(int64)
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

func encodeIntToBigInt(value any) ([]byte, error) {
	v := int64(value.(int))
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

func encodeInt16ToSmallInt(value any) ([]byte, error) {
	v := value.(int16)
	return []byte{byte(v >> 8), byte(v)}, nil
}

func encodeIntToSmallInt(value any) ([]byte, error) {
	v := value.(int)
	if v > math.MaxInt16 || v < math.MinInt16 {
		return nil, marshalErrorf("marshal smallint: value %d out of range", v)
	}
	i := int16(v)
	return []byte{byte(i >> 8), byte(i)}, nil
}

func encodeInt8ToTinyInt(value any) ([]byte, error) {
	v := value.(int8)
	return []byte{byte(v)}, nil
}

func encodeBoolToBool(value any) ([]byte, error) {
	v := value.(bool)
	if v {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func encodeFloat32ToFloat(value any) ([]byte, error) {
	v := math.Float32bits(value.(float32))
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

func encodeFloat64ToDouble(value any) ([]byte, error) {
	v := math.Float64bits(value.(float64))
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

// Timestamp min/max boundaries matching upstream.
var (
	encMaxTimestamp = time.Date(292278994, 8, 17, 7, 12, 55, 807*1000000, time.UTC)
	encMinTimestamp = time.Date(-292275055, 5, 16, 16, 47, 4, 192*1000000, time.UTC)
)

func encodeTimeToTimestamp(value any) ([]byte, error) {
	v := value.(time.Time)
	if v.After(encMaxTimestamp) || v.Before(encMinTimestamp) {
		return nil, marshalErrorf("marshal timestamp: value out of range")
	}
	if v.IsZero() {
		return make([]byte, 0), nil
	}
	ms := v.UTC().UnixMilli()
	return []byte{byte(ms >> 56), byte(ms >> 48), byte(ms >> 40), byte(ms >> 32),
		byte(ms >> 24), byte(ms >> 16), byte(ms >> 8), byte(ms)}, nil
}

func encodeUUIDToUUID(value any) ([]byte, error) {
	v := value.(UUID)
	buf := make([]byte, 16)
	copy(buf, v[:])
	return buf, nil
}

func encodeIPToInet(value any) ([]byte, error) {
	v := value.(net.IP)
	if len(v) == 0 {
		return nil, nil
	}
	if v4 := v.To4(); v4 != nil {
		return []byte{v4[0], v4[1], v4[2], v4[3]}, nil
	}
	if len(v) == 16 {
		buf := make([]byte, 16)
		copy(buf, v)
		return buf, nil
	}
	return nil, marshalErrorf("marshal inet: invalid IP length %d", len(v))
}

func encodeStringToInet(value any) ([]byte, error) {
	v := value.(string)
	if v == "" {
		return make([]byte, 0), nil
	}
	ip := net.ParseIP(v)
	if ip == nil {
		return nil, marshalErrorf("marshal inet: invalid IP address %q", v)
	}
	if v4 := ip.To4(); v4 != nil {
		return []byte{v4[0], v4[1], v4[2], v4[3]}, nil
	}
	buf := make([]byte, 16)
	copy(buf, ip.To16())
	return buf, nil
}

// --- Pointer-type encoders ---

func encodePtrStringToVarchar(value any) ([]byte, error) {
	v := value.(*string)
	if v == nil {
		return nil, nil
	}
	return encodeStringToVarchar(*v)
}

func encodePtrBytesToBlob(value any) ([]byte, error) {
	v := value.(*[]byte)
	if v == nil {
		return nil, nil
	}
	return *v, nil
}

func encodePtrInt32ToInt(value any) ([]byte, error) {
	v := value.(*int32)
	if v == nil {
		return nil, nil
	}
	return encodeInt32ToInt(*v)
}

func encodePtrIntToInt(value any) ([]byte, error) {
	v := value.(*int)
	if v == nil {
		return nil, nil
	}
	return encodeIntToInt(*v)
}

func encodePtrInt64ToBigInt(value any) ([]byte, error) {
	v := value.(*int64)
	if v == nil {
		return nil, nil
	}
	return encodeInt64ToBigInt(*v)
}

func encodePtrBoolToBool(value any) ([]byte, error) {
	v := value.(*bool)
	if v == nil {
		return nil, nil
	}
	return encodeBoolToBool(*v)
}

func encodePtrFloat32ToFloat(value any) ([]byte, error) {
	v := value.(*float32)
	if v == nil {
		return nil, nil
	}
	return encodeFloat32ToFloat(*v)
}

func encodePtrFloat64ToDouble(value any) ([]byte, error) {
	v := value.(*float64)
	if v == nil {
		return nil, nil
	}
	return encodeFloat64ToDouble(*v)
}

func encodePtrTimeToTimestamp(value any) ([]byte, error) {
	v := value.(*time.Time)
	if v == nil {
		return nil, nil
	}
	return encodeTimeToTimestamp(*v)
}

func encodePtrUUIDToUUID(value any) ([]byte, error) {
	v := value.(*UUID)
	if v == nil {
		return nil, nil
	}
	return encodeUUIDToUUID(*v)
}

func encodePtrInt16ToSmallInt(value any) ([]byte, error) {
	v := value.(*int16)
	if v == nil {
		return nil, nil
	}
	return encodeInt16ToSmallInt(*v)
}

func encodePtrInt8ToTinyInt(value any) ([]byte, error) {
	v := value.(*int8)
	if v == nil {
		return nil, nil
	}
	return encodeInt8ToTinyInt(*v)
}

func encodePtrIPToInet(value any) ([]byte, error) {
	v := value.(*net.IP)
	if v == nil {
		return nil, nil
	}
	return encodeIPToInet(*v)
}

func encodePtrStringToInet(value any) ([]byte, error) {
	v := value.(*string)
	if v == nil {
		return nil, nil
	}
	return encodeStringToInet(*v)
}
