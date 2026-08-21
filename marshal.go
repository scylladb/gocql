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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"gopkg.in/inf.v0"

	"github.com/gocql/gocql/serialization/ascii"
	"github.com/gocql/gocql/serialization/bigint"
	"github.com/gocql/gocql/serialization/blob"
	"github.com/gocql/gocql/serialization/boolean"
	"github.com/gocql/gocql/serialization/counter"
	"github.com/gocql/gocql/serialization/cqlint"
	"github.com/gocql/gocql/serialization/cqltime"
	"github.com/gocql/gocql/serialization/date"
	"github.com/gocql/gocql/serialization/decimal"
	"github.com/gocql/gocql/serialization/double"
	"github.com/gocql/gocql/serialization/duration"
	"github.com/gocql/gocql/serialization/float"
	"github.com/gocql/gocql/serialization/inet"
	"github.com/gocql/gocql/serialization/smallint"
	"github.com/gocql/gocql/serialization/text"
	"github.com/gocql/gocql/serialization/timestamp"
	"github.com/gocql/gocql/serialization/timeuuid"
	"github.com/gocql/gocql/serialization/tinyint"
	"github.com/gocql/gocql/serialization/uuid"
	"github.com/gocql/gocql/serialization/varchar"
	"github.com/gocql/gocql/serialization/varint"
)

var (
	emptyValue reflect.Value
)

var (
	ErrorUDTUnavailable = errors.New("UDT are not available on protocols less than 3, please update config")
)

// marshalBufPool is a sync.Pool of *bytes.Buffer used by collection and vector
// marshal functions to avoid allocating a new Buffer on every call.
//
// Lifecycle: marshalList/marshalMap/marshalVector call getMarshalBuf to get a
// pooled buffer, write the serialized data into it, then call finishMarshalBuf
// which copies the data into a new []byte and returns the buffer to the pool.
// This ensures the returned slice never aliases pooled storage.
var marshalBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// marshalBufMaxCap is the maximum capacity of a buffer that will be returned
// to the pool. Buffers larger than this are left for GC to avoid holding
// excessive memory in the pool from occasional large payloads.
const marshalBufMaxCap = 64 * 1024 // 64 KiB

// getMarshalBuf returns a *bytes.Buffer from the pool, reset and optionally
// pre-grown to the given size hint. If sizeHint <= 0, no pre-grow is done.
func getMarshalBuf(sizeHint int) *bytes.Buffer {
	buf := marshalBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if sizeHint > 0 {
		buf.Grow(sizeHint)
	}
	return buf
}

// putMarshalBuf returns a *bytes.Buffer to the pool. Buffers whose capacity
// exceeds marshalBufMaxCap are discarded to prevent the pool from holding
// oversized allocations.
func putMarshalBuf(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > marshalBufMaxCap {
		return // let GC collect oversized buffers
	}
	marshalBufPool.Put(buf)
}

// finishMarshalBuf copies the contents of buf into a new []byte and returns
// the buffer to the pool. This ensures the returned slice does not alias the
// pooled buffer's internal storage.
func finishMarshalBuf(buf *bytes.Buffer) []byte {
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	putMarshalBuf(buf)
	return result
}

// marshalOutputPool pools []byte slices returned by fast-path marshal functions
// (vectors and lists/sets). These slices are the final marshal output that gets
// copied into the framer buffer by writeBytes. After the framer copies them,
// the connection layer returns them to this pool via putMarshalOutput.
var marshalOutputPool sync.Pool

// getMarshalOutput returns a []byte of exactly the requested size, from the
// pool if a suitable buffer is available, or freshly allocated otherwise.
func getMarshalOutput(size int) []byte {
	if bp := marshalOutputPool.Get(); bp != nil {
		buf := bp.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

// putMarshalOutput returns a []byte to the output pool. Nil slices are ignored.
// Buffers larger than marshalBufMaxCap are discarded to avoid holding excessive
// memory.
func putMarshalOutput(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) > marshalBufMaxCap {
		return
	}
	marshalOutputPool.Put(buf) //nolint:staticcheck // SA6002: []byte is a value type; boxing cost is acceptable for pool reuse
}

// pooledMarshalType returns true if the given TypeInfo uses a marshal fast path
// that allocates from marshalOutputPool. This is used by the connection layer to
// determine which queryValues.value slices can be returned to the pool after
// the framer copies them.
func pooledMarshalType(info TypeInfo) bool {
	switch ti := info.(type) {
	case VectorType:
		switch ti.SubType.Type() {
		case TypeFloat, TypeDouble, TypeInt, TypeBigInt, TypeTimestamp, TypeCounter, TypeUUID, TypeTimeUUID:
			return true
		}
	case CollectionType:
		if ti.typ == TypeList || ti.typ == TypeSet {
			switch ti.Elem.Type() {
			case TypeFloat, TypeDouble, TypeInt, TypeBigInt, TypeTimestamp, TypeCounter:
				return true
			}
		}
	}
	return false
}

// pooledMarshalValue is like pooledMarshalType but also checks value's
// concrete type, since a poolable CQL type can still fall through to the
// generic reflect path (non-pool buffer) for a mismatched Go type, e.g. a
// TypeInt list bound with []int8 instead of []int32/[]int.
// Keep in sync with the type switches in marshalList and marshalVector.
func pooledMarshalValue(typ TypeInfo, value any) bool {
	switch ti := typ.(type) {
	case VectorType:
		switch ti.SubType.Type() {
		case TypeCounter:
			_, ok := value.([]int64)
			return ok
		case TypeFloat:
			_, ok := value.([]float32)
			return ok
		case TypeDouble:
			_, ok := value.([]float64)
			return ok
		case TypeInt:
			if _, ok := value.([]int32); ok {
				return true
			}
			_, ok := value.([]int)
			return ok
		case TypeBigInt, TypeTimestamp:
			_, ok := value.([]int64)
			return ok
		case TypeUUID, TypeTimeUUID:
			_, ok := value.([]UUID)
			return ok
		}
	case CollectionType:
		if ti.typ != TypeList && ti.typ != TypeSet {
			return false
		}
		switch ti.Elem.Type() {
		case TypeFloat:
			_, ok := value.([]float32)
			return ok
		case TypeDouble:
			_, ok := value.([]float64)
			return ok
		case TypeInt:
			if _, ok := value.([]int32); ok {
				return true
			}
			_, ok := value.([]int)
			return ok
		case TypeBigInt, TypeTimestamp, TypeCounter:
			_, ok := value.([]int64)
			return ok
		}
	}
	return false
}

// Marshaler is an interface for custom unmarshaler.
// Each value of the 'CQL binary protocol' consist of <value_len> and <value_data>.
// <value_len> can be 'unset'(-2), 'nil'(-1), 'zero'(0) or any value up to 2147483647.
// When <value_len> is 'unset', 'nil' or 'zero', <value_data> is not present.
// 'unset' is applicable only to columns, with some exceptions.
// As you can see from API MarshalCQL only returns <value_data>, but there is a way for it to control <value_len>:
//  1. If MarshalCQL returns (gocql.UnsetValue, nil), gocql writes 'unset' to <value_len>
//  2. If MarshalCQL returns ([]byte(nil), nil), gocql writes 'nil' to <value_len>
//  3. If MarshalCQL returns ([]byte{}, nil), gocql writes 'zero' to <value_len>
//
// Some CQL databases have proprietary value coding features, which you may want to consider.
// CQL binary protocol info:https://github.com/apache/cassandra/tree/trunk/doc
type Marshaler interface {
	MarshalCQL(info TypeInfo) ([]byte, error)
}

type DirectMarshal []byte

func (m DirectMarshal) MarshalCQL(_ TypeInfo) ([]byte, error) {
	return m, nil
}

// Unmarshaler is an interface for custom unmarshaler.
// Each value of the 'CQL binary protocol' consist of <value_len> and <value_data>.
// <value_len> can be 'unset'(-2), 'nil'(-1), 'zero'(0) or any value up to 2147483647.
// When <value_len> is 'unset', 'nil' or 'zero', <value_data> is not present.
// As you can see from an API UnmarshalCQL receives only 'info TypeInfo' and
// 'data []byte', but gocql has the following way to signal about <value_len>:
//  1. When <value_len> is 'nil' gocql feeds nil to 'data []byte'
//  2. When <value_len> is 'zero' gocql feeds []byte{} to 'data []byte'
//
// The data []byte slice passed to UnmarshalCQL is only valid for the duration
// of the call. The backing memory may be reused after the call returns.
// Implementations that need to retain data must copy it (e.g. using
// bytes.Clone or append([]byte(nil), data...)).
//
// Some CQL databases have proprietary value coding features, which you may want to consider.
// CQL binary protocol info:https://github.com/apache/cassandra/tree/trunk/doc
type Unmarshaler interface {
	UnmarshalCQL(info TypeInfo, data []byte) error
}

type DirectUnmarshal []byte

func (d *DirectUnmarshal) UnmarshalCQL(_ TypeInfo, data []byte) error {
	*d = bytes.Clone(data)
	return nil
}

// Marshal returns the CQL encoding of the value for the Cassandra
// internal type described by the info parameter.
//
// nil is serialized as CQL null.
// If value implements Marshaler, its MarshalCQL method is called to marshal the data.
// If value is a pointer, the pointed-to value is marshaled.
//
// Supported conversions are as follows, other type combinations may be added in the future:
//
//	CQL type                    | Go type (value)    | Note
//	varchar, ascii, blob, text  | string, []byte     |
//	boolean                     | bool               |
//	tinyint, smallint, int      | integer types      |
//	tinyint, smallint, int      | string             | formatted as base 10 number
//	bigint, counter             | integer types      |
//	bigint, counter             | big.Int            | value limited as int64
//	bigint, counter             | string             | formatted as base 10 number
//	float                       | float32            |
//	double                      | float64            |
//	decimal                     | inf.Dec            |
//	time                        | int64              | nanoseconds since start of day
//	time                        | time.Duration      | duration since start of day
//	timestamp                   | int64              | milliseconds since Unix epoch
//	timestamp                   | time.Time          |
//	list, set                   | slice, array       |
//	list, set                   | map[X]struct{}     |
//	map                         | map[X]Y            |
//	uuid, timeuuid              | gocql.UUID         |
//	uuid, timeuuid              | [16]byte           | raw UUID bytes
//	uuid, timeuuid              | []byte             | raw UUID bytes, length must be 16 bytes
//	uuid, timeuuid              | string             | hex representation, see ParseUUID
//	varint                      | integer types      |
//	varint                      | big.Int            |
//	varint                      | string             | value of number in decimal notation
//	inet                        | net.IP             |
//	inet                        | string             | IPv4 or IPv6 address string
//	tuple                       | slice, array       |
//	tuple                       | struct             | fields are marshaled in order of declaration
//	user-defined type           | gocql.UDTMarshaler | MarshalUDT is called
//	user-defined type           | map[string]any         |
//	user-defined type           | struct             | struct fields' cql tags are used for column names
//	date                        | int64              | milliseconds since Unix epoch to start of day (in UTC)
//	date                        | time.Time          | start of day (in UTC)
//	date                        | string             | parsed using "2006-01-02" format
//	duration                    | int64              | duration in nanoseconds
//	duration                    | time.Duration      |
//	duration                    | gocql.Duration     |
//	duration                    | string             | parsed with time.ParseDuration
//
// The marshal/unmarshal error provides a list of supported types when an unsupported type is attempted.

func Marshal(info TypeInfo, value any) ([]byte, error) {
	if info.Version() < protoVersion1 {
		panic("protocol version not set")
	}

	if valueRef := reflect.ValueOf(value); valueRef.Kind() == reflect.Ptr {
		if valueRef.IsNil() {
			return nil, nil
		} else if v, ok := value.(Marshaler); ok {
			return v.MarshalCQL(info)
		} else {
			return Marshal(info, valueRef.Elem().Interface())
		}
	}

	if v, ok := value.(Marshaler); ok {
		return v.MarshalCQL(info)
	}

	switch info.Type() {
	case TypeVarchar:
		return marshalVarchar(value)
	case TypeText:
		return marshalText(value)
	case TypeBlob:
		return marshalBlob(value)
	case TypeAscii:
		return marshalAscii(value)
	case TypeBoolean:
		return marshalBool(value)
	case TypeTinyInt:
		return marshalTinyInt(value)
	case TypeSmallInt:
		return marshalSmallInt(value)
	case TypeInt:
		return marshalInt(value)
	case TypeBigInt:
		return marshalBigInt(value)
	case TypeCounter:
		return marshalCounter(value)
	case TypeFloat:
		return marshalFloat(value)
	case TypeDouble:
		return marshalDouble(value)
	case TypeDecimal:
		return marshalDecimal(value)
	case TypeTime:
		return marshalTime(value)
	case TypeTimestamp:
		return marshalTimestamp(value)
	case TypeList, TypeSet:
		ct, ok := info.(CollectionType)
		if !ok {
			return nil, marshalErrorf("can not marshal %T into %s", value, info)
		}
		return marshalList(ct, value)
	case TypeMap:
		ct, ok := info.(CollectionType)
		if !ok {
			return nil, marshalErrorf("can not marshal %T into %s", value, info)
		}
		return marshalMap(ct, value)
	case TypeUUID:
		return marshalUUID(value)
	case TypeTimeUUID:
		return marshalTimeUUID(value)
	case TypeVarint:
		return marshalVarint(value)
	case TypeInet:
		return marshalInet(value)
	case TypeTuple:
		return marshalTuple(info, value)
	case TypeUDT:
		return marshalUDT(info, value)
	case TypeDate:
		return marshalDate(value)
	case TypeDuration:
		return marshalDuration(value)
	case TypeCustom:
		if vector, ok := info.(VectorType); ok {
			return marshalVector(vector, value)
		}
	}

	// TODO(tux21b): add the remaining types
	return nil, fmt.Errorf("can not marshal %T into %s", value, info)
}

// Unmarshal parses the CQL encoded data based on the info parameter that
// describes the Cassandra internal data type and stores the result in the
// value pointed by value.
//
// If value implements Unmarshaler, it's UnmarshalCQL method is called to
// unmarshal the data.
// If value is a pointer to pointer, it is set to nil if the CQL value is
// null. Otherwise, nulls are unmarshalled as zero value.
//
// Supported conversions are as follows, other type combinations may be added in the future:
//
//	CQL type                                | Go type (value)         | Note
//	varchar, ascii, blob, text              | *string                 |
//	varchar, ascii, blob, text              | *[]byte                 | non-nil buffer is reused
//	bool                                    | *bool                   |
//	tinyint, smallint, int, bigint, counter | *integer types          |
//	tinyint, smallint, int, bigint, counter | *big.Int                |
//	tinyint, smallint, int, bigint, counter | *string                 | formatted as base 10 number
//	float                                   | *float32                |
//	double                                  | *float64                |
//	decimal                                 | *inf.Dec                |
//	time                                    | *int64                  | nanoseconds since start of day
//	time                                    | *time.Duration          |
//	timestamp                               | *int64                  | milliseconds since Unix epoch
//	timestamp                               | *time.Time              |
//	list, set                               | *slice, *array          |
//	map                                     | *map[X]Y                |
//	uuid, timeuuid                          | *string                 | see UUID.String
//	uuid, timeuuid                          | *[]byte                 | raw UUID bytes
//	uuid, timeuuid                          | *gocql.UUID             |
//	timeuuid                                | *time.Time              | timestamp of the UUID
//	inet                                    | *net.IP                 |
//	inet                                    | *string                 | IPv4 or IPv6 address string
//	tuple                                   | *slice, *array          |
//	tuple                                   | *struct                 | struct fields are set in order of declaration
//	user-defined types                      | gocql.UDTUnmarshaler    | UnmarshalUDT is called
//	user-defined types                      | *map[string]any         |
//	user-defined types                      | *struct                 | cql tag is used to determine field name
//	date                                    | *time.Time              | time of beginning of the day (in UTC)
//	date                                    | *string                 | formatted with 2006-01-02 format
//	duration                                | *gocql.Duration         |
func Unmarshal(info TypeInfo, data []byte, value any) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}

	if isNullableValue(value) {
		return unmarshalNullable(info, data, value)
	}

	switch info.Type() {
	case TypeVarchar:
		return unmarshalVarchar(data, value)
	case TypeText:
		return unmarshalText(data, value)
	case TypeBlob:
		return unmarshalBlob(data, value)
	case TypeAscii:
		return unmarshalAscii(data, value)
	case TypeBoolean:
		return unmarshalBool(data, value)
	case TypeInt:
		return unmarshalInt(data, value)
	case TypeBigInt:
		return unmarshalBigInt(data, value)
	case TypeCounter:
		return unmarshalCounter(data, value)
	case TypeVarint:
		return unmarshalVarint(data, value)
	case TypeSmallInt:
		return unmarshalSmallInt(data, value)
	case TypeTinyInt:
		return unmarshalTinyInt(data, value)
	case TypeFloat:
		return unmarshalFloat(data, value)
	case TypeDouble:
		return unmarshalDouble(data, value)
	case TypeDecimal:
		return unmarshalDecimal(data, value)
	case TypeTime:
		return unmarshalTime(data, value)
	case TypeTimestamp:
		return unmarshalTimestamp(data, value)
	case TypeList, TypeSet:
		ct, ok := info.(CollectionType)
		if !ok {
			return unmarshalErrorf("can not unmarshal %s into %T", info, value)
		}
		return unmarshalList(ct, data, value)
	case TypeMap:
		ct, ok := info.(CollectionType)
		if !ok {
			return unmarshalErrorf("can not unmarshal %s into %T", info, value)
		}
		return unmarshalMap(ct, data, value)
	case TypeTimeUUID:
		return unmarshalTimeUUID(data, value)
	case TypeUUID:
		return unmarshalUUID(data, value)
	case TypeInet:
		return unmarshalInet(data, value)
	case TypeTuple:
		return unmarshalTuple(info, data, value)
	case TypeUDT:
		return unmarshalUDT(info, data, value)
	case TypeDate:
		return unmarshalDate(data, value)
	case TypeDuration:
		return unmarshalDuration(data, value)
	case TypeCustom:
		if vector, ok := info.(VectorType); ok {
			return unmarshalVector(vector, data, value)
		}
	}

	// TODO(tux21b): add the remaining types
	return fmt.Errorf("can not unmarshal %s into %T", info, value)
}

func isNullableValue(value any) bool {
	// Fast path: common single-pointer destination types are not nullable.
	// This avoids reflect.ValueOf + Kind checks on the hot unmarshal path.
	// Note: Unmarshaler is already checked before isNullableValue in Unmarshal(),
	// so we don't need to list it here.
	switch value.(type) {
	case *string, *[]byte, *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64,
		*float32, *float64, *bool,
		*time.Time, *Duration,
		*big.Int, *inf.Dec,
		*UUID, *[]UUID:
		return false
	}
	v := reflect.ValueOf(value)
	return v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Ptr
}

func isNullData(info TypeInfo, data []byte) bool {
	return data == nil
}

func unmarshalNullable(info TypeInfo, data []byte, value any) error {
	valueRef := reflect.ValueOf(value)

	if isNullData(info, data) {
		nilValue := reflect.Zero(valueRef.Type().Elem())
		valueRef.Elem().Set(nilValue)
		return nil
	}

	newValue := reflect.New(valueRef.Type().Elem().Elem())
	valueRef.Elem().Set(newValue)
	return Unmarshal(info, data, newValue.Interface())
}

func marshalVarchar(value any) ([]byte, error) {
	data, err := varchar.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalText(value any) ([]byte, error) {
	data, err := text.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalBlob(value any) ([]byte, error) {
	data, err := blob.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalAscii(value any) ([]byte, error) {
	data, err := ascii.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalVarchar(data []byte, value any) error {
	err := varchar.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalText(data []byte, value any) error {
	err := text.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalBlob(data []byte, value any) error {
	err := blob.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalAscii(data []byte, value any) error {
	err := ascii.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalSmallInt(value any) ([]byte, error) {
	data, err := smallint.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalTinyInt(value any) ([]byte, error) {
	data, err := tinyint.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalInt(value any) ([]byte, error) {
	data, err := cqlint.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalBigInt(value any) ([]byte, error) {
	data, err := bigint.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalCounter(value any) ([]byte, error) {
	data, err := counter.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalCounter(data []byte, value any) error {
	err := counter.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalInt(data []byte, value any) error {
	err := cqlint.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalBigInt(data []byte, value any) error {
	err := bigint.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalSmallInt(data []byte, value any) error {
	err := smallint.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalTinyInt(data []byte, value any) error {
	if err := tinyint.Unmarshal(data, value); err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func unmarshalVarint(data []byte, value any) error {
	if err := varint.Unmarshal(data, value); err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalVarint(value any) ([]byte, error) {
	data, err := varint.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func marshalBool(value any) ([]byte, error) {
	data, err := boolean.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalBool(data []byte, value any) error {
	if err := boolean.Unmarshal(data, value); err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalFloat(value any) ([]byte, error) {
	data, err := float.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalFloat(data []byte, value any) error {
	if err := float.Unmarshal(data, value); err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalDouble(value any) ([]byte, error) {
	data, err := double.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalDouble(data []byte, value any) error {
	err := double.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalDecimal(value any) ([]byte, error) {
	data, err := decimal.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalDecimal(data []byte, value any) error {
	if err := decimal.Unmarshal(data, value); err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalTime(value any) ([]byte, error) {
	data, err := cqltime.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalTime(data []byte, value any) error {
	err := cqltime.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalTimestamp(value any) ([]byte, error) {
	data, err := timestamp.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalTimestamp(data []byte, value any) error {
	err := timestamp.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalDate(value any) ([]byte, error) {
	data, err := date.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalDate(data []byte, value any) error {
	err := date.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalDuration(value any) ([]byte, error) {
	switch uv := value.(type) {
	case Duration:
		value = duration.Duration(uv)
	case *Duration:
		value = (*duration.Duration)(uv)
	}
	data, err := duration.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalDuration(data []byte, value any) error {
	switch uv := value.(type) {
	case *Duration:
		value = (*duration.Duration)(uv)
	case **Duration:
		if uv == nil {
			value = (**duration.Duration)(nil)
		} else {
			value = (**duration.Duration)(unsafe.Pointer(uv))
		}
	}
	err := duration.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

// allocCollBuf allocates a collection buffer, writes the element count header,
// and returns the buffer along with the start offset for element data.
func allocCollBuf(total, n int) (buf []byte, off int) {
	buf = make([]byte, total)
	binary.BigEndian.PutUint32(buf, uint32(n))
	return buf, 4
}

// putLen writes a 4-byte length prefix and returns the new offset.
func putLen(buf []byte, off int, length int) int {
	binary.BigEndian.PutUint32(buf[off:], uint32(length))
	return off + 4
}

// writeStr writes a length-prefixed string and returns the new offset.
func writeStr(buf []byte, off int, s string) int {
	off = putLen(buf, off, len(s))
	copy(buf[off:], s)
	return off + len(s)
}

// writeBytes writes a length-prefixed []byte (or CQL null if nil) and returns the new offset.
func writeBytes(buf []byte, off int, b []byte) int {
	if b == nil {
		binary.BigEndian.PutUint32(buf[off:], math.MaxUint32)
		return off + 4
	}
	off = putLen(buf, off, len(b))
	copy(buf[off:], b)
	return off + len(b)
}

// extractMapStrKey extracts string-key map entries into parallel slices and
// computes total wire size. valFixed is the fixed data size of the value (e.g.
// 8 for int64/float64, 4 for int/float32, 2 for int16, 1 for bool).
func extractMapStrKey[T any](m map[string]T, valFixed int) (keys []string, vals []T, total int) {
	keys = make([]string, 0, len(m))
	vals = make([]T, 0, len(m))
	total = 4 + (8+valFixed)*len(m)
	for k, v := range m {
		keys = append(keys, k)
		vals = append(vals, v)
		total += len(k)
	}
	return
}

func checkMarshalLen(n int) (int, error) {
	if n > math.MaxInt32 {
		return 0, marshalErrorf("marshal: collection too large")
	}
	return n, nil
}

func writeCollectionSize(n int, buf *bytes.Buffer) error {
	if n > math.MaxInt32 {
		return marshalErrorf("marshal: collection too large")
	}

	tmp := [4]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	buf.Write(tmp[:])

	return nil
}

func marshalList(info CollectionType, value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	} else if _, ok := value.(unsetColumn); ok {
		return nil, nil
	}
	if value == nil {
		return nil, nil
	}

	// Fast path: type-switch on concrete slice types for common fixed-size
	// CQL element types. These bypass reflect and per-element Marshal entirely.
	// List wire format: [4-byte count] + N × ([4-byte elem-len] + [elem-bytes])
	switch info.Elem.Type() {
	case TypeFloat:
		if vec, ok := value.([]float32); ok {
			if vec == nil {
				return nil, nil
			}
			return marshalListFloat32(vec)
		}
	case TypeDouble:
		if vec, ok := value.([]float64); ok {
			if vec == nil {
				return nil, nil
			}
			return marshalListFloat64(vec)
		}
	case TypeInt:
		if vec, ok := value.([]int32); ok {
			if vec == nil {
				return nil, nil
			}
			return marshalListInt32(vec)
		}
		if vec, ok := value.([]int); ok {
			if vec == nil {
				return nil, nil
			}
			return marshalListInt(vec)
		}
	case TypeBigInt, TypeTimestamp, TypeCounter:
		if vec, ok := value.([]int64); ok {
			if vec == nil {
				return nil, nil
			}
			return marshalListInt64(vec)
		}
	}

	// Fast path: type-switch on concrete slice types for common variable-length
	// CQL element types. These bypass reflect and per-element Marshal entirely.
	// List wire format: [4-byte count] + N × ([4-byte elem-len] + [elem-bytes])
	switch info.Elem.Type() {
	case TypeVarchar, TypeText, TypeAscii:
		if v, ok := value.([]string); ok {
			if v == nil {
				return nil, nil
			}
			return marshalListString(v)
		}
	case TypeBoolean:
		if v, ok := value.([]bool); ok {
			if v == nil {
				return nil, nil
			}
			return marshalListBool(v)
		}
	case TypeBlob:
		if v, ok := value.([][]byte); ok {
			if v == nil {
				return nil, nil
			}
			return marshalListBytes(v)
		}
	case TypeSmallInt:
		if v, ok := value.([]int16); ok {
			if v == nil {
				return nil, nil
			}
			return marshalListInt16(v)
		}
	case TypeTinyInt:
		if v, ok := value.([]int8); ok {
			if v == nil {
				return nil, nil
			}
			return marshalListInt8(v)
		}
	case TypeUUID, TypeTimeUUID:
		if v, ok := value.([]UUID); ok {
			if v == nil {
				return nil, nil
			}
			return marshalListUUID(v)
		}
	}

	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	if k == reflect.Slice && rv.IsNil() {
		return nil, nil
	}

	switch k {
	case reflect.Slice, reflect.Array:
		n := rv.Len()

		// Variable-length elements get an estimate rather than no hint at
		// all, which otherwise leaves text and blob lists growing by doubling.
		// Hint only when the element size is exactly known; see
		// collectionEntrySize for why estimating is counterproductive.
		sizeHint := 4
		if elemSize := collectionEntrySize(info.Elem); elemSize > 0 {
			sizeHint = collectionSizeHint(n, 4+elemSize)
		}
		buf := getMarshalBuf(sizeHint)

		if err := writeCollectionSize(n, buf); err != nil {
			putMarshalBuf(buf)
			return nil, err
		}

		for i := 0; i < n; i++ {
			item, err := Marshal(info.Elem, rv.Index(i).Interface())
			if err != nil {
				putMarshalBuf(buf)
				return nil, err
			}
			itemLen := len(item)
			// Set the value to null for supported protocols
			if item == nil {
				itemLen = -1
			}
			if err := writeCollectionSize(itemLen, buf); err != nil {
				putMarshalBuf(buf)
				return nil, err
			}
			buf.Write(item)
		}
		return finishMarshalBuf(buf), nil
	case reflect.Map:
		elem := t.Elem()
		if elem.Kind() == reflect.Struct && elem.NumField() == 0 {
			rkeys := rv.MapKeys()
			keys := make([]any, len(rkeys))
			for i := 0; i < len(keys); i++ {
				keys[i] = rkeys[i].Interface()
			}
			return marshalList(info, keys)
		}
	}
	return nil, marshalErrorf("can not marshal %T into %s", value, info)
}

var errFastPathNotApplicable = errors.New("fast path not applicable")

// unmarshalListFast attempts to unmarshal a CQL list/set directly into
// common concrete Go slice types without reflection. Returns
// errFastPathNotApplicable if the type combination is not handled.
//
// Every leaf allocates a fresh backing array rather than reusing the one in
// *dst. Reuse looks free but aliases results the caller kept: the ordinary
// `for iter.Scan(&v) { out = append(out, v) }` loop would leave every element
// of out pointing at the last row. Iter.Scan documents that it copies into
// dest, and there is no RawBytes-style "valid until the next Scan" contract.
func unmarshalListFast(info CollectionType, data []byte, value any) error {
	elemTyp := info.Elem.Type()

	switch v := value.(type) {
	case *[]string:
		// TypeAscii is excluded: the generic path validates ASCII payloads
		// (rejecting bytes > 127) via serialization/ascii, whereas this fast
		// path would decode with string(elem) and silently accept invalid data.
		if elemTyp != TypeVarchar && elemTyp != TypeText {
			return errFastPathNotApplicable
		}
		return unmarshalListString(info, data, v)
	case *[]int64:
		if elemTyp != TypeBigInt && elemTyp != TypeCounter {
			return errFastPathNotApplicable
		}
		return unmarshalListInt64(info, data, v)
	case *[]int32:
		if elemTyp != TypeInt {
			return errFastPathNotApplicable
		}
		return unmarshalListInt32(info, data, v)
	case *[]float64:
		if elemTyp != TypeDouble {
			return errFastPathNotApplicable
		}
		return unmarshalListFloat64(info, data, v)
	case *[]float32:
		if elemTyp != TypeFloat {
			return errFastPathNotApplicable
		}
		return unmarshalListFloat32(info, data, v)
	case *[]bool:
		if elemTyp != TypeBoolean {
			return errFastPathNotApplicable
		}
		return unmarshalListBool(info, data, v)
	case *[][]byte:
		if elemTyp != TypeBlob {
			return errFastPathNotApplicable
		}
		return unmarshalListBlob(info, data, v)
	case *[]int16:
		if elemTyp != TypeSmallInt {
			return errFastPathNotApplicable
		}
		return unmarshalListInt16(info, data, v)
	case *[]int8:
		if elemTyp != TypeTinyInt {
			return errFastPathNotApplicable
		}
		return unmarshalListInt8(info, data, v)
	case *[]time.Time:
		if elemTyp != TypeTimestamp && elemTyp != TypeDate {
			return errFastPathNotApplicable
		}
		return unmarshalListTime(info, data, v)
	case *[]UUID:
		if elemTyp != TypeUUID && elemTyp != TypeTimeUUID {
			return errFastPathNotApplicable
		}
		return unmarshalListUUID(info, data, v)
	default:
		return errFastPathNotApplicable
	}
}

// readListHeader reads the collection count and advances past the header.
// Returns the element count and remaining data, or an error.
func readListHeader(data []byte) (int, []byte, error) {
	n, p, err := readCollectionSize(data)
	if err != nil {
		return 0, nil, err
	}
	if n < 0 {
		return 0, nil, unmarshalErrorf("unmarshal list: negative size %d", n)
	}
	rest := data[p:]
	// Reject counts that cannot possibly fit in the remaining payload before
	// allocating: each element carries at least a 4-byte length prefix, so a
	// valid count is bounded by len(rest)/4. This prevents a malformed frame
	// with a huge positive size from triggering a massive up-front allocation.
	if n > len(rest)/4 {
		return 0, nil, unmarshalErrorf("unmarshal list: invalid size %d", n)
	}
	return n, rest, nil
}

// readListElement reads one element's raw bytes from data.
// Returns the element bytes (nil if null), remaining data, and any error.
func readListElement(data []byte) ([]byte, []byte, error) {
	m, p, err := readCollectionSize(data)
	if err != nil {
		return nil, nil, err
	}
	data = data[p:]
	if m < 0 {
		return nil, data, nil // null element
	}
	if len(data) < m {
		return nil, nil, unmarshalErrorf("unmarshal list: unexpected eof")
	}
	return data[:m], data[m:], nil
}

func unmarshalListString(info CollectionType, data []byte, dst *[]string) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []string
	if n == 0 {
		s = make([]string, 0)
	} else {
		s = make([]string, n)
		// Total element data bytes = remaining frame bytes minus n×4-byte length prefixes.
		// Share one buffer across all strings (avoiding n individual string
		// allocations), but only up to marshalBufMaxCap: sharing an unbounded
		// buffer would let a caller keeping one small string pin all of it.
		total := len(data) - n*4
		if total > 0 && total <= marshalBufMaxCap {
			buf := make([]byte, total)
			offset := 0
			for i := 0; i < n; i++ {
				var elem []byte
				elem, data, err = readListElement(data)
				if err != nil {
					return err
				}
				if len(elem) > 0 {
					copy(buf[offset:], elem)
					s[i] = unsafe.String(&buf[offset], len(elem))
					offset += len(elem)
				} else {
					// Null/empty slot — explicitly clear (don't leave a stale
					// string from a reused backing array).
					s[i] = ""
				}
			}
		} else {
			for i := 0; i < n; i++ {
				var elem []byte
				elem, data, err = readListElement(data)
				if err != nil {
					return err
				}
				s[i] = string(elem) // "" for both nil and empty, matches DecString
			}
		}
	}
	*dst = s
	return nil
}

func unmarshalListInt64(info CollectionType, data []byte, dst *[]int64) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []int64
	if n == 0 {
		s = make([]int64, 0)
	} else {
		s = make([]int64, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = 0
			continue
		}
		if len(elem) != 8 {
			return unmarshalErrorf("unmarshal list: invalid bigint size %d", len(elem))
		}
		s[i] = int64(elem[0])<<56 | int64(elem[1])<<48 | int64(elem[2])<<40 | int64(elem[3])<<32 |
			int64(elem[4])<<24 | int64(elem[5])<<16 | int64(elem[6])<<8 | int64(elem[7])
	}
	*dst = s
	return nil
}

func unmarshalListInt32(info CollectionType, data []byte, dst *[]int32) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []int32
	if n == 0 {
		s = make([]int32, 0)
	} else {
		s = make([]int32, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = 0
			continue
		}
		if len(elem) != 4 {
			return unmarshalErrorf("unmarshal list: invalid int size %d", len(elem))
		}
		s[i] = int32(elem[0])<<24 | int32(elem[1])<<16 | int32(elem[2])<<8 | int32(elem[3])
	}
	*dst = s
	return nil
}

func unmarshalListFloat64(info CollectionType, data []byte, dst *[]float64) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []float64
	if n == 0 {
		s = make([]float64, 0)
	} else {
		s = make([]float64, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = 0
			continue
		}
		if len(elem) != 8 {
			return unmarshalErrorf("unmarshal list: invalid double size %d", len(elem))
		}
		bits := uint64(elem[0])<<56 | uint64(elem[1])<<48 | uint64(elem[2])<<40 | uint64(elem[3])<<32 |
			uint64(elem[4])<<24 | uint64(elem[5])<<16 | uint64(elem[6])<<8 | uint64(elem[7])
		s[i] = math.Float64frombits(bits)
	}
	*dst = s
	return nil
}

func unmarshalListFloat32(info CollectionType, data []byte, dst *[]float32) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []float32
	if n == 0 {
		s = make([]float32, 0)
	} else {
		s = make([]float32, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = 0
			continue
		}
		if len(elem) != 4 {
			return unmarshalErrorf("unmarshal list: invalid float size %d", len(elem))
		}
		bits := uint32(elem[0])<<24 | uint32(elem[1])<<16 | uint32(elem[2])<<8 | uint32(elem[3])
		s[i] = math.Float32frombits(bits)
	}
	*dst = s
	return nil
}

func unmarshalListBool(info CollectionType, data []byte, dst *[]bool) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []bool
	if n == 0 {
		s = make([]bool, 0)
	} else {
		s = make([]bool, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = false
			continue
		}
		if len(elem) != 1 {
			return unmarshalErrorf("unmarshal list: invalid boolean size %d", len(elem))
		}
		s[i] = elem[0] != 0
	}
	*dst = s
	return nil
}

func unmarshalListBlob(info CollectionType, data []byte, dst *[][]byte) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s [][]byte
	if n == 0 {
		s = make([][]byte, 0)
	} else {
		s = make([][]byte, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if elem == nil {
			// Null slot — explicitly clear (don't leave stale backing-array
			// bytes when reusing a caller-provided slice across calls).
			s[i] = nil
			continue
		}
		// Copy to avoid aliasing the read buffer
		cp := make([]byte, len(elem))
		copy(cp, elem)
		s[i] = cp
	}
	*dst = s
	return nil
}

func unmarshalListInt16(info CollectionType, data []byte, dst *[]int16) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []int16
	if n == 0 {
		s = make([]int16, 0)
	} else {
		s = make([]int16, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = 0
			continue
		}
		if len(elem) != 2 {
			return unmarshalErrorf("unmarshal list: invalid smallint size %d", len(elem))
		}
		s[i] = int16(elem[0])<<8 | int16(elem[1])
	}
	*dst = s
	return nil
}

func unmarshalListInt8(info CollectionType, data []byte, dst *[]int8) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []int8
	if n == 0 {
		s = make([]int8, 0)
	} else {
		s = make([]int8, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			s[i] = 0
			continue
		}
		if len(elem) != 1 {
			return unmarshalErrorf("unmarshal list: invalid tinyint size %d", len(elem))
		}
		s[i] = int8(elem[0])
	}
	*dst = s
	return nil
}

func unmarshalListTime(info CollectionType, data []byte, dst *[]time.Time) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []time.Time
	if n == 0 {
		s = make([]time.Time, 0)
	} else {
		s = make([]time.Time, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			// Null/empty slot — explicitly clear (don't leave a stale
			// time.Time from a reused backing array).
			if info.Elem.Type() == TypeDate {
				s[i] = time.Date(-5877641, 06, 23, 0, 0, 0, 0, time.UTC)
			} else {
				s[i] = time.Time{}
			}
			continue
		}
		if info.Elem.Type() == TypeTimestamp {
			if len(elem) != 8 {
				return unmarshalErrorf("unmarshal list: invalid timestamp size %d", len(elem))
			}
			msec := int64(elem[0])<<56 | int64(elem[1])<<48 | int64(elem[2])<<40 | int64(elem[3])<<32 |
				int64(elem[4])<<24 | int64(elem[5])<<16 | int64(elem[6])<<8 | int64(elem[7])
			s[i] = time.UnixMilli(msec).UTC()
		} else {
			if len(elem) != 4 {
				return unmarshalErrorf("unmarshal list: invalid date size %d", len(elem))
			}
			msec := (int64(elem[0])<<24 | int64(elem[1])<<16 | int64(elem[2])<<8 | int64(elem[3]) - (1 << 31)) * 86400000
			s[i] = time.UnixMilli(msec).UTC()
		}
	}
	*dst = s
	return nil
}

func unmarshalListUUID(info CollectionType, data []byte, dst *[]UUID) error {
	if data == nil {
		*dst = nil
		return nil
	}
	n, data, err := readListHeader(data)
	if err != nil {
		return err
	}
	var s []UUID
	if n == 0 {
		s = make([]UUID, 0)
	} else {
		s = make([]UUID, n)
	}
	for i := 0; i < n; i++ {
		var elem []byte
		elem, data, err = readListElement(data)
		if err != nil {
			return err
		}
		if len(elem) == 0 {
			// Null/empty slot — explicitly clear (don't leave stale backing-array
			// bytes when reusing a caller-provided slice across calls).
			s[i] = UUID{}
			continue
		}
		if len(elem) != 16 {
			return unmarshalErrorf("unmarshal list: invalid uuid size %d", len(elem))
		}
		copy(s[i][:], elem)
	}
	*dst = s
	return nil
}

func readCollectionSize(data []byte) (size, read int, err error) {
	if len(data) < 4 {
		return 0, 0, unmarshalErrorf("unmarshal list: unexpected eof")
	}
	size = int(int32(data[0])<<24 | int32(data[1])<<16 | int32(data[2])<<8 | int32(data[3]))
	read = 4
	return
}

func unmarshalList(info CollectionType, data []byte, value any) error {
	// Fast path: type-switch on concrete pointer-to-slice types for common
	// fixed-size CQL element types. Bypasses reflect and per-element Unmarshal.
	// nil data is handled here; leaf functions assume non-nil data.
	if dst, ok := value.(*[]int); ok && info.Elem.Type() == TypeInt {
		if data == nil {
			*dst = nil
			return nil
		}
		return unmarshalListInt(data, dst)
	}

	if err := unmarshalListFast(info, data, value); err != errFastPathNotApplicable {
		return err
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()

	// Handle *any destination
	if k == reflect.Interface {
		if t.NumMethod() != 0 {
			return unmarshalErrorf("can not unmarshal into non-empty interface %T", value)
		}
		// Create a properly typed slice based on the element type
		elemGoType, err := goType(info.Elem)
		if err != nil {
			return unmarshalErrorf("unmarshal list: cannot determine element type: %v", err)
		}
		t = reflect.SliceOf(elemGoType)
		k = reflect.Slice
	}

	switch k {
	case reflect.Slice, reflect.Array:
		if data == nil {
			if k == reflect.Array {
				return unmarshalErrorf("unmarshal list: can not store nil in array value")
			}
			if rv.IsNil() {
				return nil
			}
			rv.Set(reflect.Zero(t))
			return nil
		}
		n, p, err := readCollectionSize(data)
		if err != nil {
			return err
		}
		if n < 0 {
			return unmarshalErrorf("unmarshal list: negative size %d", n)
		}
		data = data[p:]
		if k == reflect.Array {
			if rv.Len() != n {
				return unmarshalErrorf("unmarshal list: array with wrong size")
			}
		} else {
			if n > len(data)/4 {
				return unmarshalErrorf("unmarshal list: invalid size %d", n)
			}
			rv.Set(reflect.MakeSlice(t, n, n))
			// If rv was an interface, get the underlying slice
			if rv.Kind() == reflect.Interface {
				rv = rv.Elem()
			}
		}
		for i := 0; i < n; i++ {
			m, p, err := readCollectionSize(data)
			if err != nil {
				return err
			}
			data = data[p:]
			// In case m < 0, the value is null, and unmarshalData should be nil.
			var unmarshalData []byte
			if m >= 0 {
				if len(data) < m {
					return unmarshalErrorf("unmarshal list: unexpected eof")
				}
				unmarshalData = data[:m]
				data = data[m:]
			}
			if err := Unmarshal(info.Elem, unmarshalData, rv.Index(i).Addr().Interface()); err != nil {
				return err
			}
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: *slice, *array, *any.", info, value)
}

func marshalVector(info VectorType, value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	} else if _, ok := value.(unsetColumn); ok {
		return nil, nil
	}
	if value == nil {
		return nil, nil
	}

	dim := info.Dimensions

	// Fast paths for []float64/[]float32/[]int32/[]int/[]int64/[]UUID/[]int64
	// (counter) — skip reflect/per-element dispatch. dim=0 falls through to
	// the generic path (CQL null semantics): each fast-path function is only
	// reached for dim>0 below, and getMarshalOutput(0) would otherwise hand
	// back a non-nil empty slice instead of CQL null.
	if dim > 0 {
		switch info.SubType.Type() {
		case TypeCounter:
			if vec, ok := value.([]int64); ok {
				if vec == nil {
					return nil, nil
				}
				return marshalVectorCounter(vec, dim)
			}
		case TypeDouble:
			if v, ok := value.([]float64); ok {
				if v == nil {
					return nil, nil
				}
				if len(v) != dim {
					return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(v))
				}
				return marshalVectorFloat64(v, dim)
			}
		case TypeFloat:
			if v, ok := value.([]float32); ok {
				if v == nil {
					return nil, nil
				}
				if len(v) != dim {
					return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(v))
				}
				return marshalVectorFloat32(v, dim)
			}
		case TypeInt:
			if vec, ok := value.([]int32); ok {
				if vec == nil {
					return nil, nil
				}
				return marshalVectorInt32(vec, dim)
			}
			if vec, ok := value.([]int); ok {
				if vec == nil {
					return nil, nil
				}
				return marshalVectorInt(vec, dim)
			}
		case TypeBigInt, TypeTimestamp:
			if vec, ok := value.([]int64); ok {
				if vec == nil {
					return nil, nil
				}
				return marshalVectorInt64(vec, dim)
			}
		case TypeUUID, TypeTimeUUID:
			if vec, ok := value.([]UUID); ok {
				if vec == nil {
					return nil, nil
				}
				return marshalVectorUUID(vec, dim)
			}
		}
	}

	// Slow path: reflection-based marshal for all other types.
	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()
	if k == reflect.Slice && rv.IsNil() {
		return nil, nil
	}

	switch k {
	case reflect.Slice, reflect.Array:
		n := rv.Len()
		if n != dim {
			return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, n)
		}
		// A 0-dimension vector has no wire representation; treat it as
		// CQL null rather than a zero-length, non-nil byte slice.
		if n == 0 {
			return nil, nil
		}

		isLengthType := isVectorVariableLengthType(info.SubType)
		sizeHint := 0
		if !isLengthType {
			if elemSize := fixedElemSize(info.SubType); elemSize > 0 {
				if needed := int64(n) * int64(elemSize); needed > 0 && needed <= math.MaxInt32 {
					sizeHint = int(needed)
				}
			}
		}
		buf := getMarshalBuf(sizeHint)
		for i := 0; i < n; i++ {
			item, err := Marshal(info.SubType, rv.Index(i).Interface())
			if err != nil {
				putMarshalBuf(buf)
				return nil, err
			}
			if isLengthType {
				writeUnsignedVInt(buf, uint64(len(item)))
			}
			buf.Write(item)
		}
		return finishMarshalBuf(buf), nil
	}
	return nil, marshalErrorf("can not marshal %T into %s. Accepted types: slice, array.", value, info)
}

// marshalVectorFloat64 encodes []float64 as contiguous big-endian IEEE 754 doubles.
func marshalVectorFloat64(vec []float64, dim int) ([]byte, error) {
	size, err := vectorByteSize(dim, 8)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	if dim > 0 {
		_ = buf[dim*8-1] // BCE hint
	}
	for i, v := range vec {
		binary.BigEndian.PutUint64(buf[i*8:], math.Float64bits(v))
	}
	return buf, nil
}

// marshalVectorFloat32 encodes []float32 as contiguous big-endian IEEE 754 floats.
func marshalVectorFloat32(vec []float32, dim int) ([]byte, error) {
	size, err := vectorByteSize(dim, 4)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	if dim > 0 {
		_ = buf[dim*4-1] // BCE hint
	}
	for i, v := range vec {
		binary.BigEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf, nil
}

// unmarshalVectorFloat64 decodes contiguous big-endian IEEE 754 doubles into vec.
// The caller is responsible for ensuring data is dim*8 bytes and vec has length dim.
func unmarshalVectorFloat64(data []byte, vec []float64) {
	for i := range vec {
		vec[i] = math.Float64frombits(binary.BigEndian.Uint64(data[i*8:]))
	}
}

// unmarshalVectorFloat32 decodes contiguous big-endian IEEE 754 floats into vec.
// The caller is responsible for ensuring data is dim*4 bytes and vec has length dim.
func unmarshalVectorFloat32(data []byte, vec []float32) {
	for i := range vec {
		vec[i] = math.Float32frombits(binary.BigEndian.Uint32(data[i*4:]))
	}
}

func unmarshalVector(info VectorType, data []byte, value any) error {
	dim := info.Dimensions

	// Fast paths for *[]float64/*[]float32/*[]int32/*[]int/*[]int64/*[]UUID/
	// counter *[]int64 — skip reflect/per-element dispatch. nil/empty and
	// dim=0 fall through to the generic path.
	if dim > 0 && data != nil {
		switch info.SubType.Type() {
		case TypeCounter:
			if dst, ok := value.(*[]int64); ok {
				return unmarshalVectorCounter(data, dim, dst)
			}
		case TypeDouble:
			if dst, ok := value.(*[]float64); ok {
				expected := dim * 8
				if len(data) != expected {
					return unmarshalErrorf("unmarshal vector<double>: expected %d bytes, got %d", expected, len(data))
				}
				// Always allocate fresh: reusing *dst's backing array would
				// alias a result the caller retained from an earlier decode
				// (Iter.Scan gives no "valid until next Scan" guarantee).
				vec := make([]float64, dim)
				unmarshalVectorFloat64(data, vec)
				*dst = vec
				return nil
			}
		case TypeFloat:
			if dst, ok := value.(*[]float32); ok {
				expected := dim * 4
				if len(data) != expected {
					return unmarshalErrorf("unmarshal vector<float>: expected %d bytes, got %d", expected, len(data))
				}
				vec := make([]float32, dim)
				unmarshalVectorFloat32(data, vec)
				*dst = vec
				return nil
			}
		case TypeInt:
			if dst, ok := value.(*[]int32); ok {
				return unmarshalVectorInt32(data, dim, dst)
			}
			if dst, ok := value.(*[]int); ok {
				return unmarshalVectorInt(data, dim, dst)
			}
		case TypeBigInt, TypeTimestamp:
			if dst, ok := value.(*[]int64); ok {
				return unmarshalVectorInt64(data, dim, dst)
			}
		case TypeUUID, TypeTimeUUID:
			if dst, ok := value.(*[]UUID); ok {
				return unmarshalVectorUUID(data, dim, dst)
			}
		}
	}

	// Slow path: reflection-based unmarshal for all other types.
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	if t.Kind() == reflect.Interface {
		if t.NumMethod() != 0 {
			return unmarshalErrorf("can not unmarshal into non-empty interface %T", value)
		}
		t = reflect.TypeOf(info.Zero())
	}

	k := t.Kind()
	switch k {
	case reflect.Slice, reflect.Array:
		if data == nil {
			if k == reflect.Array {
				return unmarshalErrorf("unmarshal vector: can not store nil in array value")
			}
			if rv.IsNil() {
				return nil
			}
			rv.Set(reflect.Zero(t))
			return nil
		}
		if dim == 0 {
			if len(data) > 0 {
				return unmarshalErrorf("unmarshal vector: %d bytes of data for 0-dimension vector", len(data))
			}
			if k == reflect.Array {
				if rv.Len() != 0 {
					return unmarshalErrorf("unmarshal vector: array of size %d cannot store vector of 0 dimensions", rv.Len())
				}
			} else if k == reflect.Slice {
				rv.Set(reflect.MakeSlice(t, 0, 0))
			}
			return nil
		}
		if k == reflect.Array {
			if rv.Len() != dim {
				return unmarshalErrorf("unmarshal vector: array of size %d cannot store vector of %d dimensions", rv.Len(), dim)
			}
		} else {
			rv.Set(reflect.MakeSlice(t, dim, dim))
			if rv.Kind() == reflect.Interface {
				rv = rv.Elem()
			}
		}
		elemSize := len(data) / dim
		isLengthType := isVectorVariableLengthType(info.SubType)
		for i := 0; i < dim; i++ {
			offset := 0
			if isLengthType {
				m, p, err := readUnsignedVInt(data)
				if err != nil {
					return err
				}
				elemSize = int(m)
				offset = p
			}
			if offset > 0 {
				data = data[offset:]
			}
			var unmarshalData []byte
			if elemSize >= 0 {
				if len(data) < elemSize {
					return unmarshalErrorf("unmarshal vector: unexpected eof")
				}
				unmarshalData = data[:elemSize]
				data = data[elemSize:]
			}
			err := Unmarshal(info.SubType, unmarshalData, rv.Index(i).Addr().Interface())
			if err != nil {
				return unmarshalErrorf("failed to unmarshal %s into %T: %s", info.SubType, unmarshalData, err.Error())
			}
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: *slice, *array, *any.", info, value)
}

// fixedElemSize returns the wire-format byte size for fixed-length CQL types.
// Returns 0 for variable-length or unknown types.
// Note: TypeBoolean/TypeTinyInt (1B) and TypeSmallInt (2B) are intentionally
// excluded — Cassandra's vector implementation treats them as variable-length,
// and the pre-sizing benefit for such small types is negligible.
// collectionElemWireSize returns the wire-encoded size of a fixed-length CQL
// element, or 0 for variable-length types. Kept separate from fixedElemSize,
// which the vector path shares: Cassandra's vector encoding treats smallint,
// tinyint, time, counter and date as variable-length (see
// isVectorVariableLengthType), while collections have no such restriction.
// Inet is excluded from both: it encodes to 4 or 16 bytes.
func collectionElemWireSize(elemType TypeInfo) int {
	switch elemType.Type() {
	case TypeTinyInt, TypeBoolean:
		return 1
	case TypeSmallInt:
		return 2
	case TypeInt, TypeFloat, TypeDate:
		return 4
	case TypeBigInt, TypeDouble, TypeTimestamp, TypeCounter, TypeTime:
		return 8
	case TypeUUID, TypeTimeUUID:
		return 16
	}
	return 0
}

// maxCollectionPreallocBytes caps speculative preallocation in marshalList and
// marshalMap. The hint is computed before any element has been marshalled, so
// a bogus element count must not trigger an arbitrarily large allocation — or
// overflow n*perEntry — before the first element's Marshal reports the error.
const maxCollectionPreallocBytes = 1 << 20 // 1 MiB

// collectionSizeHint sizes the marshal buffer for n entries of perEntry wire
// bytes each. Past the cap, buffer doubling takes over as before.
func collectionSizeHint(n, perEntry int) int {
	if n <= 0 || perEntry <= 0 {
		return 4
	}
	if n > (maxCollectionPreallocBytes-4)/perEntry {
		return maxCollectionPreallocBytes
	}
	return 4 + n*perEntry
}

// collectionEntrySize returns the per-entry wire size to preallocate for, or 0
// when the element is variable-length and no honest figure exists.
//
// Deliberately no estimate for variable-length elements. An estimate that
// overshoots is worse than no hint: it inflates the buffer past
// marshalBufMaxCap, at which point putMarshalBuf drops it instead of returning
// it to the pool, so every call allocates afresh. Measured on lists of short
// blobs, a 64-byte-per-element guess cost +27% time and +192% B/op against no
// hint at all. bytes.Buffer doubling is the cheaper default here.
func collectionEntrySize(elemType TypeInfo) int {
	return collectionElemWireSize(elemType)
}

func fixedElemSize(elemType TypeInfo) int {
	switch elemType.Type() {
	case TypeInt, TypeFloat, TypeDate:
		return 4
	case TypeBigInt, TypeDouble, TypeTimestamp, TypeCounter, TypeTime:
		return 8
	case TypeUUID, TypeTimeUUID:
		return 16
	}
	return 0
}

// vectorByteSize returns the total byte size of a vector with the given number
// of dimensions and per-element byte size. It returns an error if the result
// would overflow int.
func vectorByteSize(dim, elemSize int) (int, error) {
	n := int64(dim) * int64(elemSize)
	if n < 0 || n > math.MaxInt32 {
		return 0, marshalErrorf("vector byte size overflow: %d * %d", dim, elemSize)
	}
	return int(n), nil
}

// --- Vector fast-path marshal functions ---
// These use encoding/binary.BigEndian directly, avoiding per-element
// reflection and the generic Marshal() call for common fixed-size types.
// Each function validates dimensions, checks for byte-size overflow,
// and uses a BCE hint to eliminate bounds checks in the hot loop.

func marshalVectorInt32(vec []int32, dim int) ([]byte, error) {
	if len(vec) != dim {
		return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(vec))
	}
	size, err := vectorByteSize(dim, 4)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	if dim > 0 {
		_ = buf[dim*4-1] // BCE hint
	}
	for i, v := range vec {
		binary.BigEndian.PutUint32(buf[i*4:], uint32(v))
	}
	return buf, nil
}

func marshalVectorInt(vec []int, dim int) ([]byte, error) {
	if len(vec) != dim {
		return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(vec))
	}
	size, err := vectorByteSize(dim, 4)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	if dim > 0 {
		_ = buf[dim*4-1] // BCE hint
	}
	for i, v := range vec {
		if v > math.MaxInt32 || v < math.MinInt32 {
			putMarshalOutput(buf)
			return nil, marshalErrorf("marshal vector: int element value %d out of int32 range", v)
		}
		binary.BigEndian.PutUint32(buf[i*4:], uint32(int32(v)))
	}
	return buf, nil
}

func marshalVectorInt64(vec []int64, dim int) ([]byte, error) {
	if len(vec) != dim {
		return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(vec))
	}
	size, err := vectorByteSize(dim, 8)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	if dim > 0 {
		_ = buf[dim*8-1] // BCE hint
	}
	for i, v := range vec {
		binary.BigEndian.PutUint64(buf[i*8:], uint64(v))
	}
	return buf, nil
}

func marshalVectorCounter(vec []int64, dim int) ([]byte, error) {
	if len(vec) != dim {
		return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(vec))
	}
	// Counter vectors are length-prefixed in the current wire format.
	// Each element is encoded as: uVInt(8) + 8-byte big-endian payload.
	size, err := vectorByteSize(dim, 9)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	off := 0
	for _, v := range vec {
		buf[off] = 8
		off++
		binary.BigEndian.PutUint64(buf[off:], uint64(v))
		off += 8
	}
	return buf, nil
}

func marshalVectorUUID(vec []UUID, dim int) ([]byte, error) {
	if len(vec) != dim {
		return nil, marshalErrorf("expected vector with %d dimensions, received %d", dim, len(vec))
	}
	size, err := vectorByteSize(dim, 16)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	if dim > 0 {
		_ = buf[dim*16-1] // BCE hint
	}
	for i := range vec {
		copy(buf[i*16:], vec[i][:])
	}
	return buf, nil
}

// --- Vector fast-path unmarshal functions ---
// These read from raw bytes using encoding/binary.BigEndian directly, into a
// fresh backing array per decode (see marshal_alias_test.go).
//
// Deliberately stricter than the generic path on payload length: each decoder
// requires len(data) to be exactly the fixed element width times dim, where
// the reflect path derives the element size as len(data)/dim and silently
// ignores trailing bytes. Trailing bytes mean a corrupt or misframed value,
// and an error beats quietly decoding it.

func unmarshalVectorInt32(data []byte, dim int, dst *[]int32) error {
	expected, err := vectorByteSize(dim, 4)
	if err != nil {
		return err
	}
	if len(data) != expected {
		return unmarshalErrorf("unmarshal vector: expected %d bytes for %d int32 dimensions, got %d", expected, dim, len(data))
	}
	vec := make([]int32, dim)
	if dim > 0 {
		_ = data[dim*4-1] // BCE hint
	}
	for i := 0; i < dim; i++ {
		vec[i] = int32(binary.BigEndian.Uint32(data[i*4:]))
	}
	*dst = vec
	return nil
}

func unmarshalVectorInt(data []byte, dim int, dst *[]int) error {
	expected, err := vectorByteSize(dim, 4)
	if err != nil {
		return err
	}
	if len(data) != expected {
		return unmarshalErrorf("unmarshal vector: expected %d bytes for %d int dimensions, got %d", expected, dim, len(data))
	}
	vec := make([]int, dim)
	if dim > 0 {
		_ = data[dim*4-1] // BCE hint
	}
	for i := 0; i < dim; i++ {
		vec[i] = int(int32(binary.BigEndian.Uint32(data[i*4:])))
	}
	*dst = vec
	return nil
}

func unmarshalVectorInt64(data []byte, dim int, dst *[]int64) error {
	expected, err := vectorByteSize(dim, 8)
	if err != nil {
		return err
	}
	if len(data) != expected {
		return unmarshalErrorf("unmarshal vector: expected %d bytes for %d int64 dimensions, got %d", expected, dim, len(data))
	}
	vec := make([]int64, dim)
	if dim > 0 {
		_ = data[dim*8-1] // BCE hint
	}
	for i := 0; i < dim; i++ {
		vec[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	*dst = vec
	return nil
}

func unmarshalVectorCounter(data []byte, dim int, dst *[]int64) error {
	// Each element is uVInt(8) + 8 payload bytes, so a valid dim is bounded by
	// len(data)/9. Checked before allocating, matching the other vector decoders.
	if _, err := vectorByteSize(dim, 9); err != nil {
		return err
	}
	if dim > len(data)/9 {
		return unmarshalErrorf("unmarshal vector: %d counter dimensions exceed %d bytes of data", dim, len(data))
	}
	vec := make([]int64, dim)

	off := 0
	for i := 0; i < dim; i++ {
		if off >= len(data) {
			return unmarshalErrorf("unmarshal vector: unexpected eof")
		}
		m, p, err := readUnsignedVInt(data[off:])
		if err != nil {
			return err
		}
		off += p
		if m != 8 {
			return unmarshalErrorf("unmarshal vector: expected 8-byte counter element, got %d bytes at index %d", m, i)
		}
		if off+8 > len(data) {
			return unmarshalErrorf("unmarshal vector: unexpected eof")
		}
		vec[i] = int64(binary.BigEndian.Uint64(data[off:]))
		off += 8
	}
	if off != len(data) {
		return unmarshalErrorf("unmarshal vector: expected %d counter elements, got %d trailing bytes", dim, len(data)-off)
	}
	*dst = vec
	return nil
}

func unmarshalVectorUUID(data []byte, dim int, dst *[]UUID) error {
	expected, err := vectorByteSize(dim, 16)
	if err != nil {
		return err
	}
	if len(data) != expected {
		return unmarshalErrorf("unmarshal vector: expected %d bytes for %d UUID dimensions, got %d", expected, dim, len(data))
	}
	vec := make([]UUID, dim)
	if dim > 0 {
		_ = data[dim*16-1] // BCE hint
	}
	for i := 0; i < dim; i++ {
		copy(vec[i][:], data[i*16:])
	}
	*dst = vec
	return nil
}

// --- List/Set fast-path marshal functions ---
// These use encoding/binary.BigEndian directly, avoiding per-element
// reflection and the generic Marshal() call for common fixed-size types.
// List wire format: [4-byte count] + N × ([4-byte elem-length] + [elem-bytes])

// listByteSize returns the total byte size for a list of n fixed-size elements,
// checking for integer overflow. Each element has a 4-byte length prefix.
func listByteSize(n, elemSize int) (int, error) {
	// Total = 4 (count header) + n * (4 + elemSize)
	perElem := 4 + elemSize
	if n > 0 && perElem > (math.MaxInt-4)/n {
		return 0, marshalErrorf("marshal list: byte size overflow for %d elements of %d bytes", n, elemSize)
	}
	return 4 + n*perElem, nil
}

func marshalListFloat32(list []float32) ([]byte, error) {
	n := len(list)
	if n > math.MaxInt32 {
		return nil, marshalErrorf("marshal list: collection too large")
	}
	size, err := listByteSize(n, 4)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	binary.BigEndian.PutUint32(buf, uint32(n))
	off := 4
	for _, v := range list {
		binary.BigEndian.PutUint32(buf[off:], 4) // elem length
		binary.BigEndian.PutUint32(buf[off+4:], math.Float32bits(v))
		off += 8
	}
	return buf, nil
}

func marshalListFloat64(list []float64) ([]byte, error) {
	n := len(list)
	if n > math.MaxInt32 {
		return nil, marshalErrorf("marshal list: collection too large")
	}
	size, err := listByteSize(n, 8)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	binary.BigEndian.PutUint32(buf, uint32(n))
	off := 4
	for _, v := range list {
		binary.BigEndian.PutUint32(buf[off:], 8) // elem length
		binary.BigEndian.PutUint64(buf[off+4:], math.Float64bits(v))
		off += 12
	}
	return buf, nil
}

func marshalListInt32(list []int32) ([]byte, error) {
	n := len(list)
	if n > math.MaxInt32 {
		return nil, marshalErrorf("marshal list: collection too large")
	}
	size, err := listByteSize(n, 4)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	binary.BigEndian.PutUint32(buf, uint32(n))
	off := 4
	for _, v := range list {
		binary.BigEndian.PutUint32(buf[off:], 4) // elem length
		binary.BigEndian.PutUint32(buf[off+4:], uint32(v))
		off += 8
	}
	return buf, nil
}

func marshalListInt(list []int) ([]byte, error) {
	n := len(list)
	if n > math.MaxInt32 {
		return nil, marshalErrorf("marshal list: collection too large")
	}
	size, err := listByteSize(n, 4)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	binary.BigEndian.PutUint32(buf, uint32(n))
	off := 4
	for _, v := range list {
		if v > math.MaxInt32 || v < math.MinInt32 {
			putMarshalOutput(buf)
			return nil, marshalErrorf("marshal list: int element value %d out of int32 range", v)
		}
		binary.BigEndian.PutUint32(buf[off:], 4) // elem length
		binary.BigEndian.PutUint32(buf[off+4:], uint32(int32(v)))
		off += 8
	}
	return buf, nil
}

func marshalListInt64(list []int64) ([]byte, error) {
	n := len(list)
	if n > math.MaxInt32 {
		return nil, marshalErrorf("marshal list: collection too large")
	}
	size, err := listByteSize(n, 8)
	if err != nil {
		return nil, err
	}
	buf := getMarshalOutput(size)
	binary.BigEndian.PutUint32(buf, uint32(n))
	off := 4
	for _, v := range list {
		binary.BigEndian.PutUint32(buf[off:], 8) // elem length
		binary.BigEndian.PutUint64(buf[off+4:], uint64(v))
		off += 12
	}
	return buf, nil
}

func unmarshalListInt(data []byte, dst *[]int) error {
	if len(data) < 4 {
		return unmarshalErrorf("unmarshal list: unexpected eof reading count")
	}
	n := int(int32(binary.BigEndian.Uint32(data[:4])))
	if n < 0 {
		return unmarshalErrorf("unmarshal list: negative count %d", n)
	}
	// Each element needs ≥4 bytes (length prefix); reject implausible counts early.
	if n > (len(data)-4)/4 {
		return unmarshalErrorf("unmarshal list: count %d exceeds available data", n)
	}
	// make is non-nil even at n==0, which TestMarshalSetListV3/zero_elems and
	// the other leaf decoders expect.
	vec := make([]int, n)
	off := 4
	for i := 0; i < n; i++ {
		if off+4 > len(data) {
			return unmarshalErrorf("unmarshal list: unexpected eof reading element length at index %d", i)
		}
		elemLen := int(int32(binary.BigEndian.Uint32(data[off:])))
		off += 4
		if elemLen <= 0 {
			// Null (<0) and legacy empty (0) elements both decode to zero,
			// matching the sibling leaves and serialization/cqlint.
			vec[i] = 0
			continue
		}
		if elemLen != 4 {
			return unmarshalErrorf("unmarshal list: expected 4-byte int element, got %d bytes at index %d", elemLen, i)
		}
		if off+4 > len(data) {
			return unmarshalErrorf("unmarshal list: unexpected eof reading element data at index %d", i)
		}
		vec[i] = int(int32(binary.BigEndian.Uint32(data[off:])))
		off += 4
	}
	*dst = vec
	return nil
}

// isVectorVariableLengthType determines if a type requires explicit length serialization within a vector.
// Variable-length types need their length encoded (as a vint prefix) before the actual data.
// Fixed-length types don't require this prefix.
//
// This classification must match Cassandra's VectorType behavior. Cassandra's VectorType constructor
// selects FixedLengthSerializer vs VariableLengthSerializer based on elementType.isValueLengthFixed(),
// which checks whether the type overrides valueLengthIfFixed() to return something other than -1.
//
// Several types that are conceptually fixed-size do NOT override valueLengthIfFixed() in Cassandra
// and are therefore treated as variable-length inside vectors on the wire:
//   - CounterColumnType  (counter)  — no valueLengthIfFixed() override
//   - ShortType          (smallint) — no valueLengthIfFixed() override
//   - ByteType           (tinyint)  — no valueLengthIfFixed() override
//   - TimeType           (time)     — no valueLengthIfFixed() override
//   - SimpleDateType     (date)     — no valueLengthIfFixed() override
//
// gocql must match this to produce wire-compatible encoding, even though these types always
// serialize to a known number of bytes.
//
// Reference: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/VectorType.java
func isVectorVariableLengthType(elemType TypeInfo) bool {
	switch elemType.Type() {
	case TypeVarchar, TypeAscii, TypeBlob, TypeText,
		TypeCounter,
		TypeDuration, TypeDate, TypeTime,
		TypeDecimal, TypeSmallInt, TypeTinyInt, TypeVarint,
		TypeInet,
		TypeList, TypeSet, TypeMap, TypeUDT, TypeTuple:
		return true
	case TypeCustom:
		if vecType, ok := elemType.(VectorType); ok {
			return isVectorVariableLengthType(vecType.SubType)
		}
		return true
	}
	return false
}

func writeUnsignedVInt(buf *bytes.Buffer, v uint64) {
	numBytes := computeUnsignedVIntSize(v)
	if numBytes <= 1 {
		buf.WriteByte(byte(v))
		return
	}

	extraBytes := numBytes - 1
	var tmp = make([]byte, numBytes)
	for i := extraBytes; i >= 0; i-- {
		tmp[i] = byte(v)
		v >>= 8
	}
	tmp[0] |= byte(^(0xff >> uint(extraBytes)))
	buf.Write(tmp)
}

func readUnsignedVInt(data []byte) (uint64, int, error) {
	if len(data) <= 0 {
		return 0, 0, errors.New("unexpected eof")
	}
	firstByte := data[0]
	if firstByte&0x80 == 0 {
		return uint64(firstByte), 1, nil
	}
	numBytes := bits.LeadingZeros32(uint32(^firstByte)) - 24
	ret := uint64(firstByte & (0xff >> uint(numBytes)))
	if len(data) < numBytes+1 {
		return 0, 0, fmt.Errorf("data expect to have %d bytes, but it has only %d", numBytes+1, len(data))
	}
	for i := 0; i < numBytes; i++ {
		ret <<= 8
		ret |= uint64(data[i+1] & 0xff)
	}
	return ret, numBytes + 1, nil
}

func computeUnsignedVIntSize(v uint64) int {
	lead0 := bits.LeadingZeros64(v)
	return (639 - lead0*9) >> 6
}

func marshalMap(info CollectionType, value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	} else if _, ok := value.(unsetColumn); ok {
		return nil, nil
	}
	if value == nil {
		return nil, nil
	}

	// Fast path: type-switch on concrete map types for common key/value CQL type
	// combinations. These bypass reflect and per-element Marshal entirely.
	// Map wire format: [4-byte count] + N × ([4-byte key-len] + [key-bytes] + [4-byte val-len] + [val-bytes])
	switch info.Key.Type() {
	case TypeVarchar, TypeText, TypeAscii:
		switch info.Elem.Type() {
		case TypeVarchar, TypeText, TypeAscii:
			if v, ok := value.(map[string]string); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringString(v)
			}
		case TypeBigInt:
			if v, ok := value.(map[string]int64); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringInt64(v)
			}
		case TypeInt:
			if v, ok := value.(map[string]int); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringInt(v)
			}
		case TypeDouble:
			if v, ok := value.(map[string]float64); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringFloat64(v)
			}
		case TypeFloat:
			if v, ok := value.(map[string]float32); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringFloat32(v)
			}
		case TypeBoolean:
			if v, ok := value.(map[string]bool); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringBool(v)
			}
		case TypeBlob:
			if v, ok := value.(map[string][]byte); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringBytes(v)
			}
		case TypeSmallInt:
			if v, ok := value.(map[string]int16); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapStringInt16(v)
			}
		}
	case TypeBigInt:
		switch info.Elem.Type() {
		case TypeVarchar, TypeText, TypeAscii:
			if v, ok := value.(map[int64]string); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapInt64String(v)
			}
		case TypeBigInt:
			if v, ok := value.(map[int64]int64); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapInt64Int64(v)
			}
		case TypeDouble:
			if v, ok := value.(map[int64]float64); ok {
				if v == nil {
					return nil, nil
				}
				return marshalMapInt64Float64(v)
			}
		}
	}

	rv := reflect.ValueOf(value)

	t := rv.Type()
	if t.Kind() != reflect.Map {
		return nil, marshalErrorf("can not marshal %T into %s", value, info)
	}

	if rv.IsNil() {
		return nil, nil
	}

	n := rv.Len()

	sizeHint := 4
	keySize := collectionEntrySize(info.Key)
	valSize := collectionEntrySize(info.Elem)
	if keySize > 0 && valSize > 0 {
		sizeHint = collectionSizeHint(n, 4+keySize+4+valSize)
	}
	buf := getMarshalBuf(sizeHint)

	if err := writeCollectionSize(n, buf); err != nil {
		putMarshalBuf(buf)
		return nil, err
	}

	// MapRange avoids the intermediate []reflect.Value that MapKeys allocates,
	// and the second hash lookup per entry that MapIndex would cost.
	iter := rv.MapRange()
	for iter.Next() {
		key, val := iter.Key(), iter.Value()

		item, err := Marshal(info.Key, key.Interface())
		if err != nil {
			putMarshalBuf(buf)
			return nil, err
		}
		itemLen := len(item)
		// Set the key to null for supported protocols
		if item == nil {
			itemLen = -1
		}
		if err := writeCollectionSize(itemLen, buf); err != nil {
			putMarshalBuf(buf)
			return nil, err
		}
		buf.Write(item)

		item, err = Marshal(info.Elem, val.Interface())
		if err != nil {
			putMarshalBuf(buf)
			return nil, err
		}
		itemLen = len(item)
		// Set the value to null for supported protocols
		if item == nil {
			itemLen = -1
		}
		if err := writeCollectionSize(itemLen, buf); err != nil {
			putMarshalBuf(buf)
			return nil, err
		}
		buf.Write(item)
	}
	return finishMarshalBuf(buf), nil
}

// readMapHeader reads a map's element count, bounded by len(rest)/8 (each
// entry needs at least two 4-byte length prefixes). Prevents make(map, n)
// from a bogus huge n causing a massive allocation.
func readMapHeader(data []byte) (int, []byte, error) {
	n, p, err := readCollectionSize(data)
	if err != nil {
		return 0, nil, err
	}
	if n < 0 {
		return 0, nil, unmarshalErrorf("unmarshal map: negative size %d", n)
	}
	rest := data[p:]
	if n > len(rest)/8 {
		return 0, nil, unmarshalErrorf("unmarshal map: invalid size %d", n)
	}
	return n, rest, nil
}

// readMapEntryData reads a single collection entry (key or value) from data.
// Returns the entry bytes (nil if the entry is null, i.e. size < 0),
// the number of bytes consumed from data, and any error.
func readMapEntryData(data []byte) (entryData []byte, consumed int, err error) {
	if len(data) < 4 {
		return nil, 0, unmarshalErrorf("unmarshal map: unexpected eof")
	}
	m := int(int32(data[0])<<24 | int32(data[1])<<16 | int32(data[2])<<8 | int32(data[3]))
	if m < 0 {
		return nil, 4, nil
	}
	// Compare against len(data)-4 instead of 4+m: on 32-bit targets a large
	// positive m could overflow 4+m, wrap negative, bypass this guard, and then
	// panic on the data[4:4+m] slice. len(data) >= 4 and m >= 0 here, so
	// len(data)-4 is non-negative and the comparison is safe.
	if m > len(data)-4 {
		return nil, 0, unmarshalErrorf("unmarshal map: unexpected eof")
	}
	return data[4 : 4+m], 4 + m, nil
}

// isStringKeyType returns true if the CQL type encodes as raw bytes that can be
// interpreted as a Go string without further validation (text, varchar).
//
// TypeAscii is deliberately excluded: the generic path validates ASCII payloads
// via serialization/ascii.DecString (rejecting bytes > 127), so routing ascii
// through the raw-string fast path would silently accept invalid data.
func isStringKeyType(t Type) bool {
	return t == TypeVarchar || t == TypeText
}

// unmarshalMapFast attempts to unmarshal a map using type-switch fast paths
// for common concrete map types, avoiding all reflection. Returns (true, err)
// if the fast path handled the value, or (false, nil) to fall through to the
// generic reflect-based path.
func unmarshalMapFast(info CollectionType, data []byte, value any) (bool, error) {
	if data == nil {
		return false, nil
	}

	keyType := info.Key.Type()
	elemType := info.Elem.Type()

	// We only fast-path string-keyed maps and int64-keyed maps, which cover
	// the vast majority of real-world CQL map usage. TypeAscii is excluded so
	// the generic path can validate ASCII payloads (bytes > 127 are rejected).
	switch keyType {
	case TypeVarchar, TypeText:
		switch v := value.(type) {
		case *map[string]string:
			if !isStringKeyType(elemType) {
				return false, nil
			}
			return true, unmarshalMapStringString(data, v)
		case *map[string][]byte:
			if elemType != TypeBlob {
				return false, nil
			}
			return true, unmarshalMapStringBytes(data, v)
		case *map[string]int64:
			if elemType != TypeBigInt && elemType != TypeCounter {
				return false, nil
			}
			return true, unmarshalMapStringInt64(data, v)
		case *map[string]int32:
			if elemType != TypeInt {
				return false, nil
			}
			return true, unmarshalMapStringInt32(data, v)
		case *map[string]float64:
			if elemType != TypeDouble {
				return false, nil
			}
			return true, unmarshalMapStringFloat64(data, v)
		case *map[string]bool:
			if elemType != TypeBoolean {
				return false, nil
			}
			return true, unmarshalMapStringBool(data, v)
		}
	case TypeBigInt, TypeCounter:
		switch v := value.(type) {
		case *map[int64]string:
			if !isStringKeyType(elemType) {
				return false, nil
			}
			return true, unmarshalMapInt64String(data, v)
		case *map[int64]int64:
			if elemType != TypeBigInt && elemType != TypeCounter {
				return false, nil
			}
			return true, unmarshalMapInt64Int64(data, v)
		}
	}
	return false, nil
}

func unmarshalMapStringString(data []byte, dest *map[string]string) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		m[string(keyData)] = string(valData)
	}
	*dest = m
	return nil
}

func unmarshalMapStringBytes(data []byte, dest *map[string][]byte) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		// Copy the value bytes since the underlying buffer may be reused.
		var valCopy []byte
		if valData != nil {
			valCopy = make([]byte, len(valData))
			copy(valCopy, valData)
		}
		m[string(keyData)] = valCopy
	}
	*dest = m
	return nil
}

func unmarshalMapStringInt64(data []byte, dest *map[string]int64) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[string]int64, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var v int64
		if err := bigint.DecInt64(valData, &v); err != nil {
			return unmarshalErrorf("unmarshal map value: %v", err)
		}
		m[string(keyData)] = v
	}
	*dest = m
	return nil
}

func unmarshalMapStringInt32(data []byte, dest *map[string]int32) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[string]int32, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var v int32
		if err := cqlint.DecInt32(valData, &v); err != nil {
			return unmarshalErrorf("unmarshal map value: %v", err)
		}
		m[string(keyData)] = v
	}
	*dest = m
	return nil
}

func unmarshalMapStringFloat64(data []byte, dest *map[string]float64) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[string]float64, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var v float64
		if err := double.DecFloat64(valData, &v); err != nil {
			return unmarshalErrorf("unmarshal map value: %v", err)
		}
		m[string(keyData)] = v
	}
	*dest = m
	return nil
}

func unmarshalMapStringBool(data []byte, dest *map[string]bool) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var v bool
		if err := boolean.DecBool(valData, &v); err != nil {
			return unmarshalErrorf("unmarshal map value: %v", err)
		}
		m[string(keyData)] = v
	}
	*dest = m
	return nil
}

func unmarshalMapInt64String(data []byte, dest *map[int64]string) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[int64]string, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var k int64
		if err := bigint.DecInt64(keyData, &k); err != nil {
			return unmarshalErrorf("unmarshal map key: %v", err)
		}
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		m[k] = string(valData)
	}
	*dest = m
	return nil
}

func unmarshalMapInt64Int64(data []byte, dest *map[int64]int64) error {
	n, data, err := readMapHeader(data)
	if err != nil {
		return err
	}
	m := make(map[int64]int64, n)
	for i := 0; i < n; i++ {
		keyData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var k int64
		if err := bigint.DecInt64(keyData, &k); err != nil {
			return unmarshalErrorf("unmarshal map key: %v", err)
		}
		valData, c, err := readMapEntryData(data)
		if err != nil {
			return err
		}
		data = data[c:]
		var v int64
		if err := bigint.DecInt64(valData, &v); err != nil {
			return unmarshalErrorf("unmarshal map value: %v", err)
		}
		m[k] = v
	}
	*dest = m
	return nil
}

func unmarshalMap(info CollectionType, data []byte, value any) error {
	// Try fast path for common concrete map types (no reflection).
	if handled, err := unmarshalMapFast(info, data, value); handled {
		return err
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()

	// Handle *any destination
	if t.Kind() == reflect.Interface {
		if t.NumMethod() != 0 {
			return unmarshalErrorf("can not unmarshal into non-empty interface %T", value)
		}
		// Create a properly typed map based on the key and element types
		keyGoType, err := goType(info.Key)
		if err != nil {
			return unmarshalErrorf("unmarshal map: cannot determine key type: %v", err)
		}
		elemGoType, err := goType(info.Elem)
		if err != nil {
			return unmarshalErrorf("unmarshal map: cannot determine element type: %v", err)
		}
		t = reflect.MapOf(keyGoType, elemGoType)
	}

	if t.Kind() != reflect.Map {
		return unmarshalErrorf("can not unmarshal %s into %T. Accepted types: *map, *any.", info, value)
	}
	if data == nil {
		rv.Set(reflect.Zero(t))
		return nil
	}
	n, p, err := readCollectionSize(data)
	if err != nil {
		return err
	}
	if n < 0 {
		return unmarshalErrorf("negative map size %d", n)
	}
	if n > (len(data)-p)/8 {
		return unmarshalErrorf("unmarshal map: invalid size %d", n)
	}
	rv.Set(reflect.MakeMapWithSize(t, n))
	// If rv was an interface, get the underlying map
	if rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	data = data[p:]
	for i := 0; i < n; i++ {
		m, p, err := readCollectionSize(data)
		if err != nil {
			return err
		}
		data = data[p:]
		key := reflect.New(t.Key())
		// In case m < 0, the key is null, and unmarshalData should be nil.
		var unmarshalData []byte
		if m >= 0 {
			if len(data) < m {
				return unmarshalErrorf("unmarshal map: unexpected eof")
			}
			unmarshalData = data[:m]
			data = data[m:]
		}
		if err := Unmarshal(info.Key, unmarshalData, key.Interface()); err != nil {
			return err
		}

		m, p, err = readCollectionSize(data)
		if err != nil {
			return err
		}
		data = data[p:]
		val := reflect.New(t.Elem())

		// In case m < 0, the value is null, and unmarshalData should be nil.
		unmarshalData = nil
		if m >= 0 {
			if len(data) < m {
				return unmarshalErrorf("unmarshal map: unexpected eof")
			}
			unmarshalData = data[:m]
			data = data[m:]
		}
		if err := Unmarshal(info.Elem, unmarshalData, val.Interface()); err != nil {
			return err
		}

		rv.SetMapIndex(key.Elem(), val.Elem())
	}
	return nil
}

func marshalUUID(value any) ([]byte, error) {
	switch uv := value.(type) {
	case UUID:
		value = [16]byte(uv)
	case *UUID:
		value = (*[16]byte)(uv)
	}
	data, err := uuid.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalUUID(data []byte, value any) error {
	switch uv := value.(type) {
	case *UUID:
		value = (*[16]byte)(uv)
	case **UUID:
		if uv == nil {
			value = (**[16]byte)(nil)
		} else {
			value = (**[16]byte)(unsafe.Pointer(uv))
		}
	}
	err := uuid.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalTimeUUID(value any) ([]byte, error) {
	switch uv := value.(type) {
	case UUID:
		value = [16]byte(uv)
	case *UUID:
		value = (*[16]byte)(uv)
	}
	data, err := timeuuid.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalTimeUUID(data []byte, value any) error {
	switch uv := value.(type) {
	case *UUID:
		value = (*[16]byte)(uv)
	case **UUID:
		if uv == nil {
			value = (**[16]byte)(nil)
		} else {
			value = (**[16]byte)(unsafe.Pointer(uv))
		}
	}
	err := timeuuid.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalInet(value any) ([]byte, error) {
	data, err := inet.Marshal(value)
	if err != nil {
		return nil, wrapMarshalError(err, "marshal error")
	}
	return data, nil
}

func unmarshalInet(data []byte, value any) error {
	err := inet.Unmarshal(data, value)
	if err != nil {
		return wrapUnmarshalError(err, "unmarshal error")
	}
	return nil
}

func marshalTuple(info TypeInfo, value any) ([]byte, error) {
	tuple := info.(TupleTypeInfo)
	switch v := value.(type) {
	case unsetColumn:
		return nil, unmarshalErrorf("Invalid request: UnsetValue is unsupported for tuples")
	case []any:
		if len(v) != len(tuple.Elems) {
			return nil, unmarshalErrorf("cannont marshal tuple: wrong number of elements")
		}

		var buf []byte
		for i, elem := range v {
			if elem == nil {
				buf = appendIntNeg1(buf)
				continue
			}

			data, err := Marshal(tuple.Elems[i], elem)
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = appendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	}

	rv := reflect.ValueOf(value)
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		if v := t.NumField(); v != len(tuple.Elems) {
			return nil, marshalErrorf("can not marshal tuple into struct %v, not enough fields have %d need %d", t, v, len(tuple.Elems))
		}

		var buf []byte
		for i, elem := range tuple.Elems {
			field := rv.Field(i)

			if field.Kind() == reflect.Ptr && field.IsNil() {
				buf = appendIntNeg1(buf)
				continue
			}

			data, err := Marshal(elem, field.Interface())
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = appendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	case reflect.Slice, reflect.Array:
		size := rv.Len()
		if size != len(tuple.Elems) {
			return nil, marshalErrorf("can not marshal tuple into %v of length %d need %d elements", k, size, len(tuple.Elems))
		}

		var buf []byte
		for i, elem := range tuple.Elems {
			item := rv.Index(i)

			if item.Kind() == reflect.Ptr && item.IsNil() {
				buf = appendIntNeg1(buf)
				continue
			}

			data, err := Marshal(elem, item.Interface())
			if err != nil {
				return nil, err
			}

			n := len(data)
			buf = appendInt(buf, int32(n))
			buf = append(buf, data...)
		}

		return buf, nil
	}

	return nil, marshalErrorf("cannot marshal %T into %s", value, tuple)
}

func readBytes(p []byte) ([]byte, []byte) {
	// TODO: really should use a framer
	size := readInt(p)
	p = p[4:]
	if size < 0 {
		return nil, p
	}
	return p[:size], p[size:]
}

// currently only support unmarshal into a list of values, this makes it possible
// to support tuples without changing the query API. In the future this can be extend
// to allow unmarshalling into custom tuple types.
func unmarshalTuple(info TypeInfo, data []byte, value any) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}

	tuple := info.(TupleTypeInfo)
	switch v := value.(type) {
	case []any:
		for i, elem := range tuple.Elems {
			// each element inside data is a [bytes]
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}
			err := Unmarshal(elem, p, v[i])
			if err != nil {
				return err
			}
		}

		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}

	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		if v := t.NumField(); v != len(tuple.Elems) {
			return unmarshalErrorf("can not unmarshal tuple into struct %v, not enough fields have %d need %d", t, v, len(tuple.Elems))
		}

		for i, elem := range tuple.Elems {
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}

			v, err := elem.NewWithError()
			if err != nil {
				return err
			}
			if err := Unmarshal(elem, p, v); err != nil {
				return err
			}

			switch rv.Field(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					rv.Field(i).Set(reflect.ValueOf(v))
				} else {
					rv.Field(i).Set(reflect.Zero(reflect.TypeOf(v)))
				}
			default:
				rv.Field(i).Set(reflect.ValueOf(v).Elem())
			}
		}

		return nil
	case reflect.Slice, reflect.Array:
		if k == reflect.Array {
			size := rv.Len()
			if size != len(tuple.Elems) {
				return unmarshalErrorf("can not unmarshal tuple into array of length %d need %d elements", size, len(tuple.Elems))
			}
		} else {
			rv.Set(reflect.MakeSlice(t, len(tuple.Elems), len(tuple.Elems)))
		}

		for i, elem := range tuple.Elems {
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}

			v, err := elem.NewWithError()
			if err != nil {
				return err
			}
			if err := Unmarshal(elem, p, v); err != nil {
				return err
			}

			switch rv.Index(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					rv.Index(i).Set(reflect.ValueOf(v))
				} else {
					rv.Index(i).Set(reflect.Zero(reflect.TypeOf(v)))
				}
			default:
				rv.Index(i).Set(reflect.ValueOf(v).Elem())
			}
		}

		return nil
	}

	return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
}

// UDTMarshaler is an interface which should be implemented by users wishing to
// handle encoding UDT types to sent to Cassandra. Note: due to current implentations
// methods defined for this interface must be value receivers not pointer receivers.
type UDTMarshaler interface {
	// MarshalUDT will be called for each field in the the UDT returned by Cassandra,
	// the implementor should marshal the type to return by for example calling
	// Marshal.
	MarshalUDT(name string, info TypeInfo) ([]byte, error)
}

// UDTUnmarshaler should be implemented by users wanting to implement custom
// UDT unmarshaling.
type UDTUnmarshaler interface {
	// UnmarshalUDT will be called for each field in the UDT return by Cassandra,
	// the implementor should unmarshal the data into the value of their chosing,
	// for example by calling Unmarshal.
	//
	// The data []byte slice is only valid for the duration of the call.
	// The backing memory may be reused after the call returns.
	// Implementations that need to retain data must copy it.
	UnmarshalUDT(name string, info TypeInfo, data []byte) error
}

func marshalUDT(info TypeInfo, value any) ([]byte, error) {
	udt := info.(UDTTypeInfo)

	switch v := value.(type) {
	case Marshaler:
		return v.MarshalCQL(info)
	case unsetColumn:
		return nil, unmarshalErrorf("invalid request: UnsetValue is unsupported for user defined types")
	case UDTMarshaler:
		var buf []byte
		for _, e := range udt.Elements {
			data, err := v.MarshalUDT(e.Name, e.Type)
			if err != nil {
				return nil, err
			}

			buf = appendBytes(buf, data)
		}

		return buf, nil
	case map[string]any:
		var buf []byte
		for _, e := range udt.Elements {
			val, ok := v[e.Name]
			var data []byte

			if ok {
				var err error
				data, err = Marshal(e.Type, val)
				if err != nil {
					return nil, err
				}
			}

			buf = appendBytes(buf, data)
		}

		return buf, nil
	}

	k := reflect.ValueOf(value)
	if k.Kind() == reflect.Ptr {
		if k.IsNil() {
			return nil, marshalErrorf("cannot marshal %T into %s", value, info)
		}
		k = k.Elem()
	}

	if k.Kind() != reflect.Struct || !k.IsValid() {
		return nil, marshalErrorf("cannot marshal %T into %s", value, info)
	}

	fields := make(map[string]reflect.Value)
	t := reflect.TypeOf(value)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		if tag := sf.Tag.Get("cql"); tag != "" {
			fields[tag] = k.Field(i)
		}
	}

	var buf []byte
	for _, e := range udt.Elements {
		f, ok := fields[e.Name]
		if !ok {
			f = k.FieldByName(e.Name)
		}

		var data []byte
		if f.IsValid() && f.CanInterface() {
			var err error
			data, err = Marshal(e.Type, f.Interface())
			if err != nil {
				return nil, err
			}
		}

		buf = appendBytes(buf, data)
	}

	return buf, nil
}

func unmarshalUDT(info TypeInfo, data []byte, value any) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case UDTUnmarshaler:
		udt := info.(UDTTypeInfo)

		for id, e := range udt.Elements {
			if len(data) == 0 {
				return nil
			}
			if len(data) < 4 {
				return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", info, id, e.Name)
			}

			var p []byte
			p, data = readBytes(data)
			if err := v.UnmarshalUDT(e.Name, e.Type, p); err != nil {
				return err
			}
		}

		return nil
	case *any:
		if v != nil {
			if data == nil {
				*v = nil
				return nil
			}
			var m map[string]any
			if err := unmarshalUDTIntoMap(info.(UDTTypeInfo), data, &m); err != nil {
				return err
			}
			*v = m
			return nil
		}
	case *map[string]any:
		return unmarshalUDTIntoMap(info.(UDTTypeInfo), data, v)
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	k := rv.Elem()
	if k.Kind() != reflect.Struct || !k.IsValid() {
		return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
	}

	if len(data) == 0 {
		if k.CanSet() {
			k.Set(reflect.Zero(k.Type()))
		}

		return nil
	}

	t := k.Type()
	fields := make(map[string]reflect.Value, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		if tag := sf.Tag.Get("cql"); tag != "" {
			fields[tag] = k.Field(i)
		}
	}

	udt := info.(UDTTypeInfo)
	for id, e := range udt.Elements {
		if len(data) == 0 {
			return nil
		}
		if len(data) < 4 {
			// UDT def does not match the column value
			return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", info, id, e.Name)
		}

		var p []byte
		p, data = readBytes(data)

		f, ok := fields[e.Name]
		if !ok {
			f = k.FieldByName(e.Name)
			if f == emptyValue { //nolint:govet // no other way to do that
				// skip fields which exist in the UDT but not in
				// the struct passed in
				continue
			}
		}

		if !f.IsValid() || !f.CanAddr() {
			return unmarshalErrorf("cannot unmarshal %s into %T: field %v is not valid", info, value, e.Name)
		}

		fk := f.Addr().Interface()
		if err := Unmarshal(e.Type, p, fk); err != nil {
			return err
		}
	}

	return nil
}

// unmarshalUDTIntoMap unmarshals UDT data into a *map[string]any.
func unmarshalUDTIntoMap(udt UDTTypeInfo, data []byte, dstMap *map[string]any) error {
	if data == nil {
		*dstMap = nil
		return nil
	}

	m := make(map[string]any, len(udt.Elements))
	*dstMap = m

	for id, e := range udt.Elements {
		if len(data) == 0 {
			return nil
		}
		if len(data) < 4 {
			return unmarshalErrorf("can not unmarshal %s: field [%d]%s: unexpected eof", udt, id, e.Name)
		}

		valType, err := goType(e.Type)
		if err != nil {
			return unmarshalErrorf("can not unmarshal %s: %v", udt, err)
		}

		val := reflect.New(valType)

		var p []byte
		p, data = readBytes(data)

		if err := Unmarshal(e.Type, p, val.Interface()); err != nil {
			return err
		}
		m[e.Name] = val.Elem().Interface()
	}

	return nil
}

// TypeInfo describes a Cassandra specific data type.
type TypeInfo interface {
	Type() Type
	Version() byte
	Custom() string

	// NewWithError creates a pointer to an empty version of whatever type
	// is referenced by the TypeInfo receiver.
	//
	// If there is no corresponding Go type for the CQL type, NewWithError returns an error.
	NewWithError() (any, error)
}

type NativeType struct {
	//only used for TypeCustom
	custom string
	typ    Type
	proto  byte
}

func NewNativeType(proto byte, typ Type) NativeType {
	return NativeType{proto: proto, typ: typ, custom: ""}
}

func NewCustomType(proto byte, typ Type, custom string) NativeType {
	return NativeType{proto: proto, typ: typ, custom: custom}
}

func (t NativeType) NewWithError() (any, error) {
	// Fast path for common types to avoid reflection overhead
	switch t.typ {
	case TypeInt:
		return new(int), nil
	case TypeBigInt, TypeCounter:
		return new(int64), nil
	case TypeVarchar, TypeAscii, TypeText, TypeInet:
		return new(string), nil
	case TypeBoolean:
		return new(bool), nil
	case TypeFloat:
		return new(float32), nil
	case TypeDouble:
		return new(float64), nil
	case TypeTimestamp, TypeDate:
		return new(time.Time), nil
	case TypeUUID, TypeTimeUUID:
		return new(UUID), nil
	case TypeBlob:
		return new([]byte), nil
	case TypeSmallInt:
		return new(int16), nil
	case TypeTinyInt:
		return new(int8), nil
	case TypeTime:
		return new(time.Duration), nil
	case TypeDecimal:
		return new(*inf.Dec), nil
	case TypeVarint:
		return new(*big.Int), nil
	case TypeDuration:
		return new(Duration), nil
	}

	// Fallback to reflection for complex/custom types
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t NativeType) Type() Type {
	return t.typ
}

func (t NativeType) Version() byte {
	return t.proto
}

func (t NativeType) Custom() string {
	return t.custom
}

func (t NativeType) String() string {
	switch t.typ {
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", t.typ, t.custom)
	default:
		return t.typ.String()
	}
}

func NewCollectionType(m NativeType, key, elem TypeInfo) CollectionType {
	return CollectionType{
		NativeType: m,
		Key:        key,
		Elem:       elem,
	}
}

type CollectionType struct {
	// Key is used only for TypeMap
	Key TypeInfo
	// Elem is used for TypeMap, TypeList and TypeSet
	Elem TypeInfo
	NativeType
}

type VectorType struct {
	SubType TypeInfo
	NativeType
	Dimensions int
}

// Zero returns the zero value for the vector CQL type.
func (v VectorType) Zero() any {
	t, e := v.SubType.NewWithError()
	if e != nil {
		return nil
	}
	return reflect.Zero(reflect.SliceOf(reflect.TypeOf(t))).Interface()
}

// NewWithError returns a pointer to an empty slice of the appropriate Go type
// for common vector element types, avoiding the expensive goType() → asVectorType()
// re-parse of Java type strings on every call.
func (v VectorType) NewWithError() (any, error) {
	if nt, ok := v.SubType.(NativeType); ok {
		switch nt.typ {
		case TypeFloat:
			return new([]float32), nil
		case TypeDouble:
			return new([]float64), nil
		case TypeInt:
			return new([]int), nil
		case TypeBigInt:
			return new([]int64), nil
		case TypeTimestamp:
			return new([]time.Time), nil
		case TypeUUID, TypeTimeUUID:
			return new([]UUID), nil
		case TypeText, TypeVarchar, TypeAscii:
			return new([]string), nil
		case TypeBlob:
			return new([][]byte), nil
		case TypeBoolean:
			return new([]bool), nil
		case TypeSmallInt:
			return new([]int16), nil
		case TypeTinyInt:
			return new([]int8), nil
		case TypeTime:
			return new([]time.Duration), nil
		case TypeCounter:
			return new([]int64), nil
		case TypeDate:
			return new([]time.Time), nil
		}
	}
	// Fallback to reflection for complex/nested types
	typ, err := goType(v)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t CollectionType) NewWithError() (any, error) {
	// Fast path for common collection patterns
	switch t.typ {
	case TypeList, TypeSet:
		// Fast path for lists/sets of primitive types
		if nt, ok := t.Elem.(NativeType); ok {
			switch nt.typ {
			case TypeInt:
				return new([]int), nil
			case TypeBigInt, TypeCounter:
				return new([]int64), nil
			case TypeText, TypeVarchar, TypeAscii:
				return new([]string), nil
			case TypeBoolean:
				return new([]bool), nil
			case TypeFloat:
				return new([]float32), nil
			case TypeDouble:
				return new([]float64), nil
			case TypeUUID, TypeTimeUUID:
				return new([]UUID), nil
			case TypeTimestamp, TypeDate:
				return new([]time.Time), nil
			case TypeSmallInt:
				return new([]int16), nil
			case TypeTinyInt:
				return new([]int8), nil
			case TypeBlob:
				return new([][]byte), nil
			}
		}
	case TypeMap:
		// Fast path for maps with primitive key/value types
		if keyNT, keyOk := t.Key.(NativeType); keyOk {
			if valNT, valOk := t.Elem.(NativeType); valOk {
				// String keys are most common
				if keyNT.typ == TypeText || keyNT.typ == TypeVarchar {
					switch valNT.typ {
					case TypeInt:
						return new(map[string]int), nil
					case TypeBigInt:
						return new(map[string]int64), nil
					case TypeText, TypeVarchar:
						return new(map[string]string), nil
					case TypeBoolean:
						return new(map[string]bool), nil
					case TypeFloat:
						return new(map[string]float32), nil
					case TypeDouble:
						return new(map[string]float64), nil
					case TypeUUID:
						return new(map[string]UUID), nil
					}
				}
				// Int keys
				if keyNT.typ == TypeInt {
					switch valNT.typ {
					case TypeText, TypeVarchar:
						return new(map[int]string), nil
					case TypeInt:
						return new(map[int]int), nil
					case TypeFloat:
						return new(map[int]float32), nil
					}
				}
			}
		}
	}

	// Fallback to reflection for complex types
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t CollectionType) String() string {
	switch t.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", t.typ, t.Key, t.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", t.typ, t.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", t.typ, t.custom)
	default:
		return t.typ.String()
	}
}

func NewTupleType(n NativeType, elems ...TypeInfo) TupleTypeInfo {
	return TupleTypeInfo{
		NativeType: n,
		Elems:      elems,
	}
}

type TupleTypeInfo struct {
	Elems []TypeInfo
	NativeType
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

func (t TupleTypeInfo) NewWithError() (any, error) {
	// Tuples scan into *[]any — no reflection needed.
	return new([]any), nil
}

type UDTField struct {
	Type TypeInfo
	Name string
}

func NewUDTType(proto byte, name, keySpace string, elems ...UDTField) UDTTypeInfo {
	return UDTTypeInfo{
		NativeType: NativeType{proto: proto, typ: TypeUDT, custom: ""},
		Name:       name,
		KeySpace:   keySpace,
		Elements:   elems,
	}
}

type UDTTypeInfo struct {
	KeySpace string
	Name     string
	Elements []UDTField
	NativeType
}

func (t UDTTypeInfo) NewWithError() (any, error) {
	typ, err := goType(t)
	if err != nil {
		return nil, err
	}
	return reflect.New(typ).Interface(), nil
}

func (t UDTTypeInfo) String() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%s.%s{", t.KeySpace, t.Name)
	first := true
	for _, e := range t.Elements {
		if !first {
			fmt.Fprint(buf, ",")
		} else {
			first = false
		}

		fmt.Fprintf(buf, "%s=%v", e.Name, e.Type)
	}
	fmt.Fprint(buf, "}")

	return buf.String()
}

// String returns a human readable name for the Cassandra datatype
// described by t.
// Type is the identifier of a Cassandra internal datatype.
type Type int

const (
	TypeCustom    Type = 0x0000
	TypeAscii     Type = 0x0001
	TypeBigInt    Type = 0x0002
	TypeBlob      Type = 0x0003
	TypeBoolean   Type = 0x0004
	TypeCounter   Type = 0x0005
	TypeDecimal   Type = 0x0006
	TypeDouble    Type = 0x0007
	TypeFloat     Type = 0x0008
	TypeInt       Type = 0x0009
	TypeText      Type = 0x000A
	TypeTimestamp Type = 0x000B
	TypeUUID      Type = 0x000C
	TypeVarchar   Type = 0x000D
	TypeVarint    Type = 0x000E
	TypeTimeUUID  Type = 0x000F
	TypeInet      Type = 0x0010
	TypeDate      Type = 0x0011
	TypeTime      Type = 0x0012
	TypeSmallInt  Type = 0x0013
	TypeTinyInt   Type = 0x0014
	TypeDuration  Type = 0x0015
	TypeList      Type = 0x0020
	TypeMap       Type = 0x0021
	TypeSet       Type = 0x0022
	TypeUDT       Type = 0x0030
	TypeTuple     Type = 0x0031
)

// String returns the name of the identifier.
func (t Type) String() string {
	switch t {
	case TypeCustom:
		return "custom"
	case TypeAscii:
		return "ascii"
	case TypeBigInt:
		return "bigint"
	case TypeBlob:
		return "blob"
	case TypeBoolean:
		return "boolean"
	case TypeCounter:
		return "counter"
	case TypeDecimal:
		return "decimal"
	case TypeDouble:
		return "double"
	case TypeFloat:
		return "float"
	case TypeInt:
		return "int"
	case TypeText:
		return "text"
	case TypeTimestamp:
		return "timestamp"
	case TypeUUID:
		return "uuid"
	case TypeVarchar:
		return "varchar"
	case TypeTimeUUID:
		return "timeuuid"
	case TypeInet:
		return "inet"
	case TypeDate:
		return "date"
	case TypeDuration:
		return "duration"
	case TypeTime:
		return "time"
	case TypeSmallInt:
		return "smallint"
	case TypeTinyInt:
		return "tinyint"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	case TypeVarint:
		return "varint"
	case TypeTuple:
		return "tuple"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}

type MarshalError struct {
	cause error
	msg   string
}

func (m MarshalError) Error() string {
	if m.cause != nil {
		return m.msg + ": " + m.cause.Error()
	}
	return m.msg
}

func (m MarshalError) Cause() error { return m.cause }

func (m MarshalError) Unwrap() error {
	return m.cause
}

func marshalErrorf(format string, args ...any) MarshalError {
	return MarshalError{msg: fmt.Sprintf(format, args...)}
}

func wrapMarshalError(err error, msg string) MarshalError {
	return MarshalError{msg: msg, cause: err}
}

func wrapMarshalErrorf(err error, format string, a ...any) MarshalError {
	return MarshalError{msg: fmt.Sprintf(format, a...), cause: err}
}

type UnmarshalError struct {
	cause error
	msg   string
}

func (m UnmarshalError) Error() string {
	if m.cause != nil {
		return m.msg + ": " + m.cause.Error()
	}
	return m.msg
}

func (m UnmarshalError) Cause() error { return m.cause }

func (m UnmarshalError) Unwrap() error {
	return m.cause
}

func unmarshalErrorf(format string, args ...any) UnmarshalError {
	return UnmarshalError{msg: fmt.Sprintf(format, args...)}
}

func wrapUnmarshalError(err error, msg string) UnmarshalError {
	return UnmarshalError{msg: msg, cause: err}
}

func wrapUnmarshalErrorf(err error, format string, a ...any) UnmarshalError {
	return UnmarshalError{msg: fmt.Sprintf(format, a...), cause: err}
}

// --- Collection marshal fast paths ---

func marshalListString(list []string) ([]byte, error) {
	n, err := checkMarshalLen(len(list))
	if err != nil {
		return nil, err
	}
	total := 4 + 4*len(list)
	for _, s := range list {
		total += len(s)
	}
	buf, off := allocCollBuf(total, n)
	for _, s := range list {
		off = writeStr(buf, off, s)
	}
	return buf, nil
}

func marshalListBool(list []bool) ([]byte, error) {
	n, err := checkMarshalLen(len(list))
	if err != nil {
		return nil, err
	}
	buf, off := allocCollBuf(4+n*(4+1), n)
	for _, b := range list {
		off = putLen(buf, off, 1)
		if b {
			buf[off] = 1
		}
		off++
	}
	return buf, nil
}

func marshalListBytes(list [][]byte) ([]byte, error) {
	n, err := checkMarshalLen(len(list))
	if err != nil {
		return nil, err
	}
	total := 4 + 4*len(list)
	for _, b := range list {
		if b != nil {
			total += len(b)
		}
	}
	buf, off := allocCollBuf(total, n)
	for _, b := range list {
		off = writeBytes(buf, off, b)
	}
	return buf, nil
}

func marshalListInt16(list []int16) ([]byte, error) {
	n, err := checkMarshalLen(len(list))
	if err != nil {
		return nil, err
	}
	buf, off := allocCollBuf(4+n*(4+2), n)
	for _, v := range list {
		off = putLen(buf, off, 2)
		binary.BigEndian.PutUint16(buf[off:], uint16(v))
		off += 2
	}
	return buf, nil
}

func marshalListInt8(list []int8) ([]byte, error) {
	n, err := checkMarshalLen(len(list))
	if err != nil {
		return nil, err
	}
	buf, off := allocCollBuf(4+n*(4+1), n)
	for _, v := range list {
		off = putLen(buf, off, 1)
		buf[off] = byte(v)
		off++
	}
	return buf, nil
}

func marshalListUUID(list []UUID) ([]byte, error) {
	n, err := checkMarshalLen(len(list))
	if err != nil {
		return nil, err
	}
	buf, off := allocCollBuf(4+n*(4+16), n)
	for _, v := range list {
		off = putLen(buf, off, 16)
		copy(buf[off:], v[:])
		off += 16
	}
	return buf, nil
}

// --- Map marshal fast paths ---

func marshalMapStringString(m map[string]string) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys := make([]string, 0, n)
	vals := make([]string, 0, n)
	total := 4 + 8*len(m)
	for k, v := range m {
		keys = append(keys, k)
		vals = append(vals, v)
		total += len(k) + len(v)
	}
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = writeStr(buf, off, vals[i])
	}
	return buf, nil
}

func marshalMapStringInt64(m map[string]int64) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys, vals, total := extractMapStrKey(m, 8)
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], uint64(vals[i]))
		off += 8
	}
	return buf, nil
}

func marshalMapStringInt(m map[string]int) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys, vals, total := extractMapStrKey(m, 4)
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		if vals[i] > math.MaxInt32 || vals[i] < math.MinInt32 {
			return nil, marshalErrorf("marshal: value %d out of range for int", vals[i])
		}
		off = writeStr(buf, off, keys[i])
		off = putLen(buf, off, 4)
		binary.BigEndian.PutUint32(buf[off:], uint32(vals[i]))
		off += 4
	}
	return buf, nil
}

func marshalMapStringFloat64(m map[string]float64) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys, vals, total := extractMapStrKey(m, 8)
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], math.Float64bits(vals[i]))
		off += 8
	}
	return buf, nil
}

func marshalMapStringFloat32(m map[string]float32) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys, vals, total := extractMapStrKey(m, 4)
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = putLen(buf, off, 4)
		binary.BigEndian.PutUint32(buf[off:], math.Float32bits(vals[i]))
		off += 4
	}
	return buf, nil
}

func marshalMapStringBool(m map[string]bool) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys, vals, total := extractMapStrKey(m, 1)
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = putLen(buf, off, 1)
		if vals[i] {
			buf[off] = 1
		}
		off++
	}
	return buf, nil
}

func marshalMapStringBytes(m map[string][]byte) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys := make([]string, 0, n)
	vals := make([][]byte, 0, n)
	total := 4 + 8*len(m)
	for k, v := range m {
		keys = append(keys, k)
		vals = append(vals, v)
		total += len(k)
		if v != nil {
			total += len(v)
		}
	}
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = writeBytes(buf, off, vals[i])
	}
	return buf, nil
}

func marshalMapStringInt16(m map[string]int16) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys, vals, total := extractMapStrKey(m, 2)
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = writeStr(buf, off, keys[i])
		off = putLen(buf, off, 2)
		binary.BigEndian.PutUint16(buf[off:], uint16(vals[i]))
		off += 2
	}
	return buf, nil
}

func marshalMapInt64String(m map[int64]string) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return make([]byte, 4), nil
	}
	keys := make([]int64, 0, n)
	vals := make([]string, 0, n)
	total := 4 + 16*len(m)
	for k, v := range m {
		keys = append(keys, k)
		vals = append(vals, v)
		total += len(v)
	}
	buf, off := allocCollBuf(total, n)
	for i := range keys {
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], uint64(keys[i]))
		off += 8
		off = writeStr(buf, off, vals[i])
	}
	return buf, nil
}

func marshalMapInt64Int64(m map[int64]int64) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	buf, off := allocCollBuf(4+n*(4+8+4+8), n)
	for k, v := range m {
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], uint64(k))
		off += 8
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], uint64(v))
		off += 8
	}
	return buf, nil
}

func marshalMapInt64Float64(m map[int64]float64) ([]byte, error) {
	n, err := checkMarshalLen(len(m))
	if err != nil {
		return nil, err
	}
	buf, off := allocCollBuf(4+n*(4+8+4+8), n)
	for k, v := range m {
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], uint64(k))
		off += 8
		off = putLen(buf, off, 8)
		binary.BigEndian.PutUint64(buf[off:], math.Float64bits(v))
		off += 8
	}
	return buf, nil
}
