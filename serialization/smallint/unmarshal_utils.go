package smallint

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
)

var errWrongDataLen = fmt.Errorf("failed to unmarshal smallint: the length of the data should be 0 or 2")

func errNilReference(v interface{}) error {
	return fmt.Errorf("failed to unmarshal smallint: can not unmarshal into nil reference (%T)(%[1]v))", v)
}

func DecInt8(p []byte, v *int8) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		val := decInt16(p)
		if val > math.MaxInt8 || val < math.MinInt8 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into int8, the data value should be in the int8 range")
		}
		*v = int8(val)
	default:
		return errWrongDataLen
	}
	return nil
}

func DecInt8R(p []byte, v **int8) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(int8)
		return DecInt8(p, *v)
	}
	*v = nil
	return nil
}

func DecInt16(p []byte, v *int16) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		*v = decInt16(p)
	default:
		return errWrongDataLen
	}
	return nil
}

func DecInt16R(p []byte, v **int16) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(int16)
		return DecInt16(p, *v)
	}
	*v = nil
	return nil
}

func DecInt32(p []byte, v *int32) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		*v = int32(decInt16(p))
	default:
		return errWrongDataLen
	}
	return nil
}

func DecInt32R(p []byte, v **int32) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(int32)
		return DecInt32(p, *v)
	}
	*v = nil
	return nil
}

func DecInt64(p []byte, v *int64) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		*v = int64(decInt16(p))
	default:
		return errWrongDataLen
	}
	return nil
}

func DecInt64R(p []byte, v **int64) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(int64)
		return DecInt64(p, *v)
	}
	*v = nil
	return nil
}

func DecInt(p []byte, v *int) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		*v = int(decInt16(p))
	default:
		return errWrongDataLen
	}
	return nil
}

func DecIntR(p []byte, v **int) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(int)
		return DecInt(p, *v)
	}
	*v = nil
	return nil
}

func DecUint8(p []byte, v *uint8) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		if p[0] != 0 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into uint8, the data value should be in the uint8 range")
		}
		*v = p[1]
	default:
		return errWrongDataLen
	}
	return nil
}

func DecUint8R(p []byte, v **uint8) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(uint8)
		return DecUint8(p, *v)
	}
	*v = nil
	return nil
}

func DecUint16(p []byte, v *uint16) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		if p[0] > math.MaxInt8 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into uint16, the data value should be positive")
		}
		*v = uint16(p[0])<<8 | uint16(p[1])
	default:
		return errWrongDataLen
	}
	return nil
}

func DecUint16R(p []byte, v **uint16) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(uint16)
		return DecUint16(p, *v)
	}
	*v = nil
	return nil
}

func DecUint32(p []byte, v *uint32) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		if p[0] > math.MaxInt8 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into uint32, the data value should be positive")
		}
		*v = uint32(p[0])<<8 | uint32(p[1])
	default:
		return errWrongDataLen
	}
	return nil
}

func DecUint32R(p []byte, v **uint32) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(uint32)
		return DecUint32(p, *v)
	}
	*v = nil
	return nil
}

func DecUint64(p []byte, v *uint64) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		if p[0] > math.MaxInt8 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into uint64, the data value should be positive")
		}
		*v = decUint64(p)
	default:
		return errWrongDataLen
	}
	return nil
}

func DecUint64R(p []byte, v **uint64) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(uint64)
		return DecUint64(p, *v)
	}
	*v = nil
	return nil
}

func DecUint(p []byte, v *uint) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
	case 2:
		if p[0] > math.MaxInt8 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into uint, the data value should be positive")
		}
		*v = uint(p[0])<<8 | uint(p[1])
	default:
		return errWrongDataLen
	}
	return nil
}

func DecUintR(p []byte, v **uint) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(uint)
		return DecUint(p, *v)
	}
	*v = nil
	return nil
}

func DecString(p []byte, v *string) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		if p != nil {
			*v = "0"
		} else {
			*v = ""
		}
	case 2:
		*v = strconv.FormatInt(int64(decInt16(p)), 10)
	default:
		return errWrongDataLen
	}
	return nil
}

func DecStringR(p []byte, v **string) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = new(string)
		return DecString(p, *v)
	}
	*v = nil
	return nil
}

func DecBigInt(p []byte, v *big.Int) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		v.SetInt64(0)
	case 2:
		v.SetInt64(int64(decInt16(p)))
	default:
		return errWrongDataLen
	}
	return nil
}

func DecBigIntR(p []byte, v **big.Int) error {
	if v == nil {
		return errNilReference(v)
	}
	if p != nil {
		*v = big.NewInt(0)
		return DecBigInt(p, *v)
	}
	*v = nil
	return nil
}

func DecReflect(p []byte, v reflect.Value) error {
	if v.IsNil() {
		return fmt.Errorf("failed to unmarshal smallint: can not unmarshal into nil reference (%T)(%[1]v)", v.Interface())
	}

	switch v = v.Elem(); v.Kind() {
	case reflect.Int8:
		return decReflectInt8(p, v)
	case reflect.Uint8:
		return decReflectUint8(p, v)
	case reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return decReflectInts(p, v)
	case reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return decReflectUints(p, v)
	case reflect.String:
		return decReflectString(p, v)
	default:
		return fmt.Errorf("failed to unmarshal smallint: unsupported value type (%T)(%[1]v)", v.Interface())
	}
}

func DecReflectR(p []byte, v reflect.Value) error {
	if p != nil {
		zeroValue := reflect.New(v.Type().Elem().Elem())
		v.Elem().Set(zeroValue)
		return DecReflect(p, v.Elem())
	}
	nilValue := reflect.Zero(v.Elem().Type())
	v.Elem().Set(nilValue)
	return nil
}

func decReflectInt8(p []byte, v reflect.Value) error {
	switch len(p) {
	case 0:
		v.SetInt(0)
	case 2:
		val := decInt16(p)
		if val > math.MaxInt8 || val < math.MinInt8 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into %T, the data value should be in the int8 range", v.Interface())
		}
		v.SetInt(int64(val))
	default:
		return errWrongDataLen
	}
	return nil
}

func decReflectInts(p []byte, v reflect.Value) error {
	switch len(p) {
	case 0:
		v.SetInt(0)
	case 2:
		v.SetInt(int64(decInt16(p)))
	default:
		return errWrongDataLen
	}
	return nil
}

func decReflectUint8(p []byte, v reflect.Value) error {
	switch len(p) {
	case 0:
		v.SetUint(0)
	case 2:
		if p[0] != 0 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into %T, the data value should be in the uint8 range", v.Interface())
		}
		v.SetUint(uint64(p[1]))
	default:
		return errWrongDataLen
	}
	return nil
}

func decReflectUints(p []byte, v reflect.Value) error {
	switch len(p) {
	case 0:
		v.SetUint(0)
	case 2:
		val := decUint64(p)
		if val > math.MaxInt16 {
			return fmt.Errorf("failed to unmarshal smallint: to unmarshal into %T, the data value should be positive", v.Interface())
		}
		v.SetUint(val)
	default:
		return errWrongDataLen
	}
	return nil
}

func decReflectString(p []byte, v reflect.Value) error {
	switch len(p) {
	case 0:
		if p != nil {
			v.SetString("0")
		} else {
			v.SetString("")
		}
	case 2:
		v.SetString(strconv.FormatInt(int64(decInt16(p)), 10))
	default:
		return errWrongDataLen
	}
	return nil
}

func decInt16(p []byte) int16 {
	return int16(p[0])<<8 | int16(p[1])
}

func decUint64(p []byte) uint64 {
	return uint64(p[0])<<8 | uint64(p[1])
}
