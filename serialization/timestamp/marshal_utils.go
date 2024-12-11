package timestamp

import (
	"fmt"
	"reflect"
	"time"
)

const (
	maxValInt64 int64         = 86399999999999
	minValInt64 int64         = 0
	maxValDur   time.Duration = 86399999999999
	minValDur   time.Duration = 0
)

func EncInt64(v int64) ([]byte, error) {
	return encInt64(v), nil
}

func EncInt64R(v *int64) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncInt64(*v)
}

func EncTime(v time.Time) ([]byte, error) {
	if v.IsZero() {
		return make([]byte, 0), nil
	}
	ms := v.Unix()*1e3 + int64(v.Nanosecond())/1e6
	return []byte{byte(ms >> 56), byte(ms >> 48), byte(ms >> 40), byte(ms >> 32), byte(ms >> 24), byte(ms >> 16), byte(ms >> 8), byte(ms)}, nil
}

func EncTimeR(v *time.Time) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return EncTime(*v)
}

func EncReflect(v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.Int64:
		return encInt64(v.Int()), nil
	case reflect.Struct:
		if v.Type().String() == "gocql.unsetColumn" {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to marshal timestamp: unsupported value type (%T)(%[1]v)", v.Interface())
	default:
		return nil, fmt.Errorf("failed to marshal timestamp: unsupported value type (%T)(%[1]v)", v.Interface())
	}
}

func EncReflectR(v reflect.Value) ([]byte, error) {
	if v.IsNil() {
		return nil, nil
	}
	return EncReflect(v.Elem())
}

func encInt64(v int64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}