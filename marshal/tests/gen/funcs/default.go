package funcs

import (
	"bytes"
	"fmt"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
)

func ExcludedMarshal() *Funcs { return Default(nil, nil).ExcludeMarshal() }

func ExcludedUnmarshal() *Funcs { return Default(nil, nil).ExcludeUnmarshal() }

func Default(m func(interface{}) ([]byte, error), u func([]byte, interface{}) error) *Funcs {
	return &Funcs{
		Marshal:   m,
		Unmarshal: u,
		NewVar:    defaultNew,
		EqualData: defaultEqualData,
		EqualVals: defaultEqualVals,
	}
}

func defaultEqualData(in1, in2 []byte) bool {
	if in1 == nil || in2 == nil {
		return in1 == nil && in2 == nil
	}
	return bytes.Equal(in1, in2)
}

func defaultNew(in interface{}) interface{} {
	out := reflect.New(reflect.TypeOf(in)).Interface()
	return out
}

func defaultEqualVals(in1, in2 interface{}) bool {
	rin1 := reflect.ValueOf(in1)
	rin2 := reflect.ValueOf(in2)
	if rin1.Kind() == reflect.Ptr && (rin1.IsNil() || rin2.IsNil()) {
		return rin1.IsNil() && rin2.IsNil()
	}

	switch vin1 := in1.(type) {
	case float32:
		vin2 := in2.(float32)
		return *(*[4]byte)(unsafe.Pointer(&vin1)) == *(*[4]byte)(unsafe.Pointer(&vin2))
	case *float32:
		vin2 := in2.(*float32)
		return *(*[4]byte)(unsafe.Pointer(vin1)) == *(*[4]byte)(unsafe.Pointer(vin2))
	case *mod.Float32:
		vin2 := in2.(*mod.Float32)
		return *(*[4]byte)(unsafe.Pointer(vin1)) == *(*[4]byte)(unsafe.Pointer(vin2))
	case mod.Float32:
		vin2 := in2.(mod.Float32)
		return *(*[4]byte)(unsafe.Pointer(&vin1)) == *(*[4]byte)(unsafe.Pointer(&vin2))
	case float64:
		vin2 := in2.(float64)
		return *(*[8]byte)(unsafe.Pointer(&vin1)) == *(*[8]byte)(unsafe.Pointer(&vin2))
	case *float64:
		vin2 := in2.(*float64)
		return *(*[8]byte)(unsafe.Pointer(vin1)) == *(*[8]byte)(unsafe.Pointer(vin2))
	case *mod.Float64:
		vin2 := in2.(*mod.Float64)
		return *(*[8]byte)(unsafe.Pointer(vin1)) == *(*[8]byte)(unsafe.Pointer(vin2))
	case mod.Float64:
		vin2 := in2.(mod.Float64)
		return *(*[8]byte)(unsafe.Pointer(&vin1)) == *(*[8]byte)(unsafe.Pointer(&vin2))
	case big.Int:
		vin2 := in2.(big.Int)
		return vin1.Cmp(&vin2) == 0
	case *big.Int:
		vin2 := in2.(*big.Int)
		return vin1.Cmp(vin2) == 0
	case inf.Dec:
		vin2 := in2.(inf.Dec)
		return vin1.Cmp(&vin2) == 0
	case *inf.Dec:
		vin2 := in2.(*inf.Dec)
		return vin1.Cmp(vin2) == 0
	case fmt.Stringer:
		vin2 := in2.(fmt.Stringer)
		return vin1.String() == vin2.String()
	default:
		return reflect.DeepEqual(in1, in2)
	}
}
