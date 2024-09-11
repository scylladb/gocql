package funcs

import (
	"bytes"
	"fmt"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/gocql/gocql/marshal/tests/mods"
)

var Default = func(m func(interface{}) ([]byte, error), u func([]byte, interface{}) error) *List {
	return &List{
		Marshal:   m,
		Unmarshal: u,
		NewVar:    DefaultNew,
		EqualData: DefaultEqualData,
		EqualVals: DefaultEqualVals,
	}
}

func DefaultEqualData(in1, in2 []byte) bool {
	if in1 == nil || in2 == nil {
		return in1 == nil && in2 == nil
	}
	return bytes.Equal(in1, in2)
}

func DefaultNew(in interface{}) interface{} {
	out := reflect.New(reflect.TypeOf(in)).Interface()
	return out
}

func DefaultEqualVals(in1, in2 interface{}) bool {
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
	case *mods.Float32:
		vin2 := in2.(*mods.Float32)
		return *(*[4]byte)(unsafe.Pointer(vin1)) == *(*[4]byte)(unsafe.Pointer(vin2))
	case mods.Float32:
		vin2 := in2.(mods.Float32)
		return *(*[4]byte)(unsafe.Pointer(&vin1)) == *(*[4]byte)(unsafe.Pointer(&vin2))
	case float64:
		vin2 := in2.(float64)
		return *(*[8]byte)(unsafe.Pointer(&vin1)) == *(*[8]byte)(unsafe.Pointer(&vin2))
	case *float64:
		vin2 := in2.(*float64)
		return *(*[8]byte)(unsafe.Pointer(vin1)) == *(*[8]byte)(unsafe.Pointer(vin2))
	case *mods.Float64:
		vin2 := in2.(*mods.Float64)
		return *(*[8]byte)(unsafe.Pointer(vin1)) == *(*[8]byte)(unsafe.Pointer(vin2))
	case mods.Float64:
		vin2 := in2.(mods.Float64)
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
