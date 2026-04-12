package serialization

import (
	"reflect"
)

func newRef(in any) any {
	out := reflect.New(reflect.TypeOf(in)).Interface()
	return out
}

func newRefToZero(in any) any {
	rv := reflect.ValueOf(in)
	nw := reflect.New(rv.Type().Elem())
	out := reflect.New(rv.Type())
	out.Elem().Set(nw)
	return out.Interface()
}
