package tests

import (
	"fmt"
	"reflect"
)

type wasPanic struct {
	p error
	s []byte
}

func (e wasPanic) Error() string {
	return fmt.Sprintf("%v\n%s", e.p, e.s)
}

func getPtr(i interface{}) uintptr {
	rv := reflect.ValueOf(i)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return 0
		}
		if rv.Elem().Kind() == reflect.Ptr {
			return getPtr(rv.Elem().Interface())
		}
		return rv.Pointer()
	}
	return 0
}

func delNils(in []interface{}) []interface{} {
	out := make([]interface{}, 0)
	for i := range in {
		if in[i] != nil {
			out = append(out, in[i])
		}
	}
	return out
}

func appendNotNil(ins []interface{}, in interface{}) []interface{} {
	if in != nil {
		ins = append(ins, in)
	}
	return delNils(ins)
}
