package utils

import (
	"reflect"
)

func DeReference(in interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(in)).Interface()
}

func AppendNotNil(ins []interface{}, in interface{}) []interface{} {
	if in != nil {
		ins = append(ins, in)
	}
	return delNils(ins)
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
