package mod

import "reflect"

func NilRefs(vals ...interface{}) []interface{} {
	out := make([]interface{}, len(vals))
	for i := range vals {
		if vals[i] != nil {
			out[i] = NilRef(vals[i])
		}
	}
	return out
}

func NilRef(i interface{}) interface{} {
	if i == nil {
		return nil
	}
	rt := reflect.TypeOf(i)
	if rt.Kind() == reflect.Ptr {
		panic("ReplaceValsNilRefs option do not support references")
	}
	return reflect.NewAt(reflect.TypeOf(i), nil).Interface()
}
