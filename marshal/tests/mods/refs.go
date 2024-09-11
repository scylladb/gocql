package mods

import "reflect"

type intoRef struct{}

func (m intoRef) Suffix() string {
	return "/ref"
}

func (m intoRef) Apply(vals []interface{}) []interface{} {
	out := make([]interface{}, 0)
	for i := range vals {
		if vals[i] != nil {
			out = append(out, m.apply(vals[i]))
		}
	}
	return out
}

func (m intoRef) apply(val interface{}) interface{} {
	inV := reflect.ValueOf(val)
	out := reflect.New(reflect.TypeOf(val))
	out.Elem().Set(inV)
	return out.Interface()
}
