package serialization

import (
	"errors"
	"reflect"
)

// errFirstPtrChanged this error indicates that a double or single reference was passed to the Unmarshal function
// (example (**int)(**0) or (*int)(*0)) and Unmarshal overwritten first reference.
var errFirstPtrChanged = errors.New("unmarshal function rewrote first pointer")

// errSecondPtrNotChanged this error indicates that a double reference was passed to the Unmarshal function
// (example (**int)(**0)) and the function did not overwrite the second reference.
// Of course, it's not friendly to the garbage collector, overwriting references to values all the time,
// but this is the current implementation `gocql` and changing it can lead to unexpected results in some cases.
var errSecondPtrNotChanged = errors.New("unmarshal function did not rewrite second pointer")

func getPointers(i any) *pointer {
	rv := reflect.ValueOf(i)
	if rv.Kind() != reflect.Ptr {
		return nil
	}
	out := pointer{
		Fist: rv.Pointer(),
	}
	rt := rv.Type()
	if rt.Elem().Kind() == reflect.Ptr && !rv.Elem().IsNil() {
		out.Second = rv.Elem().Pointer()
	}
	return &out
}

type pointer struct {
	Fist   uintptr
	Second uintptr
}

func (p *pointer) NotNil() bool {
	return p != nil
}

// Valid validates if pointers has been manipulated by unmarshal functions in an expected manner:
// Fist pointer should not be overwritten,
// Second pointer, if applicable, should be overwritten.
func (p *pointer) Valid(v any) error {
	p2 := getPointers(v)
	if p.Fist != p2.Fist {
		return errFirstPtrChanged
	}
	if p.Second != 0 && p2.Second != 0 && p2.Second == p.Second {
		return errSecondPtrNotChanged
	}
	return nil
}
