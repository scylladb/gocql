package funcs

import (
	"fmt"
	"reflect"
)

var (
	ExcludeMarshal   = func() *List { return &List{Marshal: excludedMarshal} }
	ExcludeUnmarshal = func() *List { return &List{Unmarshal: excludedUnmarshal} }

	excludedMarshal   = func(interface{}) ([]byte, error) { return nil, fmt.Errorf("run on excludedMarshal func") }
	excludedUnmarshal = func([]byte, interface{}) error { return fmt.Errorf("run on excludedUnmarshal func") }
)

// List is a set of functions that used in Serialization.
// Marshal, Unmarshal are should be provided.
type List struct {
	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	EqualData func(in1, in2 []byte) bool
	NewVar    func(interface{}) interface{}
	EqualVals func(in1, in2 interface{}) bool
}

func (f *List) Copy() *List {
	out := &List{
		EqualData: f.EqualData,
		NewVar:    f.NewVar,
		EqualVals: f.EqualVals,
	}
	if f.Marshal != nil {
		out.Marshal = f.Marshal
	}
	if f.Unmarshal != nil {
		out.Unmarshal = f.Unmarshal
	}
	return out
}

func (f *List) IsExcludedMarshal() bool {
	return reflect.ValueOf(f.Marshal).Pointer() == reflect.ValueOf(excludedMarshal).Pointer()
}

func (f *List) IsExcludedUnmarshal() bool {
	return reflect.ValueOf(f.Unmarshal).Pointer() == reflect.ValueOf(excludedUnmarshal).Pointer()
}

func (f *List) Valid() bool {
	if f.EqualData == nil {
		return false
	}
	if f.NewVar == nil {
		return false
	}
	if f.EqualVals == nil {
		return false
	}
	if f.Unmarshal == nil {
		return false
	}
	if f.Marshal == nil {
		return false
	}
	return true
}

func (f *List) PutDefaults(d *List) {
	if f.Marshal == nil {
		f.Marshal = d.Marshal
	}
	if f.Unmarshal == nil {
		f.Unmarshal = d.Unmarshal
	}
	if f.EqualData == nil {
		f.EqualData = d.EqualData
	}
	if f.NewVar == nil {
		f.NewVar = d.NewVar
	}
	if f.EqualVals == nil {
		f.EqualVals = d.EqualVals
	}
}
