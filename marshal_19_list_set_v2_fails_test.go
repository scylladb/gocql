package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"math"
	"testing"
)

func TestMarshalSetListV2MustFail(t *testing.T) {
	tTypes := []TypeInfo{
		CollectionType{
			NativeType: NativeType{typ: TypeList, proto: 2},
			Elem:       NativeType{typ: TypeSmallInt, proto: 2},
		},
		CollectionType{
			NativeType: NativeType{typ: TypeSet, proto: 2},
			Elem:       NativeType{typ: TypeSmallInt, proto: 2},
		},
	}

	names := []string{"list<smallint>", "set<smallint>"}

	fullSlice := make([]int16, math.MaxInt16+1)
	for i := range fullSlice {
		fullSlice[i] = 1
	}

	mCases := &marshal.Group{
		Func: func(i interface{}) ([]byte, error) { return Marshal(nil, i) },
		Sets: []*marshal.Set{
			{
				Name:      "big_vals",
				Mods:      mod.Ref,
				MarshalIn: []int32{2147483647},
			},
			{
				Name:      "refiled_slice",
				Mods:      mod.Non,
				MarshalIn: fullSlice,
				Issue:     "return set(list) data with negative elems cont",
			},
		},
	}
	for i, tType := range tTypes {
		mCases.Func = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		cp := mCases.Copy()

		cp.Name = names[i]
		cp.Gen().RunGroup(t)
	}

	uCases := &unmarshal.Group{
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tTypes[0], bytes, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name: "one elem+",
				Data: []byte("\x00\x01\x00\x02\xff\xff\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one zero elem+",
				Data: []byte("\x00\x01\x00\x00\xff"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elems+1",
				Data: []byte("\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elems+",
				Data: []byte("\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one elem-v",
				Data: []byte("\x00\x01\x00\x02\xff"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one elem-l",
				Data: []byte("\x00\x01\x00\x02"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
			{
				Name: "one elem-",
				Data: []byte("\x00\x01\x00"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
			{
				Name: "one elem--",
				Data: []byte("\x00\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
			{
				Name: "elems-",
				Data: []byte("\x00"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
		},
	}

	for i, tType := range tTypes {
		uCases.Func = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }
		cp := uCases.Copy()
		cp.Name = names[i]

		cp.Gen().RunGroup(t)
	}
}
