package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalSetListV4MustFail(t *testing.T) {
	tTypes := []TypeInfo{
		CollectionType{
			NativeType: NativeType{typ: TypeList, proto: 4},
			Elem:       NativeType{typ: TypeSmallInt, proto: 4},
		},
		CollectionType{
			NativeType: NativeType{typ: TypeSet, proto: 4},
			Elem:       NativeType{typ: TypeSmallInt, proto: 4},
		},
	}

	names := []string{"list<smallint>", "set<smallint>"}

	mCases := &marshal.Group{
		DefaultMods: mod.Ref,
		Sets: []*marshal.Set{
			{
				Name:      "big_vals",
				MarshalIn: []int32{2147483647},
			},
		},
	}
	for i, tType := range tTypes {
		cp := mCases.Copy()
		cp.Name = names[i]
		cp.Func = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }

		cp.Gen().RunGroup(t)
	}

	uCases := &unmarshal.Group{
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name: "one elem+",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one zero elem+",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\xff"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elems+1",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elems+",
				Data: []byte("\x00\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one elem-v",
				Data: []byte("\x00\x00\x00\x00\x01\x00\x00\x00\x01\xff"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one elem-l",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
			{
				Name: "one elem-",
				Data: []byte("\x00\x00\x00\x01\x00\x00"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
			{
				Name: "one elem--",
				Data: []byte("\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
			{
				Name: "elems-",
				Data: []byte("\x00\x00"),
				UnmarshalIns: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
			},
		},
	}

	for i, tType := range tTypes {
		cp := uCases.Copy()
		cp.Name = names[i]
		cp.Func = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		cp.Gen().RunGroup(t)
	}
}
