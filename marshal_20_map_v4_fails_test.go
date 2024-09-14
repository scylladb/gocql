package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalMapV4MustFail(t *testing.T) {
	tType := CollectionType{
		NativeType: NativeType{typ: TypeMap, proto: 4},
		Key:        NativeType{typ: TypeSmallInt, proto: 4},
		Elem:       NativeType{typ: TypeSmallInt, proto: 4},
	}

	ref := func(v int16) *int16 { return &v }
	ref32 := func(v int32) *int32 { return &v }

	mCases := &marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
			{
				Name: "big_vals",
				MarshalIns: []interface{}{
					map[int32]int32{1: 2147483647},
					map[int32]int32{2147483647: 1},
					map[int32]*int32{1: ref32(2147483647)},
					map[int32]*int32{2147483647: ref32(1)},
					map[mod.Int32]mod.Int32{1: 2147483647},
					map[mod.Int32]mod.Int32{2147483647: 1},
					map[mod.Int32]*mod.Int32{1: (*mod.Int32)(ref32(2147483647))},
					map[mod.Int32]*mod.Int32{2147483647: (*mod.Int32)(ref32(1))},
				},
			},
		},
	}
	mCases.Gen().RunGroup(t)

	uCases := &unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name: "one pair+",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02\xff\xff\x00"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one zero pair+",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xff"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elems+1",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elems+",
				Data: []byte("\x00\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one pair <value><value>-",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02\xff"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair <value>-<value>",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair <value><len>-",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair -<value>",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair <key><value>-",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair <key>-<value>",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair <key><len>-",
				Data: []byte("\x00\x00\x00\x01\x00\x00"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "one pair-",
				Data: []byte("\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
			{
				Name: "pairs-",
				Data: []byte("\x00\x00"),
				UnmarshalIns: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
			},
		},
	}
	uCases.Gen().RunGroup(t)
}
