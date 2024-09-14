package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalTupleMustFail(t *testing.T) {
	tType := TupleTypeInfo{
		NativeType: NativeType{typ: TypeTuple, proto: 4},
		Elems: []TypeInfo{
			NativeType{typ: TypeSmallInt, proto: 4},
		},
	}

	type (
		structF struct {
			V int32 `cql:"1"`
		}
		structS struct {
			V int16 `cql:"1"`
		}
		structR struct {
			V *int16 `cql:"1"`
		}
	)

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
					structF{V: 2147483647},
					[]int32{2147483647},
					[]*int32{ref32(2147483647)},
					[1]int32{2147483647},
					[1]*int32{ref32(2147483647)},
					[]interface{}{int32(2147483647)},
				},
			},
			{
				Name: "big_count",
				MarshalIns: []interface{}{
					[]int16{32767, 32767},
					[]*int16{ref(32767), ref(32767)},
					[2]int16{32767, 32767},
					[2]*int16{ref(32767), ref(32767)},
					[]interface{}{int16(32767), int16(32767)},
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
				Name: "one elem <value>+",
				Data: []byte("\x00\x00\x00\x02\x7f\xff\xff"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elem <len>++",
				Data: []byte("\x00\x00\x00\x00\x7f\xff"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "the error is not returned",
			},
			{
				Name: "zero elem <len>+",
				Data: []byte("\x00\x00\x00\x00\x01"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one elem <value>-",
				Data: []byte("\x00\x00\x00\x02\x7f"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "panic",
			},
			{
				Name: "one elem <value>--",
				Data: []byte("\x00\x00\x00\x02"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "panic",
			},
			{
				Name: "one elem <len>-",
				Data: []byte("\x00\x00\x00"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "the error is not returned",
			},
			{
				Name: "one elem <len>--",
				Data: []byte("\x00"),
				UnmarshalIns: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				Issue: "the error is not returned",
			},
		},
	}
	uCases.Gen().RunGroup(t)
}
