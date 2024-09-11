package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalsTuple(t *testing.T) {
	tType := TupleTypeInfo{
		NativeType: NativeType{typ: TypeTuple, proto: 4},
		Elems: []TypeInfo{
			NativeType{typ: TypeSmallInt, proto: 4},
		},
	}

	type (
		structS struct {
			V int16 `cql:"1"`
		}
		structR struct {
			V *int16 `cql:"1"`
		}
		structC struct {
			V mods.Int16 `cql:"1"`
		}
		structCR struct {
			V *mods.Int16 `cql:"1"`
		}
	)

	var (
		nilRInt16 *int16 = nil

		nilSlice    []int16       = nil
		nilSliceR   []*int16      = nil
		nilSliceC   []mods.Int16  = nil
		nilSliceCR  []*mods.Int16 = nil
		nilSliceAny []interface{} = nil
	)

	ref := func(v int16) *int16 { return &v }

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:  "unmarshal custom elems",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					structC{},
					structCR{},
					nilSliceC,
					nilSliceCR,
					[1]mods.Int16{},
					[1]*mods.Int16{},
				},
				IssueUnmarshal: "panic",
			},
			{
				Name: "[nil]refs",
				Mods: mods.Custom,
				Data: nil,
				Values: tests.NilRefs(
					structS{},
					structR{},
					nilSlice,
					nilSliceR,
					[1]int16{},
					[1]*int16{},
					nilSliceAny,
				),
			},
			{
				Name:  "unmarshal nil",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					structS{},
					structR{},
				},
			},
			{
				Name:  "unmarshal nil to slices",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilSlice,
					nilSliceR,
					[1]int16{},
					[1]*int16{},
					nilSliceAny,
				},
				IssueUnmarshal: "unmarshal nil data to slices with elem",
			},
			{
				Name: "unmarshal zero data",
				Data: make([]byte, 0),
				Values: []interface{}{
					structS{},
					structR{},
					make([]int16, 0),
					make([]*int16, 0),
					[1]int16{},
					[1]*int16{},
					make([]interface{}, 0),
				},
				IssueUnmarshal: "a lot of different problems",
				IssueMarshal:   "a lot of different problems",
			},
			{
				Name: "one nil elem",
				Data: []byte("\xff\xff\xff\xff"),
				Values: []interface{}{
					structR{},
					[]*int16{nil},
					[1]*int16{nil},
				},
			},
			{
				Name:  "unmarshal one nil elem",
				Funcs: funcs.ExcludeMarshal(),
				Data:  []byte("\xff\xff\xff\xff"),
				Values: []interface{}{
					structS{},
					[]int16{0},
					[1]int16{0},
					[]interface{}{int16(0)},
				},
			},
			{
				Name: "one nil elem to slice of any",
				Data: []byte("\xff\xff\xff\xff"),
				Values: []interface{}{
					[]interface{}{nilRInt16},
				},
				IssueMarshal:   "marshals nil values into full zeros data",
				IssueUnmarshal: "unmarshal function replace references by values",
			},
			{
				Name: "one zero elem",
				Data: []byte("\x00\x00\x00\x00"),
				Values: []interface{}{
					structS{},
					structR{V: ref(0)},
					[]int16{0},
					[]*int16{ref(0)},
					[1]int16{0},
					[1]*int16{ref(0)},
					[]interface{}{int16(0)},
				},
				IssueMarshal: "marshals zero values to full zeros data",
			},
			{
				Name: "one elem",
				Data: []byte("\x00\x00\x00\x02\x7f\xff"),
				Values: []interface{}{
					structS{V: 32767},
					structR{V: ref(32767)},
					[]int16{32767},
					[]*int16{ref(32767)},
					[1]int16{32767},
					[1]*int16{ref(32767)},
					[]interface{}{int16(32767)},
				},
			},
		},
	}

	cases.GetModified().Run(t)
}

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

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailMarshal{
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
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
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
		},
	}

	cases.GetModified().Run(t)
}
