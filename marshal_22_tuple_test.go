package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
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
			V mod.Int16 `cql:"1"`
		}
		structCR struct {
			V *mod.Int16 `cql:"1"`
		}
	)

	var (
		nilRInt16 *int16 = nil

		nilSlice    []int16       = nil
		nilSliceR   []*int16      = nil
		nilSliceC   []mod.Int16   = nil
		nilSliceCR  []*mod.Int16  = nil
		nilSliceAny []interface{} = nil
	)

	ref := func(v int16) *int16 { return &v }

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:  "unmarshal custom elems",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					structC{},
					structCR{},
					nilSliceC,
					nilSliceCR,
					[1]mod.Int16{},
					[1]*mod.Int16{},
				},
				IssueUnmarshal: "panic",
			},
			{
				Name: "[nil]refs",
				Mods: mod.Custom,
				Data: nil,
				Values: mod.NilRefs(
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
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					structS{},
					structR{},
				},
			},
			{
				Name:  "unmarshal nil to slices",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
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
				Funcs: funcs.ExcludedMarshal(),
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

	cases.Gen().RunGroup(t)
}
