package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalSetListV4(t *testing.T) {
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

	var (
		nilSlice   []int16      = nil
		nilSliceR  []*int16     = nil
		nilSliceC  []mod.Int16  = nil
		nilSliceCR []*mod.Int16 = nil

		nilRSlice   *[]int16      = nil
		nilRSliceR  *[]*int16     = nil
		nilRSliceC  *[]mod.Int16  = nil
		nilRSliceCR *[]*mod.Int16 = nil
	)

	var (
		nilRArr   *[1]int16      = nil
		nilRArrR  *[1]*int16     = nil
		nilRArrC  *[1]mod.Int16  = nil
		nilRArrCR *[1]*mod.Int16 = nil
	)

	ref := func(v int16) *int16 { return &v }

	cases := serialization.Group{
		Funcs:       funcs.Default(nil, nil),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name: "[nil]refs",
				Mods: mod.Custom,
				Data: nil,
				Values: mod.NilRefs(
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				),
			},
			{
				Name:  "unmarshal nil to slice",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilSlice, nilSliceR, nilSliceC, nilSliceCR,
					nilRSliceR, nilRSlice, nilRSliceC, nilRSliceCR,
				},
			},
			{
				Name:  "unmarshal nil to arr",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				IssueUnmarshal: "error: unmarshal list: can not store nil in array value",
			},
			{
				Name:  "unmarshal nil to arr refs",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilRArr, nilRArrR, nilRArrC, nilRArrCR,
				},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				IssueUnmarshal: "error: unmarshal list: unexpected eof",
			},
			{
				Name: "zero elems to slice",
				Data: []byte("\x00\x00\x00\x00"),
				Values: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mod.Int16, 0), make([]*mod.Int16, 0),
				},
			},
			{
				Name: "zero elems to arr",
				Data: []byte("\x00\x00\x00\x00"),
				Values: []interface{}{
					[1]int16{}, [1]*int16{}, [1]mod.Int16{}, [1]*mod.Int16{},
				},
				IssueMarshal:   "marshals zero array to full of zeros data",
				IssueUnmarshal: "error: unmarshal list: array with wrong size",
			},
			{
				Name: "one zero elem",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00"),
				Values: []interface{}{
					[]int16{0}, []*int16{ref(0)}, []mod.Int16{0}, []*mod.Int16{(*mod.Int16)(ref(0))},
					[1]int16{0}, [1]*int16{ref(0)}, [1]mod.Int16{0}, [1]*mod.Int16{(*mod.Int16)(ref(0))},
				},
				IssueMarshal: "marshals zero elem to full of zeros data",
			},
			{
				Name: "one elem",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x7f\xff"),
				Values: []interface{}{
					[]int16{32767}, []*int16{ref(32767)}, []mod.Int16{32767}, []*mod.Int16{(*mod.Int16)(ref(32767))},
					[1]int16{32767}, [1]*int16{ref(32767)}, [1]mod.Int16{32767}, [1]*mod.Int16{(*mod.Int16)(ref(32767))},
				},
			},
		},
	}

	for i, tType := range tTypes {
		tCases := cases.Copy()
		tCases.Name = names[i]
		tCases.Funcs.Marshal = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Funcs.Unmarshal = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases.Gen().RunGroup(t)
	}
}
