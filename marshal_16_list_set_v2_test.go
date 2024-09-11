package gocql

import (
	"math"
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalSetListV2(t *testing.T) {
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

	var (
		nilSlice   []int16       = nil
		nilSliceR  []*int16      = nil
		nilSliceC  []mods.Int16  = nil
		nilSliceCR []*mods.Int16 = nil

		nilRSlice   *[]int16       = nil
		nilRSliceR  *[]*int16      = nil
		nilRSliceC  *[]mods.Int16  = nil
		nilRSliceCR *[]*mods.Int16 = nil
	)

	var (
		nilRArr   *[1]int16       = nil
		nilRArrR  *[1]*int16      = nil
		nilRArrC  *[1]mods.Int16  = nil
		nilRArrCR *[1]*mods.Int16 = nil
	)

	ref := func(v int16) *int16 { return &v }

	cases := tests.Serialization{
		Funcs:       funcs.Default(nil, nil),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name: "[nil]refs",
				Mods: mods.Custom,
				Data: nil,
				Values: tests.NilRefs(
					make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
				),
			},
			{
				Name:  "unmarshal nil to slice",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilSlice, nilSliceR, nilSliceC, nilSliceCR,
					nilRSliceR, nilRSlice, nilRSliceC, nilRSliceCR,
				},
			},
			{
				Name:  "unmarshal nil to arr",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
				},
				IssueUnmarshal: "error: unmarshal list: can not store nil in array value",
			},
			{
				Name:  "unmarshal nil to arr refs",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilRArr, nilRArrR, nilRArrC, nilRArrCR,
				},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
					[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
				},
				IssueUnmarshal: "error: unmarshal list: unexpected eof",
			},
			{
				Name: "zero elems to slice",
				Data: []byte("\x00\x00"),
				Values: []interface{}{
					make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
				},
			},
			{
				Name: "zero elems to arr",
				Data: []byte("\x00\x00"),
				Values: []interface{}{
					[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
				},
				IssueMarshal:   "marshals zero array to full data [000100010000]",
				IssueUnmarshal: "error: unmarshal list: array with wrong size",
			},
			{
				Name: "one zero elem",
				Data: []byte("\x00\x01\x00\x00"),
				Values: []interface{}{
					[]int16{0}, []*int16{ref(0)}, []mods.Int16{0}, []*mods.Int16{(*mods.Int16)(ref(0))},
					[1]int16{0}, [1]*int16{ref(0)}, [1]mods.Int16{0}, [1]*mods.Int16{(*mods.Int16)(ref(0))},
				},
				IssueMarshal: "marshals zero elem to full data [000100010000]",
			},
			{
				Name: "one elem",
				Data: []byte("\x00\x01\x00\x02\x7f\xff"),
				Values: []interface{}{
					[]int16{32767}, []*int16{ref(32767)}, []mods.Int16{32767}, []*mods.Int16{(*mods.Int16)(ref(32767))},
					[1]int16{32767}, [1]*int16{ref(32767)}, [1]mods.Int16{32767}, [1]*mods.Int16{(*mods.Int16)(ref(32767))},
				},
			},
		},
	}

	for i, tType := range tTypes {
		tCases := cases.Copy()
		tCases.Funcs.Marshal = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Funcs.Unmarshal = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases = tCases.GetModified()
		t.Run(names[i], func(t *testing.T) {
			tCases.Run(t)
		})
	}
}

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

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Cases: []*tests.MustFailMarshal{
				{
					Name:      "big_vals",
					Mods:      mods.Ref,
					MarshalIn: []int32{2147483647},
				},
				{
					Name:      "refiled_slice",
					Mods:      mods.Non,
					MarshalIn: fullSlice,
					Issue:     "return set(list) data with negative elems cont",
				},
			},
		},
		Unmarshal: &tests.UnmarshalCases{
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
				{
					Name: "one elem+",
					Data: []byte("\x00\x01\x00\x02\xff\xff\x01"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "one zero elem+",
					Data: []byte("\x00\x01\x00\x00\xff"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "zero elems+1",
					Data: []byte("\x00\x00\x00\x01"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "zero elems+",
					Data: []byte("\x00\x00\x01"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "one elem-v",
					Data: []byte("\x00\x01\x00\x02\xff"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "one elem-l",
					Data: []byte("\x00\x01\x00\x02"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
				},
				{
					Name: "one elem-",
					Data: []byte("\x00\x01\x00"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
				},
				{
					Name: "one elem--",
					Data: []byte("\x00\x01"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
				},
				{
					Name: "elems-",
					Data: []byte("\x00"),
					UnmarshalIns: []interface{}{
						make([]int16, 0), make([]*int16, 0), make([]mods.Int16, 0), make([]*mods.Int16, 0),
						[1]int16{}, [1]*int16{}, [1]mods.Int16{}, [1]*mods.Int16{},
					},
				},
			},
		},
	}

	for i, tType := range tTypes {
		tCases := cases.Copy()
		tCases.Marshal.Func = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Unmarshal.Func = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases = tCases.GetModified()
		t.Run(names[i], func(t *testing.T) {
			tCases.Run(t)
		})
	}
}
