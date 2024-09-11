package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalMapV4(t *testing.T) {
	tType := CollectionType{
		NativeType: NativeType{typ: TypeMap, proto: 4},
		Key:        NativeType{typ: TypeSmallInt, proto: 4},
		Elem:       NativeType{typ: TypeSmallInt, proto: 4},
	}

	var (
		nilMap   map[int16]int16            = nil
		nilMapR  map[int16]*int16           = nil
		nilMapC  map[mods.Int16]mods.Int16  = nil
		nilMapCR map[mods.Int16]*mods.Int16 = nil

		zeroMap   = make(map[int16]int16)
		zeroMapR  = make(map[int16]*int16)
		zeroMapC  = make(map[mods.Int16]mods.Int16)
		zeroMapCR = make(map[mods.Int16]*mods.Int16)
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
				Name: "[nil]refs",
				Mods: mods.Custom,
				Data: nil,
				Values: tests.NilRefs(
					nilMap, nilMapR, nilMapC, nilMapCR,
				),
			},
			{
				Name:  "unmarshal nil",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilMap, nilMapR, nilMapC, nilMapCR,
				},
			},
			{
				Name:  "zero",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					zeroMap, zeroMapR, zeroMapC, zeroMapCR,
				},
				IssueUnmarshal: "error: unmarshal list: unexpected eof",
			},
			{
				Name: "zero elems",
				Data: []byte("\x00\x00\x00\x00"),
				Values: []interface{}{
					zeroMap, zeroMapR, zeroMapC, zeroMapCR,
				},
			},
			{
				Name: "one zero pair",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mods.Int16]mods.Int16{0: 0},
					map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
				},
				IssueMarshal: "marshals zero pair to full of zeros data",
			},
			{
				Name: "one elem",
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x7f\xff\x00\x00\x00\x02\x7f\xff"),
				Values: []interface{}{
					map[int16]int16{32767: 32767},
					map[int16]*int16{32767: ref(32767)},
					map[mods.Int16]mods.Int16{32767: 32767},
					map[mods.Int16]*mods.Int16{32767: (*mods.Int16)(ref(32767))},
				},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalMapV4MustFail(t *testing.T) {
	tType := CollectionType{
		NativeType: NativeType{typ: TypeMap, proto: 4},
		Key:        NativeType{typ: TypeSmallInt, proto: 4},
		Elem:       NativeType{typ: TypeSmallInt, proto: 4},
	}

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
						map[int32]int32{1: 2147483647},
						map[int32]int32{2147483647: 1},
						map[int32]*int32{1: ref32(2147483647)},
						map[int32]*int32{2147483647: ref32(1)},
						map[mods.Int32]mods.Int32{1: 2147483647},
						map[mods.Int32]mods.Int32{2147483647: 1},
						map[mods.Int32]*mods.Int32{1: (*mods.Int32)(ref32(2147483647))},
						map[mods.Int32]*mods.Int32{2147483647: (*mods.Int32)(ref32(1))},
					},
				},
			},
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
				{
					Name: "one pair+",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02\xff\xff\x00"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "one zero pair+",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xff"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "zero elems+1",
					Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "zero elems+",
					Data: []byte("\x00\x00\x00\x00\x01"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "one pair <value><value>-",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02\xff"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair <value>-<value>",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair <value><len>-",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair -<value>",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair <key><value>-",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair <key>-<value>",
					Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair <key><len>-",
					Data: []byte("\x00\x00\x00\x01\x00\x00"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "one pair-",
					Data: []byte("\x00\x00\x00\x01"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
				{
					Name: "pairs-",
					Data: []byte("\x00\x00"),
					UnmarshalIns: []interface{}{
						map[int16]int16{0: 0},
						map[int16]*int16{0: ref(0)},
						map[mods.Int16]mods.Int16{0: 0},
						map[mods.Int16]*mods.Int16{0: (*mods.Int16)(ref(0))},
					},
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
