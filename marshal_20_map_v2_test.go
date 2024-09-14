package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalMapV2(t *testing.T) {
	tType := CollectionType{
		NativeType: NativeType{typ: TypeMap, proto: 2},
		Key:        NativeType{typ: TypeSmallInt, proto: 2},
		Elem:       NativeType{typ: TypeSmallInt, proto: 2},
	}

	var (
		nilMap   map[int16]int16          = nil
		nilMapR  map[int16]*int16         = nil
		nilMapC  map[mod.Int16]mod.Int16  = nil
		nilMapCR map[mod.Int16]*mod.Int16 = nil

		zeroMap   = make(map[int16]int16)
		zeroMapR  = make(map[int16]*int16)
		zeroMapC  = make(map[mod.Int16]mod.Int16)
		zeroMapCR = make(map[mod.Int16]*mod.Int16)
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
				Name: "[nil]refs",
				Mods: mod.Custom,
				Data: nil,
				Values: mod.NilRefs(
					nilMap, nilMapR, nilMapC, nilMapCR,
				),
			},
			{
				Name:  "unmarshal nil",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					nilMap, nilMapR, nilMapC, nilMapCR,
				},
			},
			{
				Name:  "zero",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					zeroMap, zeroMapR, zeroMapC, zeroMapCR,
				},
				IssueUnmarshal: "error: unmarshal list: unexpected eof",
			},
			{
				Name: "zero elems",
				Data: []byte("\x00\x00"),
				Values: []interface{}{
					zeroMap, zeroMapR, zeroMapC, zeroMapCR,
				},
			},
			{
				Name: "one zero pair",
				Data: []byte("\x00\x01\x00\x00\x00\x00"),
				Values: []interface{}{
					map[int16]int16{0: 0},
					map[int16]*int16{0: ref(0)},
					map[mod.Int16]mod.Int16{0: 0},
					map[mod.Int16]*mod.Int16{0: (*mod.Int16)(ref(0))},
				},
				IssueMarshal: "marshals zero pair to full of zeros data",
			},
			{
				Name: "one elem",
				Data: []byte("\x00\x01\x00\x02\x7f\xff\x00\x02\x7f\xff"),
				Values: []interface{}{
					map[int16]int16{32767: 32767},
					map[int16]*int16{32767: ref(32767)},
					map[mod.Int16]mod.Int16{32767: 32767},
					map[mod.Int16]*mod.Int16{32767: (*mod.Int16)(ref(32767))},
				},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
