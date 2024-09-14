package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalUDT(t *testing.T) {
	tType := UDTTypeInfo{
		Name:       "udt",
		NativeType: NativeType{typ: TypeUDT, proto: 4},
		KeySpace:   "",
		Elements: []UDTField{
			{Name: "1", Type: NativeType{typ: TypeSmallInt, proto: 4}},
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
		nilMap   map[string]interface{} = nil
		nilInt16 *int16                 = nil
	)

	ref := func(v int16) *int16 { return &v }

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Ref,
		Sets: []*serialization.Set{
			{
				Mods:           mod.Non,
				Data:           []byte("\x00\x00\x00\x01\x7f\xff"),
				Value:          mod.MapUDT{"1": int16(32767)},
				IssueMarshal:   "not support custom maps",
				IssueUnmarshal: "not support custom maps",
			},
			{
				Name: "map ref one elem",
				Data: []byte("\x00\x00\x00\x02\x7f\xff"),
				Values: []interface{}{
					map[string]interface{}{"1": ref(32767)},
				},
				IssueUnmarshal: "unmarshal rewrite reference in interface to value",
			},
			{
				Name: "[nil]refs",
				Mods: mod.Non,
				Data: nil,
				Values: mod.NilRefs(
					structS{},
					structR{},
					structC{},
					structCR{},
					map[string]interface{}{},
				),
			},
			{
				Name:  "unmarshal nil",
				Mods:  mod.Non,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Values: []interface{}{
					structS{},
					structR{},
					structC{},
					structCR{},
					nilMap,
				},
			},
			{
				Name: "unmarshal zero data",
				Data: make([]byte, 0),
				Values: []interface{}{
					structS{},
					structR{},
					structC{},
					structCR{},
					map[string]interface{}{},
				},
				IssueMarshal: "marshals zero values into full zeros data",
			},
			{
				Name: "one nil elem",
				Data: []byte("\xff\xff\xff\xff"),
				Values: []interface{}{
					structS{},
					structR{},
					structC{},
					structCR{},
				},
				IssueMarshal: "marshals nil values into full zeros data",
			},
			{
				Name: "one nil elem to map",
				Data: []byte("\xff\xff\xff\xff"),
				Values: []interface{}{
					map[string]interface{}{"1": nilInt16},
				},
				IssueUnmarshal: "unmarshal nil elem data into zero elem",
			},
			{
				Name: "one zero elem",
				Data: []byte("\x00\x00\x00\x00"),
				Values: []interface{}{
					structS{},
					structR{V: ref(0)},
					structC{},
					structCR{V: (*mod.Int16)(ref(0))},
					map[string]interface{}{"1": int16(0)},
				},
				IssueMarshal: "marshals zero values to full zeros data",
			},
			{
				Name: "one elem",
				Data: []byte("\x00\x00\x00\x02\x7f\xff"),
				Values: []interface{}{
					structS{V: 32767},
					structR{V: ref(32767)},
					structC{V: 32767},
					structCR{V: (*mod.Int16)(ref(32767))},
					map[string]interface{}{"1": int16(32767)},
				},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
