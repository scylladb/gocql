package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
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
			V mods.Int16 `cql:"1"`
		}
		structCR struct {
			V *mods.Int16 `cql:"1"`
		}
	)

	var (
		nilMap   map[string]interface{} = nil
		nilInt16 *int16                 = nil
	)

	ref := func(v int16) *int16 { return &v }

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Ref,
		Cases: []*tests.Case{
			{
				Mods:           mods.Non,
				Data:           []byte("\x00\x00\x00\x01\x7f\xff"),
				Value:          mods.MapUDT{"1": int16(32767)},
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
				Mods: mods.Non,
				Data: nil,
				Values: tests.NilRefs(
					structS{},
					structR{},
					structC{},
					structCR{},
					map[string]interface{}{},
				),
			},
			{
				Name:  "unmarshal nil",
				Mods:  mods.Non,
				Funcs: funcs.ExcludeMarshal(),
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
					structCR{V: (*mods.Int16)(ref(0))},
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
					structCR{V: (*mods.Int16)(ref(32767))},
					map[string]interface{}{"1": int16(32767)},
				},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalUDTMustFail(t *testing.T) {
	tType := UDTTypeInfo{
		Name:       "udt",
		NativeType: NativeType{typ: TypeUDT, proto: 4},
		KeySpace:   "",
		Elements: []UDTField{
			{Name: "1", Type: NativeType{typ: TypeSmallInt, proto: 4}},
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
		structC struct {
			V mods.Int16 `cql:"1"`
		}
		structCR struct {
			V *mods.Int16 `cql:"1"`
		}
	)

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func: func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			Cases: []*tests.MustFailMarshal{
				{
					Name:      "big_vals",
					MarshalIn: structF{V: 2147483647},
					Mods:      mods.Ref,
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
						structC{},
						structCR{},
						map[string]interface{}{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "zero elem <len>++",
					Data: []byte("\x00\x00\x00\x00\x7f\xff"),
					UnmarshalIns: []interface{}{
						structS{},
						structR{},
						structC{},
						structCR{},
						map[string]interface{}{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "zero elem <len>+",
					Data: []byte("\x00\x00\x00\x00\x01"),
					UnmarshalIns: []interface{}{
						structS{},
						structR{},
						structC{},
						structCR{},
						map[string]interface{}{},
					},
					Issue: "the error is not returned",
				},
				{
					Name: "one elem <value>-",
					Data: []byte("\x00\x00\x00\x02\x7f"),
					UnmarshalIns: []interface{}{
						structS{},
						structR{},
						structC{},
						structCR{},
						map[string]interface{}{},
					},
					Issue: "panic",
				},
				{
					Name: "one elem <value>--",
					Data: []byte("\x00\x00\x00\x02"),
					UnmarshalIns: []interface{}{
						structS{},
						structR{},
						structC{},
						structCR{},
						map[string]interface{}{},
					},
					Issue: "panic",
				},
				{
					Name: "one elem <len>-",
					Data: []byte("\x00\x00\x00"),
					UnmarshalIns: []interface{}{
						structS{},
						structR{},
						structC{},
						structCR{},
						map[string]interface{}{},
					},
				},
				{
					Name: "one elem <len>--",
					Data: []byte("\x00"),
					UnmarshalIns: []interface{}{
						structS{},
						structR{},
						structC{},
						structCR{},
						map[string]interface{}{},
					},
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
