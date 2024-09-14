package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

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
			V mod.Int16 `cql:"1"`
		}
		structCR struct {
			V *mod.Int16 `cql:"1"`
		}
	)

	mCases := &marshal.Group{
		Name: tType.Type().String(),
		Func: func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		Sets: []*marshal.Set{
			{
				Name:      "big_vals",
				MarshalIn: structF{V: 2147483647},
				Mods:      mod.Ref,
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
	}

	uCases.Gen().RunGroup(t)
}
