package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalTinyint(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTinyInt}

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:  "[nil]str",
				Mods:  mod.Non,
				Data:  nil,
				Value: mod.NilRef(""),
			},
			{
				Name:  "[00]str",
				Mods:  mod.Ref,
				Data:  []byte("\x00"),
				Value: "0",
			},
			{
				Name:  "[7f]str",
				Mods:  mod.Ref,
				Data:  []byte("\x7f"),
				Value: "127",
			},
			{
				Name:  "[80]str",
				Mods:  mod.Ref,
				Data:  []byte("\x80"),
				Value: "-128",
			},
			{
				Name: "[nil]refs",
				Mods: mod.Custom,
				Data: nil,
				Values: mod.NilRefs(
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				),
			},
			{
				Name:  "unmarshal nil data",
				Funcs: funcs.ExcludedMarshal(),
				Mods:  mod.Custom,
				Data:  nil,
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00"),
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},

			{
				Data: []byte("\x7f"),
				Values: []interface{}{
					int8(127), int16(127), int32(127), int64(127), int(127),
				},
			},

			{
				Data: []byte("\x80"),
				Values: []interface{}{
					int8(-128), int16(-128), int32(-128), int64(-128), int(-128),
				},
			},
			{
				Data: []byte("\xff"),
				Values: []interface{}{
					uint8(255), uint16(255), uint32(255), uint64(255), uint(255),
				},
			},
		},
	}
	cases.Gen().RunGroup(t)
}
