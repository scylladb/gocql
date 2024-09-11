package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalTinyint(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTinyInt}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:  "[nil]str",
				Mods:  mods.Non,
				Data:  nil,
				Value: tests.NilRef(""),
			},
			{
				Name:  "[00]str",
				Mods:  mods.Ref,
				Data:  []byte("\x00"),
				Value: "0",
			},
			{
				Name:  "[7f]str",
				Mods:  mods.Ref,
				Data:  []byte("\x7f"),
				Value: "127",
			},
			{
				Name:  "[80]str",
				Mods:  mods.Ref,
				Data:  []byte("\x80"),
				Value: "-128",
			},
			{
				Name: "[nil]refs",
				Mods: mods.Custom,
				Data: nil,
				Values: tests.NilRefs(
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				),
			},
			{
				Name:  "unmarshal nil data",
				Funcs: funcs.ExcludeMarshal(),
				Mods:  mods.Custom,
				Data:  nil,
				Values: []interface{}{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
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

	cases.GetModified().Run(t)
}

func TestMarshalTinyintMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTinyInt}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailMarshal{
				{
					Name: "big_vals",
					MarshalIns: []interface{}{
						int16(128), int32(128), int64(128), int(128), "128",
						int16(-129), int32(-129), int64(-129), int(-129), "-129",
						uint16(256), uint32(256), uint64(256), uint(256),
					},
				},
				{
					Name: "corrupt_vals",
					MarshalIns: []interface{}{
						"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1",
					},
				},
			},
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
				{
					Name: "big data",
					Data: []byte("\x80\x00"),
					UnmarshalIns: []interface{}{
						int8(0), int16(0), int32(0), int64(0), int(0), "",
						uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					},
					Issue: "https://github.com/scylladb/gocql/issues/246",
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
