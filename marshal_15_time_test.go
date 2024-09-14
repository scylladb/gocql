package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"testing"
	"time"
)

func TestMarshalsTime(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTime}

	cases := serialization.Group{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:   "[nil]refs",
				Mods:   mod.Non,
				Data:   nil,
				Values: mod.NilRefs(int64(0), mod.Int16(0), time.Duration(0)),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mod.Custom,
				Funcs:  funcs.ExcludedMarshal(),
				Data:   nil,
				Values: []interface{}{int64(0), time.Duration(0)},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludedMarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{int64(0), time.Duration(0)},
			},
			{
				Name:   "zeros",
				Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{int64(0), time.Duration(0)},
			},
			{
				Name:   "86399999999999",
				Data:   []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff"),
				Values: []interface{}{int64(86399999999999), time.Duration(86399999999999)},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
