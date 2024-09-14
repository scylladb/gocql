package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"testing"
	"time"
)

func TestMarshalsTimestamp(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTimestamp}

	cases := serialization.Group{
		Name: tType.Type().String(),
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
				Values: mod.NilRefs(int64(0), mod.Int16(0), time.Duration(0), time.Time{}),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mod.Custom,
				Funcs:  funcs.ExcludedMarshal(),
				Data:   nil,
				Values: []interface{}{int64(0), time.Duration(0), time.Time{}},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludedMarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{int64(0), time.Duration(0), time.Time{}},
			},
			{
				Name:   "zeros",
				Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{int64(0), time.Duration(0), time.UnixMilli(0).UTC()},
			},
			{
				Name: "9223372036854775807",
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: []interface{}{
					int64(9223372036854775807),
					time.Duration(9223372036854775807),
					time.UnixMilli(9223372036854775807).UTC(),
				},
			},
			{
				Name: "-9223372036854775808",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{
					int64(-9223372036854775808),
					time.Duration(-9223372036854775808),
					time.UnixMilli(-9223372036854775808).UTC(),
				},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
