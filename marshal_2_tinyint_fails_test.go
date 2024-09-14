package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalTinyintMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTinyInt}

	mCases := marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
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
	}
	mCases.Gen().RunGroup(t)

	uCases := unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
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
	}
	uCases.Gen().RunGroup(t)
}
