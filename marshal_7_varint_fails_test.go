package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"math/big"
	"testing"
)

func TestMarshalVarIntMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeVarint}

	mCases := marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
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
				Name:        "big_data",
				Data:        []byte("\xff\x00\xff"),
				UnmarshalIn: &big.Int{},
				Issue:       "the error is not returned",
			},
		},
	}

	uCases.Gen().RunGroup(t)
}
