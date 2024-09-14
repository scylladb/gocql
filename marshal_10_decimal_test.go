package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"testing"
)

func TestMarshalDecimal(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDecimal}

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Ref,
		Sets: []*serialization.Set{
			{
				Name:  "[nil]refs",
				Mods:  mod.Non,
				Data:  nil,
				Value: mod.NilRef(inf.Dec{}),
			},
			{
				Name:           "unmarshal nil data",
				Mods:           mod.Non,
				Funcs:          funcs.ExcludedMarshal(),
				Data:           nil,
				Value:          inf.Dec{},
				IssueUnmarshal: "error inf.Dec needs at least 4 bytes, while value has only 0",
			},
			{
				Name:           "unmarshal zero data",
				Funcs:          funcs.ExcludedMarshal(),
				Data:           make([]byte, 0),
				Value:          inf.Dec{},
				IssueUnmarshal: "error inf.Dec needs at least 4 bytes, while value has only 0",
			},
			{
				Name:  "zeros",
				Data:  []byte("\x00\x00\x00\x00\x00"),
				Value: inf.Dec{},
			},
			{
				Data:  []byte("\x7f\xff\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Value: *inf.NewDec(int64(math.MaxInt64), inf.Scale(int32(math.MaxInt32))),
			},
			{
				Data:  []byte("\x80\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00"),
				Value: *inf.NewDec(int64(math.MinInt64), inf.Scale(int32(math.MinInt32))),
			},
			{
				Data:  []byte("\x00\x00\x00\x00\x00"),
				Value: *inf.NewDec(int64(0), inf.Scale(0)),
			},
			{
				Data:  []byte("\x7f\xff\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Value: *inf.NewDecBig(big.NewInt(int64(math.MaxInt64)), inf.Scale(int32(math.MaxInt32))),
			},
		},
	}
	cases.Gen().RunGroup(t)
}
