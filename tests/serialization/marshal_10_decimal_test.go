//go:build all || unit
// +build all unit

package serialization_test

import (
	"fmt"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/decimal"
)

func TestMarshalDecimal(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeDecimal, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.decimal",
			marshal:   decimal.Marshal,
			unmarshal: decimal.Unmarshal,
		},
		{
			name: "glob",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(tType, bytes, i)
			},
		},
	}

	getValues := func(scale inf.Scale, unscaled ...int64) mod.Values {
		out := make(mod.Values, 2)
		switch len(unscaled) {
		case 0:
			panic("unscaled should be")
		case 1:
			out[0] = *inf.NewDec(unscaled[0], scale)
			out[1] = fmt.Sprintf("%d;%d", scale, unscaled[0])
		default:
			bg := new(big.Int)
			for _, u := range unscaled {
				bg = bg.Add(bg, big.NewInt(u))
			}
			out[0] = *inf.NewDecBig(bg, scale)
			out[1] = fmt.Sprintf("%d;%s", scale, bg.String())
		}
		return out
	}

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{(*inf.Dec)(nil), ""}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{*inf.NewDec(0, 0), ""}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   make([]byte, 0),
				Values: getValues(0, 0).AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x00\x00"),
				Values: getValues(0, 0).AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x00\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: getValues(0, math.MaxInt64).AddVariants(mod.All...),
			}.Run("scale0_maxInt64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x01\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: getValues(1, math.MaxInt64).AddVariants(mod.All...),
			}.Run("scale+1_maxInt64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff\xff\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: getValues(-1, math.MaxInt64).AddVariants(mod.All...),
			}.Run("scale-1_maxInt64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\xff\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: getValues(math.MaxInt32, math.MaxInt64).AddVariants(mod.All...),
			}.Run("maxInt32_maxInt64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x80\x00\x00\x00\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: getValues(math.MinInt32, math.MaxInt64).AddVariants(mod.All...),
			}.Run("minInt32_maxInt64", t, marshal, unmarshal)

			scale := inf.Scale(math.MaxInt16)
			t.Run("scaleMaxInt16", func(t *testing.T) {
				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01"),
					Values: getValues(scale, 1).AddVariants(mod.All...),
				}.Run("+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff"),
					Values: getValues(scale, -1).AddVariants(mod.All...),
				}.Run("-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f"),
					Values: getValues(scale, 127).AddVariants(mod.All...),
				}.Run("maxInt8", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80"),
					Values: getValues(scale, -128).AddVariants(mod.All...),
				}.Run("minInt8", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80"),
					Values: getValues(scale, 128).AddVariants(mod.All...),
				}.Run("maxInt8+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f"),
					Values: getValues(scale, -129).AddVariants(mod.All...),
				}.Run("minInt8-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff"),
					Values: getValues(scale, 32767).AddVariants(mod.All...),
				}.Run("maxInt16", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00"),
					Values: getValues(scale, -32768).AddVariants(mod.All...),
				}.Run("minInt16", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00"),
					Values: getValues(scale, 32768).AddVariants(mod.All...),
				}.Run("maxInt16+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff"),
					Values: getValues(scale, -32769).AddVariants(mod.All...),
				}.Run("minInt16-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff\xff"),
					Values: getValues(scale, 8388607).AddVariants(mod.All...),
				}.Run("maxInt24", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00\x00"),
					Values: getValues(scale, -8388608).AddVariants(mod.All...),
				}.Run("minInt24", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00\x00"),
					Values: getValues(scale, 8388608).AddVariants(mod.All...),
				}.Run("maxInt24+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff\xff"),
					Values: getValues(scale, -8388609).AddVariants(mod.All...),
				}.Run("minInt24-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff\xff\xff"),
					Values: getValues(scale, 2147483647).AddVariants(mod.All...),
				}.Run("maxInt32", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00\x00\x00"),
					Values: getValues(scale, -2147483648).AddVariants(mod.All...),
				}.Run("minInt32", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00\x00\x00"),
					Values: getValues(scale, 2147483648).AddVariants(mod.All...),
				}.Run("maxInt32+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff\xff\xff"),
					Values: getValues(scale, -2147483649).AddVariants(mod.All...),
				}.Run("minInt32-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff\xff\xff\xff"),
					Values: getValues(scale, 549755813887).AddVariants(mod.All...),
				}.Run("maxInt40", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00\x00\x00\x00"),
					Values: getValues(scale, -549755813888).AddVariants(mod.All...),
				}.Run("minInt40", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00\x00\x00\x00"),
					Values: getValues(scale, 549755813888).AddVariants(mod.All...),
				}.Run("maxInt40+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff\xff\xff\xff"),
					Values: getValues(scale, -549755813889).AddVariants(mod.All...),
				}.Run("minInt40-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 140737488355327).AddVariants(mod.All...),
				}.Run("maxInt48", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00\x00\x00\x00\x00"),
					Values: getValues(scale, -140737488355328).AddVariants(mod.All...),
				}.Run("minInt48", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 140737488355328).AddVariants(mod.All...),
				}.Run("maxInt48+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff\xff\xff\xff\xff"),
					Values: getValues(scale, -140737488355329).AddVariants(mod.All...),
				}.Run("minInt48-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 36028797018963967).AddVariants(mod.All...),
				}.Run("maxInt56", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, -36028797018963968).AddVariants(mod.All...),
				}.Run("minInt56", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 36028797018963968).AddVariants(mod.All...),
				}.Run("maxInt56+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, -36028797018963969).AddVariants(mod.All...),
				}.Run("minInt56-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 9223372036854775807).AddVariants(mod.All...),
				}.Run("maxInt64", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x80\x00\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, -9223372036854775808).AddVariants(mod.All...),
				}.Run("minInt64", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\x80\x00\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 9223372036854775807, 1).AddVariants(mod.All...),
				}.Run("maxInt64+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, -9223372036854775808, -1).AddVariants(mod.All...),
				}.Run("minInt64-1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff"),
					Values: getValues(scale, 255).AddVariants(mod.All...),
				}.Run("maxUint8", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00"),
					Values: getValues(scale, 256).AddVariants(mod.All...),
				}.Run("maxUint8+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff"),
					Values: getValues(scale, 65535).AddVariants(mod.All...),
				}.Run("maxUint16", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00"),
					Values: getValues(scale, 65536).AddVariants(mod.All...),
				}.Run("maxUint16+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff\xff"),
					Values: getValues(scale, 16777215).AddVariants(mod.All...),
				}.Run("maxUint24", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00\x00"),
					Values: getValues(scale, 16777216).AddVariants(mod.All...),
				}.Run("maxUint24+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff\xff\xff"),
					Values: getValues(scale, 4294967295).AddVariants(mod.All...),
				}.Run("maxUint32", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00\x00\x00"),
					Values: getValues(scale, 4294967296).AddVariants(mod.All...),
				}.Run("maxUint32+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 1099511627775).AddVariants(mod.All...),
				}.Run("maxUint40", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 1099511627776).AddVariants(mod.All...),
				}.Run("maxUint40+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 281474976710655).AddVariants(mod.All...),
				}.Run("maxUint48", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 281474976710656).AddVariants(mod.All...),
				}.Run("maxUint48+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 72057594037927935).AddVariants(mod.All...),
				}.Run("maxUint56", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 72057594037927936).AddVariants(mod.All...),
				}.Run("maxUint56+1", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x00\xff\xff\xff\xff\xff\xff\xff\xff"),
					Values: getValues(scale, 9223372036854775807, 9223372036854775807, 1).AddVariants(mod.All...),
				}.Run("maxUint64", t, marshal, unmarshal)

				serialization.PositiveSet{
					Data:   []byte("\x00\x00\x7f\xff\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
					Values: getValues(scale, 9223372036854775807, 9223372036854775807, 2).AddVariants(mod.All...),
				}.Run("maxUint64+1", t, marshal, unmarshal)
			})
		})
	}
}
