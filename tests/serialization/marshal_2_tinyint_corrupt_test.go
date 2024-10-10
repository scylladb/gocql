package serialization_test

import (
	"math"
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/tinyint"
)

func TestMarshalTinyintCorrupt(t *testing.T) {
	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeTinyInt, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.tinyint",
			marshal:   tinyint.Marshal,
			unmarshal: tinyint.Unmarshal,
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

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					int16(math.MaxInt8 + 1), int32(math.MaxInt8 + 1), int64(math.MaxInt8 + 1), int(math.MaxInt8 + 1),
					uint8(math.MaxInt8 + 1), uint16(math.MaxInt8 + 1), uint32(math.MaxInt8 + 1), uint64(math.MaxInt8 + 1), uint(math.MaxInt8 + 1),
					"128", *big.NewInt(math.MaxInt8 + 1),
					int16(math.MinInt8 - 1), int32(math.MinInt8 - 1), int64(math.MinInt8 - 1), int(math.MinInt8 - 1),
					"-129", *big.NewInt(math.MinInt8 - 1),
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{"1s2", "1s", "-1s", ".1", ",1", "0.1", "0,1"}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints-1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80"),
				Values: mod.Values{
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
				}.AddVariants(mod.All...),
			}.Run("neg_uints-128", t, unmarshal)
		})
	}
}
