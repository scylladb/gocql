//go:build unit
// +build unit

package serialization_test

import (
	"math/big"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/tinyint"
)

func TestMarshalTinyintCorrupt(t *testing.T) {
	t.Parallel()

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
			t.Parallel()

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					int16(128), int32(128), int64(128), int(128),
					"128", *big.NewInt(128),
					int16(-129), int32(-129), int64(-129), int(-129),
					"-129", *big.NewInt(-129),
					uint16(256), uint32(256), uint64(256), uint(256),
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
		})
	}
}
