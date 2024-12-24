//go:build all || unit
// +build all unit

package serialization_test

import (
	"github.com/gocql/gocql/serialization/boolean"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalBooleanCorrupt(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeBoolean, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.boolean",
			marshal:   boolean.Marshal,
			unmarshal: boolean.Unmarshal,
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
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00"),
				Values: mod.Values{
					false,
				}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)
		})
	}
}
