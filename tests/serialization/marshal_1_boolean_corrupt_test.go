//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/serialization/boolean"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
)

func TestMarshalBooleanCorrupt(t *testing.T) {
	t.Parallel()

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
			t.Parallel()

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00"),
				Values: mod.Values{
					false,
				}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)
		})
	}
}
