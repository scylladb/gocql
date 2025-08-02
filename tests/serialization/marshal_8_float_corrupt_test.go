//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/float"
)

func TestMarshalFloatCorrupt(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeFloat, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.float",
			marshal:   float.Marshal,
			unmarshal: float.Unmarshal,
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
				Data:   []byte("\x80\x00\x00\x00\x00"),
				Values: mod.Values{float32(0)}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x80"),
				Values: mod.Values{float32(0)}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x80\x00\x00"),
				Values: mod.Values{float32(0)}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
