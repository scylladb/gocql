//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/ascii"
)

func TestMarshalAsciiMustFail(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeAscii, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.ascii",
			marshal:   ascii.Marshal,
			unmarshal: ascii.Unmarshal,
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
				Data:   []byte{255},
				Values: mod.Values{[]byte{}, ""}.AddVariants(mod.All...),
			}.Run("corrupt_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte{127, 255, 127},
				Values: mod.Values{[]byte{}, ""}.AddVariants(mod.All...),
			}.Run("corrupt_data2", t, unmarshal)
		})
	}
}
