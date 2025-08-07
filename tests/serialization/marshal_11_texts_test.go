//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/blob"
	"github.com/scylladb/gocql/v2/serialization/text"
	"github.com/scylladb/gocql/v2/serialization/varchar"
)

func TestMarshalTexts(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := []testSuite{
		{
			name:      "serialization.varchar",
			marshal:   varchar.Marshal,
			unmarshal: varchar.Unmarshal,
		},
		{
			name:      "serialization.text",
			marshal:   text.Marshal,
			unmarshal: text.Unmarshal,
		},
		{
			name:      "serialization.blob",
			marshal:   blob.Marshal,
			unmarshal: blob.Unmarshal,
		},
		{
			name: "glob.varchar",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(gocql.NewNativeType(4, gocql.TypeVarchar, ""), i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(gocql.NewNativeType(4, gocql.TypeVarchar, ""), bytes, i)
			},
		},
		{
			name: "glob.text",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(gocql.NewNativeType(4, gocql.TypeText, ""), i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(gocql.NewNativeType(4, gocql.TypeText, ""), bytes, i)
			},
		},
		{
			name: "glob.blob",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(gocql.NewNativeType(4, gocql.TypeBlob, ""), i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(gocql.NewNativeType(4, gocql.TypeBlob, ""), bytes, i)
			},
		},
	}

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					([]byte)(nil),
					(*[]byte)(nil),
					(*string)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{""}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   make([]byte, 0),
				Values: mod.Values{make([]byte, 0), ""}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("$test text string$"),
				Values: mod.Values{[]byte("$test text string$"), "$test text string$"}.AddVariants(mod.All...),
			}.Run("text", t, marshal, unmarshal)
		})
	}
}
