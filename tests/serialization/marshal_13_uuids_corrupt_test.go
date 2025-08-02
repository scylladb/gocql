//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/timeuuid"
	"github.com/scylladb/gocql/v2/serialization/uuid"
)

func TestMarshalUUIDsMustFail(t *testing.T) {
	t.Parallel()

	tTypes := []gocql.NativeType{
		gocql.NewNativeType(4, gocql.TypeUUID, ""),
		gocql.NewNativeType(4, gocql.TypeTimeUUID, ""),
	}

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [4]testSuite{
		{
			name:      "serialization.uuid",
			marshal:   uuid.Marshal,
			unmarshal: uuid.Unmarshal,
		},
		{
			name: "glob.uuid",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tTypes[0], i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(tTypes[0], bytes, i)
			},
		},
		{
			name:      "serialization.timeuuid",
			marshal:   timeuuid.Marshal,
			unmarshal: timeuuid.Unmarshal,
		},
		{
			name:      "glob.timeuuid",
			marshal:   func(i interface{}) ([]byte, error) { return gocql.Marshal(tTypes[1], i) },
			unmarshal: func(bytes []byte, i interface{}) error { return gocql.Unmarshal(tTypes[1], bytes, i) },
		},
	}

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					"b6b77c23-c776-40ff-828d-a385f3e8a2aff",
					"00000000-0000-0000-0000-0000000000000",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175, 175},
					[]byte{00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[17]byte{},
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					"b6b77c23-c776-40ff-828d-a385f3e8a2a",
					"00000000-0000-0000-0000-00000000000",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162},
					[]byte{00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[15]byte{},
				}.AddVariants(mod.All...),
			}.Run("small_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					"b6b77c@3-c776-40ff-828d-a385f3e8a2a",
					"00000000-0000-0000-0000-0#0000000000",
				}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf"),
				Values: mod.Values{"", make([]byte, 0), [16]byte{}, gocql.UUID{}}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
				Values: mod.Values{"", make([]byte, 0), [16]byte{}, gocql.UUID{}}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00"),
				Values: mod.Values{"", make([]byte, 0), [16]byte{}, gocql.UUID{}}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
