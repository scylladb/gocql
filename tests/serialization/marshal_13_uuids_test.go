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

func TestMarshalUUIDs(t *testing.T) {
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

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					([]byte)(nil), (*[]byte)(nil),
					"", (*string)(nil),
					(*[16]byte)(nil),
					(*gocql.UUID)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					"00000000-0000-0000-0000-000000000000",
					make([]byte, 0),
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Values: mod.Values{
					[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[16]byte{},
					gocql.UUID{},
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xe9\x39\xf5\x2a\xd6\x90\x11\xef\x9c\xd2\x02\x42\xac\x12\x00\x02"),
				Values: mod.Values{
					"e939f52a-d690-11ef-9cd2-0242ac120002",
					[]byte{233, 57, 245, 42, 214, 144, 17, 239, 156, 210, 2, 66, 172, 18, 0, 2},
					[16]byte{233, 57, 245, 42, 214, 144, 17, 239, 156, 210, 2, 66, 172, 18, 0, 2},
					gocql.UUID{233, 57, 245, 42, 214, 144, 17, 239, 156, 210, 2, 66, 172, 18, 0, 2},
				}.AddVariants(mod.All...),
			}.Run("uuid", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					"ffffffff-ffff-ffff-ffff-ffffffffffff",
					[]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
					[16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
					gocql.UUID{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				}.AddVariants(mod.All...),
			}.Run("max", t, marshal, unmarshal)
		})
	}
}

func TestMarshalTimeUUID(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeTimeUUID, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [4]testSuite{
		{
			name:      "serialization.timeuuid",
			marshal:   timeuuid.Marshal,
			unmarshal: timeuuid.Unmarshal,
		},
		{
			name:      "glob.timeuuid",
			marshal:   func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) },
			unmarshal: func(bytes []byte, i interface{}) error { return gocql.Unmarshal(tType, bytes, i) },
		},
	}

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					"00000000-0000-0000-0000-000000000000",
				}.AddVariants(mod.All...),
			}.Run("zero", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\x1f\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					"ffffffff-ffff-1fff-ffff-ffffffffffff",
					[]byte{255, 255, 255, 255, 255, 255, 31, 255, 255, 255, 255, 255, 255, 255, 255, 255},
					[16]byte{255, 255, 255, 255, 255, 255, 31, 255, 255, 255, 255, 255, 255, 255, 255, 255},
					gocql.UUID{255, 255, 255, 255, 255, 255, 31, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				}.AddVariants(mod.All...),
			}.Run("max", t, marshal, unmarshal)
		})
	}
}
