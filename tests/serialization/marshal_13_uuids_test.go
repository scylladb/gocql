//go:build unit
// +build unit

package serialization_test

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/timeuuid"
	"github.com/gocql/gocql/serialization/uuid"
)

func TestMarshalUUIDs(t *testing.T) {
	t.Parallel()

	tTypes := []gocql.NativeType{
		gocql.NewNativeType(4, gocql.TypeUUID),
		gocql.NewNativeType(4, gocql.TypeTimeUUID),
	}

	type testSuite struct {
		name      string
		marshal   func(any) ([]byte, error)
		unmarshal func(bytes []byte, i any) error
	}

	testSuites := [4]testSuite{
		{
			name:      "serialization.uuid",
			marshal:   uuid.Marshal,
			unmarshal: uuid.Unmarshal,
		},
		{
			name: "glob.uuid",
			marshal: func(i any) ([]byte, error) {
				return gocql.Marshal(tTypes[0], i)
			},
			unmarshal: func(bytes []byte, i any) error {
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
			marshal:   func(i any) ([]byte, error) { return gocql.Marshal(tTypes[1], i) },
			unmarshal: func(bytes []byte, i any) error { return gocql.Unmarshal(tTypes[1], bytes, i) },
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

	tType := gocql.NewNativeType(4, gocql.TypeTimeUUID)

	type testSuite struct {
		name      string
		marshal   func(any) ([]byte, error)
		unmarshal func(bytes []byte, i any) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.timeuuid",
			marshal:   timeuuid.Marshal,
			unmarshal: timeuuid.Unmarshal,
		},
		{
			name:      "glob.timeuuid",
			marshal:   func(i any) ([]byte, error) { return gocql.Marshal(tType, i) },
			unmarshal: func(bytes []byte, i any) error { return gocql.Unmarshal(tType, bytes, i) },
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

			// timeuuid is the only type here that decodes into a time.Time:
			// Unmarshal routes *time.Time to DecTime and **time.Time to
			// DecTimeR, which read the RFC 4122 v1 timestamp -- 100-nanosecond
			// intervals since 1582-10-15 UTC -- out of the first eight bytes.
			// Marshal has no matching case, so these sets are unmarshal-only.
			//
			// The vectors are built from the RFC layout rather than from the
			// decoder, so they do not merely restate it; each one also matches
			// gocql.UUIDFromTime for the same instant.
			// DecTimeR separates a CQL NULL from an empty-but-present value:
			// nil data leaves the pointer nil, zero-length data allocates a
			// zero time. Only the second is reachable with make([]byte, 0), so
			// both are fed here.
			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{time.Time{}, (*time.Time)(nil)},
			}.Run("[nil]unmarshal_time", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					time.Time{},
				}.AddVariants(mod.Reference),
			}.Run("[]unmarshal_time", t, nil, unmarshal)

			// timestamp 0: the v1 epoch itself.
			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x10\x00\x92\x34\x01\x02\x03\x04\x05\x06"),
				Values: mod.Values{
					time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC),
				}.AddVariants(mod.Reference),
			}.Run("time_epoch", t, nil, unmarshal)

			// A whole second, and the same instant offset by 123456 microseconds,
			// which is exact at the format's 100-nanosecond resolution. The
			// second one is what separates the seconds field from the remainder.
			serialization.PositiveSet{
				Data: []byte("\x4a\x78\x40\x00\x4b\xc4\x11\xeb\x92\x34\x01\x02\x03\x04\x05\x06"),
				Values: mod.Values{
					time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC),
				}.AddVariants(mod.Reference),
			}.Run("time_second", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x4a\x8b\x16\x80\x4b\xc4\x11\xeb\x92\x34\x01\x02\x03\x04\x05\x06"),
				Values: mod.Values{
					time.Date(2021, time.January, 1, 0, 0, 0, 123456000, time.UTC),
				}.AddVariants(mod.Reference),
			}.Run("time_subsecond", t, nil, unmarshal)
		})
	}
}
