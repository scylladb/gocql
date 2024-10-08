package gocql_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalUUIDsMustFail(t *testing.T) {
	tTypes := []gocql.NativeType{
		gocql.NewNativeType(4, gocql.TypeUUID, ""),
		gocql.NewNativeType(4, gocql.TypeTimeUUID, ""),
	}

	for _, tType := range tTypes {
		marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
		unmarshal := func(bytes []byte, i interface{}) error {
			return gocql.Unmarshal(tType, bytes, i)
		}

		t.Run(tType.String(), func(t *testing.T) {
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