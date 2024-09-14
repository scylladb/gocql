package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalUUIDsMustFail(t *testing.T) {
	tTypes := []NativeType{
		{proto: 4, typ: TypeUUID},
		{proto: 4, typ: TypeTimeUUID},
	}

	mCases := &marshal.Group{
		DefaultMods: mod.Ref,
		Sets: []*marshal.Set{
			{
				Name: "big_vals",
				MarshalIns: []interface{}{
					"b6b77c23-c776-40ff-828d-a385f3e8a2aff",
					"00000000-0000-0000-0000-0000000000000",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175, 175},
					[]byte{00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[17]byte{},
				},
			},
			{
				Name: "small_vals",
				MarshalIns: []interface{}{
					"b6b77c23-c776-40ff-828d-a385f3e8a2a",
					"00000000-0000-0000-0000-00000000000",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162},
					[]byte{00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[15]byte{},
				},
			},
			{
				Name: "corrupt_vals",
				MarshalIns: []interface{}{
					"b6b77c@3-c776-40ff-828d-a385f3e8a2a",
					"00000000-0000-0000-0000-0#0000000000",
				},
			},
		},
	}
	for _, tType := range tTypes {
		cCases := mCases.Copy()
		cCases.Name = tType.String()
		cCases.Func = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }

		cCases.Gen().RunGroup(t)
	}

	uCases := &unmarshal.Group{
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name: "big_data",
				Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf"),
				UnmarshalIns: []interface{}{
					"",
					make([]byte, 0),
					[16]byte{},
					UUID{},
				},
			},
			{
				Name: "small_data1",
				Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
				UnmarshalIns: []interface{}{
					"",
					make([]byte, 0),
					[16]byte{},
					UUID{},
				},
			},
			{
				Name: "small_data2",
				Data: []byte("\x00"),
				UnmarshalIns: []interface{}{
					"",
					make([]byte, 0),
					[16]byte{},
					UUID{},
				},
			},
		},
	}
	for _, tType := range tTypes {
		cCases := uCases.Copy()
		cCases.Name = tType.String()
		cCases.Func = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		cCases.Gen().RunGroup(t)
	}
}
