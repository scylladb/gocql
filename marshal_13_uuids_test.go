package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalUUIDs(t *testing.T) {
	tTypes := []NativeType{
		{proto: 4, typ: TypeUUID},
		{proto: 4, typ: TypeTimeUUID},
	}

	var nilBytes []byte = nil

	cases := serialization.Group{
		Funcs:       funcs.Default(nil, nil),
		DefaultMods: mod.Ref,
		Sets: []*serialization.Set{
			{
				Name:           "custom",
				Mods:           mod.Non,
				Data:           nil,
				Values:         []interface{}{mod.String(""), mod.Bytes{}, mod.Bytes16{}},
				IssueUnmarshal: "not supported by gocql",
				IssueMarshal:   "not supported by gocql",
			},
			{
				Name: "[nil]refs",
				Mods: mod.Non,
				Data: nil,
				Values: mod.NilRefs(
					"", make([]byte, 0), [16]byte{}, UUID{},
				),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mod.Non,
				Funcs:  funcs.ExcludedMarshal(),
				Data:   nil,
				Values: []interface{}{"", nilBytes, [16]byte{}, UUID{}},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludedMarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{"", [16]byte{}, UUID{}},
			},
			{
				Name:           "unmarshal zero data to bytes",
				Data:           make([]byte, 0),
				Value:          make([]byte, 0),
				IssueMarshal:   "error: can not marshal []byte 0 bytes long into uuid, must be exactly 16 bytes long",
				IssueUnmarshal: "unmarshalls zero bytes to nil bytes",
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{
					"00000000-0000-0000-0000-000000000000",
					[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[16]byte{},
					UUID{}},
			},
			{
				Name: "uuid",
				Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf"),
				Values: []interface{}{
					"b6b77c23-c776-40ff-828d-a385f3e8a2af",
					[]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
					[16]byte{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
					UUID{182, 183, 124, 35, 199, 118, 64, 255, 130, 141, 163, 133, 243, 232, 162, 175},
				},
			},
		},
	}

	for _, tType := range tTypes {
		tCases := cases.Copy()
		tCases.Name = tType.String()
		tCases.Funcs.Marshal = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Funcs.Unmarshal = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases.Gen().RunGroup(t)
	}
}
