package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalUUIDs(t *testing.T) {
	tTypes := []NativeType{
		{proto: 4, typ: TypeUUID},
		{proto: 4, typ: TypeTimeUUID},
	}

	var nilBytes []byte = nil

	cases := tests.Serialization{
		Funcs:       funcs.Default(nil, nil),
		DefaultMods: mods.Ref,
		Cases: []*tests.Case{
			{
				Name:           "custom",
				Mods:           mods.Non,
				Data:           nil,
				Values:         []interface{}{mods.String(""), mods.Bytes{}, mods.Bytes16{}},
				IssueUnmarshal: "not supported by gocql",
				IssueMarshal:   "not supported by gocql",
			},
			{
				Name: "[nil]refs",
				Mods: mods.Non,
				Data: nil,
				Values: tests.NilRefs(
					"", make([]byte, 0), [16]byte{}, UUID{},
				),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mods.Non,
				Funcs:  funcs.ExcludeMarshal(),
				Data:   nil,
				Values: []interface{}{"", nilBytes, [16]byte{}, UUID{}},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludeMarshal(),
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
		tCases.Funcs.Marshal = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Funcs.Unmarshal = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases = tCases.GetModified()
		t.Run(tType.String(), func(t *testing.T) {
			tCases.Run(t)
		})
	}
}

func TestMarshalUUIDsMustFail(t *testing.T) {
	tTypes := []NativeType{
		{proto: 4, typ: TypeUUID},
		{proto: 4, typ: TypeTimeUUID},
	}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			DefaultMods: mods.Ref,
			Cases: []*tests.MustFailMarshal{
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
		},
		Unmarshal: &tests.UnmarshalCases{
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
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
		},
	}

	for _, tType := range tTypes {
		tCases := cases.Copy()
		tCases.Marshal.Func = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Unmarshal.Func = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases = tCases.GetModified()
		t.Run(tType.String(), func(t *testing.T) {
			tCases.Run(t)
		})
	}
}
