package gocql

import (
	"net"
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalsInet(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInet}

	var nilIP net.IP = nil

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Ref,
		Cases: []*tests.Case{
			{
				Name:           "custom string",
				Mods:           mods.Non,
				Data:           []byte{192, 168, 0, 1},
				Values:         []interface{}{mods.String("")},
				IssueUnmarshal: "not supported by gocql",
				IssueMarshal:   "not supported by gocql",
			},
			{
				Name:   "[nil]refs",
				Mods:   mods.Non,
				Data:   nil,
				Values: tests.NilRefs("", net.IP{}),
			},
			{
				Name:  "unmarshal nil data to string",
				Mods:  mods.Non,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Value: "",
			},
			{
				Name:           "unmarshal nil data to ip",
				Mods:           mods.Non,
				Funcs:          funcs.ExcludeMarshal(),
				Data:           nil,
				Value:          nilIP,
				IssueUnmarshal: "error: cannot unmarshal inet into *net.IP: invalid sized IP: got 0 bytes not 4 or 16",
			},
			{
				Name:  "unmarshal zero data to sting",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: "",
			},
			{
				Name:           "unmarshal zero data to ip",
				Funcs:          funcs.ExcludeMarshal(),
				Data:           make([]byte, 0),
				Value:          make(net.IP, 0),
				IssueUnmarshal: "error: cannot unmarshal inet into *net.IP: invalid sized IP: got 0 bytes not 4 or 16",
			},
			{
				Name: "zeros1",
				Data: []byte{0, 0, 0, 0},
				Values: []interface{}{
					"0.0.0.0",
					net.IP{0, 0, 0, 0},
				},
			},
			{
				Name: "zeros2",
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Values: []interface{}{
					"::",
					net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
			{
				Name: "localhost",
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
				Values: []interface{}{
					"::1",
					net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
				},
			},
			{
				Name: "ip_v4",
				Data: []byte{192, 168, 0, 1},
				Values: []interface{}{
					"192.168.0.1",
					net.IP{192, 168, 0, 1},
				},
			},
			{
				Name: "ip_v6",
				Data: []byte("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
				Values: []interface{}{
					"fe80:cd00:0:cde:1257:0:211e:729c",
					net.IP("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
				},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalsInetMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInet}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Ref,
			Cases: []*tests.MustFailMarshal{
				{
					Name:      "big_vals_string",
					MarshalIn: "fe80:cd00:0:cde:1257:0:211e:729cc",
				},
				{
					Name:      "small_vals_string",
					MarshalIn: "fe80:cd00:0:cde:1257:0:211e",
				},
				{
					Name:      "big_vals_ip4",
					MarshalIn: net.IP{192, 168, 0, 1, 1},
					Issue:     "return a zero len data instead of error",
				},
				{
					Name:      "small_vals_ip4",
					MarshalIn: net.IP{192, 168, 0},
					Issue:     "return a zero len data instead of error",
				},
				{
					Name:      "big_vals_ip6",
					MarshalIn: net.IP("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf"),
					Issue:     "return a zero len data instead of error",
				},
				{
					Name:      "small_vals_ip6",
					MarshalIn: net.IP("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
					Issue:     "return a data instead of error",
				},
				{
					Name: "corrupt_vals",
					MarshalIns: []interface{}{
						"b6b77c@3-c776-40ff-828d-a385f3e8a2a",
						"00000000-0000-0000-0000-0#0000000000",
						"192.168.a.1",
					},
				},
			},
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Ref,
			Cases: []*tests.MustFailUnmarshal{
				{
					Name:        "big_data_string_v4",
					Data:        []byte{192, 168, 0, 1, 1},
					UnmarshalIn: "",
					Issue:       "the error is not returned",
				},
				{
					Name:        "big_data_string_v6",
					Data:        []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf\xaf"),
					UnmarshalIn: "",
					Issue:       "the error is not returned",
				},
				{
					Name:        "big_data_ip_v4",
					Data:        []byte{192, 168, 0, 1, 1},
					UnmarshalIn: net.IP{},
				},
				{
					Name:        "big_data_ip_v6",
					Data:        []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf\xaf"),
					UnmarshalIn: net.IP{},
				},
				{
					Name:        "small_data_string",
					Data:        []byte{0},
					UnmarshalIn: "",
					Issue:       "the error is not returned",
				},
				{
					Name:        "small_data_string_v4",
					Data:        []byte{192, 168, 0},
					UnmarshalIn: "",
					Issue:       "the error is not returned",
				},
				{
					Name:        "small_data_string_v6",
					Data:        []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
					UnmarshalIn: "",
					Issue:       "the error is not returned",
				},
				{
					Name:        "small_data_ip",
					Data:        []byte{0},
					UnmarshalIn: net.IP{},
				},
				{
					Name:        "small_data_ip_v4",
					Data:        []byte{192, 168, 0},
					UnmarshalIn: net.IP{},
				},
				{
					Name:        "small_data_ip_v6",
					Data:        []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
					UnmarshalIn: net.IP{},
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
