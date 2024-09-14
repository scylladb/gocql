package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"net"
	"testing"
)

func TestMarshalsInetMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInet}

	mCases := &marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Ref,
		Sets: []*marshal.Set{
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
	}
	mCases.Gen().RunGroup(t)

	cCases := &unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Ref,
		Sets: []*unmarshal.Set{
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
	}
	cCases.Gen().RunGroup(t)
}
