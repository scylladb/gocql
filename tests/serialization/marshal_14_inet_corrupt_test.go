package serialization_test

import (
	"net"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalsInetMustFail(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeInet, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// marshal big and small `net.IP` values does not return error
	brokenNetIP := serialization.GetTypes(net.IP{}, &net.IP{})

	// unmarshal big and small data into `string` and `*string` does not return error
	brokenString := serialization.GetTypes("", (*string)(nil))

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"192.168.0.1.1",
			net.IP{192, 168, 0, 1, 1},
			[]byte{192, 168, 0, 1, 1},
			[5]byte{192, 168, 0, 1, 1},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenNetIP,
	}.Run("big_valsV4", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"fe80:cd00:0:cde:1257:0:211e:729cc",
			net.IP("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf"),
			[]byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf"),
			[17]byte{254, 128, 205, 0, 0, 0, 12, 222, 18, 87, 0, 0, 33, 30, 114, 156, 156},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenNetIP,
	}.Run("big_valsV6", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"192.168.0",
			net.IP{192, 168, 0},
			[]byte{192, 168, 0},
			[3]byte{192, 168, 0},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenNetIP,
	}.Run("small_valsV4", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"fe80:cd00:0:cde:1257:0:211e",
			net.IP("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
			[]byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
			[15]byte{254, 128, 205, 0, 0, 0, 12, 222, 18, 87, 0, 0, 33, 30, 114},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenNetIP,
	}.Run("small_valsV6", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"b6b77c@3-c776-40ff-828d-a385f3e8a2a",
			"00000000-0000-0000-0000-0#0000000000",
			"192.168.a.1",
		}.AddVariants(mod.All...),
	}.Run("corrupt_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte{192, 168, 0, 1, 1},
		Values: mod.Values{
			"",
			net.IP{},
			[]byte{},
			[4]byte{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenString,
	}.Run("big_dataV4", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2\xaf\xaf"),
		Values: mod.Values{
			"",
			net.IP{},
			[]byte{},
			[16]byte{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenString,
	}.Run("big_dataV6", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte{192, 168, 0},
		Values: mod.Values{
			"",
			net.IP{},
			[]byte{},
			[4]byte{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenString,
	}.Run("small_dataV4", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xb6\xb7\x7c\x23\xc7\x76\x40\xff\x82\x8d\xa3\x85\xf3\xe8\xa2"),
		Values: mod.Values{
			"",
			net.IP{},
			[]byte{},
			[16]byte{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenString,
	}.Run("small_dataV6", t, unmarshal)
}