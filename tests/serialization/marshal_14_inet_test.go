//go:build unit
// +build unit

package serialization_test

import (
	"net"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/inet"
)

func TestMarshalsInet(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeInet, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.inet",
			marshal:   inet.Marshal,
			unmarshal: inet.Unmarshal,
		},
		{
			name: "glob",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
				return gocql.Unmarshal(tType, bytes, i)
			},
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
					([]byte)(nil),
					(*[]byte)(nil),
					(*[4]byte)(nil),
					(*[16]byte)(nil),
					(net.IP)(nil),
					(*net.IP)(nil),
					"",
					(*string)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					[4]byte{},
					[16]byte{},
				}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					make([]byte, 0),
					[4]byte{},
					[16]byte{},
					make(net.IP, 0),
					"0.0.0.0",
				}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte{0, 0, 0, 0},
				Values: mod.Values{
					"0.0.0.0",
					[]byte{0, 0, 0, 0},
					net.IP{0, 0, 0, 0},
					[4]byte{},
				}.AddVariants(mod.All...),
			}.Run("v4zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte{0, 0, 0, 0},
				Values: mod.Values{
					[16]byte{},
				}.AddVariants(mod.All...),
			}.Run("v4zerosUnmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte{192, 168, 0, 1},
				Values: mod.Values{
					"192.168.0.1",
					[]byte{192, 168, 0, 1},
					net.IP{192, 168, 0, 1},
					[4]byte{192, 168, 0, 1},
				}.AddVariants(mod.All...),
			}.Run("v4", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte{192, 168, 0, 1},
				Values: mod.Values{
					[16]byte{192, 168, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				}.AddVariants(mod.All...),
			}.Run("v4unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte{255, 255, 255, 255},
				Values: mod.Values{
					"255.255.255.255",
					[]byte{255, 255, 255, 255},
					net.IP{255, 255, 255, 255},
					[4]byte{255, 255, 255, 255},
				}.AddVariants(mod.All...),
			}.Run("v4max", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte{255, 255, 255, 255},
				Values: mod.Values{
					[16]byte{255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				}.AddVariants(mod.All...),
			}.Run("v4maxUnmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Values: mod.Values{
					"::",
					[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					[16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				}.AddVariants(mod.All...),
			}.Run("v6zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Values: mod.Values{
					[4]byte{0, 0, 0, 0},
				}.AddVariants(mod.All...),
			}.Run("v6zerosUnmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
				Values: mod.Values{
					"fe80:cd00:0:cde:1257:0:211e:729c",
					[]byte("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
					net.IP("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
					[16]byte{254, 128, 205, 0, 0, 0, 12, 222, 18, 87, 0, 0, 33, 30, 114, 156},
				}.AddVariants(mod.All...),
			}.Run("v6", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
					[]byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
					net.IP("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
					[16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
				}.AddVariants(mod.All...),
			}.Run("v6max", t, marshal, unmarshal)
		})
	}
}
