//go:build unit
// +build unit

package serialization_test

import (
	"math/big"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/smallint"
)

func TestMarshalSmallint(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeSmallInt)

	testSuites := [2]testSuite{
		{
			name:      "serialization.smallint",
			marshal:   smallint.Marshal,
			unmarshal: smallint.Unmarshal,
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
					(*int8)(nil), (*int16)(nil), (*int32)(nil), (*int64)(nil), (*int)(nil),
					(*uint8)(nil), (*uint16)(nil), (*uint32)(nil), (*uint64)(nil), (*uint)(nil),
					(*string)(nil), (*big.Int)(nil), "",
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"", big.Int{},
				}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"0", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"0", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x01"),
				Values: mod.Values{
					int8(1), int16(1), int32(1), int64(1), int(1),
					uint8(1), uint16(1), uint32(1), uint64(1), uint(1),
					"1", *big.NewInt(1),
				}.AddVariants(mod.All...),
			}.Run("1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff"),
				Values: mod.Values{
					int8(-1), int16(-1), int32(-1), int64(-1), int(-1),
					"-1", *big.NewInt(-1),
				}.AddVariants(mod.All...),
			}.Run("-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x7f"),
				Values: mod.Values{
					int8(127), int16(127), int32(127), int64(127), int(127),
					uint16(127), uint32(127), uint64(127), uint(127), uint(127),
					"127", *big.NewInt(127),
				}.AddVariants(mod.All...),
			}.Run("maxInt8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x80"),
				Values: mod.Values{
					int8(-128), int16(-128), int32(-128), int64(-128), int(-128),
					"-128", *big.NewInt(-128),
				}.AddVariants(mod.All...),
			}.Run("minInt8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80"),
				Values: mod.Values{
					int16(128), int32(128), int64(128), int(128),
					uint16(128), uint32(128), uint64(128), uint(128), uint(128),
					"128", *big.NewInt(128)}.AddVariants(mod.All...),
			}.Run("maxInt8+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f"),
				Values: mod.Values{
					int16(-129), int32(-129), int64(-129), int(-129),
					"-129", *big.NewInt(-129),
				}.AddVariants(mod.All...),
			}.Run("minInt8-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff"),
				Values: mod.Values{
					int16(32767), int32(32767), int64(32767), int(32767),
					uint16(32767), uint32(32767), uint64(32767), uint(32767),
					"32767", *big.NewInt(32767)}.AddVariants(mod.All...),
			}.Run("maxInt16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00"),
				Values: mod.Values{
					int16(-32768), int32(-32768), int64(-32768), int(-32768),
					"-32768", *big.NewInt(-32768),
				}.AddVariants(mod.All...),
			}.Run("minInt16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\xff"),
				Values: mod.Values{
					uint8(255), uint16(255), uint32(255), uint64(255), uint(255),
					int16(255), int32(255), int64(255), int(255),
					"255", *big.NewInt(255),
				}.AddVariants(mod.All...),
			}.Run("maxUint8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00"),
				Values: mod.Values{
					uint16(256), uint32(256), uint64(256), uint(256),
					int16(256), int32(256), int64(256), int(256),
					"256", *big.NewInt(256),
				}.AddVariants(mod.All...),
			}.Run("maxUint8+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff"),
				Values: mod.Values{
					uint16(65535), uint32(65535), uint64(65535), uint(65535),
				}.AddVariants(mod.All...),
			}.Run("maxUint16", t, marshal, unmarshal)
		})
	}
}
