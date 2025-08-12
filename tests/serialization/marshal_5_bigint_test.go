//go:build unit
// +build unit

package serialization_test

import (
	"math/big"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/bigint"
)

func TestMarshalBigInt(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}
	tType := gocql.NewNativeType(4, gocql.TypeBigInt, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.bigint",
			marshal:   bigint.Marshal,
			unmarshal: bigint.Unmarshal,
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
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"0", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
				Values: mod.Values{
					int8(1), int16(1), int32(1), int64(1), int(1),
					uint8(1), uint16(1), uint32(1), uint64(1), uint(1),
					"1", *big.NewInt(1),
				}.AddVariants(mod.All...),
			}.Run("+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int8(-1), int16(-1), int32(-1), int64(-1), int(-1),
					"-1", *big.NewInt(-1),
				}.AddVariants(mod.All...),
			}.Run("-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x7f"),
				Values: mod.Values{
					int8(127), int16(127), int32(127), int64(127), int(127),
					uint8(127), uint16(127), uint32(127), uint64(127), uint(127),
					"127", *big.NewInt(127),
				}.AddVariants(mod.All...),
			}.Run("maxInt8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\x80"),
				Values: mod.Values{
					int8(-128), int16(-128), int32(-128), int64(-128), int(-128),
					"-128", *big.NewInt(-128),
				}.AddVariants(mod.All...),
			}.Run("minInt8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x80"),
				Values: mod.Values{
					int16(128), int32(128), int64(128), int(128),
					uint8(128), uint16(128), uint32(128), uint64(128), uint(128),
					"128", *big.NewInt(128),
				}.AddVariants(mod.All...),
			}.Run("maxInt8+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\x7f"),
				Values: mod.Values{
					int16(-129), int32(-129), int64(-129), int(-129),
					"-129", *big.NewInt(-129),
				}.AddVariants(mod.All...),
			}.Run("minInt8-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x7f\xff"),
				Values: mod.Values{
					int16(32767), int32(32767), int64(32767), int(32767),
					uint16(32767), uint32(32767), uint64(32767), uint(32767),
					"32767", *big.NewInt(32767),
				}.AddVariants(mod.All...),
			}.Run("maxInt16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\x80\x00"),
				Values: mod.Values{
					int16(-32768), int32(-32768), int64(-32768), int(-32768),
					"-32768", *big.NewInt(-32768),
				}.AddVariants(mod.All...),
			}.Run("minInt16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x80\x00"),
				Values: mod.Values{
					int32(32768), int64(32768), int(32768),
					uint16(32768), uint32(32768), uint64(32768), uint(32768),
					"32768", *big.NewInt(32768),
				}.AddVariants(mod.All...),
			}.Run("maxInt16+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\x7f\xff"),
				Values: mod.Values{
					int32(-32769), int64(-32769), int(-32769),
					"-32769", *big.NewInt(-32769),
				}.AddVariants(mod.All...),
			}.Run("minInt16-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x7f\xff\xff\xff"),
				Values: mod.Values{
					int32(2147483647), int64(2147483647), int(2147483647),
					uint32(2147483647), uint64(2147483647), uint(2147483647),
					"2147483647", *big.NewInt(2147483647),
				}.AddVariants(mod.All...),
			}.Run("maxInt32", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\x80\x00\x00\x00"),
				Values: mod.Values{
					int32(-2147483648), int64(-2147483648), int(-2147483648),
					"-2147483648", *big.NewInt(-2147483648),
				}.AddVariants(mod.All...),
			}.Run("minInt32", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x80\x00\x00\x00"),
				Values: mod.Values{
					int64(2147483648), int(2147483648),
					uint32(2147483648), uint64(2147483648), uint(2147483648),
					"2147483648", *big.NewInt(2147483648),
				}.AddVariants(mod.All...),
			}.Run("maxInt32+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\x7f\xff\xff\xff"),
				Values: mod.Values{
					int64(-2147483649), int(-2147483649),
					"-2147483649", *big.NewInt(-2147483649),
				}.AddVariants(mod.All...),
			}.Run("minInt32-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(9223372036854775807), int(9223372036854775807),
					uint64(9223372036854775807), uint(9223372036854775807),
					"9223372036854775807", *big.NewInt(9223372036854775807),
				}.AddVariants(mod.All...),
			}.Run("maxInt64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(-9223372036854775808), int(-9223372036854775808),
					"-9223372036854775808", *big.NewInt(-9223372036854775808),
				}.AddVariants(mod.All...),
			}.Run("minInt64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\xff"),
				Values: mod.Values{
					uint8(255), uint16(255), uint32(255), uint64(255), uint(255),
					int16(255), int32(255), int64(255), int(255),
					"255", *big.NewInt(255),
				}.AddVariants(mod.All...),
			}.Run("maxUint8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\x01\x00"),
				Values: mod.Values{
					uint16(256), uint32(256), uint64(256), uint(256),
					int16(256), int32(256), int64(256), int(256),
					"256", *big.NewInt(256),
				}.AddVariants(mod.All...),
			}.Run("maxUint8+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x00\xff\xff"),
				Values: mod.Values{
					uint16(65535), uint32(65535), uint64(65535), uint(65535),
					int32(65535), int64(65535), int(65535),
					"65535", *big.NewInt(65535),
				}.AddVariants(mod.All...),
			}.Run("maxUint16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\x00\x01\x00\x00"),
				Values: mod.Values{
					uint32(65536), uint64(65536), uint(65536),
					int32(65536), int64(65536), int(65536),
					"65536", *big.NewInt(65536),
				}.AddVariants(mod.All...),
			}.Run("maxUint16+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00\xff\xff\xff\xff"),
				Values: mod.Values{
					uint32(4294967295), uint64(4294967295), uint(4294967295),
					int64(4294967295), int(4294967295),
					"4294967295", *big.NewInt(4294967295),
				}.AddVariants(mod.All...),
			}.Run("maxUint32", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00"),
				Values: mod.Values{
					uint64(4294967296), uint(4294967296),
					int64(4294967296), int(4294967296),
					"4294967296", *big.NewInt(4294967296),
				}.AddVariants(mod.All...),
			}.Run("maxUint32+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					uint64(18446744073709551615), uint(18446744073709551615),
				}.AddVariants(mod.All...),
			}.Run("maxUint64", t, marshal, unmarshal)
		})
	}
}
