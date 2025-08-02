//go:build unit
// +build unit

package serialization_test

import (
	"math/big"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/varint"
)

func TestMarshalVarIntNew(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeVarint, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.varint",
			marshal:   varint.Marshal,
			unmarshal: varint.Unmarshal,
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
				Data: []byte("\x00"),
				Values: mod.Values{
					int8(0), int16(0), int32(0), int64(0), int(0),
					uint8(0), uint16(0), uint32(0), uint64(0), uint(0),
					"0", *big.NewInt(0),
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01"),
				Values: mod.Values{
					int8(1), int16(1), int32(1), int64(1), int(1),
					uint8(1), uint16(1), uint32(1), uint64(1), uint(1),
					"1", *big.NewInt(1),
				}.AddVariants(mod.All...),
			}.Run("+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff"),
				Values: mod.Values{
					int8(-1), int16(-1), int32(-1), int64(-1), int(-1),
					"-1", *big.NewInt(-1),
				}.AddVariants(mod.All...),
			}.Run("-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f"),
				Values: mod.Values{
					int8(127), int16(127), int32(127), int64(127), int(127),
					uint8(127), uint16(127), uint32(127), uint64(127), uint(127),
					"127", *big.NewInt(127),
				}.AddVariants(mod.All...),
			}.Run("maxInt8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80"),
				Values: mod.Values{
					int8(-128), int16(-128), int32(-128), int64(-128), int(-128),
					"-128", *big.NewInt(-128),
				}.AddVariants(mod.All...),
			}.Run("minInt8", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80"),
				Values: mod.Values{
					int16(128), int32(128), int64(128), int(128),
					uint8(128), uint16(128), uint32(128), uint64(128), uint(128),
					"128", *big.NewInt(128),
				}.AddVariants(mod.All...),
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
					"32767", *big.NewInt(32767),
				}.AddVariants(mod.All...),
			}.Run("maxInt16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00"),
				Values: mod.Values{
					int16(-32768), int32(-32768), int64(-32768), int(-32768),
					"-32768", *big.NewInt(-32768),
				}.AddVariants(mod.All...),
			}.Run("minInt16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80\x00"),
				Values: mod.Values{
					int32(32768), int64(32768), int(32768),
					uint16(32768), uint32(32768), uint64(32768), uint(32768),
					"32768", *big.NewInt(32768),
				}.AddVariants(mod.All...),
			}.Run("maxInt16+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff"),
				Values: mod.Values{
					int32(-32769), int64(-32769), int(-32769),
					"-32769", *big.NewInt(-32769),
				}.AddVariants(mod.All...),
			}.Run("minInt16-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff"),
				Values: mod.Values{
					int32(8388607), int64(8388607), int(8388607),
					uint32(8388607), uint64(8388607), uint(8388607),
					"8388607", *big.NewInt(8388607),
				}.AddVariants(mod.All...),
			}.Run("maxInt24", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00"),
				Values: mod.Values{
					int32(-8388608), int64(-8388608), int(-8388608),
					"-8388608", *big.NewInt(-8388608),
				}.AddVariants(mod.All...),
			}.Run("minInt24", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80\x00\x00"),
				Values: mod.Values{
					int64(8388608), int(8388608),
					uint32(8388608), uint64(8388608), uint(8388608),
					"8388608", *big.NewInt(8388608),
				}.AddVariants(mod.All...),
			}.Run("maxInt24+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff\xff"),
				Values: mod.Values{
					int64(-8388609), int(-8388609),
					"-8388609", *big.NewInt(-8388609),
				}.AddVariants(mod.All...),
			}.Run("minInt24-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff\xff"),
				Values: mod.Values{
					int32(2147483647), int64(2147483647), int(2147483647),
					uint32(2147483647), uint64(2147483647), uint(2147483647),
					"2147483647", *big.NewInt(2147483647),
				}.AddVariants(mod.All...),
			}.Run("maxInt32", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00"),
				Values: mod.Values{
					int32(-2147483648), int64(-2147483648), int(-2147483648),
					"-2147483648", *big.NewInt(-2147483648),
				}.AddVariants(mod.All...),
			}.Run("minInt32", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80\x00\x00\x00"),
				Values: mod.Values{
					int64(2147483648), int(2147483648),
					uint32(2147483648), uint64(2147483648), uint(2147483648),
					"2147483648", *big.NewInt(2147483648),
				}.AddVariants(mod.All...),
			}.Run("maxInt32+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff\xff\xff"),
				Values: mod.Values{
					int64(-2147483649), int(-2147483649),
					"-2147483649", *big.NewInt(-2147483649),
				}.AddVariants(mod.All...),
			}.Run("minInt32-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(549755813887), int(549755813887),
					uint64(549755813887), uint(549755813887),
					"549755813887", *big.NewInt(549755813887),
				}.AddVariants(mod.All...),
			}.Run("maxInt40", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(-549755813888), int(-549755813888),
					"-549755813888", *big.NewInt(-549755813888),
				}.AddVariants(mod.All...),
			}.Run("minInt40", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(549755813888), int(549755813888),
					uint64(549755813888), uint(549755813888),
					"549755813888", *big.NewInt(549755813888),
				}.AddVariants(mod.All...),
			}.Run("maxInt40+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(-549755813889), int(-549755813889),
					"-549755813889", *big.NewInt(-549755813889),
				}.AddVariants(mod.All...),
			}.Run("minInt40-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(140737488355327), int(140737488355327),
					uint64(140737488355327), uint(140737488355327),
					"140737488355327", *big.NewInt(140737488355327),
				}.AddVariants(mod.All...),
			}.Run("maxInt48", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(-140737488355328), int(-140737488355328),
					"-140737488355328", *big.NewInt(-140737488355328),
				}.AddVariants(mod.All...),
			}.Run("minInt48", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(140737488355328), int(140737488355328),
					uint64(140737488355328), uint(140737488355328),
					"140737488355328", *big.NewInt(140737488355328),
				}.AddVariants(mod.All...),
			}.Run("maxInt48+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(-140737488355329), int(-140737488355329),
					"-140737488355329", *big.NewInt(-140737488355329),
				}.AddVariants(mod.All...),
			}.Run("minInt48-1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(36028797018963967), int(36028797018963967),
					uint64(36028797018963967), uint(36028797018963967),
					"36028797018963967", *big.NewInt(36028797018963967),
				}.AddVariants(mod.All...),
			}.Run("maxInt56", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(-36028797018963968), int(-36028797018963968),
					"-36028797018963968", *big.NewInt(-36028797018963968),
				}.AddVariants(mod.All...),
			}.Run("minInt56", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x80\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(36028797018963968), int(36028797018963968),
					uint64(36028797018963968), uint(36028797018963968),
					"36028797018963968", *big.NewInt(36028797018963968),
				}.AddVariants(mod.All...),
			}.Run("maxInt56+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(-36028797018963969), int(-36028797018963969),
					"-36028797018963969", *big.NewInt(-36028797018963969),
				}.AddVariants(mod.All...),
			}.Run("minInt56-1", t, marshal, unmarshal)

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
				Data: []byte("\x00\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					"9223372036854775808", *big.NewInt(0).Add(big.NewInt(1), big.NewInt(9223372036854775807)),
				}.AddVariants(mod.All...),
			}.Run("maxInt64+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					"-9223372036854775809", *big.NewInt(0).Add(big.NewInt(-1), big.NewInt(-9223372036854775808)),
				}.AddVariants(mod.All...),
			}.Run("minInt64-1", t, marshal, unmarshal)

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
				Data: []byte("\x00\xff\xff"),
				Values: mod.Values{
					uint16(65535), uint32(65535), uint64(65535), uint(65535),
					int32(65535), int64(65535), int(65535),
					"65535", *big.NewInt(65535),
				}.AddVariants(mod.All...),
			}.Run("maxUint16", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00"),
				Values: mod.Values{
					uint32(65536), uint64(65536), uint(65536),
					int32(65536), int64(65536), int(65536),
					"65536", *big.NewInt(65536),
				}.AddVariants(mod.All...),
			}.Run("maxUint16+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\xff\xff\xff"),
				Values: mod.Values{
					uint32(16777215), uint64(16777215), uint(16777215),
					int32(16777215), int64(16777215), int(16777215),
					"16777215", *big.NewInt(16777215),
				}.AddVariants(mod.All...),
			}.Run("maxUint24", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00\x00"),
				Values: mod.Values{
					uint32(16777216), uint64(16777216), uint(16777216),
					int32(16777216), int64(16777216), int(16777216),
					"16777216", *big.NewInt(16777216),
				}.AddVariants(mod.All...),
			}.Run("maxUint24+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\xff\xff\xff\xff"),
				Values: mod.Values{
					uint32(4294967295), uint64(4294967295), uint(4294967295),
					int64(4294967295), int(4294967295),
					"4294967295", *big.NewInt(4294967295),
				}.AddVariants(mod.All...),
			}.Run("maxUint32", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00\x00\x00"),
				Values: mod.Values{
					uint64(4294967296), uint(4294967296),
					int64(4294967296), int(4294967296),
					"4294967296", *big.NewInt(4294967296),
				}.AddVariants(mod.All...),
			}.Run("maxUint32+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					uint64(1099511627775), uint(1099511627775),
					int64(1099511627775), int(1099511627775),
					"1099511627775", *big.NewInt(1099511627775),
				}.AddVariants(mod.All...),
			}.Run("maxUint40", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					uint64(1099511627776), uint(1099511627776),
					int64(1099511627776), int(1099511627776),
					"1099511627776", *big.NewInt(1099511627776),
				}.AddVariants(mod.All...),
			}.Run("maxUint40+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					uint64(281474976710655), uint(281474976710655),
					int64(281474976710655), int(281474976710655),
					"281474976710655", *big.NewInt(281474976710655),
				}.AddVariants(mod.All...),
			}.Run("maxUint48", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					uint64(281474976710656), uint(281474976710656),
					int64(281474976710656), int(281474976710656),
					"281474976710656", *big.NewInt(281474976710656),
				}.AddVariants(mod.All...),
			}.Run("maxUint48+1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					uint64(72057594037927935), uint(72057594037927935),
					int64(72057594037927935), int(72057594037927935),
					"72057594037927935", *big.NewInt(72057594037927935),
				}.AddVariants(mod.All...),
			}.Run("maxUint56", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					uint64(72057594037927936), uint(72057594037927936),
					int64(72057594037927936), int(72057594037927936),
					"72057594037927936", *big.NewInt(72057594037927936),
				}.AddVariants(mod.All...),
			}.Run("maxUint56+1", t, marshal, unmarshal)

			bigMaxUint64 := new(big.Int)
			bigMaxUint64.SetString("18446744073709551615", 10)

			serialization.PositiveSet{
				Data: []byte("\x00\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					uint64(18446744073709551615), uint(18446744073709551615),
					"18446744073709551615", *bigMaxUint64,
				}.AddVariants(mod.All...),
			}.Run("maxUint64", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					"18446744073709551616", *big.NewInt(0).Add(bigMaxUint64, big.NewInt(1)),
				}.AddVariants(mod.All...),
			}.Run("maxUint64+1", t, marshal, unmarshal)
		})
	}
}
