package valcases

import (
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"net"
	"time"

	"github.com/gocql/gocql/serialization/duration"
)

var simpleTypesCases = SimpleTypes{
	{
		CQLName: "boolean",
		CQLType: 0x0004,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte{1},
				LangCases: []LangCase{
					{LangType: "bool", Value: true},
				},
			},
			{
				Name: "min",
				Data: []byte{0},
				LangCases: []LangCase{
					{LangType: "bool", Value: false},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "bool", Value: nilRef(false)},
				},
			},
		},
	},
	{
		CQLName: "tinyint",
		CQLType: 0x0014,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f"),
				LangCases: []LangCase{
					{LangType: "int8", Value: int8(math.MaxInt8)},
					{LangType: "big.Int", Value: big.NewInt(math.MaxInt8)},
				},
			},
			{
				Name: "min",
				Data: []byte("\x80"),
				LangCases: []LangCase{
					{LangType: "int8", Value: int8(math.MinInt8)},
					{LangType: "big.Int", Value: big.NewInt(math.MinInt8)},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x01"),
				LangCases: []LangCase{
					{LangType: "int8", Value: int8(1)},
					{LangType: "big.Int", Value: big.NewInt(1)},
				},
			},
			{
				Name: "-1",
				Data: []byte("\xff"),
				LangCases: []LangCase{
					{LangType: "int8", Value: int8(-1)},
					{LangType: "big.Int", Value: big.NewInt(-1)},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00"),
				LangCases: []LangCase{
					{LangType: "int8", Value: int8(0)},
					{LangType: "big.Int", Value: big.NewInt(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int8", Value: nilRef(int8(0))},
					{LangType: "big.Int", Value: nilRef(big.Int{})},
				},
			},
		},
	},
	{
		CQLName: "smallint",
		CQLType: 0x0013,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\xff"),
				LangCases: []LangCase{
					{LangType: "int16", Value: int16(math.MaxInt16)},
					{LangType: "big.Int", Value: big.NewInt(math.MaxInt16)},
				},
			},
			{
				Name: "min",
				Data: []byte("\x80\x00"),
				LangCases: []LangCase{
					{LangType: "int16", Value: int16(math.MinInt16)},
					{LangType: "big.Int", Value: big.NewInt(math.MinInt16)},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x00\x01"),
				LangCases: []LangCase{
					{LangType: "int16", Value: int16(1)},
					{LangType: "big.Int", Value: big.NewInt(1)},
				},
			},
			{
				Name: "-1",
				Data: []byte("\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int16", Value: int16(-1)},
					{LangType: "big.Int", Value: big.NewInt(-1)},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int16", Value: int16(0)},
					{LangType: "big.Int", Value: big.NewInt(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int16", Value: nilRef(int16(0))},
					{LangType: "big.Int", Value: nilRef(big.Int{})},
				},
			},
		},
	},
	{
		CQLName: "int",
		CQLType: 0x0009,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int32", Value: int32(math.MaxInt32)},
					{LangType: "big.Int", Value: big.NewInt(math.MaxInt32)},
				},
			},
			{
				Name: "min",
				Data: []byte("\x80\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int32", Value: int32(math.MinInt32)},
					{LangType: "big.Int", Value: big.NewInt(math.MinInt32)},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "int32", Value: int32(1)},
					{LangType: "big.Int", Value: big.NewInt(1)},
				},
			},
			{
				Name: "-1",
				Data: []byte("\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int32", Value: int32(-1)},
					{LangType: "big.Int", Value: big.NewInt(-1)},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int32", Value: int32(0)},
					{LangType: "big.Int", Value: big.NewInt(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int32", Value: nilRef(int32(0))},
					{LangType: "big.Int", Value: nilRef(big.Int{})},
				},
			},
		},
	},
	{
		CQLName: "bigint",
		CQLType: 0x0002,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(math.MaxInt64)},
					{LangType: "big.Int", Value: big.NewInt(math.MaxInt64)},
				},
			},
			{
				Name: "min",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(math.MinInt64)},
					{LangType: "big.Int", Value: big.NewInt(math.MinInt64)},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(1)},
					{LangType: "big.Int", Value: big.NewInt(1)},
				},
			},
			{
				Name: "-1",
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(-1)},
					{LangType: "big.Int", Value: big.NewInt(-1)},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(0)},
					{LangType: "big.Int", Value: big.NewInt(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int64", Value: nilRef(int64(0))},
					{LangType: "big.Int", Value: nilRef(big.Int{})},
				},
			},
		},
	},
	{
		CQLName: "varint",
		CQLType: 0x000E,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(math.MaxInt64)},
					{LangType: "big.Int", Value: big.NewInt(math.MaxInt64)},
					{LangType: "string", Value: "9223372036854775807"},
				},
			},
			{
				Name: "min",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(math.MinInt64)},
					{LangType: "big.Int", Value: big.NewInt(math.MinInt64)},
					{LangType: "string", Value: "-9223372036854775808"},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x01"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(1)},
					{LangType: "big.Int", Value: big.NewInt(1)},
					{LangType: "string", Value: "1"},
				},
			},
			{
				Name: "-1",
				Data: []byte("\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(-1)},
					{LangType: "big.Int", Value: big.NewInt(-1)},
					{LangType: "string", Value: "-1"},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(0)},
					{LangType: "big.Int", Value: big.NewInt(0)},
					{LangType: "string", Value: "0"},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int64", Value: nilRef(int64(0))},
					{LangType: "big.Int", Value: nilRef(big.Int{})},
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "float",
		CQLType: 0x0008,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\x7f\xff\xff"),
				LangCases: []LangCase{
					{LangType: "float32", Value: float32(math.MaxFloat32)},
				},
			},
			{
				Name: "min",
				Data: []byte("\xff\x7f\xff\xff"),
				LangCases: []LangCase{
					{LangType: "float32", Value: float32(-math.MaxFloat32)},
				},
			},
			{
				Name: "smallest_pos",
				Data: []byte("\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "float32", Value: float32(math.SmallestNonzeroFloat32)},
				},
			},
			{
				Name: "smallest_neg",
				Data: []byte("\x80\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "float32", Value: float32(-math.SmallestNonzeroFloat32)},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "float32", Value: float32(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "float32", Value: nilRef(float32(0))},
				},
			},
		},
	},
	{
		CQLName: "double",
		CQLType: 0x0007,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\xef\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "float64", Value: math.MaxFloat64},
				},
			},
			{
				Name: "min",
				Data: []byte("\xff\xef\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "float64", Value: -math.MaxFloat64},
				},
			},
			{
				Name: "smallest_pos",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "float64", Value: math.SmallestNonzeroFloat64},
				},
			},
			{
				Name: "smallest_neg",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "float64", Value: -math.SmallestNonzeroFloat64},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "float64", Value: float64(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "float64", Value: nilRef(float64(0))},
				},
			},
		},
	},
	{
		CQLName: "decimal",
		CQLType: 0x0006,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x00\x00\x7f\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "inf.Dec", Value: *inf.NewDec(math.MaxInt64, math.MaxInt16)},
					{LangType: "string", Value: "32767;9223372036854775807"},
				},
			},
			{
				Name: "min",
				Data: []byte("\xff\xff\x80\x00\x80\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "inf.Dec", Value: *inf.NewDec(math.MinInt64, math.MinInt16)},
					{LangType: "string", Value: "-32768;-9223372036854775808"},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "inf.Dec", Value: *inf.NewDec(0, 0)},
					{LangType: "string", Value: "0;0"},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "inf.Dec", Value: nilRef(inf.Dec{})},
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "varchar",
		CQLType: 0x000D,
		Cases: []SimpleTypeCase{
			{
				Name: "val",
				Data: []byte("test string"),
				LangCases: []LangCase{
					{LangType: "string", Value: "test string"},
				},
			},
			{
				Name: "zeros",
				Data: make([]byte, 0),
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "text",
		CQLType: 0x000A,
		Cases: []SimpleTypeCase{
			{
				Name: "val",
				Data: []byte("test string"),
				LangCases: []LangCase{
					{LangType: "string", Value: "test string"},
				},
			},
			{
				Name: "zeros",
				Data: make([]byte, 0),
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "blob",
		CQLType: 0x0003,
		Cases: []SimpleTypeCase{
			{
				Name: "val",
				Data: []byte("test string"),
				LangCases: []LangCase{
					{LangType: "string", Value: "test string"},
				},
			},
			{
				Name: "zeros",
				Data: make([]byte, 0),
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "ascii",
		CQLType: 0x0001,
		Cases: []SimpleTypeCase{
			{
				Name: "val",
				Data: []byte("test string"),
				LangCases: []LangCase{
					{LangType: "string", Value: "test string"},
				},
			},
			{
				Name: "zeros",
				Data: make([]byte, 0),
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "uuid",
		CQLType: 0x000C,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "string", Value: "ffffffff-ffff-ffff-ffff-ffffffffffff"},
					{LangType: "[16]byte", Value: [16]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}},
				},
			},
			{
				Name: "val",
				Data: []byte("\xe9\x39\xf5\x2a\xd6\x90\x11\xef\x9c\xd2\x02\x42\xac\x12\x00\x02"),
				LangCases: []LangCase{
					{LangType: "string", Value: "e939f52a-d690-11ef-9cd2-0242ac120002"},
					{LangType: "[16]byte", Value: [16]byte{233, 57, 245, 42, 214, 144, 17, 239, 156, 210, 2, 66, 172, 18, 0, 2}},
				},
			},
			{
				Name: "zeros",
				Data: make([]byte, 16),
				LangCases: []LangCase{
					{LangType: "string", Value: "00000000-0000-0000-0000-000000000000"},
					{LangType: "[16]byte", Value: [16]byte{}},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
					{LangType: "[16]byte", Value: nilRef([16]byte{})},
				},
			},
		},
	},
	{
		CQLName: "timeuuid",
		CQLType: 0x000F,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\xff\xff\xff\xff\xff\xff\x1f\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "string", Value: "ffffffff-ffff-1fff-ffff-ffffffffffff"},
					{LangType: "[16]byte", Value: [16]byte{255, 255, 255, 255, 255, 255, 31, 255, 255, 255, 255, 255, 255, 255, 255, 255}},
				},
			},
			{
				Name: "val",
				Data: []byte("\xe9\x39\xf5\x2a\xd6\x90\x11\xef\x9c\xd2\x02\x42\xac\x12\x00\x02"),
				LangCases: []LangCase{
					{LangType: "string", Value: "e939f52a-d690-11ef-9cd2-0242ac120002"},
					{LangType: "[16]byte", Value: [16]byte{233, 57, 245, 42, 214, 144, 17, 239, 156, 210, 2, 66, 172, 18, 0, 2}},
				},
			},
			{
				Name: "zeros",
				Data: []byte{0, 0, 0, 0, 0, 0, 1 << 4, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				LangCases: []LangCase{
					{LangType: "string", Value: "00000000-0000-1000-0000-000000000000"},
					{LangType: "[16]byte", Value: [16]byte{0, 0, 0, 0, 0, 0, 1 << 4, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
					{LangType: "[16]byte", Value: nilRef([16]byte{})},
				},
			},
		},
	},
	{
		CQLName: "inet",
		CQLType: 0x0010,
		Cases: []SimpleTypeCase{
			{
				Name: "v6max",
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "string", Value: "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"},
					{LangType: "net.IP", Value: net.IP("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff")},
				},
			},
			{
				Name: "v4max",
				Data: []byte("\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "string", Value: "255.255.255.255"},
					{LangType: "net.IP", Value: net.IP("\xff\xff\xff\xff")},
				},
			},
			{
				Name: "v6zeros",
				Data: make([]byte, 16),
				LangCases: []LangCase{
					{LangType: "string", Value: "::"},
					{LangType: "net.IP", Value: make(net.IP, 16)},
				},
			},
			{
				Name: "v4zeros",
				Data: make([]byte, 4),
				LangCases: []LangCase{
					{LangType: "string", Value: "0.0.0.0"},
					{LangType: "net.IP", Value: make(net.IP, 4)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
					{LangType: "net.IP", Value: (net.IP)(nil)},
					{LangType: "net.IP_ref", Value: nilRef(net.IP{})},
				},
			},
		},
	},
	{
		CQLName: "time",
		CQLType: 0x0012,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(86399999999999)},
					{LangType: "time.Duration", Value: time.Duration(86399999999999)},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(0)},
					{LangType: "time.Duration", Value: time.Duration(0)},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int64", Value: nilRef(int64(0))},
					{LangType: "time.Duration", Value: nilRef(time.Duration(0))},
				},
			},
		},
	},
	{
		CQLName: "timestamp",
		CQLType: 0x000B,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(math.MaxInt64)},
					{LangType: "time.Time", Value: time.UnixMilli(math.MaxInt64).UTC()},
				},
			},
			{
				Name: "min",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(math.MinInt64)},
					{LangType: "time.Time", Value: time.UnixMilli(math.MinInt64).UTC()},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(1)},
					{LangType: "time.Time", Value: time.UnixMilli(1).UTC()},
				},
			},
			{
				Name: "-1",
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(-1)},
					{LangType: "time.Time", Value: time.UnixMilli(-1).UTC()},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "int64", Value: int64(0)},
					{LangType: "time.Time", Value: time.UnixMilli(0).UTC()},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int64", Value: nilRef(int64(0))},
					{LangType: "time.Time", Value: nilRef(time.Time{})},
				},
			},
		},
	},
	{
		CQLName: "date",
		CQLType: 0x0011,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "uint32", Value: uint32(math.MaxUint32)},
					{LangType: "int32", Value: int32(-1)},
					{LangType: "time.Time", Value: time.Date(5881580, 07, 11, 0, 0, 0, 0, time.UTC)},
					{LangType: "string", Value: "5881580-07-11"},
				},
			},
			{
				Name: "mid",
				Data: []byte("\x80\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "uint32", Value: uint32(1 << 31)},
					{LangType: "int32", Value: int32(math.MinInt32)},
					{LangType: "time.Time", Value: time.Date(1970, 01, 01, 0, 0, 0, 0, time.UTC)},
					{LangType: "string", Value: "1970-01-01"},
				},
			},
			{
				Name: "1",
				Data: []byte("\x00\x00\x00\x01"),
				LangCases: []LangCase{
					{LangType: "uint32", Value: uint32(1)},
					{LangType: "int32", Value: int32(1)},
					{LangType: "time.Time", Value: time.Date(-5877641, 06, 24, 0, 0, 0, 0, time.UTC)},
					{LangType: "string", Value: "-5877641-06-24"},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "uint32", Value: uint32(0)},
					{LangType: "int32", Value: int32(0)},
					{LangType: "time.Time", Value: time.Date(-5877641, 06, 23, 0, 0, 0, 0, time.UTC)},
					{LangType: "string", Value: "-5877641-06-23"},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "int32", Value: nilRef(int32(0))},
					{LangType: "time.Time", Value: nilRef(time.Time{})},
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
				},
			},
		},
	},
	{
		CQLName: "duration",
		CQLType: 0x0015,
		Cases: []SimpleTypeCase{
			{
				Name: "max",
				Data: []byte("\xf0\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{Days: math.MaxInt32, Months: math.MaxInt32, Nanoseconds: math.MaxInt64}},
					{LangType: "string", Value: "178956970y7mo306783378w1d2562047h47m16.854775807s"},
				},
			},
			{
				Name: "min",
				Data: []byte("\xf0\xff\xff\xff\xff\xf0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{Days: math.MinInt32, Months: math.MinInt32, Nanoseconds: math.MinInt64}},
					{LangType: "string", Value: "-178956970y8mo306783378w2d2562047h47m16.854775808s"},
				},
			},
			{
				Name: "+1",
				Data: []byte("\x02\x02\x02"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{Days: 1, Months: 1, Nanoseconds: 1}},
					{LangType: "string", Value: "1mo1d1ns"},
				},
			},
			{
				Name: "-1",
				Data: []byte("\x01\x01\x01"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{Days: -1, Months: -1, Nanoseconds: -1}},
					{LangType: "string", Value: "-1mo1d1ns"},
				},
			},
			{
				Name: "maxNanos",
				Data: []byte("\x00\xc3\x41\xfe\xfc\x9b\xc5\xc4\x9d\xff\xfe"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{Days: 106751, Months: 0, Nanoseconds: 85636854775807}},
					{LangType: "int64", Value: int64(math.MaxInt64)},
					{LangType: "time.Duration", Value: time.Duration(math.MaxInt64)},
					{LangType: "string", Value: "15250w1d23h47m16.854775807s"},
				},
			},
			{
				Name: "minNanos",
				Data: []byte("\x00\xc3\x41\xfd\xfc\x9b\xc5\xc4\x9d\xff\xff"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{Days: -106751, Months: 0, Nanoseconds: -85636854775808}},
					{LangType: "int64", Value: int64(math.MinInt64)},
					{LangType: "time.Duration", Value: time.Duration(math.MinInt64)},
					{LangType: "string", Value: "-15250w1d23h47m16.854775808s"},
				},
			},
			{
				Name: "zeros",
				Data: []byte("\x00\x00\x00"),
				LangCases: []LangCase{
					{LangType: "duration", Value: duration.Duration{}},
					{LangType: "int64", Value: int64(0)},
					{LangType: "time.Duration", Value: time.Duration(0)},
					{LangType: "string", Value: "0s"},
				},
			},
			{
				Name: "nil",
				Data: nilBytes,
				LangCases: []LangCase{
					{LangType: "duration", Value: nilRef(duration.Duration{})},
					{LangType: "int64", Value: nilRef(int64(0))},
					{LangType: "time.Duration", Value: nilRef(time.Duration(0))},
					{LangType: "string", Value: ""},
					{LangType: "string_ref", Value: nilRef("")},
				},
			},
		},
	},
}
