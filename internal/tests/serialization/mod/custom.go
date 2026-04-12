package mod

type (
	Bool bool

	Int8  int8
	Int16 int16
	Int32 int32
	Int64 int64
	Int   int

	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64
	Uint   uint

	Float32 float32
	Float64 float64

	String string

	Bytes   []byte
	Bytes3  [3]byte
	Bytes4  [4]byte
	Bytes5  [5]byte
	Bytes15 [15]byte
	Bytes16 [16]byte
	Bytes17 [17]byte

	SliceInt16   []int16
	SliceInt16R  []*int16
	SliceInt16C  []Int16
	SliceInt16CR []*Int16

	SliceInt32   []int32
	SliceInt32R  []*int32
	SliceInt32C  []Int32
	SliceInt32CR []*Int32

	SliceAny []any

	Arr1Int16   [1]int16
	Arr1Int16R  [1]*int16
	Arr1Int16C  [1]Int16
	Arr1Int16CR [1]*Int16

	Arr1Int32   [1]int32
	Arr1Int32R  [1]*int32
	Arr1Int32C  [1]Int32
	Arr1Int32CR [1]*Int32

	ArrAny [1]any

	MapInt16   map[int16]int16
	MapInt16R  map[int16]*int16
	MapInt16C  map[Int16]Int16
	MapInt16CR map[Int16]*Int16

	MapInt32   map[int32]int32
	MapInt32R  map[int32]*int32
	MapInt32C  map[Int32]Int32
	MapInt32CR map[Int32]*Int32

	MapUDT map[string]any
)

var CustomType Mod = func(vals ...any) []any {
	out := make([]any, 0)
	for i := range vals {
		if vals[i] == nil {
			continue
		}
		ct := customType(vals[i])
		if ct != nil {
			out = append(out, ct)
		}
	}
	return out
}

func customType(i any) any {
	switch v := i.(type) {
	case bool:
		return Bool(v)
	case int8:
		return Int8(v)
	case int16:
		return Int16(v)
	case int32:
		return Int32(v)
	case int64:
		return Int64(v)
	case int:
		return Int(v)
	case uint:
		return Uint(v)
	case uint8:
		return Uint8(v)
	case uint16:
		return Uint16(v)
	case uint32:
		return Uint32(v)
	case uint64:
		return Uint64(v)
	case float32:
		return Float32(v)
	case float64:
		return Float64(v)
	case string:
		return String(v)
	case []byte:
		return Bytes(v)
	case [3]byte:
		return Bytes3(v)
	case [4]byte:
		return Bytes4(v)
	case [5]byte:
		return Bytes5(v)
	case [15]byte:
		return Bytes15(v)
	case [16]byte:
		return Bytes16(v)
	case [17]byte:
		return Bytes17(v)
	case []int16:
		return SliceInt16(v)
	case []*int16:
		return SliceInt16R(v)
	case []Int16:
		return SliceInt16C(v)
	case []*Int16:
		return SliceInt16CR(v)
	case []int32:
		return SliceInt32(v)
	case []*int32:
		return SliceInt32R(v)
	case []Int32:
		return SliceInt32C(v)
	case []*Int32:
		return SliceInt32CR(v)
	case [1]int16:
		return Arr1Int16(v)
	case [1]*int16:
		return Arr1Int16R(v)
	case [1]Int16:
		return Arr1Int16C(v)
	case [1]*Int16:
		return Arr1Int16CR(v)
	case [1]int32:
		return Arr1Int32(v)
	case [1]*int32:
		return Arr1Int32R(v)
	case [1]Int32:
		return Arr1Int32C(v)
	case [1]*Int32:
		return Arr1Int32CR(v)
	case map[int16]int16:
		return MapInt16(v)
	case map[int16]*int16:
		return MapInt16R(v)
	case map[Int16]Int16:
		return MapInt16C(v)
	case map[Int16]*Int16:
		return MapInt16CR(v)
	case map[int32]int32:
		return MapInt32(v)
	case map[int32]*int32:
		return MapInt32R(v)
	case map[Int32]Int32:
		return MapInt32C(v)
	case map[Int32]*Int32:
		return MapInt32CR(v)
	case map[string]any:
		return MapUDT(v)
	case []any:
		return SliceAny(v)
	case [1]any:
		return ArrAny(v)
	default:
		return intoCustomR(i)
	}
}

func intoCustomR(i any) any {
	switch v := i.(type) {
	case *bool:
		return (*Bool)(v)
	case *int8:
		return (*Int8)(v)
	case *int16:
		return (*Int16)(v)
	case *int32:
		return (*Int32)(v)
	case *int64:
		return (*Int64)(v)
	case *int:
		return (*Int)(v)
	case *uint:
		return (*Uint)(v)
	case *uint8:
		return (*Uint8)(v)
	case *uint16:
		return (*Uint16)(v)
	case *uint32:
		return (*Uint32)(v)
	case *uint64:
		return (*Uint64)(v)
	case *float32:
		return (*Float32)(v)
	case *float64:
		return (*Float64)(v)
	case *string:
		return (*String)(v)
	case *[]byte:
		return (*Bytes)(v)
	case *[4]byte:
		return (*Bytes4)(v)
	case *[16]byte:
		return (*Bytes16)(v)
	case *[]int16:
		return (*SliceInt16)(v)
	case *[]*int16:
		return (*SliceInt16R)(v)
	case *[]Int16:
		return (*SliceInt16C)(v)
	case *[]*Int16:
		return (*SliceInt16CR)(v)
	case *[]int32:
		return (*SliceInt32)(v)
	case *[]*int32:
		return (*SliceInt32R)(v)
	case *[]Int32:
		return (*SliceInt32C)(v)
	case *[]*Int32:
		return (*SliceInt32CR)(v)
	case *[1]int16:
		return (*Arr1Int16)(v)
	case *[1]*int16:
		return (*Arr1Int16R)(v)
	case *[1]Int16:
		return (*Arr1Int16C)(v)
	case *[1]*Int16:
		return (*Arr1Int16CR)(v)
	case *[1]int32:
		return (*Arr1Int32)(v)
	case *[1]*int32:
		return (*Arr1Int32R)(v)
	case *[1]Int32:
		return (*Arr1Int32C)(v)
	case *[1]*Int32:
		return (*Arr1Int32CR)(v)
	case *map[int16]int16:
		return (*MapInt16)(v)
	case *map[int16]*int16:
		return (*MapInt16R)(v)
	case *map[Int16]Int16:
		return (*MapInt16C)(v)
	case *map[Int16]*Int16:
		return (*MapInt16CR)(v)
	case *map[int32]int32:
		return (*MapInt32)(v)
	case *map[int32]*int32:
		return (*MapInt32R)(v)
	case *map[Int32]Int32:
		return (*MapInt32C)(v)
	case *map[Int32]*Int32:
		return (*MapInt32CR)(v)
	case *map[string]any:
		return (*MapUDT)(v)
	case *[]any:
		return (*SliceAny)(v)
	case *[1]any:
		return (*ArrAny)(v)
	default:
		return nil
	}
}
