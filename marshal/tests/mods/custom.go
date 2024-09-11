package mods

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
	Bytes4  [4]byte
	Bytes16 [16]byte

	SliceInt16   []int16
	SliceInt16R  []*int16
	SliceInt16C  []Int16
	SliceInt16CR []*Int16
	SliceAny     []interface{}

	Arr1Int16   [1]int16
	Arr1Int16R  [1]*int16
	Arr1Int16C  [1]Int16
	Arr1Int16CR [1]*Int16
	ArrAny      [1]interface{}

	MapInt16   map[int16]int16
	MapInt16R  map[int16]*int16
	MapInt16C  map[Int16]Int16
	MapInt16CR map[Int16]*Int16

	MapUDT map[string]interface{}
)

type intoCustom struct{}

func (m intoCustom) Suffix() string {
	return "/custom"
}

func (m intoCustom) Apply(vals []interface{}) []interface{} {
	out := make([]interface{}, 0)
	for i := range vals {
		if vals[i] == nil {
			continue
		}
		ct := m.apply(vals[i])
		if ct != nil {
			out = append(out, ct)
		}
	}
	return out
}

func (m intoCustom) apply(i interface{}) interface{} {
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
	case [4]byte:
		return Bytes4(v)
	case [16]byte:
		return Bytes16(v)
	case []int16:
		return SliceInt16(v)
	case []*int16:
		return SliceInt16R(v)
	case []Int16:
		return SliceInt16C(v)
	case []*Int16:
		return SliceInt16CR(v)
	case [1]int16:
		return Arr1Int16(v)
	case [1]*int16:
		return Arr1Int16R(v)
	case [1]Int16:
		return Arr1Int16C(v)
	case [1]*Int16:
		return Arr1Int16CR(v)
	case map[int16]int16:
		return MapInt16(v)
	case map[int16]*int16:
		return MapInt16R(v)
	case map[Int16]Int16:
		return MapInt16C(v)
	case map[Int16]*Int16:
		return MapInt16CR(v)
	case map[string]interface{}:
		return MapUDT(v)
	case []interface{}:
		return SliceAny(v)
	case [1]interface{}:
		return ArrAny(v)
	default:
		return m.applyRef(i)
	}
}

func (m intoCustom) applyRef(i interface{}) interface{} {
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
	case *[1]int16:
		return (*Arr1Int16)(v)
	case *[1]*int16:
		return (*Arr1Int16R)(v)
	case *[1]Int16:
		return (*Arr1Int16C)(v)
	case *[1]*Int16:
		return (*Arr1Int16CR)(v)
	case *map[int16]int16:
		return (*MapInt16)(v)
	case *map[int16]*int16:
		return (*MapInt16R)(v)
	case *map[Int16]Int16:
		return (*MapInt16C)(v)
	case *map[Int16]*Int16:
		return (*MapInt16CR)(v)
	case *map[string]interface{}:
		return (*MapUDT)(v)
	case *[]interface{}:
		return (*SliceAny)(v)
	case *[1]interface{}:
		return (*ArrAny)(v)
	default:
		return nil
	}
}
