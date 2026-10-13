//go:build unit
// +build unit

package gocql

import (
	"encoding/binary"
	"net"
	"reflect"
	"testing"
	"time"
)

// mockFramer returns pre-built column data for benchmarking Scan.
type mockFramer struct {
	cols [][]byte
	pos  int
}

func (f *mockFramer) ReadBytesInternal() ([]byte, error) {
	if f.pos >= len(f.cols) {
		f.pos = 0
	}
	data := f.cols[f.pos]
	f.pos++
	return data, nil
}

func (f *mockFramer) GetCustomPayload() map[string][]byte { return nil }
func (f *mockFramer) GetHeaderWarnings() []string         { return nil }
func (f *mockFramer) Release()                            {}

func (f *mockFramer) reset() {
	f.pos = 0
}

// buildMockColumns builds column metadata and matching raw byte data for a
// typical row: (int, bigint, text, bool, timestamp, uuid).
func buildMockColumns() ([]ColumnInfo, [][]byte) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt}},
		{Name: "counter", TypeInfo: NativeType{typ: TypeBigInt}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean}},
		{Name: "created", TypeInfo: NativeType{typ: TypeTimestamp}},
		{Name: "uuid", TypeInfo: NativeType{typ: TypeUUID}},
	}

	// int = 42
	intData := make([]byte, 4)
	binary.BigEndian.PutUint32(intData, 42)

	// bigint = 123456789
	bigintData := make([]byte, 8)
	binary.BigEndian.PutUint64(bigintData, 123456789)

	// text = "hello world"
	textData := []byte("hello world")

	// bool = true
	boolData := []byte{1}

	// timestamp = 2024-01-01 00:00:00 UTC (millis)
	tsData := make([]byte, 8)
	millis := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	binary.BigEndian.PutUint64(tsData, uint64(millis))

	// uuid = sequential bytes
	uuidData := make([]byte, 16)
	for i := range uuidData {
		uuidData[i] = byte(i + 1)
	}

	colData := [][]byte{intData, bigintData, textData, boolData, tsData, uuidData}

	return columns, colData
}

func newTestIter(framer *mockFramer, meta resultMetadata, rows int) *Iter {
	return &Iter{
		framer:  framer,
		meta:    meta,
		numRows: rows,
	}
}

func resetIter(iter *Iter, framer *mockFramer, rows int) {
	framer.reset()
	iter.pos = 0
	iter.numRows = rows
	iter.err = nil
}

// BenchmarkScanJIT benchmarks the JIT row decoder via both Scan (variadic)
// and ScanInto (pre-allocated slice) against the generic Unmarshal path.
func BenchmarkScanJIT(b *testing.B) {
	columns, colData := buildMockColumns()

	meta := resultMetadata{
		columns:        columns,
		actualColCount: len(columns),
	}

	const rowsPerIter = 100

	// JIT via Scan (variadic) — includes variadic heap alloc per call
	b.Run("JIT_Scan", func(b *testing.B) {
		framer := &mockFramer{cols: colData}
		iter := newTestIter(framer, meta, rowsPerIter)
		b.ReportAllocs()

		var (
			id      int32
			counter int64
			name    string
			active  bool
			created time.Time
			uid     UUID
		)

		for i := 0; i < b.N; i++ {
			resetIter(iter, framer, rowsPerIter)
			for j := 0; j < rowsPerIter; j++ {
				if !iter.Scan(&id, &counter, &name, &active, &created, &uid) {
					b.Fatal("Scan failed:", iter.err)
				}
			}
		}
	})

	// JIT via ScanInto (pre-allocated slice) — no variadic alloc
	b.Run("JIT_ScanInto", func(b *testing.B) {
		framer := &mockFramer{cols: colData}
		iter := newTestIter(framer, meta, rowsPerIter)
		b.ReportAllocs()

		var (
			id      int32
			counter int64
			name    string
			active  bool
			created time.Time
			uid     UUID
		)
		dest := []any{&id, &counter, &name, &active, &created, &uid}

		for i := 0; i < b.N; i++ {
			resetIter(iter, framer, rowsPerIter)
			for j := 0; j < rowsPerIter; j++ {
				if !iter.ScanInto(dest) {
					b.Fatal("ScanInto failed:", iter.err)
				}
			}
		}
	})

	// Generic Unmarshal direct (no Iter overhead) — baseline
	b.Run("GenericUnmarshal", func(b *testing.B) {
		b.ReportAllocs()

		var (
			id      int32
			counter int64
			name    string
			active  bool
			created time.Time
			uid     UUID
		)
		dests := []any{&id, &counter, &name, &active, &created, &uid}

		for i := 0; i < b.N; i++ {
			for j := 0; j < rowsPerIter; j++ {
				for k, col := range columns {
					if err := Unmarshal(col.TypeInfo, colData[k], dests[k]); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
	})
}

func TestJITDecoderCorrectness(t *testing.T) {
	columns, colData := buildMockColumns()
	framer := &mockFramer{cols: colData}

	meta := resultMetadata{
		columns:        columns,
		actualColCount: len(columns),
	}

	var (
		id      int32
		counter int64
		name    string
		active  bool
		created time.Time
		uid     UUID
	)

	iter := &Iter{
		framer:  framer,
		meta:    meta,
		numRows: 1,
	}

	if !iter.Scan(&id, &counter, &name, &active, &created, &uid) {
		t.Fatal("Scan failed:", iter.err)
	}

	if id != 42 {
		t.Fatalf("id: expected 42, got %d", id)
	}
	if counter != 123456789 {
		t.Fatalf("counter: expected 123456789, got %d", counter)
	}
	if name != "hello world" {
		t.Fatalf("name: expected 'hello world', got '%s'", name)
	}
	if !active {
		t.Fatal("active: expected true")
	}
	expectedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if !created.Equal(expectedTime) {
		t.Fatalf("created: expected %v, got %v", expectedTime, created)
	}
	expectedUUID := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if uid != expectedUUID {
		t.Fatalf("uuid: expected %v, got %v", expectedUUID, uid)
	}
}

func TestScanIntoCorrectness(t *testing.T) {
	columns, colData := buildMockColumns()
	framer := &mockFramer{cols: colData}

	meta := resultMetadata{
		columns:        columns,
		actualColCount: len(columns),
	}

	var (
		id      int32
		counter int64
		name    string
		active  bool
		created time.Time
		uid     UUID
	)
	dest := []any{&id, &counter, &name, &active, &created, &uid}

	iter := &Iter{
		framer:  framer,
		meta:    meta,
		numRows: 1,
	}

	if !iter.ScanInto(dest) {
		t.Fatal("ScanInto failed:", iter.err)
	}

	if id != 42 {
		t.Fatalf("id: expected 42, got %d", id)
	}
	if counter != 123456789 {
		t.Fatalf("counter: expected 123456789, got %d", counter)
	}
	if name != "hello world" {
		t.Fatalf("name: expected 'hello world', got '%s'", name)
	}
	if !active {
		t.Fatal("active: expected true")
	}
	expectedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if !created.Equal(expectedTime) {
		t.Fatalf("created: expected %v, got %v", expectedTime, created)
	}
	expectedUUID := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if uid != expectedUUID {
		t.Fatalf("uuid: expected %v, got %v", expectedUUID, uid)
	}
}

// Named types must fall back to generic Unmarshal, not panic.
type testNamedInt int32
type testNamedString string

func TestJITDecoderNamedTypes(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar}},
	}

	intData := make([]byte, 4)
	binary.BigEndian.PutUint32(intData, 99)
	colData := [][]byte{intData, []byte("hello")}

	framer := &mockFramer{cols: colData}
	meta := resultMetadata{
		columns:        columns,
		actualColCount: len(columns),
	}

	var (
		id   testNamedInt
		name testNamedString
	)

	iter := &Iter{
		framer:  framer,
		meta:    meta,
		numRows: 1,
	}

	if !iter.Scan(&id, &name) {
		t.Fatal("Scan failed with named types:", iter.err)
	}
	if id != 99 {
		t.Fatalf("named int: expected 99, got %d", id)
	}
	if name != "hello" {
		t.Fatalf("named string: expected 'hello', got '%s'", name)
	}
}

func TestJITDecoderNilDest(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean}},
	}

	intData := make([]byte, 4)
	binary.BigEndian.PutUint32(intData, 42)
	colData := [][]byte{intData, []byte("hello"), {1}}

	framer := &mockFramer{cols: colData}
	meta := resultMetadata{
		columns:        columns,
		actualColCount: len(columns),
	}

	var (
		id     int32
		active bool
	)

	iter := &Iter{
		framer:  framer,
		meta:    meta,
		numRows: 1,
	}

	// nil in the middle to skip the "name" column
	if !iter.Scan(&id, nil, &active) {
		t.Fatal("Scan failed with nil dest:", iter.err)
	}
	if id != 42 {
		t.Fatalf("id: expected 42, got %d", id)
	}
	if !active {
		t.Fatal("active: expected true")
	}
}

// TestJITDecoderEmptyData verifies that empty (non-nil, zero-length) data
// is treated as zero-value, matching the upstream Unmarshal behavior.
func TestJITDecoderEmptyData(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt}},
		{Name: "counter", TypeInfo: NativeType{typ: TypeBigInt}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar}},
		{Name: "active", TypeInfo: NativeType{typ: TypeBoolean}},
		{Name: "created", TypeInfo: NativeType{typ: TypeTimestamp}},
		{Name: "uuid", TypeInfo: NativeType{typ: TypeUUID}},
	}

	// All columns get empty (non-nil, zero-length) data.
	empty := []byte{}
	colData := [][]byte{empty, empty, empty, empty, empty, empty}

	framer := &mockFramer{cols: colData}
	meta := resultMetadata{
		columns:        columns,
		actualColCount: len(columns),
	}

	var (
		id      int32
		counter int64
		name    string
		active  bool
		created time.Time
		uid     UUID
	)

	iter := &Iter{
		framer:  framer,
		meta:    meta,
		numRows: 1,
	}

	if !iter.Scan(&id, &counter, &name, &active, &created, &uid) {
		t.Fatal("Scan failed with empty data:", iter.err)
	}
	if id != 0 {
		t.Fatalf("id: expected 0, got %d", id)
	}
	if counter != 0 {
		t.Fatalf("counter: expected 0, got %d", counter)
	}
	if name != "" {
		t.Fatalf("name: expected empty, got '%s'", name)
	}
	if active {
		t.Fatal("active: expected false")
	}
	if !created.IsZero() {
		t.Fatalf("created: expected zero, got %v", created)
	}
	if uid != (UUID{}) {
		t.Fatalf("uuid: expected zero, got %v", uid)
	}
}

// TestJITDecoderInetToIP verifies that decodeInetToIP matches upstream semantics:
// - nil data → nil IP
// - empty (non-nil) data → make(net.IP, 0) (non-nil, zero-length)
// - 4 bytes → IPv4
// - 16 bytes IPv4-mapped → canonicalized to 4-byte via To4()
// - 16 bytes pure IPv6 → 16-byte IP
func TestJITDecoderInetToIP(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected net.IP
		isNil    bool
		length   int
	}{
		{"nil data", nil, nil, true, 0},
		{"empty non-nil", []byte{}, net.IP{}, false, 0},
		{"ipv4", net.IPv4(192, 168, 1, 1).To4(), net.IPv4(192, 168, 1, 1).To4(), false, 4},
		{"ipv6 mapped ipv4", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1}, net.IPv4(192, 168, 1, 1).To4(), false, 4},
		{"pure ipv6", net.ParseIP("2001:db8::1"), net.ParseIP("2001:db8::1"), false, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ip net.IP
			err := decodeInetToIP(tt.data, &ip)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.isNil {
				if ip != nil {
					t.Fatalf("expected nil, got %v", ip)
				}
				return
			}
			if ip == nil {
				t.Fatal("expected non-nil IP")
			}
			if len(ip) != tt.length {
				t.Fatalf("expected length %d, got %d (ip=%v)", tt.length, len(ip), ip)
			}
			if !ip.Equal(tt.expected) {
				t.Fatalf("expected %v, got %v", tt.expected, ip)
			}
		})
	}

	// Invalid length should return error.
	t.Run("invalid length", func(t *testing.T) {
		var ip net.IP
		err := decodeInetToIP([]byte{1, 2, 3}, &ip)
		if err == nil {
			t.Fatal("expected error for 3-byte inet data")
		}
	})
}

// TestJITDecoderInetToString verifies decodeInetToString matches upstream:
// - nil data → ""
// - empty non-nil → "0.0.0.0"
// - 4/16 bytes → IP string
func TestJITDecoderInetToString(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{"nil data", nil, ""},
		{"empty non-nil", []byte{}, "0.0.0.0"},
		{"ipv4", net.IPv4(10, 0, 0, 1).To4(), "10.0.0.1"},
		{"ipv6", net.ParseIP("::1"), "::1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s string
			err := decodeInetToString(tt.data, &s)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if s != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, s)
			}
		})
	}
}

// TestJITDecoderCacheKeyVersion verifies that different protocol versions
// produce different cache keys, preventing cross-version cache collisions.
func TestJITDecoderCacheKeyVersion(t *testing.T) {
	columnsV3 := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 3}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 3}},
	}
	columnsV4 := []ColumnInfo{
		{Name: "id", TypeInfo: NativeType{typ: TypeInt, proto: 4}},
		{Name: "name", TypeInfo: NativeType{typ: TypeVarchar, proto: 4}},
	}

	destTypes := []reflect.Type{
		reflect.TypeOf((*int32)(nil)),
		reflect.TypeOf((*string)(nil)),
	}

	key3 := makeDecoderCacheKey(columnsV3, destTypes)
	key4 := makeDecoderCacheKey(columnsV4, destTypes)

	if key3 == key4 {
		t.Fatal("cache keys for different protocol versions must differ")
	}

	// Same version should produce the same key.
	key3b := makeDecoderCacheKey(columnsV3, destTypes)
	if key3 != key3b {
		t.Fatal("same columns should produce the same cache key")
	}
}
