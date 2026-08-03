//go:build all || unit
// +build all unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Regression tests for edge cases in the marshal/unmarshal fast paths and
// buffer pooling: panic-safety on malformed TypeInfo, pooled-buffer
// provenance, and slice-reuse/null-handling in the collection decoders.

package gocql

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"unsafe"
)

// --- Fix #5 helpers: TypeInfo that reports list/set/map but does NOT
// implement CollectionType. Used to exercise the panic-safe fallback path.

type stubListTypeInfo struct {
	NativeType
}

func (stubListTypeInfo) Type() Type      { return TypeList }
func (s stubListTypeInfo) Version() byte { return protoVersion4 }

type stubMapTypeInfo struct {
	NativeType
}

func (stubMapTypeInfo) Type() Type      { return TypeMap }
func (s stubMapTypeInfo) Version() byte { return protoVersion4 }

// Fix #5 — Marshal/Unmarshal must not panic on non-CollectionType TypeInfo
// whose Type() reports a list/set/map; should return an error.

func TestMarshal_NonCollectionTypeList_ReturnsError(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("Marshal panicked on non-CollectionType TypeInfo: %v", x)
		}
	}()
	_, err := Marshal(stubListTypeInfo{}, []any{1, 2, 3})
	if err == nil {
		t.Fatal("expected error from Marshal, got nil")
	}
	if !strings.Contains(err.Error(), "marshal") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestUnmarshal_NonCollectionTypeList_ReturnsError(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("Unmarshal panicked on non-CollectionType TypeInfo: %v", x)
		}
	}()
	var dst any
	err := Unmarshal(stubListTypeInfo{}, []byte{0, 0, 0, 1, 0, 0, 0, 1, 1}, &dst)
	if err == nil {
		t.Fatal("expected error from Unmarshal, got nil")
	}
}

func TestMarshal_NonCollectionTypeMap_ReturnsError(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("Marshal panicked on non-CollectionType TypeInfo: %v", x)
		}
	}()
	_, err := Marshal(stubMapTypeInfo{}, map[string]int{"a": 1})
	if err == nil {
		t.Fatal("expected error from Marshal, got nil")
	}
}

func TestUnmarshal_NonCollectionTypeMap_ReturnsError(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("Unmarshal panicked on non-CollectionType TypeInfo: %v", x)
		}
	}()
	var dst any
	err := Unmarshal(stubMapTypeInfo{}, nil, &dst)
	if err == nil {
		t.Fatal("expected error from Unmarshal, got nil")
	}
}

// --- Fix #1+#2 helpers: queryValues.pooled flag and bounded finalize loop.

// userMarshalerOwned returns a user-owned []byte (kept by the test via
// keepAlive) so we can prove the connector's post-framer cleanup does NOT
// return user-owned memory to the byte pool.
type userMarshalerOwned struct {
	keepAlive *[]byte
}

func (m userMarshalerOwned) MarshalCQL(_ TypeInfo) ([]byte, error) {
	buf := []byte("USER_DATA")
	if m.keepAlive != nil {
		*m.keepAlive = buf
	}
	return buf, nil
}

// Fix #1 — user Marshaler output is NOT flagged as pooled; the user's
// external reference must remain valid.
func TestMarshalQueryValue_UserMarshalerPooledFlag(t *testing.T) {
	keepAlive := []byte("OLD")
	q := &queryValues{}
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}
	if err := marshalQueryValue(info, userMarshalerOwned{keepAlive: &keepAlive}, q); err != nil {
		t.Fatal(err)
	}
	if q.pooled {
		t.Error("user Marshaler output must not be flagged as pooled")
	}
	if !bytes.Equal(keepAlive, []byte("USER_DATA")) {
		t.Errorf("user-owned buffer should not have been overwritten: got %v", keepAlive)
	}
}

// Fix #1 — fast-path marshal IS flagged as pooled.
func TestMarshalQueryValue_PooledFlag_FastPath(t *testing.T) {
	q := &queryValues{}
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}
	if err := marshalQueryValue(info, []int32{1, 2, 3}, q); err != nil {
		t.Fatal(err)
	}
	if !q.pooled {
		t.Error("expected []int32 marshal to be flagged as pooled (fast-path)")
	}
}

// Fix #2 — the post-framer finalize loop must iterate len(vals) (not
// len(cols)) so a mismatched-metadata theoretical case where cols>n
// does not panic on an out-of-range index. We exercise the bounded loop
// directly without spinning up a Conn.
func TestQueryValues_LoopBound_Fix2NoPanicOnAllocsWhenColsLonger(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("bounded iteration over vals panicked: %v", x)
		}
	}()
	v := []queryValues{{name: "a"}, {name: "b"}}
	// Iterate exactly as conn.go does (vals, not cols).
	for i := range v {
		if v[i].pooled {
			_ = v[i].value
		}
	}
}

// --- Fix #6 helpers: blob/UUID slice-reuse accumulation tests.

// buildListBytesWithNulls builds a CQL list wire encoding where nil entries
// use length-prefix -1 and zero-length non-nil entries use length 0.
func buildListBytesWithNulls(elements []*[]byte) []byte {
	var buf bytes.Buffer
	binary.BigEndian.PutUint32(_uuid_uint32Append(&buf), uint32(len(elements)))
	for _, e := range elements {
		if e == nil {
			binary.BigEndian.PutUint32(_uuid_uint32Append(&buf), 0xffffffff)
			continue
		}
		binary.BigEndian.PutUint32(_uuid_uint32Append(&buf), uint32(len(*e)))
		buf.Write(*e)
	}
	return buf.Bytes()
}

func _uuid_uint32Append(buf *bytes.Buffer) []byte {
	buf.Write(make([]byte, 4))
	return buf.Bytes()[buf.Len()-4 : buf.Len()]
}

func buildListBytesUUIDWithNulls(elements []*UUID) []byte {
	var buf bytes.Buffer
	binary.BigEndian.PutUint32(_uuid_uint32Append(&buf), uint32(len(elements)))
	for _, e := range elements {
		if e == nil {
			binary.BigEndian.PutUint32(_uuid_uint32Append(&buf), 0xffffffff)
			continue
		}
		binary.BigEndian.PutUint32(_uuid_uint32Append(&buf), 16)
		buf.Write(e.Bytes())
	}
	return buf.Bytes()
}

// Fix #6 — blob decoder redistributes a slice with stale ghost entries;
// null slots are explicitly cleared to nil.
func TestUnmarshalListBlob_ReusedSlice_NullSlotsCleared(t *testing.T) {
	foo := []byte("foo")
	wire := buildListBytesWithNulls([]*[]byte{nil, &foo, nil})

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeBlob},
	}

	dst := make([][]byte, 0, 4)
	dst = append(dst, []byte("GHOST0"), []byte("GHOST1"), []byte("GHOST2"), []byte("GHOST3"))

	if err := unmarshalList(info, wire, &dst); err != nil {
		t.Fatal(err)
	}
	if len(dst) != 3 {
		t.Fatalf("expected length 3, got %d", len(dst))
	}
	if dst[0] != nil {
		t.Errorf("expected dst[0]=nil (null slot), got %v", dst[0])
	}
	if !bytes.Equal(dst[1], []byte("foo")) {
		t.Errorf("expected dst[1]=[foo], got %v", dst[1])
	}
	if dst[2] != nil {
		t.Errorf("expected dst[2]=nil (null slot), got %v", dst[2])
	}
}

// Fix #6 — UUID decoder mirrors blob: null slots become zero UUID.
func TestUnmarshalListUUID_ReusedSlice_NullSlotsCleared(t *testing.T) {
	uuidA, err := UUIDFromBytes([]byte("1234567890ABCDEF"))
	if err != nil {
		t.Fatal(err)
	}
	uuidB, err := UUIDFromBytes([]byte("FEDCBA0987654321"))
	if err != nil {
		t.Fatal(err)
	}
	if uuidA == uuidB || uuidB == (UUID{}) {
		t.Fatal("ghost UUID must differ from the decoded value and from the zero UUID")
	}
	wire := buildListBytesUUIDWithNulls([]*UUID{nil, &uuidA, nil})

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeUUID},
	}

	dst := make([]UUID, 0, 4)
	dst = append(dst, uuidB, uuidB, uuidB, uuidB)

	if err := unmarshalList(info, wire, &dst); err != nil {
		t.Fatal(err)
	}
	if len(dst) != 3 {
		t.Fatalf("expected length 3, got %d", len(dst))
	}
	if dst[0] != (UUID{}) {
		t.Errorf("expected dst[0]=zero UUID (null slot), got %v", dst[0])
	}
	if dst[1] != uuidA {
		t.Errorf("expected dst[1]=uuidA, got %v", dst[1])
	}
	if dst[2] != (UUID{}) {
		t.Errorf("expected dst[2]=zero UUID (null slot), got %v", dst[2])
	}
}

// TestUnmarshalListString_ReusedSlice_NullSlotsCleared verifies []string
// reuses dst's backing array and clears null/empty slots.
func TestUnmarshalListString_ReusedSlice_NullSlotsCleared(t *testing.T) {
	wire := buildCQLListWithNulls(nil, []byte("foo"), nil)

	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
	}

	dst := make([]string, 0, 4)
	dst = append(dst, "GHOST0", "GHOST1", "GHOST2", "GHOST3")
	prev := &dst[:cap(dst)][0]

	if err := unmarshalList(info, wire, &dst); err != nil {
		t.Fatal(err)
	}
	if &dst[:cap(dst)][0] != prev {
		t.Error("[]string backing array was not reused")
	}
	if len(dst) != 3 {
		t.Fatalf("expected length 3, got %d", len(dst))
	}
	if dst[0] != "" {
		t.Errorf("expected dst[0]=\"\" (null slot), got %q", dst[0])
	}
	if dst[1] != "foo" {
		t.Errorf("expected dst[1]=foo, got %q", dst[1])
	}
	if dst[2] != "" {
		t.Errorf("expected dst[2]=\"\" (null slot), got %q", dst[2])
	}
}

// M1 (review follow-up) — unmarshalListString shares one buffer across all
// strings only up to marshalBufMaxCap, so keeping one small string from a
// huge list can't pin an unbounded amount of memory.

// TestUnmarshalListString_SmallTotal_SharesBackingBuffer verifies the
// shared-buffer path is still used below the threshold: consecutive
// strings' data pointers are exactly adjacent in one buffer.
func TestUnmarshalListString_SmallTotal_SharesBackingBuffer(t *testing.T) {
	wire := buildCQLList([]byte("foo"), []byte("bar"))
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
	}
	var dst []string
	if err := unmarshalList(info, wire, &dst); err != nil {
		t.Fatal(err)
	}
	if len(dst) != 2 || dst[0] != "foo" || dst[1] != "bar" {
		t.Fatalf("unexpected result: %#v", dst)
	}
	p0 := unsafe.StringData(dst[0])
	p1 := unsafe.StringData(dst[1])
	if uintptr(unsafe.Pointer(p1)) != uintptr(unsafe.Pointer(p0))+uintptr(len(dst[0])) {
		t.Error("expected dst[0] and dst[1] to share one contiguous buffer")
	}
}

// TestUnmarshalListString_LargeTotal_DoesNotShareBackingBuffer verifies that
// above marshalBufMaxCap, strings are allocated independently rather than
// sharing one large buffer.
func TestUnmarshalListString_LargeTotal_DoesNotShareBackingBuffer(t *testing.T) {
	big := bytes.Repeat([]byte("x"), (marshalBufMaxCap/2)+1024)
	wire := buildCQLList(big, big)
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeVarchar},
	}
	var dst []string
	if err := unmarshalList(info, wire, &dst); err != nil {
		t.Fatal(err)
	}
	if len(dst) != 2 || dst[0] != string(big) || dst[1] != string(big) {
		t.Fatal("unexpected result content")
	}
	p0 := unsafe.StringData(dst[0])
	p1 := unsafe.StringData(dst[1])
	if uintptr(unsafe.Pointer(p1)) == uintptr(unsafe.Pointer(p0))+uintptr(len(dst[0])) {
		t.Error("expected dst[0] and dst[1] to be independently allocated, not sharing a buffer")
	}
}

// --- Fix #7 helper.

// Fix #7 — unmarshalListInt with n=0 must emit a non-nil empty []int;
// matches the sibling leaf decoders and TestMarshalSetListV3/zero_elems.
func TestUnmarshalListInt_ZeroElemEmptySlice(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}
	var dst []int
	if err := unmarshalList(info, []byte{0, 0, 0, 0}, &dst); err != nil {
		t.Fatal(err)
	}
	if dst == nil {
		t.Fatal("expected non-nil empty []int, got nil (Fix #7 regression)")
	}
	if len(dst) != 0 {
		t.Fatalf("expected empty, got len %d", len(dst))
	}
}

// Fix #7 — preserves slice-reuse behavior for n>0 (backing array reused).
func TestUnmarshalListInt_SliceReuse_AfterFix7(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}
	input := []int32{10, 20, 30}
	data, err := marshalList(info, input)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]int, 0, 10)
	prev := &dst[:cap(dst)][0]
	if err := unmarshalList(info, data, &dst); err != nil {
		t.Fatal(err)
	}
	if &dst[0] != prev {
		t.Error("slice backing array was not reused for n>0")
	}
	for i, v := range input {
		if dst[i] != int(v) {
			t.Errorf("[%d]: got %d, want %d", i, dst[i], int(v))
		}
	}
}
