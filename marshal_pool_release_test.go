//go:build all || unit

package gocql

import (
	"testing"
	"unsafe"
)

// The buffer pool's safety rests on one invariant: a buffer is returned to
// marshalOutputPool only if a pooled fast path actually produced it. That is a
// property of the code path taken, not of the column's type — the reflect
// path, pointer values and user Marshalers all yield non-pooled buffers for
// columns whose TypeInfo says "poolable". Returning one of those would hand
// getMarshalOutput memory that is still referenced elsewhere, and in the
// Marshaler case memory the caller owns.
//
// These tests pin both halves: that marshalQueryValue sets the flag from the
// path it took, and that the release path honours the flag rather than
// re-deriving poolability from the schema.

// captureReleases swaps the pool-return seam for a recorder, so a test can see
// exactly which buffers a release path handed back. Not parallel-safe.
func captureReleases(t *testing.T) *[][]byte {
	t.Helper()
	released := &[][]byte{}
	prev := putPooledOutput
	putPooledOutput = func(buf []byte) { *released = append(*released, buf) }
	t.Cleanup(func() { putPooledOutput = prev })
	return released
}

func sameBuffer(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return cap(a) == cap(b) && len(a) == len(b)
	}
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}

// listOfInt is a poolable column type: pooledMarshalType reports true for it,
// so a schema-driven release would try to recycle every value bound to it.
func listOfInt() CollectionType {
	return CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: TypeInt},
	}
}

type marshalerInts []int32

func (m marshalerInts) MarshalCQL(info TypeInfo) ([]byte, error) {
	return Marshal(info, []int32(m))
}

// TestMarshalQueryValuePooledFlagMatchesPathTaken is the regression guard: for
// every value below the column type is poolable, so an implementation that
// predicted from TypeInfo would mark them all pooled. Only the concrete
// fast-path slice actually comes from the pool.
func TestMarshalQueryValuePooledFlagMatchesPathTaken(t *testing.T) {
	ints := []int32{1, 2, 3}

	cases := []struct {
		name       string
		value      any
		wantPooled bool
	}{
		{"fast path []int32", ints, true},
		{"fast path []int", []int{1, 2, 3}, true},
		// Reflect path: no fast path for []int8 elements.
		{"reflect path []int8", []int8{1, 2, 3}, false},
		// Marshal dereferences the pointer and the fast path does produce a
		// pooled buffer, but pooledMarshalValue type-asserts on the pointer
		// and declines. Losing the optimisation is safe; recycling is not.
		{"pointer to slice", &ints, false},
		// User-owned memory: must never be recycled.
		{"user Marshaler", marshalerInts(ints), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var dst queryValues
			if err := marshalQueryValue(listOfInt(), tc.value, &dst); err != nil {
				t.Fatalf("marshalQueryValue: %v", err)
			}
			if dst.pooled != tc.wantPooled {
				t.Fatalf("pooled = %v, want %v (column type is poolable, so this "+
					"fails if poolability is predicted from TypeInfo)", dst.pooled, tc.wantPooled)
			}
		})
	}

	// The premise of the test: TypeInfo alone says "poolable" for all of them.
	if !pooledMarshalType(listOfInt()) {
		t.Fatal("list<int> is expected to be a poolable column type; test premise is stale")
	}
}

// TestReleasePooledValuesHonoursFlag checks the release path returns exactly
// the flagged buffers — not every buffer, and not those on poolable columns
// that took a non-pooled path.
func TestReleasePooledValuesHonoursFlag(t *testing.T) {
	released := captureReleases(t)

	pooledBuf := []byte{1, 2, 3}
	plainBuf := []byte{4, 5, 6}
	vals := []queryValues{
		{value: pooledBuf, pooled: true},
		{value: plainBuf, pooled: false},
		{value: nil, pooled: false},
	}

	releasePooledValues(vals)

	if len(*released) != 1 {
		t.Fatalf("released %d buffers, want exactly 1", len(*released))
	}
	if !sameBuffer((*released)[0], pooledBuf) {
		t.Fatal("released the wrong buffer")
	}
	for i := range vals {
		if vals[i].pooled {
			t.Fatalf("value %d left flagged after release", i)
		}
	}
}

// TestReleasePooledValuesIsIdempotent guards the double-return case: a buffer
// handed back twice is handed to two callers by sync.Pool, which corrupts
// both. Clearing the flag on release is what prevents it.
func TestReleasePooledValuesIsIdempotent(t *testing.T) {
	released := captureReleases(t)

	vals := []queryValues{{value: []byte{1, 2, 3}, pooled: true}}
	releasePooledValues(vals)
	releasePooledValues(vals)

	if len(*released) != 1 {
		t.Fatalf("released %d times, want 1 — the same buffer must not be pooled twice", len(*released))
	}
}

// TestReleasePooledValuesEndToEnd runs the real marshal step and then the real
// release step over a mixed set of values, which is the combination neither
// half covers alone.
func TestReleasePooledValuesEndToEnd(t *testing.T) {
	released := captureReleases(t)

	values := []any{
		[]int32{1, 2, 3},    // pooled fast path
		[]int8{4, 5},        // reflect path, same poolable column type
		marshalerInts{6, 7}, // user-owned memory
	}
	vals := make([]queryValues, len(values))
	for i, v := range values {
		if err := marshalQueryValue(listOfInt(), v, &vals[i]); err != nil {
			t.Fatalf("marshalQueryValue(%d): %v", i, err)
		}
	}

	want := vals[0].value
	releasePooledValues(vals)

	if len(*released) != 1 {
		t.Fatalf("released %d buffers, want exactly 1 (only the fast-path value was pooled)", len(*released))
	}
	if !sameBuffer((*released)[0], want) {
		t.Fatal("released a buffer that did not come from the pool")
	}
}
