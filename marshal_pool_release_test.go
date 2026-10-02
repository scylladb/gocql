//go:build all || unit

package gocql

import (
	"context"
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
// This test pins that marshalQueryValue sets the flag from the path it took;
// the release loops only recycle flagged buffers.

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
		// marshalQueryValue checks poolability against the dereferenced value.
		{"pointer to slice", &ints, true},
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

// drainOutputPool empties marshalOutputPool and returns the backing arrays it held.
func drainOutputPool() map[*byte]int {
	seen := map[*byte]int{}
	for i := 0; i < 1024; i++ {
		bp := marshalOutputPool.Get()
		if bp == nil {
			break
		}
		if b := bp.([]byte); cap(b) > 0 {
			seen[unsafe.SliceData(b)]++
		}
	}
	return seen
}

// TestExecuteQueryReleasesPooledBufferOnce runs a pooled value through a real
// prepared EXECUTE: the buffer must be handed back at most once (the explicit
// release and the deferred safety net must not both fire), and a user
// Marshaler's buffer must never reach the pool.
func TestExecuteQueryReleasesPooledBufferOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewTestServer(t, protoVersion4, ctx)
	defer srv.Stop()
	db, err := testCluster(protoVersion4, srv.Address).CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	userOwned := marshalerInts{4, 5, 6}
	for _, v := range []any{[]int32{1, 2, 3}, userOwned} {
		drainOutputPool()
		if err := db.Query("select listint", v).Exec(); err != nil {
			t.Fatalf("exec %T: %v", v, err)
		}
		for ptr, n := range drainOutputPool() {
			if n > 1 {
				t.Fatalf("%T: buffer %p released %d times", v, ptr, n)
			}
		}
	}
	// A Marshaler's output is its own allocation; it can only be in the pool if
	// the release path recycled memory it did not get from the pool.
	userBuf, err := Marshal(listOfInt(), userOwned)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Query("select listint", userOwned).Exec(); err != nil {
		t.Fatal(err)
	}
	for ptr := range drainOutputPool() {
		if ptr == unsafe.SliceData(userBuf) {
			t.Fatal("user Marshaler buffer was released to the pool")
		}
	}
}
