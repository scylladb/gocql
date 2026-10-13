//go:build unit
// +build unit

package gocql

import (
	"errors"
	"testing"
	"unsafe"
)

// tornString returns a string whose header has a nil data pointer but a non-zero
// length, as produced by a racing write to a string field (issue #1160).
func tornString(n int) string {
	var s string
	(*struct {
		p unsafe.Pointer
		n int
	})(unsafe.Pointer(&s)).n = n
	return s
}

type tornUDT struct{ name string }

func (u tornUDT) MarshalUDT(name string, info TypeInfo) ([]byte, error) {
	return Marshal(info, u.name)
}

// TestMarshalTornStringIssue1160 mirrors the scylla-manager crash: list<UDT> whose
// MarshalUDT marshals a torn string. Marshal must return an error, not panic.
func TestMarshalTornStringIssue1160(t *testing.T) {
	info := CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem: UDTTypeInfo{
			NativeType: NativeType{proto: protoVersion4, typ: TypeUDT},
			Name:       "view",
			Elements: []UDTField{
				{Name: "name", Type: NativeType{proto: protoVersion4, typ: TypeVarchar}},
			},
		},
	}
	value := []tornUDT{{name: tornString(5)}}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Marshal panicked instead of returning an error: %v", r)
		}
	}()
	_, err := Marshal(info, value)
	if !errors.As(err, &MarshalError{}) {
		t.Fatalf("expected a MarshalError marshaling a torn string, got %v", err)
	}
}
