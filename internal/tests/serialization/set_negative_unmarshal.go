package serialization

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"
)

// NegativeUnmarshalSet is a tool for unmarshal funcs testing for cases when the function should an error.
type NegativeUnmarshalSet struct {
	Data        []byte
	Values      []any
	BrokenTypes []reflect.Type
}

func (s NegativeUnmarshalSet) Run(name string, t *testing.T, unmarshal func([]byte, any) error) {
	if name == "" {
		t.Fatal("name should be provided")
	}
	if unmarshal == nil {
		t.Fatal("unmarshal function should be provided")
	}
	t.Run(name, func(t *testing.T) {
		for m := range s.Values {
			val := s.Values[m]

			if rt := reflect.TypeOf(val); rt.Kind() != reflect.Ptr {
				unmarshalIn := newRef(val)
				s.run(fmt.Sprintf("%T", val), t, unmarshal, val, unmarshalIn)
			} else {
				// Test unmarshal to (*type)(nil)
				unmarshalIn := newRef(val)
				s.run(fmt.Sprintf("%T**nil", val), t, unmarshal, val, unmarshalIn)

				// Test unmarshal to &type{}
				unmarshalInZero := newRefToZero(val)
				s.run(fmt.Sprintf("%T**zero", val), t, unmarshal, val, unmarshalInZero)
			}
		}
	})
}

func (s NegativeUnmarshalSet) run(name string, t *testing.T, f func([]byte, any) error, val, unmarshalIn any) {
	t.Run(name, func(t *testing.T) {
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = panicErr{err: r.(error), stack: debug.Stack()}
				}
			}()
			return f(bytes.Clone(s.Data), unmarshalIn)
		}()

		testFailed := false
		wasPanic := errors.As(err, &panicErr{})
		if err == nil || wasPanic {
			testFailed = true
		}

		if isTypeOf(val, s.BrokenTypes) {
			if testFailed {
				t.Skipf("skipped bacause there is unsolved problem")
			}
			t.Fatalf("expected to panic or no error for (%T), but got an error", unmarshalIn)
		}

		if testFailed {
			if wasPanic {
				t.Fatalf("was panic %s", err)
			}
			t.Errorf("expected an error for (%T), but got no error", unmarshalIn)
		}
	})
}
