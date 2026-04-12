package serialization

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"
)

// PositiveSet is a tool for marshal and unmarshall funcs testing for cases when the function should no error,
// on marshal - marshaled data from PositiveSet.Values should be equal with PositiveSet.Data,
// on unmarshall - unmarshalled value from PositiveSet.Data should be equal with PositiveSet.Values.
type PositiveSet struct {
	Data   []byte
	Values []any

	BrokenMarshalTypes   []reflect.Type
	BrokenUnmarshalTypes []reflect.Type
}

func (s PositiveSet) Run(name string, t *testing.T, marshal func(any) ([]byte, error), unmarshal func([]byte, any) error) {
	if name == "" {
		t.Fatal("name should be provided")
	}

	t.Run(name, func(t *testing.T) {
		for i := range s.Values {
			val := s.Values[i]

			t.Run(fmt.Sprintf("%T", val), func(t *testing.T) {
				if marshal != nil {
					s.runMarshalTest(t, marshal, val)
				}

				if unmarshal != nil {
					if rt := reflect.TypeOf(val); rt.Kind() != reflect.Ptr {
						unmarshalIn := newRef(val)
						s.runUnmarshalTest("unmarshal", t, unmarshal, val, unmarshalIn)
					} else {
						// Test unmarshal to (*type)(nil)
						unmarshalIn := newRef(val)
						s.runUnmarshalTest("unmarshal**nil", t, unmarshal, val, unmarshalIn)

						// Test unmarshal to &type{}
						unmarshalInZero := newRefToZero(val)
						s.runUnmarshalTest("unmarshal**zero", t, unmarshal, val, unmarshalInZero)
					}
				}
			})
		}
	})
}

func (s PositiveSet) runMarshalTest(t *testing.T, f func(any) ([]byte, error), val any) {
	t.Run("marshal", func(t *testing.T) {

		result, err := func() (d []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = panicErr{err: r.(error), stack: debug.Stack()}
				}
			}()
			return f(val)
		}()

		expected := bytes.Clone(s.Data)
		if err != nil {
			if !errors.As(err, &panicErr{}) {
				err = errors.Join(marshalErr, err)
			}
		} else if !equalData(expected, result) {
			err = unequalError{Expected: stringData(s.Data), Got: stringData(result)}
		}

		if isTypeOf(val, s.BrokenMarshalTypes) {
			if err == nil {
				t.Fatalf("expected to fail for (%T), but did not fail", val)
			}
			t.Skipf("skipped bacause there is unsolved problem")
		}
		if err != nil {
			t.Error(err)
		}
	})
}

func (s PositiveSet) runUnmarshalTest(name string, t *testing.T, f func([]byte, any) error, expected, result any) {
	t.Run(name, func(t *testing.T) {

		expectedPtr := getPointers(result)

		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = panicErr{err: fmt.Errorf("%s", r), stack: debug.Stack()}
				}
			}()
			return f(bytes.Clone(s.Data), result)
		}()

		if err != nil {
			if !errors.As(err, &panicErr{}) {
				err = errors.Join(unmarshalErr, err)
			}
		} else if !equalVals(expected, deReference(result)) {
			err = unequalError{Expected: stringValue(expected), Got: stringValue(deReference(result))}
		} else {
			err = expectedPtr.Valid(result)
		}

		if isTypeOf(expected, s.BrokenUnmarshalTypes) {
			if err == nil {
				t.Fatalf("expected to fail for (%T), but did not fail", expected)
			}
			t.Skipf("skipped bacause there is unsolved problem")
		}
		if err != nil {
			t.Error(err)
		}
	})
}
