package serialization

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/utils"
)

func newUnmarshal(f funcs.Funcs, val interface{}, data []byte, issue string) unmarshal {
	return unmarshal{
		tFunc:     f.Unmarshal,
		equal:     f.EqualVals,
		inputData: data,
		inputVal:  f.NewVar(val),
		expected:  val,
		issue:     issue,
	}
}

type unmarshal struct {
	tFunc func([]byte, interface{}) error
	equal func(in1, in2 interface{}) bool

	inputData []byte
	inputVal  interface{}

	expected interface{}
	issue    string
}

func (c unmarshal) run(t *testing.T, parallel bool) {
	if funcs.IsExcludedUnmarshal(c.tFunc) {
		return
	}

	t.Run("unmarshal", func(tt *testing.T) {
		if c.issue != "" {
			tt.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.issue)
		}
		if parallel {
			tt.Parallel()
		}

		inValStr := valStr(deRef(c.inputVal))
		inValPtr := ptrStr(c.inputVal)

		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = utils.PanicErr{Err: r.(error), Stack: debug.Stack()}
				}
			}()
			return c.tFunc(c.inputData, c.inputVal)
		}()
		if err != nil {
			tt.Error(err)
			return
		}

		outValStr := valStr(deRef(c.inputVal))
		if outValPtr := ptrStr(c.inputVal); inValPtr != "" && outValPtr != "" && inValPtr != outValPtr {
			tt.Errorf("unmarshal function rewrites existing pointer")
		}

		result := ""
		if expectedStr := valStr(c.expected); len(expectedStr)+len(inValStr)+len(outValStr) < utils.PrintLimit*3 {
			result = fmt.Sprintf("\n     expected:%s\nunmarshal  in:%s\nunmarshal out:%s", expectedStr, inValStr, outValStr)
		}

		if !c.equal(c.expected, deRef(c.inputVal)) {
			tt.Error("expected and received values are not equal" + result)
		} else {
			tt.Log("test done" + result)
		}
	})
}
