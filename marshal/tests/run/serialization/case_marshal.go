package serialization

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/utils"
)

func newMarshal(f funcs.Funcs, val interface{}, data []byte, i string) marshal {
	return marshal{
		tFunc:    f.Marshal,
		equal:    f.EqualData,
		input:    val,
		expected: data,
		issue:    i,
	}
}

type marshal struct {
	tFunc func(interface{}) ([]byte, error)
	equal func(in1, in2 []byte) bool

	input    interface{}
	expected []byte
	issue    string
}

func (c marshal) run(t *testing.T, parallel bool) {
	if funcs.IsExcludedMarshal(c.tFunc) {
		return
	}
	t.Run("  marshal", func(tt *testing.T) {
		if c.issue != "" {
			tt.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.issue)
		}
		if parallel {
			tt.Parallel()
		}

		received, err := func() (d []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = utils.PanicErr{Err: r.(error), Stack: debug.Stack()}
				}
			}()
			return c.tFunc(c.input)
		}()

		result := ""
		if inStr := valStr(c.input); len(inStr)+len(c.expected)+len(received) < utils.PrintLimit*3 {
			result = fmt.Sprintf("\n marshal   in:%s\nexpected data:%s\nreceived data:%s\n", valStr(c.input), dataStr(c.expected), dataStr(received))
		}

		switch {
		case err != nil:
			tt.Error(err)
		case !c.equal(c.expected, received):
			tt.Error("expected and received data are not equal" + result)
		default:
			tt.Log("test done" + result)
		}
	})
}
