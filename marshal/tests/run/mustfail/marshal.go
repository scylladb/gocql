package mustfail

import (
	"errors"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/run"
	"github.com/gocql/gocql/marshal/tests/utils"
)

func NewMarshalCase(name string, f func(interface{}) ([]byte, error), marshalIn interface{}, issue string) run.Test {
	if name == "" {
		panic("name should be provided")
	}
	if f == nil {
		panic("marshal func should be provided")
	}
	if marshalIn == nil {
		panic("value for marshal input should be provided")
	}
	return marshal{
		name:      name,
		tFunc:     f,
		marshalIn: marshalIn,
		issue:     issue,
	}
}

type marshal struct {
	name      string
	tFunc     func(interface{}) ([]byte, error)
	marshalIn interface{}
	issue     string
}

func (c marshal) Run(t *testing.T, parallel bool) {
	t.Run(c.name, func(tt *testing.T) {
		if c.issue != "" {
			tt.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.issue)
		}
		if parallel {
			tt.Parallel()
		}

		data, err := func() (d []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = utils.PanicErr{Err: r.(error), Stack: debug.Stack()}
				}
			}()
			return c.tFunc(c.marshalIn)
		}()

		if err == nil {
			val := utils.StringValue(c.marshalIn)
			if len(val) > utils.PrintLimit || len(data) > utils.PrintLimit {
				tt.Error("marshal does not return error")
			} else {
				tt.Errorf("marshal does not return error.\nvalue:%s\nreceived data:%x", val, data)
			}
		} else if errors.As(err, &utils.PanicErr{}) {
			tt.Errorf("was panic: %s", err)
		}
	})
}
