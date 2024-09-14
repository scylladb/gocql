package mustfail

import (
	"bytes"
	"errors"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/run"
	"github.com/gocql/gocql/marshal/tests/utils"
)

func NewUnmarshalCase(name string, f func([]byte, interface{}) error, data []byte, unmarshalIn interface{}, issue string) run.Test {
	if name == "" {
		panic("name should be provided")
	}
	if f == nil {
		panic("unmarshal func should be provided")
	}
	if unmarshalIn == nil {
		panic("value for unmarshal input should be provided")
	}
	return &unmarshal{
		name:        name,
		tFunc:       f,
		data:        data,
		unmarshalIn: unmarshalIn,
		issue:       issue,
	}
}

type unmarshal struct {
	name        string
	tFunc       func([]byte, interface{}) error
	data        []byte
	unmarshalIn interface{}
	issue       string
}

func (c unmarshal) Run(t *testing.T, parallel bool) {
	t.Run(c.name, func(tt *testing.T) {
		if parallel {
			tt.Parallel()
		}
		if c.issue != "" {
			tt.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.issue)
		}
		sUnmarshalIn := utils.StringValue(deRef(c.unmarshalIn))

		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = utils.PanicErr{Err: r.(error), Stack: debug.Stack()}
				}
			}()
			return c.tFunc(bytes.Clone(c.data), c.unmarshalIn)
		}()

		if err == nil {
			sData := utils.StringData(c.data)
			unmarshalOut := utils.StringValue(deRef(c.unmarshalIn))

			if len(unmarshalOut) > utils.PrintLimit || len(sUnmarshalIn) > utils.PrintLimit || len(sData) > utils.PrintLimit {
				tt.Error("unmarshal does not return an error")
			} else {
				tt.Errorf("unmarshal does not return an error.\n  tested data:%s\nunmarshal  in:%s\nunmarshal out:%s\n", sData, sUnmarshalIn, unmarshalOut)
			}
		} else if errors.As(err, &utils.PanicErr{}) {
			tt.Errorf("was panic: %s", err)
		}
	})

}

func deRef(in interface{}) interface{} {
	return utils.DeReference(in)
}
