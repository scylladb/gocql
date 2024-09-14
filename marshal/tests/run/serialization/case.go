package serialization

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/run"
)

func NewTest(name string, f funcs.Funcs, data []byte, val interface{}, issueM, issueUm string) run.Test {
	if name == "" {
		panic("name should be provided")
	}
	if val == nil {
		panic("value should be provided")
	}
	if !f.Valid() {
		panic("provided Funcs should be valid")
	}
	return &tCase{
		name:      name,
		marshal:   newMarshal(f, val, data, issueM),
		unmarshal: newUnmarshal(f, val, data, issueUm),
	}
}

type tCase struct {
	name      string
	marshal   marshal
	unmarshal unmarshal
}

func (c *tCase) Run(t *testing.T, parallel bool) {
	t.Run(c.name, func(tt *testing.T) {
		if parallel {
			t.Parallel()
		}
		c.marshal.run(tt, parallel)
		c.unmarshal.run(tt, parallel)
	})
}
