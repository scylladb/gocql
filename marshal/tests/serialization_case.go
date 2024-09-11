package tests

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

type Case struct {
	Name  string
	Funcs *funcs.List
	Mods  mods.Mods
	Data  []byte
	// Value for tests Data with single Value.
	Value interface{}
	// Values for test Data with multiple Values.
	Values []interface{}

	IssueMarshal   string
	IssueUnmarshal string

	// SoloRun, if true, then only this Case will be run, without skips by IssueMarshal, IssueUnmarshal.
	// This option for easier tuning only. Please remove all SoloRun options after finished tuning.
	SoloRun bool
}

func (c *Case) Copy() *Case {
	return &Case{
		Name:           c.Name,
		Funcs:          c.Funcs.Copy(),
		Mods:           c.Mods,
		Data:           bytes.Clone(c.Data),
		Value:          c.Value,
		Values:         c.Values,
		IssueMarshal:   c.IssueMarshal,
		IssueUnmarshal: c.IssueUnmarshal,
		SoloRun:        c.SoloRun,
	}
}

func (c *Case) GetModified() *Serialization {
	c.Values = appendNotNil(c.Values, c.Value)
	c.Value = nil

	return &Serialization{
		Funcs:    c.Funcs.Copy(),
		Cases:    c.getModified(),
		modified: true,
	}
}

func (c *Case) Run(t *testing.T) {
	if len(c.Mods) > 0 {
		t.Fatal("to run case with Mods should GetModified fist")
	}
	if c.Funcs == nil || !c.Funcs.Valid() {
		t.Fatal("all funcs should be provided")
	}

	c.Values = appendNotNil(c.Values, c.Value)
	c.Value = nil

	if len(c.Values) == 0 {
		t.Run(c.getName(), func(tt *testing.T) {
			tt.Fatal("nothing to run")
		})
	}

	t.Run(c.getName(), func(tt *testing.T) {
		if c.SoloRun {
			tt.Error(soloRunMsg)
			c.IssueMarshal = ""
			c.IssueUnmarshal = ""
		}

		if len(c.Values) == 1 {
			c.runValue(tt, c.Values[0])
			return
		}

		for v := range c.Values {
			val := c.Values[v]
			name := stringValLimited(100, val)

			tt.Run(name, func(ttt *testing.T) {
				c.runValue(ttt, val)
			})
		}
	})
}

func (c *Case) runValue(t *testing.T, value interface{}) {
	if value == nil {
		t.Fatal("value should be not nil interface")
	}
	if !c.Funcs.IsExcludedMarshal() {
		t.Run("  marshal", func(tt *testing.T) {
			c.runMarshal(tt, value)
		})
	}
	if !c.Funcs.IsExcludedUnmarshal() {
		t.Run("unmarshal", func(tt *testing.T) {
			c.runUnmarshal(tt, value)
		})
	}
}

func (c *Case) runMarshal(t *testing.T, val interface{}) {
	if c.IssueMarshal != "" {
		t.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.IssueMarshal)
	}

	received, err := func() (d []byte, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = wasPanic{p: r.(error), s: debug.Stack()}
			}
		}()
		return c.Funcs.Marshal(val)
	}()

	expected := bytes.Clone(c.Data)
	expectedStr := string(expected)
	receivedStr := string(received)
	switch {
	case err != nil:
		t.Error(err)
	case !c.Funcs.EqualData(expected, received):
		t.Errorf("expected and received data are not equal.\nexpected:%s\nreceived:%s\n", expectedStr, receivedStr)
	default:
		t.Logf("test done:\n marshal   in:%s\nexpected data:%s\nreceived data:%s\n", stringVal(val), expectedStr, receivedStr)
	}
}

func (c *Case) runUnmarshal(t *testing.T, expected interface{}) {
	if c.IssueUnmarshal != "" {
		t.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.IssueUnmarshal)
	}

	received := c.Funcs.NewVar(expected)
	unmarshalInStr := stringVal(deRef(received))
	expectedPtr := getPtr(received)

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = wasPanic{p: r.(error), s: debug.Stack()}
			}
		}()
		return c.Funcs.Unmarshal(bytes.Clone(c.Data), received)
	}()
	if err != nil {
		t.Error(err)
		return
	}

	if receivedPtr := getPtr(received); expectedPtr != 0 && receivedPtr != 0 && expectedPtr != receivedPtr {
		t.Errorf("unmarshal function rewrites existing pointer on value")
	}

	result := fmt.Sprintf("\n     expected:%s\nunmarshal  in:%s\nunmarshal out:%s", stringVal(expected), unmarshalInStr, stringVal(deRef(received)))

	if !c.Funcs.EqualVals(deRef(received), expected) {
		t.Error("expected and received values are not equal" + result)
	} else {
		t.Log("test done:" + result)
	}
}

func (c *Case) getName() string {
	if c.Name != "" {
		return c.Name
	}
	return stringData(c.Data)
}

func (c *Case) getModified() []*Case {
	allCases := make([]*Case, 0)
	for _, mod := range c.Mods {
		modified := mod.Apply(c.Values)
		if len(modified) == 0 {
			continue
		}

		allCases = append(allCases,
			&Case{
				Name:           c.Name + mod.Suffix(),
				Funcs:          c.Funcs,
				Data:           bytes.Clone(c.Data),
				Values:         modified,
				IssueMarshal:   c.IssueMarshal,
				IssueUnmarshal: c.IssueUnmarshal,
				SoloRun:        c.SoloRun,
			},
		)
	}
	return allCases
}

func (c *Case) putDefaults(f *funcs.List, mods mods.Mods) {
	if c.Funcs == nil {
		c.Funcs = &funcs.List{}
	}
	c.Funcs.PutDefaults(f)

	if c.Mods == nil {
		c.Mods = mods
	}
}
