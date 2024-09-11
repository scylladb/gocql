package tests

import (
	"errors"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/mods"
)

type MustFailMarshal struct {
	Name string

	Func func(interface{}) ([]byte, error)
	// MarshalIn for tests Data with single MarshalIn.
	MarshalIn interface{}
	// MarshalIns for test Data with multiple MarshalIns.
	MarshalIns []interface{}

	Mods mods.Mods

	Issue string
	// SoloRun, if true, then only this Case will be run, without skips by IssueMarshal, IssueUnmarshal.
	// This option for easier tuning only. Please remove all SoloRun options after finished tuning.
	SoloRun bool
}

func (c *MustFailMarshal) GetModified() *MarshalCases {
	c.MarshalIns = appendNotNil(c.MarshalIns, c.MarshalIn)
	c.MarshalIn = nil

	return &MarshalCases{
		Func:     c.Func,
		Cases:    c.getModified(),
		modified: true,
	}
}

func (c *MustFailMarshal) Copy() *MustFailMarshal {
	return &MustFailMarshal{
		Name:       c.Name,
		Func:       c.Func,
		MarshalIn:  c.MarshalIn,
		MarshalIns: c.MarshalIns,
		Mods:       c.Mods,
		Issue:      c.Issue,
		SoloRun:    c.SoloRun,
	}
}

func (c *MustFailMarshal) Run(t *testing.T) {
	if c.Name == "" {
		t.Fatal("name should be provided")
	}
	if c.Func == nil {
		t.Fatal("marshal func should be provided")
	}
	if len(c.Mods) > 0 {
		t.Fatal("to run case with Mods should to run GetModified fist")
	}

	c.MarshalIns = appendNotNil(c.MarshalIns, c.MarshalIn)
	c.MarshalIn = nil

	if len(c.MarshalIns) == 0 {
		t.Run(c.Name, func(tt *testing.T) {
			t.Fatal("nothing to run ")
		})
	}

	t.Run(c.Name, func(tt *testing.T) {
		if c.SoloRun {
			tt.Error(soloRunMsg)
		} else if c.Issue != "" {
			tt.Skipf("\nskipped bacause there is unsolved issue:\n%s", c.Issue)
		}

		if len(c.MarshalIns) == 1 {
			c.runValue(tt, c.MarshalIns[0])
			return
		}

		for v := range c.MarshalIns {
			val := c.MarshalIns[v]
			name := stringValLimited(100, val)

			tt.Run(name, func(ttt *testing.T) {
				c.runValue(ttt, val)
			})
		}
	})
}

func (c *MustFailMarshal) runValue(t *testing.T, val interface{}) {
	if val == nil {
		t.Fatal("value should be not nil interface")
	}

	data, err := func() (d []byte, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = wasPanic{p: r.(error), s: debug.Stack()}
			}
		}()
		return c.Func(val)
	}()

	if err == nil {
		if len(data) <= printLimit {
			t.Errorf("marshal does not return error.\nreceived data:%x", data)
		} else {
			t.Errorf("marshal does not return error.\nreceived data:%x...%x", data[:100], data[len(data)-100:])
		}
	} else if errors.As(err, &wasPanic{}) {
		t.Errorf("was panic: %s", err)
	}
}

func (c *MustFailMarshal) getModified() []*MustFailMarshal {
	allCases := make([]*MustFailMarshal, 0)
	for _, mod := range c.Mods {
		modified := mod.Apply(c.MarshalIns)
		if len(modified) == 0 {
			continue
		}

		allCases = append(allCases,
			&MustFailMarshal{
				Name:       c.Name + mod.Suffix(),
				Func:       c.Func,
				MarshalIns: modified,
				Issue:      c.Issue,
				SoloRun:    c.SoloRun,
			},
		)
	}
	return allCases
}

func (c *MustFailMarshal) putDefaults(f func(interface{}) ([]byte, error), mods mods.Mods) {
	if c.Func == nil {
		c.Func = f
	}
	if c.Mods == nil {
		c.Mods = mods
	}
}
