package tests

import (
	"bytes"
	"errors"
	"runtime/debug"
	"testing"

	"github.com/gocql/gocql/marshal/tests/mods"
)

type MustFailUnmarshal struct {
	Name string

	Func func([]byte, interface{}) error
	Data []byte
	// UnmarshalIn for tests Data with single UnmarshalIn.
	UnmarshalIn interface{}
	// UnmarshalIns for test Data with multiple UnmarshalIns.
	UnmarshalIns []interface{}

	Mods mods.Mods

	Issue string
	// SoloRun, if true, then only this Case will be run, without skips by IssueMarshal, IssueUnmarshal.
	// This option for easier tuning only. Please remove all SoloRun options after finished tuning.
	SoloRun bool
}

func (c *MustFailUnmarshal) GetModified() *UnmarshalCases {
	c.UnmarshalIns = appendNotNil(c.UnmarshalIns, c.UnmarshalIn)
	c.UnmarshalIn = nil

	return &UnmarshalCases{
		Func:     c.Func,
		Cases:    c.getModified(),
		modified: true,
	}
}

func (c *MustFailUnmarshal) Copy() *MustFailUnmarshal {
	return &MustFailUnmarshal{
		Name:         c.Name,
		Func:         c.Func,
		Data:         bytes.Clone(c.Data),
		UnmarshalIn:  c.UnmarshalIn,
		UnmarshalIns: c.UnmarshalIns,
		Mods:         c.Mods,
		Issue:        c.Issue,
		SoloRun:      c.SoloRun,
	}
}

func (c *MustFailUnmarshal) Run(t *testing.T) {
	if c.Name == "" {
		t.Fatal("name should be provided")
	}
	if c.Func == nil {
		t.Fatal("unmarshal func should be provided")
	}
	if len(c.Mods) > 0 {
		t.Fatal("to run case with Mods should GetModified fist")
	}

	c.UnmarshalIns = appendNotNil(c.UnmarshalIns, c.UnmarshalIn)
	c.UnmarshalIn = nil

	if len(c.UnmarshalIns) == 0 {
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

		if len(c.UnmarshalIns) == 1 {
			c.runValue(tt, c.UnmarshalIns[0])
			return
		}

		for v := range c.UnmarshalIns {
			val := c.UnmarshalIns[v]
			name := stringValLimited(100, val)

			tt.Run(name, func(ttt *testing.T) {
				c.runValue(ttt, val)
			})
		}
	})
}

func (c *MustFailUnmarshal) runValue(t *testing.T, value interface{}) {
	if value == nil {
		t.Fatal("value should be not nil interface")
	}

	unmarshalIn := stringVal(deRef(value))

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = wasPanic{p: r.(error), s: debug.Stack()}
			}
		}()
		return c.Func(bytes.Clone(c.Data), value)
	}()

	if err == nil {
		sData := stringData(c.Data)
		unmarshalOut := stringVal(deRef(value))

		if len(unmarshalOut) > printLimit || len(unmarshalIn) > printLimit || len(sData) > printLimit {
			t.Error("unmarshal does not return an error")
		} else {
			t.Errorf("unmarshal does not return an error.\n  tested data:%s\nunmarshal  in:%s\nunmarshal out:%s\n", sData, unmarshalIn, unmarshalOut)
		}
	} else if errors.As(err, &wasPanic{}) {
		t.Errorf("was panic: %s", err)
	}
}

func (c *MustFailUnmarshal) getModified() []*MustFailUnmarshal {
	allCases := make([]*MustFailUnmarshal, 0)
	for _, mod := range c.Mods {
		modified := mod.Apply(c.UnmarshalIns)
		if len(modified) == 0 {
			continue
		}

		allCases = append(allCases,
			&MustFailUnmarshal{
				Name:         c.Name + mod.Suffix(),
				Func:         c.Func,
				Data:         bytes.Clone(c.Data),
				UnmarshalIns: modified,
				Issue:        c.Issue,
				SoloRun:      c.SoloRun,
			},
		)
	}
	return allCases
}

func (c *MustFailUnmarshal) putDefaults(f func([]byte, interface{}) error, mods mods.Mods) {
	if c.Func == nil {
		c.Func = f
	}
	if c.Mods == nil {
		c.Mods = mods
	}
}
