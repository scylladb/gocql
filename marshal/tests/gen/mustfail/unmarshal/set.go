package unmarshal

import (
	"bytes"

	"github.com/gocql/gocql/marshal/tests/gen"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/run"
	"github.com/gocql/gocql/marshal/tests/run/mustfail"
	"github.com/gocql/gocql/marshal/tests/utils"
)

type Sets []*Set

// Set is a tool for generating test cases of unmarshal funcs.
// For cases when the function should an error.
type Set struct {
	Name string

	Func func([]byte, interface{}) error
	Data []byte
	// UnmarshalIn for tests Data with single UnmarshalIn.
	UnmarshalIn interface{}
	// UnmarshalIns for test Data with multiple UnmarshalIns.
	UnmarshalIns []interface{}

	Mods mod.Mods

	Issue string
	// SoloRun, if true, then only Sets with SoloRun will be run, without skips by IssueMarshal, IssueUnmarshal.
	// This option for easier tuning only. Please remove all SoloRun options after finished tuning.
	SoloRun bool
}

func (s *Set) Copy() *Set {
	return &Set{
		Name:         s.Name,
		Func:         s.Func,
		Data:         bytes.Clone(s.Data),
		UnmarshalIn:  s.UnmarshalIn,
		UnmarshalIns: s.UnmarshalIns,
		Mods:         s.Mods,
		Issue:        s.Issue,
		SoloRun:      s.SoloRun,
	}
}

func (s *Set) Gen() run.Group {
	return gen.FromSet(s)
}

func (s *Set) GetName() string {
	if s.Name == "" {
		panic("name should be provided")
	}
	return s.Name
}

func (s *Set) GetMods() mod.Mods {
	if len(s.Mods) == 0 {
		panic("mods should be provided")
	}
	return s.Mods
}

func (s *Set) GetValues() []interface{} {
	if s.UnmarshalIn != nil {
		s.UnmarshalIns = utils.AppendNotNil(s.UnmarshalIns, s.UnmarshalIn)
		s.UnmarshalIn = nil
	}

	if len(s.UnmarshalIns) == 0 {
		panic("UnmarshalIn or UnmarshalIns should be provided")
	}
	return s.UnmarshalIns
}

func (s *Set) NewCase(name string, val interface{}) run.Test {
	return mustfail.NewUnmarshalCase(name, s.Func, bytes.Clone(s.Data), val, s.getIssue())
}

func (s *Set) IsSoloRun() bool {
	return s.SoloRun
}

func (s *Set) getIssue() string {
	if s.SoloRun {
		return ""
	}
	return s.Issue
}

func (s *Set) putDefault(f func([]byte, interface{}) error, mods mod.Mods) {
	if s.Func == nil {
		s.Func = f
	}
	if s.Mods == nil {
		s.Mods = mods
	}
}
