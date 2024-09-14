package marshal

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/run"
	"github.com/gocql/gocql/marshal/tests/run/mustfail"
	"github.com/gocql/gocql/marshal/tests/utils"
)

type Sets []*Set

// Set is a tool for generating test cases of marshal funcs.
// For cases when the function should an error.
type Set struct {
	Name      string
	Func      func(interface{}) ([]byte, error)
	MarshalIn interface{}
	// MarshalIns for test Data with multiple MarshalIns.
	MarshalIns []interface{}
	Mods       mod.Mods

	Issue string
	// SoloRun, if true, then only Sets with SoloRun will be run, without skips by IssueMarshal, IssueUnmarshal.
	// This option for easier tuning only. Please remove all SoloRun options after finished tuning.
	SoloRun bool
}

func (s *Set) Copy() *Set {
	return &Set{
		Name:       s.Name,
		Func:       s.Func,
		MarshalIn:  s.MarshalIn,
		MarshalIns: s.MarshalIns,
		Mods:       s.Mods,
		Issue:      s.Issue,
		SoloRun:    s.SoloRun,
	}
}

func (s *Set) Gen(t *testing.T) run.Group {
	if s.SoloRun {
		t.Error(utils.SoloRunMsg)
	}
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
	if s.MarshalIn != nil {
		s.MarshalIns = utils.AppendNotNil(s.MarshalIns, s.MarshalIn)
		s.MarshalIn = nil
	}

	if len(s.MarshalIns) == 0 {
		panic("MarshalIn or MarshalIns should be provided")
	}
	return s.MarshalIns
}

func (s *Set) NewCase(name string, val interface{}) run.Test {
	return mustfail.NewMarshalCase(name, s.Func, val, s.getIssue())
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

func (s *Set) putDefault(f func(interface{}) ([]byte, error), mods mod.Mods) {
	if s.Func == nil {
		s.Func = f
	}
	if s.Mods == nil {
		s.Mods = mods
	}
}
