package serialization

import (
	"bytes"

	"github.com/gocql/gocql/marshal/tests/gen"
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/run"
	"github.com/gocql/gocql/marshal/tests/run/serialization"
	"github.com/gocql/gocql/marshal/tests/utils"
)

type Sets []*Set

// Set is a tool for generating test cases of marshal and unmarshall funcs.
// For cases when the function should no error,
// marshaled data from Set.Values should be equal with Set.Data,
// unmarshalled value from Set.Data should be equal with Set.Values.
type Set struct {
	Name  string
	Funcs *funcs.Funcs
	Mods  mod.Mods
	Data  []byte
	// Value for tests Data with single Value.
	Value interface{}
	// Values for test Data with multiple Values.
	Values []interface{}

	IssueMarshal   string
	IssueUnmarshal string

	// SoloRun, if true, then only Sets with SoloRun will be run, without skips by IssueMarshal, IssueUnmarshal.
	// This option for easier tuning only. Please remove all SoloRun options after finished tuning.
	SoloRun bool
}

func (s *Set) Copy() *Set {
	return &Set{
		Name:           s.Name,
		Funcs:          s.Funcs.Copy(),
		Mods:           s.Mods,
		Data:           bytes.Clone(s.Data),
		Value:          s.Value,
		Values:         s.Values,
		IssueMarshal:   s.IssueMarshal,
		IssueUnmarshal: s.IssueUnmarshal,
		SoloRun:        s.SoloRun,
	}
}

func (s *Set) Gen() run.Group {
	return gen.FromSet(s)
}

func (s *Set) GetName() string {
	if s.Name == "" {
		s.Name = utils.StringData(s.Data)
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
	if s.Value != nil {
		s.Values = utils.AppendNotNil(s.Values, s.Value)
		s.Value = nil
	}

	if len(s.Values) == 0 {
		panic("Value or Values should be provided")
	}
	return s.Values
}

func (s *Set) NewCase(name string, val interface{}) run.Test {
	return serialization.NewTest(name, *s.Funcs.Copy(), bytes.Clone(s.Data), val, s.getIssueMarshal(), s.getIssueUnmarshal())
}

func (s *Set) IsSoloRun() bool {
	return s.SoloRun
}

func (s *Set) getIssueMarshal() string {
	if s.SoloRun {
		return ""
	}
	return s.IssueMarshal
}

func (s *Set) getIssueUnmarshal() string {
	if s.SoloRun {
		return ""
	}
	return s.IssueUnmarshal
}

func (s *Set) putDefault(f *funcs.Funcs, mods mod.Mods) {
	if s.Funcs == nil {
		s.Funcs = f.Copy()
	}
	s.Funcs.PutDefaults(f)

	if s.Mods == nil {
		s.Mods = mods
	}
}
