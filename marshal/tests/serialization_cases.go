package tests

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

// Serialization is a tool for testing `marshaling` and `unmarshalling` functions for cases where there should be no error,
// marshaled data from Value should be equal with Data, unmarshalled value from Data should be equal with Value.
type Serialization struct {
	Funcs       *funcs.List
	DefaultMods mods.Mods
	Cases       []*Case
	modified    bool
}

func (l *Serialization) GetModified() *Serialization {
	return &Serialization{
		Funcs:    l.Funcs.Copy(),
		Cases:    l.getModified(),
		modified: true,
	}
}

func (l *Serialization) Copy() *Serialization {
	out := &Serialization{
		Funcs:       l.Funcs.Copy(),
		DefaultMods: l.DefaultMods,
		Cases:       make([]*Case, 0, len(l.Cases)),
	}
	for i := range l.Cases {
		l.Cases[i].putDefaults(l.Funcs.Copy(), l.DefaultMods)
		out.Cases = append(out.Cases, l.Cases[i].Copy())
	}
	return out
}

func (l *Serialization) Run(t *testing.T) {
	if len(l.Cases) == 0 {
		t.Fatal("nothing to run")
	}

	if l.Funcs == nil || !l.Funcs.Valid() {
		t.Fatal("all funcs should be provided")
	}

	if !l.modified {
		t.Fatal("run GetModified fist")
	}

	cases := l.Cases
	if l.haveSoloRuns() {
		cases = l.getSoloRuns()
	}
	for c := range cases {
		cases[c].Run(t)
	}
}

func (l *Serialization) getModified() []*Case {
	all := make([]*Case, 0, len(l.Cases))
	for i := range l.Cases {
		l.Cases[i].putDefaults(l.Funcs, l.DefaultMods)
		c := l.Cases[i].GetModified()
		all = append(all, c.Cases...)
	}
	return all
}

func (l *Serialization) haveSoloRuns() bool {
	for i := range l.Cases {
		if l.Cases[i].SoloRun {
			return true
		}
	}
	return false
}

func (l *Serialization) getSoloRuns() []*Case {
	out := make([]*Case, 0, len(l.Cases))
	for i := range l.Cases {
		if l.Cases[i].SoloRun {
			out = append(out, l.Cases[i].Copy())
		}
	}
	return out
}
