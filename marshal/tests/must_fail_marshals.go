package tests

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/mods"
)

type MarshalCases struct {
	Func        func(interface{}) ([]byte, error)
	Cases       []*MustFailMarshal
	DefaultMods mods.Mods
	modified    bool
}

func (l *MarshalCases) GetModified() *MarshalCases {
	return &MarshalCases{
		Func:     l.Func,
		Cases:    l.getModified(),
		modified: true,
	}
}

func (l *MarshalCases) Copy() *MarshalCases {
	out := MarshalCases{
		Func:        l.Func,
		Cases:       make([]*MustFailMarshal, 0, len(l.Cases)),
		DefaultMods: l.DefaultMods,
	}
	for i := range l.Cases {
		l.Cases[i].putDefaults(l.Func, l.DefaultMods)
		out.Cases = append(out.Cases, l.Cases[i].Copy())
	}
	return &out
}

func (l *MarshalCases) Run(t *testing.T) {
	if len(l.Cases) == 0 {
		t.Fatal("there is no cases to run")
	}

	if l.Func == nil {
		t.Fatal("marshal func should be provided")
	}

	if !l.modified {
		t.Fatal("run GetModified fist")
	}

	for c := range l.Cases {
		l.Cases[c].putDefaults(l.Func, l.DefaultMods)
	}

	cases := l.Cases
	if l.haveSoloRuns() {
		cases = l.getSoloRuns()
	}

	t.Run("  marshal", func(t *testing.T) {
		for _, tCase := range cases {
			tCase.Run(t)
		}
	})
}

func (l *MarshalCases) getModified() []*MustFailMarshal {
	all := make([]*MustFailMarshal, 0, len(l.Cases))
	for i := range l.Cases {
		l.Cases[i].putDefaults(l.Func, l.DefaultMods)
		c := l.Cases[i].GetModified()
		all = append(all, c.Cases...)
	}
	return all
}

func (l *MarshalCases) haveSoloRuns() bool {
	for i := range l.Cases {
		if l.Cases[i].SoloRun {
			return true
		}
	}
	return false
}

func (l *MarshalCases) getSoloRuns() []*MustFailMarshal {
	out := make([]*MustFailMarshal, 0, len(l.Cases))
	for i := range l.Cases {
		if l.Cases[i].SoloRun {
			out = append(out, l.Cases[i].Copy())
		}
	}
	return out
}
