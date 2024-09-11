package tests

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/mods"
)

type UnmarshalCases struct {
	Func        func([]byte, interface{}) error
	Cases       []*MustFailUnmarshal
	DefaultMods mods.Mods
	modified    bool
}

func (l *UnmarshalCases) GetModified() *UnmarshalCases {
	return &UnmarshalCases{
		Func:     l.Func,
		Cases:    l.getModified(),
		modified: true,
	}
}

func (l *UnmarshalCases) Copy() *UnmarshalCases {
	out := &UnmarshalCases{
		Func:        l.Func,
		Cases:       make([]*MustFailUnmarshal, 0, len(l.Cases)),
		DefaultMods: l.DefaultMods,
	}
	for i := range l.Cases {
		l.Cases[i].putDefaults(l.Func, l.DefaultMods)
		out.Cases = append(out.Cases, l.Cases[i].Copy())
	}
	return out
}

func (l *UnmarshalCases) Run(t *testing.T) {
	if len(l.Cases) == 0 {
		t.Fatal("nothing to run")
	}

	if l.Func == nil {
		t.Fatal("unmarshal func should be provided")
	}

	if !l.modified {
		t.Fatal("run GetModified fist")
	}

	cases := l.Cases
	if l.haveSoloRuns() {
		cases = l.getSoloRuns()
	}

	t.Run("unmarshal", func(t *testing.T) {
		for _, tCase := range cases {
			tCase.Run(t)
		}
	})
}

func (l *UnmarshalCases) getModified() []*MustFailUnmarshal {
	all := make([]*MustFailUnmarshal, 0, len(l.Cases))
	for i := range l.Cases {
		l.Cases[i].putDefaults(l.Func, l.DefaultMods)
		c := l.Cases[i].GetModified()
		all = append(all, c.Cases...)
	}
	return all
}

func (l *UnmarshalCases) haveSoloRuns() bool {
	for i := range l.Cases {
		if l.Cases[i].SoloRun {
			return true
		}
	}
	return false
}

func (l *UnmarshalCases) getSoloRuns() []*MustFailUnmarshal {
	out := make([]*MustFailUnmarshal, 0, len(l.Cases))
	for i := range l.Cases {
		if l.Cases[i].SoloRun {
			out = append(out, l.Cases[i].Copy())
		}
	}
	return out
}
