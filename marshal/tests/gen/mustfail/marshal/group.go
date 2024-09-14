package marshal

import (
	"github.com/gocql/gocql/marshal/tests/gen"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/run"
)

// Group is a tool for grouping Sets.
type Group struct {
	Name        string
	Func        func(interface{}) ([]byte, error)
	Sets        Sets
	DefaultMods mod.Mods
}

func (g Group) Copy() Group {
	out := Group{
		Func:        g.Func,
		Sets:        make(Sets, 0, len(g.Sets)),
		DefaultMods: g.DefaultMods,
	}
	for i := range g.Sets {
		out.Sets = append(out.Sets, g.Sets[i].Copy())
	}
	return out
}

func (g Group) Gen() run.Group {
	return gen.FromGroup(g)
}

func (g Group) Prepare() {
	for i := range g.Sets {
		g.Sets[i].putDefault(g.Func, g.DefaultMods)
	}
}

func (g Group) GetName() string {
	if g.Name == "" {
		return "marshal"
	}
	return g.Name
}

func (g Group) GetSets() gen.Sets {
	out := make(gen.Sets, len(g.Sets))
	for i := range g.Sets {
		out[i] = g.Sets[i].Copy()
	}
	return out
}
