package serialization

import (
	"github.com/gocql/gocql/marshal/tests/gen"
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/run"
)

// Group is a tool for grouping Sets.
type Group struct {
	Name        string
	Funcs       *funcs.Funcs
	DefaultMods mod.Mods
	Sets        Sets
}

func (g Group) Copy() Group {
	out := Group{
		Funcs:       g.Funcs.Copy(),
		DefaultMods: g.DefaultMods,
		Sets:        make(Sets, 0, len(g.Sets)),
	}
	for i := range g.Sets {
		out.Sets = append(out.Sets, g.Sets[i].Copy())
	}
	return out
}

func (g Group) Gen() run.Group {
	if !g.Funcs.Valid() {
		panic("provided Funcs should be valid")
	}
	return gen.FromGroup(g)
}

func (g Group) Prepare() {
	if !g.Funcs.Valid() {
		panic("provided Funcs should be valid")
	}
	for i := range g.Sets {
		g.Sets[i].putDefault(g.Funcs, g.DefaultMods)
	}
}

func (g Group) GetName() string {
	if g.Name == "" {
		return "serialization"
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
