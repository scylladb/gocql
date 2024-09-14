package gen

import "github.com/gocql/gocql/marshal/tests/run"

func FromGroup(g Group) run.Group {
	g.Prepare()
	group := run.NewGroup(g.GetName())

	sets := g.GetSets()
	if haveSoloRun(sets) {
		group.SoloRun()
		sets = getSoloRun(sets)
	}

	for _, set := range sets {
		setGroup := group.AddGroup(set.GetName())
		addByMods(set, setGroup)
	}
	return group
}
