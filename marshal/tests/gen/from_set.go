package gen

import (
	"github.com/gocql/gocql/marshal/tests/run"
	"github.com/gocql/gocql/marshal/tests/utils"
)

func FromSet(s Set) run.Group {
	setGroup := run.NewGroup(s.GetName())
	if s.IsSoloRun() {
		setGroup.SoloRun()
	}

	addByMods(s, setGroup)
	return setGroup
}

func addByMods(s Set, setGroup run.Group) {
	for _, mod := range s.GetMods() {
		modified := mod.Apply(s.GetValues())
		if len(modified) == 0 {
			continue
		}

		modGroup := setGroup.AddGroup(mod.Name())
		addModified(s, modGroup, modified)
	}
}

func addModified(s Set, modGroup run.Group, vals []interface{}) {
	names := utils.ValueNames(vals)
	for i := range vals {
		modGroup.AddTest(s.NewCase(names[i], vals[i]))
	}
}

func getSoloRun(sets Sets) Sets {
	out := make(Sets, 0)
	for i := range sets {
		if sets[i].IsSoloRun() {
			out = append(out, sets[i])
		}
	}
	return out
}

func haveSoloRun(sets Sets) bool {
	for i := range sets {
		if sets[i].IsSoloRun() {
			return true
		}
	}
	return false
}
