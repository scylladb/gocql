package gen

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/run"
)

type Sets []Set

type Group interface {
	Prepare()
	GetName() string
	GetSets() Sets
}

type Set interface {
	IsSoloRun() bool
	GetName() string
	GetValues() []interface{}
	GetMods() mod.Mods
	NewCase(name string, val interface{}) run.Test
}
