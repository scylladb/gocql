package run

import (
	"testing"
)

type Group interface {
	AddGroup(name string) Group
	AddTest(Test)
	Parallel()
	SoloRun()
	RunGroup(t *testing.T)
	Run(t *testing.T, parallel bool)
}

type Test interface {
	Run(t *testing.T, parallel bool)
}
