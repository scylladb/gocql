package run

import (
	"testing"
)

// group is a tool for grouping and running various test cases such as serialization.tCase, mustfail.marshal and mustfail.unmarshal.
type group struct {
	name     string
	elems    []Test
	parallel bool
	soloRun  bool
}

func (gr *group) Parallel() {
	gr.parallel = true
}

func (gr *group) SoloRun() {
	gr.soloRun = true
}

func (gr *group) RunGroup(t *testing.T) {
	if gr.soloRun {
		t.Error("SoloRun is true, please remove it after finished tuning")
	}
	gr.Run(t, gr.parallel)
}

func (gr *group) Run(t *testing.T, parallel bool) {
	if len(gr.elems) == 0 {
		return
	}
	t.Run(gr.name, func(tt *testing.T) {
		if parallel {
			tt.Parallel()
		}
		for i := range gr.elems {
			gr.elems[i].Run(tt, parallel)
		}
	})
}

func (gr *group) AddTest(e Test) {
	gr.elems = append(gr.elems, e)
}

func (gr *group) AddGroup(name string) Group {
	out := NewGroup(name)
	gr.AddTest(out)
	return out
}
