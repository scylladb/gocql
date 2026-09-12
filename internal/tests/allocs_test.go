package tests

import "testing"

func TestMinAllocsPerRun(t *testing.T) {
	var sink *int
	for _, tc := range []struct {
		name string
		f    func()
		want float64
	}{
		{"no allocations", func() {}, 0},
		{"one allocation", func() { n := 1; sink = &n }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MinAllocsPerRun(3, 100, tc.f); got != tc.want {
				t.Errorf("MinAllocsPerRun = %v, want %v", got, tc.want)
			}
		})
	}
	_ = sink
}
