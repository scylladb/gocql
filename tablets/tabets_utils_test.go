package tablets

import (
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestCreateTablets(t *testing.T) {
	t.Run("BasicDistribution", func(t *testing.T) {
		hosts := tests.GenerateHostNames(3)
		tl := createTablets("ks", "tbl", hosts, 2, 6, 6)
		if len(tl) != 6 {
			t.Errorf("expected 6 tablets, got %d", len(tl))
		}

		for _, tablet := range tl {
			if len(tablet.replicas) != 2 {
				t.Errorf("each tablet should have 2 replicas, got %d", len(tablet.replicas))
			}
			replicaSet := map[string]bool{}
			for _, r := range tablet.replicas {
				if replicaSet[r.hostId] {
					t.Errorf("duplicate replica %s in tablet", r.hostId)
				}
				replicaSet[r.hostId] = true
			}
		}
	})

	t.Run("SingleTabletFullRange", func(t *testing.T) {
		hosts := tests.GenerateHostNames(3)
		tl := createTablets("ks", "tbl", hosts, 3, 1, 1)
		t0 := tl[0]
		if t0.firstToken != math.MinInt64 {
			t.Errorf("unexpected firstToken: %d", t0.firstToken)
		}
		if t0.lastToken != math.MaxInt64 {
			t.Errorf("unexpected lastToken: %d", t0.lastToken)
		}
	})
}

func TestReplicaGenerator(t *testing.T) {
	hosts := []string{"a", "b", "c", "d"}
	rf := 2
	g := NewReplicaSetGenerator(hosts, rf)

	var seen [][]string
	for i := 0; i < 6; i++ {
		gen := g.Next()

		if len(gen) != rf {
			t.Fatalf("expected %d replicas, got %d", rf, len(gen))
		}

		var ids []string
		for _, r := range gen {
			ids = append(ids, r.HostID())
		}
		seen = append(seen, ids)
	}

	for i := 0; i < len(seen); i++ {
	outer:
		for j := i + 1; j < len(seen); j++ {
			for k := 0; k < len(seen[i]); k++ {
				if seen[i][k] != seen[j][k] {
					continue outer
				}
			}
			t.Errorf("expected different output for different seeds, but found same seeds for %d and %d: %s == %s", i, j, seen[i], seen[j])
		}
	}
}
