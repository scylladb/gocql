//go:build unit
// +build unit

package tablets

import (
	"math"
	"testing"
)

func TestFindEntryForToken(t *testing.T) {
	t.Parallel()

	t.Run("ExactLastToken", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		entry := entries.findEntryForToken(0, 0, len(entries))
		if entry == nil {
			t.Fatal("expected entry for token at exact lastToken boundary")
		}
		if entry.lastToken != 0 {
			t.Fatalf("expected lastToken=0, got %d", entry.lastToken)
		}
	})

	t.Run("ExactFirstToken", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		entry := entries.findEntryForToken(-100, 0, len(entries))
		if entry == nil {
			t.Fatal("expected entry for token at exact firstToken boundary")
		}
		if entry.firstToken != -100 {
			t.Fatalf("expected firstToken=-100, got %d", entry.firstToken)
		}
	})

	t.Run("BeyondAll", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		entry := entries.findEntryForToken(200, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token beyond all tablets")
		}
	})

	t.Run("BeforeAll", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		entry := entries.findEntryForToken(-200, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token before all tablets")
		}
	})

	t.Run("InGap", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -200, lastToken: -100},
			{firstToken: 100, lastToken: 200},
		}
		entry := entries.findEntryForToken(0, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token in gap between non-contiguous tablets")
		}
	})

	t.Run("EmptyList", func(t *testing.T) {
		entries := TabletEntryList{}
		entry := entries.findEntryForToken(0, 0, 0)
		if entry != nil {
			t.Fatal("expected nil for empty entry list")
		}
	})

	t.Run("SingleEntry", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -50, lastToken: 50},
		}

		entry := entries.findEntryForToken(0, 0, len(entries))
		if entry == nil {
			t.Fatal("expected entry for token inside single entry")
		}

		entry = entries.findEntryForToken(-50, 0, len(entries))
		if entry == nil {
			t.Fatal("expected entry for token at firstToken of single entry")
		}

		entry = entries.findEntryForToken(50, 0, len(entries))
		if entry == nil {
			t.Fatal("expected entry for token at lastToken of single entry")
		}

		entry = entries.findEntryForToken(-51, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token before single entry")
		}

		entry = entries.findEntryForToken(51, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token after single entry")
		}
	})

	t.Run("InvalidBounds", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{"host1", 0}}},
		}

		testCases := []struct {
			name string
			l, r int
		}{
			{"negative l", -1, 1},
			{"r beyond length", 0, 10},
			{"l > r", 1, 0},
			{"both invalid", -1, 10},
			{"l == r (empty range)", 0, 0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := entries.findEntryForToken(50, tc.l, tc.r)
				if result != nil {
					t.Errorf("expected nil for invalid bounds l=%d r=%d, got %+v", tc.l, tc.r, result)
				}
			})
		}
	})

	t.Run("SingleTokenTablet", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -100, lastToken: -50},
			{firstToken: 42, lastToken: 42},
			{firstToken: 100, lastToken: 200},
		}

		entry := entries.findEntryForToken(42, 0, len(entries))
		if entry == nil {
			t.Fatal("expected entry for single-token tablet")
		}
		if entry.firstToken != 42 || entry.lastToken != 42 {
			t.Fatalf("expected [42,42], got [%d,%d]", entry.firstToken, entry.lastToken)
		}

		entry = entries.findEntryForToken(41, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token just before single-token tablet")
		}

		entry = entries.findEntryForToken(43, 0, len(entries))
		if entry != nil {
			t.Fatal("expected nil for token just after single-token tablet")
		}
	})
}

func TestFindOverlapRange(t *testing.T) {
	t.Parallel()

	t.Run("ContiguousBoundary", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{"host1", 0}}},
			{firstToken: 200, lastToken: 300, replicas: []ReplicaInfo{{"host2", 1}}},
		}

		start, tailStart := entries.findOverlapRange(100, 200)

		if start != 1 {
			t.Errorf("expected start=1 for contiguous boundary, got %d", start)
		}
		if tailStart != 2 {
			t.Errorf("expected tailStart=2 for contiguous boundary, got %d", tailStart)
		}
	})

	t.Run("ExtremeValues", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: math.MinInt64, lastToken: 0, replicas: []ReplicaInfo{{"host1", 0}}},
			{firstToken: 0, lastToken: math.MaxInt64, replicas: []ReplicaInfo{{"host2", 1}}},
		}

		start, tailStart := entries.findOverlapRange(math.MinInt64, math.MaxInt64)
		if start != 0 {
			t.Errorf("expected start=0 for full range overlap, got %d", start)
		}
		if tailStart != 2 {
			t.Errorf("expected tailStart=2 for full range overlap, got %d", tailStart)
		}

		start, tailStart = entries.findOverlapRange(math.MinInt64, -100)
		if start != 0 {
			t.Errorf("expected start=0 for MinInt64 range, got %d", start)
		}
		if tailStart != 1 {
			t.Errorf("expected tailStart=1 for MinInt64 range, got %d", tailStart)
		}

		start, tailStart = entries.findOverlapRange(100, math.MaxInt64)
		if start != 1 {
			t.Errorf("expected start=1 for MaxInt64 range, got %d", start)
		}
		if tailStart != 2 {
			t.Errorf("expected tailStart=2 for MaxInt64 range, got %d", tailStart)
		}
	})

	t.Run("SingleEntry", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -100, lastToken: 100, replicas: []ReplicaInfo{{"host1", 0}}},
		}

		start, tailStart := entries.findOverlapRange(-50, 50)
		if start != 0 || tailStart != 1 {
			t.Errorf("expected start=0 tailStart=1 for overlapping range, got start=%d tailStart=%d", start, tailStart)
		}

		start, tailStart = entries.findOverlapRange(-200, 200)
		if start != 0 || tailStart != 1 {
			t.Errorf("expected start=0 tailStart=1 for extended range, got start=%d tailStart=%d", start, tailStart)
		}

		start, tailStart = entries.findOverlapRange(-200, -150)
		if start != 0 || tailStart != 0 {
			t.Errorf("expected start=0 tailStart=0 for range before, got start=%d tailStart=%d", start, tailStart)
		}

		start, tailStart = entries.findOverlapRange(150, 200)
		if start != 1 || tailStart != 1 {
			t.Errorf("expected start=1 tailStart=1 for range after, got start=%d tailStart=%d", start, tailStart)
		}

		start, tailStart = entries.findOverlapRange(-100, -50)
		if start != 0 || tailStart != 1 {
			t.Errorf("expected start=0 tailStart=1 for range sharing firstToken, got start=%d tailStart=%d", start, tailStart)
		}

		start, tailStart = entries.findOverlapRange(100, 200)
		if start != 1 || tailStart != 1 {
			t.Errorf("expected start=1 tailStart=1 for contiguous range at lastToken, got start=%d tailStart=%d", start, tailStart)
		}
	})

	t.Run("SingleTokenTablet", func(t *testing.T) {
		entries := TabletEntryList{}
		start, tailStart := entries.findOverlapRange(42, 42)
		if start != 0 || tailStart != 0 {
			t.Errorf("empty list: expected start=0 tailStart=0, got start=%d tailStart=%d", start, tailStart)
		}

		entries = TabletEntryList{
			{firstToken: 40, lastToken: 50},
		}
		start, tailStart = entries.findOverlapRange(42, 42)
		if start != 0 || tailStart != 1 {
			t.Errorf("contained: expected start=0 tailStart=1, got start=%d tailStart=%d", start, tailStart)
		}

		entries = TabletEntryList{
			{firstToken: 0, lastToken: 42},
		}
		start, tailStart = entries.findOverlapRange(42, 42)
		if start != 1 || tailStart != 1 {
			t.Errorf("adjacent: expected start=1 tailStart=1, got start=%d tailStart=%d", start, tailStart)
		}
	})
}

func TestAddEntry(t *testing.T) {
	t.Parallel()

	t.Run("SingleTokenTablet", func(t *testing.T) {
		tl := TabletEntryList{}
		tl = tl.addEntry(&TabletEntry{firstToken: 42, lastToken: 42})
		if len(tl) != 1 || tl[0].firstToken != 42 || tl[0].lastToken != 42 {
			t.Fatalf("expected single [42,42] entry, got %v", tl)
		}

		tl = TabletEntryList{
			{firstToken: -100, lastToken: -50},
			{firstToken: 100, lastToken: 200},
		}
		tl = tl.addEntry(&TabletEntry{firstToken: 42, lastToken: 42})
		if len(tl) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(tl))
		}
		if tl[1].firstToken != 42 || tl[1].lastToken != 42 {
			t.Fatalf("expected middle entry [42,42], got [%d,%d]", tl[1].firstToken, tl[1].lastToken)
		}
	})
}
