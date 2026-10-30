package testkit

import (
	"encoding/hex"
	"testing"
)

func TestUnitGolden(t *testing.T) {
	// Fixed independently pinned native v5 OPTIONS frame from protocol bytes.
	got := hex.EncodeToString(Unit([]byte{4, 0, 0, 0, 5, 0, 0, 0, 0}, false, true))
	const want = "090002a4c8c10400000005000000005392535a"
	if got != want {
		t.Fatalf("unit = %s", got)
	}
}
