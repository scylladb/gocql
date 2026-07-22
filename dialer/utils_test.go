package dialer

import (
	"testing"

	"github.com/gocql/gocql/internal/murmur"
)

// TestGetFrameHashGuardsProtoV5Segments verifies that GetFrameHash does not
// attempt to parse protocol v5+ input as a CQL frame. On v5 the recorded bytes
// are a transport segment (see prepareModernLayout), so frame[0] is segment
// data, not a CQL version byte. The function must fall back to hashing the raw
// bytes instead of walking CQL offsets. Tracked: scylladb/gocql#937.
func TestGetFrameHashGuardsProtoV5Segments(t *testing.T) {
	// First byte has the low 7 bits >= 5, i.e. a v5+ version (or arbitrary
	// segment header data). The remaining bytes are deliberately too short to
	// contain a valid CQL request body; the pre-guard code would index past
	// them and panic.
	segment := []byte{0x05, 0x00, 0x11, 0x22}

	got := GetFrameHash(segment)
	want := murmur.Murmur3H1(segment)
	if got != want {
		t.Fatalf("GetFrameHash(v5 segment) = %d, want raw-bytes hash %d", got, want)
	}
}

// TestGetFrameHashEmpty verifies the empty-input guard.
func TestGetFrameHashEmpty(t *testing.T) {
	if got := GetFrameHash(nil); got != murmur.Murmur3H1(nil) {
		t.Fatalf("GetFrameHash(nil) = %d, want %d", got, murmur.Murmur3H1(nil))
	}
}
