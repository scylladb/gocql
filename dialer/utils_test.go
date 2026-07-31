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

// TestFrameIsProtoV5OrNewer pins that the version comparison masks off the
// direction bit (a v5 response byte is 0x85) and that short input is not a
// match.
func TestFrameIsProtoV5OrNewer(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    []byte
		want bool
	}{
		{"v5 request", []byte{0x05, 0x00}, true},
		{"v5 response", []byte{0x85}, true},
		{"v4 request", []byte{0x04}, false},
		{"v4 response", []byte{0x84}, false},
		{"empty", nil, false},
	} {
		if got := FrameIsProtoV5OrNewer(tc.b); got != tc.want {
			t.Errorf("%s: FrameIsProtoV5OrNewer(% X) = %v, want %v", tc.name, tc.b, got, tc.want)
		}
	}
}

// executeFrameV4 builds a minimal protocol v4 EXECUTE request frame. version is
// taken as-is so callers can set the request/response direction bit.
//
//	header: version, flags, stream(2), opcode, length(4)   -> body starts at 9
//	body:   preparedID [short bytes], consistency [short], query flags [byte]
func executeFrameV4(version byte) []byte {
	body := []byte{
		0x00, 0x02, 0xAA, 0xBB, // preparedID: 2-byte length + 2 bytes
		0x00, 0x01, // consistency = ONE
		0x00, // query flags: none set
	}

	frame := []byte{
		version,
		0x00,       // no header flags (notably no custom payload)
		0x00, 0x7B, // stream id
		byte(opExecute),
		0x00, 0x00, 0x00, byte(len(body)),
	}

	return append(frame, body...)
}

// TestGetFrameHashIgnoresDirectionBit pins that every protocol-version
// comparison in GetFrameHash masks off the direction bit before comparing.
//
// The version tests used to be a mix of masked and unmasked. The unmasked ones
// read 0x84 (v4 with the direction bit set) as "greater than v4", i.e. as v5, so
// they took the v5-only branches: the EXECUTE parser skipped a resultMetadataID
// that is not on the wire and addQueryParams read a 4-byte flags field instead of
// a 1-byte one. Both walk the offsets off the end of the body, so before the fix
// the 0x84 case panicked instead of producing the v4 hash.
func TestGetFrameHashIgnoresDirectionBit(t *testing.T) {
	plain := executeFrameV4(0x04)
	directionBitSet := executeFrameV4(0x04 | 0x80)

	got := GetFrameHash(plain)
	withBit := GetFrameHash(directionBitSet)

	if got != withBit {
		t.Errorf("direction bit changed the hash: 0x04 -> %d, 0x84 -> %d", got, withBit)
	}

	// Also confirm the frame was really parsed, rather than diverted to the
	// raw-bytes fallback: the hash must cover just the query-params range.
	want := murmur.Murmur3H1(plain[9:])
	if got != want {
		t.Errorf("GetFrameHash(v4 EXECUTE) = %d, want body hash %d", got, want)
	}
	if raw := murmur.Murmur3H1(plain); got == raw {
		t.Error("GetFrameHash fell back to hashing the raw frame")
	}
}
