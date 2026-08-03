package dialer

import (
	"testing"

	"github.com/gocql/gocql/internal/murmur"
)

// startupFrameV4 builds a protocol v4 STARTUP request frame carrying body as
// its options blob. The contents of body are irrelevant to the hash; only its
// length is varied by the caller.
func startupFrameV4(body []byte) []byte {
	frame := []byte{
		0x04,
		0x00,       // no header flags
		0x00, 0x7B, // stream id
		byte(opStartup),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}

	return append(frame, body...)
}

// TestGetFrameHashStartupIgnoresBodyLength pins that the STARTUP hash does not
// depend on the body, directly or through the length field in the header.
//
// The checked-in replay recordings in tests/bench store raw frames and are
// rehashed at load time, so a body-dependent STARTUP hash silently couples them
// to the exact set of STARTUP options the driver sent when they were recorded.
// Adding one shifts the length field, no recorded frame matches any more, and
// the replay benchmarks panic with "unable to find a response to replay" until
// every recording is regenerated.
func TestGetFrameHashStartupIgnoresBodyLength(t *testing.T) {
	// useMetadataID is irrelevant to a STARTUP frame — it only moves the EXECUTE
	// query-params offset — so pass false and keep this about the body length.
	short := GetFrameHash(startupFrameV4([]byte{0x00, 0x01}), false)
	long := GetFrameHash(startupFrameV4(make([]byte, 512)), false)

	if short != long {
		t.Errorf("STARTUP hash depends on the body length: %d != %d", short, long)
	}

	// Confirm the frame was parsed rather than diverted to the raw-bytes
	// fallback, which would also make the two hashes differ if it regressed.
	if raw := murmur.Murmur3H1(startupFrameV4([]byte{0x00, 0x01})); short == raw {
		t.Error("GetFrameHash fell back to hashing the raw frame")
	}
}

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

	got := GetFrameHash(segment, false)
	want := murmur.Murmur3H1(segment)
	if got != want {
		t.Fatalf("GetFrameHash(v5 segment) = %d, want raw-bytes hash %d", got, want)
	}
}

// TestGetFrameHashEmpty verifies the empty-input guard.
func TestGetFrameHashEmpty(t *testing.T) {
	if got := GetFrameHash(nil, false); got != murmur.Murmur3H1(nil) {
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

// v4ExecuteFrame builds a minimal protocol-v4 EXECUTE request frame. When
// withMetadataID is true it inserts a resultMetadataID short-bytes field between
// the preparedID and the query params, as SCYLLA_USE_METADATA_ID does on v4.
// The query params are a bare consistency + zero flags, so a correct parse walks
// to exactly len(frame).
//
//	header: version, flags, stream(2), opcode, length(4)   -> body starts at 9
//	body:   preparedID [short bytes], (resultMetadataID [short bytes],)
//	        consistency [short], query flags [byte]
func v4ExecuteFrame(withMetadataID bool) []byte {
	body := []byte{
		0x00, 0x03, 0xAA, 0xBB, 0xCC, // preparedID: len 3
	}
	if withMetadataID {
		body = append(body, 0x00, 0x02, 0xDE, 0xAD) // resultMetadataID: len 2
	}
	body = append(body, 0x00, 0x01, 0x00) // consistency (2) + query flags (1) = 0
	header := []byte{
		0x04,       // version v4 (request)
		0x00,       // flags
		0x00, 0x01, // stream id
		byte(opExecute),                   // opcode
		0x00, 0x00, 0x00, byte(len(body)), // body length
	}
	return append(header, body...)
}

// TestGetFrameHashV4MetadataID verifies that on protocol v4 the caller-supplied
// useMetadataID flag makes GetFrameHash skip the EXECUTE resultMetadataID field,
// so the extracted query-params range ends exactly at the frame boundary. Without
// the skip the parser would mis-read the metadata-id bytes as query params (and
// can index past the frame), which is the bug SCYLLA_USE_METADATA_ID introduces
// on v4.
func TestGetFrameHashV4MetadataID(t *testing.T) {
	frame := v4ExecuteFrame(true)
	// GetFrameHash hashes frame[bodyStart:endIndex]; a correct skip lands endIndex
	// on len(frame), so the hashed range is the whole body.
	const bodyStart = 9
	want := murmur.Murmur3H1(frame[bodyStart:])
	if got := GetFrameHash(frame, true); got != want {
		t.Fatalf("GetFrameHash(v4+ext EXECUTE, true) = %d, want %d (resultMetadataID not skipped correctly)", got, want)
	}

	// The v4-without-extension path must still parse an EXECUTE that carries no
	// resultMetadataID field.
	noExt := v4ExecuteFrame(false)
	wantNoExt := murmur.Murmur3H1(noExt[bodyStart:])
	if got := GetFrameHash(noExt, false); got != wantNoExt {
		t.Fatalf("GetFrameHash(v4 EXECUTE, false) = %d, want %d", got, wantNoExt)
	}
}

// TestGetFrameHashIgnoresDirectionBit pins that every protocol-version
// comparison in GetFrameHash masks off the direction bit before comparing.
//
// The version tests used to be a mix of masked and unmasked. The unmasked ones
// read 0x84 (v4 with the direction bit set) as "greater than v4", i.e. as v5, so
// they took the v5-only branches: the EXECUTE parser skipped a resultMetadataID
// that is not on the wire (useMetadataID being false here) and addQueryParams
// read a 4-byte query-flags field instead of a 1-byte one. Both walk the offsets
// off the end of the body, so before the fix the 0x84 case panicked instead of
// producing the v4 hash.
func TestGetFrameHashIgnoresDirectionBit(t *testing.T) {
	plain := v4ExecuteFrame(false)
	directionBitSet := v4ExecuteFrame(false)
	directionBitSet[0] |= 0x80

	got := GetFrameHash(plain, false)
	withBit := GetFrameHash(directionBitSet, false)

	if got != withBit {
		t.Errorf("direction bit changed the hash: 0x04 -> %d, 0x84 -> %d", got, withBit)
	}

	// Also confirm the frame was really parsed, rather than diverted to the
	// raw-bytes fallback: the hash must cover the query-params range.
	want := murmur.Murmur3H1(plain[9:])
	if got != want {
		t.Errorf("GetFrameHash(v4 EXECUTE) = %d, want body hash %d", got, want)
	}
	if raw := murmur.Murmur3H1(plain); got == raw {
		t.Error("GetFrameHash fell back to hashing the raw frame")
	}
}

// TestStartupNegotiatesMetadataID verifies detection of the SCYLLA_USE_METADATA_ID
// opt-in in a STARTUP request, and that the opcode guard rejects other frames.
func TestStartupNegotiatesMetadataID(t *testing.T) {
	startupHeader := func(op frameOp) []byte {
		return []byte{0x04, 0x00, 0x00, 0x00, byte(op), 0x00, 0x00, 0x00, 0x00}
	}
	withKey := append(startupHeader(opStartup), []byte("SCYLLA_USE_METADATA_ID")...)
	withoutKey := append(startupHeader(opStartup), []byte("COMPRESSION")...)
	// Same key bytes, but not a STARTUP opcode — must not match.
	queryWithKey := append(startupHeader(opQuery), []byte("SCYLLA_USE_METADATA_ID")...)

	if !StartupNegotiatesMetadataID(withKey) {
		t.Error("StartupNegotiatesMetadataID(STARTUP with key) = false, want true")
	}
	if StartupNegotiatesMetadataID(withoutKey) {
		t.Error("StartupNegotiatesMetadataID(STARTUP without key) = true, want false")
	}
	if StartupNegotiatesMetadataID(queryWithKey) {
		t.Error("StartupNegotiatesMetadataID(QUERY with key) = true, want false")
	}
	if StartupNegotiatesMetadataID(nil) {
		t.Error("StartupNegotiatesMetadataID(nil) = true, want false")
	}
}
