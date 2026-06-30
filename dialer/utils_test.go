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

// v4ExecuteFrame builds a minimal protocol-v4 EXECUTE request frame. When
// withMetadataID is true it inserts a resultMetadataID short-bytes field between
// the preparedID and the query params, as SCYLLA_USE_METADATA_ID does on v4.
// The query params are a bare consistency + zero flags, so a correct parse walks
// to exactly len(frame).
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
