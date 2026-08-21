package dialer

import (
	"bytes"
	"math"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/murmur"
)

// frameV4 builds a protocol v4 request frame: version, header flags, a 2-byte
// stream id, the opcode and the 4-byte body length, followed by body.
func frameV4(op frameOp, headerFlags byte, body []byte) []byte {
	frame := []byte{
		0x04,
		headerFlags,
		0x00, 0x7B, // stream id
		byte(op),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}

	return append(frame, body...)
}

// frameV2 builds the same frame on protocol v2, whose stream id is one byte, so
// the opcode sits at index 3 and the body at index 8. Pins the shift=0 side of
// headerShift; reading the opcode at index 4 here finds a length byte instead and
// the frame is misclassified.
func frameV2(op frameOp, body []byte) []byte {
	frame := []byte{
		0x02,
		0x00, // header flags
		0x00, // stream id (1 byte)
		byte(op),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}

	return append(frame, body...)
}

// startupFrameV4 builds a protocol v4 STARTUP request frame carrying body as
// its options blob. The contents of body are irrelevant to the hash; only its
// length is varied by the caller.
func startupFrameV4(body []byte) []byte {
	return frameV4(opStartup, 0x00, body)
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

// TestGetFrameHashParsesProtoV5Frames pins that a protocol v5 request frame is
// parsed rather than diverted to a whole-frame hash.
//
// It used to be diverted, by a guard that treated any frame[0] whose low 7 bits were
// >= 5 as transport-segment data. That guard was unsound in both directions: a v5
// segment whose length low-byte is < 5 slipped past it into the CQL parser anyway,
// and a genuine v5 frame — which the driver does produce, unsegmented, throughout the
// handshake — was hashed whole, default timestamp included, so it could never match
// its own replay. Telling the two apart needs protocol context the bytes do not
// carry, so it is the caller's job to pass a bare frame; see GetFrameHash's contract.
func TestGetFrameHashParsesProtoV5Frames(t *testing.T) {
	// A v5 EXECUTE with a keyspace override. Parsed, the hashed range stops before
	// the frame's end; diverted, the hash is the whole frame.
	frame := v5ExecuteFrame(frm.FlagWithKeyspace, shortString("ks"))

	if got, whole := GetFrameHash(frame, false), murmur3OfStreamBlanked(frame); got == whole {
		t.Error("v5 frame was hashed whole rather than parsed")
	}
	if got, asGiven := GetFrameHash(frame, false), murmur.Murmur3H1(frame); got == asGiven {
		t.Error("v5 frame fell through to a guard that hashes it as given")
	}
}

// TestGetFrameHashShortProtoV5Input pins that v5 input too short to hold a header
// still reaches the short-frame guard, which hashes it as given.
func TestGetFrameHashShortProtoV5Input(t *testing.T) {
	short := []byte{0x05, 0x00, 0x11, 0x22}

	if got, want := GetFrameHash(short, false), murmur.Murmur3H1(short); got != want {
		t.Fatalf("GetFrameHash(short v5) = %d, want %d", got, want)
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

// stringMap encodes opts as a CQL [string map]: a short count, then that many
// [string] key / [string] value pairs, each length-prefixed with a short.
func stringMap(opts ...[2]string) []byte {
	body := []byte{byte(len(opts) >> 8), byte(len(opts))}
	appendString := func(s string) {
		body = append(body, byte(len(s)>>8), byte(len(s)))
		body = append(body, s...)
	}
	for _, kv := range opts {
		appendString(kv[0])
		appendString(kv[1])
	}
	return body
}

// TestStartupNegotiatesMetadataID verifies detection of the SCYLLA_USE_METADATA_ID
// opt-in in a STARTUP request, that the opcode guard rejects other frames, and
// that only map *keys* count — gocql's startupOptions puts caller-supplied
// DRIVER_NAME/DRIVER_VERSION/ApplicationInfo values into the same map, so a
// substring scan would let a caller latch this by naming their application after
// the extension.
func TestStartupNegotiatesMetadataID(t *testing.T) {
	// The driver serializes the extension as a key mapping to the empty string,
	// alongside the mandatory options.
	optIn := stringMap(
		[2]string{"CQL_VERSION", "3.0.0"},
		[2]string{scyllaUseMetadataIDKey, ""},
	)
	noOptIn := stringMap(
		[2]string{"CQL_VERSION", "3.0.0"},
		[2]string{"COMPRESSION", "lz4"},
	)

	if !StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, optIn)) {
		t.Error("StartupNegotiatesMetadataID(STARTUP with key) = false, want true")
	}
	if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, noOptIn)) {
		t.Error("StartupNegotiatesMetadataID(STARTUP without key) = true, want false")
	}
	// Same key bytes, but not a STARTUP opcode — must not match.
	if StartupNegotiatesMetadataID(frameV4(opQuery, 0x00, optIn)) {
		t.Error("StartupNegotiatesMetadataID(QUERY with key) = true, want false")
	}
	if !StartupNegotiatesMetadataID(frameV2(opStartup, optIn)) {
		t.Error("StartupNegotiatesMetadataID(v2 STARTUP with key) = false, want true")
	}
	if StartupNegotiatesMetadataID(frameV2(opQuery, optIn)) {
		t.Error("StartupNegotiatesMetadataID(v2 QUERY with key) = true, want false")
	}
	if StartupNegotiatesMetadataID(nil) {
		t.Error("StartupNegotiatesMetadataID(nil) = true, want false")
	}

	// A caller-supplied value carrying the key literal must not latch it.
	valueOnly := stringMap(
		[2]string{"CQL_VERSION", "3.0.0"},
		[2]string{"APPLICATION_NAME", "we love " + scyllaUseMetadataIDKey},
		[2]string{"DRIVER_NAME", scyllaUseMetadataIDKey},
	)
	if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, valueOnly)) {
		t.Error("StartupNegotiatesMetadataID(STARTUP with key only in values) = true, want false")
	}

	// A map truncated mid-entry reads as "not negotiated" rather than indexing past
	// the end: the recorder feeds this whatever bytes a connection produced, and a
	// read that straddles two frames still records the second one from its middle.
	for n := len(frameV4(opStartup, 0x00, optIn)) - 1; n > 4; n-- {
		if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, optIn)[:n]) {
			t.Errorf("StartupNegotiatesMetadataID(STARTUP truncated to %d bytes) = true, want false", n)
		}
	}
	// A count claiming more entries than the body holds.
	overCount := append([]byte{0x00, 0x09}, stringMap([2]string{"CQL_VERSION", "3.0.0"})[2:]...)
	if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, overCount)) {
		t.Error("StartupNegotiatesMetadataID(STARTUP with an overstated map count) = true, want false")
	}

	// The recorder's read-side FrameWriter calls this on every completed response
	// frame, so a SUPPORTED response advertising the key must not latch the flag.
	supported := frameV4(opSupported, 0x00, optIn)
	supported[0] |= 0x80 // response direction bit
	if StartupNegotiatesMetadataID(supported) {
		t.Error("StartupNegotiatesMetadataID(SUPPORTED response with key) = true, want false")
	}
}

// TestGetFrameHashBoundsTruncatedExecute pins that an EXECUTE frame truncated
// across one of the short-bytes length fields falls back to hashing the raw bytes
// rather than indexing past the end. The resultMetadataID skip is live on protocol
// v4 now, and loadResponseFramesFromFiles feeds GetFrameHash bytes straight out of a
// recording file, so a damaged record must not panic the replayer.
//
// This covers the two length fields in the opExecute branch; the payloads they
// announce and everything addQueryParams walks are covered by
// TestGetFrameHashBoundsTruncatedBody.
func TestGetFrameHashBoundsTruncatedExecute(t *testing.T) {
	full := v4ExecuteFrame(true)

	for _, tc := range []struct {
		name          string
		frame         []byte
		useMetadataID bool
	}{
		{
			// Ends inside the preparedID length field.
			name:          "truncated at the preparedID length",
			frame:         full[:10],
			useMetadataID: true,
		},
		{
			// preparedID parses whole — length and payload — and then the metadata-id
			// length field runs off the end. full[:11] would not get this far: it
			// stops at the preparedID payload, which is TestGetFrameHashBoundsTruncatedBody's
			// case, not this one.
			name:          "truncated at the metadata id length",
			frame:         full[:15],
			useMetadataID: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := append([]byte(nil), tc.frame...)

			// The fallback hashes the frame as the parser left it, i.e. with the stream
			// id blanked — which is what makes the hash stable between record and replay,
			// where the stream ids differ. Mirror that here.
			want := murmur3OfStreamBlanked(tc.frame)

			got := GetFrameHash(frame, tc.useMetadataID)
			if got != want {
				t.Errorf("GetFrameHash(%s) = %d, want raw-bytes hash %d", tc.name, got, want)
			}
			// The parser must also have left the caller's frame as it found it.
			if !bytes.Equal(frame, tc.frame) {
				t.Errorf("GetFrameHash mutated the frame: got % X, want % X", frame, tc.frame)
			}
		})
	}
}

// TestFitsRejectsOverflowingLengths pins that fits does not compute index+n. A
// [bytes] field can encode a length up to MaxInt32, which overflows a 32-bit int
// once added to any index; the negative sum then satisfies every upper bound and
// the parser walks on with a cursor outside the frame. Reachable on 32-bit targets
// only, but the checks below overflow on any word size.
func TestFitsRejectsOverflowingLengths(t *testing.T) {
	frame := make([]byte, 8)

	if fits(frame, 1, math.MaxInt) {
		t.Error("fits(len 8, index 1, n MaxInt) = true, want false")
	}
	if fits(frame, math.MaxInt, 1) {
		t.Error("fits(len 8, index MaxInt, n 1) = true, want false")
	}
	// The boundary either side, so the fix cannot have been an off-by-one.
	if !fits(frame, 8, 0) {
		t.Error("fits(len 8, index 8, n 0) = false, want true")
	}
	if fits(frame, 8, 1) {
		t.Error("fits(len 8, index 8, n 1) = true, want false")
	}
}

// TestGetFrameHashBoundsShortFrame pins the guard for a frame shorter than its own
// header. Everything after it indexes unconditionally — the stream-id blanking
// reads frame[2] (and frame[3] on v3+), the switch reads the opcode at frame[3+p] —
// so a one-byte v4 frame used to panic before the parser had looked at anything.
//
// The fallback here hashes the frame as given rather than with the stream id
// blanked, because there may be no stream id to blank. That is fine: record and
// replay run the same guard over the same bytes.
func TestGetFrameHashBoundsShortFrame(t *testing.T) {
	for _, tc := range []struct {
		name string
		full []byte
	}{
		{"v4", frameV4(opExecute, 0x00, nil)},
		{"v2", frameV2(opExecute, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A header-only frame is the shortest one the parser accepts, so every
			// prefix of it must divert. Its own length is the first that must not.
			for n := 1; n < len(tc.full); n++ {
				short := append([]byte(nil), tc.full[:n]...)

				got := GetFrameHash(short, false)
				if want := murmur.Murmur3H1(tc.full[:n]); got != want {
					t.Errorf("GetFrameHash(%d-byte frame) = %d, want raw-bytes hash %d", n, got, want)
				}
				if !bytes.Equal(short, tc.full[:n]) {
					t.Errorf("GetFrameHash mutated the %d-byte frame: got % X, want % X", n, short, tc.full[:n])
				}
			}
		})
	}
}

// TestGetFrameHashBoundsTruncatedBody pins that every variable-length field the
// parser walks is bounded, not just the two short-bytes length fields covered by
// TestGetFrameHashBoundsTruncatedExecute. A length that reads cleanly but announces
// more bytes than remain used to move the cursor past the end and reach
// addQueryParams, which then indexed with no check of its own.
//
// loadResponseFramesFromFiles hashes whatever a recording file holds, so each of
// these has to fall back to the raw bytes rather than panic.
func TestGetFrameHashBoundsTruncatedBody(t *testing.T) {
	// overrunAt rewrites the short at index to a length longer than the frame.
	overrunAt := func(frame []byte, index int) []byte {
		out := append([]byte(nil), frame...)
		out[index], out[index+1] = 0x00, 0xFF
		return out
	}

	for _, tc := range []struct {
		name          string
		frame         []byte
		useMetadataID bool
	}{
		{
			// The preparedID length parses but its payload runs off the end. Without
			// the extension nothing else reads before addQueryParams, so this is the
			// path that reached it with an out-of-range cursor.
			name:  "preparedID payload overruns",
			frame: overrunAt(v4ExecuteFrame(false), 9),
		},
		{
			name:          "resultMetadataID payload overruns",
			frame:         overrunAt(v4ExecuteFrame(true), 14),
			useMetadataID: true,
		},
		{
			// Both short-bytes fields parse; the query flags byte is missing.
			name:          "query params truncated at the flags",
			frame:         v4ExecuteFrame(true)[:20],
			useMetadataID: true,
		},
		{
			// Flags announce bound values, the count parses, the value itself is
			// absent — this one lands in addBytes.
			name: "query params truncated at a bound value",
			frame: frameV4(opExecute, 0x00, []byte{
				0x00, 0x03, 0xAA, 0xBB, 0xCC, // preparedID
				0x00, 0x01, // consistency
				byte(frm.FlagValues), // query flags
				0x00, 0x01,           // one value follows, and then nothing does
			}),
		},
		{
			name:  "query frame truncated at the flags",
			frame: frameV4(opQuery, 0x00, append(longString("SELECT * FROM t"), 0x00, 0x01)),
		},
		{
			// The query text's own [long string] length field is incomplete, so the
			// parameters cannot be located at all.
			name:  "query text length truncated",
			frame: frameV4(opQuery, 0x00, []byte{0x00, 0x00, 0x00}),
		},
		{
			// The length parses but announces more text than the frame holds.
			name:  "query text length overruns",
			frame: frameV4(opQuery, 0x00, append([]byte{0x7F, 0xFF, 0xFF, 0xFF}, "SELECT * FROM t"...)),
		},
		{
			// The custom-payload header flag sends the parser through
			// addCustomPayload, which reads its own lengths out of the body.
			name:  "query custom payload truncated",
			frame: frameV4(opQuery, frm.FlagCustomPayload, []byte{0x00, 0x01}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := append([]byte(nil), tc.frame...)

			// These fall back from inside the switch, i.e. with the stream id already
			// blanked — see TestGetFrameHashBoundsTruncatedExecute.
			want := murmur3OfStreamBlanked(tc.frame)

			if got := GetFrameHash(frame, tc.useMetadataID); got != want {
				t.Errorf("GetFrameHash(%s) = %d, want raw-bytes hash %d", tc.name, got, want)
			}
			if !bytes.Equal(frame, tc.frame) {
				t.Errorf("GetFrameHash mutated the frame: got % X, want % X", frame, tc.frame)
			}
		})
	}

	// A control: bounding the walk must not turn a frame that parses into a
	// fallback. The opQuery branch hashes from the body start through the end of the
	// query parameters, so a QUERY whose flags announce no optional field hashes its
	// whole body — query text included.
	t.Run("well-formed query still parses", func(t *testing.T) {
		frame := frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x00))

		got := GetFrameHash(frame, false)
		if want := murmur.Murmur3H1(frame[9:]); got != want {
			t.Errorf("GetFrameHash(QUERY) = %d, want body hash %d", got, want)
		}
	})
}
