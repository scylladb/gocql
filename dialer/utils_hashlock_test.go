package dialer

import (
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/murmur"
)

// This file pins GetFrameHash's output for a spread of real frame shapes, as a
// blast-radius detector for changes to the offset math.
//
// It is deliberately not a claim that these numbers may never change. The
// recordings under tests/bench store raw frames and are rehashed at load time
// (loadResponseFramesFromFiles) with the same function applied to the live request,
// so the hash function itself can change freely. What a recording actually depends
// on is narrower, and neither half is visible in a single hash value:
//
//   - the hash must not cover bytes that differ between record time and replay
//     time — the default timestamp is time.Now() at send, and the STARTUP option
//     map grows as the driver gains options — or a recording stops matching itself;
//   - the hash must cover enough of a request to tell it apart from the other
//     requests in the same recording, or the replayer's first-match scan serves the
//     wrong response.
//
// Those two properties are asserted directly elsewhere in this package. What this
// table adds is that any edit to the walk has to state which frame shapes it moved:
// a diff here is the change describing itself, which is how the QUERY collision in
// scylladb/gocql#1000 was found.
//
// So a moved value is not automatically a bug — but it is never incidental, and an
// unexplained one is.

// murmur3OfStreamBlanked is the raw-bytes fallback the arms of the opcode switch
// produce. They run after the stream id has been blanked and before the deferred
// restore, so the bytes they hash are the frame with 0x30 0x30 at offsets 2 and 3 —
// not the frame as the caller passed it, which is what the two guards ahead of the
// switch hash. The two are not interchangeable.
func murmur3OfStreamBlanked(frame []byte) int64 {
	normalised := append([]byte(nil), frame...)
	normalised[2], normalised[3] = byte('0'), byte('0')
	return murmur.Murmur3H1(normalised)
}

// longString encodes a CQL [long string]: a 4-byte length followed by the bytes.
func longString(s string) []byte {
	return append([]byte{byte(len(s) >> 24), byte(len(s) >> 16), byte(len(s) >> 8), byte(len(s))}, s...)
}

// shortString encodes a CQL [string]: a 2-byte length followed by the bytes.
func shortString(s string) []byte {
	return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...)
}

// bytesValue encodes a CQL [bytes]: a 4-byte signed length followed by the bytes.
func bytesValue(b []byte) []byte {
	return append([]byte{byte(len(b) >> 24), byte(len(b) >> 16), byte(len(b) >> 8), byte(len(b))}, b...)
}

// queryBody assembles a QUERY body: the query as a [long string], then the query
// parameters — consistency, a one-byte flags field (v4 and below), and whatever
// optional fields those flags announce.
func queryBody(query string, flags byte, optional ...[]byte) []byte {
	body := longString(query)
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, flags)
	for _, o := range optional {
		body = append(body, o...)
	}
	return body
}

// batchBody assembles a minimal BATCH body: type, one query count, one simple
// query with no values, consistency, and a flags byte plus whatever it announces.
func batchBody(flags byte, optional ...[]byte) []byte {
	body := []byte{0x00}            // type: LOGGED
	body = append(body, 0x00, 0x01) // one query
	body = append(body, 0x00)       // kind: query string
	body = append(body, longString("INSERT INTO t (pk) VALUES (1)")...)
	body = append(body, 0x00, 0x00) // zero values
	body = append(body, 0x00, 0x01) // consistency ONE
	body = append(body, flags)
	for _, o := range optional {
		body = append(body, o...)
	}
	return body
}

type hashLockCase struct {
	name          string
	frame         []byte
	useMetadataID bool
}

// hashLockCases is the frame spread the lock covers. Every arm of the
// GetFrameHash opcode switch is represented, along with the query-parameter
// flags that move the extracted range and the truncation paths that fall back to
// hashing raw bytes.
func hashLockCases() []hashLockCase {
	customPayload := func() []byte {
		body := []byte{0x00, 0x01} // one entry
		body = append(body, shortString("k")...)
		body = append(body, bytesValue([]byte{0xAB, 0xCD})...)
		return append(body, queryBody("SELECT * FROM system.local", 0x00)...)
	}

	return []hashLockCase{
		{name: "options", frame: frameV4(opOptions, 0x00, nil)},
		{name: "startup-short", frame: startupFrameV4([]byte{0x00, 0x01})},
		{name: "startup-long", frame: startupFrameV4(make([]byte, 512))},
		{name: "prepare", frame: frameV4(opPrepare, 0x00, longString("SELECT v FROM t WHERE pk = ?"))},
		{name: "register", frame: frameV4(opRegister, 0x00, append([]byte{0x00, 0x01}, shortString("TOPOLOGY_CHANGE")...))},
		{name: "auth-response", frame: frameV4(opAuthResponse, 0x00, bytesValue([]byte{0xDE, 0xAD, 0xBE, 0xEF}))},

		{name: "query-bare", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM system.local", 0x00))},
		{name: "query-values", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?", 0x01,
			[]byte{0x00, 0x01}, bytesValue([]byte{0x00, 0x00, 0x00, 0x2A})))},
		{name: "query-null-value", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?", 0x01,
			[]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF}))},
		{name: "query-page-size", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x04,
			[]byte{0x00, 0x00, 0x13, 0x88}))},
		{name: "query-paging-state", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x08,
			bytesValue([]byte{0x01, 0x02, 0x03})))},
		{name: "query-serial-consistency", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x10,
			[]byte{0x00, 0x09}))},
		{name: "query-default-timestamp", frame: frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x20,
			[]byte{0x00, 0x06, 0x26, 0x52, 0xBF, 0xD6, 0xE5, 0x3F}))},
		{name: "query-values-page-size-serial", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?", 0x01|0x04|0x10,
			[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}), []byte{0x00, 0x00, 0x13, 0x88}, []byte{0x00, 0x09}))},
		{name: "query-named-values", frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = :pk", 0x01|0x40,
			[]byte{0x00, 0x01}, shortString("pk"), bytesValue([]byte{0x2A})))},
		{name: "query-custom-payload", frame: frameV4(opQuery, frm.FlagCustomPayload, customPayload())},

		{name: "execute-no-metadata-id", frame: v4ExecuteFrame(false)},
		{name: "execute-metadata-id", frame: v4ExecuteFrame(true), useMetadataID: true},
		{name: "execute-metadata-id-flag-off", frame: v4ExecuteFrame(true)},

		{name: "batch", frame: frameV4(opBatch, 0x00, batchBody(0x00))},
		{name: "batch-default-timestamp", frame: frameV4(opBatch, 0x00, batchBody(0x20,
			[]byte{0x00, 0x06, 0x26, 0x52, 0xBF, 0xD6, 0xE5, 0x3F}))},

		{name: "v2-options", frame: frameV2(opOptions, nil)},
		{name: "v2-query", frame: frameV2(opQuery, queryBody("SELECT * FROM system.local", 0x00))},

		{name: "truncated-execute-prepared-id-len", frame: v4ExecuteFrame(true)[:10], useMetadataID: true},
		{name: "truncated-execute-metadata-id-len", frame: v4ExecuteFrame(true)[:15], useMetadataID: true},
		{name: "short-header", frame: frameV4(opQuery, 0x00, nil)[:5]},
		{name: "empty", frame: nil},
		{name: "unknown-opcode", frame: frameV4(frameOp(0x7F), 0x00, []byte{0x01, 0x02, 0x03})},
	}
}

// TestGetFrameHashIsStable is the load-bearing regression lock described above.
func TestGetFrameHashIsStable(t *testing.T) {
	for _, tc := range hashLockCases() {
		want, ok := hashLockWant[tc.name]
		if !ok {
			t.Errorf("%s: no pinned hash — add one rather than deleting the case", tc.name)
			continue
		}
		if got := GetFrameHash(tc.frame, tc.useMetadataID); got != want {
			t.Errorf("%s: GetFrameHash = %d, pinned %d", tc.name, got, want)
		}
	}

	if len(hashLockWant) != len(hashLockCases()) {
		t.Errorf("pinned %d hashes for %d cases", len(hashLockWant), len(hashLockCases()))
	}
}

// hashLockWant pins the hash of every case above.
//
// Captured against the implementation as it stood before protocol v5 query-parameter
// support was added, and unchanged by it: the v5 tail walk is gated on v5, so every
// value here — all of them v4 or below — is byte-identical to that baseline.
var hashLockWant = map[string]int64{
	"auth-response":                     -5612286604398787175,
	"batch":                             4614837606640265276,
	"batch-default-timestamp":           2789619218490224104,
	"empty":                             0,
	"execute-metadata-id":               8953623736212654883,
	"execute-metadata-id-flag-off":      -8761464023806847249,
	"execute-no-metadata-id":            -7853075121273079524,
	"options":                           359591853454385582,
	"prepare":                           7009575819196046835,
	"register":                          2208904296610557021,
	"short-header":                      -4915946318166194245,
	"startup-long":                      -4554951338151526776,
	"startup-short":                     -4554951338151526776,
	"truncated-execute-metadata-id-len": 1641695519491433123,
	"truncated-execute-prepared-id-len": -2990148397469877668,
	"unknown-opcode":                    -8681368666721584811,
	"v2-options":                        2835211771060518081,

	// Every QUERY case collapses to Murmur3H1({0x00, 0x00, 0x00}). That is the
	// scylladb/gocql#1000 collision, pinned here as it stands rather than hidden:
	// the parameter walk starts at the body start, so it hashes the top three bytes
	// of the query text's length and nothing else. When #1000 is fixed these values
	// move and become distinct, and that diff is the point of this table.
	"query-bare":                    8779008611884021576,
	"query-custom-payload":          8779008611884021576,
	"query-default-timestamp":       8779008611884021576,
	"query-named-values":            8779008611884021576,
	"query-null-value":              8779008611884021576,
	"query-page-size":               8779008611884021576,
	"query-paging-state":            8779008611884021576,
	"query-serial-consistency":      8779008611884021576,
	"query-values":                  8779008611884021576,
	"query-values-page-size-serial": 8779008611884021576,
	"v2-query":                      8779008611884021576,
}

// frameV5 builds a protocol v5 request frame. The header layout is v3+'s, so only
// the version byte differs from frameV4 — what changes on v5 is the body: the query
// flags become a 4-byte field and EXECUTE always carries a resultMetadataID.
func frameV5(op frameOp, headerFlags byte, body []byte) []byte {
	frame := []byte{
		0x05,
		headerFlags,
		0x00, 0x7B, // stream id
		byte(op),
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}
	return append(frame, body...)
}

// v5ExecuteFrame builds a protocol v5 EXECUTE frame whose query parameters carry
// exactly the fields flags announces, in the order writeQueryParams emits them.
func v5ExecuteFrame(flags uint32, optional ...[]byte) []byte {
	body := []byte{0x00, 0x03, 0xAA, 0xBB, 0xCC} // preparedID
	body = append(body, 0x00, 0x02, 0xDE, 0xAD)  // resultMetadataID, always present on v5
	body = append(body, 0x00, 0x01)              // consistency ONE
	body = append(body, byte(flags>>24), byte(flags>>16), byte(flags>>8), byte(flags))
	for _, o := range optional {
		body = append(body, o...)
	}
	return frameV5(opExecute, 0x00, body)
}

// timestamp8 is an 8-byte default-timestamp field.
func timestamp8(v int64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// nowInSeconds4 is a 4-byte now_in_seconds field.
func nowInSeconds4(v int32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// TestGetFrameHashV5DistinguishesTailFields pins that the protocol v5 keyspace
// override and now_in_seconds are part of a request's identity.
//
// Both sit after the default timestamp in the parameter block, so a walk that stops
// at serial consistency leaves them out and two requests differing only there hash
// the same. The replayer picks the first hash match, so that is not a near miss: it
// serves the response recorded for the other keyspace.
func TestGetFrameHashV5DistinguishesTailFields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags uint32
		a, b  [][]byte
	}{
		{
			name:  "keyspace override",
			flags: frm.FlagWithKeyspace,
			a:     [][]byte{shortString("ks_a")},
			b:     [][]byte{shortString("ks_b")},
		},
		{
			name:  "now_in_seconds",
			flags: frm.FlagWithNowInSeconds,
			a:     [][]byte{nowInSeconds4(1000)},
			b:     [][]byte{nowInSeconds4(2000)},
		},
		{
			// The keyspace sits behind the timestamp, so this only differs from the
			// first case if the walk steps over the timestamp rather than stopping.
			name:  "keyspace override behind a default timestamp",
			flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace,
			a:     [][]byte{timestamp8(111), shortString("ks_a")},
			b:     [][]byte{timestamp8(111), shortString("ks_b")},
		},
		{
			name:  "both, behind a default timestamp",
			flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace | frm.FlagWithNowInSeconds,
			a:     [][]byte{timestamp8(111), shortString("ks"), nowInSeconds4(1000)},
			b:     [][]byte{timestamp8(111), shortString("ks"), nowInSeconds4(2000)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := GetFrameHash(v5ExecuteFrame(tc.flags, tc.a...), false)
			b := GetFrameHash(v5ExecuteFrame(tc.flags, tc.b...), false)
			if a == b {
				t.Errorf("frames differing in %s hash the same (%d); the replayer would serve the wrong response", tc.name, a)
			}
		})
	}
}

// TestGetFrameHashV5IgnoresDefaultTimestamp pins the other half of the contract: the
// default timestamp must stay out of the hash even though the fields behind it are in
// it. It is time.Now() at send time (framer.writeQueryParams), so hashing it would
// make every recorded frame fail to match its own replay.
func TestGetFrameHashV5IgnoresDefaultTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags uint32
		a, b  [][]byte
	}{
		{
			name:  "timestamp alone",
			flags: frm.FlagDefaultTimestamp,
			a:     [][]byte{timestamp8(1)},
			b:     [][]byte{timestamp8(999999)},
		},
		{
			name:  "timestamp ahead of a keyspace override",
			flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace,
			a:     [][]byte{timestamp8(1), shortString("ks")},
			b:     [][]byte{timestamp8(999999), shortString("ks")},
		},
		{
			name:  "timestamp ahead of now_in_seconds",
			flags: frm.FlagDefaultTimestamp | frm.FlagWithNowInSeconds,
			a:     [][]byte{timestamp8(1), nowInSeconds4(7)},
			b:     [][]byte{timestamp8(999999), nowInSeconds4(7)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := GetFrameHash(v5ExecuteFrame(tc.flags, tc.a...), false)
			b := GetFrameHash(v5ExecuteFrame(tc.flags, tc.b...), false)
			if a != b {
				t.Errorf("the default timestamp reached the hash: %d != %d", a, b)
			}
		})
	}
}

// TestGetFrameHashV5BoundsTailFields walks every truncation of a v5 EXECUTE whose
// parameters end in the tail fields. Each length there is file-supplied — a recording
// is JSON on disk — so a truncated one must fall back to hashing the frame rather
// than index past it.
func TestGetFrameHashV5BoundsTailFields(t *testing.T) {
	full := v5ExecuteFrame(frm.FlagDefaultTimestamp|frm.FlagWithKeyspace|frm.FlagWithNowInSeconds,
		timestamp8(111), shortString("ks"), nowInSeconds4(7))

	for n := 0; n < len(full); n++ {
		truncated := append([]byte(nil), full[:n]...)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("GetFrameHash panicked on a %d-byte prefix: %v", n, r)
				}
			}()
			GetFrameHash(truncated, false)
		}()
	}

	// An overstated keyspace length must not be walked either.
	bad := v5ExecuteFrame(frm.FlagWithKeyspace, []byte{0x7F, 0xFF, 'k', 's'})
	if got, want := GetFrameHash(bad, false), murmur3OfStreamBlanked(bad); got != want {
		t.Errorf("overstated keyspace length: GetFrameHash = %d, want the raw-bytes fallback %d", got, want)
	}
}
