package dialer

import (
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// The tests in this file pin what a QUERY frame's hash has to stand for, which is the
// contract scylladb/gocql#1000 broke.
//
// A QUERY body is the query text as a [long string] followed by the query parameters.
// The parameter walk used to be handed the body start rather than the position past
// the text, so it read the text's 4-byte length as if it were parameters: the first
// two bytes as the consistency and the third as the flags. Those are 0x00 0x00 0x00
// for any query under 16 MiB, so the walk stopped immediately and every QUERY frame
// hashed the same three zero bytes — the text and the bound values were never in the
// hash at all.
//
// That was not a near miss. The replayer scans for the first matching hash, and back
// then over a slice built by iterating a map, so a recording holding two distinct
// queries served an arbitrary one of their responses, chosen differently from run to
// run, with nothing failing. The slice is sorted now, which only makes such a
// collision reproducibly wrong instead of randomly wrong.

// TestGetFrameHashQueryDistinguishesText pins that the query text is part of a
// QUERY's identity, on every protocol version.
func TestGetFrameHashQueryDistinguishesText(t *testing.T) {
	const (
		a = "SELECT * FROM system.local WHERE key='local'"
		b = "SELECT broadcast_address, cluster_name FROM system.local WHERE key='local'"
	)

	for _, tc := range []struct {
		name string
		a, b []byte
	}{
		{
			name: "v4",
			a:    frameV4(opQuery, 0x00, queryBody(a, 0x00)),
			b:    frameV4(opQuery, 0x00, queryBody(b, 0x00)),
		},
		{
			// The pair the checked-in recordings actually turn on: a page size and a
			// default timestamp, as querySystem sends.
			name: "v4 with a page size and a default timestamp",
			a: frameV4(opQuery, 0x00, queryBody(a, byte(frm.FlagPageSize|frm.FlagDefaultTimestamp),
				[]byte{0x00, 0x00, 0x13, 0x88}, timestamp8(111))),
			b: frameV4(opQuery, 0x00, queryBody(b, byte(frm.FlagPageSize|frm.FlagDefaultTimestamp),
				[]byte{0x00, 0x00, 0x13, 0x88}, timestamp8(111))),
		},
		{
			name: "v5",
			a:    v5QueryFrame(a, 0),
			b:    v5QueryFrame(b, 0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if x, y := GetFrameHash(tc.a, false), GetFrameHash(tc.b, false); x == y {
				t.Errorf("two queries differing only in their text hash the same (%d); the replayer would serve the wrong response", x)
			}
		})
	}
}

// TestGetFrameHashQueryDistinguishesValues pins the other half of a QUERY's identity.
// An unprepared statement with bound values is one the driver does send — a batch's
// simple statements and anything with skipPrepare set — and two of them differing only
// in a value are two different requests.
func TestGetFrameHashQueryDistinguishesValues(t *testing.T) {
	const query = "SELECT v FROM t WHERE pk = ?"

	v4 := func(value byte) []byte {
		return frameV4(opQuery, 0x00, queryBody(query, byte(frm.FlagValues),
			[]byte{0x00, 0x01}, bytesValue([]byte{value})))
	}
	v5 := func(value byte) []byte {
		return v5QueryFrame(query, frm.FlagValues, []byte{0x00, 0x01}, bytesValue([]byte{value}))
	}

	for _, tc := range []struct {
		name string
		a, b []byte
	}{
		{name: "v4", a: v4(0x2A), b: v4(0x2B)},
		{name: "v5", a: v5(0x2A), b: v5(0x2B)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if x, y := GetFrameHash(tc.a, false), GetFrameHash(tc.b, false); x == y {
				t.Errorf("two queries differing only in a bound value hash the same (%d)", x)
			}
		})
	}
}

// TestGetFrameHashQueryIgnoresDefaultTimestamp pins the requirement that pulls the
// other way. writeQueryParams fills the default timestamp with time.Now() at send
// time, so a hash covering it differs on every run and a recorded QUERY can never
// match its own replay — which is what happened to every v5 QUERY while the walk
// failed its bound and fell back to hashing the frame whole.
func TestGetFrameHashQueryIgnoresDefaultTimestamp(t *testing.T) {
	const query = "SELECT * FROM system.local WHERE key='local'"

	for _, tc := range []struct {
		name string
		a, b []byte
	}{
		{
			name: "v4",
			a:    frameV4(opQuery, 0x00, queryBody(query, byte(frm.FlagDefaultTimestamp), timestamp8(1))),
			b:    frameV4(opQuery, 0x00, queryBody(query, byte(frm.FlagDefaultTimestamp), timestamp8(999999))),
		},
		{
			name: "v5",
			a:    v5QueryFrame(query, frm.FlagDefaultTimestamp, timestamp8(1)),
			b:    v5QueryFrame(query, frm.FlagDefaultTimestamp, timestamp8(999999)),
		},
		{
			// On v5 the keyspace override sits behind the timestamp, so this only
			// holds if the walk steps over the timestamp rather than stopping at it.
			name: "v5 with a keyspace override behind it",
			a:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithKeyspace, timestamp8(1), shortString("ks")),
			b:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithKeyspace, timestamp8(999999), shortString("ks")),
		},
		{
			// And the same for now_in_seconds, the other field v5 puts behind it.
			name: "v5 with now_in_seconds behind it",
			a:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithNowInSeconds, timestamp8(1), nowInSeconds4(7)),
			b:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithNowInSeconds, timestamp8(999999), nowInSeconds4(7)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if x, y := GetFrameHash(tc.a, false), GetFrameHash(tc.b, false); x != y {
				t.Errorf("the default timestamp reached a QUERY's hash: %d != %d", x, y)
			}
		})
	}
}

// TestGetFrameHashV5QueryDistinguishesTailFields pins that the two fields protocol v5
// adds behind the default timestamp are part of a QUERY's identity, the same way
// TestGetFrameHashV5DistinguishesTailFields pins it for an EXECUTE. A walk that stops
// at serial consistency leaves them out, and the replayer would serve the response
// recorded for the other keyspace.
func TestGetFrameHashV5QueryDistinguishesTailFields(t *testing.T) {
	const query = "SELECT * FROM t"

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
			a := GetFrameHash(v5QueryFrame(query, tc.flags, tc.a...), false)
			b := GetFrameHash(v5QueryFrame(query, tc.flags, tc.b...), false)
			if a == b {
				t.Errorf("v5 queries differing in %s hash the same (%d)", tc.name, a)
			}
		})
	}
}

// TestGetFrameHashQueryBoundsEveryPrefix walks every truncation of a fully-populated
// QUERY. Every length the walk reads is file-supplied — a recording is JSON on disk —
// so a truncated one has to fall back to hashing the frame rather than index past it.
func TestGetFrameHashQueryBoundsEveryPrefix(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{
			name: "v4",
			frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?",
				byte(frm.FlagValues|frm.FlagPageSize|frm.FlagWithSerialConsistency|frm.FlagDefaultTimestamp),
				[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}), []byte{0x00, 0x00, 0x13, 0x88},
				[]byte{0x00, 0x09}, timestamp8(111))),
		},
		{
			name: "v5",
			frame: v5QueryFrame("SELECT v FROM t WHERE pk = ?",
				frm.FlagValues|frm.FlagPageSize|frm.FlagWithSerialConsistency|
					frm.FlagDefaultTimestamp|frm.FlagWithKeyspace|frm.FlagWithNowInSeconds,
				[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}), []byte{0x00, 0x00, 0x13, 0x88},
				[]byte{0x00, 0x09}, timestamp8(111), shortString("ks"), nowInSeconds4(7)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for n := 0; n < len(tc.frame); n++ {
				truncated := append([]byte(nil), tc.frame[:n]...)
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("GetFrameHash panicked on a %d-byte prefix: %v", n, r)
						}
					}()
					GetFrameHash(truncated, false)
				}()
			}
		})
	}

	// A length that overstates the space left must not be walked either, the case a
	// truncation cannot reach: the frame is whole and only the field lies.
	bad := v5QueryFrame("SELECT v FROM t", frm.FlagWithKeyspace, []byte{0x7F, 0xFF, 'k', 's'})
	if got, want := GetFrameHash(bad, false), murmur3OfStreamBlanked(bad); got != want {
		t.Errorf("overstated keyspace length: GetFrameHash = %d, want the raw-bytes fallback %d", got, want)
	}
}
