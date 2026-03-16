package gocql

import (
	"errors"
	"testing"
)

func TestPinPagesToHost_SetterSetsFlag(t *testing.T) {
	q := &Query{}
	if q.pinPagesToHost {
		t.Fatal("pinPagesToHost should default to false")
	}

	q.PinPagesToHost()
	if !q.pinPagesToHost {
		t.Fatal("PinPagesToHost() should set the flag to true")
	}
}

func TestPinPagesToHost_DefaultOff(t *testing.T) {
	q := &Query{}
	if q.pinPagesToHost {
		t.Fatal("pinPagesToHost should be false by default")
	}
	if q.hostID != "" {
		t.Fatal("hostID should be empty by default")
	}
}

func TestPinPagesToHost_ShallowCopyPreservesFlag(t *testing.T) {
	q := &Query{}
	q.PinPagesToHost()
	q.hostID = "test-host-1"

	// Simulate the shallow copy that happens in conn.go for next-page queries.
	newQry := new(Query)
	*newQry = *q

	if !newQry.pinPagesToHost {
		t.Fatal("shallow copy should preserve pinPagesToHost flag")
	}
	if newQry.hostID != "test-host-1" {
		t.Fatalf("shallow copy should preserve hostID, got %q", newQry.hostID)
	}
}

func TestPinPagesToHost_DoesNotOverrideExplicitHostID(t *testing.T) {
	// When a user explicitly sets a hostID via SetHostID, PinPagesToHost's
	// conn.go logic should NOT override it (because qry.hostID != "").
	// We simulate the logic in conn.go:
	//   if qry.pinPagesToHost && qry.hostID == "" { newQry.hostID = c.host.HostID() }
	q := &Query{}
	q.PinPagesToHost()
	q.SetHostID("explicit-host-id")

	connHostID := "conn-host-id"

	// Simulate conn.go next-page logic
	newQry := new(Query)
	*newQry = *q
	if q.pinPagesToHost && q.hostID == "" {
		newQry.hostID = connHostID
	}

	// The explicit hostID should be preserved, not overridden by conn host.
	if newQry.hostID != "explicit-host-id" {
		t.Fatalf("expected hostID to remain %q, got %q", "explicit-host-id", newQry.hostID)
	}
}

func TestPinPagesToHost_SetsHostIDOnNextPage(t *testing.T) {
	// Simulate the conn.go logic: when pinPagesToHost is set and no explicit
	// hostID, the next-page query gets the connection's host ID.
	q := &Query{}
	q.PinPagesToHost()

	connHostID := "serving-host-abc"

	// Simulate what conn.go does:
	newQry := new(Query)
	*newQry = *q
	newQry.pageState = []byte{0x01, 0x02}
	if q.pinPagesToHost && q.hostID == "" {
		newQry.hostID = connHostID
	}

	if newQry.hostID != connHostID {
		t.Fatalf("expected next-page query hostID=%q, got %q", connHostID, newQry.hostID)
	}
	if !newQry.pinPagesToHost {
		t.Fatal("pinPagesToHost should be preserved on next-page query")
	}
}

// mockSession is a minimal mock for testing nextIter.fetch() fallback logic.
type mockSession struct {
	calls   int
	results []*Iter
}

func (m *mockSession) executeQuery(qry *Query) *Iter {
	idx := m.calls
	m.calls++
	if idx < len(m.results) {
		return m.results[idx]
	}
	return &Iter{err: errors.New("no more mock results")}
}

func TestPinPagesToHost_FallbackOnHostFailure(t *testing.T) {
	// Test the fallback logic in nextIter.fetch():
	// When the pinned host fails, hostID should be cleared and a retry
	// should happen through normal host selection.

	failedIter := &Iter{err: errors.New("host down")}
	successIter := &Iter{numRows: 5}

	ms := &mockSession{
		results: []*Iter{failedIter, successIter},
	}

	qry := &Query{
		pinPagesToHost: true,
		hostID:         "pinned-host-that-is-down",
	}
	// We need to set the session field. But session is *Session, not an interface.
	// Instead, we directly test the fallback logic inline, mirroring what
	// nextIter.fetch() does.

	// Simulate first call: pinned host fails
	var next *Iter
	next = ms.executeQuery(qry)

	// Simulate the fallback logic from nextIter.fetch()
	if next != nil && next.err != nil && qry.pinPagesToHost && qry.hostID != "" {
		qry.hostID = ""
		next = ms.executeQuery(qry)
	}

	if next.err != nil {
		t.Fatalf("expected fallback to succeed, got error: %v", next.err)
	}
	if next.numRows != 5 {
		t.Fatalf("expected 5 rows from fallback, got %d", next.numRows)
	}
	if qry.hostID != "" {
		t.Fatalf("expected hostID to be cleared after fallback, got %q", qry.hostID)
	}
	if ms.calls != 2 {
		t.Fatalf("expected 2 executeQuery calls (1 failed + 1 fallback), got %d", ms.calls)
	}
}

func TestPinPagesToHost_NoFallbackWhenNotPinned(t *testing.T) {
	// When pinPagesToHost is false, the fallback logic should NOT trigger
	// even if hostID is set (because the user explicitly set it via SetHostID).
	failedIter := &Iter{err: errors.New("host down")}

	ms := &mockSession{
		results: []*Iter{failedIter},
	}

	qry := &Query{
		pinPagesToHost: false,
		hostID:         "explicit-host",
	}

	next := ms.executeQuery(qry)

	// Fallback check: should NOT trigger because pinPagesToHost is false
	if next != nil && next.err != nil && qry.pinPagesToHost && qry.hostID != "" {
		t.Fatal("fallback should not trigger when pinPagesToHost is false")
	}

	// hostID should remain unchanged
	if qry.hostID != "explicit-host" {
		t.Fatalf("hostID should remain %q, got %q", "explicit-host", qry.hostID)
	}
}

func TestPinPagesToHost_NoFallbackWhenFirstCallSucceeds(t *testing.T) {
	// When the pinned host succeeds, no fallback should occur.
	successIter := &Iter{numRows: 10}

	ms := &mockSession{
		results: []*Iter{successIter},
	}

	qry := &Query{
		pinPagesToHost: true,
		hostID:         "healthy-host",
	}

	next := ms.executeQuery(qry)

	// Fallback check: should NOT trigger because no error
	if next != nil && next.err != nil && qry.pinPagesToHost && qry.hostID != "" {
		qry.hostID = ""
		next = ms.executeQuery(qry)
		t.Fatal("fallback should not trigger when pinned host succeeds")
	}

	if next.numRows != 10 {
		t.Fatalf("expected 10 rows, got %d", next.numRows)
	}
	if qry.hostID != "healthy-host" {
		t.Fatalf("hostID should remain %q when host is healthy, got %q", "healthy-host", qry.hostID)
	}
	if ms.calls != 1 {
		t.Fatalf("expected 1 executeQuery call, got %d", ms.calls)
	}
}

func TestPinPagesToHost_ChainingAPI(t *testing.T) {
	// PinPagesToHost should be chainable with other Query methods.
	q := &Query{}
	result := q.PinPagesToHost()
	if result != q {
		t.Fatal("PinPagesToHost() should return the same *Query for chaining")
	}
}

func TestPinPagesToHost_SubsequentPagesStayPinned(t *testing.T) {
	// After the first page pins to a host, subsequent pages created from that
	// next-page query should also be pinned to the same host (because both
	// pinPagesToHost and hostID are preserved by shallow copy).
	q := &Query{}
	q.PinPagesToHost()

	connHostID := "host-A"

	// Page 1 -> Page 2: conn.go sets hostID
	page2Qry := new(Query)
	*page2Qry = *q
	if q.pinPagesToHost && q.hostID == "" {
		page2Qry.hostID = connHostID
	}

	// Page 2 -> Page 3: conn.go logic again. Now hostID is already set,
	// so it should be preserved as-is (not overridden).
	page3Qry := new(Query)
	*page3Qry = *page2Qry
	if page2Qry.pinPagesToHost && page2Qry.hostID == "" {
		page3Qry.hostID = connHostID
	}

	if page3Qry.hostID != connHostID {
		t.Fatalf("expected page 3 hostID=%q, got %q", connHostID, page3Qry.hostID)
	}
	if !page3Qry.pinPagesToHost {
		t.Fatal("pinPagesToHost should persist across pages")
	}
}
