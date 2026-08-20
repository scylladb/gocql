//go:build unit
// +build unit

package gocql

import (
	"context"
	"encoding/binary"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// buildResultRowsPayload constructs the minimal binary payload that
// parseResultRows expects: resultMetadata (flags + colCount + optional paging
// state) followed by numRows int.
func buildResultRowsPayload(numRows, colCount int, pagingState []byte) []byte {
	var buf []byte

	// flags (4 bytes)
	flags := 0
	if len(pagingState) > 0 {
		flags |= frm.FlagHasMorePages
	}
	flags |= frm.FlagNoMetaData // skip column specs for simplicity
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(flags))
	buf = append(buf, b...)

	// colCount (4 bytes)
	binary.BigEndian.PutUint32(b, uint32(colCount))
	buf = append(buf, b...)

	// pagingState ([int][bytes])
	if len(pagingState) > 0 {
		binary.BigEndian.PutUint32(b, uint32(len(pagingState)))
		buf = append(buf, b...)
		buf = append(buf, pagingState...)
	}

	// numRows (4 bytes)
	binary.BigEndian.PutUint32(b, uint32(numRows))
	buf = append(buf, b...)

	return buf
}

func TestResultRowsFramePool(t *testing.T) {
	t.Parallel()

	payload := buildResultRowsPayload(100, 5, []byte("page-state-token"))
	f := newFramer(nil, protoVersion4)
	f.buf = payload

	result := f.parseResultRows()
	rows, ok := result.(*resultRowsFrame)
	if !ok {
		t.Fatalf("expected *resultRowsFrame, got %T", result)
	}
	if rows.numRows != 100 {
		t.Fatalf("expected 100 rows, got %d", rows.numRows)
	}
	if rows.meta.colCount != 5 {
		t.Fatalf("expected 5 columns, got %d", rows.meta.colCount)
	}

	// Release back to pool
	rows.release()

	// Get another from pool - should reuse
	payload2 := buildResultRowsPayload(50, 3, nil)
	f2 := newFramer(nil, protoVersion4)
	f2.buf = payload2

	result2 := f2.parseResultRows()
	rows2, ok := result2.(*resultRowsFrame)
	if !ok {
		t.Fatalf("expected *resultRowsFrame, got %T", result2)
	}
	if rows2.numRows != 50 {
		t.Fatalf("expected 50 rows, got %d", rows2.numRows)
	}
	if rows2.meta.colCount != 3 {
		t.Fatalf("expected 3 columns, got %d", rows2.meta.colCount)
	}
	rows2.release()
}

func TestResultRowsFrameReleaseClears(t *testing.T) {
	t.Parallel()

	payload := buildResultRowsPayload(42, 7, []byte("state"))
	f := newFramer(nil, protoVersion4)
	f.buf = payload

	result := f.parseResultRows().(*resultRowsFrame)
	result.release()

	// After release, if we get it back from pool it should be zeroed
	got := resultRowsFramePool.Get().(*resultRowsFrame)
	if got.numRows != 0 {
		t.Fatalf("expected zeroed numRows after release, got %d", got.numRows)
	}
	if got.meta.colCount != 0 {
		t.Fatalf("expected zeroed colCount after release, got %d", got.meta.colCount)
	}
	resultRowsFramePool.Put(got)
}

// BenchmarkParseResultRows_Pooled benchmarks the current pooled implementation.
func BenchmarkParseResultRows_Pooled(b *testing.B) {
	payload := buildResultRowsPayload(1000, 10, []byte("paging-state-0123456789abcdef"))
	f := newFramer(nil, protoVersion4)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.buf = payload
		result := f.parseResultRows()
		result.(*resultRowsFrame).release()
	}
}

// BenchmarkParseResultRows_NoPool benchmarks without pooling (allocates each time).
// Uses a sink to prevent escape analysis from stack-allocating the frame.
var resultRowsFrameSink *resultRowsFrame

func BenchmarkParseResultRows_NoPool(b *testing.B) {
	payload := buildResultRowsPayload(1000, 10, []byte("paging-state-0123456789abcdef"))
	f := newFramer(nil, protoVersion4)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.buf = payload
		result := &resultRowsFrame{}
		result.meta = f.parseResultMetadata()
		result.numRows = f.readInt()
		resultRowsFrameSink = result
	}
}

// --- Part B: Paging Query Pool tests and benchmarks ---

func TestNewNextIterWithPageState(t *testing.T) {
	t.Parallel()

	session := &Session{}
	parent := &Query{
		stmt:        "SELECT * FROM tbl",
		cons:        Quorum,
		pageSize:    1000,
		prefetch:    0.25,
		session:     session,
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
		context:     context.Background(),
	}

	pageState := []byte("next-page-token-abc")
	ni := newNextIterWithPageState(parent, nil, pageState, 750)

	if ni.qry == nil {
		t.Fatal("expected qry to be set")
	}
	if ni.pos != 750 {
		t.Fatalf("expected pos=750, got %d", ni.pos)
	}
	if !ni.pooled {
		t.Fatal("expected pooled=true")
	}
	if string(ni.qry.pageState) != string(pageState) {
		t.Fatalf("expected pageState=%q, got %q", pageState, ni.qry.pageState)
	}
	if ni.qry.stmt != parent.stmt {
		t.Fatalf("expected stmt to be inherited, got %q", ni.qry.stmt)
	}
	if ni.qry.cons != parent.cons {
		t.Fatal("expected consistency to be inherited")
	}
	if ni.qry.metrics == parent.metrics {
		t.Fatal("expected fresh metrics, got same pointer as parent")
	}
	if ni.cancel == nil {
		t.Fatal("expected cancel func to be set")
	}

	// consume now calls releaseQuery which decrements refCount and nils n.qry.
	// The query is pooled when refCount hits 0 (no warningQuery ref in this test).
	ni.consume()
	if ni.qry != nil {
		t.Fatal("expected qry to be nil after consume (releaseQuery decrements refCount)")
	}
}

func TestNextIterCloseReleasesPooledQuery(t *testing.T) {
	t.Parallel()

	parent := &Query{
		stmt:        "SELECT * FROM tbl",
		session:     &Session{},
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
		context:     context.Background(),
	}

	ni := newNextIterWithPageState(parent, nil, []byte("page"), 10)
	ni.close()

	if ni.qry != nil {
		t.Fatal("expected qry to be nil after close")
	}
}

// BenchmarkNewNextIter_Pooled benchmarks the pooled paging query path (close/discard path).
// The pool benefit is realized when iterators are discarded (close path), not consumed.
func BenchmarkNewNextIter_Pooled(b *testing.B) {
	parent := &Query{
		stmt:              "SELECT * FROM tbl WHERE pk = ?",
		cons:              Quorum,
		pageSize:          5000,
		prefetch:          0.25,
		session:           &Session{},
		routingInfo:       &queryRoutingInfo{},
		metrics:           newQueryMetrics(),
		context:           context.Background(),
		pageContextParent: context.Background(),
	}
	pageState := []byte("paging-state-0123456789abcdef")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ni := newNextIterWithPageState(parent, nil, pageState, 3750)
		ni.close()
	}
}

// BenchmarkNewNextIter_NoPool benchmarks the old non-pooled path (Query copy + WithContext).
func BenchmarkNewNextIter_NoPool(b *testing.B) {
	parent := &Query{
		stmt:              "SELECT * FROM tbl WHERE pk = ?",
		cons:              Quorum,
		pageSize:          5000,
		prefetch:          0.25,
		session:           &Session{},
		routingInfo:       &queryRoutingInfo{},
		metrics:           newQueryMetrics(),
		context:           context.Background(),
		pageContextParent: context.Background(),
	}
	pageState := []byte("paging-state-0123456789abcdef")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate the old path: copy Query + new metrics + newNextIter (which copies again via WithContext)
		newQry := new(Query)
		*newQry = *parent
		newQry.pageState = pageState
		newQry.metrics = newQueryMetrics()
		ni := newNextIter(newQry, 3750)
		ni.close()
	}
}

// TestNextIterConcurrentCloseAndFetch exercises the race between close() and
// fetchAsync() to ensure no data race on n.qry under the race detector.
func TestNextIterConcurrentCloseAndFetch(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		parent := &Query{
			stmt:              "SELECT * FROM tbl",
			session:           &Session{},
			routingInfo:       &queryRoutingInfo{},
			metrics:           newQueryMetrics(),
			context:           context.Background(),
			pageContextParent: context.Background(),
		}

		ni := newNextIterWithPageState(parent, nil, []byte("page"), 10)

		// Race fetch (which reads n.qry) against close (which releases it).
		// We call fetch() directly (not fetchAsync) so the WaitGroup captures
		// the actual execution, not just the goroutine launch wrapper.
		wg.Add(2)
		go func() {
			defer wg.Done()
			ni.fetch()
		}()
		go func() {
			defer wg.Done()
			ni.close()
		}()
	}
	wg.Wait()
	// If there's a race, -race will catch it.
}

// TestQueryPoolRoundTripRoutingInfo verifies that a Query released back to the
// pool (via Release/decRefCount) and then re-acquired by Session.Query() gets a
// valid routingInfo with zeroed fields, regardless of prior state.
func TestQueryPoolRoundTripRoutingInfo(t *testing.T) {
	t.Parallel()

	s := &Session{}

	// First query: acquire from pool, mutate routingInfo, then release.
	qry1 := s.Query("SELECT 1")
	if qry1.routingInfo == nil {
		t.Fatal("expected routingInfo to be non-nil on first Query()")
	}
	qry1.routingInfo.keyspace = "dirty_ks"
	qry1.routingInfo.table = "dirty_tbl"
	qry1.Release()

	// Second query: routingInfo must be non-nil with fields zeroed by reset.
	qry2 := s.Query("SELECT 2")
	if qry2.routingInfo == nil {
		t.Fatal("expected routingInfo to be non-nil after pool round-trip")
	}
	if qry2.routingInfo.keyspace != "" {
		t.Fatalf("expected keyspace to be empty after reset, got %q", qry2.routingInfo.keyspace)
	}
	if qry2.routingInfo.table != "" {
		t.Fatalf("expected table to be empty after reset, got %q", qry2.routingInfo.table)
	}
	qry2.Release()
}

// TestNewNextIterWithPageStateReclaimsPooledMetrics verifies that, on a
// second round-trip through the pool, newNextIterWithPageState's
// metrics == nil path restores the owner claim it saved before the parent
// copy overwrote it, so prepareQueryMetrics reclaims pooledMetrics on the
// next round-trip instead of silently abandoning it. sync.Pool does not
// guarantee a *Query is reused across Get/Put, so the identity check only
// runs when that coincidentally happens - which is common in practice and
// is exactly when the bug this guards against would otherwise surface.
func TestNewNextIterWithPageStateReclaimsPooledMetrics(t *testing.T) {
	// Make sync.Pool round-trips deterministic: flush any GC already in
	// flight, then disable further GCs and pin to one P so the pool can't
	// drop or migrate entries mid-test (same technique sync/pool_test.go
	// uses upstream). Also swap in a private pool so the identity checks
	// below aren't perturbed by other tests sharing the package-level
	// queryPool.
	runtime.GC()
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	origPool := queryPool
	queryPool = &sync.Pool{New: queryPool.New}
	defer func() { queryPool = origPool }()

	parent := &Query{
		stmt:        "SELECT * FROM tbl",
		session:     &Session{},
		routingInfo: &queryRoutingInfo{},
		metrics:     newQueryMetrics(),
		context:     context.Background(),
	}

	ni1 := newNextIterWithPageState(parent, nil, []byte("page1"), 1)
	ni1.close()

	ni2 := newNextIterWithPageState(parent, nil, []byte("page2"), 2)
	qry2 := ni2.qry
	reclaimedMetrics := qry2.metrics
	ni2.close()

	ni3 := newNextIterWithPageState(parent, nil, []byte("page3"), 3)
	defer ni3.close()

	if ni3.qry != qry2 {
		t.Skip("sync.Pool did not reuse the same *Query for this round-trip")
	}
	if ni3.qry.metrics != reclaimedMetrics {
		t.Fatalf("expected pooled metrics to be reclaimed, got a new object (%p != %p)", ni3.qry.metrics, reclaimedMetrics)
	}
}

// TestNewNextIterWithPageStateRetainsSharedMetrics covers the automatic-page
// production path (conn.go's executeQueryWithMetrics passing its own
// in-flight metrics through): newNextIterWithPageState must retain() the
// caller-supplied shared metrics rather than resetting or replacing them, and
// close() must release() exactly once so refs balance back to the caller's
// count instead of drifting negative or leaking a ref.
func TestNewNextIterWithPageStateRetainsSharedMetrics(t *testing.T) {
	parent := &Query{
		stmt:        "SELECT * FROM tbl",
		session:     &Session{},
		routingInfo: &queryRoutingInfo{},
		context:     context.Background(),
	}
	metrics := newQueryMetrics()
	metrics.retain() // simulates the in-flight attempt's own reference.
	parent.metrics = metrics

	ni := newNextIterWithPageState(parent, metrics, []byte("page1"), 1)
	if ni.qry.metrics != metrics {
		t.Fatalf("expected shared metrics to be used directly, got a different object (%p != %p)", ni.qry.metrics, metrics)
	}
	if got := metrics.refs.Load(); got != 2 {
		t.Fatalf("expected retain() to bring refs to 2 (attempt + page), got %d", got)
	}

	ni.close()
	if got := metrics.refs.Load(); got != 1 {
		t.Fatalf("expected close() to release its ref back to 1, got %d", got)
	}
}

// Single-host queryMetrics inline/lazy-map behavior (recordHostAdjustment,
// preFilledQueryMetrics, and their benchmarks) is already covered on master —
// see TestQueryMetricsRecordHostAdjustmentTracksTotalsAndHosts and
// BenchmarkQueryMetricsRecordHostAdjustment in session_unit_test.go.
