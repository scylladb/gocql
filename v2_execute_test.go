package gocql

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func runtimeTestSession(t testing.TB, n int) (*Session, []*driverConn, []net.Conn) {
	t.Helper()
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = nopLogger{}
		return opaqueStorageValue
	}()
	implCluster.SessionData(s).InternalHostSource = &ringDescriber{InternalHosts: map[string]*HostInfo{}}
	implCluster.SessionData(s).InternalPool = &policyConnPool{InternalHostConnPools: map[UUID]*hostConnPool{}}
	var conns []*driverConn
	var peers []net.Conn
	for i := 0; i < n; i++ {
		conn, peer := typedTestConn(t, 4, nil)
		id := UUID{15: byte(i + 1)}
		host := func() *HostInfo {
			opaqueStorageValuex := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = id
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalState = NodeUp
			return opaqueStorageValuex
		}()
		conn.InternalHost = host
		conn.InternalHooks = newConnectionHooks(s)
		implCluster.SessionData(s).InternalHostSource.InternalHosts[id.String()] = host
		picker := newDefaultConnPicker(1)
		picker.Put(conn)
		implCluster.SessionData(s).InternalPool.InternalHostConnPools[id] = &hostConnPool{InternalHost: host, InternalConnPicker: picker}
		conns = append(conns, conn)
		peers = append(peers, peer)
		go conn.InternalServe(conn.InternalCtx)
	}
	return s, conns, peers
}
func runtimeRowsBody(token []byte, values ...string) []byte {
	b := binary.BigEndian.AppendUint32(nil, 2)
	flags := uint32(1)
	if token != nil {
		flags |= 2
	}
	b = binary.BigEndian.AppendUint32(b, flags)
	b = binary.BigEndian.AppendUint32(b, 1)
	if token != nil {
		b = binary.BigEndian.AppendUint32(b, uint32(len(token)))
		b = append(b, token...)
	}
	for _, name := range []string{"ks", "tbl", "value"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(TypeBlob))
	b = binary.BigEndian.AppendUint32(b, uint32(len(values)))
	for _, value := range values {
		b = binary.BigEndian.AppendUint32(b, uint32(len(value)))
		b = append(b, value...)
	}
	return b
}
func runtimeQuery(t testing.TB) *Bound[Bytes] {
	t.Helper()
	b, err := BindUnpreparedBorrowed(Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), Unit{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestV2RuntimePagerEmptyContinuationAndProfileSnapshot(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, body := range [][]byte{runtimeRowsBody([]byte{}), runtimeRowsBody([]byte{1}, "row"), runtimeRowsBody(nil)} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	p := defaultProfile()
	p.PageSize = 7
	profile, _ := NewProfile(p)
	bound := runtimeQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := Run(ctx, s, bound, Options{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	p.PageSize = 11
	profile.Replace(p)
	if implExecution.RowsData(rows).InternalRequest.InternalOptions.InternalProfile.PageSize != 7 {
		t.Fatal("profile changed after admission")
	}
	bound.Close()
	if !rows.Next() || string(rows.Row().OwnedBytes()) != "row" {
		t.Fatal(rows.Err())
	}
	retained, err := rows.Retain()
	if err != nil {
		t.Fatal(err)
	}
	view := rows.Row()
	if rows.Next() {
		t.Fatal("unexpected row")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err = bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if string(view.OwnedBytes()) != "row" {
		t.Fatal("retained page lost")
	}
	retained.Close()
	<-done
}
func TestV2RuntimeRepeatedPagingState(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	go func() {
		for _, value := range []string{"a", "b"} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte{1}, value)))
		}
	}()
	bound := runtimeQuery(t)
	defer bound.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := Run(ctx, s, bound, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 || !errors.Is(rows.Err(), ErrPagingNoProgress) {
		t.Fatal(count, rows.Err())
	}
}

type runtimePolicy struct {
	targets []Target
	events  atomic.Int32
}

func (p *runtimePolicy) Validate(*MetadataSnapshot) error { return nil }
func (p *runtimePolicy) Plan(RoutingInfo, *MetadataSnapshot) QueryPlan {
	return &targetPlan{InternalTargets: append([]Target(nil), p.targets...)}
}
func (p *runtimePolicy) Observe(AttemptEvent) { p.events.Add(1) }

type runtimeSpeculation struct{}

func (runtimeSpeculation) Delay(attempt int, _ MetricsSnapshot) (time.Duration, bool) {
	return time.Millisecond, attempt == 1
}
func TestV2RuntimeSpeculativeLoserWriterKeepsInput(t *testing.T) {
	for _, pageTimeout := range []time.Duration{0, time.Second} {
		t.Run(pageTimeout.String(), func(t *testing.T) {
			runtimeSpeculativeLoserWriterKeepsInput(t, pageTimeout)
		})
	}
}

func runtimeSpeculativeLoserWriterKeepsInput(t *testing.T, pageTimeout time.Duration) {
	s, conns, peers := runtimeTestSession(t, 2)
	policy := &runtimePolicy{targets: []Target{{HostID: implEndpoint.InternalHostInfoHostUUID(conns[0].InternalHost)}, {HostID: implEndpoint.InternalHostInfoHostUUID(conns[1].InternalHost)}}}
	profile := defaultProfile()
	profile.Routing = policy
	profile.Speculation = runtimeSpeculation{}
	profile.PageTimeout = pageTimeout
	handle, _ := NewProfile(profile)
	go func() {
		stream := typedTestReadRequest(t, conns[1], peers[1])
		_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "winner")))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	bound := runtimeQuery(t)
	page, err := RunPage(ctx, s, bound, StartPage(), Options{Profile: handle, Idempotent: true, CustomPayload: []PayloadEntry{{Key: "borrowed", Value: Nullable[Bytes]{Valid: true, Value: BorrowBytes([]byte("per-call-payload"))}}}})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Next() || string(page.Row().OwnedBytes()) != "winner" {
		t.Fatal(page.Err())
	}
	bound.Close()
	page.Close()
	short, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	if !errors.Is(bound.Wait(short), context.DeadlineExceeded) {
		t.Fatal("Wait released loser while its writer was blocked")
	}
	stream := typedTestReadRequest(t, conns[0], peers[0])
	go func() { _, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "loser"))) }()
	if err = bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if policy.events.Load() != 2 {
		t.Fatalf("attempt events %d", policy.events.Load())
	}
}

// A page deadline cancels accepted attempts and drains their input without
// cancelling the caller's context or releasing an orphan's stream too early.
func TestV2RuntimeBufferedPageDeadlinePreservesCaller(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	requestRead := make(chan int, 1)
	go func() {
		requestRead <- typedTestReadRequest(t, conns[0], peers[0])
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bound := runtimeQuery(t)
	page, err := RunPage(ctx, s, bound, StartPage(), Options{
		PageTimeout: Option[time.Duration]{Set: true, Value: 20 * time.Millisecond},
	})
	if page != nil {
		page.Close()
		t.Fatal("page returned without a response")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("page deadline: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("page deadline cancelled caller: %v", ctx.Err())
	}
	stream := <-requestRead
	bound.Close()
	go func() {
		_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "late")))
	}()
	if err := bound.Wait(ctx); err != nil {
		t.Fatalf("accepted attempt did not drain: %v", err)
	}
}

type runtimeDelayedRetry struct {
	delay     time.Duration
	scheduled chan<- struct{}
}

func (p runtimeDelayedRetry) Decide(e AttemptError) RetryDecision {
	if e.Attempt >= 3 {
		return RetryDecision{}
	}
	if p.scheduled != nil {
		p.scheduled <- struct{}{}
	}
	action := RetrySame
	if e.Attempt == 2 {
		action = RetryNext
	}
	return RetryDecision{Action: action, Delay: p.delay}
}

func TestV2RuntimeDelayedRetries(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	profile := defaultProfile()
	profile.Retry = runtimeDelayedRetry{delay: time.Millisecond}
	handle := codecTestMust(NewProfile(profile))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for attempt := 0; attempt < 3; attempt++ {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			response := step03ErrorResponse(t, stream, ErrCodeOverloaded, nil)
			if attempt == 2 {
				response = typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "retried"))
			}
			if _, err := peers[0].Write(response); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bound := runtimeQuery(t)
	page, err := RunPage(ctx, s, bound, StartPage(), Options{Profile: handle})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Next() || string(page.Row().OwnedBytes()) != "retried" {
		t.Fatal("delayed retry result", page.Err())
	}
	page.Close()
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal("delayed retries did not drain", err)
	}
	<-done
	if got := s.Metrics(); got.Attempts != 3 || got.Retries != 2 {
		t.Fatalf("delayed retry accounting: %+v", got)
	}
}

func TestV2RuntimeDelayedRetryCancellationDrains(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	scheduled := make(chan struct{}, 1)
	profile := defaultProfile()
	profile.Retry = runtimeDelayedRetry{delay: time.Hour, scheduled: scheduled}
	handle := codecTestMust(NewProfile(profile))
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, _ = peers[0].Write(step03ErrorResponse(t, stream, ErrCodeOverloaded, nil))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bound := runtimeQuery(t)
	finished := make(chan error, 1)
	go func() {
		page, err := RunPage(ctx, s, bound, StartPage(), Options{Profile: handle})
		if page != nil {
			page.Close()
		}
		finished <- err
	}()
	deadline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	select {
	case <-scheduled:
	case <-deadline.Done():
		t.Fatal("retry was not scheduled")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled retry: %v", err)
		}
	case <-deadline.Done():
		t.Fatal("cancelled delayed retry did not return")
	}
	bound.Close()
	if err := bound.Wait(deadline); err != nil {
		t.Fatal("cancelled delayed retry retained input", err)
	}
	if got := s.Metrics(); got.Attempts != 1 || got.Retries != 0 {
		t.Fatalf("cancelled delay launched an attempt: %+v", got)
	}
}
