package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func runtimeTestSession(t testing.TB, n int) (*Session, []*Conn, []net.Conn) {
	t.Helper()
	s := &Session{logger: nopLogger{}}
	s.hostSource = &ringDescriber{hosts: map[string]*HostInfo{}}
	s.pool = &policyConnPool{hostConnPools: map[UUID]*hostConnPool{}}
	var conns []*Conn
	var peers []net.Conn
	for i := 0; i < n; i++ {
		conn, peer := typedTestConn(t, 4, nil)
		id := UUID{15: byte(i + 1)}
		host := &HostInfo{hostId: id, state: NodeUp}
		conn.host = host
		conn.session = s
		s.hostSource.hosts[id.String()] = host
		picker := newDefaultConnPicker(1)
		picker.Put(conn)
		s.pool.hostConnPools[id] = &hostConnPool{host: host, connPicker: picker}
		conns = append(conns, conn)
		peers = append(peers, peer)
		go conn.serve(conn.ctx)
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
	if rows.request.options.profile.PageSize != 7 {
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
	return &targetPlan{targets: append([]Target(nil), p.targets...)}
}
func (p *runtimePolicy) Observe(AttemptEvent) { p.events.Add(1) }

type runtimeSpeculation struct{}

func (runtimeSpeculation) Delay(attempt int, _ MetricsSnapshot) (time.Duration, bool) {
	return time.Millisecond, attempt == 1
}
func TestV2RuntimeSpeculativeLoserWriterKeepsInput(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 2)
	policy := &runtimePolicy{targets: []Target{{HostID: conns[0].host.hostUUID()}, {HostID: conns[1].host.hostUUID()}}}
	profile := defaultProfile()
	profile.Routing = policy
	profile.Speculation = runtimeSpeculation{}
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
