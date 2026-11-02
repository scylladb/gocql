//go:build !old

package main

import (
	"context"
	"errors"
	"fmt"
	g "github.com/scylladb/gocql/v2"
	"sync"
	"time"
)

const driverName = "v2"

type borrowedRow struct {
	K, R int32
	P    g.Bytes
}
type v2Adapter struct {
	s      *g.Session
	mu     sync.Mutex
	reads  [2]*g.Prepared[int32, borrowedRow]
	writes [2]*g.Prepared[row, g.Unit]
}
type v2Worker struct {
	a        *v2Adapter
	read     *g.Prepared[int32, borrowedRow]
	write    *g.Prepared[row, g.Unit]
	stream   bool
	out      row
	borrowed borrowedRow
}

var streamDecoder = g.StreamRow(func(*row) {}, g.StreamColumn("k", g.StreamInt32, func(r *row) *int32 { return &r.K }), g.StreamColumn("r", g.StreamInt32, func(r *row) *int32 { return &r.R }), g.StreamColumn("p", g.StreamBlob, func(r *row) *[]byte { return &r.P }))
var bufferedDecoder = g.Row(g.Column("k", g.Int32, func(r *borrowedRow) *int32 { return &r.K }), g.Column("r", g.Int32, func(r *borrowedRow) *int32 { return &r.R }), g.Column("p", g.BlobCodec, func(r *borrowedRow) *g.Bytes { return &r.P }))

func connect(hosts []string) (adapter, error) {
	c := g.DefaultConfig(hosts...)
	c.Protocol = 4
	c.Metadata.PeriodicRefresh = *metadataRefresh
	c.Pool.ConnectionsPerShard = 1
	c.Pool.MaxExcessPerShard = 0
	p := c.DefaultProfile.Snapshot()
	p.Consistency = g.LocalQuorum
	p.SerialConsistency = g.Option[g.Consistency]{}
	p.Timeout = 10 * time.Second
	p.PageTimeout = 10 * time.Second
	p.PageSize = 32
	p.Prefetch = 0
	p.Retry = g.SafeRetryPolicy{MaxAttempts: 1}
	p.Speculation = nil
	p.Routing, _ = g.NewRoutingPolicy(g.RoutingOptions{TokenAware: true, Datacenter: "datacenter1"})
	if e := c.DefaultProfile.Replace(p); e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, e := g.Connect(ctx, c)
	if e != nil {
		return nil, e
	}
	// Populate table/partitioner metadata before creating immutable prepared handles.
	if e = s.RefreshMetadata(ctx); e != nil {
		s.Close()
		return nil, e
	}
	return &v2Adapter{s: s}, nil
}
func (a *v2Adapter) Close() error { return a.s.Close() }
func (a *v2Adapter) Exec(ctx context.Context, q string) error {
	b, e := g.BindUnpreparedOwned(g.Statement[g.Unit, g.Unit]{CQL: q, Params: g.NoParams, Rows: g.NoRows}, g.NewColumns(), g.Unit{})
	if e != nil {
		return e
	}
	defer b.Close()
	_, e = g.Exec(ctx, a.s, b, g.Options{})
	return e
}
func (a *v2Adapter) Evidence() any {
	routing := []g.RoutingInfo{}
	for _, prepared := range a.reads {
		if prepared != nil {
			b, e := g.BindBorrowed(prepared, int32(0))
			if e == nil {
				routing = append(routing, b.Routing())
				drain(b)
			}
		}
	}
	keyspace, _ := a.s.Metadata().Keyspace("bench_compare")
	return map[string]any{"keyspace_strategy": keyspace.StrategyOptions, "keyspace_strategy_class": keyspace.StrategyClass, "metrics": a.s.Metrics(), "routing_samples": routing, "hosts": a.s.Metadata().Hosts(), "protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": 32, "prefetch": 0, "retries": 0, "compression": "none", "tls": false, "coalesce_wait_ns": 0, "metadata_refresh_ns": int64(*metadataRefresh)}
}
func (a *v2Adapter) Worker(paged, stream bool) (worker, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	idx := 0
	rd, wr := pointRead, pointWrite
	size := 1024
	if paged {
		idx = 1
		rd, wr = pageRead, pageWrite
		size = 4096
	}
	ctx := context.Background()
	if a.reads[idx] == nil {
		p, e := g.Prepare(ctx, a.s, g.Statement[int32, borrowedRow]{CQL: rd, Params: g.Params(g.Param("k", g.Int32, func(k int32) int32 { return k })), Rows: bufferedDecoder}, g.PrepareOptions{})
		if e != nil {
			return nil, e
		}
		a.reads[idx] = p
	}
	if a.writes[idx] == nil {
		p, e := g.Prepare(ctx, a.s, g.Statement[row, g.Unit]{CQL: wr, Params: g.Params(g.Param("k", g.Int32, func(r row) int32 { return r.K }), g.Param("r", g.Int32, func(r row) int32 { return r.R }), g.Param("p", g.BytesEncoder, func(r row) []byte { return r.P })), Rows: g.NoRows}, g.PrepareOptions{})
		if e != nil {
			return nil, e
		}
		a.writes[idx] = p
	}
	probe, e := g.BindBorrowed(a.reads[idx], int32(0))
	if e != nil {
		return nil, e
	}
	routing := probe.Routing()
	if e = drain(probe); e != nil {
		return nil, e
	}
	if !routing.Token.Set {
		return nil, fmt.Errorf("prepared read lacks token routing for %s", rd)
	}
	return &v2Worker{a: a, read: a.reads[idx], write: a.writes[idx], stream: stream, out: row{P: make([]byte, 0, size)}}, nil
}
func drain[R any](b *g.Bound[R]) error {
	b.Close()
	return b.Wait(context.Background())
}

func (w *v2Worker) Write(ctx context.Context, k, r int32, p []byte) error {
	b, e := g.BindBorrowed(w.write, row{K: k, R: r, P: p})
	if e != nil {
		return e
	}
	_, e = g.Exec(ctx, w.a.s, b, g.Options{IncludeMetadata: true})
	return errors.Join(e, drain(b))
}
func (w *v2Worker) Read(ctx context.Context, k int32, consume consumer) (resultErr error) {
	b, e := g.BindBorrowed(w.read, k)
	if e != nil {
		return e
	}
	defer func() { resultErr = errors.Join(resultErr, drain(b)) }()
	if w.stream {
		rs, e := g.RunStream(ctx, w.a.s, b, streamDecoder, g.StreamOptions{})
		if e != nil {
			return e
		}
		defer func() { resultErr = errors.Join(resultErr, rs.Close()) }()
		for rs.NextInto(&w.out) {
			if e := consume(&w.out); e != nil {
				return e
			}
		}
		return rs.Err()
	}
	rs, e := g.Run(ctx, w.a.s, b, g.Options{})
	if e != nil {
		return e
	}
	defer func() { resultErr = errors.Join(resultErr, rs.Close()) }()
	for rs.NextInto(&w.borrowed) {
		n := int(w.borrowed.P.Len())
		if n > cap(w.out.P) {
			return fmt.Errorf("blob capacity %d < %d", cap(w.out.P), n)
		}
		w.out.K = w.borrowed.K
		w.out.R = w.borrowed.R
		w.out.P = w.out.P[:n]
		if _, e := w.borrowed.P.CopyTo(w.out.P); e != nil {
			return e
		}
		if e := consume(&w.out); e != nil {
			return e
		}
	}
	return rs.Err()
}
