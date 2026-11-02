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
	reads  [2]*g.Prepared[int32, row]
	writes [2]*g.Prepared[row, g.Unit]
}
type v2Worker struct {
	a           *v2Adapter
	read        *g.Query[int32, row]
	write       *g.Query[row, g.Unit]
	streamQuery *g.StreamQuery[int32, row]
	stream      bool
	out         row
}

var streamDecoder = g.StreamRow(func(*row) {}, g.StreamColumn("k", g.StreamInt32, func(r *row) *int32 { return &r.K }), g.StreamColumn("r", g.StreamInt32, func(r *row) *int32 { return &r.R }), g.StreamColumn("p", g.StreamBlob, func(r *row) *[]byte { return &r.P }))
var bufferedDecoder = g.RowInto(func(*row) {}, g.Column("k", g.Int32, func(r *row) *int32 { return &r.K }), g.Column("r", g.Int32, func(r *row) *int32 { return &r.R }), g.Column("p", g.OwnedBlobCodec, func(r *row) *[]byte { return &r.P }))

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
	b, e := g.NewUnpreparedQuery(a.s, g.Statement[g.Unit, g.Unit]{CQL: q, Params: g.NoParams, Rows: g.NoRows}, g.NewColumns(), g.Options{})
	if e != nil {
		return e
	}
	defer b.Close()
	_, e = b.Exec(ctx, g.Unit{})
	return e
}
func (a *v2Adapter) Evidence() any {
	routing := []g.RoutingInfo{}
	for _, prepared := range a.reads {
		if prepared != nil {
			b, e := g.NewQuery(prepared, g.Options{})
			if e == nil {
				info, err := b.Routing(int32(0))
				if err == nil {
					routing = append(routing, info)
				}
				b.Close()
			}
		}
	}
	keyspace, _ := a.s.Metadata().Keyspace("bench_compare")
	return map[string]any{"keyspace_strategy": keyspace.StrategyOptions, "keyspace_strategy_class": keyspace.StrategyClass, "metrics": a.s.Metrics(), "routing_samples": routing, "hosts": a.s.Metadata().Hosts(), "protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": 32, "prefetch": 0, "retries": 0, "compression": "none", "tls": false, "coalesce_wait_ns": 0, "incremental_receive": *incremental, "metadata_refresh_ns": int64(*metadataRefresh)}
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
		p, e := g.Prepare(ctx, a.s, g.Statement[int32, row]{CQL: rd, Params: g.Params(g.Param("k", g.Int32, func(k int32) int32 { return k })), Rows: bufferedDecoder}, g.PrepareOptions{})
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
	readQuery, e := g.NewQuery(a.reads[idx], g.Options{})
	if e != nil {
		return nil, e
	}
	routing, e := readQuery.Routing(int32(0))
	if e != nil {
		readQuery.Close()
		return nil, e
	}
	if !routing.Token.Set {
		readQuery.Close()
		return nil, fmt.Errorf("prepared read lacks token routing for %s", rd)
	}
	writeQuery, e := g.NewQuery(a.writes[idx], g.Options{IncludeMetadata: true})
	if e != nil {
		readQuery.Close()
		return nil, e
	}
	streamQuery, e := g.NewStreamQuery(a.reads[idx], streamDecoder, g.StreamOptions{Incremental: *incremental})
	if e != nil {
		readQuery.Close()
		writeQuery.Close()
		return nil, e
	}
	return &v2Worker{a: a, read: readQuery, write: writeQuery, streamQuery: streamQuery, stream: stream, out: row{P: make([]byte, 0, size)}}, nil
}

func (w *v2Worker) Write(ctx context.Context, k, r int32, p []byte) error {
	_, e := w.write.Exec(ctx, row{K: k, R: r, P: p})
	return errors.Join(e, w.write.Wait(ctx))
}
func (w *v2Worker) Read(ctx context.Context, k int32, consume consumer) (resultErr error) {
	if w.stream {
		rs, e := w.streamQuery.Run(ctx, k)
		if e != nil {
			return e
		}
		defer func() { resultErr = errors.Join(resultErr, rs.Close(), w.streamQuery.Wait(ctx)) }()
		for rs.NextInto(&w.out) {
			if e := consume(&w.out); e != nil {
				return e
			}
		}
		return rs.Err()
	}
	rs, e := w.read.Run(ctx, k)
	if e != nil {
		return e
	}
	defer func() { resultErr = errors.Join(resultErr, rs.Close(), w.read.Wait(ctx)) }()
	for rs.NextInto(&w.out) {
		if e := consume(&w.out); e != nil {
			return e
		}
	}
	return rs.Err()
}
