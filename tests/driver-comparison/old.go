//go:build old

package main

import (
	"context"
	"fmt"
	g "github.com/gocql/gocql"
	"time"
)

const driverName = "old"

type oldAdapter struct{ s *g.Session }
type blob []byte

func (b *blob) UnmarshalCQL(info g.TypeInfo, data []byte) error {
	if info.Type() != g.TypeBlob {
		return fmt.Errorf("unexpected blob type %v", info)
	}
	if len(data) > cap(*b) {
		return fmt.Errorf("blob capacity %d < %d", cap(*b), len(data))
	}
	*b = (*b)[:len(data)]
	copy(*b, data)
	return nil
}

type oldWorker struct {
	read, write         *g.Query
	readArgs, writeArgs []any
	out                 row
	data                blob
	scan                []any
}

func connect(hosts []string) (adapter, error) {
	c := g.NewCluster(hosts...)
	c.ProtoVersion = 4
	c.Consistency = g.LocalQuorum
	c.NumConns = 1
	c.MaxExcessShardConnectionsRate = 0
	c.Timeout = 10 * time.Second
	c.ConnectTimeout = 10 * time.Second
	c.PageSize = 32
	c.WriteCoalesceWaitTime = 0
	c.RetryPolicy = &g.SimpleRetryPolicy{NumRetries: 0}
	c.PoolConfig.HostSelectionPolicy = g.TokenAwareHostPolicy(g.DCAwareRoundRobinPolicy("datacenter1"))
	s, e := c.CreateSession()
	return &oldAdapter{s}, e
}
func (a *oldAdapter) Close() error { a.s.Close(); return nil }
func (a *oldAdapter) Exec(ctx context.Context, q string) error {
	return a.s.Query(q).WithContext(ctx).Exec()
}
func (a *oldAdapter) Evidence() any {
	pools := []any{}
	a.s.IterateHostPools(func(p g.HostPoolInfo) bool {
		pools = append(pools, map[string]any{"host": p.Host().ConnectAddress().String(), "connections": p.GetConnectionCount(), "shards": p.GetShardCount(), "excess": p.GetExcessConnectionCount()})
		return true
	})
	q := a.s.Query(pointRead, int32(0))
	key, e := q.GetRoutingKey()
	q.Release()
	return map[string]any{"routing_key_hex": fmt.Sprintf("%x", key), "routing_key_error": fmt.Sprint(e), "pools": pools, "protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": 32, "prefetch": 0, "retries": 0, "compression": "none", "tls": false, "coalesce_wait_ns": 0, "metadata_refresh_ns": 0}
}
func (a *oldAdapter) Worker(paged, stream bool) (worker, error) {
	rd, wr := pointRead, pointWrite
	size := 1024
	if paged {
		rd, wr = pageRead, pageWrite
		size = 4096
	}
	w := &oldWorker{read: a.s.Query(rd).Prefetch(0), write: a.s.Query(wr).Prefetch(0), readArgs: make([]any, 1), writeArgs: make([]any, 3), data: make(blob, 0, size)}
	w.scan = []any{&w.out.K, &w.out.R, &w.data}
	w.readArgs[0] = int32(0)
	w.read.Bind(w.readArgs...)
	key, e := w.read.GetRoutingKey()
	if e != nil {
		return nil, e
	}
	if len(key) != 4 {
		return nil, fmt.Errorf("prepared read lacks routing key for %s", rd)
	}
	return w, nil
}
func (w *oldWorker) Write(ctx context.Context, k, r int32, p []byte) error {
	w.writeArgs[0] = k
	w.writeArgs[1] = r
	w.writeArgs[2] = p
	w.write = w.write.Bind(w.writeArgs...).WithContext(ctx)
	return w.write.Exec()
}
func (w *oldWorker) Read(ctx context.Context, k int32, consume consumer) error {
	w.readArgs[0] = k
	w.read = w.read.Bind(w.readArgs...).WithContext(ctx)
	iter := w.read.Iter()
	for iter.Scan(w.scan...) {
		w.out.P = []byte(w.data)
		if e := consume(&w.out); e != nil {
			iter.Close()
			return e
		}
	}
	return iter.Close()
}
