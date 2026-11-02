//go:build scyllago

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	g "github.com/scylladb/scylla-go-driver"
	"github.com/scylladb/scylla-go-driver/frame"
	glog "github.com/scylladb/scylla-go-driver/log"
	"github.com/scylladb/scylla-go-driver/transport"
)

const driverName = "scylla-go-driver"

var stockScyllaIterator = flag.Bool("scylla-stock-iterator", false, "use upstream iterator with its mandatory prefetch")
var scyllaDebug = flag.Bool("scylla-debug", false, "log upstream setup diagnostics")

type scyllaAdapter struct {
	s             *g.Session
	cancel        context.CancelFunc
	host          string
	config        transport.ConnConfig
	mu            sync.Mutex
	reads, writes [2]*g.Query
}
type scyllaWorker struct {
	read, write             g.Query
	readValues, writeValues []frame.Value
	key, rowNumber          [4]byte
	page                    func(context.Context, frame.Bytes) (g.Result, error)
	out                     row
	paged                   bool
}

func connect(hosts []string) (adapter, error) {
	c := g.DefaultSessionConfig("bench_compare", hosts...)
	c.DefaultConsistency = frame.LOCALQUORUM
	c.Timeout = 10 * time.Second
	c.HostSelectionPolicy = transport.NewTokenAwarePolicy("datacenter1")
	c.RetryPolicy = transport.NewFallthroughRetryPolicy()
	c.AutoAwaitSchemaAgreementTimeout = 0
	if *scyllaDebug {
		c.Logger = glog.NewDebugLogger()
	}
	// The session context is its lifetime, not a setup-only context.
	ctx, cancel := context.WithCancel(context.Background())
	s, err := g.NewSession(ctx, c)
	if err != nil {
		cancel()
		return nil, err
	}
	return &scyllaAdapter{s: s, cancel: cancel, host: hosts[0], config: c.ConnConfig}, nil
}
func (a *scyllaAdapter) Close() error { a.s.Close(); a.cancel(); return nil }
func (a *scyllaAdapter) Exec(ctx context.Context, q string) error {
	// Outside measurements: upstream NTS leaves unbound-query fallback lists
	// empty. Control/cleanup statements use the public connection transport.
	conn, err := transport.OpenConn(ctx, a.host, nil, a.config)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Query(ctx, transport.Statement{Content: q, Consistency: frame.LOCALQUORUM}, nil)
	return err
}
func (a *scyllaAdapter) Evidence() any {
	return map[string]any{"protocol": 4, "consistency": "LOCAL_QUORUM", "page_size": 32, "prefetch": map[bool]string{true: "upstream mandatory", false: "0, explicit pages through Query.Exec"}[*stockScyllaIterator], "retries": 0, "tls": false, "compression": "none", "writer": "unmodified upstream coalescing, up to 100 requests and 1ms idle queue sleep; no disable API", "access_shim": "benchmark-only raw int32/blob binding, prepared consistency, query clone and page-state forwarding; all existing upstream files unchanged"}
}
func (a *scyllaAdapter) Worker(paged, stream bool) (worker, error) {
	if stream {
		return nil, errors.New("scylla-go-driver has no incremental stream API")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	i := 0
	rd, wr := pointRead, pointWrite
	if paged {
		i = 1
		rd, wr = pageRead, pageWrite
	}
	if a.reads[i] == nil {
		q, err := a.s.Prepare(context.Background(), rd)
		if err != nil {
			return nil, err
		}
		a.reads[i] = &q
	}
	if a.writes[i] == nil {
		q, err := a.s.Prepare(context.Background(), wr)
		if err != nil {
			return nil, err
		}
		a.writes[i] = &q
	}
	w := &scyllaWorker{read: a.reads[i].BenchmarkClone(), write: a.writes[i].BenchmarkClone(), paged: paged}
	w.read.SetPageSize(32)
	w.write.SetPageSize(32)
	w.readValues = w.read.BenchmarkValues(frame.LOCALQUORUM)
	w.writeValues = w.write.BenchmarkValues(frame.LOCALQUORUM)
	w.readValues[0] = frame.Value{N: 4, Bytes: w.key[:]}
	w.writeValues[0] = frame.Value{N: 4, Bytes: w.key[:]}
	w.writeValues[1] = frame.Value{N: 4, Bytes: w.rowNumber[:]}
	if _, ok := w.read.BenchmarkRoutingToken(); !ok {
		return nil, fmt.Errorf("prepared read lacks routing token")
	}
	if paged && !*stockScyllaIterator {
		w.page = w.read.BenchmarkPageExecutor()
	}
	return w, nil
}
func (w *scyllaWorker) Write(ctx context.Context, k, r int32, p []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	binary.BigEndian.PutUint32(w.key[:], uint32(k))
	binary.BigEndian.PutUint32(w.rowNumber[:], uint32(r))
	w.writeValues[2] = frame.Value{N: int32(len(p)), Bytes: p}
	_, err := w.write.Exec(ctx)
	return err
}
func (w *scyllaWorker) consume(cells frame.Row, consume consumer) error {
	if len(cells) != 3 {
		return fmt.Errorf("expected 3 cells, got %d", len(cells))
	}
	k, err := cells[0].AsInt32()
	if err != nil {
		return err
	}
	r, err := cells[1].AsInt32()
	if err != nil {
		return err
	}
	if cells[2].Type == nil || cells[2].Type.ID != frame.BlobID {
		return errors.New("expected blob cell")
	}
	// Upstream ParseRowsResult has already copied every cell into owned memory.
	w.out.K, w.out.R, w.out.P = k, r, cells[2].Value
	return consume(&w.out)
}
func (w *scyllaWorker) Read(ctx context.Context, k int32, consume consumer) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	binary.BigEndian.PutUint32(w.key[:], uint32(k))
	if w.paged && *stockScyllaIterator {
		it := w.read.Iter(ctx)
		defer it.Close()
		for {
			cells, err := it.Next()
			if errors.Is(err, g.ErrNoMoreRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if err = w.consume(cells, consume); err != nil {
				return err
			}
		}
	}
	var state frame.Bytes
	for {
		var res g.Result
		var err error
		if w.page != nil {
			res, err = w.page(ctx, state)
		} else {
			res, err = w.read.Exec(ctx)
		}
		if err != nil {
			return err
		}
		for _, cells := range res.Rows {
			if err = w.consume(cells, consume); err != nil {
				return err
			}
		}
		if !res.HasMorePages {
			return nil
		}
		if w.page == nil {
			return errors.New("unexpected point continuation")
		}
		state = res.PagingState
	}
}
