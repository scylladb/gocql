//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

// encodeRoutingKeyComponent is the composite routing key layout createRoutingKey
// produces for one partition key component: a 2-byte big-endian length, the
// marshaled value, and a 0x00 terminator.
func encodeRoutingKeyComponent(b []byte) []byte {
	out := binary.BigEndian.AppendUint16(nil, uint16(len(b)))
	out = append(out, b...)
	return append(out, 0x00)
}

func int32Bytes(v int32) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(v))
}

func TestCreateRoutingKey(t *testing.T) {
	t.Parallel()

	intType := NewNativeType(4, TypeInt)
	textType := NewNativeType(4, TypeVarchar)

	long := strings.Repeat("x", 300)

	tests := []struct {
		name   string
		info   *routingKeyInfo
		values []any
		want   []byte
	}{
		{
			name: "NilInfo",
			info: nil,
			want: nil,
		},
		{
			name:   "SingleColumnIsBareValue",
			info:   &routingKeyInfo{indexes: []int{1}, types: []TypeInfo{intType}},
			values: []any{"ignored", int32(7)},
			want:   int32Bytes(7),
		},
		{
			// Components follow partition key order, which need not match the
			// order the values are bound in.
			name:   "CompositeFollowsIndexOrder",
			info:   &routingKeyInfo{indexes: []int{2, 0}, types: []TypeInfo{textType, intType}},
			values: []any{int32(7), "ignored", "ab"},
			want:   append(encodeRoutingKeyComponent([]byte("ab")), encodeRoutingKeyComponent(int32Bytes(7))...),
		},
		{
			// Larger than the 256-byte stack buffer, so the append has to grow
			// onto the heap without losing what was already written.
			name:   "CompositeLargerThanStackBuffer",
			info:   &routingKeyInfo{indexes: []int{0, 1}, types: []TypeInfo{intType, textType}},
			values: []any{int32(1), long},
			want:   append(encodeRoutingKeyComponent(int32Bytes(1)), encodeRoutingKeyComponent([]byte(long))...),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := createRoutingKey(tc.info, tc.values)
			if err != nil {
				t.Fatalf("createRoutingKey: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("routing key = %x, want %x", got, tc.want)
			}
		})
	}

	t.Run("MarshalError", func(t *testing.T) {
		t.Parallel()
		for _, info := range []*routingKeyInfo{
			{indexes: []int{0}, types: []TypeInfo{intType}},
			{indexes: []int{1, 0}, types: []TypeInfo{intType, intType}},
		} {
			if _, err := createRoutingKey(info, []any{"not an int", int32(1)}); err == nil {
				t.Fatalf("indexes %v: expected a marshal error", info.indexes)
			}
		}
	})

	// The returned slice must not alias the stack buffer it was built in.
	t.Run("CompositeResultIsIndependent", func(t *testing.T) {
		t.Parallel()
		info := &routingKeyInfo{indexes: []int{0, 1}, types: []TypeInfo{intType, intType}}
		first, err := createRoutingKey(info, []any{int32(1), int32(2)})
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte(nil), first...)
		if _, err := createRoutingKey(info, []any{int32(3), int32(4)}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, want) {
			t.Fatalf("first key changed to %x after a second call, want %x", first, want)
		}
	})
}

type testPreparedCol struct {
	keyspace, table, name string
	typ                   Type
}

// testPrepared describes a RESULT/Prepared reply. keyspace/table set means a
// global table spec; otherwise each column carries its own.
type testPrepared struct {
	id              uint64
	flags           int
	keyspace, table string
	cols            []testPreparedCol
	pkIndexes       []uint16
}

func (p testPrepared) write(resp *framer, stream int, writeResultMetadataID func(uint64)) {
	resp.writeHeader(0, frm.OpResult, stream)
	resp.writeInt(frm.ResultKindPrepared)
	resp.writeShortBytes(binary.BigEndian.AppendUint64(nil, p.id))
	writeResultMetadataID(p.id)

	global := p.keyspace != ""
	flags := p.flags
	if global {
		flags |= frm.FlagGlobalTableSpec
	}
	resp.writeInt(int32(flags))
	resp.writeInt(int32(len(p.cols)))
	if resp.proto >= protoVersion4 {
		resp.writeInt(int32(len(p.pkIndexes)))
		for _, idx := range p.pkIndexes {
			resp.writeShort(idx)
		}
	}
	if global {
		resp.writeString(p.keyspace)
		resp.writeString(p.table)
	}
	for _, c := range p.cols {
		if !global {
			resp.writeString(c.keyspace)
			resp.writeString(c.table)
		}
		resp.writeString(c.name)
		resp.writeShort(uint16(c.typ))
	}

	// <result_metadata>
	resp.writeInt(int32(frm.FlagNoMetaData))
	resp.writeInt(0)
}

// routingTestServer is a TestServer answering PREPARE from a fixed table of
// statements and counting how many PREPAREs each one received. Statements not
// in the table get a server error.
type routingTestServer struct {
	*TestServer
	mu       sync.Mutex
	prepared map[string]testPrepared
	counts   map[string]int
}

func newRoutingTestServer(t *testing.T, proto uint8, prepared map[string]testPrepared) *routingTestServer {
	t.Helper()
	return newRoutingTestServerWithSupported(t, proto, prepared, nil)
}

func newRoutingTestServerWithSupported(t *testing.T, proto uint8, prepared map[string]testPrepared, supported map[string][]string) *routingTestServer {
	t.Helper()
	var factory testSupportedFactory
	if supported != nil {
		factory = func(net.Conn) map[string][]string { return supported }
	}
	srv := &routingTestServer{
		TestServer: NewTestServerWithAddressAndSupportedFactory("127.0.0.1:0", t, proto, context.Background(), factory),
		prepared:   prepared,
		counts:     map[string]int{},
	}
	t.Cleanup(srv.Stop)
	srv.setPrepareHook(func(query string, stream int, resp *framer, writeID func(uint64)) bool {
		srv.mu.Lock()
		srv.counts[query]++
		p, ok := srv.prepared[query]
		srv.mu.Unlock()
		if !ok {
			resp.writeHeader(0, frm.OpError, stream)
			resp.writeInt(ErrCodeInvalid)
			resp.writeString("unknown statement: " + query)
			return true
		}
		p.write(resp, stream, writeID)
		return true
	})
	return srv
}

func (srv *routingTestServer) prepareCount(stmt string) int {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.counts[stmt]
}

// routingKeyInfoCached reports whether the routing key cache holds an entry for
// stmt under keyspace. PREPARE counts cannot tell this: the connection's own
// prepared statement cache sits underneath routingKeyInfo.
func routingKeyInfoCached(s *Session, keyspace, stmt string) bool {
	s.routingKeyInfoCache.mu.Lock()
	defer s.routingKeyInfoCache.mu.Unlock()
	_, ok := s.routingKeyInfoCache.lru.Get(routingKeyInfoCacheKey{keyspace: keyspace, stmt: stmt})
	return ok
}

func (srv *routingTestServer) newSession(t *testing.T) *Session {
	t.Helper()
	s, err := srv.session()
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

const (
	routingInsertGlobal = "INSERT INTO ks1.tbl1 (a, b, c) VALUES (?, ?, ?)"
	routingInsertPerCol = "INSERT INTO ks2.tbl2 (a, b) VALUES (?, ?)"
	routingSelectNoArgs = "SELECT * FROM ks1.tbl1"
	routingInsertCDC    = "INSERT INTO ks1.tbl1_scylla_cdc_log (a) VALUES (?)"
	routingInsertNoMeta = "INSERT INTO ks1.missing (a) VALUES (?)"
	routingInsertUnprep = "INSERT INTO ks1.broken (a) VALUES (?)"
)

// routingPrepared serves both protocol paths: on v4+ the partition key comes
// from pkIndexes, on v3 routingKeyInfo has to read it from table metadata.
var routingPrepared = map[string]testPrepared{
	routingInsertGlobal: {
		id:       10,
		keyspace: "ks1", table: "tbl1",
		cols: []testPreparedCol{
			{name: "a", typ: TypeInt},
			{name: "b", typ: TypeVarchar},
			{name: "c", typ: TypeInt},
		},
		// partition key (c, a)
		pkIndexes: []uint16{2, 0},
	},
	routingInsertPerCol: {
		id: 11,
		cols: []testPreparedCol{
			{keyspace: "ks2", table: "tbl2", name: "a", typ: TypeInt},
			{keyspace: "ks2", table: "tbl2", name: "b", typ: TypeInt},
		},
		pkIndexes: []uint16{0},
	},
	routingSelectNoArgs: {
		id:       12,
		keyspace: "ks1", table: "tbl1",
	},
	routingInsertCDC: {
		id:       13,
		keyspace: "ks1", table: "tbl1_scylla_cdc_log",
		cols:      []testPreparedCol{{name: "a", typ: TypeInt}},
		pkIndexes: []uint16{0},
	},
	routingInsertNoMeta: {
		id:       14,
		keyspace: "ks1", table: "missing",
		cols:      []testPreparedCol{{name: "a", typ: TypeInt}},
		pkIndexes: []uint16{0},
	},
}

func TestRoutingKeyInfoFromPreparedPartitionKey(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)
	ctx := context.Background()

	info, err := s.routingKeyInfo(ctx, routingInsertGlobal, "", 0)
	if err != nil {
		t.Fatalf("routingKeyInfo: %v", err)
	}
	if info == nil {
		t.Fatal("routingKeyInfo returned nil for a statement with a partition key")
	}
	if got, want := info.indexes, []int{2, 0}; !slices.Equal(got, want) {
		t.Fatalf("indexes = %v, want %v", got, want)
	}
	for i, typ := range info.types {
		if typ.Type() != TypeInt {
			t.Fatalf("types[%d] = %v, want int", i, typ.Type())
		}
	}
	if info.keyspace != "ks1" || info.table != "tbl1" {
		t.Fatalf("target = %s.%s, want ks1.tbl1", info.keyspace, info.table)
	}
	if info.partitioner != nil {
		t.Fatalf("partitioner = %T, want nil (default) for a non-CDC table", info.partitioner)
	}

	again, err := s.routingKeyInfo(ctx, routingInsertGlobal, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if again != info {
		t.Fatal("second lookup did not return the cached routingKeyInfo")
	}
}

func TestRoutingKeyInfoPerColumnTableSpec(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	info, err := s.routingKeyInfo(context.Background(), routingInsertPerCol, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || info.keyspace != "ks2" || info.table != "tbl2" {
		t.Fatalf("routingKeyInfo = %+v, want target ks2.tbl2 from the column specs", info)
	}
}

func TestRoutingKeyInfoNoBoundColumns(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	for i := 0; i < 2; i++ {
		info, err := s.routingKeyInfo(context.Background(), routingSelectNoArgs, "", 0)
		if err != nil || info != nil {
			t.Fatalf("call %d: routingKeyInfo = %v, %v; want nil, nil", i, info, err)
		}
	}
	// "No routing key" is a result, not an error, so it is cached too.
	if !routingKeyInfoCached(s, "", routingSelectNoArgs) {
		t.Fatal("nil routing key result was not cached")
	}
}

// Every failure must reach the caller and be left out of the cache, so the
// next lookup retries rather than replaying a stale error.
func TestRoutingKeyInfoErrorsAreNotCached(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stmt string
		// seed, when set, populates schema metadata before the lookups.
		seed func(s *Session)
	}{
		{
			name: "PrepareFails",
			stmt: routingInsertUnprep,
		},
		{
			// A CDC log table's partitioner is read from its table metadata;
			// the keyspace is known but the table is not.
			name: "CDCPartitionerLookupFails",
			stmt: routingInsertCDC,
			seed: func(s *Session) {
				s.metadataDescriber.metadata.keyspaceMetadata.set("ks1", &KeyspaceMetadata{
					Name:   "ks1",
					Tables: map[string]*TableMetadata{},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
			s := srv.newSession(t)
			if tc.seed != nil {
				tc.seed(s)
			}

			for i := 0; i < 2; i++ {
				info, err := s.routingKeyInfo(context.Background(), tc.stmt, "", 0)
				if err == nil {
					t.Fatalf("call %d: routingKeyInfo = %v, want an error", i, info)
				}
				if routingKeyInfoCached(s, "", tc.stmt) {
					t.Fatalf("call %d: the error was cached", i)
				}
			}
		})
	}
}

func TestRoutingKeyInfoCDCPartitioner(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)
	s.metadataDescriber.metadata.keyspaceMetadata.set("ks1", &KeyspaceMetadata{
		Name: "ks1",
		Tables: map[string]*TableMetadata{
			"tbl1_scylla_cdc_log": {
				Keyspace: "ks1",
				Name:     "tbl1_scylla_cdc_log",
				Options:  TableMetadataOptions{Partitioner: scyllaCDCPartitionerFullName},
			},
		},
	})

	info, err := s.routingKeyInfo(context.Background(), routingInsertCDC, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := info.partitioner.(scyllaCDCPartitioner); !ok {
		t.Fatalf("partitioner = %T, want scyllaCDCPartitioner", info.partitioner)
	}
}

// Concurrent lookups of one uncached statement must share a single PREPARE
// and all see the same result.
func TestRoutingKeyInfoConcurrentLookupsShareOnePrepare(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	const n = 32
	results := make([]*routingKeyInfo, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = s.routingKeyInfo(context.Background(), routingInsertGlobal, "", 0)
		}(i)
	}
	wg.Wait()

	for i := range results {
		if errs[i] != nil {
			t.Fatalf("lookup %d: %v", i, errs[i])
		}
		if results[i] != results[0] {
			t.Fatalf("lookup %d returned a different routingKeyInfo", i)
		}
	}
	if c := srv.prepareCount(routingInsertGlobal); c != 1 {
		t.Fatalf("statement prepared %d times, want 1", c)
	}
}

// The cache is keyed on the requested keyspace as well as the statement, since
// SetKeyspace can change what an unqualified statement targets.
func TestRoutingKeyInfoCacheKeyIncludesKeyspace(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	byKeyspace := map[string]*routingKeyInfo{}
	for _, ks := range []string{"", "other", "other"} {
		info, err := s.routingKeyInfo(context.Background(), routingInsertGlobal, ks, 0)
		if err != nil {
			t.Fatal(err)
		}
		if prev, ok := byKeyspace[ks]; ok && prev != info {
			t.Fatalf("keyspace %q: repeated lookup missed the cache", ks)
		}
		byKeyspace[ks] = info
	}
	if byKeyspace[""] == byKeyspace["other"] {
		t.Fatal("lookups under different keyspaces shared one cache entry")
	}
}

func TestRoutingKeyInfoNoConnection(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)
	// Hide the hosts rather than marking them down: an asynchronous pool fill
	// can mark a host up again while the lookup is running.
	s.hostSource.mu.Lock()
	hosts := s.hostSource.hosts
	s.hostSource.hosts = nil
	s.hostSource.mu.Unlock()
	restoreHosts := func() {
		s.hostSource.mu.Lock()
		s.hostSource.hosts = hosts
		s.hostSource.mu.Unlock()
	}
	defer restoreHosts()

	for i := 0; i < 2; i++ {
		if _, err := s.routingKeyInfo(context.Background(), routingInsertGlobal, "", 0); err == nil {
			t.Fatal("expected an error with no connection available")
		}
		if routingKeyInfoCached(s, "", routingInsertGlobal) {
			t.Fatal("no-connection error was cached")
		}
	}

	restoreHosts()
	if s.getConn() == nil {
		t.Fatal("expected a connection after restoring hosts")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := s.routingKeyInfo(ctx, routingInsertGlobal, "", 0)
	if err != nil || info == nil {
		t.Fatalf("routingKeyInfo after recovery = %v, %v", info, err)
	}
	if !routingKeyInfoCached(s, "", routingInsertGlobal) {
		t.Fatal("successful routing metadata was not cached")
	}
	want := append(encodeRoutingKeyComponent(int32Bytes(2)), encodeRoutingKeyComponent(int32Bytes(1))...)
	q := s.Query(routingInsertGlobal, int32(1), "b", int32(2))
	if key, err := q.GetRoutingKey(); err != nil || !bytes.Equal(key, want) {
		t.Fatalf("query routing key after recovery = %x, %v; want %x", key, err, want)
	}
	b := s.Batch(LoggedBatch)
	b.Query(routingInsertGlobal, int32(1), "b", int32(2))
	if key, err := b.GetRoutingKey(); err != nil || !bytes.Equal(key, want) {
		t.Fatalf("batch routing key after recovery = %x, %v; want %x", key, err, want)
	}
}

func TestRoutingKeyInfoNoConnectionPreservesReplacement(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)
	s.routingKeyInfoCache.Max(1)
	cacheKey := routingKeyInfoCacheKey{keyspace: s.cfg.Keyspace, stmt: routingInsertGlobal}

	// Block getConn after the inflight entry is inserted, then evict it.
	s.hostSource.mu.Lock()
	hosts := s.hostSource.hosts
	s.hostSource.hosts = nil
	locked := true
	defer func() {
		if !locked {
			s.hostSource.mu.Lock()
		}
		s.hostSource.hosts = hosts
		s.hostSource.mu.Unlock()
	}()
	result := make(chan error, 1)
	go func() {
		_, err := s.routingKeyInfo(context.Background(), routingInsertGlobal, "", 0)
		result <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if routingKeyInfoCached(s, s.cfg.Keyspace, routingInsertGlobal) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lookup did not insert its inflight entry")
		}
		time.Sleep(time.Millisecond)
	}
	replacement := &inflightCachedEntry{value: &routingKeyInfo{}}
	s.routingKeyInfoCache.mu.Lock()
	s.routingKeyInfoCache.lru.Add(routingKeyInfoCacheKey{stmt: "evict original"}, new(inflightCachedEntry))
	s.routingKeyInfoCache.lru.Add(cacheKey, replacement)
	s.routingKeyInfoCache.mu.Unlock()
	s.hostSource.mu.Unlock()
	locked = false

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected the original lookup to fail without a connection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original lookup did not complete")
	}
	s.routingKeyInfoCache.mu.Lock()
	entry, cached := s.routingKeyInfoCache.lru.Get(cacheKey)
	s.routingKeyInfoCache.mu.Unlock()
	if !cached || entry != replacement {
		t.Fatal("failed lookup removed the replacement entry")
	}
}

// Protocol v3 has no partition key indexes in PREPARED, so routingKeyInfo maps
// the table's partition key columns onto the bound columns by name.
func TestRoutingKeyInfoFromTableMetadata(t *testing.T) {
	t.Parallel()

	seed := func(s *Session, partitionKey ...string) {
		pk := make([]*ColumnMetadata, len(partitionKey))
		for i, name := range partitionKey {
			pk[i] = &ColumnMetadata{Keyspace: "ks1", Table: "tbl1", Name: name, Kind: ColumnPartitionKey}
		}
		s.metadataDescriber.metadata.keyspaceMetadata.set("ks1", &KeyspaceMetadata{
			Name: "ks1",
			Tables: map[string]*TableMetadata{
				"tbl1": {Keyspace: "ks1", Name: "tbl1", PartitionKey: pk},
			},
		})
	}

	t.Run("PartitionKeyMappedByName", func(t *testing.T) {
		t.Parallel()
		srv := newRoutingTestServer(t, protoVersion3, routingPrepared)
		s := srv.newSession(t)
		seed(s, "c", "a")

		info, err := s.routingKeyInfo(context.Background(), routingInsertGlobal, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if info == nil {
			t.Fatal("routingKeyInfo returned nil")
		}
		if got, want := info.indexes, []int{2, 0}; !slices.Equal(got, want) {
			t.Fatalf("indexes = %v, want %v", got, want)
		}
		if info.types[0].Type() != TypeInt || info.types[1].Type() != TypeInt {
			t.Fatalf("types = %v, want [int int]", info.types)
		}
	})

	t.Run("PartitionKeyColumnNotBound", func(t *testing.T) {
		t.Parallel()
		srv := newRoutingTestServer(t, protoVersion3, routingPrepared)
		s := srv.newSession(t)
		seed(s, "a", "z")

		info, err := s.routingKeyInfo(context.Background(), routingInsertGlobal, "", 0)
		if err != nil || info != nil {
			t.Fatalf("routingKeyInfo = %v, %v; want nil, nil when a partition key column is not bound", info, err)
		}
	})

	t.Run("TableMetadataMissing", func(t *testing.T) {
		t.Parallel()
		srv := newRoutingTestServer(t, protoVersion3, routingPrepared)
		s := srv.newSession(t)
		seed(s, "a")

		if _, err := s.routingKeyInfo(context.Background(), routingInsertNoMeta, "", 0); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
		if routingKeyInfoCached(s, "", routingInsertNoMeta) {
			t.Fatal("the error was cached")
		}
	})
}

func TestRoutingKeyValueCount(t *testing.T) {
	t.Parallel()

	srv := newBatchTestServer(t, routingPrepared, nil, nil)
	cfg := testCluster(protoVersion4, srv.Address)
	cfg.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	cfg.ConnectTimeout = 5 * time.Second
	cfg.Timeout = 5 * time.Second
	s, err := cfg.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	for _, stmt := range []string{routingInsertPerCol, routingInsertGlobal} {
		expected := len(routingPrepared[stmt].cols)
		for count := 0; count <= expected+1; count++ {
			if count == expected {
				continue
			}
			t.Run(fmt.Sprintf("%dMarkers/%dValues", expected, count), func(t *testing.T) {
				values := make([]any, count)
				for i := range values {
					values[i] = int32(i)
				}
				want := fmt.Sprintf("gocql: expected %d values send got %d", expected, count)
				var wantKey []byte
				if expected == 2 && count >= 1 {
					wantKey = int32Bytes(0)
				} else if expected == 3 && count >= 3 {
					wantKey = append(encodeRoutingKeyComponent(int32Bytes(2)), encodeRoutingKeyComponent(int32Bytes(0))...)
				}
				checkKey := func(key []byte, err error) {
					t.Helper()
					if wantKey != nil {
						if err != nil || !bytes.Equal(key, wantKey) {
							t.Fatalf("routing key = %x, %v; want %x, nil", key, err, wantKey)
						}
					} else if key != nil || err == nil || err.Error() != want {
						t.Fatalf("routing key = %x, %v; want nil, %q", key, err, want)
					}
				}
				q := s.Query(stmt, values...).WithContext(ctx)
				checkKey(q.GetRoutingKey())
				if err := q.Exec(); err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("query execution error = %v; want %q", err, want)
				}
				b := s.Batch(LoggedBatch).WithContext(ctx)
				b.Query(stmt, values...)
				checkKey(b.GetRoutingKey())
				batchWant := fmt.Sprintf("batch statement 0 expected %d values send got %d", expected, count)
				if err := b.Exec(); err == nil || !strings.Contains(err.Error(), batchWant) {
					t.Fatalf("batch execution error = %v; want %q", err, batchWant)
				}
			})
		}
	}
	if got := len(srv.received()); got != 0 {
		t.Fatalf("invalid batches sent to server = %d; want 0", got)
	}
	t.Run("CallbackWithRoutingValues", func(t *testing.T) {
		called := false
		srv.setExecuteHook(func(id uint64, _ *framer, stream int, resp *framer) bool {
			if id != uint64(routingPrepared[routingInsertPerCol].id) {
				return false
			}
			writeVoidResult(resp, stream)
			return true
		})
		q := s.Bind(routingInsertPerCol, func(*QueryInfo) ([]any, error) {
			called = true
			return []any{int32(7), int32(8)}, nil
		}).Bind(int32(7)).WithContext(ctx)
		defer q.Release()
		if key, err := q.GetRoutingKey(); err != nil || !bytes.Equal(key, int32Bytes(7)) {
			t.Fatalf("callback query routing key = %x, %v; want 00000007, nil", key, err)
		}
		if err := q.Exec(); err != nil || !called {
			t.Fatalf("callback query execution = %v, callback called = %t", err, called)
		}
	})
	t.Run("ReusedBatch", func(t *testing.T) {
		b := s.Batch(LoggedBatch).WithContext(ctx)
		b.Query(routingInsertPerCol)
		if _, err := b.GetRoutingKey(); err == nil {
			t.Fatal("missing values should fail routing")
		}
		b = b.WithContext(ctx)
		b.Entries[0].Args = []any{int32(1), int32(2)}
		if err := b.Exec(); err != nil {
			t.Fatalf("corrected batch: %v", err)
		}
		b.Entries[0].Args = nil
		if _, err := b.GetRoutingKey(); err == nil {
			t.Fatal("removed values should fail routing")
		}

		const literal = "INSERT INTO ks2.tbl2 (a, b) VALUES (1, 2)"
		b.Entries[0] = BatchEntry{Stmt: literal}
		conn := s.getConn()
		if conn == nil {
			t.Fatal("no connection for raw batch")
		}
		if err := conn.executeBatch(ctx, b).Close(); err != nil {
			t.Fatalf("raw batch after statement replacement: %v", err)
		}
		if got := srv.received(); len(got) != 2 || got[1].stmts[0].query != literal {
			t.Fatalf("received batches = %+v; want corrected and raw batches", got)
		}
		if got := srv.prepareCount(literal); got != 0 {
			t.Fatalf("raw batch PREPARE count = %d; want 0", got)
		}
	})
}

func TestQueryGetRoutingKey(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	t.Run("Computed", func(t *testing.T) {
		q := s.Query(routingInsertGlobal, int32(1), "x", int32(3))
		key, err := q.GetRoutingKey()
		if err != nil {
			t.Fatal(err)
		}
		want := append(encodeRoutingKeyComponent(int32Bytes(3)), encodeRoutingKeyComponent(int32Bytes(1))...)
		if !bytes.Equal(key, want) {
			t.Fatalf("routing key = %x, want %x", key, want)
		}
		if q.Keyspace() != "ks1" || q.Table() != "tbl1" {
			t.Fatalf("query target = %s.%s, want ks1.tbl1", q.Keyspace(), q.Table())
		}
	})

	t.Run("ExplicitKeyWins", func(t *testing.T) {
		explicit := []byte{0xde, 0xad}
		key, err := s.Query("INSERT INTO ks1.never (a) VALUES (?)", 1).RoutingKey(explicit).GetRoutingKey()
		if err != nil || !bytes.Equal(key, explicit) {
			t.Fatalf("GetRoutingKey = %x, %v; want %x", key, err, explicit)
		}
	})

	// Non-DML statements have no routing key and must not cost a PREPARE.
	t.Run("NonDMLSkipsPrepare", func(t *testing.T) {
		const stmt = "CREATE TABLE ks1.t (a int PRIMARY KEY)"
		key, err := s.Query(stmt).GetRoutingKey()
		if err != nil || key != nil {
			t.Fatalf("GetRoutingKey = %x, %v; want nil, nil", key, err)
		}
		if c := srv.prepareCount(stmt); c != 0 {
			t.Fatalf("non-DML statement prepared %d times, want 0", c)
		}
	})

	t.Run("BindWithoutValues", func(t *testing.T) {
		q := s.Bind(routingInsertGlobal, func(*QueryInfo) ([]any, error) { return nil, nil })
		key, err := q.GetRoutingKey()
		if err != nil || key != nil {
			t.Fatalf("GetRoutingKey = %x, %v; want nil, nil", key, err)
		}
	})

	t.Run("PrepareError", func(t *testing.T) {
		if _, err := s.Query(routingInsertUnprep, 1).GetRoutingKey(); err == nil {
			t.Fatal("expected the PREPARE error")
		}
	})
}

// Without the Scylla LWT_ADD_METADATA_MARK extension the server has no way to
// mark a statement as LWT, so a plain INSERT must not be treated as one: that
// would turn off replica shuffling and slow-replica avoidance in the token-aware
// policy and stop DowngradingConsistencyRetryPolicy from downgrading.
func TestRoutingKeyInfoNotLWTWithoutExtension(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	info, err := s.routingKeyInfo(context.Background(), routingInsertGlobal, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.lwt {
		t.Fatal("plain INSERT marked as LWT on a connection without the LWT extension")
	}

	q := s.Query(routingInsertGlobal, int32(1), "x", int32(3))
	if _, err := q.GetRoutingKey(); err != nil {
		t.Fatal(err)
	}
	if q.IsLWT() {
		t.Fatal("Query.IsLWT() = true for a plain INSERT")
	}
}

func TestBatchGetRoutingKey(t *testing.T) {
	t.Parallel()

	srv := newRoutingTestServer(t, protoVersion4, routingPrepared)
	s := srv.newSession(t)

	t.Run("Empty", func(t *testing.T) {
		key, err := s.Batch(LoggedBatch).GetRoutingKey()
		if err != nil || key != nil {
			t.Fatalf("GetRoutingKey = %x, %v; want nil, nil", key, err)
		}
	})

	t.Run("ExplicitKeyWins", func(t *testing.T) {
		explicit := []byte{0xbe, 0xef}
		b := s.Batch(LoggedBatch)
		b.routingKey = explicit
		b.Query(routingInsertUnprep, 1)
		key, err := b.GetRoutingKey()
		if err != nil || !bytes.Equal(key, explicit) {
			t.Fatalf("GetRoutingKey = %x, %v; want %x", key, err, explicit)
		}
	})

	// Only the first entry decides the batch's routing key.
	t.Run("FromFirstEntry", func(t *testing.T) {
		b := s.Batch(LoggedBatch)
		b.Query(routingInsertPerCol, int32(5), int32(6))
		b.Query(routingInsertUnprep, 1)
		key, err := b.GetRoutingKey()
		if err != nil {
			t.Fatal(err)
		}
		if want := int32Bytes(5); !bytes.Equal(key, want) {
			t.Fatalf("routing key = %x, want %x", key, want)
		}
	})

	t.Run("FirstEntryBound", func(t *testing.T) {
		b := s.Batch(LoggedBatch)
		b.Bind(routingInsertPerCol, func(*QueryInfo) ([]any, error) { return nil, nil })
		key, err := b.GetRoutingKey()
		if err != nil || key != nil {
			t.Fatalf("GetRoutingKey = %x, %v; want nil, nil", key, err)
		}
	})

	t.Run("PrepareError", func(t *testing.T) {
		b := s.Batch(LoggedBatch)
		b.Query(routingInsertUnprep, 1)
		if _, err := b.GetRoutingKey(); err == nil {
			t.Fatal("expected the PREPARE error")
		}
	})
}

// With the extension negotiated, the bit it names marks exactly the LWT
// statements.
func TestRoutingKeyInfoLWTWithExtension(t *testing.T) {
	t.Parallel()

	const (
		lwtBit = 1 << 31
		stmt   = "INSERT INTO ks1.tbl1 (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS"
	)
	prepared := map[string]testPrepared{
		routingInsertGlobal: routingPrepared[routingInsertGlobal],
	}
	lwt := routingPrepared[routingInsertGlobal]
	lwt.id, lwt.flags = 20, lwtBit
	prepared[stmt] = lwt

	srv := newRoutingTestServerWithSupported(t, protoVersion4, prepared, map[string][]string{
		lwtAddMetadataMarkKey: {"LWT_OPTIMIZATION_META_BIT_MASK=2147483648"},
	})
	s := srv.newSession(t)

	for _, tc := range []struct {
		stmt string
		want bool
	}{
		{routingInsertGlobal, false},
		{stmt, true},
	} {
		info, err := s.routingKeyInfo(context.Background(), tc.stmt, "", 0)
		if err != nil {
			t.Fatalf("%s: %v", tc.stmt, err)
		}
		if info.lwt != tc.want {
			t.Fatalf("%s: lwt = %v, want %v", tc.stmt, info.lwt, tc.want)
		}
	}
}
