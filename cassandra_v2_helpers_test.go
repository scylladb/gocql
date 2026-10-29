//go:build integration

package gocql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// These helpers accept typed descriptors and explicit values. Retained results
// are cloned before their page is closed.
func cassandraValue(value Value, err error) Value {
	if err != nil {
		panic(err)
	}
	return value
}
func cassandraField(t testing.TB, row DynamicRow, name string) Value {
	t.Helper()
	v, err := row.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraInt(t testing.TB, row DynamicRow, name string) int64 {
	t.Helper()
	v, err := cassandraField(t, row, name).Signed()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraText(t testing.TB, row DynamicRow, name string) string {
	t.Helper()
	v, err := cassandraField(t, row, name).Text()
	if err != nil {
		t.Fatal(err)
	}
	return v.String()
}
func cassandraBool(t testing.TB, row DynamicRow, name string) bool {
	t.Helper()
	v, err := cassandraField(t, row, name).Bool()
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func cassandraRead(t testing.TB, s *Session, cql string, values ...Value) DynamicRow {
	t.Helper()
	row, err := v2ReadOne(context.Background(), s, cql, values...)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
func cassandraWrite(t testing.TB, s *Session, cql string, values ...Value) Result {
	t.Helper()
	result, err := v2Exec(context.Background(), s, cql, values...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func cassandraDDL(t testing.TB, s *Session, cql string) {
	t.Helper()
	if err := v2CreateTable(context.Background(), s, cql); err != nil {
		t.Fatal(err)
	}
}
func cassandraPrepared[P, R any](t testing.TB, s *Session, statement Statement[P, R], options PrepareOptions) *Prepared[P, R] {
	t.Helper()
	p, err := Prepare(context.Background(), s, statement, options)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func cassandraBound[P, R any](t testing.TB, p *Prepared[P, R], value P) *Bound[R] {
	t.Helper()
	b, err := BindOwned(p, value)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Wait(ctx); err != nil {
			t.Error(err)
		}
	})
	return b
}
func cassandraUnprepared[R any](t testing.TB, cql string, decoder RowDecoder[R]) *Bound[R] {
	t.Helper()
	b, err := BindUnpreparedOwned(Statement[Unit, R]{CQL: cql, Params: NoParams, Rows: decoder}, NewColumns(), Unit{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Wait(ctx); err != nil {
			t.Error(err)
		}
	})
	return b
}
func cassandraReadBound[R any](t testing.TB, s *Session, b *Bound[R], options Options) R {
	t.Helper()
	rows, err := Run(context.Background(), s, b, options)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row: %v", rows.Err())
	}
	row, err := rows.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("expected exactly one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return row
}
func cassandraItem[R any](t testing.TB, b *Bound[R]) BatchItem {
	t.Helper()
	item, err := BatchEntry(b)
	if err != nil {
		t.Fatal(err)
	}
	return item
}
func cassandraDynamicBound(t testing.TB, s *Session, cql string, values []Value, options PrepareOptions) *Bound[DynamicRow] {
	t.Helper()
	return cassandraBound(t, cassandraPrepared(t, s, Statement[[]Value, DynamicRow]{CQL: cql, Params: DynamicParams, Rows: DynamicRows}, options), values)
}
func cassandraWriteBound(t testing.TB, s *Session, cql string, values []Value, options PrepareOptions) *Bound[Unit] {
	t.Helper()
	return cassandraBound(t, cassandraPrepared(t, s, Statement[[]Value, Unit]{CQL: cql, Params: DynamicParams, Rows: NoRows}, options), values)
}
func cassandraRows(t testing.TB, s *Session, b *Bound[DynamicRow], options Options) []DynamicRow {
	t.Helper()
	rows, err := Run(context.Background(), s, b, options)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []DynamicRow
	for rows.Next() {
		row, err := rows.Clone()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
func cassandraBatch(t testing.TB, s *Session, kind BatchKind, items []BatchItem, options Options) Result {
	t.Helper()
	r, e := ExecBatch(context.Background(), s, kind, items, options)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func cassandraKeyspace(s *Session, name string) (*KeyspaceMetadata, error) {
	if name == "" {
		return nil, ErrNoKeyspace
	}
	ks, ok := s.Metadata().Keyspace(name)
	if !ok {
		return nil, ErrKeyspaceDoesNotExist
	}
	return ks, nil
}
func cassandraTable(s *Session, ks, name string) (*TableMetadata, error) {
	if name == "" {
		return nil, ErrNoTable
	}
	k, err := cassandraKeyspace(s, ks)
	if err != nil {
		return nil, err
	}
	table, ok := k.Tables[name]
	if !ok {
		return nil, ErrNotFound
	}
	return table, nil
}
func cassandraOtherSession(t testing.TB, s *Session, keyspace string) *Session {
	t.Helper()
	cassandraDDL(t, s, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':%d}", keyspace, *flagRF))
	config := createV2Cluster()
	config.Keyspace = keyspace
	other, err := Connect(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	return other
}

type cassandraObserver struct {
	mu       sync.Mutex
	requests []RequestEvent
	attempts []AttemptEvent
}

func (o *cassandraObserver) ObserveRequest(e RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, e)
}
func (o *cassandraObserver) ObserveAttempt(e AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, e)
}
func (o *cassandraObserver) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = nil
	o.attempts = nil
}
func (o *cassandraObserver) snapshot() ([]RequestEvent, []AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]RequestEvent(nil), o.requests...), append([]AttemptEvent(nil), o.attempts...)
}

type cassandraRetry struct{}

func (cassandraRetry) Decide(a AttemptError) RetryDecision {
	if a.Idempotent && a.Attempt < 6 {
		return RetryDecision{Action: RetrySame}
	}
	return RetryDecision{Action: StopRetry}
}

// Custom values use explicit codecs; field discovery and reflection are absent.
type fullNameCodec struct{}

func (fullNameCodec) CheckType(t DataType) error { return TextCodec.CheckType(t) }
func (fullNameCodec) Encode(w *ValueWriter, n FullName) error {
	return StringEncoder.Encode(w, n.FirstName+" "+n.LastName)
}
func (fullNameCodec) Decode(r ValueReader) (FullName, error) {
	text, err := TextCodec.Decode(r)
	if err != nil {
		return FullName{}, err
	}
	names := strings.SplitN(text.String(), " ", 2)
	if len(names) != 2 {
		return FullName{}, errors.New("full name requires two components")
	}
	return FullName{FirstName: names[0], LastName: names[1]}, nil
}
func (fullNameCodec) Clone(n FullName) FullName { return n }
func cassandraCASBound[R any](t testing.TB, s *Session, cql string, values []Value, decoder RowDecoder[R]) *Bound[CASResult[R]] {
	t.Helper()
	return cassandraBound(t, cassandraPrepared(t, s, Statement[[]Value, CASResult[R]]{CQL: cql, Params: DynamicParams, Rows: CASRows(decoder)}, PrepareOptions{}), values)
}

func cassandraUUID(text string) UUID {
	u, err := ParseUUID(text)
	if err != nil {
		panic(err)
	}
	return u
}

type cassandraVarintInt64 struct{}

func (cassandraVarintInt64) CheckType(t DataType) error { return VarintCodec.CheckType(t) }
func (cassandraVarintInt64) Decode(r ValueReader) (int64, error) {
	v, err := VarintCodec.Decode(r)
	if err != nil {
		return 0, err
	}
	n := v.BigInt()
	if !n.IsInt64() {
		return 0, fmt.Errorf("varint is outside int64 range")
	}
	return n.Int64(), nil
}
func (cassandraVarintInt64) Clone(v int64) int64 { return v }

type cassandraReconnect struct{}

func (cassandraReconnect) Delay(attempt int) (time.Duration, bool) { return time.Second, true }
