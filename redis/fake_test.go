package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/gocql/gocql"
)

// fakeDB is an in-memory stand-in for the storage layer.
//
// It understands the exact statements the schema builds, including conditional
// batches, clustering slice deletes, cell TTLs and server side paging, so the
// tests exercise real command semantics: counts, guard outcomes, WRONGTYPE,
// expiry and atomicity. It also enforces the two server rules the design leans
// on, that a conditional batch and an unlogged batch stay inside one partition,
// so a command that quietly starts spanning partitions fails here first.
type fakeDB struct {
	sch *schema

	mu     sync.Mutex
	rows   map[rowKey]*fakeRow
	index  map[indexKey]struct{}
	expiry map[slotKey]struct{}
	wakeup map[slotKey]struct{}
	stmts  []string
	fail   map[string]error
	clock  func() time.Time

	// interfere runs just before a conditional statement is evaluated, while
	// the lock is held, so a test can act as the writer that wins the race.
	interfere func(db *fakeDB, stmt string, args []any)
}

type rowKey struct {
	bucket string
	key    string
	kind   int8
	sub    string
}

type indexKey struct {
	bucket string
	key    string
}

type slotKey struct {
	slot   time.Time
	bucket string
	key    string
}

type fakeRow struct {
	typ     string
	value   []byte
	version int64
	size    int64
	head    int64
	tail    int64
	expires time.Time
	// gone is the cell TTL: the row stops being visible at this instant, as a
	// row written with USING TTL does.
	gone time.Time
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFakeDB(sch *schema, clock func() time.Time) *fakeDB {
	if clock == nil {
		clock = time.Now
	}
	return &fakeDB{
		sch:    sch,
		rows:   map[rowKey]*fakeRow{},
		index:  map[indexKey]struct{}{},
		expiry: map[slotKey]struct{}{},
		wakeup: map[slotKey]struct{}{},
		fail:   map[string]error{},
		clock:  clock,
	}
}

// newTestClient runs on the real clock, so blocking commands and timeouts
// behave as they would in production. Tests that need to move time forward use
// newTestClientWithClock instead.
func newTestClient(t *testing.T, mutate func(*Options)) (*Client, *fakeDB) {
	t.Helper()

	opt := &Options{
		Keyspace:        "ks",
		Table:           "kv",
		CASRetryBackoff: time.Microsecond,
	}
	if mutate != nil {
		mutate(opt)
	}

	client := newConfiguredClient(opt)
	if client.configErr != nil {
		t.Fatalf("unexpected config error: %v", client.configErr)
	}
	db := newFakeDB(client.core.schema, client.core.clock)
	client.core.runner = db
	t.Cleanup(func() { _ = client.Close() })
	return client, db
}

func newTestClientWithClock(t *testing.T, mutate func(*Options)) (*Client, *fakeDB, *fakeClock) {
	t.Helper()

	clock := newFakeClock()
	opt := &Options{
		Keyspace:        "ks",
		Table:           "kv",
		CASRetryBackoff: time.Microsecond,
		clock:           clock.Now,
	}
	if mutate != nil {
		mutate(opt)
	}
	client := newConfiguredClient(opt)
	if client.configErr != nil {
		t.Fatalf("unexpected config error: %v", client.configErr)
	}
	db := newFakeDB(client.core.schema, client.core.clock)
	client.core.runner = db
	t.Cleanup(func() { _ = client.Close() })
	return client, db, clock
}

// take splits the leading key columns off an argument list.
func (db *fakeDB) take(args []any) (bucket, key string, rest []any) {
	if db.sch.bucketed {
		return args[0].(string), args[1].(string), args[2:]
	}
	return "", args[0].(string), args[1:]
}

func (db *fakeDB) meta(bucket, key string) rowKey {
	return rowKey{bucket: bucket, key: key, kind: kindMeta, sub: "\x00"}
}

func (db *fakeDB) record(stmt string) { db.stmts = append(db.stmts, stmt) }

func (db *fakeDB) count(fragment string) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	n := 0
	for _, stmt := range db.stmts {
		if strings.Contains(stmt, fragment) {
			n++
		}
	}
	return n
}

func (db *fakeDB) injected(stmt string) error {
	for fragment, err := range db.fail {
		if strings.Contains(stmt, fragment) {
			return err
		}
	}
	return nil
}

func (db *fakeDB) live(r *fakeRow) bool {
	return r != nil && (r.gone.IsZero() || db.clock().Before(r.gone))
}

func (db *fakeDB) row(rk rowKey) (*fakeRow, bool) {
	r, ok := db.rows[rk]
	if !ok || !db.live(r) {
		return nil, false
	}
	return r, true
}

// sortedRows returns the clustering order of one key: meta row first, then
// elements by kind and then by sub, bytewise.
func (db *fakeDB) sortedRows(bucket, key string) []rowKey {
	var out []rowKey
	for rk, r := range db.rows {
		if rk.bucket != bucket || rk.key != key || !db.live(r) {
			continue
		}
		out = append(out, rk)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].kind != out[b].kind {
			return out[a].kind < out[b].kind
		}
		return out[a].sub < out[b].sub
	})
	return out
}

func (db *fakeDB) Exec(ctx context.Context, stmt string, args ...any) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.execLocked(stmt, args)
}

func (db *fakeDB) execLocked(stmt string, args []any) error {
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return err
	}
	if strings.HasPrefix(stmt, "CREATE TABLE") {
		return nil
	}

	s := db.sch
	switch stmt {
	case s.strWrite, s.strWriteTTL:
		bucket, key, rest := db.take(args)
		row := &fakeRow{typ: string(typeString), value: toBytes(rest[0]), version: rest[1].(int64)}
		row.expires = toTime(rest[2])
		if stmt == s.strWriteTTL {
			row.gone = db.clock().Add(time.Duration(rest[3].(int)) * time.Second)
		}
		db.rows[db.meta(bucket, key)] = row
		return nil
	case s.elemWrite:
		bucket, key, rest := db.take(args)
		db.rows[rowKey{bucket, key, rest[0].(int8), string(toBytes(rest[1]))}] = &fakeRow{value: toBytes(rest[2])}
		return nil
	case s.elemDelete:
		bucket, key, rest := db.take(args)
		delete(db.rows, rowKey{bucket, key, rest[0].(int8), string(toBytes(rest[1]))})
		return nil
	case s.elemsDelete:
		bucket, key, _ := db.take(args)
		for rk := range db.rows {
			if rk.bucket == bucket && rk.key == key && rk.kind >= kindField {
				delete(db.rows, rk)
			}
		}
		return nil
	case s.keyDelete:
		bucket, key, _ := db.take(args)
		for rk := range db.rows {
			if rk.bucket == bucket && rk.key == key {
				delete(db.rows, rk)
			}
		}
		return nil
	case s.indexWrite:
		bucket, key, _ := db.take(args)
		db.index[indexKey{bucket, key}] = struct{}{}
		return nil
	case s.indexDelete:
		bucket, key, _ := db.take(args)
		delete(db.index, indexKey{bucket, key})
		return nil
	case s.expiryWrite:
		db.expiry[slotKey{args[0].(time.Time), args[1].(string), args[2].(string)}] = struct{}{}
		return nil
	case s.expiryDrop:
		delete(db.expiry, slotKey{args[0].(time.Time), args[1].(string), args[2].(string)})
		return nil
	case s.wakeupWrite:
		db.wakeup[slotKey{args[0].(time.Time), args[1].(string), args[2].(string)}] = struct{}{}
		return nil
	}
	return fmt.Errorf("fakeDB: unexpected Exec statement: %s", stmt)
}

func (db *fakeDB) ScanOne(ctx context.Context, stmt string, args []any, dest ...any) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return err
	}

	s := db.sch
	switch stmt {
	case s.metaRead:
		bucket, key, _ := db.take(args)
		row, ok := db.row(db.meta(bucket, key))
		if !ok {
			return gocql.ErrNotFound
		}
		return assignAll(dest, row.typ, row.value, row.version, row.size, row.head, row.tail, row.expires)
	case s.edgeLast:
		bucket, key, _ := db.take(args)
		rows := db.sortedRows(bucket, key)
		for i := len(rows) - 1; i >= 0; i-- {
			if rows[i].kind != kindPos {
				continue
			}
			return assignAll(dest, []byte(rows[i].sub), db.rows[rows[i]].value)
		}
		return gocql.ErrNotFound
	}
	return fmt.Errorf("fakeDB: unexpected ScanOne statement: %s", stmt)
}

// condition reports whether a conditional statement would apply, without
// applying it. Evaluating every condition of a batch before applying any of it
// is what makes the fake reject a batch the way the server does.
func (db *fakeDB) condition(stmt string, args []any) (applies bool, conditional bool) {
	s := db.sch
	switch stmt {
	case s.strWriteNX, s.strWriteNXTTL, s.collCreate:
		bucket, key, _ := db.take(args)
		_, ok := db.row(db.meta(bucket, key))
		return !ok, true
	case s.strCAS, s.strCASTTL:
		offset := 0
		if stmt == s.strCASTTL {
			offset = 1
		}
		bucket, key, rest := db.take(args[offset+3:])
		row, ok := db.row(db.meta(bucket, key))
		return ok && row.version == rest[0].(int64), true
	case s.collCAS:
		bucket, key, rest := db.take(args[4:])
		row, ok := db.row(db.meta(bucket, key))
		return ok && row.typ == rest[0].(string) && row.version == rest[1].(int64), true
	case s.expireCAS:
		bucket, key, rest := db.take(args[2:])
		row, ok := db.row(db.meta(bucket, key))
		return ok && row.version == rest[0].(int64), true
	case s.metaDeleteCAS:
		bucket, key, rest := db.take(args)
		row, ok := db.row(db.meta(bucket, key))
		return ok && row.version == rest[0].(int64), true
	case s.metaDeleteIf:
		bucket, key, _ := db.take(args)
		_, ok := db.row(db.meta(bucket, key))
		return ok, true
	case s.absentCAS:
		bucket, key, _ := db.take(args)
		_, ok := db.row(db.meta(bucket, key))
		return !ok, true
	}
	return true, false
}

// applyConditional performs the write half of a conditional statement, whose
// condition has already been checked.
func (db *fakeDB) applyConditional(stmt string, args []any) error {
	s := db.sch
	switch stmt {
	case s.strWriteNX:
		return db.execLocked(s.strWrite, args)
	case s.strWriteNXTTL:
		return db.execLocked(s.strWriteTTL, args)
	case s.strCAS, s.strCASTTL:
		offset := 0
		var ttl int
		if stmt == s.strCASTTL {
			ttl = args[0].(int)
			offset = 1
		}
		bucket, key, _ := db.take(args[offset+3:])
		row, _ := db.row(db.meta(bucket, key))
		row.typ = string(typeString)
		row.value = toBytes(args[offset])
		row.version = args[offset+1].(int64)
		row.expires = toTime(args[offset+2])
		row.gone = time.Time{}
		if ttl > 0 {
			row.gone = db.clock().Add(time.Duration(ttl) * time.Second)
		}
		return nil
	case s.collCreate:
		bucket, key, rest := db.take(args)
		db.rows[db.meta(bucket, key)] = &fakeRow{
			typ:     rest[0].(string),
			version: rest[1].(int64),
			size:    rest[2].(int64),
			head:    rest[3].(int64),
			tail:    rest[4].(int64),
			expires: toTime(rest[5]),
		}
		return nil
	case s.collCAS:
		bucket, key, _ := db.take(args[4:])
		row, _ := db.row(db.meta(bucket, key))
		row.version = args[0].(int64)
		row.size = args[1].(int64)
		row.head = args[2].(int64)
		row.tail = args[3].(int64)
		return nil
	case s.expireCAS:
		bucket, key, _ := db.take(args[2:])
		row, _ := db.row(db.meta(bucket, key))
		row.version = args[0].(int64)
		row.expires = toTime(args[1])
		return nil
	case s.metaDeleteCAS, s.metaDeleteIf:
		bucket, key, _ := db.take(args)
		delete(db.rows, db.meta(bucket, key))
		return nil
	case s.absentCAS:
		// A guard on absence writes a cell tombstone into a row that does not
		// exist, which leaves nothing visible behind.
		return nil
	}
	return fmt.Errorf("fakeDB: unexpected conditional statement: %s", stmt)
}

func (db *fakeDB) ExecCAS(ctx context.Context, stmt string, args []any) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return false, err
	}
	if db.interfere != nil {
		db.interfere(db, stmt, args)
	}

	applies, conditional := db.condition(stmt, args)
	if !conditional {
		return false, fmt.Errorf("fakeDB: unexpected ExecCAS statement: %s", stmt)
	}
	if !applies {
		return false, nil
	}
	if err := db.applyConditional(stmt, args); err != nil {
		return false, err
	}
	return true, nil
}

// partitionOf mirrors how the server groups statements, which is what decides
// whether a batch is legal.
func (db *fakeDB) partitionOf(stmt string, args []any) (string, bool) {
	switch stmt {
	case db.sch.expiryWrite, db.sch.expiryDrop, db.sch.wakeupWrite:
		return "side:" + args[0].(time.Time).String(), true
	case db.sch.indexWrite, db.sch.indexDelete:
		bucket, key, _ := db.take(args)
		if db.sch.bucketed {
			return "index:" + bucket, true
		}
		return "index:" + key, true
	}

	// Conditional statements carry their bound key at different offsets.
	offset := 0
	switch stmt {
	case db.sch.strCASTTL:
		offset = 4
	case db.sch.strCAS:
		offset = 3
	case db.sch.collCAS:
		offset = 4
	case db.sch.expireCAS:
		offset = 2
	}
	if len(args) <= offset {
		return "", false
	}
	bucket, key, _ := db.take(args[offset:])
	switch {
	case db.sch.grouped:
		return "key:" + bucket, true
	case db.sch.bucketed:
		return "key:" + bucket + "\x00" + key, true
	default:
		return "key:" + key, true
	}
}

func (db *fakeDB) samePartition(stmts []batchStatement) error {
	seen := map[string]struct{}{}
	for _, st := range stmts {
		part, ok := db.partitionOf(st.stmt, st.args)
		if !ok {
			return fmt.Errorf("fakeDB: cannot determine partition of %s", st.stmt)
		}
		seen[part] = struct{}{}
	}
	if len(seen) > 1 {
		return fmt.Errorf("fakeDB: batch spans %d partitions, the server would reject it", len(seen))
	}
	return nil
}

func (db *fakeDB) Batch(ctx context.Context, batchType gocql.BatchType, stmts []batchStatement) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record("BATCH")

	if batchType == gocql.UnloggedBatch {
		// An unlogged batch is only atomic inside one partition, so a command
		// that relies on it must stay there.
		if err := db.samePartition(stmts); err != nil {
			return err
		}
	}
	for _, st := range stmts {
		if err := db.execLocked(st.stmt, st.args); err != nil {
			return err
		}
	}
	return nil
}

func (db *fakeDB) BatchCAS(ctx context.Context, batchType gocql.BatchType, stmts []batchStatement) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record("BATCH_CAS")
	for _, st := range stmts {
		if err := db.injected(st.stmt); err != nil {
			return false, err
		}
	}
	if db.interfere != nil {
		for _, st := range stmts {
			db.interfere(db, st.stmt, st.args)
		}
	}
	// A conditional batch may not span partitions, whatever its type.
	if err := db.samePartition(stmts); err != nil {
		return false, err
	}

	conditions := 0
	for _, st := range stmts {
		applies, conditional := db.condition(st.stmt, st.args)
		if !conditional {
			continue
		}
		conditions++
		if !applies {
			return false, nil
		}
	}
	if conditions == 0 {
		return false, errors.New("fakeDB: conditional batch without a condition")
	}

	for _, st := range stmts {
		if _, conditional := db.condition(st.stmt, st.args); conditional {
			if err := db.applyConditional(st.stmt, st.args); err != nil {
				return false, err
			}
			continue
		}
		if err := db.execLocked(st.stmt, st.args); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (db *fakeDB) Iterate(ctx context.Context, stmt string, args []any, opt iterOptions) rowIterator {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return &fakeIter{err: err}
	}

	s := db.sch
	switch {
	case stmt == s.kindRead:
		bucket, key, rest := db.take(args)
		kind := rest[0].(int8)
		iter := &fakeIter{}
		for _, rk := range db.sortedRows(bucket, key) {
			if rk.kind != kind {
				continue
			}
			iter.rows = append(iter.rows, []any{[]byte(rk.sub), db.rows[rk].value})
		}
		return iter
	case stmt == s.edgeRead:
		bucket, key, _ := db.take(args)
		iter := &fakeIter{}
		for _, rk := range db.sortedRows(bucket, key) {
			if rk.kind != kindMeta && rk.kind != kindPos {
				continue
			}
			iter.rows = append(iter.rows, db.wideRow(rk))
			if len(iter.rows) == 2 {
				break
			}
		}
		return iter
	case stmt == s.indexScan:
		bucket := ""
		if db.sch.bucketed {
			bucket = args[0].(string)
		}
		keys := make([]string, 0, len(db.index))
		for ik := range db.index {
			if db.sch.bucketed && ik.bucket != bucket {
				continue
			}
			keys = append(keys, ik.key)
		}
		sort.Strings(keys)

		start := 0
		if len(opt.pageState) > 0 {
			start, _ = strconv.Atoi(string(opt.pageState))
		}
		end := len(keys)
		if opt.singlePage && opt.pageSize > 0 && start+opt.pageSize < end {
			end = start + opt.pageSize
		}
		if start > end {
			start = end
		}
		iter := &fakeIter{}
		for _, key := range keys[start:end] {
			iter.rows = append(iter.rows, []any{key})
		}
		if opt.singlePage && end < len(keys) {
			iter.state = []byte(strconv.Itoa(end))
		}
		return iter
	case stmt == s.expiryScan, stmt == s.wakeupScan:
		source := db.expiry
		if stmt == s.wakeupScan {
			source = db.wakeup
		}
		slot := args[0].(time.Time)
		var entries []slotKey
		for sk := range source {
			if sk.slot.Equal(slot) {
				entries = append(entries, sk)
			}
		}
		sort.Slice(entries, func(a, b int) bool {
			if entries[a].bucket != entries[b].bucket {
				return entries[a].bucket < entries[b].bucket
			}
			return entries[a].key < entries[b].key
		})
		iter := &fakeIter{}
		for _, sk := range entries {
			iter.rows = append(iter.rows, []any{sk.bucket, sk.key})
		}
		return iter
	case strings.Contains(stmt, "(key, kind, sub) IN"):
		// The grouped layout clusters by key, so every tuple names it.
		bucket, key, rest := args[0].(string), args[1].(string), args[2:]
		iter := &fakeIter{}
		if rk := db.meta(bucket, key); db.rows[rk] != nil {
			if _, ok := db.row(rk); ok {
				iter.rows = append(iter.rows, db.wideRow(rk))
			}
		}
		for i := 0; i+2 < len(rest); i += 3 {
			rk := rowKey{bucket, rest[i].(string), rest[i+1].(int8), string(toBytes(rest[i+2]))}
			if _, ok := db.row(rk); ok {
				iter.rows = append(iter.rows, db.wideRow(rk))
			}
		}
		return iter
	case strings.Contains(stmt, "(kind, sub) IN"):
		bucket, key, rest := db.take(args)
		iter := &fakeIter{}
		if rk := db.meta(bucket, key); db.rows[rk] != nil {
			if _, ok := db.row(rk); ok {
				iter.rows = append(iter.rows, db.wideRow(rk))
			}
		}
		for i := 0; i+1 < len(rest); i += 2 {
			rk := rowKey{bucket, key, rest[i].(int8), string(toBytes(rest[i+1]))}
			if _, ok := db.row(rk); ok {
				iter.rows = append(iter.rows, db.wideRow(rk))
			}
		}
		return iter
	}
	return &fakeIter{err: fmt.Errorf("fakeDB: unexpected Iterate statement: %s", stmt)}
}

// wideRow renders a row in the projection the meta-and-elements reads use.
func (db *fakeDB) wideRow(rk rowKey) []any {
	r := db.rows[rk]
	return []any{rk.kind, []byte(rk.sub), r.typ, r.value, r.version, r.size, r.head, r.tail, r.expires}
}

type fakeIter struct {
	rows  [][]any
	idx   int
	state []byte
	err   error
}

func (i *fakeIter) Scan(dest ...any) bool {
	if i.err != nil || i.idx >= len(i.rows) {
		return false
	}
	row := i.rows[i.idx]
	i.idx++
	if err := assignAll(dest, row...); err != nil {
		i.err = err
		return false
	}
	return true
}

func (i *fakeIter) PageState() []byte { return i.state }
func (i *fakeIter) Close() error      { return i.err }

func assignAll(dest []any, values ...any) error {
	for i := range dest {
		var value any
		if i < len(values) {
			value = values[i]
		}
		if err := assign(dest[i], value); err != nil {
			return err
		}
	}
	return nil
}

func assign(dest any, value any) error {
	switch d := dest.(type) {
	case *string:
		switch v := value.(type) {
		case nil:
			*d = ""
		case string:
			*d = v
		case []byte:
			*d = string(v)
		default:
			return fmt.Errorf("fakeDB: cannot assign %T to *string", value)
		}
	case *[]byte:
		switch v := value.(type) {
		case nil:
			*d = nil
		case []byte:
			*d = append([]byte(nil), v...)
		case string:
			*d = []byte(v)
		default:
			return fmt.Errorf("fakeDB: cannot assign %T to *[]byte", value)
		}
	case *int8:
		switch v := value.(type) {
		case nil:
			*d = 0
		case int8:
			*d = v
		default:
			return fmt.Errorf("fakeDB: cannot assign %T to *int8", value)
		}
	case *int64:
		switch v := value.(type) {
		case nil:
			*d = 0
		case int64:
			*d = v
		case int:
			*d = int64(v)
		default:
			return fmt.Errorf("fakeDB: cannot assign %T to *int64", value)
		}
	case *time.Time:
		switch v := value.(type) {
		case nil:
			*d = time.Time{}
		case time.Time:
			*d = v
		default:
			return fmt.Errorf("fakeDB: cannot assign %T to *time.Time", value)
		}
	default:
		return fmt.Errorf("fakeDB: unsupported destination %T", dest)
	}
	return nil
}

func toBytes(v any) []byte {
	switch t := v.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), t...)
	case string:
		return []byte(t)
	default:
		panic(fmt.Sprintf("fakeDB: unexpected value %T", v))
	}
}

func toTime(v any) time.Time {
	switch t := v.(type) {
	case nil:
		return time.Time{}
	case time.Time:
		return t
	default:
		panic(fmt.Sprintf("fakeDB: unexpected timestamp %T", v))
	}
}

// metaOf is a test helper for asserting on stored state.
func (db *fakeDB) metaOf(bucket, key string) (fakeRow, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	row, ok := db.row(db.meta(bucket, key))
	if !ok {
		return fakeRow{}, false
	}
	return *row, true
}

func (db *fakeDB) elementCount(bucket, key string, kind int8) int {
	db.mu.Lock()
	defer db.mu.Unlock()
	n := 0
	for _, rk := range db.sortedRows(bucket, key) {
		if rk.kind == kind {
			n++
		}
	}
	return n
}

func (db *fakeDB) indexed(bucket, key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, ok := db.index[indexKey{bucket, key}]
	return ok
}

func mustEqualBytes(t *testing.T, got []byte, want string) {
	t.Helper()
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("value = %q, want %q", got, want)
	}
}
