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

// fakeDB is a small in-memory stand-in for the storage layer. It understands
// the exact statements the schema builds, including conditional writes and
// paged reads, so the tests exercise real command semantics (counts, CAS
// outcomes, WRONGTYPE, paging) instead of asserting on CQL strings.
type fakeDB struct {
	sch *schema

	mu    sync.Mutex
	kv    map[string]*fakeRow
	hash  map[string]map[string][]byte
	set   map[string]map[string]struct{}
	list  map[string]map[int64][]byte
	stmts []string
	fail  map[string]error
}

type fakeRow struct {
	bucket string
	key    string
	typ    string
	value  []byte
	ttl    int
}

func newFakeDB(sch *schema) *fakeDB {
	return &fakeDB{
		sch:  sch,
		kv:   map[string]*fakeRow{},
		hash: map[string]map[string][]byte{},
		set:  map[string]map[string]struct{}{},
		list: map[string]map[int64][]byte{},
		fail: map[string]error{},
	}
}

func newTestClient(t *testing.T, mutate func(*Options)) (*Client, *fakeDB) {
	t.Helper()

	opt := &Options{Keyspace: "ks", Table: "kv", CASRetryBackoff: time.Microsecond}
	if mutate != nil {
		mutate(opt)
	}

	client := newConfiguredClient(opt)
	if client.configErr != nil {
		t.Fatalf("unexpected config error: %v", client.configErr)
	}
	db := newFakeDB(client.core.schema)
	client.core.runner = db
	return client, db
}

func (db *fakeDB) id(args []any) (string, []any) {
	if db.sch.bucketed {
		return fmt.Sprintf("%v\x00%v", args[0], args[1]), args[2:]
	}
	return fmt.Sprint(args[0]), args[1:]
}

func (db *fakeDB) idParts(args []any) (bucket, key string) {
	if db.sch.bucketed {
		return args[0].(string), args[1].(string)
	}
	return "", args[0].(string)
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
	if strings.HasPrefix(stmt, "CREATE TABLE") || strings.HasPrefix(stmt, "ALTER TABLE") {
		return nil
	}

	s := db.sch
	switch stmt {
	case s.kvUpsert, s.kvUpsertTTL:
		id, rest := db.id(args)
		bucket, key := db.idParts(args)
		row := &fakeRow{bucket: bucket, key: key, typ: rest[0].(string), value: toBytes(rest[1])}
		if stmt == s.kvUpsertTTL {
			row.ttl = rest[2].(int)
		}
		db.kv[id] = row
		return nil
	case s.kvDelete:
		id, _ := db.id(args)
		delete(db.kv, id)
		return nil
	case s.hashUpsert:
		id, rest := db.id(args)
		if db.hash[id] == nil {
			db.hash[id] = map[string][]byte{}
		}
		db.hash[id][rest[0].(string)] = toBytes(rest[1])
		return nil
	case s.hashDeleteKey:
		id, _ := db.id(args)
		delete(db.hash, id)
		return nil
	case s.setDeleteKey:
		id, _ := db.id(args)
		delete(db.set, id)
		return nil
	case s.listDeleteKey:
		id, _ := db.id(args)
		delete(db.list, id)
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
	case s.kvSelect, s.kvSelectTTL, s.kvSelectType, s.kvSelectKey:
		id, _ := db.id(args)
		row, ok := db.kv[id]
		if !ok {
			return gocql.ErrNotFound
		}
		switch stmt {
		case s.kvSelect:
			return assignAll(dest, row.typ, row.value)
		case s.kvSelectTTL:
			var ttl any
			if row.ttl > 0 {
				ttl = row.ttl
			}
			return assignAll(dest, row.typ, row.value, ttl)
		case s.kvSelectType:
			return assignAll(dest, row.typ)
		default:
			return assignAll(dest, row.key)
		}
	case s.hashSelect:
		id, rest := db.id(args)
		value, ok := db.hash[id][rest[0].(string)]
		if !ok {
			return gocql.ErrNotFound
		}
		return assignAll(dest, value)
	case s.setSelect:
		id, rest := db.id(args)
		member := rest[0].(string)
		if _, ok := db.set[id][member]; !ok {
			return gocql.ErrNotFound
		}
		return assignAll(dest, member)
	case s.setCount:
		id, _ := db.id(args)
		return assignAll(dest, int64(len(db.set[id])))
	case s.listCount:
		id, _ := db.id(args)
		return assignAll(dest, int64(len(db.list[id])))
	case s.listEdgeAsc, s.listEdgeDesc:
		id, _ := db.id(args)
		positions := sortedPositions(db.list[id])
		if len(positions) == 0 {
			return gocql.ErrNotFound
		}
		pos := positions[0]
		if stmt == s.listEdgeDesc {
			pos = positions[len(positions)-1]
		}
		return assignAll(dest, pos, db.list[id][pos])
	}
	return fmt.Errorf("fakeDB: unexpected ScanOne statement: %s", stmt)
}

func (db *fakeDB) ExecCAS(ctx context.Context, stmt string, args []any) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return false, err
	}

	s := db.sch
	switch stmt {
	case s.kvUpdateCAS, s.kvUpdateCASTTL:
		offset := 0
		ttl := 0
		if stmt == s.kvUpdateCASTTL {
			ttl = args[0].(int)
			offset = 1
		}
		typ := args[offset].(string)
		value := toBytes(args[offset+1])
		id, rest := db.id(args[offset+2:])
		expect := toBytes(rest[0])
		row, ok := db.kv[id]
		if !ok || !bytes.Equal(row.value, expect) {
			return false, nil
		}
		row.typ = typ
		row.value = value
		row.ttl = ttl
		return true, nil
	case s.kvGuardCAS:
		value := toBytes(args[0])
		id, rest := db.id(args[1:])
		expect := toBytes(rest[0])
		row, ok := db.kv[id]
		if !ok || !bytes.Equal(row.value, expect) {
			return false, nil
		}
		row.value = value
		return true, nil
	case s.kvDeleteIfExists:
		id, _ := db.id(args)
		if _, ok := db.kv[id]; !ok {
			return false, nil
		}
		delete(db.kv, id)
		return true, nil
	case s.kvDeleteCAS:
		id, rest := db.id(args)
		expect := toBytes(rest[0])
		row, ok := db.kv[id]
		if !ok || !bytes.Equal(row.value, expect) {
			return false, nil
		}
		delete(db.kv, id)
		return true, nil
	case s.hashDeleteIf:
		id, rest := db.id(args)
		field := rest[0].(string)
		if _, ok := db.hash[id][field]; !ok {
			return false, nil
		}
		delete(db.hash[id], field)
		return true, nil
	case s.setDeleteIf:
		id, rest := db.id(args)
		member := rest[0].(string)
		if _, ok := db.set[id][member]; !ok {
			return false, nil
		}
		delete(db.set[id], member)
		return true, nil
	case s.listDeleteIf:
		id, rest := db.id(args)
		pos := rest[0].(int64)
		if _, ok := db.list[id][pos]; !ok {
			return false, nil
		}
		delete(db.list[id], pos)
		return true, nil
	}
	return false, fmt.Errorf("fakeDB: unexpected ExecCAS statement: %s", stmt)
}

func (db *fakeDB) MapScanCAS(ctx context.Context, stmt string, args []any, dest map[string]any) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return false, err
	}

	s := db.sch
	switch stmt {
	case s.kvInsertNX, s.kvInsertNXTTL, s.kvMarkerNX:
		id, rest := db.id(args)
		bucket, key := db.idParts(args)
		if row, ok := db.kv[id]; ok {
			dest["key"] = row.key
			dest["type"] = row.typ
			dest["value"] = row.value
			return false, nil
		}
		row := &fakeRow{bucket: bucket, key: key, typ: rest[0].(string)}
		if stmt != s.kvMarkerNX {
			row.value = toBytes(rest[1])
		}
		if stmt == s.kvInsertNXTTL {
			row.ttl = rest[2].(int)
		}
		db.kv[id] = row
		return true, nil
	case s.hashInsertNX:
		id, rest := db.id(args)
		field := rest[0].(string)
		if current, ok := db.hash[id][field]; ok {
			dest["field"] = field
			dest["value"] = current
			return false, nil
		}
		if db.hash[id] == nil {
			db.hash[id] = map[string][]byte{}
		}
		db.hash[id][field] = toBytes(rest[1])
		return true, nil
	case s.setInsertNX:
		id, rest := db.id(args)
		member := rest[0].(string)
		if _, ok := db.set[id][member]; ok {
			dest["member"] = member
			return false, nil
		}
		if db.set[id] == nil {
			db.set[id] = map[string]struct{}{}
		}
		db.set[id][member] = struct{}{}
		return true, nil
	case s.listInsertNX:
		id, rest := db.id(args)
		pos := rest[0].(int64)
		if current, ok := db.list[id][pos]; ok {
			dest["pos"] = pos
			dest["value"] = current
			return false, nil
		}
		if db.list[id] == nil {
			db.list[id] = map[int64][]byte{}
		}
		db.list[id][pos] = toBytes(rest[1])
		return true, nil
	}
	return false, fmt.Errorf("fakeDB: unexpected MapScanCAS statement: %s", stmt)
}

func (db *fakeDB) Iterate(ctx context.Context, stmt string, args []any, opt iterOptions) rowIterator {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record(stmt)
	if err := db.injected(stmt); err != nil {
		return &fakeIter{err: err}
	}

	s := db.sch
	switch stmt {
	case s.kvScan:
		bucket := ""
		if db.sch.bucketed {
			bucket = args[0].(string)
		}
		keys := make([]string, 0, len(db.kv))
		byKey := map[string]*fakeRow{}
		for _, row := range db.kv {
			if db.sch.bucketed && row.bucket != bucket {
				continue
			}
			keys = append(keys, row.key)
			byKey[row.key] = row
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
			iter.rows = append(iter.rows, []any{key, byKey[key].typ})
		}
		if opt.singlePage && end < len(keys) {
			iter.state = []byte(strconv.Itoa(end))
		}
		return iter
	case s.hashSelectAll:
		id, _ := db.id(args)
		fields := make([]string, 0, len(db.hash[id]))
		for field := range db.hash[id] {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		iter := &fakeIter{}
		for _, field := range fields {
			iter.rows = append(iter.rows, []any{field, db.hash[id][field]})
		}
		return iter
	case s.setSelectAll:
		id, _ := db.id(args)
		members := make([]string, 0, len(db.set[id]))
		for member := range db.set[id] {
			members = append(members, member)
		}
		sort.Strings(members)
		iter := &fakeIter{}
		for _, member := range members {
			iter.rows = append(iter.rows, []any{member})
		}
		return iter
	case s.listSelectAll:
		id, _ := db.id(args)
		iter := &fakeIter{}
		for _, pos := range sortedPositions(db.list[id]) {
			iter.rows = append(iter.rows, []any{db.list[id][pos]})
		}
		return iter
	}
	return &fakeIter{err: fmt.Errorf("fakeDB: unexpected Iterate statement: %s", stmt)}
}

func (db *fakeDB) BatchCAS(ctx context.Context, batchType gocql.BatchType, stmts []batchStatement) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.record("BATCH")

	partitions := map[string]struct{}{}
	for _, st := range stmts {
		if st.stmt == db.sch.kvGuardCAS {
			bucket, _ := db.idParts(st.args[1:])
			partitions[bucket] = struct{}{}
			id, rest := db.id(st.args[1:])
			expect := toBytes(rest[0])
			row, ok := db.kv[id]
			if !ok || !bytes.Equal(row.value, expect) {
				return false, nil
			}
			continue
		}
		bucket, _ := db.idParts(st.args)
		partitions[bucket] = struct{}{}
	}
	if len(partitions) > 1 {
		// Mirrors the server: a conditional batch may not span partitions.
		return false, errors.New("fakeDB: conditional batch spans multiple partitions")
	}

	for _, st := range stmts {
		if st.stmt == db.sch.kvGuardCAS {
			id, rest := db.id(st.args[1:])
			_ = rest
			db.kv[id].value = toBytes(st.args[0])
			continue
		}
		if err := db.execLocked(st.stmt, st.args); err != nil {
			return false, err
		}
	}
	return true, nil
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
	case **int:
		switch v := value.(type) {
		case nil:
			*d = nil
		case int:
			cp := v
			*d = &cp
		default:
			return fmt.Errorf("fakeDB: cannot assign %T to **int", value)
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

func sortedPositions(m map[int64][]byte) []int64 {
	out := make([]int64, 0, len(m))
	for pos := range m {
		out = append(out, pos)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

// ---------------------------------------------------------------------------

func TestSetGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	value, err := client.Get(ctx, "greeting").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if value != "hello" {
		t.Fatalf("Get = %q, want %q", value, "hello")
	}

	if err := client.Get(ctx, "missing").Err(); err != Nil {
		t.Fatalf("Get(missing) error = %v, want Nil", err)
	}
}

func TestSetKeepTTLPreservesExpiry(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "session", "a", 90*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Set(ctx, "session", "b", KeepTTL).Err(); err != nil {
		t.Fatalf("Set KeepTTL: %v", err)
	}

	row := db.kv["session"]
	if row.ttl != 90 {
		t.Fatalf("ttl = %d, want 90", row.ttl)
	}
	if string(row.value) != "b" {
		t.Fatalf("value = %q, want b", row.value)
	}
}

func TestSetExRejectsNonPositiveExpiration(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.SetEx(ctx, "k", "v", 0).Err(); !errors.Is(err, ErrInvalidExpire) {
		t.Fatalf("SetEx(0) error = %v, want ErrInvalidExpire", err)
	}
	if err := client.SetEx(ctx, "k", "v", -time.Second).Err(); !errors.Is(err, ErrInvalidExpire) {
		t.Fatalf("SetEx(-1s) error = %v, want ErrInvalidExpire", err)
	}
}

func TestSetNXIsAtomicAndSingleRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	ok, err := client.SetNX(ctx, "lock", "owner-a", time.Minute).Result()
	if err != nil || !ok {
		t.Fatalf("first SetNX = %v, %v; want true, nil", ok, err)
	}
	ok, err = client.SetNX(ctx, "lock", "owner-b", time.Minute).Result()
	if err != nil || ok {
		t.Fatalf("second SetNX = %v, %v; want false, nil", ok, err)
	}
	if got := string(db.kv["lock"].value); got != "owner-a" {
		t.Fatalf("lock holder = %q, want owner-a", got)
	}
	if n := db.count(db.sch.kvSelect); n != 0 {
		t.Fatalf("SetNX issued %d reads, want 0 (conditional insert only)", n)
	}
}

func TestIncrPreservesTTLAndDetectsOverflow(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "counter", "1", 30*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := client.Incr(ctx, "counter").Result()
	if err != nil || got != 2 {
		t.Fatalf("Incr = %d, %v; want 2, nil", got, err)
	}
	if db.kv["counter"].ttl != 30 {
		t.Fatalf("ttl = %d, want 30 (Incr must not drop the expiry)", db.kv["counter"].ttl)
	}

	if err := client.Set(ctx, "big", strconv.FormatInt(1<<62, 10), 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.IncrBy(ctx, "big", 1<<62).Err(); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("IncrBy overflow error = %v, want ErrIncrOverflow", err)
	}

	if err := client.Set(ctx, "word", "abc", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Incr(ctx, "word").Err(); !errors.Is(err, ErrValueNotInteger) {
		t.Fatalf("Incr(non numeric) error = %v, want ErrValueNotInteger", err)
	}
}

func TestDecrByMinInt64DoesNotWrap(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.DecrBy(ctx, "k", -9223372036854775808).Err(); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("DecrBy(MinInt64) error = %v, want ErrIncrOverflow", err)
	}
}

func TestAppendPreservesTTL(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "buf", "ab", 45*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	length, err := client.Append(ctx, "buf", "cd").Result()
	if err != nil || length != 4 {
		t.Fatalf("Append = %d, %v; want 4, nil", length, err)
	}
	if db.kv["buf"].ttl != 45 {
		t.Fatalf("ttl = %d, want 45", db.kv["buf"].ttl)
	}
}

func TestGetSetAndGetDel(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.GetSet(ctx, "k", "v1").Err(); err != Nil {
		t.Fatalf("GetSet on missing key error = %v, want Nil", err)
	}
	previous, err := client.GetSet(ctx, "k", "v2").Result()
	if err != nil || previous != "v1" {
		t.Fatalf("GetSet = %q, %v; want v1, nil", previous, err)
	}

	value, err := client.GetDel(ctx, "k").Result()
	if err != nil || value != "v2" {
		t.Fatalf("GetDel = %q, %v; want v2, nil", value, err)
	}
	if _, ok := db.kv["k"]; ok {
		t.Fatal("GetDel left the row in place")
	}
	if err := client.GetDel(ctx, "k").Err(); err != Nil {
		t.Fatalf("GetDel on missing key error = %v, want Nil", err)
	}
}

func TestWrongTypeIsReported(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.HSet(ctx, "profile", "name", "ada").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Get(ctx, "profile").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Get on hash key error = %v, want WRONGTYPE", err)
	}

	if err := client.Set(ctx, "plain", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.SAdd(ctx, "plain", "m").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SAdd on string key error = %v, want WRONGTYPE", err)
	}
}

func TestSetReplacesCollectionKey(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.HSet(ctx, "k", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Set(ctx, "k", "plain", 0).Err(); err != nil {
		t.Fatalf("Set over hash: %v", err)
	}
	if len(db.hash["k"]) != 0 {
		t.Fatal("hash rows survived a SET that replaced the key")
	}
	if got := client.Get(ctx, "k").Val(); got != "plain" {
		t.Fatalf("Get = %q, want plain", got)
	}
}

func TestDelCountsAndCascades(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.SAdd(ctx, "tags", "a", "b").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if err := client.Set(ctx, "plain", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	removed, err := client.Del(ctx, "tags", "plain", "missing").Result()
	if err != nil {
		t.Fatalf("Del: %v", err)
	}
	if removed != 2 {
		t.Fatalf("Del = %d, want 2", removed)
	}
	if len(db.set["tags"]) != 0 {
		t.Fatal("set members survived Del of the key")
	}
}

func TestExistsSeesEveryKeyType(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.LPush(ctx, "queue", "job").Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if err := client.HSet(ctx, "profile", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	found, err := client.Exists(ctx, "queue", "profile", "nope").Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if found != 2 {
		t.Fatalf("Exists = %d, want 2", found)
	}
}

func TestExpireTTLPersist(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := client.TTL(ctx, "k").Val(); got != -1*time.Second {
		t.Fatalf("TTL without expiry = %v, want -1s", got)
	}
	if got := client.TTL(ctx, "missing").Val(); got != -2*time.Second {
		t.Fatalf("TTL of missing key = %v, want -2s", got)
	}

	ok, err := client.Expire(ctx, "k", time.Minute).Result()
	if err != nil || !ok {
		t.Fatalf("Expire = %v, %v; want true, nil", ok, err)
	}
	if db.kv["k"].ttl != 60 {
		t.Fatalf("ttl = %d, want 60", db.kv["k"].ttl)
	}
	if string(db.kv["k"].value) != "v" {
		t.Fatalf("Expire changed the value to %q", db.kv["k"].value)
	}

	ok, err = client.Persist(ctx, "k").Result()
	if err != nil || !ok {
		t.Fatalf("Persist = %v, %v; want true, nil", ok, err)
	}
	if db.kv["k"].ttl != 0 {
		t.Fatalf("ttl after Persist = %d, want 0", db.kv["k"].ttl)
	}

	ok, err = client.Expire(ctx, "k", 0).Result()
	if err != nil || !ok {
		t.Fatalf("Expire(0) = %v, %v; want true, nil", ok, err)
	}
	if _, exists := db.kv["k"]; exists {
		t.Fatal("Expire(0) must delete the key")
	}
}

func TestRenameSameKeyIsNoOp(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "k", "v", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Rename(ctx, "k", "k").Err(); err != nil {
		t.Fatalf("Rename onto itself: %v", err)
	}
	if got := client.Get(ctx, "k").Val(); got != "v" {
		t.Fatalf("Get after self rename = %q, want v", got)
	}
	if err := client.Rename(ctx, "gone", "gone").Err(); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("Rename missing key error = %v, want ErrNoSuchKey", err)
	}
}

func TestRenamePreservesTTL(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "src", "v", 120*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Rename(ctx, "src", "dst").Err(); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if db.kv["dst"].ttl != 120 {
		t.Fatalf("dst ttl = %d, want 120", db.kv["dst"].ttl)
	}
	if _, ok := db.kv["src"]; ok {
		t.Fatal("source survived Rename")
	}
}

func TestCopySemantics(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	copied, err := client.Copy(ctx, "missing", "dst", 0, false).Result()
	if err != nil {
		t.Fatalf("Copy of missing source returned an error: %v", err)
	}
	if copied != 0 {
		t.Fatalf("Copy = %d, want 0", copied)
	}

	if err := client.Set(ctx, "src", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Set(ctx, "dst", "other", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if copied, _ := client.Copy(ctx, "src", "dst", 0, false).Result(); copied != 0 {
		t.Fatalf("Copy without replace = %d, want 0", copied)
	}
	if copied, _ := client.Copy(ctx, "src", "dst", 0, true).Result(); copied != 1 {
		t.Fatalf("Copy with replace = %d, want 1", copied)
	}
	if got := client.Get(ctx, "dst").Val(); got != "v" {
		t.Fatalf("dst = %q, want v", got)
	}
}

func TestHashCountsAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	created, err := client.HSet(ctx, "h", "a", "1", "b", "2").Result()
	if err != nil || created != 2 {
		t.Fatalf("HSet = %d, %v; want 2, nil", created, err)
	}
	created, err = client.HSet(ctx, "h", "a", "9").Result()
	if err != nil || created != 0 {
		t.Fatalf("HSet overwrite = %d, %v; want 0, nil", created, err)
	}
	if got := client.HGet(ctx, "h", "a").Val(); got != "9" {
		t.Fatalf("HGet = %q, want 9", got)
	}

	all, err := client.HGetAll(ctx, "h").Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if len(all) != 2 || all["b"] != "2" {
		t.Fatalf("HGetAll = %v", all)
	}

	removed, err := client.HDel(ctx, "h", "a", "zz").Result()
	if err != nil || removed != 1 {
		t.Fatalf("HDel = %d, %v; want 1, nil", removed, err)
	}
}

func TestSetCommandsAndServerSideCard(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	added, err := client.SAdd(ctx, "s", "a", "b", "a").Result()
	if err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if added != 2 {
		t.Fatalf("SAdd = %d, want 2", added)
	}
	if got := client.SCard(ctx, "s").Val(); got != 2 {
		t.Fatalf("SCard = %d, want 2", got)
	}
	if db.count(db.sch.setCount) == 0 {
		t.Fatal("SCard must count on the server")
	}
	if db.count(db.sch.setSelectAll) != 0 {
		t.Fatal("SCard must not stream every member to the client")
	}
	if ok := client.SIsMember(ctx, "s", "b").Val(); !ok {
		t.Fatal("SIsMember(b) = false, want true")
	}
	if removed := client.SRem(ctx, "s", "b", "zz").Val(); removed != 1 {
		t.Fatalf("SRem = %d, want 1", removed)
	}
}

func TestListPushPopOrdering(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.RPush(ctx, "q", "one", "two").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	if err := client.LPush(ctx, "q", "zero").Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if got := client.LLen(ctx, "q").Val(); got != 3 {
		t.Fatalf("LLen = %d, want 3", got)
	}
	if got := client.LPop(ctx, "q").Val(); got != "zero" {
		t.Fatalf("LPop = %q, want zero", got)
	}
	if got := client.RPop(ctx, "q").Val(); got != "two" {
		t.Fatalf("RPop = %q, want two", got)
	}

	popped, err := client.BLPop(ctx, time.Second, "q").Result()
	if err != nil {
		t.Fatalf("BLPop: %v", err)
	}
	if len(popped) != 2 || popped[0] != "q" || popped[1] != "one" {
		t.Fatalf("BLPop = %v", popped)
	}
}

func TestBlockingPopTimesOutWithBoundedPolling(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) {
		o.BlockingPollInterval = time.Millisecond
		o.BlockingPollMaxInterval = 4 * time.Millisecond
	})

	start := time.Now()
	err := client.BLPop(ctx, 40*time.Millisecond, "empty").Err()
	if err != Nil {
		t.Fatalf("BLPop timeout error = %v, want Nil", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("BLPop returned after %v, want at least the timeout", elapsed)
	}
	// With backoff the poll count stays far below timeout/interval.
	if polls := db.count(db.sch.listEdgeAsc); polls > 30 {
		t.Fatalf("BLPop polled %d times in 40ms, backoff is not being applied", polls)
	}
}

func TestKeysCoversAllTypesAndHidesGuard(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "user:1", "a", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "user:2", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.SAdd(ctx, "other", "m").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	db.kv[guardKey] = &fakeRow{key: guardKey, typ: string(typeGuard), value: []byte("0")}

	keys, err := client.Keys(ctx, "user:*").Result()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 2 || keys[0] != "user:1" || keys[1] != "user:2" {
		t.Fatalf("Keys = %v, want [user:1 user:2]", keys)
	}

	all, err := client.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("Keys(*): %v", err)
	}
	for _, key := range all {
		if key == guardKey {
			t.Fatal("Keys exposed the internal guard row")
		}
	}
}

func TestScanPagesWithServerCursor(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("k%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var (
		seen   []string
		cursor uint64
	)
	for i := 0; i < 10; i++ {
		page, next, err := client.Scan(ctx, cursor, "k*", 2).Result()
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		seen = append(seen, page...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if cursor != 0 {
		t.Fatal("Scan did not terminate")
	}
	if len(seen) != 5 {
		t.Fatalf("Scan returned %d keys, want 5: %v", len(seen), seen)
	}

	if err := client.Scan(ctx, 999999, "k*", 2).Err(); !errors.Is(err, ErrCursorUnknown) {
		t.Fatalf("Scan with unknown cursor error = %v, want ErrCursorUnknown", err)
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern string
		input   string
		want    bool
	}{
		{"*", "anything", true},
		{"user:*", "user:1", true},
		{"user:*", "order:1", false},
		{"h?llo", "hello", true},
		{"h?llo", "heello", false},
		{"h[ae]llo", "hallo", true},
		{"h[^ae]llo", "hallo", false},
		{"h[a-c]llo", "hbllo", true},
		{"\\*literal", "*literal", true},
		{"a*b*c", "axxbxxc", true},
		{"a*b*c", "axxbxx", false},
		{"[unterminated", "[unterminated", true},
		{"*a*a*a*a*a*b", strings.Repeat("a", 40), false},
	}
	for _, tc := range cases {
		if got := globMatch(tc.pattern, tc.input); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.input, got, tc.want)
		}
	}
}

func TestSortDefaultsToNoLimit(t *testing.T) {
	values := []string{"3", "1", "2"}

	cfg, err := normalizeSortOptions(&Sort{})
	if err != nil {
		t.Fatalf("normalizeSortOptions: %v", err)
	}
	out, err := sortAndSliceValues(values, cfg)
	if err != nil {
		t.Fatalf("sortAndSliceValues: %v", err)
	}
	if strings.Join(out, ",") != "1,2,3" {
		t.Fatalf("sorted = %v, want [1 2 3]; a zero Count must not mean LIMIT 0", out)
	}

	cfg, err = normalizeSortOptions(&Sort{Order: "DESC", Count: 2})
	if err != nil {
		t.Fatalf("normalizeSortOptions: %v", err)
	}
	out, err = sortAndSliceValues(values, cfg)
	if err != nil {
		t.Fatalf("sortAndSliceValues: %v", err)
	}
	if strings.Join(out, ",") != "3,2" {
		t.Fatalf("sorted = %v, want [3 2]", out)
	}

	cfg, _ = normalizeSortOptions(&Sort{})
	if _, err := sortAndSliceValues([]string{"a"}, cfg); !errors.Is(err, ErrScoreNotDouble) {
		t.Fatalf("numeric sort of non numeric data error = %v, want ErrScoreNotDouble", err)
	}
}

func TestSortUsesDeclaredKeyType(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.SAdd(ctx, "s", "2", "1").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	// A stale list partition under the same key must not shadow the set.
	db.list["s"] = map[int64][]byte{0: []byte("9")}

	out, err := client.Sort(ctx, "s", &Sort{}).Result()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if strings.Join(out, ",") != "1,2" {
		t.Fatalf("Sort = %v, want [1 2]", out)
	}
}

func TestAtomicMSetUsesSinglePartitionBatch(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) {
		o.AtomicMSetByBucket = true
		o.Bucket = "tenant-1"
	})

	if err := client.MSet(ctx, "a", "1", "b", "2").Err(); err != nil {
		t.Fatalf("atomic MSet: %v", err)
	}
	if got := client.Get(ctx, "a").Val(); got != "1" {
		t.Fatalf("Get(a) = %q, want 1", got)
	}
	if db.count("BATCH") != 1 {
		t.Fatalf("expected exactly one batch, got %d", db.count("BATCH"))
	}

	guard := db.kv["tenant-1\x00"+guardKey]
	if guard == nil || string(guard.value) != "1" {
		t.Fatalf("guard version = %v, want 1", guard)
	}

	if err := client.MSet(ctx, "c", "3").Err(); err != nil {
		t.Fatalf("second atomic MSet: %v", err)
	}
	if string(db.kv["tenant-1\x00"+guardKey].value) != "2" {
		t.Fatal("guard version did not advance")
	}
}

func TestAtomicMSetRejectsOversizedBatch(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) {
		o.AtomicMSetByBucket = true
		o.AtomicMSetMaxPairs = 2
	})

	err := client.MSet(ctx, "a", "1", "b", "2", "c", "3").Err()
	if err == nil || !strings.Contains(err.Error(), "at most 2 pairs") {
		t.Fatalf("MSet error = %v, want pair cap error", err)
	}
}

func TestBucketedViewsShareInitAndIsolateKeys(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) { o.PartitionByBucket = true })

	tenantA := client.Bucketed("a")
	tenantB := client.Bucketed("b")

	if err := tenantA.Set(ctx, "k", "va", 0).Err(); err != nil {
		t.Fatalf("Set in bucket a: %v", err)
	}
	if err := tenantB.Set(ctx, "k", "vb", 0).Err(); err != nil {
		t.Fatalf("Set in bucket b: %v", err)
	}

	if got := tenantA.Get(ctx, "k").Val(); got != "va" {
		t.Fatalf("bucket a Get = %q, want va", got)
	}
	if got := tenantB.Get(ctx, "k").Val(); got != "vb" {
		t.Fatalf("bucket b Get = %q, want vb", got)
	}
	keys, err := tenantA.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("bucket a Keys = %v, want one key", keys)
	}
	if n := db.count("CREATE TABLE"); n != len(db.sch.ddl) {
		t.Fatalf("schema created %d times, want once across views", n/len(db.sch.ddl))
	}
}

func TestBucketedRequiresBucketMode(t *testing.T) {
	client, _ := newTestClient(t, nil)
	view := client.Bucketed("a")
	if view.configErr == nil {
		t.Fatal("Bucketed on a non bucketed client must fail")
	}
	if err := view.Set(context.Background(), "k", "v", 0).Err(); err == nil {
		t.Fatal("operations on a misconfigured view must fail")
	}
}

func TestInitFailureIsRetried(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	db.fail["CREATE TABLE"] = errors.New("cluster unavailable")
	if err := client.Set(ctx, "k", "v", 0).Err(); err == nil {
		t.Fatal("Set must fail while the schema cannot be created")
	}

	delete(db.fail, "CREATE TABLE")
	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after recovery: %v (a transient init failure must not be latched)", err)
	}
}

func TestInitIgnoresCallerCancellation(t *testing.T) {
	client, _ := newTestClient(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := client.ensureReady(ctx); err != nil {
		t.Fatalf("ensureReady with a cancelled context: %v", err)
	}
	if !client.core.initialized.Load() {
		t.Fatal("schema initialization must not be abandoned because one caller cancelled")
	}
}

func TestIdentifiersAreValidated(t *testing.T) {
	cases := []Options{
		{Keyspace: "ks", Table: "kv; DROP TABLE users --"},
		{Keyspace: "ks with space", Table: "kv"},
		{Keyspace: "ks", Table: "a.b.c"},
		{Keyspace: "ks", Table: "1kv"},
	}
	for _, opt := range cases {
		client := newConfiguredClient(&opt)
		if client.configErr == nil {
			t.Errorf("keyspace=%q table=%q was accepted, want rejection", opt.Keyspace, opt.Table)
		}
	}

	ok := newConfiguredClient(&Options{Keyspace: "ks", Table: "redis_compat_kv"})
	if ok.configErr != nil {
		t.Fatalf("valid identifiers rejected: %v", ok.configErr)
	}
}

func TestReservedKeyIsRejected(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, guardKey, "v", 0).Err(); !errors.Is(err, ErrReservedKey) {
		t.Fatalf("Set on the guard key error = %v, want ErrReservedKey", err)
	}
}

func TestMGetSkipsWrongTypeKeys(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "a", "1", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "b", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	values, err := client.MGet(ctx, "a", "b", "c").Result()
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(values) != 3 || values[0] != "1" || values[1] != nil || values[2] != nil {
		t.Fatalf("MGet = %v, want [1 <nil> <nil>]", values)
	}
}

func TestTTLClampsToServerLimit(t *testing.T) {
	if got := ttlSecondsFromDuration(time.Duration(1<<62) * time.Nanosecond); got != maxTTLSeconds {
		t.Fatalf("ttlSecondsFromDuration = %d, want %d", got, maxTTLSeconds)
	}
	if got := ttlSecondsFromDuration(time.Millisecond); got != 1 {
		t.Fatalf("ttlSecondsFromDuration(1ms) = %d, want 1", got)
	}
}

// TestStatementPlaceholdersMatchArguments pairs every statement with the
// builder that supplies its arguments. A mismatch is only visible to a real
// server ("Unmatched column names/values"), so it is checked here instead.
func TestStatementPlaceholdersMatchArguments(t *testing.T) {
	value := []byte("v")

	for _, bucketed := range []bool{false, true} {
		bucketed := bucketed
		name := "flat"
		if bucketed {
			name = "bucketed"
		}
		t.Run(name, func(t *testing.T) {
			client, _ := newTestClient(t, func(o *Options) { o.PartitionByBucket = bucketed })
			s := client.core.schema

			cases := []struct {
				name string
				stmt string
				args []any
			}{
				{"kvUpsert", s.kvUpsert, client.kvWriteArgs("k", typeString, value)},
				{"kvUpsertTTL", s.kvUpsertTTL, client.kvWriteTTLArgs("k", typeString, value, 1)},
				{"kvInsertNX", s.kvInsertNX, client.kvWriteArgs("k", typeString, value)},
				{"kvInsertNXTTL", s.kvInsertNXTTL, client.kvWriteTTLArgs("k", typeString, value, 1)},
				{"kvMarkerNX", s.kvMarkerNX, client.kvMarkerArgs("k", typeHash)},
				{"kvUpdateCAS", s.kvUpdateCAS, client.kvCASArgs("k", typeString, value, value)},
				{"kvUpdateCASTTL", s.kvUpdateCASTTL, client.kvCASTTLArgs("k", typeString, value, value, 1)},
				{"kvSelect", s.kvSelect, client.keyArgs("k")},
				{"kvSelectTTL", s.kvSelectTTL, client.keyArgs("k")},
				{"kvSelectType", s.kvSelectType, client.keyArgs("k")},
				{"kvSelectKey", s.kvSelectKey, client.keyArgs("k")},
				{"kvDelete", s.kvDelete, client.keyArgs("k")},
				{"kvDeleteIfExists", s.kvDeleteIfExists, client.keyArgs("k")},
				{"kvDeleteCAS", s.kvDeleteCAS, client.kvDeleteCASArgs("k", value)},
				{"kvGuardCAS", s.kvGuardCAS, client.guardCASArgs(value, value)},
				{"kvScan", s.kvScan, client.kvScanArgs()},
				{"hashUpsert", s.hashUpsert, client.hashWriteArgs("k", "f", value)},
				{"hashInsertNX", s.hashInsertNX, client.hashWriteArgs("k", "f", value)},
				{"hashSelect", s.hashSelect, client.hashFieldArgs("k", "f")},
				{"hashDeleteIf", s.hashDeleteIf, client.hashFieldArgs("k", "f")},
				{"hashDeleteKey", s.hashDeleteKey, client.keyArgs("k")},
				{"hashSelectAll", s.hashSelectAll, client.keyArgs("k")},
				{"setInsertNX", s.setInsertNX, client.setMemberArgs("k", "m")},
				{"setSelect", s.setSelect, client.setMemberArgs("k", "m")},
				{"setDeleteIf", s.setDeleteIf, client.setMemberArgs("k", "m")},
				{"setDeleteKey", s.setDeleteKey, client.keyArgs("k")},
				{"setSelectAll", s.setSelectAll, client.keyArgs("k")},
				{"setCount", s.setCount, client.keyArgs("k")},
				{"listInsertNX", s.listInsertNX, client.listWriteArgs("k", 1, value)},
				{"listEdgeAsc", s.listEdgeAsc, client.keyArgs("k")},
				{"listEdgeDesc", s.listEdgeDesc, client.keyArgs("k")},
				{"listDeleteIf", s.listDeleteIf, client.listPosArgs("k", 1)},
				{"listDeleteKey", s.listDeleteKey, client.keyArgs("k")},
				{"listSelectAll", s.listSelectAll, client.keyArgs("k")},
				{"listCount", s.listCount, client.keyArgs("k")},
			}

			for _, tc := range cases {
				if got, want := strings.Count(tc.stmt, "?"), len(tc.args); got != want {
					t.Errorf("%s: %d placeholders but %d arguments\n  %s", tc.name, got, want, tc.stmt)
				}
			}

			// An INSERT must also bind one value per named column.
			for _, tc := range cases {
				if !strings.HasPrefix(tc.stmt, "INSERT INTO ") {
					continue
				}
				columns := strings.Count(betweenParens(tc.stmt, "INSERT INTO"), ",") + 1
				values := strings.Count(betweenParens(tc.stmt, "VALUES"), "?")
				if columns != values {
					t.Errorf("%s: %d columns but %d values\n  %s", tc.name, columns, values, tc.stmt)
				}
			}
		})
	}
}

// betweenParens returns the contents of the first parenthesised group that
// follows marker.
func betweenParens(stmt, marker string) string {
	rest := stmt[strings.Index(stmt, marker)+len(marker):]
	open := strings.Index(rest, "(")
	close := strings.Index(rest, ")")
	if open < 0 || close < open {
		return ""
	}
	return rest[open+1 : close]
}

func TestConcurrentIncrementsDoNotLoseUpdates(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	const workers = 8
	const perWorker = 25

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := client.Incr(ctx, "counter").Err(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Incr: %v", err)
	}

	got, err := client.Get(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != strconv.Itoa(workers*perWorker) {
		t.Fatalf("counter = %s, want %d", got, workers*perWorker)
	}
}
