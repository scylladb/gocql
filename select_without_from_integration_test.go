//go:build integration

package gocql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// Scylla's no-FROM SELECT resolves selectors against virtual system.one_row.
// Capability is probed, since development servers may implement it before a release.
var selectWithoutFromOnce sync.Once
var selectWithoutFromSupported bool
var selectWithoutFromProbeErr error

func skipUnlessSelectWithoutFrom(tb testing.TB) {
	tb.Helper()
	if *flagDistribution == "cassandra" {
		tb.Skip("SELECT without FROM is a ScyllaDB feature")
	}
	selectWithoutFromOnce.Do(probeSelectWithoutFrom)
	if selectWithoutFromProbeErr != nil {
		tb.Fatalf("probe SELECT without FROM: %v", selectWithoutFromProbeErr)
	}
	if !selectWithoutFromSupported {
		tb.Skip("server does not support SELECT without FROM")
	}
}
func probeSelectWithoutFrom() {
	session, err := Connect(context.Background(), createV2Cluster())
	if err != nil {
		selectWithoutFromProbeErr = err
		return
	}
	defer session.Close()
	row, err := v2ReadOne(context.Background(), session, "SELECT 1")
	if err == nil {
		value, e := row.At(0)
		if e != nil {
			selectWithoutFromProbeErr = e
			return
		}
		number, e := value.Signed()
		if e != nil {
			selectWithoutFromProbeErr = e
			return
		}
		if number != 1 {
			selectWithoutFromProbeErr = fmt.Errorf("SELECT 1 returned %d", number)
			return
		}
		selectWithoutFromSupported = true
		return
	}
	if serverErrorCode(err) != ErrCodeSyntax {
		selectWithoutFromProbeErr = err
	}
}
func serverErrorCode(err error) int {
	var server RequestError
	if errors.As(err, &server) {
		return server.GetCode()
	}
	var frame frm.ErrorFrame
	if errors.As(err, &frame) {
		return frame.Code
	}
	return 0
}
func isRejectedByServer(err error) bool {
	code := serverErrorCode(err)
	return code == ErrCodeSyntax || code == ErrCodeInvalid
}
func createSessionWithoutKeyspace(t testing.TB) *Session {
	t.Helper()
	config := createV2Cluster()
	if config.Keyspace != "" {
		t.Fatal("unexpected default keyspace")
	}
	session, err := Connect(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	return session
}
func noFromOne(t testing.TB, s *Session, stmt string, values ...Value) DynamicRow {
	t.Helper()
	row, err := v2ReadOne(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
func noFromValue(t testing.TB, row DynamicRow, index int) Value {
	t.Helper()
	value, err := row.At(index)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func noFromInt(t testing.TB, row DynamicRow, index int) int64 {
	t.Helper()
	n, err := noFromValue(t, row, index).Signed()
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func noFromText(t testing.TB, row DynamicRow, index int) string {
	t.Helper()
	value, err := noFromValue(t, row, index).Text()
	if err != nil {
		t.Fatal(err)
	}
	return value.String()
}
func noFromRows(t testing.TB, s *Session, stmt string, values ...Value) *Rows[DynamicRow] {
	t.Helper()
	rows, err := v2Query(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	})
	return rows
}
func noFromColumn(t testing.TB, columns Columns, index int) ColumnSpec {
	t.Helper()
	column, ok := columns.At(index)
	if !ok {
		t.Fatalf("column %d absent", index)
	}
	return column
}
func logColumns(t testing.TB, stmt string, columns Columns) {
	t.Helper()
	for i := 0; i < columns.Len(); i++ {
		column, _ := columns.At(i)
		t.Logf("%s: %d = %s.%s.%s (%s)", stmt, i, column.Keyspace, column.Table, column.Name, column.Type.Kind())
	}
}

func TestSelectWithoutFromLiteral(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	rows := noFromRows(t, session, "SELECT 1")
	if rows.Columns().Len() != 1 {
		t.Fatal("SELECT 1 must have one column")
	}
	if noFromColumn(t, rows.Columns(), 0).Type.Kind() != TypeInt {
		t.Fatal("literal must have int metadata")
	}
	if rows.Info().Rows != 1 {
		t.Fatalf("page rows=%d want1", rows.Info().Rows)
	}
	if !rows.Next() {
		t.Fatalf("missing row: %v", rows.Err())
	}
	if n := noFromInt(t, rows.Row(), 0); n != 1 {
		t.Fatalf("got%d want1", n)
	}
	if rows.Next() {
		t.Fatal("virtual table returned a second row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
func TestSelectWithoutFromFilteredOut(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	const kept = `SELECT 1 WHERE "system$dummy" = ''`
	const filtered = `SELECT 1 WHERE "system$dummy" = 'no'`
	t.Run("predicate keeps the row", func(t *testing.T) {
		if noFromInt(t, noFromOne(t, session, kept), 0) != 1 {
			t.Fatal("wrong kept value")
		}
	})
	t.Run("predicate removes the row", func(t *testing.T) {
		rows := noFromRows(t, session, filtered)
		if rows.Columns().Len() != 1 || rows.Info().Rows != 0 {
			t.Fatal("empty result must retain one column and zero rows")
		}
		if rows.Next() {
			t.Fatal("filtered virtual row returned")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := v2ReadOne(context.Background(), session, filtered); !errors.Is(err, ErrNoRows) {
			t.Fatalf("empty single-row result=%v", err)
		}
	})
}
func TestSelectWithoutFromNow(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	before := time.Now()
	row := noFromOne(t, session, "SELECT now()")
	after := time.Now()
	if noFromColumn(t, row.Columns(), 0).Type.Kind() != TypeTimeUUID {
		t.Fatal("now() metadata must be timeuuid")
	}
	got, err := noFromValue(t, row, 0).UUID()
	if err != nil {
		t.Fatal(err)
	}
	if got.Version() != 1 {
		t.Fatalf("UUID version%d", got.Version())
	}
	if at := got.Time(); at.Before(before.Add(-time.Hour)) || at.After(after.Add(time.Hour)) {
		t.Fatalf("timestamp outside tolerance: %v", at)
	}
}
func TestSelectWithoutFromFunctionCast(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	before := time.Now()
	row := noFromOne(t, session, "SELECT toTimestamp(now())")
	after := time.Now()
	if noFromColumn(t, row.Columns(), 0).Type.Kind() != TypeTimestamp {
		t.Fatal("timestamp metadata expected")
	}
	got, err := Timestamp(noFromInt(t, row, 0)).Time()
	if err != nil {
		t.Fatal(err)
	}
	if got.Before(before.Add(-time.Hour)) || got.After(after.Add(time.Hour)) {
		t.Fatalf("timestamp outside tolerance: %v", got)
	}
}
func TestSelectWithoutFromMultipleSelectors(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	row := noFromOne(t, session, "SELECT 1, 'a', true")
	if row.Columns().Len() != 3 {
		t.Fatal("expected three columns")
	}
	for i, kind := range []Type{TypeInt, TypeVarchar, TypeBoolean} {
		if noFromColumn(t, row.Columns(), i).Type.Kind() != kind {
			t.Fatalf("column%d wrong type", i)
		}
	}
	yes, err := noFromValue(t, row, 2).Bool()
	if err != nil {
		t.Fatal(err)
	}
	if noFromInt(t, row, 0) != 1 || noFromText(t, row, 1) != "a" || !yes {
		t.Fatal("incorrect selector values")
	}
}
func TestSelectWithoutFromAlias(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	row := noFromOne(t, session, "SELECT 1, 'hi' AS greeting, now()")
	names := make([]string, row.Columns().Len())
	for i := range names {
		names[i] = noFromColumn(t, row.Columns(), i).Name
	}
	if !slices.Equal(names, []string{"1", "greeting", "system.now()"}) {
		t.Fatalf("aliases=%v", names)
	}
	if noFromInt(t, row, 0) != 1 || noFromText(t, row, 1) != "hi" {
		t.Fatal("incorrect literal values")
	}
	if _, err := noFromValue(t, row, 2).UUID(); err != nil {
		t.Fatal(err)
	}
	greeting, err := row.Get("greeting")
	if err != nil {
		t.Fatal(err)
	}
	text, err := greeting.Text()
	if err != nil || text.String() != "hi" {
		t.Fatalf("named lookup=%v %v", text, err)
	}
}
func TestSelectWithoutFromExecutionModes(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	const cql = "SELECT 1, 'a', true"
	statement := Statement[Unit, DynamicRow]{CQL: cql, Params: NoParams, Rows: DynamicRows}
	var baseline Columns
	for _, mode := range []string{"prepared", "prepared with metadata", "not prepared"} {
		t.Run(mode, func(t *testing.T) {
			var bound *Bound[DynamicRow]
			var err error
			if mode == "not prepared" {
				bound, err = BindUnpreparedOwned(statement, NewColumns(), Unit{})
			} else {
				prepared, e := Prepare(context.Background(), session, statement, PrepareOptions{})
				if e != nil {
					t.Fatal(e)
				}
				bound, err = BindOwned(prepared, Unit{})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				bound.Close()
				if err := bound.Wait(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			rows, err := Run(context.Background(), session, bound, Options{IncludeMetadata: mode == "prepared with metadata"})
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			columns := rows.Columns()
			logColumns(t, cql+" "+mode, columns)
			if columns.Len() != 3 {
				t.Fatal("expected three columns")
			}
			for i := 0; i < columns.Len(); i++ {
				col, _ := columns.At(i)
				if col.Keyspace != "system" || col.Table != "one_row" {
					t.Fatalf("source=%s.%s", col.Keyspace, col.Table)
				}
			}
			if baseline.Len() == 0 {
				baseline = columns
			} else if !columns.Equal(baseline) {
				t.Fatal("metadata differs across execution modes")
			}
			if !rows.Next() {
				t.Fatalf("missing row: %v", rows.Err())
			}
			row := rows.Row()
			yes, e := noFromValue(t, row, 2).Bool()
			if e != nil || !yes || noFromInt(t, row, 0) != 1 || noFromText(t, row, 1) != "a" {
				t.Fatal("wrong selector values")
			}
			if rows.Next() {
				t.Fatal("extra virtual row")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSelectWithoutFromBindMarker(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	t.Run("value round trip", func(t *testing.T) {
		if noFromInt(t, noFromOne(t, session, "SELECT blobAsInt(?)", BlobValue([]byte{0, 0, 0, 42})), 0) != 42 {
			t.Fatal("wrong bind result")
		}
	})
	t.Run("marker types the column", func(t *testing.T) {
		row := noFromOne(t, session, "SELECT intAsBlob(?)", IntValue(42))
		if noFromColumn(t, row.Columns(), 0).Type.Kind() != TypeBlob {
			t.Fatal("blob type expected")
		}
		blob, err := noFromValue(t, row, 0).Bytes()
		if err != nil || !bytes.Equal(blob.OwnedBytes(), []byte{0, 0, 0, 42}) {
			t.Fatalf("blob=%v err=%v", blob, err)
		}
	})
	t.Run("marker in LIMIT", func(t *testing.T) {
		if noFromInt(t, noFromOne(t, session, "SELECT 1 LIMIT ?", IntValue(1)), 0) != 1 {
			t.Fatal("wrong LIMIT result")
		}
	})
	t.Run("untyped markers rejected", func(t *testing.T) {
		for _, cql := range []string{"SELECT ?", "SELECT (int)?", "SELECT CAST(? AS int)"} {
			rows, err := v2Query(context.Background(), session, cql, IntValue(42))
			if rows != nil {
				_ = rows.Close()
			}
			if !isRejectedByServer(err) {
				t.Fatalf("%q error=%v", cql, err)
			}
		}
	})
}
func TestSelectWithoutFromRoutingKey(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	cases := []struct {
		name, cql string
		keyspace  bool
		values    []Value
	}{
		{name: "no bind markers", cql: "SELECT 1"},
		{name: "no bind markers, with keyspace", cql: "SELECT 1", keyspace: true},
		{name: "bind marker", cql: "SELECT intAsBlob(?)", values: []Value{IntValue(42)}},
		{name: "bind marker, with keyspace", cql: "SELECT intAsBlob(?)", keyspace: true, values: []Value{IntValue(42)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var session *Session
			if tc.keyspace {
				session = createV2Session(t)
			} else {
				session = createSessionWithoutKeyspace(t)
			}
			defer session.Close()
			p, err := Prepare(context.Background(), session, Statement[[]Value, DynamicRow]{CQL: tc.cql, Params: DynamicParams, Rows: DynamicRows}, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			b, err := BindOwned(p, tc.values)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { b.Close(); _ = b.Wait(context.Background()) }()
			// v2 resolves absent partition-key metadata directly. Legacy failed virtual-table
			// lookups are deliberately eliminated; no token may be invented in either case.
			if b.Routing().Token.Set {
				t.Fatal("no-FROM SELECT invented a partition token")
			}
			rows, err := Run(context.Background(), session, b, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatalf("query after routing failed: %v", rows.Err())
			}
			if rows.Next() {
				t.Fatal("unexpected extra row")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSelectWithoutFromTokenAware(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	config := createV2Cluster()
	profile := config.DefaultProfile.Snapshot()
	policy, err := NewRoutingPolicy(RoutingOptions{TokenAware: true, ShuffleReplicas: true})
	if err != nil {
		t.Fatal(err)
	}
	profile.Routing = policy
	if err := config.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	session, err := Connect(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, tc := range []struct {
		name, cql string
		values    []Value
		want      int64
	}{{name: "nil routing key", cql: "SELECT 1", want: 1}, {name: "routing lookup error", cql: "SELECT blobAsInt(?)", values: []Value{BlobValue([]byte{0, 0, 0, 42})}, want: 42}} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				if got := noFromInt(t, noFromOne(t, session, tc.cql, tc.values...), 0); got != tc.want {
					t.Fatalf("execution%d got%d want%d", i, got, tc.want)
				}
			}
		})
	}
}
func TestSelectWithoutFromMapScan(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	const cql = "SELECT 1, now()"
	t.Run("MapScan", func(t *testing.T) {
		row := noFromOne(t, session, cql)
		seen := map[string]bool{}
		for i := 0; i < row.Columns().Len(); i++ {
			col, _ := row.Columns().At(i)
			if _, err := row.Get(col.Name); err != nil {
				t.Fatal(err)
			}
			seen[col.Name] = true
		}
		if len(seen) != row.Columns().Len() {
			t.Fatal("duplicate generated names")
		}
	})
	t.Run("SliceMap", func(t *testing.T) {
		rows := noFromRows(t, session, cql)
		var owned []DynamicRow
		for rows.Next() {
			owned = append(owned, rows.Row().Clone())
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(owned) != 1 {
			t.Fatalf("row count=%d", len(owned))
		}
		if noFromInt(t, owned[0], 0) != 1 {
			t.Fatal("owned row changed")
		}
	})
}
func TestSelectWithoutFromAggregate(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	if got := noFromInt(t, noFromOne(t, session, "SELECT count(5)"), 0); got != 1 {
		t.Fatalf("count(5)=%d", got)
	}
}
func TestSelectWithoutFromJSON(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	if session.cfg.ProtoVersion < protoVersion4 {
		t.Skip("JSON requires protocol4")
	}
	got := noFromText(t, noFromOne(t, session, "SELECT JSON 1"), 0)
	var doc map[string]json.Number
	if err := json.Unmarshal([]byte(got), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc) != 1 || doc["1"].String() != "1" {
		t.Fatalf("incorrect JSON result: %s", got)
	}
}
func TestSelectWithoutFromWithKeyspace(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createV2Session(t)
	defer session.Close()
	if session.cfg.Keyspace == "" {
		t.Fatal("expected configured keyspace")
	}
	if got := noFromInt(t, noFromOne(t, session, "SELECT 1"), 0); got != 1 {
		t.Fatalf("got%d", got)
	}
}
func TestSelectWithoutFromUnqualifiedUDF(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	keyspaced := createV2Session(t)
	defer keyspaced.Close()
	const fn = "select_without_from_plus_one"
	cql := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s.%s(x int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return x + 1'`, keyspaced.cfg.Keyspace, fn)
	if err := v2CreateTable(context.Background(), keyspaced, cql); err != nil {
		if isRejectedByServer(err) {
			t.Skipf("server refuses UDF creation: %v", err)
		}
		t.Fatal(err)
	}
	t.Run("unqualified without a session keyspace", func(t *testing.T) {
		s := createSessionWithoutKeyspace(t)
		defer s.Close()
		_, err := v2ReadOne(context.Background(), s, "SELECT "+fn+"(1)")
		if !isRejectedByServer(err) {
			t.Fatalf("unqualified UDF error=%v", err)
		}
	})
	t.Run("qualified without a session keyspace", func(t *testing.T) {
		s := createSessionWithoutKeyspace(t)
		defer s.Close()
		if noFromInt(t, noFromOne(t, s, "SELECT "+keyspaced.cfg.Keyspace+"."+fn+"(1)"), 0) != 2 {
			t.Fatal("wrong qualified result")
		}
	})
	t.Run("unqualified with a session keyspace", func(t *testing.T) {
		if noFromInt(t, noFromOne(t, keyspaced, "SELECT "+fn+"(1)"), 0) != 2 {
			t.Fatal("wrong unqualified result")
		}
	})
}
func TestSelectWithoutFromRejected(t *testing.T) {
	t.Parallel()
	skipUnlessSelectWithoutFrom(t)
	session := createSessionWithoutKeyspace(t)
	defer session.Close()
	// Exercise the typed transport on the identical connection after every error;
	// host pinning alone would allow a different pooled socket to hide corruption.
	conn, err := session.selectTypedConn(context.Background(), RoutingInfo{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, cql string }{{"star", "SELECT *"}, {"column reference", "SELECT some_column"}, {"distinct", "SELECT DISTINCT 1"}} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := conn.executeTyped(context.Background(), &attemptRequest{Statement: tc.cql, Consistency: One})
			if page != nil {
				page.Owner.Release()
			}
			if !isRejectedByServer(err) {
				t.Fatalf("%q rejection=%v", tc.cql, err)
			}
			page, err = conn.executeTyped(context.Background(), &attemptRequest{Statement: "SELECT 1", Consistency: One})
			if err != nil {
				t.Fatalf("same connection unusable after rejection: %v", err)
			}
			typed, err := newPage(page, OneColumn(Int32), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer typed.Close()
			if !typed.Next() || typed.Row() != 1 {
				t.Fatalf("same connection wrong row: %v", typed.Err())
			}
			if typed.Next() {
				t.Fatal("extra virtual row")
			}
			if err := typed.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
