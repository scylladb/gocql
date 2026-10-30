package gocql

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStep09ConditionalPrepareAbsentResultMetadata(t *testing.T) {
	for _, tc := range []struct {
		name         string
		kind         StatementKind
		initial      bool
		badResult    bool
		prepareError bool
	}{
		{name: "unknown", kind: UnknownStatement},
		{name: "conditional", kind: ConditionalStatement},
		{name: "invalid-execute-metadata", kind: ConditionalStatement, badResult: true},
		{name: "ordinary-still-validates", kind: OrdinaryStatement, prepareError: true},
		{name: "present-metadata-still-validates", kind: UnknownStatement, initial: true, prepareError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			defer s.Close()
			defer peers[0].Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			boolType := codecTestMust(ScalarType(TypeBoolean))
			columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "wrong", Type: boolType})
			initial := NewColumns()
			if tc.initial {
				initial = columns
			}
			done := make(chan error, 1)
			go func() {
				op, stream, _, err := step03ReadRequest(peers[0])
				if err != nil {
					done <- err
					return
				}
				if op != 9 {
					done <- fmt.Errorf("expected PREPARE got %d", op)
					return
				}
				if _, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("cas"), NewColumns(), initial))); err != nil || tc.prepareError {
					done <- err
					return
				}
				op, stream, _, err = step03ReadRequest(peers[0])
				if err != nil {
					done <- err
					return
				}
				if op != 10 {
					done <- fmt.Errorf("expected EXECUTE got %d", op)
					return
				}
				name := "[applied]"
				if tc.badResult {
					name = "wrong"
				}
				body := binary.BigEndian.AppendUint32(nil, 2)
				body = binary.BigEndian.AppendUint32(body, 1)
				body = binary.BigEndian.AppendUint32(body, 1)
				for _, v := range []string{"ks", "tbl", name} {
					body = binary.BigEndian.AppendUint16(body, uint16(len(v)))
					body = append(body, v...)
				}
				body = binary.BigEndian.AppendUint16(body, uint16(TypeBoolean))
				body = binary.BigEndian.AppendUint32(body, 1)
				body = binary.BigEndian.AppendUint32(body, 1)
				body = append(body, 1)
				_, err = peers[0].Write(typedTestResponse(t, 4, stream, body))
				done <- err
			}()
			prepared, err := Prepare(ctx, s, Statement[Unit, CASResult[DynamicRow]]{CQL: "INSERT INTO ks.tbl(id)VALUES(1)IF NOT EXISTS", Kind: tc.kind, Params: NoParams, Rows: CASRows(DynamicRows)}, PrepareOptions{})
			var validation *ValidationError
			if tc.prepareError {
				if !errors.As(err, &validation) {
					t.Fatalf("eager metadata validation: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if implExecution.PreparedData(prepared).InternalPlans.InternalPlan != nil {
					t.Fatal("absent shape compiled eagerly")
				}
				bound := codecTestMust(BindOwned(prepared, Unit{}))
				defer bound.Close()
				rows, err := Run(ctx, s, bound, Options{})
				if tc.badResult {
					if !errors.As(err, &validation) {
						t.Fatalf("actual metadata was not validated before output: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					var out CASResult[DynamicRow]
					if !rows.NextInto(&out) || !out.Applied || out.Previous.Set {
						t.Fatal("conditional result", out, rows.Err())
					}
					if rows.NextInto(&out) || rows.Close() != nil {
						t.Fatal("conditional result completion", rows.Err())
					}
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Result-only schema evolution cannot invalidate an input with no partition-key
// components. The existing parameterized hasher/order regressions stay strict.
func TestStep09ReprepareResultOnlyShapeWithoutRoutingHasher(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	typ := codecTestMust(ScalarType(TypeBlob))
	oldColumns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
	state, old := step03PreparedState(t, s, oldColumns)
	state.InternalPartitioner = "known-before-schema-invalidation"
	newColumns := NewColumns(cqltype.ColumnSlice(oldColumns)[0], ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "added", Type: typ})
	done := make(chan error, 1)
	go func() {
		_, stream, _, err := step03ReadRequest(peers[0])
		if err == nil {
			_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("changed"), NewColumns(), newColumns)))
		}
		done <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	updated, err := runtimeFor(s).InternalPrepareOnConnExpected(ctx, conns[0], state, encodedValues{Columns: NewColumns()}, true, old)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.InternalColumns.Equal(newColumns) || state.InternalParameters.Len() != 0 || len(state.InternalInfo.PartitionKeyIndexes) != 0 {
		t.Fatal("result-only reprepare changed canonical inputs")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStep09StreamSchemaReconcileBeforeContinuationDoesNotReplay(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("continuation-error=%t", fail), func(t *testing.T) { step09StreamReconcile(t, fail) })
	}
}
func step09StreamReconcile(t *testing.T, failContinuation bool) {
	s, c, peer := step07StreamSession(t, 4, nil)
	defer peer.Close()
	defer s.Close()
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	blob := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: blob})
	state, _ := step03PreparedState(t, s, columns)
	changed := NewColumns(cqltype.ColumnSlice(columns)[0], ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "added", Type: blob})
	bound := func() *Bound[Bytes] {
		opaqueStorageValue := new(Bound[Bytes])
		implExecution.BoundData(opaqueStorageValue).InternalRequest = executionRequest{InternalStatement: state.InternalStatement, InternalPrepared: state, InternalValues: encodedValues{Columns: NewColumns()}, InternalOwner: newInputOwner(), InternalRouting: RoutingInfo{Kind: OrdinaryStatement}}
		implExecution.BoundData(opaqueStorageValue).InternalRows = OneColumn(BlobCodec)
		return opaqueStorageValue
	}()
	defer bound.Close()
	done := make(chan error, 1)
	go func() {
		for step := 0; step < 3; step++ {
			op, id, body, err := step03ReadRequest(peer)
			if err != nil {
				done <- err
				return
			}
			if step == 1 {
				if op != 9 {
					done <- fmt.Errorf("continuation preflight must PREPARE, got %d", op)
					return
				}
				_, err = peer.Write(typedTestResponse(t, 4, id, step03PreparedBody([]byte("changed"), NewColumns(), changed)))
			} else {
				if op != 10 {
					done <- fmt.Errorf("expected EXECUTE, got %d", op)
					return
				}
				if step == 0 {
					if bytes.Contains(body, []byte("next")) {
						done <- fmt.Errorf("first execution has continuation")
						return
					}
					_, err = peer.Write(step07Response(t, c, id, runtimeRowsBody([]byte("next"), "first")))
				} else {
					if !bytes.Contains(body, []byte("next")) {
						done <- fmt.Errorf("continuation was lost or first page replayed")
						return
					}
					if failContinuation {
						_, err = peer.Write(step03ErrorResponse(t, id, ErrCodeServer, nil))
						done <- err
						return
					}
					reply := binary.BigEndian.AppendUint32(nil, 2)
					reply = binary.BigEndian.AppendUint32(reply, 1)
					reply = binary.BigEndian.AppendUint32(reply, 2)
					reply = binary.BigEndian.AppendUint16(reply, 2)
					reply = append(reply, 'k', 's')
					reply = binary.BigEndian.AppendUint16(reply, 3)
					reply = append(reply, 't', 'b', 'l')
					for _, name := range []string{"value", "added"} {
						reply = binary.BigEndian.AppendUint16(reply, uint16(len(name)))
						reply = append(reply, name...)
						reply = binary.BigEndian.AppendUint16(reply, uint16(TypeBlob))
					}
					reply = binary.BigEndian.AppendUint32(reply, 1)
					reply = binary.BigEndian.AppendUint32(reply, 4)
					reply = append(reply, "last"...)
					reply = binary.BigEndian.AppendUint32(reply, 0xffffffff)
					_, err = peer.Write(step07Response(t, c, id, reply))
				}
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	resets := 0
	decoder := StreamRow(func(*[]byte) { resets++ }, StreamColumn("value", StreamBlob, func(v *[]byte) *[]byte { return v }))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Options: Options{Idempotent: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	dst := make([]byte, 0, 8)
	backing := &dst[:cap(dst)][0]
	if resets != 0 {
		t.Fatal("RunStream accessed caller destination")
	}
	if !cursor.NextInto(&dst) || string(dst) != "first" {
		t.Fatal(cursor.Err())
	}
	state.InternalDependencies.InternalTracker.InternalBump(preparedDependencyKey{InternalKind: 'b', InternalKeyspace: "ks", InternalName: "tbl"})
	if resets != 1 || string(dst) != "first" {
		t.Fatal("schema event accessed caller destination")
	}
	if failContinuation {
		if cursor.NextInto(&dst) {
			t.Fatal("failed continuation delivered output")
		}
		var server RequestError
		if !errors.As(cursor.Err(), &server) || server.GetCode() != ErrCodeServer {
			t.Fatal(cursor.Err())
		}
		info := cursor.Info()
		if info.DeliveredRows != 1 || info.Result.Attempts != 2 || resets != 1 || string(dst) != "first" {
			t.Fatal("continuation error replayed or accessed destination", info, resets, dst)
		}
		cursor.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		return
	}
	if !cursor.NextInto(&dst) || string(dst) != "last" || &dst[0] != backing {
		t.Fatal("next-page reconciliation", dst, cursor.Err())
	}
	if cursor.NextInto(&dst) || cursor.Close() != nil {
		t.Fatal(cursor.Err())
	}
	info := cursor.Info()
	if info.DeliveredRows != 2 || info.Pages != 2 || info.Result.Attempts != 2 || cursor.Columns().Len() != 2 || resets != 2 {
		t.Fatal("replayed rows/pages or failed shape change", info, cursor.Columns())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
