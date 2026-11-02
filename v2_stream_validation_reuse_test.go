// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type pageValidationDecoder struct{ compiles, resets, decodes int }
type pageValidationPlan struct {
	decoder *pageValidationDecoder
	plan    StreamRowPlan[[]byte]
}

func (d *pageValidationDecoder) Compile(columns Columns) (StreamRowPlan[[]byte], error) {
	d.compiles++
	plan, err := StreamOneColumn(StreamBlob).Compile(columns)
	return pageValidationPlan{decoder: d, plan: plan}, err
}
func (p pageValidationPlan) Reset(dst *[]byte) {
	p.decoder.resets++
	p.plan.Reset(dst)
}
func (p pageValidationPlan) DecodeInto(reader *StreamRowReader, dst *[]byte) error {
	p.decoder.decodes++
	return p.plan.DecodeInto(reader, dst)
}

func TestStreamContinuationValidationMetadataAndCallbacks(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, _ := step03PreparedState(t, s, columns)
	bound := new(Bound[Bytes])
	implExecution.BoundData(bound).InternalRequest = executionRequest{InternalStatement: state.InternalStatement, InternalPrepared: state, InternalValues: encodedValues{Columns: NewColumns()}, InternalOwner: newInputOwner(), InternalRouting: RoutingInfo{Kind: OrdinaryStatement}}
	implExecution.BoundData(bound).InternalRows = OneColumn(BlobCodec)
	defer bound.Close()
	// The first page is empty. The next omits metadata, then an equal full
	// metadata page is canonicalized, then a renamed column changes the plan.
	noMetadata := binary.BigEndian.AppendUint32(nil, 2)
	noMetadata = binary.BigEndian.AppendUint32(noMetadata, 4|2)
	noMetadata = binary.BigEndian.AppendUint32(noMetadata, 1)
	noMetadata = binary.BigEndian.AppendUint32(noMetadata, 1)
	noMetadata = append(noMetadata, 1)
	noMetadata = binary.BigEndian.AppendUint32(noMetadata, 1)
	noMetadata = binary.BigEndian.AppendUint32(noMetadata, 3)
	noMetadata = append(noMetadata, "abc"...)
	changed := bytes.Replace(runtimeRowsBody(nil, "ghi"), []byte("value"), []byte("other"), 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, body := range [][]byte{runtimeRowsBody([]byte{}), noMetadata, runtimeRowsBody([]byte{2}, "def"), changed} {
			id := typedTestReadRequest(t, c, peer)
			if _, err := peer.Write(step07Response(t, c, id, body)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	decoder := new(pageValidationDecoder)
	cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Incremental: true, Limits: StreamLimits{MaxMetadataBytes: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if decoder.compiles != 1 || decoder.resets != 0 || decoder.decodes != 0 {
		t.Fatal("empty page invoked application callbacks", decoder)
	}
	dst := make([]byte, 0, 3)
	for index, want := range []string{"abc", "def", "ghi"} {
		if !cursor.NextInto(&dst) || string(dst) != want {
			t.Fatal("continuation decode", index, string(dst), cursor.Err())
		}
		if decoder.resets != index+1 || decoder.decodes != index+1 {
			t.Fatal("application row callbacks changed", decoder)
		}
		if decoder.compiles != 1+index/2 {
			t.Fatal("metadata compile policy changed", decoder)
		}
		if budget := c.InternalStreamBudget.Bytes(); budget <= 0 || budget > 1024 {
			t.Fatal("live page metadata lost budget ownership", budget)
		}
	}
	if cursor.NextInto(&dst) || cursor.Err() != nil {
		t.Fatal(cursor.Err())
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if decoder.compiles != 2 || decoder.resets != 3 || decoder.decodes != 3 || cursor.Info().Pages != 4 {
		t.Fatal(decoder, cursor.Info())
	}
	if budget := c.InternalStreamBudget.Bytes(); budget != 0 {
		t.Fatal("released page metadata retained its budget", budget)
	}
	if string(dst) != "ghi" || cursor.Columns().Len() != 1 {
		t.Fatal("closed stream lost caller output or metadata")
	}
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	<-done
}

func TestStreamValidationSurvivesPriorResponseRelease(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	data := binary.BigEndian.AppendUint32(nil, 3)
	data = append(data, "abc"...)
	budget := &protocol.StreamMetadataBudget{Limit: 200}
	response := func() *streamResponse {
		if err := budget.Reserve(100); err != nil {
			t.Fatal(err)
		}
		scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{})
		if err != nil {
			t.Fatal(err)
		}
		return &streamResponse{InternalBody: scope, InternalColumnCount: 1, InternalBudget: &protocol.StreamMetadataCursor{Budget: budget, Used: 100}}
	}
	prior := response()
	if err := prior.InternalInstallColumns(columns); err != nil {
		t.Fatal(err)
	}
	next := response()
	if err := next.InternalInstallColumnsReusing(columns, prior); err != nil {
		t.Fatal(err)
	}
	if budget.Bytes() != 200 {
		t.Fatal("simultaneous metadata owners lost charges", budget.Bytes())
	}
	prior.InternalReleaseMetadata()
	if prior.InternalReader != nil || budget.Bytes() != 100 {
		t.Fatal("old reader/budget not released", budget.Bytes())
	}
	plan := codecTestMust(StreamOneColumn(StreamBlob).Compile(columns))
	dst := make([]byte, 0, 3)
	plan.Reset(&dst)
	if err := plan.DecodeInto(next.InternalReader, &dst); err != nil || string(dst) != "abc" {
		t.Fatal("new reader depended on released prior owner", string(dst), err)
	}
	if err := next.InternalReader.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	next.InternalReleaseMetadata()
	if budget.Bytes() != 0 || string(dst) != "abc" {
		t.Fatal("metadata charge leaked or caller output invalidated", budget.Bytes())
	}
}
