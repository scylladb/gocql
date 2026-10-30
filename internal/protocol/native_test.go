// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"encoding/binary"
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
	"testing"
)

func protocolSettings(protocol byte) Settings {
	return Settings{Protocol: protocol, MaxFrameBytes: frm.MaxFrameSize, MaxDecompressedBytes: frm.MaxFrameSize, MaxMetadataDepth: 64}
}
func typedTestChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, p := range parts {
		spans[i] = wire.Borrow(p)
	}
	c, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func typedTestBytes(t *testing.T, c wire.Chain) []byte {
	t.Helper()
	p := make([]byte, c.Len())
	if _, err := c.CopyTo(p); err != nil {
		t.Fatal(err)
	}
	return p
}

func typedTestRowsBody(value []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, 2)
	out = binary.BigEndian.AppendUint32(out, 1) // global table
	out = binary.BigEndian.AppendUint32(out, 1)
	for _, s := range []string{"ks", "tbl", "value"} {
		out = binary.BigEndian.AppendUint16(out, uint16(len(s)))
		out = append(out, s...)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(TypeBlob))
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}

func TestTypedMetadataAtEverySpanSplit(t *testing.T) {
	value := []byte("fragmented row bytes")
	body := typedTestRowsBody(value)
	for split := 0; split <= len(body); split++ {
		chain := typedTestChain(t, body[:split], body[split:])
		owner := wire.NewLease(chain, nil)
		c := protocolSettings(5)
		page, err := c.ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, chain, owner)
		if err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
		if page.RowCount != 1 || page.Columns.Len() != 1 {
			t.Fatalf("split %d: wrong metadata", split)
		}
		cursor := wire.NewCursor(page.Rows)
		n, _ := cursor.Uint32()
		cell, err := cursor.Take(int64(n))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(typedTestBytes(t, cell), value) {
			t.Fatal("row changed")
		}
		offset := len(body) - len(value)
		_ = cell.Each(func(s wire.Span) error {
			b := s.Bytes()
			if &b[0] != &body[offset] {
				t.Fatal("parser copied a cell")
			}
			offset += len(b)
			return nil
		})
		owner.Release()
	}
	for n := 0; n < len(body)-len(value); n++ {
		chain := typedTestChain(t, body[:n])
		owner := wire.NewLease(chain, nil)
		_, err := (protocolSettings(5)).ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, chain, owner)
		owner.Release()
		if err == nil {
			t.Fatalf("accepted truncated metadata at %d", n)
		}
	}
}

func TestTypedMetadataIDsAndPagingOrder(t *testing.T) {
	e := NewTypedEnvelope()
	e.U32(frm.ResultKindRows)
	e.U32(uint32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagMetaDataChanged))
	e.U32(1)
	e.Bytes([]byte("page-state"), false)
	e.Bytes([]byte("new-metadata-id"), true)
	e.Str("ks", false)
	e.Str("tbl", false)
	e.Str("v", false)
	e.U16(uint16(TypeInt))
	e.U32(1)
	e.U32(4)
	e.U32(19)
	body, err := e.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	bytesBody := typedTestBytes(t, body)
	var parts [][]byte
	for i := range bytesBody {
		parts = append(parts, bytesBody[i:i+1])
	}
	body = typedTestChain(t, parts...)
	owner := wire.NewLease(body, nil)
	defer owner.Release()
	page, err := (protocolSettings(5)).ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, body, owner)
	if err != nil {
		t.Fatal(err)
	}
	if string(page.PagingState) != "page-state" || string(page.MetadataID) != "new-metadata-id" {
		t.Fatal("paging state and metadata ID were swapped")
	}
	if page.ColumnCount != 1 || page.NoMetadata {
		t.Fatal("wrong metadata flags")
	}
}

func TestTypedEmptyContinuationIsPresent(t *testing.T) {
	c := protocolSettings(4)

	frame, err := c.BuildTypedRequest(&model.AttemptRequest{Statement: "q", Consistency: One, PagingState: []byte{}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	packet := typedTestBytes(t, frame)
	if packet[16]&byte(frm.FlagWithPagingState) == 0 || len(packet) != 21 {
		t.Fatalf("empty continuation omitted: %x", packet)
	}
	e := NewTypedEnvelope()
	e.U32(frm.ResultKindRows)
	e.U32(uint32(frm.FlagHasMorePages))
	e.U32(0)
	e.U32(0)
	e.U32(0)
	body, _ := e.Freeze()
	owner := wire.NewLease(body, nil)
	defer owner.Release()
	page, err := c.ParseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, body, owner)
	if err != nil {
		t.Fatal(err)
	}
	if page.PagingState == nil || len(page.PagingState) != 0 {
		t.Fatal("present empty continuation collapsed into completion")
	}
}

func TestTypedRejectsChangedMetadataWithoutColumns(t *testing.T) {
	e := NewTypedEnvelope()
	e.U32(frm.ResultKindRows)
	e.U32(uint32(frm.FlagNoMetaData | frm.FlagMetaDataChanged))
	e.U32(1)
	e.Bytes(nil, true)
	e.U32(0)
	body, _ := e.Freeze()
	owner := wire.NewLease(body, nil)
	defer owner.Release()
	if _, err := (protocolSettings(5)).ParseTypedResponse(frm.FrameHeader{Version: 0x85, Op: frm.OpResult}, body, owner); err == nil {
		t.Fatal("accepted changed metadata without column specifications")
	}
}
