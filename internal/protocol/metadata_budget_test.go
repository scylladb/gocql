// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/scylladb/gocql/v2/internal/wire"
	"testing"
)

type countedMetadataSource struct {
	*bytes.Reader
	consumed int
}

func (s *countedMetadataSource) Read(p []byte) (int, error) {
	n, e := s.Reader.Read(p)
	s.consumed += n
	return n, e
}

// The incremental parser must charge its column descriptors before reading any
// column strings or types, including when its cursor is supplied by transport.
func TestMetadataBudgetRejectsBeforeReadingColumnPayload(t *testing.T) {
	body := binary.BigEndian.AppendUint32(nil, 0)
	body = binary.BigEndian.AppendUint32(body, 1)
	body = append(body, make([]byte, 16)...)
	source := &countedMetadataSource{Reader: bytes.NewReader(body)}
	scope, err := wire.NewStream(context.Background(), source, int64(len(body)), wire.StreamLimits{})
	if err != nil {
		t.Fatal(err)
	}
	budget := &StreamMetadataBudget{Limit: 95}
	cursor := &StreamMetadataCursor{Scope: scope, Budget: budget}
	parser := &TypedParser{Cursor: cursor, Proto: 4, MaxDepth: 64}
	var metadata ParsedMetadata
	parser.MetadataFields(&metadata, false, 0)
	if !errors.Is(parser.Err, wire.ErrLimit) || source.consumed != 8 || budget.Bytes() != 0 {
		t.Fatalf("budget boundary: err=%v consumed=%d live=%d", parser.Err, source.consumed, budget.Bytes())
	}
	cursor.Release()
	if budget.Bytes() != 0 {
		t.Fatal("failed metadata retained a charge")
	}
}
