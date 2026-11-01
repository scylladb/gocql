// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/segment"
)

func TestTypedRequestHeaderPreservesBodyLimitAndErrors(t *testing.T) {
	for _, version := range []byte{3, 4, 5} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			settings := protocolSettings(version)
			// QUERY contains a four-byte string length, one CQL byte, two-byte
			// consistency and one-byte flags (four bytes for native v5).
			bodySize := int64(8)
			if version == 5 {
				bodySize += 3
			}
			settings.MaxFrameBytes = bodySize
			request := &model.AttemptRequest{Statement: "q", Consistency: cqltype.One, Trace: true}
			chain, err := settings.BuildTypedRequest(request, 23)
			if err != nil {
				t.Fatalf("body exactly at limit: %v", err)
			}
			native := typedTestBytes(t, chain)
			if version == 5 {
				reader := bytes.NewReader(native)
				header, err := segment.ReadHeader(reader, false)
				if err != nil {
					t.Fatal(err)
				}
				owner, err := segment.ReadOwnedPayload(reader, header, nil)
				if err != nil {
					t.Fatal(err)
				}
				native = typedTestBytes(t, owner.Chain())
				owner.Release()
			}
			header, err := frm.DecodeHeader(native)
			if err != nil || header.Length != int(bodySize) || len(native) != HeadSize+int(bodySize) || header.Stream != 23 || header.Op != frm.OpQuery || header.Flags != frm.FlagTracing {
				t.Fatalf("native header = %+v, bytes=%d, error=%v", header, len(native), err)
			}
			settings.MaxFrameBytes--
			if _, err := settings.BuildTypedRequest(request, 23); !errors.Is(err, ErrFrameTooBig) {
				t.Fatalf("body above limit: %v", err)
			}
			// An earlier encoding error takes precedence over the later size
			// check, including when the rest of the request exceeds the limit.
			request.PreparedID = make([]byte, 1<<16)
			request.PagingState = make([]byte, 32)
			if _, err := settings.BuildTypedRequest(request, 23); err == nil || !strings.Contains(err.Error(), "oversized short bytes") {
				t.Fatalf("sticky encoding error lost: %v", err)
			}
		})
	}
}
