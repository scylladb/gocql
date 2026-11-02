// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func reuseMetadataBody(name string, kind uint16) []byte {
	b := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	b = binary.BigEndian.AppendUint32(b, uint32(frm.FlagGlobalTableSpec))
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, s := range []string{"ks", "table", name} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
		b = append(b, s...)
	}
	b = binary.BigEndian.AppendUint16(b, kind)
	return binary.BigEndian.AppendUint32(b, 0)
}

func TestMetadataReuseIdentityAndChangedValidation(t *testing.T) {
	settings := protocolSettings(4)
	settings.MetadataReuse = new(MetadataReuse)
	parse := func(data []byte) (cqltype.Columns, error) {
		// Fragment every byte to exercise comparison across span boundaries.
		spans := make([]wire.Span, len(data))
		for i := range data {
			spans[i] = wire.Borrow(data[i : i+1])
		}
		chain, _ := wire.NewChain(spans...)
		lease := wire.NewLease(chain, nil)
		defer lease.Release()
		page, err := settings.ParseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, chain, lease)
		if page == nil {
			return cqltype.Columns{}, err
		}
		return page.Columns, err
	}
	original := reuseMetadataBody("value", uint16(cqltype.TypeBlob))
	first, err := parse(original)
	if err != nil {
		t.Fatal(err)
	}
	second, err := parse(bytes.Clone(original))
	if err != nil {
		t.Fatal(err)
	}
	if &cqltype.ColumnSlice(first)[0] != &cqltype.ColumnSlice(second)[0] {
		t.Fatal("matching validated metadata was reallocated")
	}
	changed, err := parse(reuseMetadataBody("other", uint16(cqltype.TypeInt)))
	if err != nil {
		t.Fatal(err)
	}
	if changed.Equal(first) {
		t.Fatal("changed metadata reused the old shape")
	}
	if _, err := parse(reuseMetadataBody("other", 0xffff)); err == nil {
		t.Fatal("invalid changed type bypassed validation")
	}
	for n := 0; n < len(original); n++ {
		if _, err := parse(original[:n]); err == nil {
			t.Fatalf("truncated metadata at %d accepted", n)
		}
	}
}
