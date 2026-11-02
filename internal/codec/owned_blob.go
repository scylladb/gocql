// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// OwnedBlobCodec decodes into reusable caller-owned storage. With RowInto, a
// seeded slice is reused whenever its capacity accommodates the complete cell.
// The resulting bytes remain valid after the response page is released.
var OwnedBlobCodec Codec[[]byte] = ownedBlobCodec{}

type ownedBlobCodec struct{}

func (ownedBlobCodec) CheckType(t cqltype.DataType) error {
	return checkScalarType(t, cqltype.TypeBlob, 0)
}
func (ownedBlobCodec) Encode(w *ValueWriter, value []byte) error {
	return BytesEncoder.Encode(w, value)
}
func (c ownedBlobCodec) Decode(r ValueReader) ([]byte, error) {
	var dst []byte
	err := c.DecodeInto(r, &dst)
	return dst, err
}
func (c ownedBlobCodec) DecodeInto(r ValueReader, dst *[]byte) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil blob destination")
	}
	if err := c.CheckType(r.Type()); err != nil {
		return err
	}
	if err := requirePresent(r); err != nil {
		return err
	}
	n := int(r.body.Len())
	if cap(*dst) < n {
		*dst = make([]byte, n)
	} else {
		*dst = (*dst)[:n]
	}
	_, err := r.body.CopyTo(*dst)
	return err
}
func (ownedBlobCodec) decodeColumnInto(r *RowReader, dst *[]byte) error {
	n, err := r.scalarColumn(cqltype.TypeBlob, 0, true, 0)
	if err != nil {
		return err
	}
	if cap(*dst) < int(n) {
		*dst = make([]byte, int(n))
	} else {
		*dst = (*dst)[:int(n)]
	}
	_, err = r.cursor.Read(*dst)
	return err
}
func (ownedBlobCodec) Clone(value []byte) []byte { return append([]byte(nil), value...) }
