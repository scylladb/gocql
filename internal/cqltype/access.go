// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import "github.com/scylladb/gocql/v2/internal/wire"

// These accessors are internal contracts for codecs and native metadata readers.
// Constructors take ownership of descriptor slices; consumers never mutate them.
func DescriptorOf(t DataType) *DataTypeDescriptor { return t.descriptor }
func TypeFromDescriptor(d *DataTypeDescriptor) DataType {
	return DataType{descriptor: d}
}
func ColumnSlice(c Columns) []ColumnSpec               { return c.columns }
func ColumnsOwned(c []ColumnSpec) Columns              { return Columns{columns: c} }
func BytesChain(b Bytes) wire.Chain                    { return b.chain }
func BytesLease(b Bytes) *wire.Lease                   { return b.lease }
func BytesView(c wire.Chain, l *wire.Lease) Bytes      { return Bytes{chain: c, lease: l} }
func TextView(b Bytes) Text                            { return Text{bytes: b} }
func TextBytes(t Text) Bytes                           { return t.bytes }
func NativeVersion(n NativeType) byte                  { return n.Version() }
func NativeKind(n NativeType) Type                     { return n.Type() }
func NativeCustom(n NativeType) string                 { return n.Custom() }
func CheckBytesAlive(b Bytes) error                    { return b.checkAlive() }
func EachBytes(b Bytes, f func(wire.Span) error) error { return b.each(f) }
func Fingerprint(t DataType) string                    { return t.fingerprint() }
