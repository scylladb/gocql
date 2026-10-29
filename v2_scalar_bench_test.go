// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

var scalarIntSink int64
var scalarBytesSink Bytes
var scalarTextSink Text
var scalarVarintSink Varint

func BenchmarkV2ScalarInt64Decode(b *testing.B) {
	data := []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	r := scalarReader(b, TypeBigInt, Present, data[:3], data[3:])
	b.ReportAllocs()
	b.SetBytes(8)
	b.ResetTimer()
	for b.Loop() {
		v, err := Int64.Decode(r)
		if err != nil {
			b.Fatal(err)
		}
		scalarIntSink = v
	}
}
func BenchmarkV2ScalarBlobDecode(b *testing.B) {
	data := make([]byte, 1<<20)
	r := scalarReader(b, TypeBlob, Present, data[:123], data[123:])
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		v, err := BlobCodec.Decode(r)
		if err != nil {
			b.Fatal(err)
		}
		scalarBytesSink = v
	}
}
func BenchmarkV2ScalarTextDecode(b *testing.B) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = 'a'
	}
	r := scalarReader(b, TypeText, Present, data[:123], data[123:])
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		v, err := TextCodec.Decode(r)
		if err != nil {
			b.Fatal(err)
		}
		scalarTextSink = v
	}
}
func BenchmarkV2ScalarVarintDecode(b *testing.B) {
	data := make([]byte, 1<<20)
	data[0] = 0x01
	r := scalarReader(b, TypeVarint, Present, data[:123], data[123:])
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		v, err := VarintCodec.Decode(r)
		if err != nil {
			b.Fatal(err)
		}
		scalarVarintSink = v
	}
}
func BenchmarkV2ScalarBlobEncodeBorrowed(b *testing.B) {
	data := make([]byte, 1<<20)
	typ := scalarType(b, TypeBlob)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		builder := wire.NewBuilder(16)
		w := newValueWriter(builder, typ, true)
		if err := BytesEncoder.Encode(w, data); err != nil {
			b.Fatal(err)
		}
		if err := finishValueWriter(w); err != nil {
			b.Fatal(err)
		}
		if _, err := builder.Freeze(); err != nil {
			b.Fatal(err)
		}
	}
}
