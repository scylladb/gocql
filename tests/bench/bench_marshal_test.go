/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bench_test

import (
	"bytes"
	"fmt"
	"github.com/scylladb/gocql/v2"
	"testing"
)

// benchmarkValue measures explicit borrowed encoding and borrowed decoding.
// Owned conversions have separate names so their copy costs remain visible.
func benchmarkValue[P, R any](b *testing.B, typ gocql.DataType, enc gocql.Encoder[P], dec gocql.Decoder[R], value P) {
	encoded, err := gocql.EncodeValueBorrowed(typ, enc, value)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("EncodeBorrowed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := gocql.EncodeValueBorrowed(typ, enc, value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EncodeOwned", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := gocql.EncodeValueOwned(typ, enc, value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("DecodeBorrowed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := gocql.DecodeValue(dec, encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
func scalar(kind gocql.Type) gocql.DataType {
	t, err := gocql.ScalarType(kind)
	if err != nil {
		panic(err)
	}
	return t
}
func BenchmarkSerialization(b *testing.B) {
	b.Run("Int", func(b *testing.B) { benchmarkValue(b, scalar(gocql.TypeInt), gocql.Int32, gocql.Int32, int32(42)) })
	for _, size := range []int{100, 1024, 1024 * 1024} {
		body := bytes.Repeat([]byte("x"), size)
		b.Run(fmt.Sprintf("Blob/%d", size), func(b *testing.B) {
			benchmarkValue(b, scalar(gocql.TypeBlob), gocql.BlobCodec, gocql.BlobCodec, gocql.BorrowBytes(body))
		})
		b.Run(fmt.Sprintf("Text/%d", size), func(b *testing.B) {
			benchmarkValue(b, scalar(gocql.TypeText), gocql.StringEncoder, gocql.TextCodec, string(body))
		})
	}
	b.Run("UUID", func(b *testing.B) {
		benchmarkValue(b, scalar(gocql.TypeUUID), gocql.UUIDCodec, gocql.UUIDCodec, gocql.UUID{})
	})
	b.Run("Duration", func(b *testing.B) {
		benchmarkValue(b, scalar(gocql.TypeDuration), gocql.DurationCodec, gocql.DurationCodec, gocql.Duration{Nanoseconds: 300000000000})
	})
	b.Run("Timestamp", func(b *testing.B) {
		benchmarkValue(b, scalar(gocql.TypeTimestamp), gocql.TimestampCodec, gocql.TimestampCodec, gocql.Timestamp(1700000000000))
	})
	text, integer := scalar(gocql.TypeText), scalar(gocql.TypeInt)
	list, _ := gocql.ListType(text)
	set, _ := gocql.SetType(text)
	mapping, _ := gocql.MapType(text, integer)
	b.Run("List", func(b *testing.B) {
		benchmarkValue(b, list, gocql.ListEncoder(gocql.StringEncoder), gocql.ListDecoder(gocql.TextCodec), []string{"foo", "bar", "baz"})
	})
	b.Run("Set", func(b *testing.B) {
		benchmarkValue(b, set, gocql.SetEncoder(gocql.StringEncoder), gocql.SetDecoder(gocql.TextCodec), []string{"foo", "bar", "baz"})
	})
	b.Run("Map", func(b *testing.B) {
		benchmarkValue(b, mapping, gocql.MapEncoder(gocql.StringEncoder, gocql.Int32), gocql.MapDecoder(gocql.TextCodec, gocql.Int32), []gocql.Entry[string, int32]{{Key: "a", Value: 1}, {Key: "b", Value: 2}})
	})
	type record struct {
		ID    gocql.UUID
		Name  string
		Value int32
	}
	type view struct {
		ID    gocql.UUID
		Name  gocql.Text
		Value int32
	}
	params := gocql.Params(gocql.ParamAt(0, gocql.UUIDCodec, func(v record) gocql.UUID { return v.ID }), gocql.ParamAt(1, gocql.StringEncoder, func(v record) string { return v.Name }), gocql.ParamAt(2, gocql.Int32, func(v record) int32 { return v.Value }))
	rows := gocql.Row(gocql.ColumnAt(0, gocql.UUIDCodec, func(v *view) *gocql.UUID { return &v.ID }), gocql.ColumnAt(1, gocql.TextCodec, func(v *view) *gocql.Text { return &v.Name }), gocql.ColumnAt(2, gocql.Int32, func(v *view) *int32 { return &v.Value }))
	tuple, _ := gocql.TupleType(scalar(gocql.TypeUUID), text, integer)
	b.Run("Tuple", func(b *testing.B) {
		benchmarkValue(b, tuple, gocql.TupleEncoder(params), gocql.TupleDecoder(rows), record{Name: "test tuple", Value: 123})
	})
	udt, _ := gocql.UDTType("myks", "myudt", gocql.ColumnSpec{Name: "id", Type: scalar(gocql.TypeUUID)}, gocql.ColumnSpec{Name: "name", Type: text}, gocql.ColumnSpec{Name: "value", Type: integer})
	opts := gocql.UDTOptions{Keyspace: "myks", Name: "myudt"}
	udtEnc := gocql.UDTEncoder(params, opts)
	udtDec := gocql.UDTDecoder(rows, opts)
	b.Run("UDT", func(b *testing.B) { benchmarkValue(b, udt, udtEnc, udtDec, record{Name: "test udt", Value: 123}) })
	nestedMap, _ := gocql.MapType(text, udt)
	nested, _ := gocql.ListType(nestedMap)
	nestedValue := [][]gocql.Entry[string, record]{{{Key: "key1", Value: record{Name: "name1", Value: 123}}, {Key: "key2", Value: record{Name: "name2", Value: 456}}}, {{Key: "key3", Value: record{Name: "name3", Value: 789}}}}
	b.Run("Nested/3-lvl", func(b *testing.B) {
		benchmarkValue(b, nested, gocql.ListEncoder(gocql.MapEncoder(gocql.StringEncoder, udtEnc)), gocql.ListDecoder(gocql.MapDecoder(gocql.TextCodec, udtDec)), nestedValue)
	})
}
