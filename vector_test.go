//go:build integration
// +build integration

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

import (
	"fmt"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"
	"math/big"
	"net/netip"
	"reflect"
	"sort"
	"testing"
	"time"
)

type person struct {
	FirstName, LastName string
	Age                 int32
}

func (p person) String() string {
	return fmt.Sprintf("Person{firstName: %s, lastName: %s, Age: %d}", p.FirstName, p.LastName, p.Age)
}
func serializationVectorGate(t *testing.T, allTypes bool) {
	t.Helper()
	if *flagDistribution == "cassandra" && flagCassVersion.Before(5, 0, 0) {
		t.Skip("Vector types have been introduced in Cassandra 5.0")
	}
	minor := 3
	if allTypes {
		minor = 4
	}
	if *flagDistribution == "scylla" && flagCassVersion.Before(2025, minor, 0) {
		t.Skipf("Vector types require ScyllaDB 2025.%d", minor)
	}
}

var serializationBlob = NamedScalar(BlobCodec, func(v []byte) Bytes { return BorrowBytes(v) }, func(v Bytes) []byte { return v.OwnedBytes() })
var serializationTimestamp = NamedScalar(TimestampCodec, func(v time.Time) Timestamp {
	n, e := TimestampFromTime(v)
	if e != nil {
		panic(e)
	}
	return n
}, func(v Timestamp) time.Time {
	n, e := v.Time()
	if e != nil {
		panic(e)
	}
	return n.UTC()
})
var serializationDate = NamedScalar(DateCodec, func(v time.Time) Date {
	n, e := DateFromTime(v)
	if e != nil {
		panic(e)
	}
	return n
}, func(v Date) time.Time {
	n, e := v.Time()
	if e != nil {
		panic(e)
	}
	return n.UTC()
})
var serializationDecimal = NamedScalar(DecimalCodec, func(v inf.Dec) Decimal { return Decimal{int32(v.Scale()), VarintFromBigInt(v.UnscaledBig())} }, func(v Decimal) inf.Dec { return *inf.NewDecBig(v.Unscaled.BigInt(), inf.Scale(v.Scale)) })

func TestVector_Marshaler(t *testing.T) {
	t.Parallel()
	serializationVectorGate(t, false)
	s := createV2Session(t)
	defer s.Close()
	fixed, variable := serializationTableName(t, "fixed"), serializationTableName(t, "variable")
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<float,3>)", fixed))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<text,4>)", variable))
	floats := []float32{8, 2.5, -5}
	require.Equal(t, floats, serializationRoundTrip(t, s, fixed, "vec", 1, serializationVector(Float32), floats))
	texts := []string{"apache", "cassandra", tests.RandomText(500), "gocql"}
	require.Equal(t, texts, serializationRoundTrip(t, s, variable, "vec", 1, serializationVector(serializationString), texts))
}

type serializationVectorCase struct {
	name, cql string
	run       func(*testing.T, *Session, string, string)
}

func vectorCase[T any](name, cql string, codec Codec[T], input []T, compare func(*testing.T, []T, []T)) serializationVectorCase {
	return serializationVectorCase{name, cql, func(t *testing.T, s *Session, table, column string) {
		got := serializationRoundTrip(t, s, table, column, 1, serializationVector(codec), input)
		if compare != nil {
			compare(t, input, got)
		} else if !reflect.DeepEqual(input, got) {
			t.Fatalf("expected %#v, got %#v", input, got)
		}
	}}
}
func TestVector_Types(t *testing.T) {
	t.Parallel()
	serializationVectorGate(t, true)
	s := createV2Session(t)
	defer s.Close()
	parse := func(layout, value string) time.Time {
		v, e := time.Parse(layout, value)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	timestamps := []time.Time{parse("2006-01-02", "2000-01-01"), parse("2006-01-02 15:04:05", "2024-01-01 10:31:45"), parse("2006-01-02 15:04:05.000", "2024-05-01 10:31:45.987")}
	dates := []time.Time{parse("2006-01-02", "2000-01-01"), parse("2006-01-02", "2022-03-14"), parse("2006-01-02", "2024-12-31")}
	times := []time.Time{parse("15:04:05", "01:00:00"), parse("15:04:05", "15:23:59"), parse("15:04:05.000", "10:31:45.987")}
	// The historical time subcase stores timestamp values anchored at year zero.
	// Preserve that wire coverage alongside the scalar time fixtures above.
	ascii := NamedScalar(ASCIICodec, func(s string) Text { return codecTestMust(BorrowText(s)) }, func(t Text) string { return t.String() })
	uintVarint := NamedScalar(VarintCodec, func(n uint64) Varint { return VarintFromBigInt(new(big.Int).SetUint64(n)) }, func(n Varint) uint64 { return n.BigInt().Uint64() })
	type triple struct {
		Text  string
		Int   int32
		Float float32
	}
	tripleCodec := serializationTuple(Params(ParamAt(0, serializationString, func(v triple) string { return v.Text }), ParamAt(1, Int32, func(v triple) int32 { return v.Int }), ParamAt(2, Float32, func(v triple) float32 { return v.Float })), Row(ColumnAt(0, serializationString, func(v *triple) *string { return &v.Text }), ColumnAt(1, Int32, func(v *triple) *int32 { return &v.Int }), ColumnAt(2, Float32, func(v *triple) *float32 { return &v.Float })))
	type textList struct {
		Text string
		List []string
	}
	textListCodec := serializationTuple(Params(ParamAt(0, serializationString, func(v textList) string { return v.Text }), ParamAt(1, serializationList(serializationString), func(v textList) []string { return v.List })), Row(ColumnAt(0, serializationString, func(v *textList) *string { return &v.Text }), ColumnAt(1, serializationList(serializationString), func(v *textList) *[]string { return &v.List })))
	cases := []serializationVectorCase{
		vectorCase("ascii", "ascii", ascii, []string{"a", "1", "Z"}, nil),
		vectorCase("bigint", "bigint", Int64, []int64{1, 2, 3}, nil),
		vectorCase("blob", "blob", serializationBlob, [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9}}, nil),
		vectorCase("boolean", "boolean", Bool, []bool{true, false, true}, nil),
		vectorCase("counter", "counter", CounterCodec, []Counter{5, 6, 7}, nil),
		vectorCase("decimal", "decimal", serializationDecimal, []inf.Dec{*inf.NewDec(1, 0), *inf.NewDec(2, 1), *inf.NewDec(-3, 2)}, nil),
		vectorCase("double", "double", Float64, []float64{0.1, -1.2, 3}, nil),
		vectorCase("float", "float", Float32, []float32{0.1, -1.2, 3}, nil),
		vectorCase("int", "int", Int32, []int32{1, 2, 3}, nil),
		vectorCase("text", "text", serializationString, []string{"a", "b", "c"}, nil),
		vectorCase("timestamp", "timestamp", serializationTimestamp, timestamps, nil),
		vectorCase("uuid", "uuid", UUIDCodec, []UUID{MustRandomUUID(), MustRandomUUID(), MustRandomUUID()}, nil),
		vectorCase("varchar", "varchar", serializationString, []string{"abc", "def", "ghi"}, nil),
		vectorCase("varint", "varint", uintVarint, []uint64{1234, 123498765, 18446744073709551615}, nil),
		vectorCase("timeuuid", "timeuuid", TimeUUIDCodec, []TimeUUID{TimeUUID(NewTimeUUID()), TimeUUID(NewTimeUUID()), TimeUUID(NewTimeUUID())}, nil),
		vectorCase("inet", "inet", InetCodec, []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("192.168.1.1"), netip.MustParseAddr("8.8.8.8")}, nil),
		vectorCase("date", "date", serializationDate, dates, nil),
		vectorCase("time", "timestamp", serializationTimestamp, times, nil),
		vectorCase("smallint", "smallint", Int16, []int16{127, 256, -1234}, nil),
		vectorCase("tinyint", "tinyint", Int8, []int8{127, 9, -123}, nil),
		vectorCase("duration", "duration", DurationCodec, []Duration{{0, 1, 1920000000000}, {1, 1, 1920000000000}, {31, 0, 60000000000}}, nil),
		vectorCase("vector_vector_float", "vector<float,5>", serializationVector(Float32), [][]float32{{0.1, -1.2, 3, 5, 5}, {10.1, -122222.0002, 35, 1, 1}, {0, 0, 0, 0, 0}}, nil),
		vectorCase("vector_vector_set_float", "vector<set<float>,5>", serializationVector(serializationSet(Float32)), [][][]float32{{{1, 2}, {2, -1}, {3}, {0}, {-1.3}}, {{2, 3}, {2, -1}, {3}, {0}, {-1.3}}, {{1, 1000}, {0}, {}, {12, 14, 15, 16}, {-1.3}}}, func(t *testing.T, want, got [][][]float32) {
			require.Len(t, got, len(want))
			for i := range want {
				require.Len(t, got[i], len(want[i]))
				for j := range want[i] {
					a, b := append([]float32(nil), want[i][j]...), append([]float32(nil), got[i][j]...)
					sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
					sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
					require.Equal(t, a, b)
				}
			}
		}),
		vectorCase("vector_tuple_text_int_float", "tuple<text,int,float>", tripleCodec, []triple{{"a", 1, 0.5}, {"b", 2, -1.2}, {"c", 3, 0}}, nil),
		vectorCase("vector_tuple_text_list_text", "tuple<text,list<text>>", textListCodec, []textList{{"a", []string{"b", "c"}}, {"d", []string{"e", "g", "f"}}, {"h", []string{"i"}}}, nil),
		vectorCase("vector_set_text", "set<text>", serializationSet(serializationString), [][]string{{"a", "b"}, {"c", "d"}, {"f", "e"}}, func(t *testing.T, want, got [][]string) {
			require.Len(t, got, len(want))
			for i := range want {
				a, b := append([]string(nil), want[i]...), append([]string(nil), got[i]...)
				sort.Strings(a)
				sort.Strings(b)
				require.Equal(t, a, b)
			}
		}),
		vectorCase("vector_list_int", "list<int>", serializationList(Int32), [][]int32{{1, 2, 3}, {-1, -2, -3}, {0, 0, 0}}, nil),
		vectorCase("vector_map_text_int", "map<text,int>", serializationMap(serializationString, Int32), []map[string]int32{{"a": 1, "b": 2, "c": 3}, {"abc": 123}, {}}, nil),
	}
	table := serializationTableName(t, "all_types")
	cols := ""
	for _, tc := range cases {
		cols += fmt.Sprintf(",vec_%s vector<%s,3>", tc.name, tc.cql)
	}
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY%s)", table, cols))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, s, table, "vec_"+tc.name) })
	}
}
func TestVector_MarshalerUDT(t *testing.T) {
	t.Parallel()
	serializationVectorGate(t, false)
	s := createV2Session(t)
	defer s.Close()
	table, typ := serializationTableName(t), serializationTypeName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TYPE %s(first_name text,last_name text,age int)", typ))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,couple vector<%s,2>)", table, typ))
	codec := serializationUDT(Params(Param("first_name", serializationString, func(v person) string { return v.FirstName }), Param("last_name", serializationString, func(v person) string { return v.LastName }), Param("age", Int32, func(v person) int32 { return v.Age })), Row(Column("first_name", serializationString, func(v *person) *string { return &v.FirstName }), Column("last_name", serializationString, func(v *person) *string { return &v.LastName }), Column("age", Int32, func(v *person) *int32 { return &v.Age })), serializationUDTOptions(typ))
	want := []person{{"Johny", "Bravo", 25}, {"Capitan", "Planet", 5}}
	require.Equal(t, want, serializationRoundTrip(t, s, table, "couple", 1, serializationVector(codec), want))
}
func TestVector_Empty(t *testing.T) {
	t.Parallel()
	serializationVectorGate(t, false)
	s := createV2Session(t)
	defer s.Close()
	fixed, variable := serializationTableName(t, "fixed"), serializationTableName(t, "variable")
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<float,3>)", fixed))
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<text,4>)", variable))
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", fixed))
	serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id) VALUES(1)", variable))
	fl, e := serializationOne(s, fmt.Sprintf("SELECT vec FROM %s WHERE id=1", fixed), OneColumn(NullableDecoder(serializationVector(Float32))))
	require.NoError(t, e)
	require.False(t, fl.Valid)
	require.Nil(t, fl.Value)
	tx, e := serializationOne(s, fmt.Sprintf("SELECT vec FROM %s WHERE id=1", variable), OneColumn(NullableDecoder(serializationVector(serializationString))))
	require.NoError(t, e)
	require.False(t, tx.Valid)
	require.Nil(t, tx.Value)
}
func TestVector_MissingDimension(t *testing.T) {
	t.Parallel()
	serializationVectorGate(t, false)
	s := createV2Session(t)
	defer s.Close()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vec vector<float,3>)", table))
	for _, v := range [][]float32{{8, -5}, {8, -5, 1, 3}} {
		err := serializationExec(s, fmt.Sprintf("INSERT INTO %s(id,vec) VALUES(1,?)", table), OneParam(VectorEncoder(Float32)), v)
		require.Error(t, err, "expected vector with 3 dimensions, received %d", len(v))
	}
}

func TestVector_SubTypeParsing(t *testing.T) {
	t.Parallel()

	if *flagDistribution == "scylla" && flagCassVersion.Before(2025, 4, 0) {
		t.Skip("Vector types are useful in ScyllaDB from 2025.4 and on")
	}
	prefix := apacheCassandraTypePrefix
	vectorTypePrefix := prefix + "VectorType"
	testCases := []struct {
		name     string
		custom   string
		expected TypeInfo
	}{
		{name: "text", custom: prefix + "UTF8Type", expected: cqltype.NewCustomType(0, TypeVarchar, "")},
		{name: "set_int", custom: prefix + "SetType(" + prefix + "Int32Type)", expected: CollectionType{NativeType: cqltype.NewCustomType(0, TypeSet, ""), Key: nil, Elem: cqltype.NewCustomType(0, TypeInt, "")}},
		{
			name:   "udt",
			custom: prefix + ("UserType(" + fixtureKeyspace("") + ",706572736f6e,66697273745f6e616d65:") + prefix + "UTF8Type,6c6173745f6e616d65:" + prefix + "UTF8Type,616765:" + prefix + "Int32Type)",
			expected: UDTTypeInfo{
				NativeType: cqltype.NewCustomType(0, TypeUDT, ""),
				KeySpace:   fixtureKeyspace(""),
				Name:       "person",
				Elements: []UDTField{
					UDTField{Name: "first_name", Type: cqltype.NewCustomType(0, TypeVarchar, "")},
					UDTField{Name: "last_name", Type: cqltype.NewCustomType(0, TypeVarchar, "")},
					UDTField{Name: "age", Type: cqltype.NewCustomType(0, TypeInt, "")},
				},
			},
		},
		{
			name:   "tuple",
			custom: prefix + "TupleType(" + prefix + "UTF8Type," + prefix + "Int32Type," + prefix + "UTF8Type)",
			expected: TupleTypeInfo{
				NativeType: cqltype.NewCustomType(0, TypeTuple, ""),
				Elems:      []TypeInfo{cqltype.NewCustomType(0, TypeVarchar, ""), cqltype.NewCustomType(0, TypeInt, ""), cqltype.NewCustomType(0, TypeVarchar, "")},
			},
		},
		{
			name:   "vector_vector_inet",
			custom: prefix + "VectorType(" + prefix + "VectorType(" + prefix + "InetAddressType, 2), 3)",
			expected: VectorType{
				NativeType: cqltype.NewCustomType(0, TypeCustom, vectorTypePrefix),
				SubType: VectorType{
					NativeType: cqltype.NewCustomType(0, TypeCustom, vectorTypePrefix),
					SubType:    cqltype.NewCustomType(0, TypeInet, ""),
					Dimensions: 2,
				},
				Dimensions: 3,
			},
		},
		{
			name:   "map_int_vector_text",
			custom: prefix + "MapType(" + prefix + "Int32Type," + prefix + "VectorType(" + prefix + "UTF8Type, 10))",
			expected: CollectionType{
				NativeType: cqltype.NewCustomType(0, TypeMap, ""),
				Key:        cqltype.NewCustomType(0, TypeInt, ""),
				Elem: VectorType{
					NativeType: cqltype.NewCustomType(0, TypeCustom, vectorTypePrefix),
					SubType:    cqltype.NewCustomType(0, TypeVarchar, ""),
					Dimensions: 10,
				},
			},
		},
		{
			name:   "set_map_vector_text_text",
			custom: prefix + "SetType(" + prefix + "MapType(" + prefix + "VectorType(" + prefix + "Int32Type, 10)," + prefix + "UTF8Type))",
			expected: CollectionType{
				NativeType: cqltype.NewCustomType(0, TypeSet, ""),
				Key:        nil,
				Elem: CollectionType{
					NativeType: cqltype.NewCustomType(0, TypeMap, ""),
					Key: VectorType{
						NativeType: cqltype.NewCustomType(0, TypeCustom, vectorTypePrefix),
						SubType:    cqltype.NewCustomType(0, TypeInt, ""),
						Dimensions: 10,
					},
					Elem: cqltype.NewCustomType(0, TypeVarchar, ""),
				},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			f := newFramer(nil, 0)
			f.WriteShort(0)
			f.WriteString(fmt.Sprintf("%sVectorType(%s, 2)", prefix, test.custom))
			parsedType := f.ReadTypeInfo()
			descriptor, err := dataTypeFromInfo(parsedType)
			require.NoError(t, err)
			dimension, ok := descriptor.Dimension()
			require.True(t, ok)
			require.Equal(t, uint16(2), dimension)
			element, ok := descriptor.Element()
			require.True(t, ok)
			expected, err := dataTypeFromInfo(test.expected)
			require.NoError(t, err)
			require.True(t, expected.Equal(element), "expected %s, got %s", expected, element)
		})
	}
}
