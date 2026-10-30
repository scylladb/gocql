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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cqltype

import (
	"bytes"
	"fmt"
)

type TypeInfo interface {
	Type() Type
	Version() byte
	Custom() string
}

type NativeType struct {
	//only used for TypeCustom
	custom string
	typ    Type
	proto  byte
}

func NewNativeType(proto byte, typ Type) NativeType {
	return NativeType{proto: proto, typ: typ, custom: ""}
}

func NewCustomType(proto byte, typ Type, custom string) NativeType {
	return NativeType{proto: proto, typ: typ, custom: custom}
}

func (t NativeType) Type() Type {
	return t.typ
}

func (t NativeType) Version() byte {
	return t.proto
}

func (t NativeType) Custom() string {
	return t.custom
}

func (t NativeType) String() string {
	switch t.typ {
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", t.typ, t.custom)
	default:
		return t.typ.String()
	}
}

func NewCollectionType(m NativeType, key, elem TypeInfo) CollectionType {
	return CollectionType{
		NativeType: m,
		Key:        key,
		Elem:       elem,
	}
}

type CollectionType struct {
	// Key is used only for TypeMap
	Key TypeInfo
	// Elem is used for TypeMap, TypeList and TypeSet
	Elem TypeInfo
	NativeType
}

type VectorType struct {
	SubType TypeInfo
	NativeType
	Dimensions int
}

// Zero returns the zero value for the vector CQL type.
func (t CollectionType) String() string {
	switch t.typ {
	case TypeMap:
		return fmt.Sprintf("%s(%s, %s)", t.typ, t.Key, t.Elem)
	case TypeList, TypeSet:
		return fmt.Sprintf("%s(%s)", t.typ, t.Elem)
	case TypeCustom:
		return fmt.Sprintf("%s(%s)", t.typ, t.custom)
	default:
		return t.typ.String()
	}
}

func NewTupleType(n NativeType, elems ...TypeInfo) TupleTypeInfo {
	return TupleTypeInfo{
		NativeType: n,
		Elems:      elems,
	}
}

type TupleTypeInfo struct {
	Elems []TypeInfo
	NativeType
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

type UDTField struct {
	Type TypeInfo
	Name string
}

func NewUDTType(proto byte, name, keySpace string, elems ...UDTField) UDTTypeInfo {
	return UDTTypeInfo{
		NativeType: NativeType{proto: proto, typ: TypeUDT, custom: ""},
		Name:       name,
		KeySpace:   keySpace,
		Elements:   elems,
	}
}

type UDTTypeInfo struct {
	KeySpace string
	Name     string
	Elements []UDTField
	NativeType
}

func (t UDTTypeInfo) String() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%s.%s{", t.KeySpace, t.Name)
	first := true
	for _, e := range t.Elements {
		if !first {
			fmt.Fprint(buf, ",")
		} else {
			first = false
		}

		fmt.Fprintf(buf, "%s=%v", e.Name, e.Type)
	}
	fmt.Fprint(buf, "}")

	return buf.String()
}

// String returns a human readable name for the Cassandra datatype
// described by t.
// Type is the identifier of a Cassandra internal datatype.
type Type int

const (
	TypeCustom    Type = 0x0000
	TypeAscii     Type = 0x0001
	TypeBigInt    Type = 0x0002
	TypeBlob      Type = 0x0003
	TypeBoolean   Type = 0x0004
	TypeCounter   Type = 0x0005
	TypeDecimal   Type = 0x0006
	TypeDouble    Type = 0x0007
	TypeFloat     Type = 0x0008
	TypeInt       Type = 0x0009
	TypeText      Type = 0x000A
	TypeTimestamp Type = 0x000B
	TypeUUID      Type = 0x000C
	TypeVarchar   Type = 0x000D
	TypeVarint    Type = 0x000E
	TypeTimeUUID  Type = 0x000F
	TypeInet      Type = 0x0010
	TypeDate      Type = 0x0011
	TypeTime      Type = 0x0012
	TypeSmallInt  Type = 0x0013
	TypeTinyInt   Type = 0x0014
	TypeDuration  Type = 0x0015
	TypeList      Type = 0x0020
	TypeMap       Type = 0x0021
	TypeSet       Type = 0x0022
	TypeUDT       Type = 0x0030
	TypeTuple     Type = 0x0031
)

// String returns the name of the identifier.
func (t Type) String() string {
	switch t {
	case TypeCustom:
		return "custom"
	case TypeAscii:
		return "ascii"
	case TypeBigInt:
		return "bigint"
	case TypeBlob:
		return "blob"
	case TypeBoolean:
		return "boolean"
	case TypeCounter:
		return "counter"
	case TypeDecimal:
		return "decimal"
	case TypeDouble:
		return "double"
	case TypeFloat:
		return "float"
	case TypeInt:
		return "int"
	case TypeText:
		return "text"
	case TypeTimestamp:
		return "timestamp"
	case TypeUUID:
		return "uuid"
	case TypeVarchar:
		return "varchar"
	case TypeTimeUUID:
		return "timeuuid"
	case TypeInet:
		return "inet"
	case TypeDate:
		return "date"
	case TypeDuration:
		return "duration"
	case TypeTime:
		return "time"
	case TypeSmallInt:
		return "smallint"
	case TypeTinyInt:
		return "tinyint"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	case TypeVarint:
		return "varint"
	case TypeTuple:
		return "tuple"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}
