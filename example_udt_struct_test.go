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

package gocql_test

import (
	"fmt"
	gocql "github.com/scylladb/gocql/v2"

	"log"
)

func Example_userDefinedTypes() {
	type address struct {
		Street string
		Number int32
	}
	text, _ := gocql.ScalarType(gocql.TypeText)
	integer, _ := gocql.ScalarType(gocql.TypeInt)
	typ, err := gocql.UDTType("example", "address", gocql.ColumnSpec{Name: "street", Type: text}, gocql.ColumnSpec{Name: "number", Type: integer})
	if err != nil {
		log.Fatal(err)
	}
	opts := gocql.UDTOptions{Keyspace: "example", Name: "address"}
	encoder := gocql.UDTEncoder(gocql.Params(gocql.Param("street", gocql.StringEncoder, func(v address) string { return v.Street }), gocql.Param("number", gocql.Int32, func(v address) int32 { return v.Number })), opts)
	encoded, err := gocql.EncodeValueOwned(typ, encoder, address{"Main Street", 42})
	if err != nil {
		log.Fatal(err)
	}
	type view struct {
		Street gocql.Text
		Number int32
	}
	decoder := gocql.UDTDecoder(gocql.Row(gocql.Column("street", gocql.TextCodec, func(v *view) *gocql.Text { return &v.Street }), gocql.Column("number", gocql.Int32, func(v *view) *int32 { return &v.Number })), opts)
	value, err := gocql.DecodeValue(decoder, encoded)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value.Street.String(), value.Number)
	// Output: Main Street 42
}
