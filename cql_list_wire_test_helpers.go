//go:build all || unit
// +build all unit

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

package gocql

import "encoding/binary"

// buildCQLList and buildCQLListWithNulls live under `all || unit` (rather
// than marshal_test.go's `unit`-only tag) because list_fastpath_test.go and
// marshal_edgecases_test.go, which build under `all || unit`, call them too.

func buildCQLList(elems ...[]byte) []byte {
	var buf []byte
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(elems)))
	buf = append(buf, countBytes...)
	for _, e := range elems {
		lenBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBytes, uint32(len(e)))
		buf = append(buf, lenBytes...)
		buf = append(buf, e...)
	}
	return buf
}

// buildCQLListWithNulls builds a CQL binary list where a nil entry encodes a
// null element (length -1), matching the wire format the server emits.
func buildCQLListWithNulls(elems ...[]byte) []byte {
	var buf []byte
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(elems)))
	buf = append(buf, countBytes...)
	for _, e := range elems {
		lenBytes := make([]byte, 4)
		if e == nil {
			binary.BigEndian.PutUint32(lenBytes, uint32(0xFFFFFFFF)) // -1
			buf = append(buf, lenBytes...)
			continue
		}
		binary.BigEndian.PutUint32(lenBytes, uint32(len(e)))
		buf = append(buf, lenBytes...)
		buf = append(buf, e...)
	}
	return buf
}
