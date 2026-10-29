/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"testing"
)

func FuzzV2StreamingValues(f *testing.F) {
	f.Add(byte(0), []byte{0, 0, 0, 1, 1}, uint8(1))
	f.Add(byte(1), streamCell([]byte("€")), uint8(1))
	f.Add(byte(2), []byte{0, 0, 0, 4, 0, 0, 0, 0}, uint8(3))
	f.Add(byte(3), []byte{255, 255, 255, 254}, uint8(1))
	boolean, text, blob := codecTestMust(ScalarType(TypeBoolean)), codecTestMust(ScalarType(TypeText)), codecTestMust(ScalarType(TypeBlob))
	list := codecTestMust(ListType(blob))
	f.Fuzz(func(t *testing.T, kind byte, data []byte, chunk uint8) {
		if len(data) > 4096 {
			return
		}
		switch kind % 4 {
		case 0:
			var value bool
			_ = streamDecodeFixture(t, boolean, StreamBool, data, &value, int(chunk)+1)
		case 1:
			value := make([]byte, 0, 64)
			_ = streamDecodeFixture(t, text, StreamText, data, &value, int(chunk)+1)
		case 2:
			value := make([][]byte, 8)
			for i := range value {
				value[i] = make([]byte, 0, 16)
			}
			_ = streamDecodeFixture(t, list, StreamList(StreamBlob), data, &value, int(chunk)+1)
		case 3:
			value := StreamEncodedValue{Bytes: make([]byte, 0, 128)}
			_ = streamDecodeFixture(t, list, StreamEncoded, data, &value, int(chunk)+1)
		}
	})
}
