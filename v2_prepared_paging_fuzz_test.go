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
	model "github.com/scylladb/gocql/v2/internal/model"

	"errors"
	"github.com/scylladb/gocql/v2/internal/wire"
	"testing"
)

func FuzzV2PageDestinationFraming(f *testing.F) {
	f.Add([]byte{0, 0, 0, 4, 0, 0, 0, 7}, uint8(1))
	f.Add([]byte{255, 255, 255, 255}, uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, count uint8) {
		if len(data) > 4096 {
			return
		}
		cols := NewColumns(ColumnSpec{Name: "value", Type: codecTestMust(ScalarType(TypeInt))})
		chain := codecTestMust(wire.NewChain(wire.Borrow(data)))
		owner := wire.NewLease(chain, nil)
		p := codecTestMust(newPage(&responsePage{responseData: responseData{Columns: cols, Rows: chain, RowCount: int32(count % 8), Owner: owner}}, OneColumn(Int32), nil))
		defer p.Close()
		dst := int32(99)
		for i := 0; i < 9; i++ {
			if !p.NextInto(&dst) {
				if dst != 0 {
					t.Fatal("failed destination not cleared")
				}
				err := p.Err()
				if p.NextInto(nil) || p.Err() != err {
					t.Fatal("terminal iterator changed")
				}
				break
			}
		}
	})
}
func FuzzV2PagingHistoryBudgets(f *testing.F) {
	f.Add([]byte{1, 'a', 1, 'b', 1, 'a'})
	f.Add([]byte{0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		h := pagingHistory{maxCount: 16, maxBytes: 128}
		seen := make(map[string]bool)
		bytesUsed := 0
		for len(data) > 0 {
			n := int(data[0] % 17)
			data = data[1:]
			if n > len(data) {
				n = len(data)
			}
			token := append([]byte{}, data[:n]...)
			data = data[n:]
			state := model.PageStateOwned(token, 1)
			err := h.add(state)
			if seen[string(token)] {
				if !errors.Is(err, ErrPagingNoProgress) {
					t.Fatal("duplicate", err)
				}
				continue
			}
			if len(seen) >= 16 || len(token) > 128-bytesUsed {
				if !errors.Is(err, ErrPagingStateLimit) {
					t.Fatal("budget", err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			seen[string(token)] = true
			bytesUsed += len(token)
			if h.count != len(seen) || h.bytes != int64(bytesUsed) {
				t.Fatal("history accounting")
			}
		}
	})
}
