//go:build unit
// +build unit

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

import (
	"encoding/binary"
	"math"
	"testing"
)

// TestScyllaCDCPartitioner_HashInt64MatchesHash verifies hashInt64 matches Hash().
func TestScyllaCDCPartitioner_HashInt64MatchesHash(t *testing.T) {
	t.Parallel()

	p := scyllaCDCPartitioner{logger: &defaultLogger{}}
	var _ int64Hasher = p

	mk16 := func(upper uint64, version uint64) []byte {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[0:], upper)
		binary.BigEndian.PutUint64(buf[8:], version)
		return buf
	}

	keys := [][]byte{
		nil,
		{},
		{1, 2, 3},             // < 8 bytes: min-token path
		{1, 2, 3, 4, 5, 6, 7}, // 7 bytes: still < 8
		mk16(0, 1)[:8],        // exactly 8 bytes: upper-qword path, no debug checks possible
		mk16(1, scyllaCDCMinSupportedVersion),
		mk16(math.MaxUint64, scyllaCDCMaxSupportedVersion),
		mk16(12345, 0),                  // unsupported version (0 < min) -- still just logs in debug mode
		mk16(12345, 99),                 // unsupported version (99 > max) -- still just logs in debug mode
		append(mk16(42, 1), 0xFF, 0xFF), // >16 bytes: wrong-size path, still just logs
	}

	for _, k := range keys {
		want := p.Hash(k)
		wantInt64, ok := want.(int64Token)
		if !ok {
			t.Fatalf("Hash(%v) returned %T, want int64Token", k, want)
		}
		got := int64Token(p.hashInt64(k))
		if got != wantInt64 {
			t.Errorf("hashInt64(%v) = %d, want %d (from Hash)", k, got, wantInt64)
		}
	}
}

func TestScyllaCDCPartitioner_HashInt64_ShortKeyReturnsMinToken(t *testing.T) {
	t.Parallel()

	p := scyllaCDCPartitioner{logger: &defaultLogger{}}
	got := p.hashInt64([]byte{1, 2, 3})
	if got != int64(scyllaCDCMinToken) {
		t.Fatalf("hashInt64(short key) = %d, want %d (scyllaCDCMinToken)", got, int64(scyllaCDCMinToken))
	}
}

func TestScyllaCDCPartitioner_HashInt64_ZeroAlloc(t *testing.T) {
	p := scyllaCDCPartitioner{logger: &defaultLogger{}}
	pk := make([]byte, 16)
	binary.BigEndian.PutUint64(pk[0:], 12345)
	binary.BigEndian.PutUint64(pk[8:], 1)

	var sink int64
	allocs := testing.AllocsPerRun(1000, func() {
		sink = p.hashInt64(pk)
	})
	if allocs != 0 {
		t.Errorf("hashInt64 allocated %.2f allocs/op, want 0", allocs)
	}
	_ = sink
}
