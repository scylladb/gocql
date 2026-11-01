// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"slices"
	"testing"
)

// Placement is a set of distinct host/shard destinations. Duplicate entries
// cannot satisfy an exact authoritative-set assertion in either direction.
func step17SameReplicaSet(a, b []Target) bool {
	if len(a) != len(b) {
		return false
	}
	for i, target := range a {
		if slices.Contains(a[:i], target) || !slices.Contains(b, target) {
			return false
		}
	}
	for i, target := range b {
		if slices.Contains(b[:i], target) {
			return false
		}
	}
	return true
}

func TestStep17AuthoritativeReplicaSetEquality(t *testing.T) {
	a := Target{HostID: UUID{15: 1}, ShardID: Option[uint32]{Set: true}}
	b := Target{HostID: UUID{15: 2}, ShardID: Option[uint32]{Set: true, Value: 1}}
	otherShard := a
	otherShard.ShardID.Value = 1
	for _, test := range []struct {
		name        string
		left, right []Target
		want        bool
	}{
		{"empty", nil, nil, true},
		{"same", []Target{a, b}, []Target{a, b}, true},
		{"reordered", []Target{a, b}, []Target{b, a}, true},
		{"driver-duplicate", []Target{a, a}, []Target{a, b}, false},
		{"authority-duplicate", []Target{a, b}, []Target{a, a}, false},
		{"both-duplicate", []Target{a, a}, []Target{a, a}, false},
		{"different-length", []Target{a}, []Target{a, b}, false},
		{"different-shard", []Target{a, b}, []Target{otherShard, b}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := step17SameReplicaSet(test.left, test.right); got != test.want {
				t.Fatal(got, test.want)
			}
		})
	}
}
