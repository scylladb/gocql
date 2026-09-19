//go:build unit
// +build unit

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

package gocql

import (
	"fmt"
	"testing"
)

// stubConnPicker reports a fixed InFlight and panics on everything else, so a
// test that accidentally exercises more of the pool than partitionHealthy
// needs fails loudly rather than on a nil dereference.
type stubConnPicker struct {
	inFlight int
}

func (s *stubConnPicker) InFlight() int { return s.inFlight }

func (s *stubConnPicker) Pick(Token, ExecutableQuery) *Conn {
	panic("stubConnPicker.Pick: not expected")
}
func (s *stubConnPicker) Put(*Conn) error  { panic("stubConnPicker.Put: not expected") }
func (s *stubConnPicker) Remove(*Conn)     { panic("stubConnPicker.Remove: not expected") }
func (s *stubConnPicker) Size() (int, int) { panic("stubConnPicker.Size: not expected") }
func (s *stubConnPicker) Close()           { panic("stubConnPicker.Close: not expected") }
func (s *stubConnPicker) NextShard() (int, int) {
	panic("stubConnPicker.NextShard: not expected")
}
func (s *stubConnPicker) GetConnectionCount() int {
	panic("stubConnPicker.GetConnectionCount: not expected")
}
func (s *stubConnPicker) GetExcessConnectionCount() int {
	panic("stubConnPicker.GetExcessConnectionCount: not expected")
}
func (s *stubConnPicker) GetShardCount() int {
	panic("stubConnPicker.GetShardCount: not expected")
}

// sequenceConnPicker reports a different InFlight on successive calls, holding
// the last value once the sequence runs out. It exists to make a host look
// healthy when counted and busy when placed, which is the race
// partitionHealthy's snapshot is there to survive.
type sequenceConnPicker struct {
	stubConnPicker // the panicking methods; its InFlight is shadowed below
	counts         []int
	calls          int
}

func (s *sequenceConnPicker) InFlight() int {
	n := s.calls
	s.calls++
	if n >= len(s.counts) {
		n = len(s.counts) - 1
	}
	return s.counts[n]
}

// hostLabels names hosts by the index they started at, so a failure message
// reads as an order ("h1 h0") rather than as a pair of UUIDs. HostInfo.HostID
// renders the UUID, which is not what these tests are about, so the mapping is
// kept on the side and keyed by pointer.
type hostLabels map[*HostInfo]string

func (l hostLabels) names(replicas []*HostInfo) []string {
	out := make([]string, len(replicas))
	for i, h := range replicas {
		name, ok := l[h]
		if !ok {
			name = fmt.Sprintf("<unknown %p>", h)
		}
		out[i] = name
	}
	return out
}

// requireUsableThreshold fails fast when MAX_IN_FLIGHT_THRESHOLD cannot express
// a healthy host, which is the one way these fixtures break without this file
// changing.
//
// IsBusy is `InFlight() >= MAX_IN_FLIGHT_THRESHOLD`, so at or below zero every
// host counts as busy and no in-flight value means "idle". AvoidSlowReplicas
// assigns the global when its option is applied, and a parallel test resumes
// only once every sequential test has finished -- so a single sequential test
// building a policy with AvoidSlowReplicas(0) leaves every test here failing
// for a reason visible nowhere in this file. Reading the global rather than
// writing it keeps this file from being the cause; it does not make it immune.
func requireUsableThreshold(t *testing.T) {
	t.Helper()

	if MAX_IN_FLIGHT_THRESHOLD < 1 {
		t.Fatalf("MAX_IN_FLIGHT_THRESHOLD is %d, so IsBusy reports every host busy "+
			"and these fixtures cannot express a healthy one. Some other test in "+
			"this package applied AvoidSlowReplicas with that value; pass the "+
			"existing MAX_IN_FLIGHT_THRESHOLD back the way driver_config_test.go "+
			"does, rather than a literal.", MAX_IN_FLIGHT_THRESHOLD)
	}
}

// hostsWithPickers builds replicas backed by the given pickers, the session
// those lookups resolve against, and labels naming each host by its input
// position.
func hostsWithPickers(t *testing.T, pickers []ConnPicker) ([]*HostInfo, *Session, hostLabels) {
	t.Helper()
	requireUsableThreshold(t)

	pools := make(map[UUID]*hostConnPool, len(pickers))
	replicas := make([]*HostInfo, len(pickers))
	labels := make(hostLabels, len(pickers))

	for i, picker := range pickers {
		// A distinct valid UUID per host: getPool keys on it, so two hosts
		// sharing one would silently share a pool.
		host := HostInfoBuilder{HostId: fmt.Sprintf("00000000-0000-0000-0000-%012d", i)}.Build()
		replicas[i] = &host
		labels[&host] = fmt.Sprintf("h%d", i)

		pools[host.hostUUID()] = &hostConnPool{
			host:       &host,
			connPicker: picker,
		}
	}

	return replicas, &Session{pool: &policyConnPool{hostConnPools: pools}}, labels
}

// hostsWithInFlight builds replicas whose pools report the given, constant
// in-flight counts.
func hostsWithInFlight(t *testing.T, inFlight []int) ([]*HostInfo, *Session, hostLabels) {
	t.Helper()

	pickers := make([]ConnPicker, len(inFlight))
	for i, n := range inFlight {
		pickers[i] = &stubConnPicker{inFlight: n}
	}
	return hostsWithPickers(t, pickers)
}

// busyHosts builds replicas whose IsBusy(session) follows busy, the session
// those calls resolve against, and labels naming each host by its input
// position.
//
// IsBusy is `ok && h != nil && pool.InFlight() >= MAX_IN_FLIGHT_THRESHOLD`, so
// a healthy host gets a pool reporting 0 in flight and a busy one a pool well
// above the threshold. Tests that care about the threshold itself use
// hostsWithInFlight directly. The threshold is a package global; this file only
// reads it, and requireUsableThreshold above covers what that does not.
func busyHosts(t *testing.T, busy []bool) ([]*HostInfo, *Session, hostLabels) {
	t.Helper()

	inFlight := make([]int, len(busy))
	for i, isBusy := range busy {
		if isBusy {
			inFlight[i] = MAX_IN_FLIGHT_THRESHOLD * 10
		}
	}
	return hostsWithInFlight(t, inFlight)
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPartitionHealthyOrder pins the two properties partitionHealthy promises:
// every healthy host precedes every busy one, and within each group the input
// order survives. The sizes straddle the 9-element stack buffers the function
// keeps for busy and tmp -- 9 takes the buffer, 10 takes the heap -- because
// that boundary is the one place the two paths can disagree.
func TestPartitionHealthyOrder(t *testing.T) {
	t.Parallel()

	const bufSize = 9 // len(busyBuf)/len(buf) in partitionHealthy

	tests := []struct {
		name string
		busy []bool
		want []string
	}{
		{
			name: "empty",
			busy: nil,
			want: []string{},
		},
		{
			name: "single healthy",
			busy: []bool{false},
			want: []string{"h0"},
		},
		{
			name: "single busy",
			busy: []bool{true},
			want: []string{"h0"},
		},
		{
			name: "all healthy keeps order",
			busy: []bool{false, false, false},
			want: []string{"h0", "h1", "h2"},
		},
		{
			name: "all busy keeps order",
			busy: []bool{true, true, true},
			want: []string{"h0", "h1", "h2"},
		},
		{
			name: "busy first moves to back",
			busy: []bool{true, false},
			want: []string{"h1", "h0"},
		},
		{
			name: "interleaved is stable in both groups",
			busy: []bool{true, false, true, false, true, false},
			want: []string{"h1", "h3", "h5", "h0", "h2", "h4"},
		},
		{
			name: "only the last is healthy",
			busy: []bool{true, true, true, false},
			want: []string{"h3", "h0", "h1", "h2"},
		},
		{
			name: "only the first is busy at buffer size",
			busy: append([]bool{true}, make([]bool, bufSize-1)...),
			want: []string{"h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h0"},
		},
		{
			name: "one past the buffer takes the heap path",
			busy: []bool{true, false, true, false, true, false, true, false, true, false},
			want: []string{"h1", "h3", "h5", "h7", "h9", "h0", "h2", "h4", "h6", "h8"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			replicas, session, labels := busyHosts(t, tt.busy)
			partitionHealthy(replicas, session)

			if got := labels.names(replicas); !equalNames(got, tt.want) {
				t.Errorf("partitionHealthy() order = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPartitionHealthyAcrossBufferBoundary walks every size either side of the
// stack buffer with the same alternating pattern. A fixed table can state the
// boundary is covered; this states that nothing between 1 and 20 behaves
// differently, which is what the two allocation paths are supposed to
// guarantee.
func TestPartitionHealthyAcrossBufferBoundary(t *testing.T) {
	t.Parallel()

	for n := 1; n <= 20; n++ {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			t.Parallel()

			busy := make([]bool, n)
			for i := range busy {
				busy[i] = i%2 == 0 // even indices busy, odd healthy
			}

			replicas, session, labels := busyHosts(t, busy)
			partitionHealthy(replicas, session)

			var want []string
			for i := range busy { // healthy, in input order
				if !busy[i] {
					want = append(want, fmt.Sprintf("h%d", i))
				}
			}
			for i := range busy { // then busy, in input order
				if busy[i] {
					want = append(want, fmt.Sprintf("h%d", i))
				}
			}

			if got := labels.names(replicas); !equalNames(got, want) {
				t.Errorf("partitionHealthy(n=%d) order = %v, want %v", n, got, want)
			}
		})
	}
}

// TestPartitionHealthyBusyThreshold pins the comparison the partition rests
// on. AvoidSlowReplicas documents a replica as avoided when it has "equal or
// more than MAX_IN_FLIGHT_THRESHOLD requests in flight", and IsBusy implements
// that as `>=`. Nothing else pins which side of the boundary the threshold
// itself falls on: the other tests in this file use 0 and 10x the threshold,
// so relaxing `>=` to `>` leaves the whole unit suite green.
//
// Each case puts the host under test first and a plainly healthy or busy host
// second, so a misclassification changes the resulting order. An arrangement
// that happens to leave the input order intact could not tell the two
// comparisons apart.
func TestPartitionHealthyBusyThreshold(t *testing.T) {
	t.Parallel()

	threshold := MAX_IN_FLIGHT_THRESHOLD

	tests := []struct {
		name     string
		inFlight []int
		want     []string
	}{
		{
			// The boundary itself: at exactly the threshold h0 is busy, so it
			// sorts behind the idle h1. Under `>` it would stay in front.
			name:     "exactly at the threshold counts as busy",
			inFlight: []int{threshold, 0},
			want:     []string{"h1", "h0"},
		},
		{
			// One short of the threshold is healthy, so it sorts ahead of the
			// saturated h0.
			name:     "one below the threshold counts as healthy",
			inFlight: []int{threshold * 10, threshold - 1},
			want:     []string{"h1", "h0"},
		},
		{
			name:     "one above the threshold counts as busy",
			inFlight: []int{threshold + 1, 0},
			want:     []string{"h1", "h0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			replicas, session, labels := hostsWithInFlight(t, tt.inFlight)
			partitionHealthy(replicas, session)

			if got := labels.names(replicas); !equalNames(got, tt.want) {
				t.Errorf("partitionHealthy() with in-flight %v order = %v, want %v",
					tt.inFlight, got, tt.want)
			}
		})
	}
}

// TestPartitionHealthyUsesBusySnapshot pins the snapshot itself. partitionHealthy
// samples IsBusy once per replica up front and the placement loop reads that
// array, rather than asking again while it moves hosts around. Every other test
// here uses pickers with a constant InFlight, under which a live re-read behaves
// identically -- so replacing `busy[i]` with `h.IsBusy(s)` in the placement loop
// leaves the whole unit suite green.
//
// h1 flips: idle when counted, saturated when placed. That is exactly the race
// the snapshot exists for. healthyCount is fixed at 2 during counting, so ui
// starts at index 2; a live re-read would classify h1 busy on the second pass
// and push ui past the end of the slice.
func TestPartitionHealthyUsesBusySnapshot(t *testing.T) {
	t.Parallel()

	flip := &sequenceConnPicker{counts: []int{0, MAX_IN_FLIGHT_THRESHOLD}}
	replicas, session, labels := hostsWithPickers(t, []ConnPicker{
		&stubConnPicker{inFlight: MAX_IN_FLIGHT_THRESHOLD * 10}, // h0, busy throughout
		flip,                         // h1, healthy when counted, busy when placed
		&stubConnPicker{inFlight: 0}, // h2, healthy throughout
	})

	partitionHealthy(replicas, session)

	// The snapshot counted h1 as healthy, so it stays in the healthy group and
	// keeps its position ahead of h2. Only h0 moves.
	want := []string{"h1", "h2", "h0"}
	if got := labels.names(replicas); !equalNames(got, want) {
		t.Errorf("partitionHealthy() order = %v, want %v", got, want)
	}

	// The invariant stated directly: each replica's busyness is sampled once.
	// A second call means the placement loop consulted IsBusy again, which is
	// the bug even when the cursors happen not to run off the end.
	if flip.calls != 1 {
		t.Errorf("IsBusy sampled %d times for one replica, want 1; the placement "+
			"loop must read the snapshot rather than re-check", flip.calls)
	}
}

// TestPartitionHealthyPreservesSet guards the copy-back loop: replicas must
// come out a permutation of what went in, with nothing dropped, duplicated or
// left nil. The two indices hi and ui advance independently over one shared
// slice, so an off-by-one there overwrites an entry instead of moving it --
// which a pure order assertion on a correct healthyCount would not notice.
func TestPartitionHealthyPreservesSet(t *testing.T) {
	t.Parallel()

	busy := []bool{true, false, false, true, true, false, true, false, false, true, true}
	replicas, session, labels := busyHosts(t, busy)

	before := make(map[string]int, len(replicas))
	for _, h := range replicas {
		before[labels[h]]++
	}

	partitionHealthy(replicas, session)

	after := make(map[string]int, len(replicas))
	for i, h := range replicas {
		if h == nil {
			t.Fatalf("partitionHealthy() left replicas[%d] nil", i)
		}
		after[labels[h]]++
	}

	if len(after) != len(before) {
		t.Fatalf("partitionHealthy() host set size = %d, want %d", len(after), len(before))
	}
	for name, count := range before {
		if after[name] != count {
			t.Errorf("partitionHealthy() count for %s = %d, want %d", name, after[name], count)
		}
	}
}

// TestPartitionHealthyEmptyPool covers the path where no host resolves to a
// pool at all: getPool misses, IsBusy is false for everyone, and the early
// return for "all one category" leaves the slice untouched. This is the state
// a freshly built session is in before any pool is filled.
func TestPartitionHealthyEmptyPool(t *testing.T) {
	t.Parallel()

	replicas, _, labels := busyHosts(t, []bool{false, false, false, false})
	session := &Session{pool: &policyConnPool{hostConnPools: map[UUID]*hostConnPool{}}}

	partitionHealthy(replicas, session)

	want := []string{"h0", "h1", "h2", "h3"}
	if got := labels.names(replicas); !equalNames(got, want) {
		t.Errorf("partitionHealthy() with empty pool order = %v, want %v", got, want)
	}
}
