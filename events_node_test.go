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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/debounce"
	frm "github.com/gocql/gocql/internal/frame"
)

// Session.handleNodeEvent and Session.handleNodeUp had no unit coverage at all
// -- they were reached only by the integration lane, which drives them through a
// live cluster and therefore exercises the happy path and little else. The
// branches pinned here are the ones a live cluster does not reliably produce:
// the per-host coalescing of a burst of status changes, and the two config
// switches that suppress dispatch entirely.

const (
	// nodeEventRefreshInterval is the debounce window the fixture's ring
	// refresher runs with. Production uses debounce.RingRefreshDebounceTime
	// (1s); a test only needs it short enough not to dominate the run.
	nodeEventRefreshInterval = 2 * time.Millisecond

	// nodeEventRefreshTimeout bounds a positive "a refresh should happen"
	// assertion. Generous on purpose: exceeding it means a refresh never came,
	// not that the machine was briefly busy.
	nodeEventRefreshTimeout = 2 * time.Second

	// nodeEventQuietWindow bounds a negative "no refresh should happen"
	// assertion. It is ~75x nodeEventRefreshInterval, so a refresh that was
	// wrongly scheduled has ample time to land and fail the test.
	nodeEventQuietWindow = 150 * time.Millisecond
)

// nodeEventPolicy records the HostStateNotifier half of HostSelectionPolicy.
// The rest of the interface comes from the embedded roundRobinHostPolicy, the
// same approach trackingPolicy takes in events_unit_test.go.
type nodeEventPolicy struct {
	roundRobinHostPolicy

	mu         sync.Mutex
	addHost    []string
	removeHost []string
	hostUp     []string
	hostDown   []string
}

func (p *nodeEventPolicy) AddHost(host *HostInfo)    { p.record(&p.addHost, host) }
func (p *nodeEventPolicy) RemoveHost(host *HostInfo) { p.record(&p.removeHost, host) }
func (p *nodeEventPolicy) HostUp(host *HostInfo)     { p.record(&p.hostUp, host) }
func (p *nodeEventPolicy) HostDown(host *HostInfo)   { p.record(&p.hostDown, host) }

func (p *nodeEventPolicy) record(dst *[]string, host *HostInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*dst = append(*dst, host.ConnectAddress().String())
}

func (p *nodeEventPolicy) snapshot(src []string) []string {
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func (p *nodeEventPolicy) addHostCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot(p.addHost)
}

func (p *nodeEventPolicy) removeHostCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot(p.removeHost)
}

func (p *nodeEventPolicy) hostUpCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot(p.hostUp)
}

func (p *nodeEventPolicy) hostDownCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot(p.hostDown)
}

// nodeEventFixture is a Session cut down to exactly what the node-event path
// touches: a host source to resolve event IPs against, a policy to observe
// dispatch, a ring refresher to observe debounced refreshes, and a connection
// pool.
//
// The pool starts as an empty &policyConnPool{}. policyConnPool.removeHost
// short-circuits on a missing host, so the DOWN path is safe, but addHost would
// build a real hostConnPool against a nil session and start dialing. Tests here
// therefore never let an UP event reach Session.startPoolFill -- they use either
// an unknown IP or a filtered host. startPoolFill is already covered by the
// integration lane.
//
// A test that needs to observe the pool being mutated has to populate
// hostConnPools itself, or the short-circuit makes the call unobservable; see
// TestSessionRemoveHost in connectionpool_test.go.
type nodeEventFixture struct {
	session   *Session
	policy    *nodeEventPolicy
	refreshes chan struct{}
}

func newNodeEventFixture(t *testing.T, opts ...func(*ClusterConfig)) *nodeEventFixture {
	t.Helper()

	policy := &nodeEventPolicy{}
	refreshes := make(chan struct{}, 16)

	s := &Session{
		policy: policy,
		logger: &testLogger{},
		pool:   &policyConnPool{},
	}
	for _, opt := range opts {
		opt(&s.cfg)
	}
	s.hostSource = &ringDescriber{cfg: &s.cfg, logger: s.logger}
	s.ringRefresher = debounce.NewRefreshDebouncer(nodeEventRefreshInterval, func() error {
		select {
		case refreshes <- struct{}{}:
		default:
		}
		return nil
	})
	t.Cleanup(s.ringRefresher.Stop)

	return &nodeEventFixture{session: s, policy: policy, refreshes: refreshes}
}

// addHost registers a host with the fixture's host source so that an event
// carrying ip resolves to it.
//
// broadcastAddress is what ringDescriber.addHostIfMissing keys hostIPToUUID by
// (via HostInfo.nodeToNodeAddress), and getHostByIP looks up the event's IP in
// that map -- so it has to match the IP the event carries, not just
// connectAddress.
func (f *nodeEventFixture) addHost(t *testing.T, ip string) *HostInfo {
	t.Helper()

	addr := net.ParseIP(ip)
	if addr == nil {
		t.Fatalf("addHost: %q is not an IP", ip)
	}
	host := &HostInfo{
		hostId:           MustRandomUUID(),
		connectAddress:   addr,
		broadcastAddress: addr,
		port:             9042,
	}
	f.session.hostSource.addOrUpdate(host)
	return host
}

func (f *nodeEventFixture) awaitRefresh(t *testing.T) {
	t.Helper()
	select {
	case <-f.refreshes:
	case <-time.After(nodeEventRefreshTimeout):
		t.Fatal("timed out waiting for a ring refresh")
	}
}

func (f *nodeEventFixture) assertNoRefresh(t *testing.T) {
	t.Helper()
	select {
	case <-f.refreshes:
		t.Fatal("ring refresh happened but should have been suppressed")
	case <-time.After(nodeEventQuietWindow):
	}
}

func statusChange(change, ip string) frame {
	return &frm.StatusChangeEventFrame{Change: change, Host: net.ParseIP(ip), Port: 9042}
}

func topologyChange(change, ip string) frame {
	return &frm.TopologyChangeEventFrame{Change: change, Host: net.ParseIP(ip), Port: 9042}
}

func assertStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d call(s) %v, want %d %v", what, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: call %d was %q, want %q (got %v)", what, i, got[i], want[i], got)
		}
	}
}

// TestHandleNodeEventCoalescesStatusChanges pins the per-host debouncing
// documented on handleNodeEvent: a burst of status changes collapses to one
// dispatch per host, and the last change for a host is the one that wins.
//
// Nothing asserted this before, and the collapse is easy to break -- the map in
// handleNodeEvent is keyed by IP string, so a change to how the key is derived
// would silently turn one dispatch into several.
func TestHandleNodeEventCoalescesStatusChanges(t *testing.T) {
	t.Parallel()

	t.Run("repeated DOWN for one host dispatches once", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		f.addHost(t, "127.0.10.1")

		f.session.handleNodeEvent([]frame{
			statusChange("DOWN", "127.0.10.1"),
			statusChange("DOWN", "127.0.10.1"),
			statusChange("DOWN", "127.0.10.1"),
		})

		assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
	})

	t.Run("distinct hosts each dispatch", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		f.addHost(t, "127.0.10.1")
		f.addHost(t, "127.0.10.2")

		f.session.handleNodeEvent([]frame{
			statusChange("DOWN", "127.0.10.1"),
			statusChange("DOWN", "127.0.10.2"),
		})

		got := f.policy.hostDownCalls()
		if len(got) != 2 {
			t.Fatalf("HostDown: got %v, want one call per host", got)
		}
		seen := map[string]bool{got[0]: true, got[1]: true}
		for _, want := range []string{"127.0.10.1", "127.0.10.2"} {
			if !seen[want] {
				t.Errorf("HostDown: %s never dispatched (got %v)", want, got)
			}
		}
	})

	t.Run("UP then DOWN leaves the host down", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		host := f.addHost(t, "127.0.10.1")

		f.session.handleNodeEvent([]frame{
			statusChange("UP", "127.0.10.1"),
			statusChange("DOWN", "127.0.10.1"),
		})

		assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
		assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
		if state := host.State(); state != NodeDown {
			t.Errorf("host state: got %v, want %v", state, NodeDown)
		}
	})

	t.Run("DOWN then UP leaves the host up", func(t *testing.T) {
		t.Parallel()

		// A known host the filter rejects. The filter is what keeps the UP from
		// reaching startPoolFill and dialing, and it leaves the host's state as
		// the one observable: handleNodeDown sets NodeDown *before* consulting
		// the filter, so a DOWN that was wrongly dispatched still shows up,
		// while handleNodeUp touches nothing for a filtered host.
		//
		// An unknown host does not work here. A DOWN for one is already a
		// no-op, so dispatching both events looks identical to dispatching only
		// the UP, and the assertion would hold with coalescing broken entirely.
		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.HostFilter = HostFilterFunc(func(*HostInfo) bool { return false })
		})
		host := f.addHost(t, "127.0.10.1")
		host.setState(NodeUp) // explicit: NodeUp is also the zero value

		f.session.handleNodeEvent([]frame{
			statusChange("DOWN", "127.0.10.1"),
			statusChange("UP", "127.0.10.1"),
		})

		if state := host.State(); state != NodeUp {
			t.Errorf("host state: got %v, want %v -- the superseded DOWN was dispatched", state, NodeUp)
		}
		assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
	})
}

// TestHandleNodeEventTopologyChange covers the topology half of the dispatcher:
// any topology frame in the batch collapses to a single ring refresh, and
// DisableTopologyEvents suppresses it.
func TestHandleNodeEventTopologyChange(t *testing.T) {
	t.Parallel()

	t.Run("topology change refreshes the ring", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		f.session.handleNodeEvent([]frame{topologyChange("NEW_NODE", "127.0.10.1")})
		f.awaitRefresh(t)
	})

	t.Run("DisableTopologyEvents suppresses the refresh", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.Events.DisableTopologyEvents = true
		})
		f.session.handleNodeEvent([]frame{topologyChange("NEW_NODE", "127.0.10.1")})
		f.assertNoRefresh(t)
	})

	t.Run("status changes are still dispatched when topology events are off", func(t *testing.T) {
		t.Parallel()

		// The two switches are independent; turning topology events off must not
		// take status dispatch with it.
		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.Events.DisableTopologyEvents = true
		})
		f.addHost(t, "127.0.10.1")

		f.session.handleNodeEvent([]frame{
			topologyChange("NEW_NODE", "127.0.10.1"),
			statusChange("DOWN", "127.0.10.1"),
		})

		assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
	})
}

// TestHandleNodeEventDisableNodeStatusEvents pins the guard added for
// apache/cassandra-gocql-driver#1591: a status change that arrives while node
// status events are disabled is dropped rather than dispatched.
func TestHandleNodeEventDisableNodeStatusEvents(t *testing.T) {
	t.Parallel()

	t.Run("enabled dispatches DOWN", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		f.addHost(t, "127.0.10.1")
		f.session.handleNodeEvent([]frame{statusChange("DOWN", "127.0.10.1")})

		assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
	})

	t.Run("disabled drops DOWN", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.Events.DisableNodeStatusEvents = true
		})
		host := f.addHost(t, "127.0.10.1")
		f.session.handleNodeEvent([]frame{statusChange("DOWN", "127.0.10.1")})

		assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
		if state := host.State(); state == NodeDown {
			t.Error("host was marked down even though node status events are disabled")
		}
	})

	t.Run("disabled drops UP", func(t *testing.T) {
		t.Parallel()

		// Unknown IP: were the UP dispatched, handleNodeUp would request a ring
		// refresh, so silence here means the event really was dropped.
		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.Events.DisableNodeStatusEvents = true
		})
		f.session.handleNodeEvent([]frame{statusChange("UP", "127.0.10.9")})

		f.assertNoRefresh(t)
	})
}

// TestHandleNodeUp covers Session.handleNodeUp's two early exits directly. Both
// are unreachable from a healthy cluster, which is why the integration lane
// never touched them.
func TestHandleNodeUp(t *testing.T) {
	t.Parallel()

	t.Run("unknown host triggers a ring refresh", func(t *testing.T) {
		t.Parallel()

		// An UP for a host the driver has never seen means the ring is stale,
		// so the driver re-reads it rather than guessing.
		f := newNodeEventFixture(t)
		f.session.handleNodeUp(net.ParseIP("127.0.10.9"), 9042)

		f.awaitRefresh(t)
		assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
	})

	t.Run("filtered host is ignored", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.HostFilter = HostFilterFunc(func(*HostInfo) bool { return false })
		})
		f.addHost(t, "127.0.10.1")

		f.session.handleNodeUp(net.ParseIP("127.0.10.1"), 9042)

		// The host is known, so this is not the unknown-host path: a filtered
		// host must be dropped outright, with no refresh and no pool fill.
		assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
		f.assertNoRefresh(t)
	})
}

// TestHandleNodeEventTopologySurvivesStatusForSameHost pins the reason
// handleNodeEvent tracks topology events in a separate bool instead of putting
// them in the per-host status map: a NEW_NODE must not be lost to a newer UP
// for the same host in the same batch.
//
// The batch here is NEW_NODE + UP for one unknown IP, with status events
// disabled. That combination is what makes the invariant observable at all.
// With status events enabled the property is self-healing -- handleNodeUp
// answers an unknown host with a ring refresh of its own, the same remedy the
// topology branch asks for -- so a NEW_NODE swallowed by the UP would still end
// up refreshing the ring and no test could tell. Disabling status events
// removes that second source, leaving the topology branch as the only thing
// that can produce a refresh.
//
// It catches the two refactors that would break the split: folding topology
// frames into sEvents (the UP overwrites the NEW_NODE and the refresh is lost),
// and gating the topology refresh on DisableNodeStatusEvents as well as
// DisableTopologyEvents. Note it does not pin the textual order of the two
// blocks in handleNodeEvent -- debounceRingRefresh only resets a trailing
// timer, so moving the topology block below the status loop is unobservable
// here.
func TestHandleNodeEventTopologySurvivesStatusForSameHost(t *testing.T) {
	t.Parallel()

	f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
		cfg.Events.DisableNodeStatusEvents = true
	})

	f.session.handleNodeEvent([]frame{
		topologyChange("NEW_NODE", "127.0.10.9"),
		statusChange("UP", "127.0.10.9"),
	})

	f.awaitRefresh(t)
	assertStrings(t, "AddHost", f.policy.addHostCalls(), nil)
}

// TestHandleNodeDown covers the branch of Session.handleNodeDown a healthy
// cluster does not reach: a DOWN for a host the HostFilter excludes.
//
// The asymmetry is deliberate to pin, because it is easy to "fix" in the wrong
// direction: handleNodeDown sets the host's state before consulting the filter,
// so a filtered host is still marked NodeDown, but it is not handed to
// policy.HostDown. handleNodeUp does the opposite -- it drops a filtered host
// before touching anything.
func TestHandleNodeDown(t *testing.T) {
	t.Parallel()

	t.Run("unfiltered host is marked down and dispatched", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		host := f.addHost(t, "127.0.10.1")

		// The fixture's pool is empty, and policyConnPool.removeHost
		// short-circuits on a missing host, so without an entry here the
		// s.pool.removeHost leg is unobservable -- dropping it from
		// handleNodeDown entirely would still pass. Same reasoning as
		// TestSessionRemoveHost in connectionpool_test.go.
		f.session.pool.hostConnPools = map[UUID]*hostConnPool{
			host.hostUUID(): newHostConnPool(f.session, host, 1, ""),
		}

		f.session.handleNodeDown(net.ParseIP("127.0.10.1"), 9042)

		if state := host.State(); state != NodeDown {
			t.Errorf("host state: got %v, want %v", state, NodeDown)
		}
		assertStrings(t, "HostDown", f.policy.hostDownCalls(), []string{"127.0.10.1"})
		if _, ok := f.session.pool.getPoolByHostID(host.HostID()); ok {
			t.Error("host that went down still has a connection pool")
		}
	})

	t.Run("filtered host is marked down but not dispatched", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.HostFilter = HostFilterFunc(func(*HostInfo) bool { return false })
		})
		host := f.addHost(t, "127.0.10.1")
		f.session.pool.hostConnPools = map[UUID]*hostConnPool{
			host.hostUUID(): newHostConnPool(f.session, host, 1, ""),
		}

		f.session.handleNodeDown(net.ParseIP("127.0.10.1"), 9042)

		// setState happens before the filter check, so it still lands.
		if state := host.State(); state != NodeDown {
			t.Errorf("host state: got %v, want %v", state, NodeDown)
		}
		assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
		// The filter returns before both policy.HostDown and pool.removeHost,
		// so the pool survives -- the other half of the asymmetry this test
		// exists to pin.
		if _, ok := f.session.pool.getPoolByHostID(host.HostID()); !ok {
			t.Error("filtered host lost its connection pool")
		}
	})

	t.Run("unknown host is ignored", func(t *testing.T) {
		t.Parallel()

		// Unlike handleNodeUp, a DOWN for a host the driver does not know is
		// dropped outright -- there is nothing to mark down and no reason to
		// re-read the ring for a node that just left.
		f := newNodeEventFixture(t)

		f.session.handleNodeDown(net.ParseIP("127.0.10.9"), 9042)

		assertStrings(t, "HostDown", f.policy.hostDownCalls(), nil)
		f.assertNoRefresh(t)
	})
}

// TestHandleNodeConnected covers Session.handleNodeConnected, which the pool
// calls once a connection to a host is up. Its filter branch was the uncovered
// part: a filtered host still gets its state set, but must not be announced to
// the policy.
func TestHandleNodeConnected(t *testing.T) {
	t.Parallel()

	t.Run("unfiltered host is marked up and announced", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t)
		host := f.addHost(t, "127.0.10.1")
		// nodeState's zero value IS NodeUp, so a freshly built HostInfo already
		// reads as up and the assertion below would hold even if
		// handleNodeConnected never called setState. Start from NodeDown so the
		// transition is what is being observed.
		host.setState(NodeDown)

		f.session.handleNodeConnected(host)

		if state := host.State(); state != NodeUp {
			t.Errorf("host state: got %v, want %v", state, NodeUp)
		}
		assertStrings(t, "HostUp", f.policy.hostUpCalls(), []string{"127.0.10.1"})
	})

	t.Run("filtered host is marked up but not announced", func(t *testing.T) {
		t.Parallel()

		f := newNodeEventFixture(t, func(cfg *ClusterConfig) {
			cfg.HostFilter = HostFilterFunc(func(*HostInfo) bool { return false })
		})
		host := f.addHost(t, "127.0.10.1")
		host.setState(NodeDown) // see the note in the unfiltered case

		f.session.handleNodeConnected(host)

		if state := host.State(); state != NodeUp {
			t.Errorf("host state: got %v, want %v", state, NodeUp)
		}
		assertStrings(t, "HostUp", f.policy.hostUpCalls(), nil)
	})
}
