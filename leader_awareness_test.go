//go:build unit

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
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"strings"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/tablets"
)

func v2Hint(t *testing.T, version uint64, replicas ...UUID) tablets.TabletInfo {
	t.Helper()
	field := func(v []byte) []byte { return append(binary.BigEndian.AppendUint32(nil, uint32(len(v))), v...) }
	big := func(v uint64) []byte { return field(binary.BigEndian.AppendUint64(nil, v)) }
	list := binary.BigEndian.AppendUint32(nil, uint32(len(replicas)))
	for _, host := range replicas {
		tuple := append(field(host[:]), field([]byte{0, 0, 0, 1})...)
		list = append(list, field(tuple)...)
	}
	payload := append(big(1<<63), big(math.MaxInt64)...)
	payload = append(payload, field(list)...)
	payload = append(payload, big(version)...)
	tablet, err := tablets.ParseHintV2(payload, "CaseKS", "CaseTable")
	if err != nil {
		t.Fatal(err)
	}
	return tablet
}

func TestTabletV2NegotiationAndExecute(t *testing.T) {
	for _, tc := range []struct {
		name      string
		supported map[string][]string
		v1, v2    bool
	}{
		{"none", nil, false, false},
		{"v1", map[string][]string{tabletsRoutingV1: {}}, true, false},
		{"v2", map[string][]string{tabletsRoutingV2: {}}, false, true},
		{"both", map[string][]string{tabletsRoutingV1: {}, tabletsRoutingV2: {}}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{version: protoVersion4, logger: &testLogger{}}
			c.cqlProtoExts = parseCQLProtocolExtensions(tc.supported, c.version, c.logger)
			if (findCQLProtoExtByName(c.cqlProtoExts, tabletsRoutingV1) != nil) != tc.v1 {
				t.Fatal("wrong V1 negotiation")
			}
			if (findCQLProtoExtByName(c.cqlProtoExts, tabletsRoutingV2) != nil) != tc.v2 {
				t.Fatal("wrong V2 negotiation")
			}
			c.initFramerCache()
			if c.isTabletsRoutingV2() != tc.v2 || c.isTabletSupported() != (tc.v1 || tc.v2) {
				t.Fatal("connection flags disagree")
			}
			f := c.getWriteFramer()
			defer c.releaseWriteFramer(f)
			base := newFramer(nil, protoVersion4)
			params := queryParams{consistency: Quorum, tabletVersionBlock: 0xa5, values: []queryValues{{value: []byte("bound")}}}
			payload := map[string][]byte{"x": []byte("y")}
			if err := f.writeExecuteFrame(1, []byte{1}, nil, &params, &payload); err != nil {
				t.Fatal(err)
			}
			if err := base.writeExecuteFrame(1, []byte{1}, nil, &params, &payload); err != nil {
				t.Fatal(err)
			}
			want := base.buf[9:]
			if tc.v2 {
				want = append(want, 0xa5)
			}
			if !bytes.Equal(f.buf[9:], want) {
				t.Fatalf("EXECUTE body: %x, want %x", f.buf[9:], want)
			}
			if got := binary.BigEndian.Uint32(f.buf[5:9]); int(got) != len(want) {
				t.Fatalf("frame length %d, want %d", got, len(want))
			}
			q := newFramer(nil, protoVersion4)
			q.tabletsRoutingV2 = tc.v2
			if err := q.writeQueryFrame(1, "SELECT now() FROM system.local", &params, nil); err != nil {
				t.Fatal(err)
			}
			qbase := newFramer(nil, protoVersion4)
			if err := qbase.writeQueryFrame(1, "SELECT now() FROM system.local", &params, nil); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(q.buf, qbase.buf) {
				t.Fatal("V2 changed unprepared QUERY wire format")
			}
		})
	}
}

func TestTabletVersionBlockFromBoundValues(t *testing.T) {
	s := newSchemaEventTestSessionWithMock(&schemaDataMock{})
	defer s.Close()
	version := uint64(0xfedcba9876543210)
	s.metadataDescriber.AddTablet(v2Hint(t, version, tUUID(1)))
	s.metadataDescriber.metadata.tabletsMetadata.Flush()
	q := &Query{routingInfo: &queryRoutingInfo{}}
	meta := &preparedMetadata{pkeyColumns: []int{1, 0}}
	values := []queryValues{{value: []byte("a")}, {value: []byte("b")}}
	for i := 0; i < 128; i++ {
		block := s.tabletVersionBlock(q, meta, values, "CaseKS", "CaseTable")
		if got, want := block&15, byte(version>>((block>>4)*4))&15; got != want {
			t.Fatalf("version byte %x does not sample %x", block, version)
		}
	}
	// A deterministic partitioner checks the actual composite encoding rather
	// than just checking a hash that falls inside an all-token tablet.
	q.routingInfo.partitioner = checkingPartitioner{t: t, key: []byte{0, 1, 'b', 0, 0, 1, 'a', 0}}
	s.tabletVersionBlock(q, meta, values, "CaseKS", "CaseTable")
}

type checkingPartitioner struct {
	t   *testing.T
	key []byte
}

func (p checkingPartitioner) Hash(key []byte) Token {
	p.t.Helper()
	if !bytes.Equal(key, p.key) {
		p.t.Fatalf("routing key %x, want %x", key, p.key)
	}
	return int64Token(1)
}
func (checkingPartitioner) Name() string             { return "test" }
func (checkingPartitioner) ParseString(string) Token { return int64Token(0) }

func TestTabletLeaderRouting(t *testing.T) {
	for _, tc := range []struct {
		name, mode, stmt                     string
		cl                                   Consistency
		versioned, leaderDown, missingLeader bool
		wantLeader                           bool
	}{
		{"strong read", "global", "SELECT * FROM t", Quorum, true, false, false, true},
		{"strong write", "global", "INSERT INTO t", Quorum, true, false, false, true},
		{"one write", "global", "INSERT INTO t", One, true, false, false, true},
		{"local one write", "global", "UPDATE t SET a=1", LocalOne, true, false, false, true},
		{"one read", "global", "SELECT * FROM t", One, true, false, false, false},
		{"commented read", "global", " /* note */ -- note\n SeLeCt * FROM t", LocalOne, true, false, false, false},
		{"eventual", "", "SELECT * FROM t", Quorum, true, false, false, false},
		{"unknown mode", "other", "SELECT * FROM t", Quorum, true, false, false, false},
		{"v1", "global", "SELECT * FROM t", Quorum, false, false, false, false},
		{"down leader", "global", "SELECT * FROM t", Quorum, true, true, false, false},
		{"unknown leader", "global", "SELECT * FROM t", Quorum, true, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"), ShuffleReplicas()).(*tokenAwareHostPolicy)
			policy.getKeyspaceName = func() string { return "CaseKS" }
			policy.getKeyspaceMetadata = func(string) (*KeyspaceMetadata, error) { return nil, ErrKeyspaceDoesNotExist }
			leader := &HostInfo{hostId: tUUID(1), dataCenter: "remote", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"-100"}}
			local := &HostInfo{hostId: tUUID(2), dataCenter: "local", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"100"}}
			if tc.leaderDown {
				leader.setState(NodeDown)
			}
			if !tc.missingLeader {
				policy.AddHost(leader)
			}
			policy.AddHost(local)
			policy.SetPartitioner("Murmur3Partitioner")
			s := newSchemaEventTestSessionWithMock(&schemaDataMock{})
			defer s.Close()
			s.tabletsRoutingV1 = true
			s.metadataDescriber.metadata.keyspaceMetadata.set("CaseKS", &KeyspaceMetadata{Name: "CaseKS", ConsistencyMode: tc.mode})
			tablet := v2Hint(t, 0, leader.hostId, local.hostId)
			if !tc.versioned {
				var err error
				tablet, err = tablets.NewTabletInfo("CaseKS", "CaseTable", math.MinInt64, math.MaxInt64, tablet.Replicas())
				if err != nil {
					t.Fatal(err)
				}
			}
			s.metadataDescriber.AddTablet(tablet)
			s.metadataDescriber.metadata.tabletsMetadata.Flush()
			q := &Query{stmt: tc.stmt, cons: tc.cl, session: s, routingKey: []byte("x"), routingInfo: &queryRoutingInfo{keyspace: "CaseKS", table: "CaseTable", partitioner: fixedInt64Partitioner(1)}}
			q.getKeyspace = func() string { return "CaseKS" }
			next := policy.Pick(q)
			first := next()
			if first == nil {
				t.Fatal("empty query plan")
			}
			if (first.Info() == leader) != tc.wantLeader {
				t.Fatalf("first host %s, leader wanted: %v", first.Info().HostID(), tc.wantLeader)
			}
			seen := map[*HostInfo]bool{first.Info(): true}
			for host := next(); host != nil; host = next() {
				if seen[host.Info()] {
					t.Fatal("host repeated in leader fallback plan")
				}
				seen[host.Info()] = true
			}
		})
	}
}

func TestKeyspaceConsistencyMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		err        error
		wantErr    bool
	}{
		{"global", "global", nil, false}, {"null", "", nil, false},
		{"missing table", "", &frm.ErrorFrame{Code: ErrCodeInvalid}, false},
		{"transient", "", errors.New("timeout"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"CaseKS": {}}}
			ctrl.iterFor = func(stmt string) *Iter {
				if !strings.Contains(stmt, "scylla_keyspaces") {
					return nil
				}
				if tc.err != nil {
					return &Iter{err: tc.err}
				}
				meta := resultMetadata{columns: []ColumnInfo{{Name: "consistency", TypeInfo: NewNativeType(protoVersion4, TypeText)}}, actualColCount: 1, colCount: 1}
				return makeIter(meta, [][]byte{[]byte(tc.mode)})
			}
			s := newSchemaEventTestSessionWithMock(ctrl)
			defer s.Close()
			s.control = &tabletSchemaControl{schemaDataMock: ctrl, conn: &Conn{tabletsRoutingV2: 1}}
			ks, err := getKeyspaceMetadata(s, "CaseKS")
			if (err != nil) != tc.wantErr {
				t.Fatalf("metadata error %v", err)
			}
			if err == nil && (ks.ConsistencyMode != tc.mode || ks.Clone().ConsistencyMode != tc.mode) {
				t.Fatalf("wrong consistency mode: %q", ks.ConsistencyMode)
			}
		})
	}
}

func TestTabletLeaderHonorsFallbackExclusions(t *testing.T) {
	s := newSchemaEventTestSessionWithMock(&schemaDataMock{})
	defer s.Close()
	host := &HostInfo{hostId: tUUID(1), dataCenter: "remote"}
	query := &Query{session: s}
	for _, fallback := range []HostSelectionPolicy{
		DCAwareRoundRobinPolicy("local", HostPolicyOptionDisableDCFailover),
		RackAwareRoundRobinPolicy("local", "rack", HostPolicyOptionDisableDCFailover),
		&excludingLeaderPolicy{HostSelectionPolicy: RoundRobinHostPolicy()},
	} {
		fallback.AddHost(host)
		policy := &tokenAwareHostPolicy{fallback: fallback}
		if policy.tabletLeaderAllowed(query, host) {
			t.Fatalf("leader bypassed exclusion in %T", fallback)
		}
	}
	policy := &tokenAwareHostPolicy{fallback: RoundRobinHostPolicy()}
	s.cfg.HostFilter = HostFilterFunc(func(*HostInfo) bool { return false })
	if policy.tabletLeaderAllowed(query, host) {
		t.Fatal("leader bypassed host filter")
	}
}

type excludingLeaderPolicy struct{ HostSelectionPolicy }

func (*excludingLeaderPolicy) Pick(ExecutableQuery) NextHost {
	return func() SelectedHost { return nil }
}

func TestTabletConsistencyRefreshInvalidatesOnlyAfterSuccess(t *testing.T) {
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"CaseKS": {}}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	s.control = &tabletSchemaControl{schemaDataMock: ctrl, conn: &Conn{tabletsRoutingV2: 1}}
	s.metadataDescriber.metadata.keyspaceMetadata.set("CaseKS", &KeyspaceMetadata{Name: "CaseKS", ConsistencyMode: "global"})
	s.metadataDescriber.AddTablet(v2Hint(t, 1, tUUID(1)))
	s.metadataDescriber.metadata.tabletsMetadata.Flush()
	ctrl.iterFor = func(stmt string) *Iter {
		if strings.Contains(stmt, "scylla_keyspaces") {
			return &Iter{err: errors.New("timeout")}
		}
		return nil
	}
	if err := s.metadataDescriber.refreshKeyspaceSchema("CaseKS"); err == nil {
		t.Fatal("transient failure ignored")
	}
	md, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("CaseKS")
	if md.ConsistencyMode != "global" || !hasTablets(s, "CaseKS", "CaseTable") {
		t.Fatal("failed refresh discarded mode or tablet")
	}
	ctrl.iterFor = nil // Missing consistency row means eventual.
	if err := s.metadataDescriber.refreshKeyspaceSchema("CaseKS"); err != nil {
		t.Fatal(err)
	}
	md, _ = s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace("CaseKS")
	if md.ConsistencyMode != "" || hasTablets(s, "CaseKS", "CaseTable") {
		t.Fatal("mode change retained obsolete leader hint")
	}
}

// tabletSchemaControl lets schema tests replace a negotiated control connection
// independently of the mocked system-table rows.
type tabletSchemaControl struct {
	*schemaDataMock
	conn *Conn
}

func (c *tabletSchemaControl) getConn() *connHost { return &connHost{conn: c.conn} }

func TestKeyspaceConsistencyFollowsControlReconnect(t *testing.T) {
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"CaseKS": {}}}
	consistencyQueries := 0
	ctrl.iterFor = func(stmt string) *Iter {
		if !strings.Contains(stmt, "scylla_keyspaces") {
			return nil
		}
		consistencyQueries++
		meta := resultMetadata{columns: []ColumnInfo{{Name: "consistency", TypeInfo: NewNativeType(protoVersion4, TypeText)}}, actualColCount: 1, colCount: 1}
		return makeIter(meta, [][]byte{[]byte("global")})
	}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	control := &tabletSchemaControl{schemaDataMock: ctrl, conn: &Conn{}}
	s.control = control
	check := func(wantMode string, wantQueries int) {
		t.Helper()
		ks, err := getKeyspaceMetadata(s, "CaseKS")
		if err != nil {
			t.Fatal(err)
		}
		if ks.ConsistencyMode != wantMode || consistencyQueries != wantQueries {
			t.Fatalf("mode %q, queries %d; want %q, %d", ks.ConsistencyMode, consistencyQueries, wantMode, wantQueries)
		}
	}
	check("", 0)
	control.conn = &Conn{tabletsRoutingV2: 1}
	check("global", 1)
	control.conn = &Conn{}
	check("", 1)
}
