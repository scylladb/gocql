//go:build unit
// +build unit

package gocql

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/tests"

	"github.com/scylladb/gocql/v2/internal/tests/mock"
)

func TestGetClusterPeerInfoZeroToken(t *testing.T) {
	t.Parallel()

	schema_version1 := ParseUUIDMust("af810386-a694-11ef-81fa-3aea73156247")

	peersRows := []map[string]any{
		{
			"data_center":     "datacenter1",
			"host_id":         ParseUUIDMust("b2035fd9-e0ca-4857-8c45-e63c00fb7c43"),
			"peer":            "127.0.0.3",
			"preferred_ip":    "127.0.0.3",
			"rack":            "rack1",
			"release_version": "3.0.8",
			"rpc_address":     "127.0.0.3",
			"schema_version":  schema_version1,
			"tokens":          []string{"-1296227678594315580994457470329811265"},
		},
		{
			"data_center":     "datacenter1",
			"host_id":         ParseUUIDMust("4b21ee4c-acea-4267-8e20-aaed5361a0dd"),
			"peer":            "127.0.0.2",
			"preferred_ip":    "127.0.0.2",
			"rack":            "rack1",
			"release_version": "3.0.8",
			"rpc_address":     "127.0.0.2",
			"schema_version":  schema_version1,
			"tokens":          []string{"-1129762924682054333"},
		},
		{
			"data_center":     "datacenter2",
			"host_id":         ParseUUIDMust("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3"),
			"peer":            "127.0.0.5",
			"preferred_ip":    "127.0.0.5",
			"rack":            "rack1",
			"release_version": "3.0.8",
			"rpc_address":     "127.0.0.5",
			"schema_version":  schema_version1,
			"tokens":          []string{},
		},
	}

	var logger StdLogger
	t.Run("OmitOneZeroTokenNode", func(t *testing.T) {
		peers, err := getPeersFromQuerySystemPeers(
			peersRows,
			9042,
			logger,
		)

		if err != nil {
			t.Fatalf("unable to get peers: %v", err)
		}
		tests.AssertEqual(t, "peers length", 2, len(peers))
	})

	t.Run("NoZeroTokenNodes", func(t *testing.T) {
		peersRows[2]["tokens"] = []string{"-1129762924682054333"}
		peers, err := getPeersFromQuerySystemPeers(
			peersRows,
			9042,
			logger,
		)

		if err != nil {
			t.Fatalf("unable to get peers: %v", err)
		}
		tests.AssertEqual(t, "peers length", 3, len(peers))
	})
}

type mockConnection struct{}

func (*mockConnection) Close() {}
func (*mockConnection) InternalExec(ctx context.Context, req frameBuilder, tracer Tracer, requestTimeout time.Duration) (*framer, error) {
	return nil, nil
}
func (*mockConnection) InternalAwaitSchemaAgreement(ctx context.Context) error { return nil }

var systemLocalResultMetadata = resultMetadata{
	Flags:          0,
	PagingState:    []byte{},
	ActualColCount: 11,
	Columns: []ColumnInfo{{
		Keyspace: "system",
		Table:    "local",
		Name:     "broadcast_address",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "cluster_name",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "data_center",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "host_id",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "listen_address",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "partitioner",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rack",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "release_version",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rpc_address",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "schema_version",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "tokens",
		TypeInfo: CollectionType{
			NativeType: cqltype.NewCustomType(protoVersion4, TypeSet, ""),
			Elem:       cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
		},
	}},
}

var systemPeersResultMetadata = resultMetadata{
	Flags:          0,
	PagingState:    []byte{},
	ActualColCount: 9,
	Columns: []ColumnInfo{{
		Keyspace: "system",
		Table:    "peers",
		Name:     "peer",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "data_center",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "host_id",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "preferred_ip",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "rack",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "release_version",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "rpc_address",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeInet, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "schema_version",
		TypeInfo: cqltype.NewCustomType(protoVersion4, TypeUUID, ""),
	}, {
		Keyspace: "system",
		Table:    "peers",
		Name:     "tokens",
		TypeInfo: CollectionType{
			NativeType: cqltype.NewCustomType(protoVersion4, TypeSet, ""),
			Elem:       cqltype.NewCustomType(protoVersion4, TypeVarchar, ""),
		},
	}},
}

func (*mockConnection) querySystem(ctx context.Context, query string, values ...any) *nativeResultFixture {
	// Column order matches the explicit SELECT in qrySystemLocal:
	// broadcast_address, cluster_name, data_center, host_id, listen_address, partitioner, rack, release_version, rpc_address, schema_version, tokens
	localData := []any{net.IPv4(192, 168, 100, 12), "", "datacenter1", ParseUUIDMust("045859a7-6b9f-4efd-a5e7-acd64a295e13"), net.IPv4(192, 168, 100, 12), "org.apache.cassandra.dht.Murmur3Partitioner", "rack1", "3.0.8", net.IPv4(192, 168, 100, 12), ParseUUIDMust("daf4df2c-b708-11ef-5c25-3004361afd71"), []string{}}
	// Column order matches the explicit SELECT in qrySystemPeersCassandra:
	// peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens
	peerData1 := []any{net.IPv4(192, 168, 100, 13), "datacenter1", ParseUUIDMust("b953309f-6e68-41f2-baf5-0e60da317a9c"), nil, "rack1", "3.0.8", net.IPv4(192, 168, 100, 13), ParseUUIDMust("b6ed5bde-b318-11ef-8f58-aeba19e31273"), []string{"-1032311531684407545", "-1112089412567859825"}}
	peerData2 := []any{net.IPv4(192, 168, 100, 14), "datacenter1", ParseUUIDMust("8269e111-ea38-44bd-a73f-9d3d12cfaf78"), nil, "rack1", "3.0.8", net.IPv4(192, 168, 100, 14), ParseUUIDMust("b6ed5bde-b318-11ef-8f58-aeba19e31273"), []string{}}

	if query == qrySystemLocal {
		return &nativeResultFixture{
			meta:    systemLocalResultMetadata,
			framer:  &mock.MockFramer{Data: marshalMetadataMust(systemLocalResultMetadata, localData)},
			numRows: 1,
		}
	} else if query == qrySystemPeersCassandra {
		return &nativeResultFixture{
			meta:    systemPeersResultMetadata,
			framer:  &mock.MockFramer{Data: append(marshalMetadataMust(systemPeersResultMetadata, peerData1), marshalMetadataMust(systemPeersResultMetadata, peerData2)...)},
			numRows: 2,
		}
	}
	return nil
}

func (*mockConnection) InternalGetIsSchemaV2() bool { return false }
func (*mockConnection) InternalSetSchemaV2(s bool)  {}
func (*mockConnection) InternalIsScyllaConn() bool  { return false }
func (*mockConnection) InternalGetScyllaSupported() ScyllaConnectionFeatures {
	return ScyllaConnectionFeatures{}
}

type mockControlConn struct{}

func (m *mockControlConn) querySystem(statement string, values ...any) (iter *nativeResultFixture) {
	return nil
}

func (m *mockControlConn) InternalReconnect() error {
	return nil
}

func (m *mockControlConn) InternalGetConn() *connHost {
	return &connHost{
		InternalConn: &mockConnection{},
		InternalHost: func() *HostInfo {
			opaqueStorageValue := new(HostInfo)
			return opaqueStorageValue
		}(),
	}
}

func (m *mockControlConn) InternalAwaitSchemaAgreement() error { return nil }
func (m *mockControlConn) query(statement string, values ...any) (iter *nativeResultFixture) {
	return nil
}
func (m *mockControlConn) InternalDiscoverProtocol(hosts []*HostInfo) (int, error) { return 0, nil }
func (m *mockControlConn) InternalConnect(hosts []*HostInfo) error                 { return nil }
func (m *mockControlConn) InternalClose()                                          {}
func (m *mockControlConn) InternalGetSession() *Session                            { return nil }

func marshalMetadataMust(metadata resultMetadata, data []any) [][]byte {
	if len(metadata.Columns) != len(data) {
		panic("metadata length mismatch")
	}
	res := make([][]byte, len(metadata.Columns))
	for id, col := range metadata.Columns {
		var err error
		value := data[id]
		res[id], err = encodeFixtureCell(col.TypeInfo, value)
		if err != nil {
			panic(fmt.Sprintf("unable to marshal column %d: %v", id, err))
		}
	}
	return res
}

type trackingRingConnection struct {
	iter      *nativeResultFixture
	schemaV2  bool
	scylla    bool
	lastQuery string
}

func (*trackingRingConnection) Close() {}
func (*trackingRingConnection) InternalExec(context.Context, frameBuilder, Tracer, time.Duration) (*framer, error) {
	return nil, nil
}
func (*trackingRingConnection) InternalAwaitSchemaAgreement(context.Context) error { return nil }

func (c *trackingRingConnection) querySystem(_ context.Context, q string, _ ...any) *nativeResultFixture {
	c.lastQuery = q
	return c.iter
}
func (c *trackingRingConnection) InternalGetIsSchemaV2() bool { return c.schemaV2 }
func (*trackingRingConnection) InternalSetSchemaV2(bool)      {}
func (c *trackingRingConnection) InternalIsScyllaConn() bool  { return c.scylla }
func (*trackingRingConnection) InternalGetScyllaSupported() ScyllaConnectionFeatures {
	return ScyllaConnectionFeatures{}
}

func TestMockGetHostsFromSystem(t *testing.T) {
	t.Parallel()

	r := &ringDescriber{InternalControl: &mockControlConn{}, InternalCfg: &clusterSettings{Port: 9042}}

	hosts, _, err := r.GetHostsFromSystem()
	if err != nil {
		t.Fatalf("unable to get hosts: %v", err)
	}

	// Zero-token hosts remain in topology and participate in schema agreement.
	tests.AssertEqual(t, "hosts length", 3, len(hosts))
	tests.AssertEqual(t, "host token length", 2, len(implEndpoint.HostInfoData(hosts[1]).InternalTokens))
}

func TestRingDescriberGetClusterPeerInfoClosesIter(t *testing.T) {
	t.Parallel()

	row := []any{
		net.IPv4(192, 168, 100, 13),
		"datacenter1",
		ParseUUIDMust("b953309f-6e68-41f2-baf5-0e60da317a9c"),
		nil,
		"rack1",
		"3.0.8",
		net.IPv4(192, 168, 100, 13),
		ParseUUIDMust("b6ed5bde-b318-11ef-8f58-aeba19e31273"),
		[]string{"-1032311531684407545"},
	}
	framer := &trackingMockFramer{
		MockFramer: mock.MockFramer{Data: marshalMetadataMust(systemPeersResultMetadata, row)},
	}
	r := &ringDescriber{InternalCfg: &clusterSettings{Port: 9042}}

	peers, err := r.InternalGetClusterPeerInfo(context.Background(), &trackingRingConnection{
		iter: &nativeResultFixture{
			meta:    systemPeersResultMetadata,
			framer:  framer,
			numRows: 1,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if !framer.released {
		t.Fatal("expected iterator framer to be released")
	}
}

func TestGetClusterPeerInfoQueryRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schemaV2  bool
		scylla    bool
		wantQuery string
	}{
		{"schema_v2", true, false, qrySystemPeersV2},
		{"scylla_peers", false, true, qrySystemPeers},
		{"cassandra_peers", false, false, qrySystemPeersCassandra},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &trackingRingConnection{
				schemaV2: tt.schemaV2,
				scylla:   tt.scylla,
			}
			r := &ringDescriber{InternalCfg: &clusterSettings{Port: 9042}}
			// iter is nil so getClusterPeerInfo returns errNoControl, but the query is still recorded
			_, _ = r.InternalGetClusterPeerInfo(context.Background(), conn)
			if conn.lastQuery != tt.wantQuery {
				t.Errorf("got query %q, want %q", conn.lastQuery, tt.wantQuery)
			}
		})
	}
}

func TestRing_AddHostIfMissing_Missing(t *testing.T) {
	t.Parallel()

	ring := &ringDescriber{}

	host := func() *HostInfo {
		opaqueStorageValuex := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = MustRandomUUID()
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = net.IPv4(1, 1, 1, 1)
		return opaqueStorageValuex
	}()
	h1, ok := ring.InternalAddHostIfMissing(host)
	if ok {
		t.Fatal("host was reported as already existing")
	} else if !h1.Equal(host) {
		t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
	} else if h1 != host {
		t.Fatalf("returned host same pointer: %p != %p", h1, host)
	}
}

func TestRing_AddHostIfMissing_Existing(t *testing.T) {
	t.Parallel()

	ring := &ringDescriber{}

	host := func() *HostInfo {
		opaqueStorageValuexx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = MustRandomUUID()
		implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.IPv4(1, 1, 1, 1)
		return opaqueStorageValuexx
	}()
	ring.InternalAddHostIfMissing(host)

	h2 := func() *HostInfo {
		opaqueStorageValuexxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalHostId = implEndpoint.HostInfoData(host).InternalHostId
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalConnectAddress = net.IPv4(2, 2, 2, 2)
		return opaqueStorageValuexxx
	}()

	h1, ok := ring.InternalAddHostIfMissing(h2)
	if !ok {
		t.Fatal("host was not reported as already existing")
	} else if !h1.Equal(host) {
		t.Fatalf("hosts not equal that are returned %v != %v", h1, host)
	} else if h1 != host {
		t.Fatalf("returned host same pointer: %p != %p", h1, host)
	}
}
