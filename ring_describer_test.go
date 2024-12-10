//go:build all || unit
// +build all unit

package gocql

import (
	"context"
	"net"
	"testing"

	"github.com/gocql/gocql/internal/tests/mock"
)

func TestGetClusterPeerInfoZeroToken(t *testing.T) {
	host_id1, _ := ParseUUID("b2035fd9-e0ca-4857-8c45-e63c00fb7c43")
	host_id2, _ := ParseUUID("4b21ee4c-acea-4267-8e20-aaed5361a0dd")
	host_id3, _ := ParseUUID("dfef4a22-b8d8-47e9-aee5-8c19d4b7a9e3")

	schema_version1, _ := ParseUUID("af810386-a694-11ef-81fa-3aea73156247")

	peersRows := []map[string]interface{}{
		{
			"data_center":     "datacenter1",
			"host_id":         host_id1,
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
			"host_id":         host_id2,
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
			"host_id":         host_id3,
			"peer":            "127.0.0.5",
			"preferred_ip":    "127.0.0.5",
			"rack":            "rack1",
			"release_version": "3.0.8",
			"rpc_address":     "127.0.0.5",
			"schema_version":  schema_version1,
			"tokens":          []string{},
		},
	}

	translateAddressPort := func(addr net.IP, port int) (net.IP, int) {
		return addr, port
	}

	var logger StdLogger
	t.Run("OmitOneZeroTokenNode", func(t *testing.T) {
		peers, err := getPeersFromQuerySystemPeers(
			peersRows,
			9042,
			translateAddressPort,
			logger,
		)

		if err != nil {
			t.Fatalf("unable to get peers: %v", err)
		}
		assertEqual(t, "peers length", 2, len(peers))
	})

	t.Run("NoZeroTokenNodes", func(t *testing.T) {
		peersRows[2]["tokens"] = []string{"-1129762924682054333"}
		peers, err := getPeersFromQuerySystemPeers(
			peersRows,
			9042,
			translateAddressPort,
			logger,
		)

		if err != nil {
			t.Fatalf("unable to get peers: %v", err)
		}
		assertEqual(t, "peers length", 3, len(peers))
	})
}

var framerLocalData = [][]byte{
	{108, 111, 99, 97, 108},              // key
	{67, 79, 77, 80, 76, 69, 84, 69, 68}, // bootstrapped
	{192, 168, 100, 12},                  // broadcast_address
	{},                                   // cluster_name
	{51, 46, 51, 46, 49},                 // cql_version
	{100, 97, 116, 97, 99, 101, 110, 116, 101, 114, 49}, // data_center
	{103, 81, 93, 7}, // gossip_generation
	{185, 83, 48, 159, 110, 104, 65, 242, 186, 245, 14, 96, 218, 49, 122, 156}, // host_id
	{192, 168, 100, 12}, // listen_address
	{52},                // native_protocol_version
	{111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 99, 97, 115, 115, 97, 110, 100, 114, 97, 46, 100, 104, 116, 46, 77, 117, 114, 109, 117, 114, 51, 80, 97, 114, 116, 105, 116, 105, 111, 110, 101, 114}, // partitioner
	{114, 97, 99, 107, 49}, // rack
	{51, 46, 48, 46, 56},   // release_version
	{192, 168, 100, 12},    // rpc_address
	{41, 234, 78, 26, 179, 2, 17, 239, 141, 211, 180, 122, 124, 230, 156, 22}, // schema_version
	{},           // supported_features
	{0, 0, 0, 0}, // tokens
	{0, 0, 0, 0}, // truncated_at
}

var framerPeersData = [][]byte{
	{192, 168, 100, 14}, // peer
	{100, 97, 116, 97, 99, 101, 110, 116, 101, 114, 49},                     // data_center
	{3, 169, 172, 30, 208, 56, 73, 81, 165, 63, 219, 158, 110, 136, 67, 55}, // host_id
	{},                     // preferred_ip
	{114, 97, 99, 107, 49}, // rack
	{51, 46, 48, 46, 56},   // release_version
	{192, 168, 100, 14},    // rpc_address
	{42, 44, 122, 176, 179, 12, 17, 239, 120, 46, 96, 161, 117, 228, 132, 98}, // schema_version
	{}, // supported_features
	{0, 0, 0, 2, 0, 0, 0, 20, 45, 49, 48, 56, 52, 54, 48, 55, 51, 48, 56, 48, 57, 57, 48, 50, 52, 48, 54, 57, 0, 0, 0, 20, 45, 49, 49, 52, 48, 49, 57, 52, 49, 50, 52, 54, 54, 51, 48, 56, 48, 54, 54, 53}, // tokens
	{192, 168, 100, 13}, // zero token peer
	{100, 97, 116, 97, 99, 101, 110, 116, 101, 114, 49},                        // data_center
	{130, 105, 225, 17, 234, 56, 68, 189, 167, 63, 157, 61, 18, 207, 175, 120}, // host_id
	{},                     // preferred_ip
	{114, 97, 99, 107, 49}, // rack
	{51, 46, 48, 46, 56},   // release_version
	{192, 168, 100, 13},    // rpc_address
	{9, 177, 215, 202, 179, 18, 17, 239, 85, 236, 47, 23, 19, 28, 10, 153}, // schema_version
	{},           // supported_features
	{0, 0, 0, 0}, // tokens
}

type mockConnection struct{}

func (*mockConnection) Close() {}
func (*mockConnection) exec(ctx context.Context, req frameBuilder, tracer Tracer) (*framer, error) {
	return nil, nil
}
func (*mockConnection) awaitSchemaAgreement(ctx context.Context) error     { return nil }
func (*mockConnection) executeQuery(ctx context.Context, qry *Query) *Iter { return nil }

var systemLocalResultMetadata = resultMetadata{
	flags:          0,
	pagingState:    []byte{},
	actualColCount: 18,
	columns: []ColumnInfo{{
		Keyspace: "system",
		Table:    "local",
		Name:     "key",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "bootstrapped",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "broadcast_address",
		TypeInfo: NativeType{typ: TypeInet},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "cluster_name",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "cql_version",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "data_center",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "gossip_generation",
		TypeInfo: NativeType{typ: TypeInt},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "host_id",
		TypeInfo: NativeType{typ: TypeUUID},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "listen_address",
		TypeInfo: NativeType{typ: TypeInet},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "native_protocol_version",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "partitioner",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rack",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "release_version",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rpc_address",
		TypeInfo: NativeType{typ: TypeInet},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "schema_version",
		TypeInfo: NativeType{typ: TypeUUID},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "supported_features",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "tokens",
		TypeInfo: CollectionType{
			NativeType: NativeType{typ: TypeSet},
			Elem:       NativeType{typ: TypeVarchar},
		},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "truncated_at",
		TypeInfo: CollectionType{
			NativeType: NativeType{typ: TypeMap},

			Key:  NativeType{typ: TypeUUID},
			Elem: NativeType{typ: TypeBlob},
		},
	}},
}

var systemPeersResultMetadata = resultMetadata{
	flags:          0,
	pagingState:    []byte{},
	actualColCount: 10,
	columns: []ColumnInfo{{
		Keyspace: "system",
		Table:    "local",
		Name:     "peer",
		TypeInfo: NativeType{typ: TypeInet},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "data_center",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "host_id",
		TypeInfo: NativeType{typ: TypeUUID},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "preferred_ip",
		TypeInfo: NativeType{typ: TypeInet},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rack",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "release_version",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "rpc_address",
		TypeInfo: NativeType{typ: TypeInet},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "schema_version",
		TypeInfo: NativeType{typ: TypeUUID},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "supported_features",
		TypeInfo: NativeType{typ: TypeVarchar},
	}, {
		Keyspace: "system",
		Table:    "local",
		Name:     "tokens",
		TypeInfo: CollectionType{
			NativeType: NativeType{proto: 4, typ: TypeSet},
			Elem:       NativeType{proto: 4, typ: TypeVarchar},
		},
	}},
}

func (*mockConnection) querySystem(ctx context.Context, query string) *Iter {
	if query == "SELECT * FROM system.local WHERE key='local'" {
		return &Iter{
			meta:    systemLocalResultMetadata,
			framer:  &mock.MockFramer{Data: framerLocalData},
			numRows: 1,
			next:    nil,
		}
	} else if query == "SELECT * FROM system.peers" {
		return &Iter{
			meta:    systemPeersResultMetadata,
			framer:  &mock.MockFramer{Data: framerPeersData},
			numRows: 2,
			next:    nil,
		}
	}
	return nil
}

func (*mockConnection) getIsSchemaV2() bool { return false }
func (*mockConnection) setSchemaV2(s bool)  {}
func (*mockConnection) query(ctx context.Context, statement string, values ...interface{}) (iter *Iter) {
	return nil
}
func (*mockConnection) getScyllaSupported() scyllaSupported { return scyllaSupported{} }

type mockControlConn struct{}

func (m *mockControlConn) getConn() *connHost {
	return &connHost{
		conn: &mockConnection{},
		host: &HostInfo{},
	}
}

func (m *mockControlConn) awaitSchemaAgreement() error                                { return nil }
func (m *mockControlConn) query(statement string, values ...interface{}) (iter *Iter) { return nil }
func (m *mockControlConn) discoverProtocol(hosts []*HostInfo) (int, error)            { return 0, nil }
func (m *mockControlConn) connect(hosts []*HostInfo) error                            { return nil }
func (m *mockControlConn) close()                                                     {}
func (m *mockControlConn) getSession() *Session                                       { return nil }

func TestGetHosts(t *testing.T) {
	r := &ringDescriber{control: &mockControlConn{}, cfg: &ClusterConfig{}}

	hosts, _, err := r.GetHosts()
	if err != nil {
		t.Fatalf("unable to get hosts: %v", err)
	}

	// local host and one of the peers are zero token so only one peer should be returned with 2 tokens
	assertEqual(t, "hosts length", 1, len(hosts))
	assertEqual(t, "host token length", 2, len(hosts[0].tokens))
}
