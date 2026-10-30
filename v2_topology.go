package gocql

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"
)

// Topology descriptors are decoded by the same canonical codecs as user rows.
// Optional fields accommodate the supported system.local/peers table versions;
// validation below distinguishes incomplete gossip from complete metadata.
type v2TopologyRow struct {
	id                                              UUID
	schema                                          UUID
	dc, rack, release, cluster, partitioner         string
	rpc, native, peer, broadcast, listen, preferred netip.Addr
	port                                            int
	tokens                                          []string
}

func v2DecodeTopology() RowDecoder[v2TopologyRow] {
	return v2SchemaRows[v2TopologyRow]{fields: []v2SchemaField[v2TopologyRow]{
		v2SchemaColumn("host_id", UUIDCodec, func(r *v2TopologyRow) *UUID { return &r.id }),
		v2SchemaColumn("schema_version", UUIDCodec, func(r *v2TopologyRow) *UUID { return &r.schema }),
		v2SchemaColumn("data_center", v2MetadataText{}, func(r *v2TopologyRow) *string { return &r.dc }),
		v2SchemaColumn("rack", v2MetadataText{}, func(r *v2TopologyRow) *string { return &r.rack }),
		v2SchemaColumn("release_version", v2MetadataText{}, func(r *v2TopologyRow) *string { return &r.release }),
		v2SchemaColumn("cluster_name", v2MetadataText{}, func(r *v2TopologyRow) *string { return &r.cluster }),
		v2SchemaColumn("partitioner", v2MetadataText{}, func(r *v2TopologyRow) *string { return &r.partitioner }),
		v2SchemaColumn("rpc_address", InetCodec, func(r *v2TopologyRow) *netip.Addr { return &r.rpc }),
		v2SchemaColumn("native_address", InetCodec, func(r *v2TopologyRow) *netip.Addr { return &r.native }),
		v2SchemaColumn("peer", InetCodec, func(r *v2TopologyRow) *netip.Addr { return &r.peer }),
		v2SchemaColumn("broadcast_address", InetCodec, func(r *v2TopologyRow) *netip.Addr { return &r.broadcast }),
		v2SchemaColumn("listen_address", InetCodec, func(r *v2TopologyRow) *netip.Addr { return &r.listen }),
		v2SchemaColumn("preferred_ip", InetCodec, func(r *v2TopologyRow) *netip.Addr { return &r.preferred }),
		v2SchemaColumn("native_port", v2MetadataInt{}, func(r *v2TopologyRow) *int { return &r.port }),
		v2SchemaColumn("tokens", v2MetadataStrings{}, func(r *v2TopologyRow) *[]string { return &r.tokens }),
	}}
}
func (r v2TopologyRow) host(defaultPort int) (*HostInfo, error) {
	ip := func(a netip.Addr) net.IP {
		if !a.IsValid() {
			return nil
		}
		return net.IP(a.AsSlice())
	}
	h := &HostInfo{hostId: r.id, schemaVersion: r.schema.String(), dataCenter: r.dc, rack: r.rack, clusterName: r.cluster, partitioner: r.partitioner, rpcAddress: ip(r.rpc), peer: ip(r.peer), broadcastAddress: ip(r.broadcast), listenAddress: ip(r.listen), preferredIP: ip(r.preferred), tokens: r.tokens, port: r.port}
	if r.native.IsValid() {
		h.rpcAddress = ip(r.native)
	}
	if h.port == 0 {
		h.port = defaultPort
	}
	if h.port <= 0 || h.port > 65535 {
		return nil, typeValidation("topology port", "invalid port %d", h.port)
	}
	if err := h.version.Set(r.release); err != nil {
		return nil, err
	}
	for _, token := range r.tokens {
		if _, err := strconv.ParseInt(token, 10, 64); err != nil {
			return nil, fmt.Errorf("host %s invalid token %q: %w", r.id, token, err)
		}
	}
	h.connectAddress = h.getDriverFacingIpAddressLocked()
	return h, nil
}
func readV2Topology(ctx context.Context, conn ConnInterface, stmt string, port int) ([]*HostInfo, error) {
	backend, ok := conn.(interface {
		executeTyped(context.Context, *attemptRequest) (*responsePage, error)
	})
	if !ok {
		return nil, typeValidation("topology query", "connection does not support typed requests")
	}
	timeout := timeForTopology(conn)
	execute := backend.executeTyped
	if c, ok := conn.(*Conn); ok {
		execute = c.executeV2System
	}
	rows, err := readV2SystemRowsVia(ctx, 4, timeout, execute, Statement[Unit, v2TopologyRow]{CQL: stmt, Params: NoParams, Rows: v2DecodeTopology()}, NewColumns(), Unit{})
	if err != nil {
		return nil, err
	}
	hosts := make([]*HostInfo, 0, len(rows))
	for _, row := range rows {
		h, err := row.host(port)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

func timeForTopology(conn ConnInterface) time.Duration {
	if c, ok := conn.(*Conn); ok {
		return c.getSystemRequestState().timeout
	}
	return time.Minute
}

// Resolve each explicitly configured endpoint through its own system.local.
// This establishes real identities without discovering additional peers.
func (s *Session) v2ContactIdentities(ctx context.Context, contacts []*HostInfo) ([]*HostInfo, error) {
	var hosts []*HostInfo
	current := s.control.getConn()
	seen := make(map[UUID]bool)
	for _, contact := range contacts {
		var conn *Conn
		if current != nil {
			if c, ok := current.conn.(*Conn); ok && contact.ConnectAddress().Equal(c.host.ConnectAddress()) && contact.Port() == c.host.Port() {
				conn = c
			}
		}
		temporary := conn == nil
		var err error
		if temporary {
			conn, err = s.dial(ctx, contact, s.controlConnConfig(), connErrorHandlerFn(func(c *Conn, _ error, _ bool) { c.Close() }))
			if err != nil {
				return nil, err
			}
		}
		rows, err := readV2Topology(ctx, conn, qrySystemLocal, s.cfg.Port)
		if temporary {
			conn.Close()
			conn.waitEventDispatch()
		}
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || rows[0].hostUUID().IsEmpty() {
			return nil, typeValidation("contact identity", "expected one actual local UUID")
		}
		contact.update(rows[0])
		if !seen[contact.hostUUID()] {
			hosts = append(hosts, contact)
			seen[contact.hostUUID()] = true
		}
	}
	return hosts, nil
}
