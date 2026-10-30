// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// Topology descriptors are decoded by the same canonical codecs as user rows.
// Optional fields accommodate the supported system.local/peers table versions;
// validation below distinguishes incomplete gossip from complete
type InternalV2TopologyRow struct {
	id                                              UUID
	schema                                          UUID
	dc, rack, release, cluster, partitioner         string
	rpc, native, peer, broadcast, listen, preferred netip.Addr
	InternalPort                                    int
	InternalTokens                                  []string
}

func InternalV2DecodeTopology() RowDecoder[InternalV2TopologyRow] {
	return InternalV2SchemaRows[InternalV2TopologyRow]{InternalFields: []InternalV2SchemaField[InternalV2TopologyRow]{
		InternalV2SchemaColumn("host_id", UUIDCodec, func(r *InternalV2TopologyRow) *UUID { return &r.id }),
		InternalV2SchemaColumn("schema_version", UUIDCodec, func(r *InternalV2TopologyRow) *UUID { return &r.schema }),
		InternalV2SchemaColumn("data_center", InternalV2MetadataText{}, func(r *InternalV2TopologyRow) *string { return &r.dc }),
		InternalV2SchemaColumn("rack", InternalV2MetadataText{}, func(r *InternalV2TopologyRow) *string { return &r.rack }),
		InternalV2SchemaColumn("release_version", InternalV2MetadataText{}, func(r *InternalV2TopologyRow) *string { return &r.release }),
		InternalV2SchemaColumn("cluster_name", InternalV2MetadataText{}, func(r *InternalV2TopologyRow) *string { return &r.cluster }),
		InternalV2SchemaColumn("partitioner", InternalV2MetadataText{}, func(r *InternalV2TopologyRow) *string { return &r.partitioner }),
		InternalV2SchemaColumn("rpc_address", InetCodec, func(r *InternalV2TopologyRow) *netip.Addr { return &r.rpc }),
		InternalV2SchemaColumn("native_address", InetCodec, func(r *InternalV2TopologyRow) *netip.Addr { return &r.native }),
		InternalV2SchemaColumn("peer", InetCodec, func(r *InternalV2TopologyRow) *netip.Addr { return &r.peer }),
		InternalV2SchemaColumn("broadcast_address", InetCodec, func(r *InternalV2TopologyRow) *netip.Addr { return &r.broadcast }),
		InternalV2SchemaColumn("listen_address", InetCodec, func(r *InternalV2TopologyRow) *netip.Addr { return &r.listen }),
		InternalV2SchemaColumn("preferred_ip", InetCodec, func(r *InternalV2TopologyRow) *netip.Addr { return &r.preferred }),
		InternalV2SchemaColumn("native_port", InternalV2MetadataInt{}, func(r *InternalV2TopologyRow) *int { return &r.InternalPort }),
		InternalV2SchemaColumn("tokens", InternalV2MetadataStrings{}, func(r *InternalV2TopologyRow) *[]string { return &r.InternalTokens }),
	}}
}
func (r InternalV2TopologyRow) InternalHost(defaultPort int) (*HostInfo, error) {
	ip := func(a netip.Addr) net.IP {
		if !a.IsValid() {
			return nil
		}
		return net.IP(a.AsSlice())
	}
	h := func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = r.id
		implEndpoint.HostInfoData(opaqueStorageValue).InternalSchemaVersion = r.schema.String()
		implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = r.dc
		implEndpoint.HostInfoData(opaqueStorageValue).InternalRack = r.rack
		implEndpoint.HostInfoData(opaqueStorageValue).InternalClusterName = r.cluster
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPartitioner = r.partitioner
		implEndpoint.HostInfoData(opaqueStorageValue).InternalRpcAddress = ip(r.rpc)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPeer = ip(r.peer)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalBroadcastAddress = ip(r.broadcast)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalListenAddress = ip(r.listen)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPreferredIP = ip(r.preferred)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalTokens = r.InternalTokens
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = r.InternalPort
		return opaqueStorageValue
	}()
	if r.native.IsValid() {
		implEndpoint.HostInfoData(h).InternalRpcAddress = ip(r.native)
	}
	if implEndpoint.HostInfoData(h).InternalPort == 0 {
		implEndpoint.HostInfoData(h).InternalPort = defaultPort
	}
	if implEndpoint.HostInfoData(h).InternalPort <= 0 || implEndpoint.HostInfoData(h).InternalPort > 65535 {
		return nil, typeValidation("topology port", "invalid port %d", implEndpoint.HostInfoData(h).InternalPort)
	}
	if err := implEndpoint.HostInfoData(h).InternalVersion.Set(r.release); err != nil {
		return nil, err
	}
	for _, token := range r.InternalTokens {
		if _, err := strconv.ParseInt(token, 10, 64); err != nil {
			return nil, fmt.Errorf("host %s invalid token %q: %w", r.id, token, err)
		}
	}
	implEndpoint.HostInfoData(h).InternalConnectAddress = implEndpoint.InternalHostInfoGetDriverFacingIpAddressLocked(h)
	return h, nil
}
