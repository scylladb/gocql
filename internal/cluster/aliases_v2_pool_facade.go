// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type ConstantReconnectionPolicy = pool.ConstantReconnectionPolicy

type ExponentialReconnectionPolicy = pool.ExponentialReconnectionPolicy

type HostPoolInfo = pool.HostPoolInfo
type NoReconnectionPolicy = pool.NoReconnectionPolicy
type Partitioner = implEndpoint.Partitioner
type ReconnectionPolicy = pool.ReconnectionPolicy
type SelectedHost = implEndpoint.SelectedHost

type SetPartitioner = pool.SetPartitioner

type Token = implEndpoint.Token

type defaultConnPicker = pool.InternalDefaultConnPicker

type hostConnPool = pool.InternalHostConnPool

type int64ConnPicker = pool.InternalInt64ConnPicker
type int64Hasher = implEndpoint.InternalInt64Hasher
type int64Token = implEndpoint.InternalInt64Token

type murmur3Partitioner = implEndpoint.InternalMurmur3Partitioner

func newHostPool(a0 poolServices, a1 *HostInfo, a2 int, a3 string) *hostConnPool {
	return pool.InternalNewHostPool(a0, a1, a2, a3)
}

func newTokenRing(a0 string, a1 []*HostInfo) (*tokenRing, error) {
	return implEndpoint.InternalNewTokenRing(a0, a1)
}

type policyConnPool = pool.InternalPolicyConnPool

type poolServices = pool.InternalPoolServices

const scyllaCDCLogTableNameSuffix = implEndpoint.InternalScyllaCDCLogTableNameSuffix

type scyllaCDCPartitioner = implEndpoint.InternalScyllaCDCPartitioner

const scyllaCDCPartitionerFullName = implEndpoint.InternalScyllaCDCPartitionerFullName

type scyllaConnPicker = pool.InternalScyllaConnPicker
type scyllaDialer = implTransport.InternalScyllaDialer

type tokenRing = implEndpoint.InternalTokenRing
type typedConnPicker = pool.InternalTypedConnPicker
type v2TLSHostDialer = implTransport.InternalV2TLSHostDialer
