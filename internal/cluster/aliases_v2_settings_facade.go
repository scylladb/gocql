// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"net"
)

type AddressPort = implEndpoint.AddressPort
type AddressTranslator = implEndpoint.AddressTranslator

type AddressTranslatorHostInfo = implEndpoint.AddressTranslatorHostInfo
type AddressTranslatorV2 = implEndpoint.AddressTranslatorV2
type ApplicationInfo = implEndpoint.ApplicationInfo

type Config = implSettings.Config
type ConvictionPolicy = implSettings.ConvictionPolicy
type DNSResolver = implEndpoint.DNSResolver

type HostDialer = implEndpoint.HostDialer
type HostInfo = implEndpoint.HostInfo

type LifecycleEvent = implSettings.LifecycleEvent

const MetadataDisabled = implSettings.MetadataDisabled
const MetadataFull = implSettings.MetadataFull
const MetadataMinimal = implSettings.MetadataMinimal

type MetadataOptions = implSettings.MetadataOptions

func NewStaticApplicationInfo(a0 string, a1 string, a2 string) *StaticApplicationInfo {
	return implEndpoint.NewStaticApplicationInfo(a0, a1, a2)
}

const NodeDown = implEndpoint.NodeDown
const NodeUp = implEndpoint.NodeUp

type Profile = implSettings.Profile
type ProfileHandle = implSettings.ProfileHandle

type ReconnectPolicy = implSettings.ReconnectPolicy

type RetryPolicy = implSettings.RetryPolicy

type ScyllaShardAwareDialer = implEndpoint.ScyllaShardAwareDialer
type ShardDialer = implEndpoint.ShardDialer
type SimpleConvictionPolicy = implSettings.SimpleConvictionPolicy

type StaticApplicationInfo = implEndpoint.StaticApplicationInfo
type StdLogger = logging.StdLogger

type contextBox = implEndpoint.InternalContextBox
type cqlProtocolExtension = implEndpoint.InternalCqlProtocolExtension

var defaultDnsResolver = implEndpoint.InternalDefaultDnsResolver

const defaultDriverName = implSettings.InternalDefaultDriverName

var defaultDriverVersion = implSettings.InternalDefaultDriverVersion

type defaultLogger = logging.InternalDefaultLogger

func defaultProfile() Profile { return implSettings.InternalDefaultProfile() }

type legacyDialer = implEndpoint.InternalLegacyDialer
type lwtAddMetadataMarkExt = implEndpoint.InternalLwtAddMetadataMarkExt

type nodeState = implEndpoint.InternalNodeState
type nopLogger = logging.InternalNopLogger

type rateLimitExt = implEndpoint.InternalRateLimitExt

func resolveInitialEndpoints(a0 DNSResolver, a1 []string, a2 int, a3 StdLogger) ([]*HostInfo, error) {
	return implEndpoint.InternalResolveInitialEndpoints(a0, a1, a2, a3)
}

type scyllaUseMetadataIDExt = implEndpoint.InternalScyllaUseMetadataIDExt

func snapshotConfig(a0 Config) Config { return implSettings.InternalSnapshotConfig(a0) }

type tabletsRoutingV1Ext = implEndpoint.InternalTabletsRoutingV1Ext

type translatedAddresses = implEndpoint.InternalTranslatedAddresses

func unixContact(a0 Config) (string, error) { return implSettings.InternalUnixContact(a0) }

type v2DNSResolver = implEndpoint.InternalV2DNSResolver

func validIpAddr(a0 net.IP) bool     { return implEndpoint.InternalValidIpAddr(a0) }
func validateConfig(a0 Config) error { return implSettings.InternalValidateConfig(a0) }
