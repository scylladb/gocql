// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
import logging "github.com/scylladb/gocql/v2/internal/logging"
import implSettings "github.com/scylladb/gocql/v2/internal/settings"

import "crypto/tls"
import "net"

type AddressPort = implEndpoint.AddressPort
type AddressTranslator = implEndpoint.AddressTranslator

type ApplicationInfo = implEndpoint.ApplicationInfo
type AuthExchange = implSettings.AuthExchange
type AuthProvider = implSettings.AuthProvider

type Config = implSettings.Config

type DialedHost = implEndpoint.DialedHost

type HostDialer = implEndpoint.HostDialer
type HostInfo = implEndpoint.HostInfo

type PoolOptions = implSettings.PoolOptions

type ProtocolLimits = implSettings.ProtocolLimits

type ScyllaConnectionFeatures = implEndpoint.ScyllaConnectionFeatures

type StdLogger = logging.StdLogger
type UnsupportedSettingError = implSettings.UnsupportedSettingError

type cqlProtocolExtension = implEndpoint.InternalCqlProtocolExtension

type defaultLogger = logging.InternalDefaultLogger

func findCQLProtoExtByName(a0 []cqlProtocolExtension, a1 string) cqlProtocolExtension {
	return implEndpoint.InternalFindCQLProtoExtByName(a0, a1)
}

type legacyDialer = implEndpoint.InternalLegacyDialer
type lwtAddMetadataMarkExt = implEndpoint.InternalLwtAddMetadataMarkExt

const lwtAddMetadataMarkKey = implEndpoint.InternalLwtAddMetadataMarkKey

func parseCQLProtocolExtensions(a0 map[string][]string, a1 byte, a2 StdLogger) []cqlProtocolExtension {
	return implEndpoint.InternalParseCQLProtocolExtensions(a0, a1, a2)
}
func parseSupported(a0 map[string][]string, a1 StdLogger) ScyllaConnectionFeatures {
	return implEndpoint.InternalParseSupported(a0, a1)
}

const rateLimitError = implEndpoint.InternalRateLimitError

type rateLimitExt = implEndpoint.InternalRateLimitExt

const scyllaUseMetadataID = implEndpoint.InternalScyllaUseMetadataID

type scyllaUseMetadataIDExt = implEndpoint.InternalScyllaUseMetadataIDExt

const tabletsRoutingV1 = implEndpoint.InternalTabletsRoutingV1

type tabletsRoutingV1Ext = implEndpoint.InternalTabletsRoutingV1Ext

const tabletsRoutingV2 = implEndpoint.InternalTabletsRoutingV2

type translatedAddresses = implEndpoint.InternalTranslatedAddresses
type DefaultDialer = implEndpoint.DefaultDialer
type DialTarget = implEndpoint.DialTarget

func cloneV2TLSConfig(a0 *tls.Config) *tls.Config { return implSettings.InternalCloneV2TLSConfig(a0) }

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

func unixContact(a0 Config) (string, error) { return implSettings.InternalUnixContact(a0) }

type v2DNSResolver = implEndpoint.InternalV2DNSResolver

func validIpAddr(a0 net.IP) bool { return implEndpoint.InternalValidIpAddr(a0) }
