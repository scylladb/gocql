package gocql

import (
	"context"
	"crypto/tls"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"net"
)

type AddressPort = implEndpoint.AddressPort
type AddressTranslator = implEndpoint.AddressTranslator
type AddressTranslatorFunc = implEndpoint.AddressTranslatorFunc
type AddressTranslatorFuncV2 = implEndpoint.AddressTranslatorFuncV2
type AddressTranslatorHostInfo = implEndpoint.AddressTranslatorHostInfo
type AddressTranslatorV2 = implEndpoint.AddressTranslatorV2
type ApplicationInfo = implEndpoint.ApplicationInfo
type AuthExchange = implSettings.AuthExchange
type AuthProvider = implSettings.AuthProvider
type ClientRoutesOptions = implSettings.ClientRoutesOptions
type Config = implSettings.Config
type ConvictionPolicy = implSettings.ConvictionPolicy
type DNSResolver = implEndpoint.DNSResolver

func DefaultConfig(a0 ...string) Config { return implSettings.DefaultConfig(a0...) }

type DefaultDialer = implEndpoint.DefaultDialer
type DialTarget = implEndpoint.DialTarget
type DialedHost = implEndpoint.DialedHost
type Dialer = implEndpoint.Dialer
type DialerFunc = implEndpoint.DialerFunc

var ErrCannotFindHost = implEndpoint.ErrCannotFindHost
var ErrHostAlreadyExists = implEndpoint.ErrHostAlreadyExists

type HostDialer = implEndpoint.HostDialer
type HostInfo = implEndpoint.HostInfo
type HostInfoBuilder = implEndpoint.HostInfoBuilder
type IdentityOptions = implSettings.IdentityOptions

func IdentityTranslator() AddressTranslator { return implEndpoint.IdentityTranslator() }

type LifecycleEvent = implSettings.LifecycleEvent
type LifecycleObserver = implSettings.LifecycleObserver

const MetadataDisabled = implSettings.MetadataDisabled
const MetadataFull = implSettings.MetadataFull
const MetadataMinimal = implSettings.MetadataMinimal

type MetadataMode = implSettings.MetadataMode
type MetadataOptions = implSettings.MetadataOptions

func NewProfile(a0 Profile) (*ProfileHandle, error)   { return implSettings.NewProfile(a0) }
func NewSimpleDNSResolver(a0 bool) *SimpleDNSResolver { return implEndpoint.NewSimpleDNSResolver(a0) }
func NewStaticApplicationInfo(a0 string, a1 string, a2 string) *StaticApplicationInfo {
	return implEndpoint.NewStaticApplicationInfo(a0, a1, a2)
}

const NodeDown = implEndpoint.NodeDown
const NodeUp = implEndpoint.NodeUp

type Observer = implSettings.Observer

func PasswordAuth(a0 string, a1 string) AuthProvider { return implSettings.PasswordAuth(a0, a1) }

type PoolOptions = implSettings.PoolOptions
type Profile = implSettings.Profile
type ProfileHandle = implSettings.ProfileHandle
type ProtocolLimits = implSettings.ProtocolLimits
type ReconnectPolicy = implSettings.ReconnectPolicy
type RequestEvent = implSettings.RequestEvent
type RetryPolicy = implSettings.RetryPolicy
type ScyllaConnectionFeatures = implEndpoint.ScyllaConnectionFeatures

func ScyllaGetSourcePort(a0 context.Context) uint16 { return implEndpoint.ScyllaGetSourcePort(a0) }

type ScyllaHostFeatures = implEndpoint.ScyllaHostFeatures
type ScyllaShardAwareDialer = implEndpoint.ScyllaShardAwareDialer
type ShardDialer = implEndpoint.ShardDialer
type SimpleConvictionPolicy = implSettings.SimpleConvictionPolicy
type SimpleDNSResolver = implEndpoint.SimpleDNSResolver
type SpeculationPolicy = implSettings.SpeculationPolicy
type StaticApplicationInfo = implEndpoint.StaticApplicationInfo
type StdLogger = logging.StdLogger
type UnsupportedSettingError = implSettings.UnsupportedSettingError
type cassVersion = implEndpoint.InternalCassVersion

func cloneV2TLSConfig(a0 *tls.Config) *tls.Config { return implSettings.InternalCloneV2TLSConfig(a0) }

type contextBox = implEndpoint.InternalContextBox
type cqlProtocolExtension = implEndpoint.InternalCqlProtocolExtension

var defaultDnsResolver = implEndpoint.InternalDefaultDnsResolver

const defaultDriverName = implSettings.InternalDefaultDriverName

var defaultDriverVersion = implSettings.InternalDefaultDriverVersion

type defaultLogger = logging.InternalDefaultLogger

func defaultProfile() Profile { return implSettings.InternalDefaultProfile() }

type defaultReconnectPolicy = implSettings.InternalDefaultReconnectPolicy

func findCQLProtoExtByName(a0 []cqlProtocolExtension, a1 string) cqlProtocolExtension {
	return implEndpoint.InternalFindCQLProtoExtByName(a0, a1)
}

type legacyDialer = implEndpoint.InternalLegacyDialer
type lwtAddMetadataMarkExt = implEndpoint.InternalLwtAddMetadataMarkExt

const lwtAddMetadataMarkKey = implEndpoint.InternalLwtAddMetadataMarkKey
const mainPackage = implSettings.InternalMainPackage

func newLwtAddMetaMarkExt(a0 map[string][]string, a1 StdLogger) *lwtAddMetadataMarkExt {
	return implEndpoint.InternalNewLwtAddMetaMarkExt(a0, a1)
}
func newRateLimitExt(a0 map[string][]string, a1 StdLogger) *rateLimitExt {
	return implEndpoint.InternalNewRateLimitExt(a0, a1)
}
func newScyllaUseMetadataIDExt(a0 map[string][]string, a1 byte) *scyllaUseMetadataIDExt {
	return implEndpoint.InternalNewScyllaUseMetadataIDExt(a0, a1)
}
func newTabletsRoutingV1Ext(a0 map[string][]string) *tabletsRoutingV1Ext {
	return implEndpoint.InternalNewTabletsRoutingV1Ext(a0)
}
func newTimestampGenerator() func() int64 { return implSettings.InternalNewTimestampGenerator() }

type nodeState = implEndpoint.InternalNodeState
type nopLogger = logging.InternalNopLogger

func parseCQLProtocolExtensions(a0 map[string][]string, a1 byte, a2 StdLogger) []cqlProtocolExtension {
	return implEndpoint.InternalParseCQLProtocolExtensions(a0, a1, a2)
}
func parseSupported(a0 map[string][]string, a1 StdLogger) ScyllaConnectionFeatures {
	return implEndpoint.InternalParseSupported(a0, a1)
}

type passwordAuth = implSettings.InternalPasswordAuth
type passwordExchange = implSettings.InternalPasswordExchange

const rateLimitError = implEndpoint.InternalRateLimitError

type rateLimitExt = implEndpoint.InternalRateLimitExt

func resolveInitialEndpoint(a0 DNSResolver, a1 string, a2 int) ([]*HostInfo, error) {
	return implEndpoint.InternalResolveInitialEndpoint(a0, a1, a2)
}
func resolveInitialEndpoints(a0 DNSResolver, a1 []string, a2 int, a3 StdLogger) ([]*HostInfo, error) {
	return implEndpoint.InternalResolveInitialEndpoints(a0, a1, a2, a3)
}

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

const scyllaUseMetadataID = implEndpoint.InternalScyllaUseMetadataID

type scyllaUseMetadataIDExt = implEndpoint.InternalScyllaUseMetadataIDExt

func snapshotConfig(a0 Config) Config { return implSettings.InternalSnapshotConfig(a0) }

const tabletsRoutingV1 = implEndpoint.InternalTabletsRoutingV1

type tabletsRoutingV1Ext = implEndpoint.InternalTabletsRoutingV1Ext

const tabletsRoutingV2 = implEndpoint.InternalTabletsRoutingV2

type testLogger = logging.InternalTestLogger
type translatedAddresses = implEndpoint.InternalTranslatedAddresses

func unixContact(a0 Config) (string, error) { return implSettings.InternalUnixContact(a0) }

type v2DNSResolver = implEndpoint.InternalV2DNSResolver

func validConsistency(a0 Consistency) bool { return implSettings.InternalValidConsistency(a0) }
func validIpAddr(a0 net.IP) bool           { return implEndpoint.InternalValidIpAddr(a0) }
func validateConfig(a0 Config) error       { return implSettings.InternalValidateConfig(a0) }
func validateProfile(a0 Profile) error     { return implSettings.InternalValidateProfile(a0) }
