// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"time"
)

// driverConfigStartupKey is the STARTUP option carrying the JSON description
// of the effective driver configuration. The configuration is identical for
// every connection of a session, so it is only sent on the control connection
// to keep the other STARTUP frames small.
const InternalDriverConfigStartupKey = "DRIVER_CONFIG"

// driverConfigVersion is the major version of the reported configuration schema.
// Adding keys to the report is backwards compatible and does not bump it, only
// changing or removing the meaning of an existing key does.
const InternalDriverConfigVersion = 1

// maxDriverConfigSize bounds the size of the DRIVER_CONFIG STARTUP option.
//
// writeStartupFrame serializes STARTUP options through writeString, which
// writes a 16-bit length prefix with no bounds check: a value over 65535
// bytes would silently truncate that prefix modulo 65536 while still
// appending the full body, corrupting the frame and failing the handshake.
// Custom policy objects allow implementation-specific properties, so a report
// built from user-supplied policy data could in principle grow large.
// Enforcing a limit here keeps "reporting must never prevent a connection
// from being established" a property of this code rather than of the user's
// configuration. 32KiB is generous for a configuration report and matches the
// limit csharp-driver and java-driver use for the same report.
const InternalMaxDriverConfigSize = 32 * 1024

// driverConfigReport is the value sent under driverConfigStartupKey, conforming
// to docs/driver-config-schema.json. It groups the effective configuration the
// way the schema does: connection-level settings, control-plane (system query
// and schema agreement) settings, and query-execution defaults.
// Field order here (and in the other report structs) is chosen for field
// alignment, not readability: it decides how much of the struct the GC has to
// scan, which govet's fieldalignment check enforces. The JSON key order it
// produces is not the schema's order, which is fine -- object key order is
// not significant in JSON.
type InternalDriverConfigReport struct {
	Connection   InternalConnectionReport   `json:"connection"`
	ControlPlane InternalControlPlaneReport `json:"control-plane"`
	Query        InternalQueryReport        `json:"query"`
	Version      int                        `json:"version"`
}

// connectionReport is the top-level "connection" group in the schema.
type InternalConnectionReport struct {
	// NodePreference is which nodes the driver opens connections to at all, a
	// different claim from query.load-balancing.node-preference (which of those
	// a query may be routed to). Omitted when nothing narrows pool membership;
	// see buildConnectionNodePreferenceReport.
	NodePreference any `json:"node-preference,omitempty"`
	// Read and Write are omitted entirely when their timeout is 0 (disabled),
	// matching the schema's "absent when unset or not applicable" convention
	// for optional groups. write.coalescing and the sibling connection.heartbeat
	// group are never populated: the schema declares both as
	// additionalProperties:false with no properties, an explicit placeholder
	// for a future schema version, so there is nothing a v1 report could put
	// there even though gocql does have both features (WriteCoalesceWaitTime,
	// the control connection's heartBeat()).
	Read  *InternalReadWriteTimeoutReport `json:"read,omitempty"`
	Write *InternalReadWriteTimeoutReport `json:"write,omitempty"`
	// TLS is omitted when TLS is not configured, or when a HostDialer is set:
	// HostDialer takes over the entire connection setup and SslOpts is ignored
	// (see clusterSettings.SslOpts), so the effective TLS state is unknown.
	TLS          *InternalTlsReport              `json:"tls,omitempty"`
	Reconnection InternalReconnectionGroupReport `json:"reconnection"`
	Connect      InternalConnectReport           `json:"connect"`
	Requests     InternalRequestsReport          `json:"requests"`
	Pool         InternalConnectionPoolReport    `json:"pool"`
	Socket       InternalSocketReport            `json:"socket"`
}

// connectReport carries an optional timeout-ms: the schema does not list it
// under connect.required, so an empty "connect": {} is how a disabled connect
// timeout is reported. See positiveMillis.
type InternalConnectReport struct {
	TimeoutMs *int64 `json:"timeout-ms,omitempty"`
}

type InternalReadWriteTimeoutReport struct {
	TimeoutMs int64 `json:"timeout-ms"`
}

// requestsReport is connection.requests.
//
// Its orphaned group is never populated. Orphaned requests do exist here: a
// request whose caller stopped waiting keeps its stream reserved until the
// response arrives, precisely so the id is not reused (see StreamObserver's
// documentation, and the timeout branch of Conn.InternalExec, which stops waiting
// without releasing the stream). What does not exist is any bound on them --
// no setting caps how many a connection may accumulate, and nothing closes and
// replaces a connection because too many did. The group asks for that bound,
// so there is nothing to report, and 0 would assert the opposite: close the
// connection on the very first orphan.
//
// The schema makes orphaned optional for exactly this case, and gives its
// absence that meaning, so omitting it produces a document a consumer
// validating against the schema accepts. See
// TestSchemaPermitsAnAbsentOrphanBound, which fails if that ever changes.
type InternalRequestsReport struct {
	InFlight InternalInFlightReport `json:"in-flight"`
}

type InternalInFlightReport struct {
	Max int `json:"max"`
}

type InternalConnectionPoolReport struct {
	ShardAware InternalShardAwareReport `json:"shard-aware"`
}

type InternalShardAwareReport struct {
	Enabled bool `json:"enabled"`
}

type InternalSocketReport struct {
	TCPNoDelay   bool `json:"tcp-no-delay"`
	KeepAlive    bool `json:"keep-alive"`
	ReuseAddress bool `json:"reuse-address"`
}

type InternalReconnectionGroupReport struct {
	// Policy is one of reconnectionExponentialReport, reconnectionConstantReport,
	// reconnectionCustomReport, or nil (marshals to JSON null, the schema's "no
	// reconnection attempts will be made" branch).
	Policy any `json:"policy"`
}

// MaxAttempts is always present and always positive on both reports below: a
// policy that would attempt nothing is reported as no policy at all, so it
// never reaches these. The schema's "absent when attempts are unlimited" has
// no counterpart here -- GetMaxRetries returns a plain count, with no value
// standing for unlimited.
type InternalReconnectionExponentialReport struct {
	Type        string `json:"type"`
	BaseMs      int64  `json:"base-ms"`
	MaxMs       int64  `json:"max-ms"`
	MaxAttempts int    `json:"max-attempts"`
}

type InternalReconnectionConstantReport struct {
	Type        string `json:"type"`
	DelayMs     int64  `json:"delay-ms"`
	MaxAttempts int    `json:"max-attempts"`
}

type InternalReconnectionCustomReport struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type InternalTlsReport struct {
	HostnameVerification bool `json:"hostname-verification"`
}

// controlPlaneReport is the top-level "control-plane" group in the schema.
type InternalControlPlaneReport struct {
	Queries InternalControlPlaneQueriesReport `json:"queries"`
	Schema  InternalControlPlaneSchemaReport  `json:"schema"`
}

type InternalControlPlaneQueriesReport struct {
	System InternalSystemQueriesReport `json:"system"`
}

type InternalSystemQueriesReport struct {
	Timeout InternalSystemQueriesTimeoutReport `json:"timeout"`
}

type InternalSystemQueriesTimeoutReport struct {
	ClientSideMs *int64 `json:"client-side-ms,omitempty"`
	ServerSideMs *int64 `json:"server-side-ms,omitempty"`
}

type InternalControlPlaneSchemaReport struct {
	Agreement InternalSchemaAgreementReport `json:"agreement"`
}

type InternalSchemaAgreementReport struct {
	TimeoutMs int64 `json:"timeout-ms"`
}

// queryReport is the top-level "query" group in the schema.
//
// It has no speculative-execution group: gocql has no session/cluster-wide
// default speculative execution policy, only a per-Query/per-Batch opt-in via
// SetSpeculativeExecutionPolicy. Every session's default is
// NonSpeculativeExecution (disabled), and the schema says to omit the group
// entirely when speculative execution is disabled -- which is always true of
// the session-wide default this report describes.
type InternalQueryReport struct {
	Retry         InternalQueryRetryReport         `json:"retry"`
	LoadBalancing InternalQueryLoadBalancingReport `json:"load-balancing"`
	Defaults      InternalQueryDefaultsReport      `json:"defaults"`
}

type InternalQueryDefaultsReport struct {
	Page *InternalPageReport `json:"page,omitempty"`
	// Request is omitted entirely when Timeout is 0: the schema no longer
	// requires this group (only its inner timeout-ms was ever optional), so
	// there is no reason to send an empty {} object.
	Request *InternalRequestDefaultReport `json:"request,omitempty"`
	// Consistency carries one of the schema's nine non-serial levels, or is
	// omitted. Consistency's zero value (Any) is itself a real level, not a
	// sentinel for "unset", so absence here means only one thing: the
	// configured level is outside what the schema can express. See
	// consistencyName.
	Consistency string `json:"consistency,omitempty"`
	// SerialConsistency is omitted when 0, mirroring the ">0" check
	// clusterSettings.Validate itself uses to treat the field as unset.
	SerialConsistency string `json:"serial-consistency,omitempty"`
	Idempotence       bool   `json:"idempotence"`
	// ClientTimestamps reports DefaultTimestamp as configured, not additionally
	// gated on the negotiated protocol version: the client-timestamp flag is
	// only suppressed below protocol v3 (frame.go), a corner case not worth
	// threading the negotiated version through the report for.
	ClientTimestamps bool `json:"client-timestamps"`
}

type InternalPageReport struct {
	Size int `json:"size"`
}

type InternalRequestDefaultReport struct {
	TimeoutMs int64 `json:"timeout-ms"`
}

type InternalQueryRetryReport struct {
	// Policy is one of retryPolicySimpleReport, retryPolicyDowngradingReport, or
	// retryPolicyCustomReport.
	Policy any `json:"policy"`
	// Backoff is decoupled from Policy's discriminant on purpose: the schema
	// places it as a sibling of policy, not nested under a policy variant, so
	// a policy reported as "custom" (e.g. gocql's own
	// ExponentialBackoffRetryPolicy, which isn't one of the schema's built-in
	// retry types) can still usefully report the delay it's known to apply.
	// nil (omitted) when the effective policy has no such delay.
	Backoff any `json:"backoff,omitempty"`
}

type InternalRetryPolicySimpleReport struct {
	Type       string `json:"type"`
	MaxRetries int    `json:"max-retries"`
}

type InternalRetryPolicyDowngradingReport struct {
	Type string `json:"type"`
	// MaxRetries is DowngradingConsistencyRetryPolicy's own retry limit: its
	// Attempt method stops retrying once it has stepped through every entry of
	// ConsistencyLevelsToTry, so the slice's length is a concrete, always-known
	// retry limit -- not something to guess at or leave unset.
	MaxRetries int `json:"max-retries"`
}

type InternalRetryPolicyCustomReport struct {
	// MaxRetries is populated only for a custom policy whose retry limit the
	// driver can read. That is just the built-in
	// ExponentialBackoffRetryPolicy: the oldRetryPolicy interface exposes no
	// attempt limit, so a genuinely user-supplied policy has nothing to report
	// here.
	MaxRetries *int   `json:"max-retries,omitempty"`
	Type       string `json:"type"`
	Name       string `json:"name"`
}

type InternalRetryBackoffExponentialReport struct {
	Type   string `json:"type"`
	BaseMs int64  `json:"base-ms"`
	MaxMs  int64  `json:"max-ms"`
}

type InternalQueryLoadBalancingReport struct {
	// Policy is one of loadBalancingTokenAwareReport or loadBalancingCustomReport.
	Policy any `json:"policy"`
	// NodePreference is one of nodeLocationDCReport, nodeLocationRackReport, or
	// nil (omitted): gocql has no DC-inference feature, so the schema's
	// dc-auto/rack-auto branches are never produced.
	NodePreference any `json:"node-preference,omitempty"`
}

type InternalLoadBalancingTokenAwareReport struct {
	// AdaptiveOrdering is omitted when the policy orders candidates only by
	// the static rules above, which the schema spells "absent when adaptive
	// ordering is disabled".
	AdaptiveOrdering *InternalAdaptiveOrderingReport `json:"adaptive-ordering,omitempty"`
	Type             string                          `json:"type"`
	LoadDistribution string                          `json:"load-distribution"`
	// FallbackToNonPreferredNodes reports whether requests may fail over to
	// nodes outside of query.load-balancing.node-preference (DC or rack).
	FallbackToNonPreferredNodes bool `json:"fallback-to-non-preferred-nodes"`
}

// adaptiveOrderingReport names the runtime observations that reorder
// otherwise eligible candidates. The schema deliberately stops there: it
// records which signals feed the decision, not the algorithm or its
// thresholds, so AvoidSlowReplicas' MAX_IN_FLIGHT_THRESHOLD has nowhere to go
// and is not reported.
type InternalAdaptiveOrderingReport struct {
	Signals []string `json:"signals"`
}

type InternalLoadBalancingCustomReport struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type InternalNodeLocationDCReport struct {
	Type    string `json:"type"`
	LocalDC string `json:"local-dc"`
}

type InternalNodeLocationRackReport struct {
	Type      string `json:"type"`
	LocalDC   string `json:"local-dc"`
	LocalRack string `json:"local-rack"`
}

// driverConfigReporter builds the DRIVER_CONFIG STARTUP option describing a
// session's effective configuration to the  It is created once per
// session and shared by all of its connections, but only ever contributes to
// the STARTUP options of the control connection.
//
// The report is rebuilt on every control connection (re-)establishment
// rather than cached, since some of what it describes (e.g. whether the
// server-side-ms control-plane timeout applies) is only known once a
// connection has completed its OPTIONS/SUPPORTED exchange.
//
// The reporter holds the *Session itself and builds the report lazily, at
// first use, rather than at construction time: newDriverConfigReporter runs
// inside newSessionCommon before fields such as s.policy are assigned, so a
// report built eagerly would see a partially initialized Session. For the
// same reason, the host selection policy is read off s.policy, not
// s.cfg.PoolConfig.HostSelectionPolicy: the latter is never assigned the
// default policy that newSessionCommon applies.
type InternalDriverConfigReporter struct {
	session *Session
}

func InternalNewDriverConfigReporter(s *Session) *InternalDriverConfigReporter {
	return &InternalDriverConfigReporter{session: s}
}

// updateStartupOptions adds the DRIVER_CONFIG STARTUP option.
//
// Only the control connection's startup holds a reporter, so this is not the
// place that decides which connections report: see Conn.init.
//
// isScyllaConn is known by the time this runs: startupCoordinator.options
// parses the OPTIONS/SUPPORTED exchange, which sets Conn.scyllaSupported,
// before calling startup, which is what builds these STARTUP options.
//
// Reporting is best effort: it must never prevent a connection from being
// established, so a report that cannot be built is logged and left out.
func (r *InternalDriverConfigReporter) InternalUpdateStartupOptions(opts map[string]string, isScyllaConn bool) {
	report, err := r.InternalBuildReport(isScyllaConn)
	if err != nil {
		r.session.privateStorage.InternalLogger.Printf("gocql: unable to report driver configuration: %v", err)
		return
	}
	if len(report) > InternalMaxDriverConfigSize {
		r.session.privateStorage.InternalLogger.Printf("gocql: driver configuration report is %d bytes, exceeding the %d byte limit; omitting it", len(report), InternalMaxDriverConfigSize)
		return
	}
	opts[InternalDriverConfigStartupKey] = report
}

// buildReport returns the JSON configuration report of the session, marshalled
// fresh on every call so that it reflects the session's current state rather
// than a snapshot from whenever it was first requested.
func (r *InternalDriverConfigReporter) InternalBuildReport(isScyllaConn bool) (string, error) {
	cfg := &r.session.privateStorage.InternalCfg
	report := InternalDriverConfigReport{
		Version:      InternalDriverConfigVersion,
		Connection:   InternalBuildConnectionReport(cfg),
		ControlPlane: InternalBuildControlPlaneReport(cfg, isScyllaConn),
		Query:        InternalBuildQueryReport(r.session),
	}
	data, err := json.Marshal(report)
	return string(data), err
}

func InternalBuildConnectionReport(cfg *InternalClusterSettings) InternalConnectionReport {
	report := InternalConnectionReport{
		Connect:      InternalConnectReport{TimeoutMs: InternalPositiveMillis(cfg.ConnectTimeout)},
		Requests:     InternalBuildRequestsReport(cfg),
		Pool:         InternalConnectionPoolReport{ShardAware: InternalShardAwareReport{Enabled: InternalShardAwareEnabled(cfg)}},
		Socket:       InternalBuildSocketReport(),
		Reconnection: InternalReconnectionGroupReport{Policy: InternalBuildReconnectionPolicyReport(cfg.ReconnectionPolicy)},
	}
	if ms := InternalPositiveMillis(cfg.ReadTimeout); ms != nil {
		report.Read = &InternalReadWriteTimeoutReport{TimeoutMs: *ms}
	}
	if ms := InternalPositiveMillis(cfg.WriteTimeout); ms != nil {
		report.Write = &InternalReadWriteTimeoutReport{TimeoutMs: *ms}
	}
	report.TLS = InternalBuildTLSReport(cfg)
	report.NodePreference = InternalBuildConnectionNodePreferenceReport(cfg)
	return report
}

// buildConnectionNodePreferenceReport describes the part of the cluster the
// driver holds connections to.
//
// Only HostFilter narrows that: Session.init drops every host cfg.filterHost
// rejects, so such a host never gets a pool at all. The host selection policy's
// DC/rack preference is reported separately and does not belong here -- it
// orders and restricts routing among hosts that all still have pools.
//
// Of the filters shipped here only DataCenterHostFilter states a location.
// WhiteListHostFilter selects by address, which the schema has no branch for;
// AcceptAllFilter and DenyAllFilter state no location; and a caller-supplied
// HostFilterFunc is a closure this cannot see into. All of those report no
// preference.
func InternalBuildConnectionNodePreferenceReport(cfg *InternalClusterSettings) any {
	filter, ok := cfg.HostFilter.(InternalDataCenterHostFilter)
	if !ok || filter.dataCenter == "" {
		return nil
	}
	return InternalNodeLocationDCReport{Type: "dc", LocalDC: filter.dataCenter}
}

// buildTLSReport describes the TLS transport, or returns nil when there is
// nothing trustworthy to say: TLS is not configured, or a HostDialer has taken
// over connection setup entirely and SslOpts is ignored (see
// clusterSettings.SslOpts).
//
// hostname-verification is the effective setting rather than
// SslOpts.EnableHostVerification, which is only one of its two inputs.
// setupTLSConfig only ever forces verification *on* -- it flips
// InsecureSkipVerify off when the caller asked to skip verification and set
// EnableHostVerification, and never the reverse. So a caller-supplied
// tls.Config with InsecureSkipVerify left false verifies the hostname even
// though EnableHostVerification is false, and reporting that flag alone
// inverts the truth for the most ordinary way of configuring TLS.
func InternalBuildTLSReport(cfg *InternalClusterSettings) *InternalTlsReport {
	if cfg.SslOpts == nil || cfg.HostDialer != nil {
		return nil
	}
	// The config setupTLSConfig actually produced, once ValidateAndInitSSL has
	// run -- which is every session, but not a Session assembled by hand in a
	// test.
	if actual := InternalClusterSettingsGetActualTLSConfig(cfg); actual != nil {
		return &InternalTlsReport{HostnameVerification: !actual.InsecureSkipVerify}
	}
	verify := cfg.SslOpts.EnableHostVerification
	if cfg.SslOpts.Config != nil {
		verify = !cfg.SslOpts.Config.InsecureSkipVerify || cfg.SslOpts.EnableHostVerification
	}
	return &InternalTlsReport{HostnameVerification: verify}
}

// positiveMillis renders d for a schema field declared positiveInteger
// (minimum 1).
//
// It returns nil, so the key is omitted, when nothing is configured. Otherwise
// it floors at 1ms: durations under a millisecond truncate to 0, which the
// schema rejects, and a sub-millisecond timeout is still a timeout -- reporting
// the schema minimum is truer than either a 0 no consumer will accept or an
// absent key that would read as "disabled".
//
// Fields where 0 is itself meaningful (schema.agreement.timeout-ms, the
// constant reconnection policy's delay-ms) are nonNegativeInteger in the schema
// and must not go through here: they report their zero verbatim.
func InternalPositiveMillis(d time.Duration) *int64 {
	if d <= 0 {
		return nil
	}
	ms := max(d.Milliseconds(), 1)
	return &ms
}

// isNilPolicy reports whether v is nil, or a non-nil interface holding a nil
// pointer.
//
// A type switch routes only an untyped nil to `case nil`; a policy held as a
// typed nil pointer matches its own branch instead and panics on the first
// field it reads. Reporting runs on the goroutine that establishes a
// connection and must never do that to it, so every switch over a
// caller-supplied policy checks this first.
func InternalIsNilPolicy(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// nonNegativeMillis renders d for a schema field declared nonNegativeInteger.
// A negative duration is normalized to 0 rather than reported: time.Sleep
// returns immediately on one, so an immediate retry is what it means, and the
// schema has no encoding for a negative delay.
func InternalNonNegativeMillis(d time.Duration) int64 {
	return max(d.Milliseconds(), 0)
}

func InternalBuildRequestsReport(cfg *InternalClusterSettings) InternalRequestsReport {
	return InternalRequestsReport{InFlight: InternalInFlightReport{Max: InternalEffectiveInFlightMax(cfg.MaxRequestsPerConn)}}
}

// defaultMaxStreams mirrors streams.New(), the generator Session.streamIDGenerator
// uses when MaxRequestsPerConn is left unset.
const InternalDefaultMaxStreams = 32768

// Native nonnegative signed-short stream IDs 0..32767 give 32768 slots. The
// reporting schema accepts any positive integer and can represent this exactly.
const InternalMaxReportableInFlight = 32768

// effectiveInFlightMax reports the exact configured runtime capacity.
func InternalEffectiveInFlightMax(configured int) int {
	if configured <= 0 {
		configured = InternalDefaultMaxStreams
	}
	// Invalid legacy inputs above the native range cannot increase capacity.
	return min(configured, InternalMaxReportableInFlight)
}

// shardAwareEnabled reports whether the client can reach ScyllaDB's
// shard-aware port at all. Two independent things have to hold, and the
// schema has one boolean for both.
//
// DisableShardAwarePort must be unset: it makes scyllaConnPicker.NextShard
// return no shard, which sends every dial down the plain path.
//
// The dialer must also be able to target a shard. Session.dialWithoutObserver
// type-asserts HostDialer to ShardDialer and silently falls back to DialHost
// when the assertion fails, so a custom HostDialer that does not implement
// DialShard can never reach the shard-aware port however the flag is set.
// A nil HostDialer is capable: connConfig substitutes a *scyllaDialer, which
// implements ShardDialer.
func InternalShardAwareEnabled(cfg *InternalClusterSettings) bool {
	if cfg.DisableShardAwarePort {
		return false
	}
	if cfg.HostDialer == nil {
		return true
	}
	_, ok := cfg.HostDialer.(ShardDialer)
	return ok
}

// buildSocketReport describes what gocql's own built-in dialer does with a
// TCP socket. A user-supplied Dialer/HostDialer can set one up differently;
// the driver has no way to observe that, so the report always describes the
// built-in dialer's behavior, per the schema's own framing of these fields as
// "the effective value ... the OS/platform default".
func InternalBuildSocketReport() InternalSocketReport {
	return InternalSocketReport{
		// Go's net package unconditionally disables Nagle's algorithm for every
		// TCP connection it creates (net.newTCPConn calls setNoDelay(fd, true));
		// gocql never overrides that.
		TCPNoDelay: true,
		// cfg.SocketKeepalive can't be negative (see clusterSettings.Validate), and
		// net.Dialer.KeepAlive == 0 still means "enabled, OS default period" --
		// there is currently no way to disable keep-alive through gocql's
		// configuration.
		KeepAlive: true,
		// gocql never sets SO_REUSEADDR.
		ReuseAddress: false,
	}
}

// buildReconnectionPolicyReport describes the policy hostConnPool.InternalConnect will
// drive, or nil for the schema's null branch, "no reconnection attempts will
// be made".
//
// A policy whose retry limit is not positive takes that null branch whatever
// its type: connect loops `for i := 0; i < GetMaxRetries(); i++`, so it makes
// no attempt at all. Reporting it as a policy with max-attempts omitted would
// state the opposite, since the schema reads an absent max-attempts as
// unlimited attempts.
func InternalBuildReconnectionPolicyReport(rp ReconnectionPolicy) any {
	if InternalIsNilPolicy(rp) {
		return nil
	}
	switch p := rp.(type) {
	case *NoReconnectionPolicy:
		return nil
	case *ConstantReconnectionPolicy:
		if p.MaxRetries <= 0 {
			return nil
		}
		return InternalReconnectionConstantReport{
			Type:        "constant",
			DelayMs:     InternalNonNegativeMillis(p.Interval),
			MaxAttempts: p.MaxRetries,
		}
	case *ExponentialReconnectionPolicy:
		if p.MaxRetries <= 0 {
			return nil
		}
		maxInterval := p.MaxInterval
		if maxInterval < p.InitialInterval {
			// Matches ExponentialReconnectionPolicy.GetInterval's own fallback.
			maxInterval = math.MaxInt16 * time.Second
		}
		// GetInterval delegates to getExponentialTime, which substitutes its own
		// defaults for a non-positive bound, so report the window retries
		// actually use rather than a zero the schema rejects (both fields are
		// positiveInteger).
		base, maxDelay := InternalExponentialDelayWindow(p.InitialInterval, maxInterval)
		return InternalReconnectionExponentialReport{
			Type:        "exponential",
			BaseMs:      base,
			MaxMs:       maxDelay,
			MaxAttempts: p.MaxRetries,
		}
	default:
		return InternalReconnectionCustomReport{Type: "custom", Name: InternalCustomPolicyName(rp)}
	}
}

func InternalBuildControlPlaneReport(cfg *InternalClusterSettings, isScyllaConn bool) InternalControlPlaneReport {
	var timeout InternalSystemQueriesTimeoutReport
	if clientMs := InternalPositiveMillis(cfg.MetadataSchemaRequestTimeout); clientMs != nil {
		timeout.ClientSideMs = clientMs
		if isScyllaConn {
			// The USING TIMEOUT clause is ScyllaDB-only, so this key only ever
			// applies against Scylla -- and it carries whatever
			// Conn.setSystemRequestTimeout put in the clause, which
			// truncates rather than floors: a sub-millisecond timeout is sent as
			// "USING TIMEOUT 0ms". Derive it from the same conversion so the
			// report cannot claim a clause the connection never sends, and omit
			// the key when that conversion yields 0, which the schema's
			// positiveInteger cannot carry.
			//
			// client-side-ms is deliberately not aligned with this: it bounds a
			// Go-side deadline that a sub-millisecond duration expresses just
			// fine, so it keeps positiveMillis' floor.
			if serverMs := cfg.MetadataSchemaRequestTimeout.Milliseconds(); serverMs > 0 {
				timeout.ServerSideMs = &serverMs
			}
		}
	}
	return InternalControlPlaneReport{
		Queries: InternalControlPlaneQueriesReport{System: InternalSystemQueriesReport{Timeout: timeout}},
		Schema: InternalControlPlaneSchemaReport{
			// Always emitted, including 0: the schema documents 0 as meaningful
			// ("do not wait for agreement"), not as "unset".
			Agreement: InternalSchemaAgreementReport{TimeoutMs: cfg.MaxWaitSchemaAgreement.Milliseconds()},
		},
	}
}

func InternalBuildQueryReport(s *Session) InternalQueryReport {
	cfg := s.privateStorage.InternalCfg
	loadBalancing := InternalBuildLoadBalancingReport(s.privateStorage.InternalPolicy)
	runtime := InternalCurrentExecution(s)
	if runtime != nil && runtime.ProfileHandle() != nil {
		profile := runtime.ProfileHandle().Snapshot()
		cfg.privateStorage.InternalDefaultProfile = profile
		cfg.Consistency = profile.Consistency
		cfg.Timeout = profile.Timeout
		cfg.PageSize = int(profile.PageSize)
		cfg.SerialConsistency = 0
		if profile.SerialConsistency.Set {
			cfg.SerialConsistency = profile.SerialConsistency.Value
		}
		cfg.DefaultIdempotence = false
		cfg.DefaultTimestamp = runtime.ConfigSnapshot().TimestampGenerator != nil
		loadBalancing = InternalBuildTypedLoadBalancingReport(profile.Routing)
	}
	return InternalQueryReport{Defaults: InternalBuildQueryDefaultsReport(&cfg), Retry: InternalBuildQueryRetryReport(&cfg), LoadBalancing: loadBalancing}
}
func InternalBuildTypedLoadBalancingReport(policy RoutingPolicy) InternalQueryLoadBalancingReport {
	builtin, ok := policy.(*routingPolicy)
	if !ok || builtin == nil {
		return InternalQueryLoadBalancingReport{Policy: InternalLoadBalancingCustomReport{Type: "custom", Name: InternalCustomPolicyName(policy)}}
	}
	options := builtin.InternalOptions
	result := InternalQueryLoadBalancingReport{}
	if options.Datacenter != "" {
		if options.Rack != "" {
			result.NodePreference = InternalNodeLocationRackReport{Type: "rack", LocalDC: options.Datacenter, LocalRack: options.Rack}
		} else {
			result.NodePreference = InternalNodeLocationDCReport{Type: "dc", LocalDC: options.Datacenter}
		}
	}
	if !options.TokenAware {
		result.Policy = InternalLoadBalancingCustomReport{Type: "custom", Name: "routingPolicy"}
		return result
	}
	distribution := "replica-set"
	if options.ShuffleReplicas {
		distribution = "shuffle"
	}
	report := InternalLoadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: distribution, FallbackToNonPreferredNodes: options.AllowRemote}
	if options.Latency.Enabled {
		report.AdaptiveOrdering = &InternalAdaptiveOrderingReport{Signals: []string{"latency"}}
	}
	result.Policy = report
	return result
}

func InternalBuildQueryDefaultsReport(cfg *InternalClusterSettings) InternalQueryDefaultsReport {
	report := InternalQueryDefaultsReport{
		Consistency:      InternalConsistencyName(cfg.Consistency),
		Idempotence:      cfg.DefaultIdempotence,
		ClientTimestamps: cfg.DefaultTimestamp,
	}
	if cfg.PageSize > 0 {
		report.Page = &InternalPageReport{Size: cfg.PageSize}
	}
	if cfg.SerialConsistency.IsSerial() {
		report.SerialConsistency = cfg.SerialConsistency.String()
	}
	if ms := InternalPositiveMillis(cfg.Timeout); ms != nil {
		report.Request = &InternalRequestDefaultReport{TimeoutMs: *ms}
	}
	return report
}

// consistencyName renders c for query.defaults.consistency, and returns "" for
// a value the schema's enum cannot carry so the key is omitted.
//
// The enum covers every level Consistency defines, serial ones included, so
// only an unrecognized numeric value is unreportable -- Consistency.String()
// renders those as "UNKNOWN_CONS_0x..", which no consumer can interpret.
// Nothing rejects such a value today; the fix belongs in clusterSettings.Validate
// and is left to a follow-up.
func InternalConsistencyName(c Consistency) string {
	switch c {
	case Any, One, Two, Three, Quorum, All, LocalQuorum, EachQuorum, LocalOne, Serial, LocalSerial:
		return c.String()
	default:
		return ""
	}
}

func InternalBuildQueryRetryReport(cfg *InternalClusterSettings) InternalQueryRetryReport {
	policy := cfg.privateStorage.InternalDefaultProfile.Retry
	if policy == nil {
		policy = defaultProfile().Retry
	}
	return InternalQueryRetryReport{Policy: InternalBuildRetryPolicyReport(policy)}
}
func InternalBuildRetryPolicyReport(policy RetryPolicy) any {
	switch p := policy.(type) {
	case SafeRetryPolicy:
		attempts := p.MaxAttempts
		if attempts <= 0 {
			attempts = 3
		}
		return InternalRetryPolicySimpleReport{Type: "simple", MaxRetries: attempts - 1}
	default:
		return InternalRetryPolicyCustomReport{Type: "custom", Name: InternalCustomPolicyName(policy)}
	}
}

// exponentialDelayWindow reports the delay window getExponentialTime
// (policies.go) actually produces for a configured min/max pair, in
// milliseconds.
//
// Both the exponential retry backoff and the exponential reconnection policy
// route their delays through getExponentialTime, and both describe the result
// with schema fields declared positiveInteger where max-ms must be >= base-ms.
// Neither invariant survives the configured values untouched:
//
//   - a non-positive bound is not used as-is; getExponentialTime substitutes
//     100ms for the minimum and 10s for the maximum, so reporting the raw 0
//     would name a delay the policy never waits and one the schema rejects.
//   - a maximum below the minimum caps every delay at the maximum, so the
//     minimum is never reached. Reporting it as base-ms would both misdescribe
//     the policy and break max-ms >= base-ms.
//   - a sub-millisecond bound truncates to 0, so both are floored the same way
//     positiveMillis floors the report's other positiveInteger durations.
func InternalExponentialDelayWindow(configuredMin, configuredMax time.Duration) (baseMs, maxMs int64) {
	base := InternalEffectiveExpBackoff(configuredMin, 100*time.Millisecond)
	maxDelay := InternalEffectiveExpBackoff(configuredMax, 10*time.Second)
	if maxDelay < base {
		base = maxDelay
	}
	return max(base.Milliseconds(), 1), max(maxDelay.Milliseconds(), 1)
}

func InternalEffectiveExpBackoff(configured, fallback time.Duration) time.Duration {
	if configured <= 0 {
		return fallback
	}
	return configured
}

func InternalBuildLoadBalancingReport(policy HostSelectionPolicy) InternalQueryLoadBalancingReport {
	policy = InternalUnwrapHostSelectionPolicy(policy)
	return InternalQueryLoadBalancingReport{
		Policy:         InternalBuildLoadBalancingPolicyReport(policy),
		NodePreference: InternalBuildNodeLocationPreferenceReport(policy),
	}
}

func InternalBuildLoadBalancingPolicyReport(policy HostSelectionPolicy) any {
	tap, ok := policy.(*InternalTokenAwareHostPolicy)
	if !ok || InternalIsNilPolicy(policy) {
		// The schema's built-in load-balancing type only ever admits
		// "token-aware" (matching csharp-driver/java-driver's own narrowing);
		// any other policy -- including gocql's plain RoundRobinHostPolicy or
		// DCAwareRoundRobinPolicy used without token-awareness -- is reported as
		// custom.
		return InternalLoadBalancingCustomReport{Type: "custom", Name: InternalCustomPolicyName(policy)}
	}
	distribution := "replica-set"
	if tap.privateStorage.InternalShuffleReplicas {
		distribution = "shuffle"
	}
	report := InternalLoadBalancingTokenAwareReport{
		Type:                        "token-aware",
		LoadDistribution:            distribution,
		FallbackToNonPreferredNodes: InternalFallbackToNonPreferredNodesAllowed(tap),
	}
	if tap.privateStorage.avoidSlowReplicas {
		// AvoidSlowReplicas makes Pick partition the replica set so hosts at or
		// above MAX_IN_FLIGHT_THRESHOLD outstanding requests sort last: see
		// partitionHealthy, which orders by HostInfo.IsBusy. That is adaptive
		// ordering, and the count of outstanding requests is the one signal it
		// reads.
		report.AdaptiveOrdering = &InternalAdaptiveOrderingReport{Signals: []string{"in-flight-requests"}}
	}
	return report
}

// maxPolicyUnwrapDepth bounds the walk below. A wrapper cycle is not
// reachable through the constructors, but the report must never hang a
// connection over a policy someone assembled by hand.
const InternalMaxPolicyUnwrapDepth = 16

// unwrapHostSelectionPolicy peels driver-supplied wrappers off a host
// selection policy so the report describes the policy the caller actually
// configured rather than the wrapper around it.
//
// SingleHostReadyPolicy is the only such wrapper, and it is transparent to
// routing: it forwards every selection decision to the policy it holds and
// adds only a readiness signal. Left wrapped, both the load-balancing
// discriminant and the DC/rack preference behind it would be lost, which is
// everything the group exists to report.
func InternalUnwrapHostSelectionPolicy(p HostSelectionPolicy) HostSelectionPolicy {
	for range InternalMaxPolicyUnwrapDepth {
		wrapper, ok := p.(*InternalSingleHostReadyPolicy)
		if !ok || InternalIsNilPolicy(p) {
			return p
		}
		p = wrapper.HostSelectionPolicy
	}
	return p
}

// fallbackToNonPreferredNodesAllowed reports whether a request can reach a
// node outside the DC/rack preference this policy reports.
//
// Two separate things can let it. NonLocalReplicasFallback makes Pick serve
// replicas from remote tiers itself, out of its own `remote` buckets and
// before it ever delegates to the fallback policy, so requests escape the
// preference however the fallback is configured. Failing that it comes down to
// the fallback: only dcAwareRR and rackAwareRR confine requests at all, and
// only when DC failover is disabled. Any other fallback has no DC/rack notion,
// so nothing is confined.
func InternalFallbackToNonPreferredNodesAllowed(tap *InternalTokenAwareHostPolicy) bool {
	if tap.privateStorage.InternalNonLocalReplicasFallback {
		return true
	}
	if InternalIsNilPolicy(tap.privateStorage.InternalFallback) {
		return true
	}
	switch p := tap.privateStorage.InternalFallback.(type) {
	case *InternalDcAwareRR:
		// The preference is the whole local DC, and disabling failover confines
		// requests to exactly that.
		return !p.dcFailoverDisabled()
	case *InternalRackAwareRR:
		// The preference reported for this fallback names a rack as well as a
		// DC, but Pick always serves tier 1 -- the local DC's other racks --
		// before giving up, whether or not DC failover is disabled. Requests
		// therefore always reach nodes outside the reported preference;
		// disabling failover only stops them leaving the DC, which the schema's
		// single boolean cannot express.
		return true
	default:
		// No DC/rack notion at all, so nothing is confined.
		return true
	}
}

// buildNodeLocationPreferenceReport looks for a DC/rack preference on policy
// itself or, if policy is token-aware, on its fallback -- there is no separate
// session-level DC/rack setting, only what a DCAwareRoundRobinPolicy or
// RackAwareRoundRobinPolicy carries. Returns nil (omitted) for any other
// policy: the driver infers no datacenter, so the schema's dc-auto/rack-auto
// branches are never produced.
//
// An empty datacenter or rack is also reported as no preference. The schema
// declares both local-dc and local-rack nonEmptyString and requires them on
// the branch that names them, so an empty one cannot be reported -- and a
// policy constructed with an empty name expresses no preference anyway.
func InternalBuildNodeLocationPreferenceReport(policy HostSelectionPolicy) any {
	if InternalIsNilPolicy(policy) {
		return nil
	}
	target := policy
	if tap, ok := policy.(*InternalTokenAwareHostPolicy); ok {
		target = tap.privateStorage.InternalFallback
	}
	if InternalIsNilPolicy(target) {
		return nil
	}
	switch p := target.(type) {
	case *InternalDcAwareRR:
		if p.localDatacenter() == "" {
			return nil
		}
		return InternalNodeLocationDCReport{Type: "dc", LocalDC: p.localDatacenter()}
	case *InternalRackAwareRR:
		if p.localDatacenter() == "" || p.localRackName() == "" {
			return nil
		}
		return InternalNodeLocationRackReport{Type: "rack", LocalDC: p.localDatacenter(), LocalRack: p.localRackName()}
	default:
		return nil
	}
}

// customPolicyName returns a short, package-qualifier-free name for a
// user-supplied policy value -- e.g. *mypkg.MyPolicy becomes "MyPolicy" --
// mirroring how java-driver/csharp-driver report a policy's simple class name
// rather than its fully qualified one.
func InternalCustomPolicyName(v any) string {
	t := reflect.TypeOf(v)
	if t == nil {
		// A nil policy: reflect.TypeOf returns a nil Type, which would panic
		// below. The schema declares name nonEmptyString, so there is no way to
		// say "no name" -- and a report that omitted it would be as wrong as one
		// that crashed the connection reporting it.
		return "unknown"
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if name := t.Name(); name != "" {
		if strings.HasPrefix(t.PkgPath(), "github.com/scylladb/gocql/v2/internal/") && strings.HasPrefix(name, "Internal") {
			name = strings.TrimPrefix(name, "Internal")
			if name != "" {
				name = strings.ToLower(name[:1]) + name[1:]
			}
		}
		return name
	}
	// An unnamed type, such as an anonymous struct embedding the policy
	// interface. Name() is empty for those, which nonEmptyString rejects.
	return t.String()
}
