//go:build unit
// +build unit

package gocql

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"context"
	"crypto/tls"

	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"encoding/json"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/streams"
)

// TestControlConnConfigIsMarked pins the wiring that makes the control connection,
// and only the control connection, report the driver configuration. Every path
// which (re)establishes the long-lived control connection must go through
// Session.controlConnConfig. discoverProtocol's throwaway probe connections are a
// deliberate exception, documented on controlConnConfig itself.
func TestControlConnConfigIsMarked(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalConnCfg = &driverConnConfig{}
		return opaqueStorageValue
	}()

	cfg := implCluster.InternalSessionControlConnConfig(s)
	if !cfg.InternalIsControlConn {
		t.Error("expected the control connection config to be marked as such")
	}
	if !cfg.InternalDisableCoalesce {
		t.Error("expected the control connection config to disable write coalescing")
	}
	if implCluster.SessionData(s).InternalConnCfg.InternalIsControlConn || implCluster.SessionData(s).InternalConnCfg.InternalDisableCoalesce {
		t.Error("expected the session-wide connection config to be left untouched")
	}
}

// newTestReportSession builds a *Session suitable for exercising the report
// builders directly, without dialing anything. Only the fields the builders
// read (cfg, policy, logger) are populated.
func newTestReportSession(cfg clusterSettings, policy HostSelectionPolicy) *Session {
	return func() *Session {
		opaqueStorageValuex := new(Session)
		implCluster.SessionData(opaqueStorageValuex).InternalCfg = cfg
		implCluster.SessionData(opaqueStorageValuex).InternalPolicy = policy
		implCluster.SessionData(opaqueStorageValuex).InternalLogger = &defaultLogger{}
		return opaqueStorageValuex
	}()
}

func TestBuildRequestsReport_InFlightMax(t *testing.T) {
	tests := []struct {
		name               string
		maxRequestsPerConn int
		want               int
	}{
		// The reported number is what a connection can actually have
		// outstanding, capped only by the native ID range. Stream 0 is valid
		// and the configured generator capacity is exact.
		{"unset falls back to the streams default, with all nonnegative IDs", 0, 32768},
		{"an exact configured capacity is reported", 512, 512},
		{"a non-aligned configured capacity stays exact", 100, 100},
		{"a value above the CQL stream-id range is clamped", 40000, 32768},
		// Reporting remains defensive for invalid legacy snapshots even though
		// connection setup rejects values outside the native ID range.
		{"a value near the integer limit does not overflow", math.MaxInt, 32768},
		{"an invalid huge snapshot remains clamped", math.MaxInt - 1, 32768},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *newClusterSettings("127.0.0.1")
			cfg.MaxRequestsPerConn = tt.maxRequestsPerConn
			got := buildRequestsReport(&cfg)
			if got.InFlight.Max != tt.want {
				t.Errorf("in-flight.max = %d, want %d", got.InFlight.Max, tt.want)
			}
			// The runtime cannot exceed the native nonnegative stream-ID capacity.
			if got.InFlight.Max < 1 || got.InFlight.Max > maxReportableInFlight {
				t.Errorf("in-flight.max = %d, outside the native 1..%d capacity", got.InFlight.Max, maxReportableInFlight)
			}
		})
	}
}

// TestEffectiveInFlightMaxMatchesStreamGenerator pins the report against the
// generator it describes, so the two cannot drift: Available() on a freshly
// built generator is exactly the number of requests that can be in flight.
func TestEffectiveInFlightMaxMatchesStreamGenerator(t *testing.T) {
	for _, configured := range []int{0, 1, 63, 64, 100, 512, 1000, 32766, 32767, 32768} {
		gen := streams.New()
		if configured > 0 {
			gen = streams.NewLimited(configured)
		}
		want := min(gen.Available(), maxReportableInFlight)
		if got := effectiveInFlightMax(configured); got != want {
			t.Errorf("effectiveInFlightMax(%d) = %d, but the generator allows %d", configured, got, want)
		}
	}
}

// TestBuildLoadBalancingPolicyReport_NilPolicy pins that a nil policy is
// reported rather than panicking. Reporting must never prevent a connection
// from being established, and this runs on the goroutine that establishes it.
func TestBuildLoadBalancingPolicyReport_NilPolicy(t *testing.T) {
	got, ok := buildLoadBalancingPolicyReport(nil).(loadBalancingCustomReport)
	if !ok {
		t.Fatalf("expected a custom policy report, got %#v", buildLoadBalancingPolicyReport(nil))
	}
	if got.Name == "" {
		t.Error("expected a non-empty name, which the schema requires")
	}
}

func TestBuildSocketReport(t *testing.T) {
	// gocql has no configuration surface for any of these beyond what Go's net
	// package and clusterSettings.Validate already guarantee; this always
	// reports the built-in dialer's fixed behavior.
	want := socketReport{TCPNoDelay: true, KeepAlive: true, ReuseAddress: false}
	if got := buildSocketReport(); got != want {
		t.Errorf("buildSocketReport() = %+v, want %+v", got, want)
	}
}

// fakeReconnectionPolicy wraps a ReconnectionPolicy so its dynamic type name
// (used for the "custom" report branch) is distinct from any built-in policy.
type fakeReconnectionPolicy struct {
	ReconnectionPolicy
}

func TestBuildReconnectionPolicyReport(t *testing.T) {
	tests := []struct {
		name   string
		policy ReconnectionPolicy
		want   any
	}{
		{
			name:   "no reconnection reports as null",
			policy: &NoReconnectionPolicy{},
			want:   nil,
		},
		{
			name:   "constant",
			policy: &ConstantReconnectionPolicy{MaxRetries: 5, Interval: 2 * time.Second},
			want:   reconnectionConstantReport{Type: "constant", DelayMs: 2000, MaxAttempts: 5},
		},
		{
			name:   "exponential",
			policy: &ExponentialReconnectionPolicy{MaxRetries: 10, InitialInterval: time.Second, MaxInterval: 30 * time.Second},
			want:   reconnectionExponentialReport{Type: "exponential", BaseMs: 1000, MaxMs: 30000, MaxAttempts: 10},
		},
		{
			name:   "exponential with max below base reports the effective fallback max",
			policy: &ExponentialReconnectionPolicy{MaxRetries: 3, InitialInterval: 5 * time.Second, MaxInterval: time.Second},
			want: reconnectionExponentialReport{
				Type: "exponential", BaseMs: 5000,
				MaxMs:       (math.MaxInt16 * time.Second).Milliseconds(),
				MaxAttempts: 3,
			},
		},
		{
			// hostConnPool.InternalConnect loops `for i := 0; i < GetMaxRetries(); i++`,
			// so a non-positive limit attempts nothing at all. Reporting a policy
			// with max-attempts omitted would claim unlimited attempts.
			name:   "constant with no attempts reports as no reconnection",
			policy: &ConstantReconnectionPolicy{MaxRetries: 0, Interval: time.Second},
			want:   nil,
		},
		{
			name:   "constant with a negative attempt limit reports as no reconnection",
			policy: &ConstantReconnectionPolicy{MaxRetries: -1, Interval: time.Second},
			want:   nil,
		},
		{
			name:   "exponential with no attempts reports as no reconnection",
			policy: &ExponentialReconnectionPolicy{MaxRetries: 0, InitialInterval: time.Second, MaxInterval: 2 * time.Second},
			want:   nil,
		},
		{
			// time.Sleep returns immediately on a negative delay, so an immediate
			// retry is what it means; delay-ms is nonNegativeInteger.
			name:   "constant with a negative interval reports an immediate retry",
			policy: &ConstantReconnectionPolicy{MaxRetries: 3, Interval: -5 * time.Second},
			want:   reconnectionConstantReport{Type: "constant", DelayMs: 0, MaxAttempts: 3},
		},
		{
			name:   "a typed-nil policy reports as no reconnection instead of panicking",
			policy: (*ConstantReconnectionPolicy)(nil),
			want:   nil,
		},
		{
			// A nil policy cannot come from clusterSettings.Validate, but reporting
			// must not panic on one either.
			name:   "a nil policy reports as no reconnection",
			policy: nil,
			want:   nil,
		},
		{
			// base-ms and max-ms are both positiveInteger; getExponentialTime
			// substitutes 100ms/10s for a non-positive bound, so those are the
			// delays this policy really waits.
			name:   "exponential with unset intervals reports getExponentialTime's own defaults",
			policy: &ExponentialReconnectionPolicy{MaxRetries: 3},
			want:   reconnectionExponentialReport{Type: "exponential", BaseMs: 100, MaxMs: 10000, MaxAttempts: 3},
		},
		{
			name:   "custom",
			policy: &fakeReconnectionPolicy{ReconnectionPolicy: &NoReconnectionPolicy{}},
			want:   reconnectionCustomReport{Type: "custom", Name: "fakeReconnectionPolicy"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildReconnectionPolicyReport(tt.policy)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("buildReconnectionPolicyReport() mismatch:\n%s", diff)
			}
		})
	}
}

// fakeHostSelectionPolicy wraps a HostSelectionPolicy so its dynamic type
// name is distinct from any built-in policy, without reimplementing the
// (large) HostSelectionPolicy interface by hand.
type fakeHostSelectionPolicy struct {
	HostSelectionPolicy
}

func TestBuildLoadBalancingPolicyReport(t *testing.T) {
	tests := []struct {
		name   string
		policy HostSelectionPolicy
		want   any
	}{
		{
			name:   "token-aware over plain round-robin: shuffle, fallback to non-preferred nodes allowed",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy()),
			want:   loadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: true},
		},
		{
			// AvoidSlowReplicas orders candidates by outstanding request count
			// (partitionHealthy / HostInfo.IsBusy), which is what the schema's
			// adaptive-ordering group describes. The threshold it also sets is
			// deliberately not reported: the schema records signals, not
			// algorithm parameters.
			//
			// The threshold is passed as its own default so this test does not
			// mutate the MAX_IN_FLIGHT_THRESHOLD package global out from under
			// anything else.
			name:   "token-aware with AvoidSlowReplicas reports adaptive ordering",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy(), AvoidSlowReplicas(MAX_IN_FLIGHT_THRESHOLD)),
			want: loadBalancingTokenAwareReport{
				Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: true,
				AdaptiveOrdering: &adaptiveOrderingReport{Signals: []string{"in-flight-requests"}},
			},
		},
		{
			name:   "token-aware without AvoidSlowReplicas omits adaptive ordering",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy()),
			want: loadBalancingTokenAwareReport{
				Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: true,
			},
		},
		{
			name:   "token-aware with DontShuffleReplicas reports replica-set",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy(), DontShuffleReplicas()),
			want:   loadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: "replica-set", FallbackToNonPreferredNodes: true},
		},
		{
			name:   "token-aware over DC-aware fallback with fallback to non-preferred nodes disabled",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1", HostPolicyOptionDisableDCFailover)),
			want:   loadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: false},
		},
		{
			name:   "token-aware over rack-aware fallback with fallback to non-preferred nodes allowed",
			policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1")),
			want:   loadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: true},
		},
		{
			// rackAwareRR serves the local DC's other racks whether or not DC
			// failover is disabled, and those are outside the {dc1, rack1}
			// preference this same report names. See
			// TestFallbackToNonPreferredNodesMatchesQueryPlan.
			name:   "token-aware over rack-aware fallback still leaves the rack when DC failover is disabled",
			policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1", HostPolicyOptionDisableDCFailover)),
			want:   loadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: true},
		},
		{
			// NonLocalReplicasFallback makes Pick serve remote replicas itself,
			// before it consults the fallback at all.
			name:   "NonLocalReplicasFallback escapes the preference despite a confined fallback",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1", HostPolicyOptionDisableDCFailover), NonLocalReplicasFallback()),
			want:   loadBalancingTokenAwareReport{Type: "token-aware", LoadDistribution: "shuffle", FallbackToNonPreferredNodes: true},
		},
		{
			name:   "non-token-aware built-in reports as custom",
			policy: RoundRobinHostPolicy(),
			want:   loadBalancingCustomReport{Type: "custom", Name: "roundRobinHostPolicy"},
		},
		{
			name:   "custom",
			policy: &fakeHostSelectionPolicy{HostSelectionPolicy: RoundRobinHostPolicy()},
			want:   loadBalancingCustomReport{Type: "custom", Name: "fakeHostSelectionPolicy"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildLoadBalancingPolicyReport(tt.policy)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("buildLoadBalancingPolicyReport() mismatch:\n%s", diff)
			}
		})
	}
}

func TestBuildNodeLocationPreferenceReport(t *testing.T) {
	tests := []struct {
		name   string
		policy HostSelectionPolicy
		want   any
	}{
		{
			name:   "no DC/rack preference at all",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy()),
			want:   nil,
		},
		{
			name:   "DC preference behind token-awareness",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1")),
			want:   nodeLocationDCReport{Type: "dc", LocalDC: "dc1"},
		},
		{
			name:   "rack preference behind token-awareness",
			policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1")),
			want:   nodeLocationRackReport{Type: "rack", LocalDC: "dc1", LocalRack: "rack1"},
		},
		{
			name:   "DC preference reported independently of the load-balancing discriminant, when not wrapped in token-awareness",
			policy: DCAwareRoundRobinPolicy("dc1"),
			want:   nodeLocationDCReport{Type: "dc", LocalDC: "dc1"},
		},
		{
			// local-dc is nonEmptyString and required on the branch that names
			// it, so an empty datacenter cannot be reported -- and expresses no
			// preference anyway.
			name:   "an empty datacenter is no preference",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("")),
			want:   nil,
		},
		{
			name:   "an empty rack is no preference",
			policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "")),
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildNodeLocationPreferenceReport(tt.policy)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("buildNodeLocationPreferenceReport() mismatch:\n%s", diff)
			}
		})
	}
}

// TestBuildLoadBalancingReport_UnwrapsWrappers pins that a policy wrapped in
// SingleHostReadyPolicy is still described by what it wraps. The wrapper is
// transparent to routing, so reporting it as the policy would drop both the
// token-aware discriminant and the DC preference behind it.
func TestBuildLoadBalancingReport_UnwrapsWrappers(t *testing.T) {
	inner := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1"))
	want := buildLoadBalancingReport(inner)

	for _, tt := range []struct {
		name   string
		policy HostSelectionPolicy
	}{
		{"wrapped once", SingleHostReadyPolicy(inner)},
		{"wrapped twice", SingleHostReadyPolicy(SingleHostReadyPolicy(inner))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(want, buildLoadBalancingReport(tt.policy)); diff != "" {
				t.Errorf("expected the wrapped policy to report as the policy it wraps:\n%s", diff)
			}
		})
	}
}

func TestBuildControlPlaneReport(t *testing.T) {
	tests := []struct {
		name         string
		timeout      time.Duration
		agreement    time.Duration
		isScyllaConn bool
		want         controlPlaneReport
	}{
		{
			name:      "system query timeout disabled entirely omits the timeout object's contents",
			timeout:   0,
			agreement: 60 * time.Second,
			want: controlPlaneReport{
				Schema: controlPlaneSchemaReport{Agreement: schemaAgreementReport{TimeoutMs: 60000}},
			},
		},
		{
			name:         "non-Scylla connection only reports client-side-ms",
			timeout:      30 * time.Second,
			agreement:    60 * time.Second,
			isScyllaConn: false,
			want: controlPlaneReport{
				Queries: controlPlaneQueriesReport{System: systemQueriesReport{Timeout: systemQueriesTimeoutReport{
					ClientSideMs: ptr(int64(30000)),
				}}},
				Schema: controlPlaneSchemaReport{Agreement: schemaAgreementReport{TimeoutMs: 60000}},
			},
		},
		{
			name:         "Scylla connection also reports server-side-ms",
			timeout:      30 * time.Second,
			agreement:    60 * time.Second,
			isScyllaConn: true,
			want: controlPlaneReport{
				Queries: controlPlaneQueriesReport{System: systemQueriesReport{Timeout: systemQueriesTimeoutReport{
					ClientSideMs: ptr(int64(30000)),
					ServerSideMs: ptr(int64(30000)),
				}}},
				Schema: controlPlaneSchemaReport{Agreement: schemaAgreementReport{TimeoutMs: 60000}},
			},
		},
		{
			// driverConn.setSystemRequestTimeout truncates when it builds the
			// USING TIMEOUT clause, so this connection really is sent
			// "USING TIMEOUT 0ms". The schema cannot carry that, so the key is
			// omitted rather than rounded up to a clause never sent.
			name:         "sub-millisecond timeout omits server-side-ms but keeps the client-side floor",
			timeout:      500 * time.Microsecond,
			agreement:    60 * time.Second,
			isScyllaConn: true,
			want: controlPlaneReport{
				Queries: controlPlaneQueriesReport{System: systemQueriesReport{Timeout: systemQueriesTimeoutReport{
					ClientSideMs: ptr(int64(1)),
				}}},
				Schema: controlPlaneSchemaReport{Agreement: schemaAgreementReport{TimeoutMs: 60000}},
			},
		},
		{
			name:      "schema agreement of 0 is reported verbatim, not treated as unset",
			timeout:   0,
			agreement: 0,
			want: controlPlaneReport{
				Schema: controlPlaneSchemaReport{Agreement: schemaAgreementReport{TimeoutMs: 0}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *newClusterSettings("127.0.0.1")
			cfg.MetadataSchemaRequestTimeout = tt.timeout
			cfg.MaxWaitSchemaAgreement = tt.agreement
			got := buildControlPlaneReport(&cfg, tt.isScyllaConn)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("buildControlPlaneReport() mismatch:\n%s", diff)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// TestPositiveMillis pins the conversion every positiveInteger duration field
// in the report goes through. A duration under a millisecond truncates to 0,
// which the schema rejects (minimum 1), so it has to floor at 1 rather than be
// reported verbatim or dropped as if the timeout were disabled.
func TestPositiveMillis(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want *int64
	}{
		{"unset is omitted", 0, nil},
		{"negative is omitted", -time.Second, nil},
		{"sub-millisecond floors at the schema minimum", 500 * time.Microsecond, ptr(int64(1))},
		{"exactly one millisecond", time.Millisecond, ptr(int64(1))},
		{"whole milliseconds pass through", 11 * time.Second, ptr(int64(11000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, positiveMillis(tt.d)); diff != "" {
				t.Errorf("positiveMillis(%v) mismatch:\n%s", tt.d, diff)
			}
		})
	}
}

func TestBuildConnectionReport_ReadWriteTLS(t *testing.T) {
	cfg := *newClusterSettings("127.0.0.1")
	cfg.ReadTimeout = 0
	cfg.WriteTimeout = 0
	report := buildConnectionReport(&cfg)
	if report.Read != nil {
		t.Errorf("expected read to be omitted when ReadTimeout is 0, got %+v", report.Read)
	}
	if report.Write != nil {
		t.Errorf("expected write to be omitted when WriteTimeout is 0, got %+v", report.Write)
	}
	if report.TLS != nil {
		t.Errorf("expected tls to be omitted when SslOpts is nil, got %+v", report.TLS)
	}

	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 7 * time.Second
	cfg.SslOpts = &SslOptions{EnableHostVerification: true}
	report = buildConnectionReport(&cfg)
	if report.Read == nil || report.Read.TimeoutMs != 5000 {
		t.Errorf("expected read.timeout-ms 5000, got %+v", report.Read)
	}
	if report.Write == nil || report.Write.TimeoutMs != 7000 {
		t.Errorf("expected write.timeout-ms 7000, got %+v", report.Write)
	}
	if report.TLS == nil || !report.TLS.HostnameVerification {
		t.Errorf("expected tls.hostname-verification true, got %+v", report.TLS)
	}

	// HostDialer takes over connection setup entirely, so SslOpts is ignored
	// (see clusterSettings.SslOpts) and the effective TLS state is unknown.
	cfg.HostDialer = fakeHostDialer{}
	report = buildConnectionReport(&cfg)
	if report.TLS != nil {
		t.Errorf("expected tls to be omitted when HostDialer is set, got %+v", report.TLS)
	}
}

// TestBuildTLSReport_ReportsEffectiveHostnameVerification pins the report
// against what setupTLSConfig actually produces, rather than against
// SslOpts.EnableHostVerification, which is only one of the two inputs that
// decide it. The case that matters is a caller-supplied tls.Config that does
// not skip verification while EnableHostVerification is left false: hostnames
// are verified, and reporting the flag alone would claim the opposite.
func TestBuildTLSReport_ReportsEffectiveHostnameVerification(t *testing.T) {
	tests := []struct {
		name string
		opts *SslOptions
	}{
		{"no tls.Config, verification requested", &SslOptions{EnableHostVerification: true}},
		{"no tls.Config, verification not requested", &SslOptions{EnableHostVerification: false}},
		{"caller tls.Config verifies, flag not set", &SslOptions{Config: &tls.Config{InsecureSkipVerify: false}}},
		{"caller tls.Config skips, flag not set", &SslOptions{Config: &tls.Config{InsecureSkipVerify: true}}},
		{"caller tls.Config skips, flag overrides it back on", &SslOptions{Config: &tls.Config{InsecureSkipVerify: true}, EnableHostVerification: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *newClusterSettings("127.0.0.1")
			cfg.SslOpts = tt.opts
			if err := cfg.ValidateAndInitSSL(); err != nil {
				t.Fatal(err)
			}
			// The tls.Config the driver will actually hand to the dialer is the
			// only authority on whether hostnames get verified.
			actual := implCluster.InternalClusterSettingsGetActualTLSConfig(&cfg)
			if actual == nil {
				t.Fatal("expected ValidateAndInitSSL to produce a tls.Config")
			}
			want := !actual.InsecureSkipVerify

			got := buildTLSReport(&cfg)
			if got == nil {
				t.Fatal("expected a tls group when SslOpts is set")
			}
			if got.HostnameVerification != want {
				t.Errorf("hostname-verification = %v, but the effective tls.Config verifies = %v",
					got.HostnameVerification, want)
			}

			// The same config must be described identically before
			// ValidateAndInitSSL has run, which is how a hand-built Session in a
			// test reaches the reporter.
			pristine := *newClusterSettings("127.0.0.1")
			pristine.SslOpts = tt.opts
			if fallback := buildTLSReport(&pristine); fallback == nil || fallback.HostnameVerification != want {
				t.Errorf("without a resolved tls.Config: hostname-verification = %v, want %v", fallback, want)
			}
		})
	}
}

// fakeShardDialer is a HostDialer that can target a shard, like the
// *scyllaDialer the driver substitutes when no HostDialer is configured.
type fakeShardDialer struct{ fakeHostDialer }

func (fakeShardDialer) DialShard(ctx context.Context, host *HostInfo, shardID, nrShards int) (*DialedHost, error) {
	return nil, nil
}

// TestBuildConnectionReport_ShardAware covers both gates on reaching the
// shard-aware port. DisableShardAwarePort is the obvious one; the dialer's
// capability is the one easy to miss, since Session.dialWithoutObserver falls
// back to DialHost silently when HostDialer is not a ShardDialer.
// TestBuildConnectionNodePreferenceReport pins connection.node-preference to
// the only thing that narrows pool membership: HostFilter. Session.init drops
// every host cfg.filterHost rejects, so such a host never gets a pool, which is
// exactly what this group describes.
func TestBuildConnectionNodePreferenceReport(t *testing.T) {
	tests := []struct {
		name   string
		filter HostFilter
		want   any
	}{
		{name: "no filter narrows nothing", filter: nil, want: nil},
		{
			name:   "datacenter filter is the part of the cluster we hold connections to",
			filter: DataCenterHostFilter("dc1"),
			want:   nodeLocationDCReport{Type: "dc", LocalDC: "dc1"},
		},
		{
			// The deprecated spelling delegates to the same constructor, so it
			// must report identically rather than fall through as opaque.
			name:   "the deprecated spelling reports the same",
			filter: DataCentreHostFilter("dc1"),
			want:   nodeLocationDCReport{Type: "dc", LocalDC: "dc1"},
		},
		{
			// local-dc is nonEmptyString, and an empty datacenter expresses no
			// preference anyway.
			name:   "an empty datacenter is no preference",
			filter: DataCenterHostFilter(""),
			want:   nil,
		},
		{name: "accept-all states no location", filter: AcceptAllFilter(), want: nil},
		{name: "deny-all states no location", filter: DenyAllFilter(), want: nil},
		{
			// Selects by address, which the schema has no branch for.
			name: "whitelist filter is not a location", filter: WhiteListHostFilter("127.0.0.1"),
			want: nil,
		},
		{
			name:   "a caller-supplied filter is opaque",
			filter: HostFilterFunc(func(*HostInfo) bool { return true }),
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *newClusterSettings("127.0.0.1")
			cfg.HostFilter = tt.filter
			if diff := cmp.Diff(tt.want, buildConnectionReport(&cfg).NodePreference); diff != "" {
				t.Errorf("connection.node-preference mismatch:\n%s", diff)
			}
		})
	}
}

// TestDataCenterHostFilterStillFilters guards the behaviour the introspectable
// type replaced: it is reported by reading the datacenter back off the filter,
// which is only sound if the filter still accepts exactly that datacenter.
func TestDataCenterHostFilterStillFilters(t *testing.T) {
	filter := DataCenterHostFilter("dc1")
	for _, tt := range []struct {
		dc   string
		want bool
	}{{"dc1", true}, {"dc2", false}, {"", false}} {
		if got := filter.Accept(func() *HostInfo {
			opaqueStorageValuexx := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalDataCenter = tt.dc
			return opaqueStorageValuexx
		}()); got != tt.want {
			t.Errorf("Accept(dataCenter=%q) = %v, want %v", tt.dc, got, tt.want)
		}
	}
}

func TestBuildConnectionReport_ShardAware(t *testing.T) {
	tests := []struct {
		name       string
		disable    bool
		hostDialer HostDialer
		want       bool
	}{
		{name: "default: the substituted scyllaDialer is shard-aware", want: true},
		{name: "explicitly disabled", disable: true, want: false},
		{
			name:       "a HostDialer that cannot target a shard can never reach the port",
			hostDialer: fakeHostDialer{},
			want:       false,
		},
		{
			name:       "a HostDialer implementing ShardDialer can",
			hostDialer: fakeShardDialer{},
			want:       true,
		},
		{
			name:       "a shard-aware HostDialer is still gated by the flag",
			disable:    true,
			hostDialer: fakeShardDialer{},
			want:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *newClusterSettings("127.0.0.1")
			cfg.DisableShardAwarePort = tt.disable
			cfg.HostDialer = tt.hostDialer
			if got := buildConnectionReport(&cfg).Pool.ShardAware.Enabled; got != tt.want {
				t.Errorf("shard-aware.enabled = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestShardAwareEnabledMatchesDialPath pins the report against the branch it
// describes: dialWithoutObserver only takes the shard-aware path when the
// HostDialer satisfies ShardDialer, so the two must agree on every dialer.
func TestShardAwareEnabledMatchesDialPath(t *testing.T) {
	for _, hostDialer := range []HostDialer{fakeHostDialer{}, fakeShardDialer{}} {
		cfg := *newClusterSettings("127.0.0.1")
		cfg.HostDialer = hostDialer

		_, canDialShard := hostDialer.(ShardDialer)
		if got := shardAwareEnabled(&cfg); got != canDialShard {
			t.Errorf("%T: reported %v, but dialWithoutObserver would take the shard-aware path = %v",
				hostDialer, got, canDialShard)
		}
	}
}

type fakeHostDialer struct{}

func (fakeHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	return nil, nil
}

// TestConsistencyName pins query.defaults.consistency to the schema's enum,
// which covers every level Consistency defines. Only an unrecognized numeric
// value has no representation, and nothing rejects one today, so it must be
// omitted rather than emitted as a token no consumer can interpret.
func TestConsistencyName(t *testing.T) {
	for _, c := range []Consistency{
		Any, One, Two, Three, Quorum, All, LocalQuorum, EachQuorum, LocalOne, Serial, LocalSerial,
	} {
		if got := consistencyName(c); got != c.String() {
			t.Errorf("consistencyName(%v) = %q, want %q", c, got, c.String())
		}
	}
	if got := consistencyName(Consistency(0x42)); got != "" {
		t.Errorf("consistencyName(0x42) = %q, want it omitted: outside the schema enum", got)
	}
}

// TestBuildQueryDefaultsReport_SerialConsistencyIsSerialOnly guards the sibling
// key: serial-consistency's enum is SERIAL/LOCAL_SERIAL, so a non-serial level
// left in the field must not be reported there either.
func TestBuildQueryDefaultsReport_SerialConsistencyIsSerialOnly(t *testing.T) {
	for _, tt := range []struct {
		cons Consistency
		want string
	}{
		{0, ""},
		{Serial, "SERIAL"},
		{LocalSerial, "LOCAL_SERIAL"},
		{Quorum, ""},
	} {
		cfg := *newClusterSettings("127.0.0.1")
		cfg.SerialConsistency = tt.cons
		if got := buildQueryDefaultsReport(&cfg).SerialConsistency; got != tt.want {
			t.Errorf("SerialConsistency=%v: serial-consistency = %q, want %q", tt.cons, got, tt.want)
		}
	}
}

func TestBuildQueryDefaultsReport(t *testing.T) {
	cfg := *newClusterSettings("127.0.0.1")
	cfg.PageSize = 0
	cfg.SerialConsistency = 0
	cfg.Timeout = 0
	got := buildQueryDefaultsReport(&cfg)
	if got.Page != nil {
		t.Errorf("expected page to be omitted when PageSize is 0, got %+v", got.Page)
	}
	if got.SerialConsistency != "" {
		t.Errorf("expected serial-consistency to be omitted when unset, got %q", got.SerialConsistency)
	}
	if got.Request != nil {
		t.Errorf("expected request to be omitted entirely when Timeout is 0, got %+v", got.Request)
	}

	cfg.PageSize = 1000
	cfg.SerialConsistency = LocalSerial
	cfg.Timeout = 3 * time.Second
	got = buildQueryDefaultsReport(&cfg)
	if got.Page == nil || got.Page.Size != 1000 {
		t.Errorf("expected page.size 1000, got %+v", got.Page)
	}
	if got.SerialConsistency != "LOCAL_SERIAL" {
		t.Errorf("expected serial-consistency LOCAL_SERIAL, got %q", got.SerialConsistency)
	}
	if got.Request == nil || got.Request.TimeoutMs != 3000 {
		t.Errorf("expected request.timeout-ms 3000, got %+v", got.Request)
	}
}

// TestDriverConfigReportingStartupFrame checks what actually reaches the wire
// for the connections of a session pool.
func TestDriverConfigReportingStartupFrame(t *testing.T) {
	// Enough connections that the absence asserted below is a claim about the
	// pool rather than about a single sample: only the first connection is
	// opened during session initialization, so a test that did not set
	// NumConns and wait for the pool would routinely observe just that one.
	const connsPerSession = 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu       sync.Mutex
		configs  []string
		startups int
	)

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			if f.Header.Op != frm.OpStartup {
				return
			}
			// Consuming the frame body here is only safe because the fake
			// server does not read the body of a STARTUP request.
			opts := readStartupOptions(t, f)

			mu.Lock()
			defer mu.Unlock()
			startups++
			if cfg, ok := opts[driverConfigStartupKey]; ok {
				configs = append(configs, cfg)
			}
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = connsPerSession
	session, err := implCluster.InternalClusterSettingsOpen(cluster)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// A connection only joins the pool once its handshake has completed, which
	// is strictly after the server has run recvHook on its STARTUP, so waiting
	// here is what makes the whole pool visible to the snapshot below.
	if err := waitForPoolSize(session, connsPerSession, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Without this, the absence asserted below would hold just as well on a
	// run that observed no STARTUP frame at all.
	if startups < connsPerSession {
		t.Fatalf("expected the whole pool to be observed: got %d STARTUP frames for %d connections", startups, connsPerSession)
	}

	// The control connection is disabled by testCluster, so no connection
	// of this session may report DRIVER_CONFIG.
	if len(configs) != 0 {
		t.Errorf("expected no %s outside of the control connection, got %v", driverConfigStartupKey, configs)
	}
}

// TestDriverConfigReportingDisabled pins DisableDriverConfigReporting on the
// wire: with the option set, not even a connection dialed with the config that
// marks it as the control connection reports DRIVER_CONFIG, while SESSION_ID,
// which the option is documented not to affect, is still reported by every
// connection.
//
// Nothing else in the tree sets the option, so without this test the guard in
// newSessionCommon could be deleted, or inverted, and every test would still
// pass.
func TestDriverConfigReportingDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu       sync.Mutex
		startups []map[string]string
	)

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			if f.Header.Op != frm.OpStartup {
				return
			}
			// Consuming the frame body here is only safe because the fake
			// server does not read the body of a STARTUP request.
			opts := readStartupOptions(t, f)

			mu.Lock()
			defer mu.Unlock()
			startups = append(startups, opts)
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.DisableDriverConfigReporting = true
	session, err := implCluster.InternalClusterSettingsOpen(cluster)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if implCluster.SessionData(session).InternalDriverConfigReporter != nil {
		t.Error("expected a session with reporting disabled to hold no reporter")
	}

	if err := waitForPoolSize(session, 1, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	hosts := session.GetHosts()
	if len(hosts) == 0 {
		t.Fatal("expected at least one host in the session")
	}

	// Dial with the config that reports when the option is left at its
	// default, so that the option is the only reason DRIVER_CONFIG is absent
	// below rather than the connection simply not being a control connection.
	conn, err := implCluster.InternalSessionDial(session, implCluster.SessionData(session).InternalCtx, hosts[0], implCluster.InternalSessionControlConnConfig(session), connErrorHandlerFn(func(*driverConn, error, bool) {}))
	if err != nil {
		t.Fatal(err)
	}
	// This connection only exists to observe what reaches the wire, so it is
	// discarded right away without calling conn.finalizeConnection, the same
	// way controlConn.InternalDiscoverProtocol does for its throwaway connections.
	conn.Close()

	mu.Lock()
	defer mu.Unlock()

	// Without this the absence asserted below would hold just as well on a run
	// that observed no STARTUP frame at all.
	if len(startups) == 0 {
		t.Fatal("expected at least one STARTUP frame to be observed")
	}
	for i, opts := range startups {
		if config, ok := opts[driverConfigStartupKey]; ok {
			t.Errorf("STARTUP %d: expected no %s when reporting is disabled, got %q", i, driverConfigStartupKey, config)
		}
		if got := opts[sessionIDStartupKey]; got != session.ID() {
			t.Errorf("STARTUP %d: expected %s %q to be reported whatever the option is set to, got %q",
				i, sessionIDStartupKey, session.ID(), got)
		}
	}
}

// readStartupOptions decodes the string map carried by a STARTUP frame body.
// The framer read helpers panic on a truncated buffer and this runs on the fake
// server's read goroutine, so a malformed frame is turned into a test failure
// instead of taking the whole test binary down.
func readStartupOptions(t *testing.T, f *framer) (opts map[string]string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("malformed STARTUP frame: %v", r)
			opts = nil
		}
	}()

	opts = make(map[string]string)
	for n := f.ReadShort(); n > 0; n-- {
		key := f.ReadString()
		opts[key] = f.ReadString()
	}
	return opts
}

func TestDriverConfigReporterStartupOptions_ScyllaConnGatesServerSideMs(t *testing.T) {
	cfg := *newClusterSettings("127.0.0.1")
	s := newTestReportSession(cfg, TokenAwareHostPolicy(RoundRobinHostPolicy()))
	reporter := newDriverConfigReporter(s)

	for _, isScylla := range []bool{false, true} {
		opts := map[string]string{}
		reporter.InternalUpdateStartupOptions(opts, isScylla)

		var report driverConfigReport
		if err := json.Unmarshal([]byte(opts[driverConfigStartupKey]), &report); err != nil {
			t.Fatalf("isScyllaConn=%v: DRIVER_CONFIG did not decode: %v", isScylla, err)
		}

		timeout := report.ControlPlane.Queries.System.Timeout
		if timeout.ClientSideMs == nil {
			t.Errorf("isScyllaConn=%v: expected client-side-ms to be set", isScylla)
		}
		gotServerSide := timeout.ServerSideMs != nil
		if gotServerSide != isScylla {
			t.Errorf("isScyllaConn=%v: server-side-ms present = %v, want %v", isScylla, gotServerSide, isScylla)
		}
	}
}

func TestDriverConfigReportingDial(t *testing.T) {
	tests := []struct {
		name string
		// connConfig picks the driverConnConfig to dial with: the one every path
		// (re)establishing the control connection goes through, or the
		// session-wide one every pool connection is dialed with.
		connConfig    func(*Session) *driverConnConfig
		wantReporting bool
	}{
		{
			name:          "control connection",
			connConfig:    implCluster.InternalSessionControlConnConfig,
			wantReporting: true,
		},
		{
			name:          "regular connection",
			connConfig:    func(s *Session) *driverConnConfig { return implCluster.SessionData(s).InternalConnCfg },
			wantReporting: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var (
				mu       sync.Mutex
				configs  []string
				startups int
			)

			srv := newTestServerOpts{
				addr:     "127.0.0.1:0",
				protocol: defaultProto,
				recvHook: func(f *framer) {
					if f.Header.Op != frm.OpStartup {
						return
					}
					// Consuming the frame body here is only safe because the
					// fake server does not read the body of a STARTUP request.
					opts := readStartupOptions(t, f)

					mu.Lock()
					defer mu.Unlock()
					startups++
					if cfg, ok := opts[driverConfigStartupKey]; ok {
						configs = append(configs, cfg)
					}
				},
			}.newServer(t, ctx)
			defer srv.Stop()

			// disableControlConn, set by testCluster, keeps the session's own
			// control connection out of the way, so any DRIVER_CONFIG captured
			// below can only have come from the connection dialed by hand. A
			// single pool connection, waited for below, keeps the STARTUP count
			// deterministic: the rest of a larger pool is filled
			// asynchronously and could land a frame mid-test.
			cluster := testCluster(defaultProto, srv.Address)
			cluster.NumConns = 1
			session, err := implCluster.InternalClusterSettingsOpen(cluster)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()

			if err := waitForPoolSize(session, 1, 10*time.Second); err != nil {
				t.Fatal(err)
			}

			hosts := session.GetHosts()
			if len(hosts) == 0 {
				t.Fatal("expected at least one host in the session")
			}

			mu.Lock()
			startupsBeforeDial := startups
			mu.Unlock()

			conn, err := implCluster.InternalSessionDial(session, implCluster.SessionData(session).InternalCtx, hosts[0], tt.connConfig(session), connErrorHandlerFn(func(*driverConn, error, bool) {}))
			if err != nil {
				t.Fatal(err)
			}
			// This connection only exists to observe what reaches the wire, so
			// it is discarded right away without calling
			// conn.finalizeConnection, the same way
			// controlConn.InternalDiscoverProtocol does for its throwaway connections.
			conn.Close()

			mu.Lock()
			defer mu.Unlock()

			// The connection dialed above is the only one the assertion below
			// is about, so pin its STARTUP frame down: without this the
			// "regular connection" case would pass just as well on a frame
			// that never reached the server.
			if got := startups - startupsBeforeDial; got != 1 {
				t.Fatalf("expected the connection dialed by hand to send exactly one STARTUP frame, got %d", got)
			}
			if len(configs) == 0 {
				if tt.wantReporting {
					t.Fatalf("expected %s on a %s, got none", driverConfigStartupKey, tt.name)
				}
				return
			}
			if !tt.wantReporting {
				t.Fatalf("expected no %s on a %s, got %q", driverConfigStartupKey, tt.name, configs)
			}
			var report driverConfigReport
			if err := json.Unmarshal([]byte(configs[0]), &report); err != nil {
				t.Fatalf("%s did not decode as JSON: %v", driverConfigStartupKey, err)
			}
			if report.Version != driverConfigVersion {
				t.Errorf("expected version %d, got %d", driverConfigVersion, report.Version)
			}
		})
	}
}

func TestCustomPolicyNameIsAlwaysReportable(t *testing.T) {
	type namedPolicy struct{ RetryPolicy }

	for _, v := range []any{
		nil,
		(*SafeRetryPolicy)(nil),
		&SafeRetryPolicy{},
		&namedPolicy{},
		struct{ RetryPolicy }{},
	} {
		if got := customPolicyName(v); got == "" {
			t.Errorf("customPolicyName(%T) is empty, which the schema rejects", v)
		}
	}
}

func TestDriverConfigReporterStartupOptions(t *testing.T) {
	cfg := *newClusterSettings("127.0.0.1")
	s := newTestReportSession(cfg, TokenAwareHostPolicy(RoundRobinHostPolicy()))
	reporter := newDriverConfigReporter(s)

	opts := map[string]string{}
	reporter.InternalUpdateStartupOptions(opts, false)

	raw, ok := opts[driverConfigStartupKey]
	if !ok {
		t.Fatal("expected DRIVER_CONFIG to be set")
	}

	var report driverConfigReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		t.Fatalf("DRIVER_CONFIG did not decode as JSON: %v", err)
	}

	if report.Version != driverConfigVersion {
		t.Errorf("expected version %d, got %d", driverConfigVersion, report.Version)
	}
	if got := report.Connection.Connect.TimeoutMs; got == nil || *got != cfg.ConnectTimeout.Milliseconds() {
		t.Errorf("connect.timeout-ms = %v, want %d", got, cfg.ConnectTimeout.Milliseconds())
	}
	if got, want := report.Query.Defaults.Consistency, "QUORUM"; got != want {
		t.Errorf("query.defaults.consistency = %q, want %q", got, want)
	}
	if report.ControlPlane.Schema.Agreement.TimeoutMs != cfg.MaxWaitSchemaAgreement.Milliseconds() {
		t.Errorf("control-plane.schema.agreement.timeout-ms = %d, want %d",
			report.ControlPlane.Schema.Agreement.TimeoutMs, cfg.MaxWaitSchemaAgreement.Milliseconds())
	}
	// The default RetryPolicy is nil; the effective policy must be the package
	// fallback, not an absent/zero policy.
	policyMap, ok := report.Query.Retry.Policy.(map[string]any)
	if !ok || policyMap["type"] != "simple" {
		t.Errorf("query.retry.policy = %#v, want type simple (SafeRetryPolicy fallback)", report.Query.Retry.Policy)
	}
}
