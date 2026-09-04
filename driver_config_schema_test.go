//go:build unit
// +build unit

package gocql

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// driverConfigSchemaPath is the normative schema the report is written
// against. It is shared with the other drivers, so it is validated as shipped:
// nothing here may edit it to accommodate this driver.
const driverConfigSchemaPath = "docs/driver-config-schema.json"

// loadDriverConfigSchema compiles the schema exactly as shipped. Nothing here
// may relax it: the point of validating is to prove a consumer that checks the
// payload accepts it, which a locally-adjusted schema would not establish.
func loadDriverConfigSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()

	f, err := os.Open(driverConfigSchemaPath)
	if err != nil {
		t.Fatalf("unable to open %s: %v", driverConfigSchemaPath, err)
	}
	defer f.Close()

	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatalf("unable to parse %s: %v", driverConfigSchemaPath, err)
	}

	c := jsonschema.NewCompiler()
	if err := c.AddResource(driverConfigSchemaPath, doc); err != nil {
		t.Fatalf("unable to add %s: %v", driverConfigSchemaPath, err)
	}
	sch, err := c.Compile(driverConfigSchemaPath)
	if err != nil {
		t.Fatalf("unable to compile %s: %v", driverConfigSchemaPath, err)
	}
	return sch
}

// TestSchemaPermitsAnAbsentOrphanBound guards the schema property the report
// depends on. Nothing in this driver bounds orphaned requests, so it has no
// value for connection.requests.orphaned; if that key were required again,
// every report this driver produces would fail validation, and the conformance
// test below would be the only thing standing between that and a release.
func TestSchemaPermitsAnAbsentOrphanBound(t *testing.T) {
	f, err := os.Open(driverConfigSchemaPath)
	if err != nil {
		t.Fatalf("unable to open %s: %v", driverConfigSchemaPath, err)
	}
	defer f.Close()

	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatalf("unable to parse %s: %v", driverConfigSchemaPath, err)
	}
	root, ok := doc.(map[string]any)
	if !ok {
		t.Fatalf("expected the schema root to be an object, got %T", doc)
	}
	defs, ok := root["$defs"].(map[string]any)
	if !ok {
		t.Fatal("expected the schema to have $defs")
	}
	requests, ok := defs["requests"].(map[string]any)
	if !ok {
		t.Fatal("expected the schema to have $defs.requests")
	}
	required, ok := requests["required"].([]any)
	if !ok {
		t.Fatal("expected $defs.requests.required to be an array")
	}
	for _, key := range required {
		if key == "orphaned" {
			t.Error("$defs.requests.required lists \"orphaned\", which this driver cannot report: " +
				"nothing bounds orphaned requests, so every report would fail validation")
		}
	}
}

// driverConfigCases spans the report's decision points: every optional group
// present and absent, every policy discriminant, and the boundary values that
// the schema constrains but nothing in the driver rejects.
func driverConfigCases() []struct {
	name         string
	cfg          func(*ClusterConfig)
	policy       HostSelectionPolicy
	isScyllaConn bool
} {
	return []struct {
		name         string
		cfg          func(*ClusterConfig)
		policy       HostSelectionPolicy
		isScyllaConn bool
	}{
		{name: "defaults"},
		{name: "defaults against scylla", isScyllaConn: true},
		{
			name: "every optional duration disabled",
			cfg: func(c *ClusterConfig) {
				c.ConnectTimeout = 0
				c.ReadTimeout = 0
				c.WriteTimeout = 0
				c.Timeout = 0
				c.MetadataSchemaRequestTimeout = 0
				c.MaxWaitSchemaAgreement = 0
				c.PageSize = 0
				c.FrameAssemblyTimeout = 0
			},
		},
		{
			name: "sub-millisecond durations",
			cfg: func(c *ClusterConfig) {
				c.ConnectTimeout = 500 * time.Microsecond
				c.ReadTimeout = 500 * time.Microsecond
				c.WriteTimeout = 500 * time.Microsecond
				c.Timeout = 500 * time.Microsecond
				c.MetadataSchemaRequestTimeout = 500 * time.Microsecond
				c.FrameAssemblyTimeout = 500 * time.Microsecond
			},
			isScyllaConn: true,
		},
		{
			name: "in-flight limit above the stream-id range",
			cfg:  func(c *ClusterConfig) { c.MaxRequestsPerConn = 40000 },
		},
		{
			name: "in-flight limit not a multiple of 64",
			cfg:  func(c *ClusterConfig) { c.MaxRequestsPerConn = 100 },
		},
		{
			name: "serial default consistency",
			cfg:  func(c *ClusterConfig) { c.Consistency = LocalSerial },
		},
		{
			name: "serial consistency set",
			cfg:  func(c *ClusterConfig) { c.SerialConsistency = LocalSerial },
		},
		{
			name: "no reconnection policy",
			cfg:  func(c *ClusterConfig) { c.ReconnectionPolicy = &NoReconnectionPolicy{} },
		},
		{
			name: "constant reconnection with no attempts",
			cfg: func(c *ClusterConfig) {
				c.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 0, Interval: time.Second}
			},
		},
		{
			name: "constant reconnection with a negative attempt limit",
			cfg: func(c *ClusterConfig) {
				c.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: -1, Interval: time.Second}
			},
		},
		{
			name: "nil reconnection policy",
			cfg:  func(c *ClusterConfig) { c.ReconnectionPolicy = nil },
		},
		{
			name: "typed-nil reconnection policy",
			cfg:  func(c *ClusterConfig) { c.ReconnectionPolicy = (*ConstantReconnectionPolicy)(nil) },
		},
		{
			name: "constant reconnection with a negative interval",
			cfg: func(c *ClusterConfig) {
				c.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 3, Interval: -5 * time.Second}
			},
		},
		{
			name: "typed-nil retry policy",
			cfg:  func(c *ClusterConfig) { c.RetryPolicy = (*SimpleRetryPolicy)(nil) },
		},
		{
			name:   "typed-nil load balancing fallback",
			policy: TokenAwareHostPolicy((*dcAwareRR)(nil)),
		},
		{
			name: "exponential reconnection with unset intervals",
			cfg:  func(c *ClusterConfig) { c.ReconnectionPolicy = &ExponentialReconnectionPolicy{MaxRetries: 3} },
		},
		{
			name: "exponential reconnection with max below base",
			cfg: func(c *ClusterConfig) {
				c.ReconnectionPolicy = &ExponentialReconnectionPolicy{MaxRetries: 3, InitialInterval: 5 * time.Second, MaxInterval: time.Second}
			},
		},
		{
			name: "custom reconnection policy",
			cfg: func(c *ClusterConfig) {
				c.ReconnectionPolicy = &fakeReconnectionPolicy{ReconnectionPolicy: &NoReconnectionPolicy{}}
			},
		},
		{
			name: "downgrading consistency retry policy",
			cfg: func(c *ClusterConfig) {
				c.RetryPolicy = &DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: []Consistency{One, Any}}
			},
		},
		{
			name: "exponential backoff retry policy with max below min",
			cfg: func(c *ClusterConfig) {
				c.RetryPolicy = &ExponentialBackoffRetryPolicy{NumRetries: 3, Min: 5 * time.Second, Max: time.Second}
			},
		},
		{
			name: "exponential backoff retry policy with sub-millisecond bounds",
			cfg: func(c *ClusterConfig) {
				c.RetryPolicy = &ExponentialBackoffRetryPolicy{NumRetries: 3, Min: time.Microsecond, Max: 2 * time.Microsecond}
			},
		},
		{
			name: "custom retry policy",
			cfg:  func(c *ClusterConfig) { c.RetryPolicy = &fakeRetryPolicy{RetryPolicy: &SimpleRetryPolicy{}} },
		},
		{
			name: "simple retry policy with a negative retry limit",
			cfg:  func(c *ClusterConfig) { c.RetryPolicy = &SimpleRetryPolicy{NumRetries: -1} },
		},
		{
			name: "exponential backoff retry policy with a negative retry limit",
			cfg:  func(c *ClusterConfig) { c.RetryPolicy = &ExponentialBackoffRetryPolicy{NumRetries: -2} },
		},
		{
			name: "tls without hostname verification",
			cfg:  func(c *ClusterConfig) { c.SslOpts = &SslOptions{} },
		},
		{
			name: "tls with a caller-supplied config",
			cfg:  func(c *ClusterConfig) { c.SslOpts = &SslOptions{Config: &tls.Config{InsecureSkipVerify: true}} },
		},
		{
			name: "datacenter host filter",
			cfg:  func(c *ClusterConfig) { c.HostFilter = DataCenterHostFilter("dc1") },
		},
		{
			name: "whitelist host filter",
			cfg:  func(c *ClusterConfig) { c.HostFilter = WhiteListHostFilter("127.0.0.1") },
		},
		{
			name: "host dialer that cannot target a shard",
			cfg:  func(c *ClusterConfig) { c.HostDialer = fakeHostDialer{} },
		},
		{
			name: "host dialer that can target a shard",
			cfg:  func(c *ClusterConfig) { c.HostDialer = fakeShardDialer{} },
		},
		{name: "dc-aware policy", policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1"))},
		{
			name:   "dc-aware policy without failover",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1", HostPolicyOptionDisableDCFailover)),
		},
		{name: "rack-aware policy", policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1"))},
		{
			name:   "rack-aware policy without dc failover",
			policy: TokenAwareHostPolicy(RackAwareRoundRobinPolicy("dc1", "rack1", HostPolicyOptionDisableDCFailover)),
		},
		{
			name:   "non-local replicas fallback over a confined fallback",
			policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1", HostPolicyOptionDisableDCFailover), NonLocalReplicasFallback()),
		},
		{name: "empty datacenter", policy: TokenAwareHostPolicy(DCAwareRoundRobinPolicy(""))},
		{name: "replica-set ordering", policy: TokenAwareHostPolicy(RoundRobinHostPolicy(), DontShuffleReplicas())},
		{
			name:   "adaptive ordering by in-flight requests",
			policy: TokenAwareHostPolicy(RoundRobinHostPolicy(), AvoidSlowReplicas(MAX_IN_FLIGHT_THRESHOLD)),
		},
		{name: "non-token-aware policy", policy: RoundRobinHostPolicy()},
		{name: "wrapped policy", policy: SingleHostReadyPolicy(TokenAwareHostPolicy(DCAwareRoundRobinPolicy("dc1")))},
		{name: "custom policy", policy: &fakeHostSelectionPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}},
	}
}

// buildCaseReport produces the report a case describes, decoded for
// validation.
func buildCaseReport(t *testing.T, cfgFn func(*ClusterConfig), policy HostSelectionPolicy, isScyllaConn bool) any {
	t.Helper()

	cfg := *NewCluster("127.0.0.1")
	if cfgFn != nil {
		cfgFn(&cfg)
	}
	if policy == nil {
		policy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	}
	// Resolve SslOpts the way session creation does, so the TLS group is built
	// from the config the driver would really dial with.
	if err := cfg.ValidateAndInitSSL(); err != nil {
		t.Fatalf("unable to initialize ssl config: %v", err)
	}

	raw, err := newDriverConfigReporter(newTestReportSession(cfg, policy)).buildReport(isScyllaConn)
	if err != nil {
		t.Fatalf("unable to build the report: %v", err)
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("report is not valid JSON: %v (%s)", err, raw)
	}
	return decoded
}

// TestDriverConfigReportConformsToSchema validates the report against the
// shipped JSON Schema across every configuration that changes its shape.
//
// The value constraints are the point: the schema pins ranges, enums and
// non-empty strings that are easy to satisfy by accident and easy to break by
// accident, and several of them describe values nothing in the driver's own
// validation rejects.
func TestDriverConfigReportConformsToSchema(t *testing.T) {
	schema := loadDriverConfigSchema(t)

	for _, tt := range driverConfigCases() {
		t.Run(tt.name, func(t *testing.T) {
			report := buildCaseReport(t, tt.cfg, tt.policy, tt.isScyllaConn)
			if err := schema.Validate(report); err != nil {
				encoded, _ := json.Marshal(report)
				t.Errorf("report does not conform to %s:\n%v\n\nreport: %s", driverConfigSchemaPath, err, encoded)
			}
		})
	}
}

// TestDriverConfigReportUnrepresentableConsistency documents the one
// configuration for which no conforming report exists.
//
// query.defaults.consistency is required, and its enum covers every level
// Consistency defines. An unrecognized numeric value is outside it, so such a
// consistency can be reported either as a token the enum rejects or not at all
// -- both are violations, and the report chooses to omit (see
// consistencyName).
//
// Nothing rejects such a configuration today, which is what makes it
// reachable. Adding that rejection to ClusterConfig.Validate is a behaviour
// change beyond this report and is left to a follow-up; this test pins the
// residual gap in the meantime, and should be deleted when the follow-up makes
// it unreachable.
func TestDriverConfigReportUnrepresentableConsistency(t *testing.T) {
	schema := loadDriverConfigSchema(t)

	for _, tt := range []struct {
		name string
		cons Consistency
	}{
		{"unrecognized default consistency", Consistency(0x42)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			report := buildCaseReport(t, func(c *ClusterConfig) { c.Consistency = tt.cons }, nil, false)

			err := schema.Validate(report)
			if err == nil {
				t.Fatal("expected the report to be non-conforming; if consistency is now representable, delete this test")
			}

			var validationErr *jsonschema.ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("expected a validation error, got %T: %v", err, err)
			}
			causes := leafCauses(validationErr)
			if len(causes) != 1 {
				t.Fatalf("expected the unreportable consistency to be the only deviation, got %d:\n%v", len(causes), err)
			}
			sole := causes[0]
			if got, want := strings.Join(sole.InstanceLocation, "/"), "query/defaults"; got != want {
				t.Errorf("expected the sole deviation at /%s, got /%s", want, got)
			}
			missing, ok := sole.ErrorKind.(*kind.Required)
			if !ok {
				t.Fatalf("expected a missing required key, got %T: %v", sole.ErrorKind, sole)
			}
			if diff := cmp.Diff([]string{"consistency"}, missing.Missing); diff != "" {
				t.Errorf("expected consistency to be the only missing key:\n%s", diff)
			}
		})
	}
}

// leafCauses flattens a validation error to the individual constraint failures
// at its leaves; the intermediate nodes only describe which subschema the
// failures were found under.
func leafCauses(err *jsonschema.ValidationError) []*jsonschema.ValidationError {
	if len(err.Causes) == 0 {
		return []*jsonschema.ValidationError{err}
	}
	var out []*jsonschema.ValidationError
	for _, cause := range err.Causes {
		out = append(out, leafCauses(cause)...)
	}
	return out
}
