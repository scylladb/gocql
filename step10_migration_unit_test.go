//go:build unit

/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"reflect"
	"testing"
)

type step10CustomRetry struct{}

func (*step10CustomRetry) Decide(AttemptError) RetryDecision { return RetryDecision{Action: StopRetry} }

func TestStep10RetryConfigurationReport(t *testing.T) {
	for _, tc := range []struct{ attempts, want int }{{-1, 2}, {0, 2}, {1, 0}, {3, 2}, {7, 6}} {
		got := buildRetryPolicyReport(SafeRetryPolicy{MaxAttempts: tc.attempts})
		want := retryPolicySimpleReport{Type: "simple", MaxRetries: tc.want}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("attempts %d: got %#v want %#v", tc.attempts, got, want)
		}
	}
	for _, policy := range []RetryPolicy{(*step10CustomRetry)(nil), &step10CustomRetry{}} {
		got := buildRetryPolicyReport(policy).(retryPolicyCustomReport)
		if got.Name != "step10CustomRetry" || got.Type != "custom" {
			t.Fatalf("custom report: %+v", got)
		}
	}
}

// step10ScalarParity checks canonical validation against incremental conversion
// across every short read and every truncated scalar body.
func step10ScalarParity[T any](t *testing.T, kind Type, buffered Decoder[T], incremental StreamDecoder[T], bodies ...[]byte) {
	t.Helper()
	typ := foundationScalar(t, kind)
	for _, body := range bodies {
		for length := 0; length <= len(body); length++ {
			prefix := body[:length]
			encoded, err := NewEncodedValue(typ, Present, BorrowBytes(prefix))
			if err != nil {
				t.Fatal(err)
			}
			want, wantErr := DecodeValue(buffered, encoded)
			for step := 1; step <= len(prefix)+4; step++ {
				var got T
				err := streamDecodeFixture(t, typ, incremental, streamCell(prefix), &got, step)
				if (err == nil) != (wantErr == nil) {
					t.Fatalf("%s bytes %x split %d: stream %v buffered %v", kind, prefix, step, err, wantErr)
				}
				if err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("%s bytes %x split %d: got %#v want %#v", kind, prefix, step, got, want)
				}
			}
		}
	}
}
func TestStep10IncrementalScalarValidationParity(t *testing.T) {
	step10ScalarParity(t, TypeBoolean, Bool, StreamBool, []byte{0}, []byte{1}, []byte{2})
	step10ScalarParity(t, TypeDate, DateCodec, StreamDate, []byte{0, 0, 0, 0}, []byte{255, 255, 255, 255})
	step10ScalarParity(t, TypeTime, TimeCodec, StreamTime, []byte{255, 255, 255, 255, 255, 255, 255, 255}, []byte{0, 0, 78, 148, 145, 79, 0, 0})
	step10ScalarParity(t, TypeTimeUUID, TimeUUIDCodec, StreamTimeUUID, make([]byte, 16), []byte{0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	step10ScalarParity(t, TypeInet, InetCodec, StreamInet, []byte{127, 0, 0, 1}, make([]byte, 16))
	step10ScalarParity(t, TypeDuration, DurationCodec, StreamDuration,
		[]byte{0, 0, 0}, []byte{1, 2, 0}, []byte{128, 0, 0, 0}, // accepted nonminimal zero prefix
		[]byte{240, 255, 255, 255, 255, 240, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
		[]byte{248, 1, 0, 0, 0, 0, 0, 0}, // months overflow
		[]byte{0, 0, 0, 0},               // trailing byte
	)
}

func BenchmarkStep10DurationDecode(b *testing.B) {
	reader := scalarReader(b, TypeDuration, Present, []byte{2, 4, 6})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DurationCodec.Decode(reader); err != nil {
			b.Fatal(err)
		}
	}
}

func TestStep10ReportUsesCurrentTypedProfile(t *testing.T) {
	config := DefaultConfig("127.0.0.1")
	profile := config.DefaultProfile.Snapshot()
	routing, err := NewRoutingPolicy(RoutingOptions{TokenAware: true, Datacenter: "dc", Rack: "rack", AllowRemote: true, Latency: LatencyOptions{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	profile.Routing = routing
	profile.Retry = SafeRetryPolicy{MaxAttempts: 7}
	profile.PageSize = 23
	if err := config.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalCfg = *newClusterSettings()
		implCluster.SessionData(opaqueStorageValue).InternalTypedState = &sessionRuntime{InternalProfile: config.DefaultProfile, InternalConfig: config}
		return opaqueStorageValue
	}()
	report := buildQueryReport(s)
	if report.Defaults.Page.Size != 23 || report.Retry.Policy.(retryPolicySimpleReport).MaxRetries != 6 {
		t.Fatalf("typed defaults: %+v", report)
	}
	location := report.LoadBalancing.NodePreference.(nodeLocationRackReport)
	if location.LocalDC != "dc" || location.LocalRack != "rack" {
		t.Fatalf("typed locality: %+v", location)
	}
	policy := report.LoadBalancing.Policy.(loadBalancingTokenAwareReport)
	if !policy.FallbackToNonPreferredNodes || policy.AdaptiveOrdering.Signals[0] != "latency" {
		t.Fatalf("typed routing: %+v", policy)
	}
	profile.PageSize = 37
	profile.Retry = SafeRetryPolicy{MaxAttempts: 1}
	if err := config.DefaultProfile.Replace(profile); err != nil {
		t.Fatal(err)
	}
	next := buildQueryReport(s)
	if next.Defaults.Page.Size != 37 || next.Retry.Policy.(retryPolicySimpleReport).MaxRetries != 0 {
		t.Fatalf("replaced profile: %+v", next)
	}
}
