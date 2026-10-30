//go:build unit

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
)

type convictionRecorder struct {
	callback func(error, model.HostSnapshot) bool
	resets   []model.HostSnapshot
}

func (r *convictionRecorder) AddFailure(err error, host model.HostSnapshot) bool {
	return r.callback(err, host)
}
func (r *convictionRecorder) Reset(host model.HostSnapshot) { r.resets = append(r.resets, host) }

type convictionPicker struct {
	InternalNopConnPicker
	count int
}

func (p convictionPicker) Size() (int, int) { return p.count, 0 }

func TestConvictionSnapshotFailureDecision(t *testing.T) {
	for _, convict := range []bool{false, true} {
		t.Run(map[bool]string{false: "keep", true: "convict"}[convict], func(t *testing.T) {
			id := cqltype.ParseUUIDMust("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
			host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042, DataCenter: "dc1", Rack: "r2", Tokens: []string{"-12", "64"}}).Build()
			endpoint.InternalHostInfoSetState(&host, endpoint.NodeUp)
			features := endpoint.ScyllaHostFeatures{}
			endpoint.ScyllaHostFeaturesData(&features).InternalNrShards = 4
			endpoint.InternalHostInfoSetScyllaFeatures(&host, features)
			cause := errors.New("actual pool failure completion")
			calls, downs := 0, 0
			p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{}, InternalLogger: logging.InternalNopLogger{}}
			policy := &convictionRecorder{callback: func(err error, snapshot model.HostSnapshot) bool {
				if !p.InternalMu.TryLock() {
					t.Fatal("conviction callback holds pool lock")
				}
				p.InternalMu.Unlock()
				calls++
				if !errors.Is(err, cause) {
					t.Fatal("failure replaced", err)
				}
				if snapshot.ID != id || snapshot.Address.String() != "127.0.0.7:9042" || snapshot.Datacenter != "dc1" || snapshot.Rack != "r2" || !reflect.DeepEqual(snapshot.Tokens, []int64{-12, 64}) || !snapshot.Up || snapshot.Shards != 4 || !snapshot.Enabled || snapshot.Connected {
					t.Fatalf("lost host context: %+v", snapshot)
				}
				snapshot.Tokens[0] = 999 // Detached callback storage cannot mutate HostInfo.
				return convict
			}}
			p.InternalServices = InternalPoolServices{Config: &connectivity.Config{}, Conviction: policy, Down: func(address net.IP, port int) {
				downs++
				if !address.Equal(net.ParseIP("127.0.0.7")) || port != 9042 {
					t.Fatal("wrong down endpoint")
				}
			}}
			p.fillingStopped(cause)
			if calls != 1 || downs != map[bool]int{false: 0, true: 1}[convict] {
				t.Fatal("conviction decision changed", calls, downs)
			}
			fresh, err := endpoint.SnapshotHost(&host)
			if err != nil || fresh.Tokens[0] != -12 {
				t.Fatal("callback mutated host storage", fresh, err)
			}
			var hook connectivity.ConvictionPolicy = policy
			hook.Reset(fresh)
			if len(policy.resets) != 1 || policy.resets[0].ID != id {
				t.Fatal("reset lost host context")
			}
			// A successful completion or a still-connected pool must not invoke policy.
			p.fillingStopped(nil)
			p.InternalConnPicker = convictionPicker{count: 1}
			p.fillingStopped(cause)
			if calls != 1 {
				t.Fatal("conviction invoked without an empty failed pool", calls)
			}
		})
	}
}
