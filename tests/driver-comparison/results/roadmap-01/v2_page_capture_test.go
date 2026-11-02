// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// The first page leaves its first attempt blocked in an accepted write while
// its speculative winner and a continuation run on another connection. Both
// pages use the same logical request, including when prefetch starts page two.
type pageCapturePolicy struct {
	first, next Target
	pages       atomic.Int32
	attempts    atomic.Int32
}

func (*pageCapturePolicy) Validate(*MetadataSnapshot) error { return nil }
func (p *pageCapturePolicy) Plan(RoutingInfo, *MetadataSnapshot) QueryPlan {
	if p.pages.Add(1) == 1 {
		return &targetPlan{InternalTargets: []Target{p.first, p.next}}
	}
	return &targetPlan{InternalTargets: []Target{p.next}}
}
func (p *pageCapturePolicy) Observe(AttemptEvent) { p.attempts.Add(1) }

func TestV2PageCaptureContinuationWithOutstandingLoser(t *testing.T) {
	for _, prefetch := range []int{0, 1} {
		name := "serial"
		if prefetch != 0 {
			name = "prefetch"
		}
		t.Run(name, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 2)
			policy := &pageCapturePolicy{
				first: Target{HostID: implEndpoint.InternalHostInfoHostUUID(conns[0].InternalHost)},
				next:  Target{HostID: implEndpoint.InternalHostInfoHostUUID(conns[1].InternalHost)},
			}
			profile := defaultProfile()
			profile.Routing = policy
			profile.Speculation = runtimeSpeculation{}
			profile.Prefetch = prefetch
			handle := codecTestMust(NewProfile(profile))
			served := make(chan struct{})
			go func() {
				defer close(served)
				for _, body := range [][]byte{runtimeRowsBody([]byte{1}, "page-one"), runtimeRowsBody(nil, "page-two")} {
					stream := typedTestReadRequest(t, conns[1], peers[1])
					if _, err := peers[1].Write(typedTestResponse(t, 4, stream, body)); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			bound := runtimeQuery(t)
			defer bound.Close()
			rows, err := Run(ctx, s, bound, Options{Profile: handle, Idempotent: true})
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() || string(rows.Row().OwnedBytes()) != "page-one" {
				t.Fatal("first page", rows.Err())
			}
			retained := rows.Row()
			lease, err := rows.Retain()
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			if !rows.Next() || string(rows.Row().OwnedBytes()) != "page-two" {
				t.Fatal("continuation", rows.Err())
			}
			if rows.Next() || rows.Err() != nil {
				t.Fatal("completion", rows.Err())
			}
			<-served
			if policy.pages.Load() != 2 || string(retained.OwnedBytes()) != "page-one" {
				t.Fatal("continuation changed retained page")
			}
			bound.Close()
			rows.Close()
			short, stop := context.WithTimeout(ctx, 5*time.Millisecond)
			defer stop()
			if !errors.Is(bound.Wait(short), context.DeadlineExceeded) {
				t.Fatal("outstanding loser did not retain borrowed input")
			}
			// Let the accepted loser writer drain only after both pages close.
			stream := typedTestReadRequest(t, conns[0], peers[0])
			late := make(chan struct{})
			go func() {
				defer close(late)
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "late-loser")))
			}()
			if err := bound.Wait(ctx); err != nil {
				t.Fatal("loser drain", err)
			}
			<-late
			if policy.attempts.Load() != 3 {
				t.Fatalf("got %d attempt observations, want 3", policy.attempts.Load())
			}
		})
	}
}
