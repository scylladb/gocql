// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"

	"net"

	"sync/atomic"
	"time"
)

type InternalContextBox struct{ InternalCtx context.Context }
type InternalV2DNSResolver struct {
	InternalBase                          DNSResolver
	InternalTimeout, InternalSetupTimeout time.Duration
	InternalStartup                       atomic.Pointer[InternalContextBox]
}

func (r *InternalV2DNSResolver) baseContext() context.Context {
	if box := r.InternalStartup.Load(); box != nil {
		return box.InternalCtx
	}
	return context.Background()
}
func (r *InternalV2DNSResolver) LookupIP(host string) ([]net.IP, error) {
	return r.lookupIP(r.baseContext(), host)
}
func (r *InternalV2DNSResolver) lookupSetup(host string) ([]net.IP, time.Time, error) {
	ctx := r.baseContext()
	cancel := func() {}
	if r.InternalSetupTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.InternalSetupTimeout)
	}
	defer cancel()
	deadline, _ := ctx.Deadline()
	ips, err := r.lookupIP(ctx, host)
	return ips, deadline, err
}
func (r *InternalV2DNSResolver) lookupIP(ctx context.Context, host string) ([]net.IP, error) {
	if r.InternalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.InternalTimeout)
		defer cancel()
	}
	if r.InternalBase == nil {
		return net.DefaultResolver.LookupIP(ctx, "ip", host)
	}
	type outcome struct {
		ips []net.IP
		err error
	}
	done := make(chan outcome, 1)
	go func() { ips, err := r.InternalBase.LookupIP(host); done <- outcome{ips, err} }()
	select {
	case result := <-done:
		return result.ips, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (r *InternalV2DNSResolver) InternalClearStartup() { r.InternalStartup.Store(nil) }

func (r *InternalV2DNSResolver) LookupIPContext(ctx context.Context, host string) ([]net.IP, error) {
	return r.lookupIP(ctx, host)
}
