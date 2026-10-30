// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"

	"sync"
)

// The bridge only adapts the handshake's state machine; no value marshaler is
// involved. One bridge is created for each connection and closed on every exit.
type InternalV2Authenticator struct {
	ctx      context.Context
	host     HostSnapshot
	provider AuthProvider
	exchange AuthExchange
	started  bool
	once     sync.Once
}

func InternalNewV2Authenticator(ctx context.Context, host HostSnapshot, provider AuthProvider) *InternalV2Authenticator {
	return &InternalV2Authenticator{ctx: ctx, host: host, provider: provider}
}
func (a *InternalV2Authenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	if err := a.ctx.Err(); err != nil {
		return nil, nil, err
	}
	var response Nullable[[]byte]
	var err error
	if !a.started {
		a.started = true
		a.exchange, response, err = a.provider.Start(a.ctx, a.host, string(req))
		if err == nil && a.exchange == nil {
			err = typeValidation("authentication", "provider returned a nil exchange")
		}
	} else {
		response, err = a.exchange.Challenge(a.ctx, InternalNullableAuthBytes(req))
	}
	if err != nil {
		return nil, nil, err
	}
	if !response.Valid {
		return nil, a, nil
	}
	if response.Value == nil {
		return []byte{}, a, nil
	}
	return response.Value, a, nil
}
func (a *InternalV2Authenticator) Success(data []byte) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	if a.exchange == nil {
		return typeValidation("authentication", "success before exchange")
	}
	return a.exchange.Success(a.ctx, InternalNullableAuthBytes(data))
}
func (a *InternalV2Authenticator) Close() {
	a.once.Do(func() {
		if a.exchange != nil {
			a.exchange.Close()
		}
	})
}
func InternalNullableAuthBytes(data []byte) Nullable[Bytes] {
	if data == nil {
		return Nullable[Bytes]{}
	}
	return Nullable[Bytes]{Value: BorrowBytes(data), Valid: true}
}
