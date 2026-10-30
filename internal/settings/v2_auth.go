// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"context"
	"errors"
	"strings"
)

// AuthProvider creates independent exchanges and must be concurrency safe.
type AuthProvider interface {
	Start(context.Context, HostSnapshot, string) (AuthExchange, Nullable[[]byte], error)
}
type AuthExchange interface {
	Challenge(context.Context, Nullable[Bytes]) (Nullable[[]byte], error)
	Success(context.Context, Nullable[Bytes]) error
	Close()
}
type InternalPasswordAuth struct{ username, password string }

func PasswordAuth(username, password string) AuthProvider {
	return InternalPasswordAuth{username: username, password: password}
}
func (p InternalPasswordAuth) Start(ctx context.Context, _ HostSnapshot, _ string) (AuthExchange, Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return nil, Nullable[[]byte]{}, err
	}
	if strings.ContainsRune(p.username, 0) || strings.ContainsRune(p.password, 0) {
		return nil, Nullable[[]byte]{}, typeValidation("password authentication", "credentials contain NUL")
	}
	response := make([]byte, len(p.username)+len(p.password)+2)
	copy(response[1:], p.username)
	copy(response[len(p.username)+2:], p.password)
	return &InternalPasswordExchange{}, Nullable[[]byte]{Value: response, Valid: true}, nil
}

type InternalPasswordExchange struct{}

func (*InternalPasswordExchange) Challenge(ctx context.Context, _ Nullable[Bytes]) (Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return Nullable[[]byte]{}, err
	}
	return Nullable[[]byte]{}, errors.New("gocql: password authenticator received an unexpected challenge")
}
func (*InternalPasswordExchange) Success(ctx context.Context, _ Nullable[Bytes]) error {
	return ctx.Err()
}
func (*InternalPasswordExchange) Close() {}
