package gocql

import (
	"context"
	"errors"
	"strings"
	"sync"
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
type passwordAuth struct{ username, password string }

func PasswordAuth(username, password string) AuthProvider {
	return passwordAuth{username: username, password: password}
}
func (p passwordAuth) Start(ctx context.Context, _ HostSnapshot, _ string) (AuthExchange, Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return nil, Nullable[[]byte]{}, err
	}
	if strings.ContainsRune(p.username, 0) || strings.ContainsRune(p.password, 0) {
		return nil, Nullable[[]byte]{}, typeValidation("password authentication", "credentials contain NUL")
	}
	response := make([]byte, len(p.username)+len(p.password)+2)
	copy(response[1:], p.username)
	copy(response[len(p.username)+2:], p.password)
	return &passwordExchange{}, Nullable[[]byte]{Value: response, Valid: true}, nil
}

type passwordExchange struct{}

func (*passwordExchange) Challenge(ctx context.Context, _ Nullable[Bytes]) (Nullable[[]byte], error) {
	if err := ctx.Err(); err != nil {
		return Nullable[[]byte]{}, err
	}
	return Nullable[[]byte]{}, errors.New("gocql: password authenticator received an unexpected challenge")
}
func (*passwordExchange) Success(ctx context.Context, _ Nullable[Bytes]) error { return ctx.Err() }
func (*passwordExchange) Close()                                               {}

// The bridge only adapts the handshake's state machine; no value marshaler is
// involved. One bridge is created for each connection and closed on every exit.
type v2Authenticator struct {
	ctx      context.Context
	host     HostSnapshot
	provider AuthProvider
	exchange AuthExchange
	started  bool
	once     sync.Once
}

func newV2Authenticator(ctx context.Context, host HostSnapshot, provider AuthProvider) *v2Authenticator {
	return &v2Authenticator{ctx: ctx, host: host, provider: provider}
}
func (a *v2Authenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
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
		response, err = a.exchange.Challenge(a.ctx, nullableAuthBytes(req))
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
func (a *v2Authenticator) Success(data []byte) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	if a.exchange == nil {
		return typeValidation("authentication", "success before exchange")
	}
	return a.exchange.Success(a.ctx, nullableAuthBytes(data))
}
func (a *v2Authenticator) Close() {
	a.once.Do(func() {
		if a.exchange != nil {
			a.exchange.Close()
		}
	})
}
func nullableAuthBytes(data []byte) Nullable[Bytes] {
	if data == nil {
		return Nullable[Bytes]{}
	}
	return Nullable[Bytes]{Value: BorrowBytes(data), Valid: true}
}
