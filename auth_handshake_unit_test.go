//go:build unit
// +build unit

package gocql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

const testAuthClass = "org.apache.cassandra.auth.PasswordAuthenticator"

// authReply is how the test server answers one AUTH_RESPONSE.
type authReply struct {
	op      frm.Op // OpAuthChallenge, OpAuthSuccess, OpError or anything else
	data    []byte
	errCode int
	errMsg  string
}

// authTestServer answers STARTUP with AUTHENTICATE and every AUTH_RESPONSE
// with reply(token). It records the tokens it receives.
type authTestServer struct {
	*TestServer
	mu     sync.Mutex
	tokens []string
}

func newAuthTestServer(t *testing.T, reply func(token []byte) authReply) *authTestServer {
	t.Helper()
	srv := &authTestServer{TestServer: NewTestServer(t, protoVersion4, context.Background())}
	t.Cleanup(srv.Stop)
	srv.setAuthHook(func(req *framer, stream int, resp *framer) {
		if req.header.Op == frm.OpStartup {
			resp.writeHeader(0, frm.OpAuthenticate, stream)
			resp.writeString(testAuthClass)
			return
		}
		token := req.readBytesCopy()
		srv.mu.Lock()
		srv.tokens = append(srv.tokens, string(token))
		srv.mu.Unlock()

		r := reply(token)
		switch r.op {
		case frm.OpError:
			resp.writeHeader(0, frm.OpError, stream)
			resp.writeInt(int32(r.errCode))
			resp.writeString(r.errMsg)
		case frm.OpAuthChallenge, frm.OpAuthSuccess:
			resp.writeHeader(0, r.op, stream)
			resp.writeBytes(r.data)
		default:
			resp.writeHeader(0, r.op, stream)
		}
	})
	return srv
}

func (srv *authTestServer) received() []string {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return append([]string(nil), srv.tokens...)
}

// syncLogger captures log output from the pool's goroutines.
type syncLogger struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *syncLogger) Print(v ...any) { l.write(fmt.Sprint(v...)) }
func (l *syncLogger) Printf(format string, v ...any) {
	l.write(fmt.Sprintf(format, v...))
}
func (l *syncLogger) Println(v ...any) { l.write(fmt.Sprintln(v...)) }

func (l *syncLogger) write(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.WriteString(s)
}

func (l *syncLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// cluster returns a config for one connection to srv that gives up after a
// single attempt. testCluster has no control connection, so CreateSession
// reports a failed handshake only as ErrNoConnectionsStarted; the cause is in
// the pool's log, which lands in the returned logger.
func (srv *authTestServer) cluster(auth Authenticator) (*ClusterConfig, *syncLogger) {
	cfg := testCluster(protoVersion4, srv.Address)
	cfg.NumConns = 1
	cfg.Authenticator = auth
	cfg.InitialReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1}
	cfg.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1}
	log := &syncLogger{}
	cfg.Logger = log
	return cfg, log
}

// requireHandshakeError fails t unless CreateSession failed and the pool
// logged a cause containing want.
func requireHandshakeError(t *testing.T, cfg *ClusterConfig, log *syncLogger, want string) {
	t.Helper()
	s, err := cfg.CreateSession()
	if err == nil {
		s.Close()
		t.Fatal("CreateSession succeeded, want an authentication error")
	}
	if !strings.Contains(err.Error(), want) && !strings.Contains(log.String(), want) {
		t.Fatalf("CreateSession error = %v, log = %q; want the cause %q", err, log.String(), want)
	}
}

func passwordToken(user, pass string) string {
	return "\x00" + user + "\x00" + pass
}

func TestAuthHandshakePassword(t *testing.T) {
	t.Parallel()

	want := passwordToken("cassandra", "secret")
	srv := newAuthTestServer(t, func(token []byte) authReply {
		if string(token) != want {
			return authReply{op: frm.OpError, errCode: ErrCodeCredentials, errMsg: "bad credentials"}
		}
		return authReply{op: frm.OpAuthSuccess}
	})

	cfg, _ := srv.cluster(PasswordAuthenticator{Username: "cassandra", Password: "secret"})
	s, err := cfg.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer s.Close()
	if err := s.Query("void").Exec(); err != nil {
		t.Fatalf("query after authenticating: %v", err)
	}
	if got := srv.received(); len(got) == 0 || got[0] != want {
		t.Fatalf("server received tokens %q, want %q", got, want)
	}
}

func TestAuthHandshakeFailures(t *testing.T) {
	t.Parallel()

	success := func([]byte) authReply { return authReply{op: frm.OpAuthSuccess} }

	tests := []struct {
		name    string
		auth    Authenticator
		reply   func([]byte) authReply
		wantErr string
	}{
		{
			name:    "NoAuthenticatorConfigured",
			auth:    nil,
			reply:   success,
			wantErr: fmt.Sprintf("authentication required (using %q)", testAuthClass),
		},
		{
			name:    "AuthenticatorClassNotAllowed",
			auth:    PasswordAuthenticator{Username: "u", Password: "p", AllowedAuthenticators: []string{"com.example.Other"}},
			reply:   success,
			wantErr: "unexpected authenticator",
		},
		{
			name: "BadCredentials",
			auth: PasswordAuthenticator{Username: "u", Password: "wrong"},
			reply: func([]byte) authReply {
				return authReply{op: frm.OpError, errCode: ErrCodeCredentials, errMsg: "Provided username u and/or password are incorrect"}
			},
			wantErr: "password are incorrect",
		},
		{
			name:    "UnexpectedFrame",
			auth:    PasswordAuthenticator{Username: "u", Password: "p"},
			reply:   func([]byte) authReply { return authReply{op: frm.OpReady} },
			wantErr: "unknown frame response during authentication",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newAuthTestServer(t, tc.reply)
			cfg, log := srv.cluster(tc.auth)
			requireHandshakeError(t, cfg, log, tc.wantErr)
		})
	}
}

// authLog records what a stepAuthenticator was called with.
type authLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *authLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprintf(format, args...))
}

func (l *authLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

// stepAuthenticator answers challenge n with token "r<n>" and hands back the
// authenticator for step n+1, so every connection walks the same sequence.
type stepAuthenticator struct {
	log          *authLog
	n            int
	challengeErr error
	successErr   error
}

func (a stepAuthenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	a.log.add("challenge %d %q", a.n, req)
	if a.challengeErr != nil && a.n > 0 {
		return nil, nil, a.challengeErr
	}
	next := a
	next.n++
	return []byte(fmt.Sprintf("r%d", a.n)), next, nil
}

func (a stepAuthenticator) Success(data []byte) error {
	a.log.add("success %d %q", a.n, data)
	return a.successErr
}

// A SASL-style exchange: two challenges, then a success carrying final data
// that is handed to the authenticator the last challenge returned.
func multiStepReply(token []byte) authReply {
	switch string(token) {
	case "r0":
		return authReply{op: frm.OpAuthChallenge, data: []byte("c1")}
	case "r1":
		return authReply{op: frm.OpAuthSuccess, data: []byte("done")}
	}
	return authReply{op: frm.OpError, errCode: ErrCodeCredentials, errMsg: "unexpected token " + string(token)}
}

func TestAuthHandshakeMultiStep(t *testing.T) {
	t.Parallel()

	srv := newAuthTestServer(t, multiStepReply)
	log := &authLog{}
	cfg, _ := srv.cluster(stepAuthenticator{log: log})
	s, err := cfg.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer s.Close()

	want := []string{
		fmt.Sprintf("challenge 0 %q", testAuthClass),
		`challenge 1 "c1"`,
		`success 2 "done"`,
	}
	got := log.get()
	if len(got) < len(want) {
		t.Fatalf("authenticator calls = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("authenticator calls = %q, want them to start with %q", got, want)
		}
	}
}

func TestAuthHandshakeAuthenticatorErrors(t *testing.T) {
	t.Parallel()

	errChallenge := errors.New("challenge rejected")
	errSuccess := errors.New("success rejected")

	for _, tc := range []struct {
		name string
		auth stepAuthenticator
		want error
	}{
		{"ChallengeFails", stepAuthenticator{challengeErr: errChallenge}, errChallenge},
		{"SuccessFails", stepAuthenticator{successErr: errSuccess}, errSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newAuthTestServer(t, multiStepReply)
			tc.auth.log = &authLog{}
			cfg, log := srv.cluster(tc.auth)
			requireHandshakeError(t, cfg, log, tc.want.Error())
		})
	}
}

// AuthProvider picks the authenticator per host.
func TestAuthHandshakeAuthProvider(t *testing.T) {
	t.Parallel()

	want := passwordToken("per-host", "pw")
	srv := newAuthTestServer(t, func(token []byte) authReply {
		if string(token) != want {
			return authReply{op: frm.OpError, errCode: ErrCodeCredentials, errMsg: "bad credentials"}
		}
		return authReply{op: frm.OpAuthSuccess}
	})

	var mu sync.Mutex
	var hosts []string
	cfg, _ := srv.cluster(nil)
	cfg.AuthProvider = func(h *HostInfo) (Authenticator, error) {
		mu.Lock()
		hosts = append(hosts, h.ConnectAddress().String())
		mu.Unlock()
		return PasswordAuthenticator{Username: "per-host", Password: "pw"}, nil
	}
	s, err := cfg.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	s.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(hosts) == 0 || hosts[0] != "127.0.0.1" {
		t.Fatalf("AuthProvider called for %v, want 127.0.0.1", hosts)
	}

	errProvider := errors.New("no credentials for host")
	cfg, log := srv.cluster(nil)
	cfg.AuthProvider = func(*HostInfo) (Authenticator, error) { return nil, errProvider }
	requireHandshakeError(t, cfg, log, errProvider.Error())
}

// PasswordAuthenticator is single-step: it returns no follow-up
// authenticator. A server that answers its credentials with a challenge
// anyway must fail the connection, not dereference the missing one.
func TestAuthHandshakeChallengeWithoutChallenger(t *testing.T) {
	t.Parallel()

	srv := newAuthTestServer(t, func([]byte) authReply {
		return authReply{op: frm.OpAuthChallenge, data: []byte("more?")}
	})
	cfg, log := srv.cluster(PasswordAuthenticator{Username: "u", Password: "p"})
	requireHandshakeError(t, cfg, log, "unexpected authentication challenge")
}
