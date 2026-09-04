//go:build unit
// +build unit

package gocql

import (
	"context"
	"sync"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

func TestSessionIDIsUniquePerSession(t *testing.T) {
	logger := &defaultLogger{}

	id1 := newSessionID(logger)
	id2 := newSessionID(logger)

	if id1 == "" || id2 == "" {
		t.Fatal("expected non-empty session ids")
	}
	if id1 == id2 {
		t.Errorf("expected distinct session ids, got %q twice", id1)
	}
}

// TestPseudoRandomUUID covers the fallback newSessionID uses if RandomUUID
// ever reports an error. newSessionID cannot reach it on any supported Go
// version, so the fallback is only exercised here: it must produce distinct,
// well-formed random UUIDs.
func TestPseudoRandomUUID(t *testing.T) {
	first := pseudoRandomUUID()
	second := pseudoRandomUUID()

	if first == second {
		t.Errorf("expected distinct uuids, got %q twice", first)
	}
	for _, u := range []UUID{first, second} {
		if got := u.Version(); got != 4 {
			t.Errorf("expected uuid %q to be version 4, got %d", u, got)
		}
		if got := u.Variant(); got != VariantIETF {
			t.Errorf("expected uuid %q to use the IETF variant, got %d", u, got)
		}
	}
}

// TestSessionIDReportingStartupFrame checks what actually reaches the wire:
// every connection of a session shares the same SESSION_ID, distinct
// sessions get distinct ones, and every STARTUP frame carries one.
func TestSessionIDReportingStartupFrame(t *testing.T) {
	// Enough connections per session that "they all agree" is a claim about a
	// set rather than about a single sample. The default NumConns of 2 would
	// technically do, but only the first connection is opened during session
	// initialization, so a test that did not wait would routinely observe just
	// that one and assert nothing.
	const connsPerSession = 3

	tests := []struct {
		name             string
		sessionCount     int
		expectedSessions int
	}{
		{name: "single session", sessionCount: 1, expectedSessions: 1},
		{name: "multiple sessions", sessionCount: 2, expectedSessions: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var (
				mu         sync.Mutex
				sessionIDs = map[string]int{}
				startups   int
			)

			srv := newTestServerOpts{
				addr:     "127.0.0.1:0",
				protocol: defaultProto,
				recvHook: func(f *framer) {
					if f.header.Op != frm.OpStartup {
						return
					}
					// Consuming the frame body here is only safe because the fake
					// server does not read the body of a STARTUP request.
					opts := readStartupOptions(t, f)

					mu.Lock()
					defer mu.Unlock()
					startups++
					if id, ok := opts[sessionIDStartupKey]; ok {
						sessionIDs[id]++
					}
				},
			}.newServer(t, ctx)
			defer srv.Stop()

			sessions := make([]*Session, 0, tt.sessionCount)
			for i := 0; i < tt.sessionCount; i++ {
				cluster := testCluster(defaultProto, srv.Address)
				cluster.NumConns = connsPerSession
				session, err := cluster.CreateSession()
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				sessions = append(sessions, session)
			}

			// CreateSession only guarantees one connection per host: the rest of
			// the pool is filled asynchronously when NumConns > 1, so without
			// this wait the snapshot below would capture an arbitrary prefix of
			// the STARTUP frames. Waiting on the pool is what makes the counts
			// deterministic, and a connection only joins the pool once its
			// handshake has completed, which is strictly after the server has
			// run recvHook on its STARTUP.
			for i, session := range sessions {
				if err := waitForPoolSize(session, connsPerSession, 10*time.Second); err != nil {
					t.Fatalf("session %d: %s", i, err)
				}
			}

			mu.Lock()
			defer mu.Unlock()

			if len(sessionIDs) != tt.expectedSessions {
				t.Errorf("expected %d distinct SESSION_ID, got %d: %v", tt.expectedSessions, len(sessionIDs), sessionIDs)
			}

			// Every connection of a session, not merely the first, must report
			// the id that session's own ID reports. Keying by ID rather than
			// iterating the observed ids also pins the two ends of the
			// correlation together: an accessor that returned anything other
			// than the value on the wire would leave an application unable to
			// match its logs to a system.clients row, which is the reason the
			// id exists. Without the count check the test would pass on a
			// driver that generated a fresh id per connection, as long as only
			// one connection per session had been observed.
			for i, session := range sessions {
				if count := sessionIDs[session.ID()]; count < connsPerSession {
					t.Errorf("session %d: expected SESSION_ID %q to be reported by all %d of its connections, got %d: %v",
						i, session.ID(), connsPerSession, count, sessionIDs)
				}
			}

			// Every STARTUP frame must carry a SESSION_ID: summing the
			// per-id counts and comparing against the unconditional STARTUP
			// count catches a frame that omits SESSION_ID entirely, which
			// indexing sessionIDs by the (possibly absent) id would hide.
			var reported int
			for _, count := range sessionIDs {
				reported += count
			}
			if reported != startups {
				t.Errorf("expected every STARTUP frame to carry SESSION_ID: got %d STARTUP frames but only %d reported one", startups, reported)
			}
		})
	}
}
