// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestRuntimeAttachmentPrecedesStartupAndDrainsFailure(t *testing.T) {
	config := settings.DefaultConfig("127.0.0.1")
	config.Metadata.InitialDiscovery = false
	config.Metadata.PeriodicRefresh = 0
	config.ReportConfiguration = false
	config.ConnectTimeout = 25 * time.Millisecond
	dialError := errors.New("attachment startup fault")
	var rt *InternalSessionRuntime
	stopped := make(chan struct{})
	config.DialHost = func(context.Context, endpoint.DialTarget) (net.Conn, error) {
		if rt == nil || cluster.InternalCurrentExecution(rt.Session) != rt {
			t.Error("startup preceded execution attachment")
		}
		return nil, dialError
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session, err := cluster.InternalConnectCluster(ctx, config, func(s *cluster.Session, c Config) *Config {
		rt = InternalRuntimeFor(s)
		if InternalRuntimeFor(s) != rt {
			t.Error("execution attachment replaced")
		}
		rt.InternalConfig = c
		rt.InternalProfile = c.DefaultProfile
		if !rt.beginPreparedWorker(s) {
			t.Fatal("initial prepare worker rejected")
		}
		go func() { defer rt.prepareWorkers.Done(); <-cluster.SessionData(s).InternalCtx.Done(); close(stopped) }()
		return &rt.InternalConfig
	})
	if err == nil || session != nil || !strings.Contains(err.Error(), dialError.Error()) {
		t.Fatalf("startup result session=%p err=%v", session, err)
	}
	if rt == nil || !rt.Session.Closed() {
		t.Fatal("failed startup retained a live owner")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("failed startup returned before joining prepare worker")
	}
	rt.prepareWorkersMu.Lock()
	closed := rt.prepareWorkersClosed
	rt.prepareWorkersMu.Unlock()
	if !closed {
		t.Fatal("failed startup left prepare admission open")
	}
}

func attachmentSession() (*cluster.Session, *InternalSessionRuntime) {
	s := new(cluster.Session)
	data := cluster.SessionData(s)
	data.InternalCtx, data.InternalCancel = context.WithCancel(context.Background())
	return s, InternalRuntimeFor(s)
}
func attachmentLogical(s *cluster.Session, parent *inputOwner) (*InternalLogicalExecution, context.CancelFunc) {
	_, cancel := context.WithCancel(context.Background())
	return &InternalLogicalExecution{session: s, request: &InternalExecutionRequest{InternalOwner: parent}, lifetime: InternalExecutionLifetime{cancel: cancel, done: make(chan struct{})}}, cancel
}

func TestRuntimeAttachmentStopsAllBeforeJoiningWriter(t *testing.T) {
	s, rt := attachmentSession()
	parent := newInputOwner()
	if err := parent.InternalAdmit(); err != nil {
		t.Fatal(err)
	}
	if err := parent.InternalAdmit(); err != nil {
		t.Fatal(err)
	}
	first, cancelFirst := attachmentLogical(s, parent)
	defer cancelFirst()
	second, cancelSecond := attachmentLogical(s, parent)
	defer cancelSecond()
	if err := rt.registerExecution(first, parent); err != nil {
		t.Fatal(err)
	}
	if err := rt.registerExecution(second, parent); err != nil {
		t.Fatal(err)
	}
	writer := first.request.InternalOwner.InternalWriterLease(wire.Chain{})
	firstStopped, secondStopped := make(chan struct{}), make(chan struct{})
	first.installStop(func() { first.InternalReleaseInput(); close(firstStopped) })
	second.installStop(func() { second.InternalReleaseInput(); close(secondStopped) })
	parent.InternalClose()
	done := make(chan struct{})
	go func() { _ = s.Close(); close(done) }()
	for _, stopped := range []<-chan struct{}{firstStopped, secondStopped} {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("Close joined one owner before stopping all owners")
		}
	}
	select {
	case <-done:
		t.Fatal("Close retired an acknowledged writer before completion")
	default:
	}
	writer.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close failed to join retired owners")
	}
	if err := parent.InternalWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt.InternalMu.Lock()
	remaining := len(rt.InternalExecutions)
	rt.InternalMu.Unlock()
	if remaining != 0 {
		t.Fatal("execution registry retained drained owners")
	}
}

func TestRuntimeAttachmentAdmissionCloseRace(t *testing.T) {
	for i := 0; i < 30; i++ {
		s, rt := attachmentSession()
		parent := newInputOwner()
		if err := parent.InternalAdmit(); err != nil {
			t.Fatal(err)
		}
		logical, cancel := attachmentLogical(s, parent)
		logical.installStop(logical.InternalReleaseInput)
		start := make(chan struct{})
		admitted := make(chan error, 1)
		closed := make(chan struct{})
		go func() { <-start; admitted <- rt.registerExecution(logical, parent) }()
		go func() { <-start; _ = s.Close(); close(closed) }()
		close(start)
		err := <-admitted
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
			parent.InternalRelease()
		}
		<-closed
		cancel()
		parent.InternalClose()
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		err = parent.InternalWait(ctx)
		stop()
		if err != nil {
			t.Fatal("admission escaped Close drain", err)
		}
	}
}
