/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStep07KindDeclarationAndServerRefinement(t *testing.T) {
	for _, kind := range []StatementKind{UnknownStatement, OrdinaryStatement, ConditionalStatement} {
		for _, cql := range []string{"SELECT value FROM ks.tbl", "INSERT INTO t (x) VALUES (1) IF NOT EXISTS", "SELECT 'IF' FROM t"} {
			b, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Kind: kind, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
			if err != nil {
				t.Fatal(err)
			}
			if b.Routing().Kind != kind || b.Routing().Conditional != (kind != OrdinaryStatement) {
				t.Fatal(cql, b.Routing())
			}
			b.Close()
		}
	}
	s, cs, _ := runtimeTestSession(t, 1)
	for _, test := range []struct {
		negotiated, lwt bool
		want            StatementKind
	}{{false, false, UnknownStatement}, {true, false, OrdinaryStatement}, {true, true, ConditionalStatement}, {false, true, ConditionalStatement}} {
		cs[0].framers.defaults.flagLWT = 0
		if test.negotiated {
			cs[0].framers.defaults.flagLWT = 1 << 30
		}
		info, err := s.hostPreparedInfo(cs[0], &responsePage{responseData: responseData{Lwt: test.lwt}}, &preparedState{statement: "INSERT x IF y"})
		if err != nil || info.Kind != test.want {
			t.Fatal(info, err)
		}
	}
}

type step07CountSpeculation struct{ calls atomic.Int32 }

func (p *step07CountSpeculation) Delay(int, MetricsSnapshot) (time.Duration, bool) {
	p.calls.Add(1)
	return 0, false
}
func TestStep07UnknownConditionalNeverSpeculateAndProfileStable(t *testing.T) {
	for _, kind := range []StatementKind{UnknownStatement, OrdinaryStatement, ConditionalStatement} {
		t.Run(map[StatementKind]string{UnknownStatement: "unknown", OrdinaryStatement: "ordinary", ConditionalStatement: "conditional"}[kind], func(t *testing.T) {
			s, cs, ps := runtimeTestSession(t, 1)
			spec := &step07CountSpeculation{}
			profile := defaultProfile()
			profile.Speculation = spec
			profile.Prefetch = 0
			handle, _ := NewProfile(profile)
			seen := make(chan struct{})
			resume := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, cs[0], ps[0])
				close(seen)
				<-resume
				_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "result")))
			}()
			b := runtimeQuery(t)
			b.request.routing.Kind = kind
			b.request.routing.Conditional = kind != OrdinaryStatement
			result := make(chan error, 1)
			go func() {
				page, err := RunPage(context.Background(), s, b, StartPage(), Options{Profile: handle, Idempotent: true})
				if page != nil {
					page.Close()
				}
				result <- err
			}()
			<-seen
			profile.Speculation = nil
			profile.Consistency = One
			handle.Replace(profile)
			close(resume)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			want := int32(0)
			if kind == OrdinaryStatement {
				want = 1
			}
			if spec.calls.Load() != want {
				t.Fatal("speculation classification", kind, spec.calls.Load())
			}
			b.Close()
			<-done
		})
	}
}
func TestStep07SessionClosePreservesDeliveredPageAndDrains(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(map[bool]string{false: "page", true: "rows"}[automatic], func(t *testing.T) {
			s, cs, ps := runtimeTestSession(t, 1)
			s.ctx, s.cancel = context.WithCancel(context.Background())
			go func() {
				stream := typedTestReadRequest(t, cs[0], ps[0])
				_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "retained")))
			}()
			b := runtimeQuery(t)
			var view Bytes
			var advance func() bool
			var closeRows func() error
			var retain func() (*PageLease, error)
			if automatic {
				r, err := Run(context.Background(), s, b, Options{})
				if err != nil {
					t.Fatal(err)
				}
				if !r.Next() {
					t.Fatal(r.Err())
				}
				view = r.Row()
				advance = r.Next
				closeRows = r.Close
				retain = r.Retain
			} else {
				p, err := RunPage(context.Background(), s, b, StartPage(), Options{})
				if err != nil {
					t.Fatal(err)
				}
				if !p.Next() {
					t.Fatal(p.Err())
				}
				view = p.Row()
				advance = p.Next
				closeRows = p.Close
				retain = p.Retain
			}
			lease, err := retain()
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			b.Close()
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); s.Close() }()
			}
			wg.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := b.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if string(view.OwnedBytes()) != "retained" {
				t.Fatal("shutdown invalidated current row")
			}
			if advance() {
				t.Fatal("advance after shutdown")
			}
			closeRows()
			if string(view.OwnedBytes()) != "retained" {
				t.Fatal("retained lease lost")
			}
			if _, err := Run(context.Background(), s, b, Options{}); !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}
func TestStep07BatchAdmissionRollbackAndClassification(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	a := runtimeQuery(t)
	b := runtimeQuery(t)
	b.Close()
	ea, _ := BatchEntry(a)
	eb, _ := BatchEntry(b)
	_, err := RunBatch(context.Background(), s, LoggedBatch, []BatchItem{ea, eb}, NoRows, Options{})
	if !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Wait(ctx); err != nil {
		t.Fatal("partial batch admission retained", err)
	}
}

type step07PanicObserver struct{}

func (step07PanicObserver) ObserveRequest(RequestEvent) { panic("request") }
func (step07PanicObserver) ObserveAttempt(AttemptEvent) { panic("attempt") }

type step07PanicLogger struct{}

func (step07PanicLogger) Print(...any)          { panic("logger") }
func (step07PanicLogger) Printf(string, ...any) { panic("logger") }
func (step07PanicLogger) Println(...any)        { panic("logger") }
func TestStep07ObserverAndLoggerPanicsContained(t *testing.T) {
	logger := v2SafeLogger{step07PanicLogger{}}
	logger.Print("x")
	logger.Printf("%s", "x")
	logger.Println("x")
	s, cs, ps := runtimeTestSession(t, 1)
	s.logger = step07PanicLogger{}
	go func() {
		stream := typedTestReadRequest(t, cs[0], ps[0])
		_, _ = ps[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
	}()
	b := runtimeQuery(t)
	defer b.Close()
	rows, err := Run(context.Background(), s, b, Options{Observer: step07PanicObserver{}})
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	rows.Close()
	if m := s.Metrics(); m.Requests != 1 || m.Attempts != 1 || m.InFlight != 0 {
		t.Fatal(m)
	}
}
func TestStep07ConfigurationDefaultsAndZeroSemantics(t *testing.T) {
	c := DefaultConfig("localhost")
	p := c.DefaultProfile.Snapshot()
	if c.Metadata.PeriodicRefresh != time.Minute || c.Metadata.RequestTimeout != 31*time.Second || p.Timeout != 30*time.Second || p.PageTimeout != 0 || p.Prefetch != 1 || p.PageSize != 5000 || p.Consistency != LocalQuorum {
		t.Fatal(c.Metadata, p)
	}
	for _, edit := range []func(*Config){func(c *Config) { c.Metadata.AgreementTimeout = 0 }, func(c *Config) { c.Metadata.AgreementPoll = 0 }, func(c *Config) { c.Metadata.ServerTimeout = Option[time.Duration]{Set: true, Value: time.Nanosecond} }} {
		invalid := DefaultConfig("localhost")
		edit(&invalid)
		if validateConfig(invalid) == nil {
			t.Fatal("invalid explicit setting accepted")
		}
	}
	c.ConnectTimeout = 0
	c.DNSTimeout = 0
	c.Metadata.PeriodicRefresh = 0
	c.Metadata.RequestTimeout = 0
	c.Metadata.ServerTimeout = Option[time.Duration]{Set: true, Value: 0}
	if err := validateConfig(c); err != nil {
		t.Fatal(err)
	}
	s := &Session{}
	s.typedRuntime().profile = c.DefaultProfile
	options := Options{Timeout: Option[time.Duration]{Set: true}, PageTimeout: Option[time.Duration]{Set: true}, SerialConsistency: Option[Option[Consistency]]{Set: true}, Prefetch: Option[int]{Set: true}, Timestamp: Option[int64]{Set: true}, NowInSeconds: Option[int32]{Set: true}}
	resolved, err := resolveOptions(s, options)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.profile.Timeout != 0 || resolved.profile.PageTimeout != 0 || resolved.profile.Prefetch != 0 || resolved.profile.SerialConsistency.Set || !resolved.options.Timestamp.Set || !resolved.options.NowInSeconds.Set {
		t.Fatal(resolved)
	}
}

func TestStep07QualifiedLegacyProtocolKeyspace(t *testing.T) {
	for _, test := range []struct {
		cql, keyspace string
		want          bool
	}{
		{"SELECT value FROM ks.tbl", "ks", true}, {"INSERT INTO ks.tbl(k)VALUES('ks.tbl')", "ks", true}, {"UPDATE ks.tbl SET value=1 WHERE k=2", "ks", true}, {"DELETE FROM ks.tbl WHERE k=2", "ks", true}, {"TRUNCATE TABLE ks.tbl", "ks", true}, {"CREATE TABLE IF NOT EXISTS ks.tbl(k int PRIMARY KEY)", "ks", true}, {"SELECT * FROM \"Ks\".\"Tbl\"", "Ks", true}, {"SELECT * FROM tbl WHERE value='ks.tbl'", "ks", false}, {"SELECT * FROM other.tbl -- ks.tbl", "ks", false}, {"SELECT * FROM ks.tbl /* unfinished", "ks", false},
	} {
		if got := qualifiedForKeyspace(test.cql, test.keyspace); got != test.want {
			t.Fatal(test.cql, got, test.want)
		}
	}
}

func TestStep07HostPinSurvivesCustomPolicy(t *testing.T) {
	s, cs, _ := runtimeTestSession(t, 2)
	pinned := cs[0].host.hostUUID()
	other := cs[1].host.hostUUID()
	plan := &targetPlan{targets: []Target{{HostID: other}}}
	if conn, err := s.selectTypedConn(context.Background(), RoutingInfo{HostID: Option[UUID]{Set: true, Value: pinned}}, plan); conn != nil || !errors.Is(err, ErrNoConnections) {
		t.Fatal("host pin escaped", conn, err)
	}
	if _, err := s.selectTypedConn(context.Background(), RoutingInfo{}, &targetPlan{targets: []Target{{HostID: pinned, StrictShard: true}}}); err == nil {
		t.Fatal("strict target without shard accepted")
	}
}
func TestStep07SafeRetryBytesWritten(t *testing.T) {
	for _, written := range []bool{false, true} {
		err := &QueryError{err: errors.New("write"), potentiallyExecuted: written}
		decision := (SafeRetryPolicy{}).Decide(AttemptError{Err: err, Attempt: 1, BytesWritten: attemptBytesWritten(err)})
		if (decision.Action == RetryNext) == written {
			t.Fatal(written, decision)
		}
	}
	if !attemptBytesWritten(errors.New("unknown completion")) {
		t.Fatal("unproven write classified safe")
	}
}

func TestStep07ManualAndBatchPagingSettingsAreExplicit(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	b := runtimeQuery(t)
	defer b.Close()
	_, err := RunUnpaged(context.Background(), s, b, Options{PageSize: Option[int32]{Set: true, Value: 1}})
	var unsupported *UnsupportedSettingError
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
	_, err = RunPage(context.Background(), s, b, StartPage(), Options{Prefetch: Option[int]{Set: true, Value: 1}})
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
	item, _ := BatchEntry(b)
	_, err = RunBatch(context.Background(), s, LoggedBatch, []BatchItem{item}, NoRows, Options{PageSize: Option[int32]{Set: true, Value: 1}})
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
}
