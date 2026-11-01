//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This subprocess is a normal consumer build: root _test.go aliases cannot make
// the standalone example compile or run.
func TestStep13PublicExample(t *testing.T) {
	cfg := createV2Cluster()
	host := getClusterHosts()[0]
	args := []string{"run", "./examples/streaming", "-hosts", net.JoinHostPort(host, strconv.Itoa(int(cfg.Port))), "-protocol", strconv.Itoa(cfg.Protocol)}
	switch cfg.Compression {
	case LZ4Compression:
		args = append(args, "-compression", "lz4")
	case SnappyCompression:
		args = append(args, "-compression", "snappy")
	}
	if cfg.Auth != nil {
		args = append(args, "-username", "cassandra", "-password", "cassandra")
	}
	if cfg.TLS != nil {
		args = append(args, "-ca", "testdata/pki/ca.crt", "-cert", "testdata/pki/gocql.crt", "-key", "testdata/pki/gocql.key", "-tls-uri", "spiffe://test.cassandra.apache.org/cassandra-gocql-driver/integrationTest/cassandra")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", args...)
	output, err := cmd.CombinedOutput()
	t.Logf("command: go %s\n%s", strings.Join(args, " "), output)
	if err != nil {
		t.Fatal(err)
	}
	want := "3 rows; seeded nested storage reused; pool occupancy and binding drain verified"
	if cfg.Protocol < 5 && cfg.Compression != NoCompression {
		want = "frame compression explicitly rejected before streaming submission"
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "CLEANED ") {
			step13Namespace(t, cfg, strings.TrimPrefix(line, "CLEANED "), false)
		}
	}
	if !strings.Contains(string(output), "CLEANED ") {
		t.Fatal("example did not confirm cleanup")
	}
	if !strings.Contains(string(output), want) {
		t.Fatalf("example did not report verified behavior: %s", output)
	}
}

func TestStep13RegenerateReplay(t *testing.T) {
	if os.Getenv("GOCQL_REGENERATE_BENCH") != "1" {
		t.Skip("explicit recording mode only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-race", "-count=1", "-run", "^TestSingleConnectionReplay$", "./tests/bench", "-args", "-update-golden")
	cmd.Env = append(os.Environ(), "GOCQL_BENCH_ADDRESS="+net.JoinHostPort(getClusterHosts()[0], "9042"))
	output, err := cmd.CombinedOutput()
	t.Logf("generation command: %s\n%s", strings.Join(cmd.Args, " "), output)
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Namespace string `json:"namespace"`
	}
	data, err := os.ReadFile("tests/bench/corpus.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	step13Namespace(t, createV2Cluster(), corpus.Namespace, false)
	t.Log("GENERATED CORPUS NAMESPACE ABSENT", corpus.Namespace)
}

// TestStep13Ownership executes the actual compiled children. A killed child is
// joined before the existing runner prefix cleanup is allowed to remove its data.
func TestStep13Ownership(t *testing.T) {
	if os.Getenv("GOCQL_TEST_RUN") == "" {
		t.Skip("requires locked native run")
	}
	cfg := createV2Cluster()
	if cfg.Protocol != 4 || cfg.TLS != nil {
		t.Skip("raw v4 ownership lane")
	}
	dir := t.TempDir()
	example := filepath.Join(dir, "example")
	benchmark := filepath.Join(dir, "benchmark.test")
	for _, command := range [][]string{{"build", "-o", example, "./examples/streaming"}, {"test", "-c", "-o", benchmark, "./tests/bench"}} {
		cmd := exec.Command("go", command...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build: %v\n%s", err, output)
		}
	}
	host := net.JoinHostPort(getClusterHosts()[0], "9042")
	exampleArgs := []string{"-hosts", host, "-protocol", "4"}
	if cfg.Auth != nil {
		exampleArgs = append(exampleArgs, "-username", "cassandra", "-password", "cassandra")
	}
	var killed []string
	for _, workload := range []string{"example", "benchmark"} {
		for _, failure := range []string{"error", "cancel", "pause"} {
			t.Run(workload+"/"+failure, func(t *testing.T) {
				binary, args := example, exampleArgs
				env := append(os.Environ(), "GOCQL_EXAMPLE_FAILURE="+failure)
				if workload == "benchmark" {
					binary = benchmark
					args = []string{"-update-golden", "-test.run=^$"}
					env = append(os.Environ(), "GOCQL_BENCH_FAILURE="+failure, "GOCQL_BENCH_ADDRESS="+host, "GOCQL_BENCH_CORPUS_DIR="+t.TempDir())
				}
				namespace, output, err := step13OwnedChild(t, binary, args, env, failure == "pause", cfg)
				t.Logf("owned %s %s namespace=%s\n%s", workload, failure, namespace, output)
				if err == nil {
					t.Fatal("injected failure unexpectedly succeeded")
				}
				if failure == "pause" {
					if strings.Contains(output, "CLEANED ") {
						t.Fatal("killed child unexpectedly cleaned")
					}
					killed = append(killed, namespace)
					return
				}
				if !strings.Contains(output, "CLEANED "+namespace) {
					t.Fatal("cleanup did not complete")
				}
				if failure == "error" && !strings.Contains(output, "injected ") {
					t.Fatal("missing injected error")
				}
				if failure == "cancel" && !strings.Contains(output, "context canceled") {
					t.Fatal("missing cancellation error")
				}
				step13Namespace(t, cfg, namespace, false)
			})
		}
	}
	// Both actual binaries have exited and been joined above. Exercise the same
	// cleanup implementation as the native runner, scoped to these exact children.
	step08CleanupNamespaces(t, killed)
	for _, namespace := range killed {
		step13Namespace(t, cfg, namespace, false)
		t.Log("KILLED CHILD PREFIX CLEANUP VERIFIED", namespace)
	}
}

func step13Namespace(t *testing.T, cfg Config, namespace string, wantPresent bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.AwaitSchemaAgreement(ctx); err != nil {
		t.Fatal("namespace observation schema agreement", err)
	}
	value, err := TextValue(namespace)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := v2Query(ctx, s, "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", value)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	present := rows.Next()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if present != wantPresent {
		t.Fatalf("namespace %s present=%t want=%t", namespace, present, wantPresent)
	}
}
func step13OwnedChild(t *testing.T, binary string, args, env []string, kill bool, cfg Config) (string, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	output := make(chan string, 1)
	go func() {
		var text strings.Builder
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			line := scanner.Text()
			text.WriteString(line + "\n")
			if strings.HasPrefix(line, "READY ") {
				ready <- strings.TrimPrefix(line, "READY ")
			}
		}
		if err := scanner.Err(); err != nil {
			text.WriteString(err.Error())
		}
		output <- text.String()
	}()
	joined := false
	t.Cleanup(func() {
		if !joined {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	var namespace string
	select {
	case namespace = <-ready:
	case text := <-output:
		_ = cmd.Wait()
		joined = true
		t.Fatalf("child ended before creating namespace: %s", text)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !strings.HasPrefix(namespace, os.Getenv("GOCQL_TEST_RUN")+"_") {
		t.Fatal("child escaped run prefix", namespace)
	}
	if kill {
		step13Namespace(t, cfg, namespace, true)
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	}
	text := <-output
	err = cmd.Wait()
	joined = true
	return namespace, text, err
}

type step13Conviction struct {
	mu      sync.Mutex
	cause   error
	hosts   []HostSnapshot
	targets []HostSnapshot
	verdict bool
	resets  int
}

func (p *step13Conviction) AddFailure(err error, host HostSnapshot) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !errors.Is(err, p.cause) {
		panic("conviction lost original failure")
	}
	host.Tokens = append([]int64(nil), host.Tokens...)
	p.hosts = append(p.hosts, host)
	return p.verdict
}
func (p *step13Conviction) Reset(host HostSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.hosts) == 0 || p.hosts[0].ID != host.ID {
		panic("reset lost host identity")
	}
	p.resets++
}

type step13NoReconnect struct{}

func (step13NoReconnect) Delay(int) (time.Duration, bool) { return 0, false }

// The control connection completes real startup, then the data dial fails. The
// public conviction hook must receive the discovered host, not a mutable core.
func TestStep13ConvictionLive(t *testing.T) {
	cfg := createV2Cluster()
	if cfg.Protocol != 4 || cfg.TLS != nil {
		t.Skip("raw v4 callback lane")
	}
	for _, verdict := range []bool{false, true} {
		t.Run(fmt.Sprint(verdict), func(t *testing.T) {
			cause := errors.New("injected data connection failure")
			policy := &step13Conviction{cause: cause, verdict: verdict}
			config := createV2Cluster(func(c *Config) {
				c.ContactPoints = []string{getClusterHosts()[0]}
				c.Metadata.InitialDiscovery = false
				c.Pool.ConnectionsPerHost = 1
				c.Pool.ShardAwarePort = false
				c.Reconnect = step13NoReconnect{}
				c.Conviction = policy
				c.KeepaliveInterval = 0
				c.Metadata.PeriodicRefresh = 0
			})
			var dials atomic.Int32
			config.Dialer = DialerFunc(func(ctx context.Context, target DialTarget) (net.Conn, error) {
				if dials.Add(1) > 1 {
					policy.mu.Lock()
					policy.targets = append(policy.targets, target.Host)
					policy.mu.Unlock()
					return nil, cause
				}
				return (DefaultDialer{}).DialContext(ctx, target)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s, err := Connect(ctx, config)
			if s != nil {
				s.Close()
			}
			if err == nil {
				t.Fatal("failed data connection unexpectedly connected")
			}
			policy.mu.Lock()
			hosts := append([]HostSnapshot(nil), policy.hosts...)
			targets := append([]HostSnapshot(nil), policy.targets...)
			resets := policy.resets
			policy.mu.Unlock()
			if len(hosts) == 0 || len(targets) == 0 {
				t.Fatal("custom policy not called", err)
			}
			for _, host := range hosts {
				if !reflect.DeepEqual(host, targets[0]) {
					t.Fatalf("conviction lost dial host context: got=%+v want=%+v", host, targets[0])
				}
				if host.ID.IsEmpty() || host.Address.Addr().String() != getClusterHosts()[0] || host.Datacenter == "" || host.Rack == "" || !host.Enabled || host.Connected {
					t.Fatalf("incomplete host context: %+v", host)
				}
			}
			if resets != 0 {
				t.Fatal("driver introduced a new reset call")
			}
			config.Conviction.Reset(hosts[0])
			policy.mu.Lock()
			resets = policy.resets
			policy.mu.Unlock()
			if resets != 1 {
				t.Fatal("public reset callback not usable")
			}
			t.Log("PUBLIC CONVICTION CALLBACK", verdict, hosts[0].ID, hosts[0].Address, len(hosts[0].Tokens))
		})
	}
}
