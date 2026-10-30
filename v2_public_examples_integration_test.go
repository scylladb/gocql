//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
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
}
