//go:build integration

// Copyright (C) 2017 ScyllaDB

package gocql

import "testing"

func TestSplitStatements(t *testing.T) {
	in := "CREATE TABLE t (a int);CREATE FUNCTION f() AS $$ a; b; $$;SELECT 1"
	want := []string{
		"CREATE TABLE t (a int)",
		"CREATE FUNCTION f() AS $$ a; b; $$",
		"SELECT 1",
	}
	got := splitStatements(in)
	if len(got) != len(want) {
		t.Fatalf("got %d statements, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement %d = %q, want %q", i, got[i], want[i])
		}
	}
}
