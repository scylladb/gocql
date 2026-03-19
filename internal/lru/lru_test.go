//go:build unit
// +build unit

/*
Copyright 2015 To gocql authors
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package lru

import (
	"fmt"
	"testing"
)

var getTests = []struct {
	name       string
	keyToAdd   string
	keyToGet   string
	expectedOk bool
}{
	{"string_hit", "mystring", "mystring", true},
	{"string_miss", "mystring", "nonsense", false},
	{"simple_struct_hit", "two", "two", true},
	{"simeple_struct_miss", "two", "noway", false},
}

func TestGet(t *testing.T) {
	t.Parallel()

	for _, tt := range getTests {
		lru := New[string](0)
		lru.Add(tt.keyToAdd, 1234)
		val, ok := lru.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

func TestRemove(t *testing.T) {
	t.Parallel()

	lru := New[string](0)
	lru.Add("mystring", 1234)
	if val, ok := lru.Get("mystring"); !ok {
		t.Fatal("TestRemove returned no match")
	} else if val != 1234 {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove("mystring")
	if _, ok := lru.Get("mystring"); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

// TestStructKey verifies that struct keys work correctly with the generic cache.
func TestStructKey(t *testing.T) {
	t.Parallel()

	type compositeKey struct {
		A string
		B string
	}

	c := New[compositeKey](0)
	k1 := compositeKey{A: "ab", B: "cd"}
	k2 := compositeKey{A: "a", B: "bcd"}

	c.Add(k1, "value1")
	c.Add(k2, "value2")

	if val, ok := c.Get(k1); !ok || val != "value1" {
		t.Fatalf("expected value1 for k1, got %v (ok=%v)", val, ok)
	}
	if val, ok := c.Get(k2); !ok || val != "value2" {
		t.Fatalf("expected value2 for k2, got %v (ok=%v)", val, ok)
	}

	// Verify that keys with same concatenation but different field boundaries
	// are distinct (this was a bug with string concatenation keys).
	if c.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", c.Len())
	}
}

type stmtKey struct {
	hostID    string
	keyspace  string
	statement string
}

// BenchmarkStructKeyLookup benchmarks the hot path: looking up a struct key
// in a populated cache.
func BenchmarkStructKeyLookup(b *testing.B) {
	c := New[stmtKey](1000)
	key := stmtKey{
		hostID:    "550e8400-e29b-41d4-a716-446655440000",
		keyspace:  "my_keyspace",
		statement: "SELECT id, name, email FROM users WHERE id = ?",
	}
	c.Add(key, "prepared-id")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Get(key)
	}
}

// BenchmarkStringKeyLookup benchmarks the old approach: looking up a
// concatenated string key in a populated cache.
func BenchmarkStringKeyLookup(b *testing.B) {
	c := New[string](1000)
	key := "550e8400-e29b-41d4-a716-446655440000" + "my_keyspace" + "SELECT id, name, email FROM users WHERE id = ?"
	c.Add(key, "prepared-id")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Get(key)
	}
}

// BenchmarkStructKeyInsert benchmarks inserting entries with struct keys,
// including eviction when the cache is full.
func BenchmarkStructKeyInsert(b *testing.B) {
	c := New[stmtKey](1000)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := stmtKey{
			hostID:    "550e8400-e29b-41d4-a716-446655440000",
			keyspace:  "my_keyspace",
			statement: fmt.Sprintf("SELECT id FROM users WHERE id = %d", i),
		}
		c.Add(k, "prepared-id")
	}
}

// BenchmarkStringKeyInsert benchmarks inserting entries with concatenated
// string keys, including the per-query allocation cost of key construction.
func BenchmarkStringKeyInsert(b *testing.B) {
	c := New[string](1000)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("%s%s%s", "550e8400-e29b-41d4-a716-446655440000", "my_keyspace", fmt.Sprintf("SELECT id FROM users WHERE id = %d", i))
		c.Add(k, "prepared-id")
	}
}
