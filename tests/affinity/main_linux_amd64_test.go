//go:build linux && amd64

// Copyright 2026 The Apache Software Foundation.
// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE
// in the repository root, or https://www.apache.org/licenses/LICENSE-2.0.

package main

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"unsafe"
)

func checkLRU(t *testing.T, c *cache) {
	t.Helper()
	seen := make(map[int32]bool)
	previous := int32(-1)
	for i := c.head; i >= 0; i = c.entries[i].next {
		if int(i) >= len(c.entries) || seen[i] {
			t.Fatal("invalid/cyclic LRU")
		}
		seen[i] = true
		if c.entries[i].prev != previous {
			t.Fatal("broken previous link")
		}
		previous = i
	}
	if previous != c.tail || len(seen) != len(c.entries) {
		t.Fatal("lost LRU entries")
	}
}

func TestCacheAndEncodingEquivalentAcrossOwnership(t *testing.T) {
	if unsafe.Sizeof(entry{}) != 128 {
		t.Fatal("entry size changed")
	}
	shared := newCache(0, 128, true)
	for worker := 0; worker < 4; worker++ {
		private := newCache(worker*32, 32, false)
		for seq := uint64(0); seq < 10000; seq++ {
			key := uint64(worker*32) + seq*17%32
			a, b := make([]byte, 32), make([]byte, 32)
			encode(a, key, seq, shared.lookup(key, seq%100 == 0, seq))
			encode(b, key, seq, private.lookup(key, seq%100 == 0, seq))
			if !bytes.Equal(a, b) {
				t.Fatalf("different records: worker=%d sequence=%d", worker, seq)
			}
		}
		checkLRU(t, private)
	}
	checkLRU(t, shared)
	var wire [32]byte
	encode(wire[:], 3, 7, [4]uint64{10, 20, 30, 40})
	for i, want := range []uint64{14, 16, 26, 44} {
		if got := binary.BigEndian.Uint64(wire[i*8:]); got != want {
			t.Fatalf("field=%d got=%d want=%d", i, got, want)
		}
	}
}

func TestSharedCacheConcurrentLRUMutation(t *testing.T) {
	c := newCache(0, 128, true)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for seq := uint64(0); seq < 10000; seq++ {
				c.lookup((seq*17+uint64(worker))%128, seq%100 == 0, seq)
			}
		}(worker)
	}
	wg.Wait()
	checkLRU(t, c)
}

func TestInputStreamsCoverPartitionWithoutOverlap(t *testing.T) {
	cfg := config{Mode: "cpu", Cache: "sharded", Batch: 32}
	seen := make(map[uint64]int)
	for worker := 0; worker < 4; worker++ {
		o, err := makeOperation(cfg, worker, worker, 2048, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range o.keys {
			if owner, exists := seen[key]; exists {
				t.Fatalf("duplicate key %d owners=%d/%d", key, owner, worker)
			}
			seen[key] = worker
		}
	}
	if len(seen) != 8192 {
		t.Fatal("incomplete working set")
	}
}

func TestLatencySamplingCoversRepeatingInputPhases(t *testing.T) {
	for _, blocks := range []uint64{64, 4096} {
		sampler := newLatencySampler(1234, 64)
		phases := make(map[uint64]bool)
		windowSamples := 0
		for batch := uint64(0); batch < 32768*256; batch++ {
			if batch&255 == 0 {
				windowSamples = 0
			}
			if sampler.selectBatch(batch) {
				windowSamples++
				phases[batch%blocks] = true
				sampler.record(int64(batch))
			}
			if batch&255 == 255 && windowSamples != 1 {
				t.Fatal("expected one sample per window")
			}
		}
		if len(phases) < int(blocks)*99/100 {
			t.Fatalf("phase-locked sampling: %d/%d", len(phases), blocks)
		}
		if sampler.seen != 32768 || len(sampler.samples) != 64 {
			t.Fatal("unbounded/incomplete reservoir")
		}
		late := false
		for _, value := range sampler.samples {
			if value > 16384*256 {
				late = true
			}
		}
		if !late {
			t.Fatal("reservoir retained only early observations")
		}
	}
}

func TestSocketStatusNeverClaimsProvenAffinity(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		requested            bool
		setup, before, after int
		other                uint64
		want                 string
	}{
		{"default", false, -1, -1, -1, 0, "not-requested"},
		{"setup failure", true, -1, 2, 2, 0, "setup-failed"},
		{"lost during warmup", true, 2, 3, 2, 0, "unsupported-observed-mismatch"},
		{"lost during run", true, 2, 2, 2, 1, "unsupported-observed-mismatch"},
		{"lost at end", true, 2, 2, 3, 0, "unsupported-observed-mismatch"},
		{"samples match", true, 2, 2, 2, 0, "unverified-no-mismatch-observed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := socketAffinityStatus(tc.requested, 2, tc.setup, tc.before, tc.after, tc.other); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}
