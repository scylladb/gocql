//go:build unit
// +build unit

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

package gocql

import (
	"crypto/rand"
	"fmt"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// TestResolveCompressionThreshold verifies that the per-connection compression
// threshold is resolved correctly from CompressionPolicy, the host selection
// policy, and the target host.
func TestResolveCompressionThreshold(t *testing.T) {
	t.Parallel()

	localRackHost := &HostInfo{dataCenter: "dc1", rack: "rack1"}
	localDCRemoteRackHost := &HostInfo{dataCenter: "dc1", rack: "rack2"}
	remoteDCHost := &HostInfo{dataCenter: "dc2", rack: "rack1"}

	t.Run("zero value policy returns 0 for all hosts", func(t *testing.T) {
		policy := CompressionPolicy{} // zero value
		rackPolicy := RackAwareRoundRobinPolicy("dc1", "rack1")
		rackPolicy.AddHost(localRackHost)

		for _, host := range []*HostInfo{localRackHost, localDCRemoteRackHost, remoteDCHost} {
			threshold := resolveCompressionThreshold(policy, rackPolicy, host)
			if threshold != 0 {
				t.Errorf("expected threshold 0 for host %s/%s, got %d",
					host.DataCenter(), host.Rack(), threshold)
			}
		}
	})

	t.Run("RackAware with CompressNonLocalRack", func(t *testing.T) {
		policy := CompressionPolicy{
			MinCompressLocalSize:  neverCompressSize,
			MinCompressRemoteSize: 1024,
			Scope:                 CompressNonLocalRack,
		}
		rackPolicy := RackAwareRoundRobinPolicy("dc1", "rack1")
		rackPolicy.AddHost(localRackHost)

		// Local rack → local threshold
		threshold := resolveCompressionThreshold(policy, rackPolicy, localRackHost)
		if threshold != neverCompressSize {
			t.Errorf("expected threshold %d for local rack, got %d", neverCompressSize, threshold)
		}

		// Remote rack, same DC → remote threshold
		threshold = resolveCompressionThreshold(policy, rackPolicy, localDCRemoteRackHost)
		if threshold != 1024 {
			t.Errorf("expected threshold 1024 for remote rack, got %d", threshold)
		}

		// Remote DC → remote threshold
		threshold = resolveCompressionThreshold(policy, rackPolicy, remoteDCHost)
		if threshold != 1024 {
			t.Errorf("expected threshold 1024 for remote DC, got %d", threshold)
		}
	})

	t.Run("RackAware with CompressNonLocalDC", func(t *testing.T) {
		policy := CompressionPolicy{
			MinCompressLocalSize:  5000,
			MinCompressRemoteSize: 500,
			Scope:                 CompressNonLocalDC,
		}
		rackPolicy := RackAwareRoundRobinPolicy("dc1", "rack1")
		rackPolicy.AddHost(localRackHost)

		// Local rack → local (same DC, tier < maxTier)
		threshold := resolveCompressionThreshold(policy, rackPolicy, localRackHost)
		if threshold != 5000 {
			t.Errorf("expected threshold 5000 for local rack, got %d", threshold)
		}

		// Remote rack, same DC → local (still same DC, tier < maxTier)
		threshold = resolveCompressionThreshold(policy, rackPolicy, localDCRemoteRackHost)
		if threshold != 5000 {
			t.Errorf("expected threshold 5000 for remote rack same DC, got %d", threshold)
		}

		// Remote DC → remote (tier == maxTier)
		threshold = resolveCompressionThreshold(policy, rackPolicy, remoteDCHost)
		if threshold != 500 {
			t.Errorf("expected threshold 500 for remote DC, got %d", threshold)
		}
	})

	t.Run("DCAware with CompressNonLocalRack", func(t *testing.T) {
		// dcAwareRR has only 2 tiers: 0=local DC, 1=remote DC.
		// With CompressNonLocalRack scope, tier 0 is local, everything else remote.
		policy := CompressionPolicy{
			MinCompressLocalSize:  2000,
			MinCompressRemoteSize: 200,
			Scope:                 CompressNonLocalRack,
		}
		dcPolicy := DCAwareRoundRobinPolicy("dc1")
		dcPolicy.AddHost(localRackHost)

		// Local DC → local (tier 0)
		threshold := resolveCompressionThreshold(policy, dcPolicy, localRackHost)
		if threshold != 2000 {
			t.Errorf("expected threshold 2000 for local DC, got %d", threshold)
		}

		// Remote DC → remote (tier 1)
		threshold = resolveCompressionThreshold(policy, dcPolicy, remoteDCHost)
		if threshold != 200 {
			t.Errorf("expected threshold 200 for remote DC, got %d", threshold)
		}
	})

	t.Run("DCAware with CompressNonLocalDC", func(t *testing.T) {
		policy := CompressionPolicy{
			MinCompressLocalSize:  3000,
			MinCompressRemoteSize: 100,
			Scope:                 CompressNonLocalDC,
		}
		dcPolicy := DCAwareRoundRobinPolicy("dc1")
		dcPolicy.AddHost(localRackHost)

		// Local DC → local (tier 0 < maxTier 1)
		threshold := resolveCompressionThreshold(policy, dcPolicy, localRackHost)
		if threshold != 3000 {
			t.Errorf("expected threshold 3000 for local DC, got %d", threshold)
		}

		// Remote DC → remote (tier 1 == maxTier 1)
		threshold = resolveCompressionThreshold(policy, dcPolicy, remoteDCHost)
		if threshold != 100 {
			t.Errorf("expected threshold 100 for remote DC, got %d", threshold)
		}
	})

	t.Run("RoundRobin policy without HostTierer", func(t *testing.T) {
		// RoundRobin doesn't implement HostTierer, so all hosts should
		// be treated as local.
		policy := CompressionPolicy{
			MinCompressLocalSize:  4096,
			MinCompressRemoteSize: 256,
			Scope:                 CompressNonLocalRack,
		}
		rrPolicy := RoundRobinHostPolicy()
		rrPolicy.AddHost(localRackHost)

		threshold := resolveCompressionThreshold(policy, rrPolicy, localRackHost)
		if threshold != 4096 {
			t.Errorf("expected threshold 4096 for RoundRobin, got %d", threshold)
		}

		threshold = resolveCompressionThreshold(policy, rrPolicy, remoteDCHost)
		if threshold != 4096 {
			t.Errorf("expected threshold 4096 for RoundRobin remote host, got %d", threshold)
		}
	})

	t.Run("TokenAware wrapping DCAware delegates HostTierer", func(t *testing.T) {
		policy := CompressionPolicy{
			MinCompressLocalSize:  3000,
			MinCompressRemoteSize: 100,
			Scope:                 CompressNonLocalDC,
		}
		dcPolicy := DCAwareRoundRobinPolicy("dc1")
		dcPolicy.AddHost(localRackHost)
		taPolicy := TokenAwareHostPolicy(dcPolicy)

		// Local DC → local threshold
		threshold := resolveCompressionThreshold(policy, taPolicy, localRackHost)
		if threshold != 3000 {
			t.Errorf("expected threshold 3000 for local DC via TokenAware, got %d", threshold)
		}

		// Remote DC → remote threshold
		threshold = resolveCompressionThreshold(policy, taPolicy, remoteDCHost)
		if threshold != 100 {
			t.Errorf("expected threshold 100 for remote DC via TokenAware, got %d", threshold)
		}
	})

	t.Run("TokenAware wrapping RoundRobin has no HostTierer", func(t *testing.T) {
		policy := CompressionPolicy{
			MinCompressLocalSize:  4096,
			MinCompressRemoteSize: 256,
			Scope:                 CompressNonLocalRack,
		}
		rrPolicy := RoundRobinHostPolicy()
		rrPolicy.AddHost(localRackHost)
		taPolicy := TokenAwareHostPolicy(rrPolicy)

		// RoundRobin fallback has no HostTierer, so tokenAwareHostPolicy
		// returns tier 0 for all hosts → all treated as local.
		threshold := resolveCompressionThreshold(policy, taPolicy, remoteDCHost)
		if threshold != 4096 {
			t.Errorf("expected threshold 4096 for TokenAware(RoundRobin), got %d", threshold)
		}
	})
}

// TestFramerFinishCompressionThreshold verifies that framer.finish() applies
// compression conditionally based on the compressionOpts.threshold field.
func TestFramerFinishCompressionThreshold(t *testing.T) {
	t.Parallel()

	comp := SnappyCompressor{}

	// writeTestFrame creates a framer with the given compressor, opts,
	// and body payload, then calls finish() and returns whether the compress
	// flag is set in the resulting wire bytes.
	writeTestFrame := func(compressor Compressor, opts compressionOpts, bodySize int) (compressedFlagSet bool, bodyBytes int, err error) {
		f := newFramer(compressor, protoVersion4, opts)
		f.writeHeader(f.flags, frm.OpQuery, 1)
		// write a body of the requested size
		f.buf = append(f.buf, make([]byte, bodySize)...)
		err = f.finish()
		if err != nil {
			return false, 0, err
		}
		compressedFlagSet = f.buf[1]&frm.FlagCompress != 0
		bodyBytes = len(f.buf) - headSize
		return compressedFlagSet, bodyBytes, nil
	}

	t.Run("threshold 0 compresses everything", func(t *testing.T) {
		flagSet, _, err := writeTestFrame(comp, compressionOpts{}, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !flagSet {
			t.Error("expected compression flag to be set with threshold 0")
		}
	})

	t.Run("threshold -1 never compresses", func(t *testing.T) {
		flagSet, bodyBytes, err := writeTestFrame(comp, compressionOpts{threshold: neverCompressSize}, 10000)
		if err != nil {
			t.Fatal(err)
		}
		if flagSet {
			t.Error("expected compression flag to NOT be set with threshold -1")
		}
		if bodyBytes != 10000 {
			t.Errorf("expected uncompressed body of 10000 bytes, got %d", bodyBytes)
		}
	})

	t.Run("threshold above body size skips compression", func(t *testing.T) {
		flagSet, bodyBytes, err := writeTestFrame(comp, compressionOpts{threshold: 2048}, 500)
		if err != nil {
			t.Fatal(err)
		}
		if flagSet {
			t.Error("expected compression flag to NOT be set when body < threshold")
		}
		if bodyBytes != 500 {
			t.Errorf("expected uncompressed body of 500 bytes, got %d", bodyBytes)
		}
	})

	t.Run("threshold at body size triggers compression", func(t *testing.T) {
		flagSet, _, err := writeTestFrame(comp, compressionOpts{threshold: 500}, 500)
		if err != nil {
			t.Fatal(err)
		}
		if !flagSet {
			t.Error("expected compression flag to be set when body == threshold")
		}
	})

	t.Run("threshold below body size triggers compression", func(t *testing.T) {
		flagSet, _, err := writeTestFrame(comp, compressionOpts{threshold: 100}, 500)
		if err != nil {
			t.Fatal(err)
		}
		if !flagSet {
			t.Error("expected compression flag to be set when body > threshold")
		}
	})

	t.Run("nil compressor never compresses regardless of threshold", func(t *testing.T) {
		f := newFramer(nil, protoVersion4, compressionOpts{})
		f.writeHeader(f.flags, frm.OpQuery, 1)
		f.buf = append(f.buf, make([]byte, 100)...)
		if err := f.finish(); err != nil {
			t.Fatal(err)
		}
		if f.buf[1]&frm.FlagCompress != 0 {
			t.Error("expected no compression flag with nil compressor")
		}
	})

	t.Run("PREPARE frame strips compress flag before finish", func(t *testing.T) {
		f := newFramer(comp, protoVersion4, compressionOpts{})
		frame := &writePrepareFrame{statement: "SELECT * FROM large_table WHERE id = ?"}
		if err := frame.buildFrame(f, 1); err != nil {
			t.Fatal(err)
		}
		if f.buf[1]&frm.FlagCompress != 0 {
			t.Error("expected PREPARE frame to NOT have compression flag set")
		}
	})

	t.Run("QUERY frame respects threshold", func(t *testing.T) {
		// Small query with high threshold → no compression
		f := newFramer(comp, protoVersion4, compressionOpts{threshold: 10000})
		frame := &writeQueryFrame{
			statement: "INSERT INTO t (id, v) VALUES (?, ?)",
			params:    queryParams{consistency: One},
		}
		if err := frame.buildFrame(f, 1); err != nil {
			t.Fatal(err)
		}
		if f.buf[1]&frm.FlagCompress != 0 {
			t.Error("expected small QUERY frame to NOT be compressed with high threshold")
		}
	})
}

// TestFramerFinishMinSavingsPercent verifies that framer.finish() discards
// compressed output when compression doesn't save enough bytes.
func TestFramerFinishMinSavingsPercent(t *testing.T) {
	t.Parallel()

	comp := SnappyCompressor{}

	// makeCompressibleBody returns a body of the given size that compresses
	// very well (all zeros → near-100% savings with Snappy).
	makeCompressibleBody := func(size int) []byte {
		return make([]byte, size)
	}

	// makeIncompressibleBody returns a body of the given size filled with
	// random bytes that defeat compression (savings ≈ 0%).
	makeIncompressibleBody := func(size int) []byte {
		b := make([]byte, size)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		return b
	}

	// writeTestFrame builds a QUERY frame with the given body, compressionOpts,
	// and returns whether the compress flag is set and the resulting body size.
	writeTestFrame := func(opts compressionOpts, body []byte) (compressedFlagSet bool, bodyBytes int, err error) {
		f := newFramer(comp, protoVersion4, opts)
		f.writeHeader(f.flags, frm.OpQuery, 1)
		f.buf = append(f.buf, body...)
		err = f.finish()
		if err != nil {
			return false, 0, err
		}
		compressedFlagSet = f.buf[1]&frm.FlagCompress != 0
		bodyBytes = len(f.buf) - headSize
		return compressedFlagSet, bodyBytes, nil
	}

	t.Run("minSavingsPct 0 accepts any compression result", func(t *testing.T) {
		// Even incompressible data should be "compressed" (flag set) when
		// minSavingsPct is 0, because the ratio check is disabled.
		body := makeIncompressibleBody(4096)
		flagSet, _, err := writeTestFrame(compressionOpts{minSavingsPct: 0}, body)
		if err != nil {
			t.Fatal(err)
		}
		if !flagSet {
			t.Error("expected compression flag set when minSavingsPct is 0")
		}
	})

	t.Run("compressible data kept with minSavingsPct 15", func(t *testing.T) {
		// All-zero body → Snappy compresses this to a few bytes (>15% savings).
		body := makeCompressibleBody(4096)
		flagSet, bodyBytes, err := writeTestFrame(
			compressionOpts{minSavingsPct: 15}, body)
		if err != nil {
			t.Fatal(err)
		}
		if !flagSet {
			t.Error("expected compression flag set for highly compressible data")
		}
		if bodyBytes >= 4096 {
			t.Errorf("expected compressed body < 4096 bytes, got %d", bodyBytes)
		}
	})

	t.Run("incompressible data discarded with minSavingsPct 15", func(t *testing.T) {
		// Random bytes → Snappy saves ~0%, well below 15%.
		body := makeIncompressibleBody(4096)
		flagSet, bodyBytes, err := writeTestFrame(
			compressionOpts{minSavingsPct: 15}, body)
		if err != nil {
			t.Fatal(err)
		}
		if flagSet {
			t.Error("expected compression flag NOT set for incompressible data with minSavingsPct 15")
		}
		if bodyBytes != 4096 {
			t.Errorf("expected original body of 4096 bytes, got %d", bodyBytes)
		}
	})

	t.Run("minSavingsPct 99 discards nearly all compression", func(t *testing.T) {
		// Even compressible data rarely saves 99%. For a 4KB all-zero body
		// Snappy achieves great compression, but let's use a smaller body
		// where savings might be less dramatic.
		body := makeCompressibleBody(128)
		flagSet, _, err := writeTestFrame(
			compressionOpts{minSavingsPct: 99}, body)
		if err != nil {
			t.Fatal(err)
		}
		// Snappy on 128 zeros: compressed is ~5 bytes → savings ≈ 96%.
		// 96% < 99%, so compression should be discarded.
		if flagSet {
			t.Error("expected compression flag NOT set with minSavingsPct 99 on small body")
		}
	})

	t.Run("zero-length body does not panic", func(t *testing.T) {
		// Edge case: empty body with minSavingsPct set should not
		// divide by zero. bodyLen==0 guard skips the ratio check,
		// so compression proceeds (flag stays set).
		f := newFramer(comp, protoVersion4, compressionOpts{minSavingsPct: 50})
		f.writeHeader(f.flags, frm.OpQuery, 1)
		// Don't append any body bytes.
		err := f.finish()
		if err != nil {
			t.Fatal(err)
		}
		if f.buf[1]&frm.FlagCompress == 0 {
			t.Error("expected compression flag set for zero-length body (guard skips ratio check)")
		}
	})

	t.Run("threshold and minSavingsPct interact correctly", func(t *testing.T) {
		// Body passes the size threshold but fails the ratio check.
		body := makeIncompressibleBody(4096)
		flagSet, bodyBytes, err := writeTestFrame(
			compressionOpts{threshold: 1024, minSavingsPct: 15}, body)
		if err != nil {
			t.Fatal(err)
		}
		if flagSet {
			t.Error("expected compression flag NOT set: passes threshold but fails ratio")
		}
		if bodyBytes != 4096 {
			t.Errorf("expected original body preserved, got %d bytes", bodyBytes)
		}

		// Body passes both threshold and ratio check.
		body = makeCompressibleBody(4096)
		flagSet, bodyBytes, err = writeTestFrame(
			compressionOpts{threshold: 1024, minSavingsPct: 15}, body)
		if err != nil {
			t.Fatal(err)
		}
		if !flagSet {
			t.Error("expected compression flag set: passes both threshold and ratio")
		}
		if bodyBytes >= 4096 {
			t.Errorf("expected compressed body < 4096 bytes, got %d", bodyBytes)
		}
	})
}

// TestDcAwareRRHostTierer verifies that dcAwareRR now implements HostTierer.
func TestDcAwareRRHostTierer(t *testing.T) {
	t.Parallel()

	policy := DCAwareRoundRobinPolicy("dc1")

	tierer, ok := policy.(HostTierer)
	if !ok {
		t.Fatal("dcAwareRR should implement HostTierer")
	}

	if tierer.MaxHostTier() != 1 {
		t.Errorf("expected MaxHostTier() == 1, got %d", tierer.MaxHostTier())
	}

	localHost := &HostInfo{dataCenter: "dc1", rack: "rack1"}
	remoteHost := &HostInfo{dataCenter: "dc2", rack: "rack1"}

	if tier := tierer.HostTier(localHost); tier != 0 {
		t.Errorf("expected tier 0 for local DC host, got %d", tier)
	}
	if tier := tierer.HostTier(remoteHost); tier != 1 {
		t.Errorf("expected tier 1 for remote DC host, got %d", tier)
	}
}

// TestCompressionPolicyDefaults verifies that the zero-value CompressionPolicy
// preserves backward-compatible behaviour (compress everything).
func TestCompressionPolicyDefaults(t *testing.T) {
	t.Parallel()

	var policy CompressionPolicy
	if policy.MinCompressLocalSize != 0 {
		t.Errorf("expected default MinCompressLocalSize == 0, got %d", policy.MinCompressLocalSize)
	}
	if policy.MinCompressRemoteSize != 0 {
		t.Errorf("expected default MinCompressRemoteSize == 0, got %d", policy.MinCompressRemoteSize)
	}
	if policy.Scope != CompressNonLocalRack {
		t.Errorf("expected default Scope == CompressNonLocalRack, got %d", policy.Scope)
	}
	if policy.MinSavingsPercent != 0 {
		t.Errorf("expected default MinSavingsPercent == 0, got %d", policy.MinSavingsPercent)
	}
}

// TestFramerFinishCompressionBatchFrame verifies threshold-based compression
// on batch frames which typically carry larger payloads.
func TestFramerFinishCompressionBatchFrame(t *testing.T) {
	t.Parallel()

	comp := SnappyCompressor{}
	typ := NativeType{proto: protoVersion4, typ: TypeInt}

	makeBatch := func(numStatements int) *writeBatchFrame {
		frame := &writeBatchFrame{
			typ:              LoggedBatch,
			statements:       make([]batchStatment, numStatements),
			consistency:      Quorum,
			defaultTimestamp: true,
		}
		for j := 0; j < numStatements; j++ {
			bs := &frame.statements[j]
			bs.preparedID = []byte(fmt.Sprintf("prepared_%d", j%5))
			bs.values = make([]queryValues, 2)
			for k := 0; k < 2; k++ {
				val, _ := Marshal(typ, j+k)
				bs.values[k] = queryValues{value: val}
			}
		}
		return frame
	}

	t.Run("large batch compressed with low threshold", func(t *testing.T) {
		frame := makeBatch(100)
		f := newFramer(comp, protoVersion4, compressionOpts{threshold: 64})
		if err := frame.buildFrame(f, 1); err != nil {
			t.Fatal(err)
		}
		if f.buf[1]&frm.FlagCompress == 0 {
			t.Error("expected large batch frame to be compressed with threshold 64")
		}
	})

	t.Run("large batch NOT compressed when threshold is -1", func(t *testing.T) {
		frame := makeBatch(100)
		f := newFramer(comp, protoVersion4, compressionOpts{threshold: neverCompressSize})
		if err := frame.buildFrame(f, 1); err != nil {
			t.Fatal(err)
		}
		if f.buf[1]&frm.FlagCompress != 0 {
			t.Error("expected batch frame to NOT be compressed with threshold -1")
		}
	})
}
