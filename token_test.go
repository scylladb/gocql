// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"fmt"
	"math/big"
	"net"
	"sort"
	"strconv"
	"testing"
)

// Tests of the murmur3Patitioner
func TestMurmur3Partitioner(t *testing.T) {
	t.Parallel()

	token := murmur3Partitioner{}.ParseString("-1053604476080545076")

	if "-1053604476080545076" != token.String() {
		t.Errorf("Expected '-1053604476080545076' but was '%s'", token)
	}

	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := marshalInt(1)
	token = murmur3Partitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

// Tests of the int64Token
func TestInt64Token(t *testing.T) {
	t.Parallel()

	if int64Token(42).Less(int64Token(42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !int64Token(-42).Less(int64Token(42)) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if int64Token(42).Less(int64Token(-42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

// Tests of the orderedPartitioner
func TestOrderedPartitioner(t *testing.T) {
	t.Parallel()

	// at least verify that the partitioner
	// doesn't return nil
	p := orderedPartitioner{}
	pk, _ := marshalInt(1)
	token := p.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}

	str := token.String()
	parsedToken := p.ParseString(str)

	if !bytes.Equal([]byte(token.(orderedToken)), []byte(parsedToken.(orderedToken))) {
		t.Errorf("Failed to convert to and from a string %s expected %x but was %x",
			str,
			[]byte(token.(orderedToken)),
			[]byte(parsedToken.(orderedToken)),
		)
	}
}

// Tests of the orderedToken
func TestOrderedToken(t *testing.T) {
	t.Parallel()

	if orderedToken([]byte{0, 0, 4, 2}).Less(orderedToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !orderedToken([]byte{0, 0, 3}).Less(orderedToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if orderedToken([]byte{0, 0, 4, 2}).Less(orderedToken([]byte{0, 0, 3})) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

// Tests of the randomPartitioner
func TestRandomPartitioner(t *testing.T) {
	t.Parallel()

	// at least verify that the partitioner
	// doesn't return nil
	p := randomPartitioner{}
	pk, _ := marshalInt(1)
	token := p.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}

	str := token.String()
	parsedToken := p.ParseString(str)

	if (*big.Int)(token.(*randomToken)).Cmp((*big.Int)(parsedToken.(*randomToken))) != 0 {
		t.Errorf("Failed to convert to and from a string %s expected %v but was %v",
			str,
			token,
			parsedToken,
		)
	}
}

func TestRandomPartitionerMatchesReference(t *testing.T) {
	t.Parallel()

	// example taken from datastax python driver
	//    >>> from cassandra.metadata import MD5Token
	//    >>> MD5Token.hash_fn("test")
	//    12707736894140473154801792860916528374L
	var p randomPartitioner
	expect := "12707736894140473154801792860916528374"
	actual := p.Hash([]byte("test")).String()
	if actual != expect {
		t.Errorf("expected random partitioner to generate tokens in the same way as the reference"+
			" python client. Expected %s, but got %s", expect, actual)
	}
}

// Tests of the randomToken
func TestRandomToken(t *testing.T) {
	t.Parallel()

	if ((*randomToken)(big.NewInt(42))).Less((*randomToken)(big.NewInt(42))) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !((*randomToken)(big.NewInt(41))).Less((*randomToken)(big.NewInt(42))) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if ((*randomToken)(big.NewInt(42))).Less((*randomToken)(big.NewInt(41))) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

type intToken int

func (i intToken) String() string        { return strconv.Itoa(int(i)) }
func (i intToken) Less(token Token) bool { return i < token.(intToken) }

// Test of the token ring implementation based on example at the start of this
// page of documentation:
// http://www.datastax.com/docs/0.8/cluster_architecture/partitioning
func TestTokenRing_Int(t *testing.T) {
	t.Parallel()

	host0 := &HostInfo{}
	host25 := &HostInfo{}
	host50 := &HostInfo{}
	host75 := &HostInfo{}
	ring := &tokenRing{
		partitioner: nil,
		// these tokens and hosts are out of order to test sorting
		tokens: []hostToken{
			{intToken(0), host0},
			{intToken(50), host50},
			{intToken(75), host75},
			{intToken(25), host25},
		},
	}

	sort.Sort(ring)

	if host, endToken := ring.GetHostForToken(intToken(0)); host != host0 || endToken != intToken(0) {
		t.Error("Expected host 0 for token 0")
	}
	if host, endToken := ring.GetHostForToken(intToken(1)); host != host25 || endToken != intToken(25) {
		t.Error("Expected host 25 for token 1")
	}
	if host, endToken := ring.GetHostForToken(intToken(24)); host != host25 || endToken != intToken(25) {
		t.Error("Expected host 25 for token 24")
	}
	if host, endToken := ring.GetHostForToken(intToken(25)); host != host25 || endToken != intToken(25) {
		t.Error("Expected host 25 for token 25")
	}
	if host, endToken := ring.GetHostForToken(intToken(26)); host != host50 || endToken != intToken(50) {
		t.Error("Expected host 50 for token 26")
	}
	if host, endToken := ring.GetHostForToken(intToken(49)); host != host50 || endToken != intToken(50) {
		t.Error("Expected host 50 for token 49")
	}
	if host, endToken := ring.GetHostForToken(intToken(50)); host != host50 || endToken != intToken(50) {
		t.Error("Expected host 50 for token 50")
	}
	if host, endToken := ring.GetHostForToken(intToken(51)); host != host75 || endToken != intToken(75) {
		t.Error("Expected host 75 for token 51")
	}
	if host, endToken := ring.GetHostForToken(intToken(74)); host != host75 || endToken != intToken(75) {
		t.Error("Expected host 75 for token 74")
	}
	if host, endToken := ring.GetHostForToken(intToken(75)); host != host75 || endToken != intToken(75) {
		t.Error("Expected host 75 for token 75")
	}
	if host, endToken := ring.GetHostForToken(intToken(76)); host != host0 || endToken != intToken(0) {
		t.Error("Expected host 0 for token 76")
	}
	if host, endToken := ring.GetHostForToken(intToken(99)); host != host0 || endToken != intToken(0) {
		t.Error("Expected host 0 for token 99")
	}
	if host, endToken := ring.GetHostForToken(intToken(100)); host != host0 || endToken != intToken(0) {
		t.Error("Expected host 0 for token 100")
	}
}

// Test for the behavior of a nil pointer to tokenRing
func TestTokenRing_Nil(t *testing.T) {
	t.Parallel()

	var ring *tokenRing = nil

	if host, endToken := ring.GetHostForToken(nil); host != nil || endToken != nil {
		t.Error("Expected nil for nil token ring")
	}
	if host, token, endToken := ring.GetHostForPartitionKey(nil); host != nil || token != nil || endToken != nil {
		t.Error("Expected nil for nil token ring")
	}
}

// Test of the recognition of the partitioner class
func TestTokenRing_UnknownPartition(t *testing.T) {
	t.Parallel()

	_, err := newTokenRing("UnknownPartitioner", nil)
	if err == nil {
		t.Error("Expected error for unknown partitioner value, but was nil")
	}
}

func hostsForTests(n int) []*HostInfo {
	hosts := make([]*HostInfo, n)
	for i := 0; i < n; i++ {
		host := &HostInfo{
			connectAddress: net.IPv4(1, 1, 1, byte(n)),
			tokens:         []string{fmt.Sprintf("%d", n)},
		}

		hosts[i] = host
	}
	return hosts
}

// Test of the tokenRing with the Murmur3Partitioner
func TestTokenRing_Murmur3(t *testing.T) {
	t.Parallel()

	// Note, strings are parsed directly to int64, they are not murmur3 hashed
	hosts := hostsForTests(4)
	ring, err := newTokenRing("Murmur3Partitioner", hosts)
	if err != nil {
		t.Fatalf("Failed to create token ring due to error: %v", err)
	}

	p := murmur3Partitioner{}

	for _, host := range hosts {
		actual, _ := ring.GetHostForToken(p.ParseString(host.tokens[0]))
		if !actual.ConnectAddress().Equal(host.ConnectAddress()) {
			t.Errorf("Expected address %v for token %q, but was %v", host.ConnectAddress(),
				host.tokens[0], actual.ConnectAddress())
		}
	}

	actual, _ := ring.GetHostForToken(p.ParseString("12"))
	if !actual.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
		t.Errorf("Expected address 1 for token \"12\", but was %s", actual.ConnectAddress())
	}

	actual, _ = ring.GetHostForToken(p.ParseString("24324545443332"))
	if !actual.ConnectAddress().Equal(hosts[0].ConnectAddress()) {
		t.Errorf("Expected address 0 for token \"24324545443332\", but was %s", actual.ConnectAddress())
	}
}

// Test of the tokenRing with the OrderedPartitioner
func TestTokenRing_Ordered(t *testing.T) {
	t.Parallel()

	// Tokens here more or less are similar layout to the int tokens above due
	// to each numeric character translating to a consistently offset byte.
	hosts := hostsForTests(4)
	ring, err := newTokenRing("OrderedPartitioner", hosts)
	if err != nil {
		t.Fatalf("Failed to create token ring due to error: %v", err)
	}

	p := orderedPartitioner{}

	var actual *HostInfo
	for _, host := range hosts {
		actual, _ := ring.GetHostForToken(p.ParseString(host.tokens[0]))
		if !actual.ConnectAddress().Equal(host.ConnectAddress()) {
			t.Errorf("Expected address %v for token %q, but was %v", host.ConnectAddress(),
				host.tokens[0], actual.ConnectAddress())
		}
	}

	actual, _ = ring.GetHostForToken(p.ParseString("12"))
	if !actual.peer.Equal(hosts[1].peer) {
		t.Errorf("Expected address 1 for token \"12\", but was %s", actual.ConnectAddress())
	}

	actual, _ = ring.GetHostForToken(p.ParseString("24324545443332"))
	if !actual.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
		t.Errorf("Expected address 1 for token \"24324545443332\", but was %s", actual.ConnectAddress())
	}
}

// Test of the tokenRing with the RandomPartitioner
func TestTokenRing_Random(t *testing.T) {
	t.Parallel()

	// String tokens are parsed into big.Int in base 10
	hosts := hostsForTests(4)
	ring, err := newTokenRing("RandomPartitioner", hosts)
	if err != nil {
		t.Fatalf("Failed to create token ring due to error: %v", err)
	}

	p := randomPartitioner{}

	var actual *HostInfo
	for _, host := range hosts {
		actual, _ := ring.GetHostForToken(p.ParseString(host.tokens[0]))
		if !actual.ConnectAddress().Equal(host.ConnectAddress()) {
			t.Errorf("Expected address %v for token %q, but was %v", host.ConnectAddress(),
				host.tokens[0], actual.ConnectAddress())
		}
	}

	actual, _ = ring.GetHostForToken(p.ParseString("12"))
	if !actual.peer.Equal(hosts[1].peer) {
		t.Errorf("Expected address 1 for token \"12\", but was %s", actual.ConnectAddress())
	}

	actual, _ = ring.GetHostForToken(p.ParseString("24324545443332"))
	if !actual.ConnectAddress().Equal(hosts[0].ConnectAddress()) {
		t.Errorf("Expected address 1 for token \"24324545443332\", but was %s", actual.ConnectAddress())
	}
}
