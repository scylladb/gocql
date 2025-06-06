// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"crypto/md5"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/gocql/gocql/internal/murmur"
)

// a token partitioner
type Partitioner interface {
	Name() string
	Hash([]byte) Token
	ParseString(string) Token
}

// a Token
type Token interface {
	fmt.Stringer
	Less(Token) bool
}

// murmur3 partitioner
type murmur3Partitioner struct{}

func (p murmur3Partitioner) Name() string {
	return "Murmur3Partitioner"
}

func (p murmur3Partitioner) Hash(partitionKey []byte) Token {
	h1 := murmur.Murmur3H1(partitionKey)
	return int64Token(h1)
}

// murmur3 little-endian, 128-bit hash, but returns only h1
func (p murmur3Partitioner) ParseString(str string) Token {
	return parseInt64Token(str)
}

// int64 token
type int64Token int64

func parseInt64Token(str string) int64Token {
	val, _ := strconv.ParseInt(str, 10, 64)
	return int64Token(val)
}

func (m int64Token) String() string {
	return strconv.FormatInt(int64(m), 10)
}

func (m int64Token) Less(token Token) bool {
	return m < token.(int64Token)
}

// order preserving partitioner and token
type orderedPartitioner struct{}
type orderedToken string

func (p orderedPartitioner) Name() string {
	return "OrderedPartitioner"
}

func (p orderedPartitioner) Hash(partitionKey []byte) Token {
	// the partition key is the token
	return orderedToken(partitionKey)
}

func (p orderedPartitioner) ParseString(str string) Token {
	return orderedToken(str)
}

func (o orderedToken) String() string {
	return string(o)
}

func (o orderedToken) Less(token Token) bool {
	return o < token.(orderedToken)
}

// random partitioner and token
type randomPartitioner struct{}
type randomToken big.Int

func (r randomPartitioner) Name() string {
	return "RandomPartitioner"
}

// 2 ** 128
var maxHashInt, _ = new(big.Int).SetString("340282366920938463463374607431768211456", 10)

func (p randomPartitioner) Hash(partitionKey []byte) Token {
	sum := md5.Sum(partitionKey)
	val := new(big.Int)
	val.SetBytes(sum[:])
	if sum[0] > 127 {
		val.Sub(val, maxHashInt)
		val.Abs(val)
	}

	return (*randomToken)(val)
}

func (p randomPartitioner) ParseString(str string) Token {
	val := new(big.Int)
	val.SetString(str, 10)
	return (*randomToken)(val)
}

func (r *randomToken) String() string {
	return (*big.Int)(r).String()
}

func (r *randomToken) Less(token Token) bool {
	return -1 == (*big.Int)(r).Cmp((*big.Int)(token.(*randomToken)))
}

type hostToken struct {
	token Token
	host  *HostInfo
}

func (ht hostToken) String() string {
	return fmt.Sprintf("{token=%v host=%v}", ht.token, ht.host.HostID())
}

// a data structure for organizing the relationship between tokens and hosts
type tokenRing struct {
	partitioner Partitioner

	// tokens map token range to primary replica.
	// The elements in tokens are sorted by token ascending.
	// The range for a given item in tokens starts after preceding range and ends with the token specified in
	// token. The end token is part of the range.
	// The lowest (i.e. index 0) range wraps around the ring (its preceding range is the one with largest index).
	tokens []hostToken

	hosts []*HostInfo
}

func newTokenRing(partitioner string, hosts []*HostInfo) (*tokenRing, error) {
	tokenRing := &tokenRing{
		hosts: hosts,
	}

	if strings.HasSuffix(partitioner, "Murmur3Partitioner") {
		tokenRing.partitioner = murmur3Partitioner{}
	} else if strings.HasSuffix(partitioner, "OrderedPartitioner") {
		tokenRing.partitioner = orderedPartitioner{}
	} else if strings.HasSuffix(partitioner, "RandomPartitioner") {
		tokenRing.partitioner = randomPartitioner{}
	} else {
		return nil, fmt.Errorf("unsupported partitioner '%s'", partitioner)
	}

	for _, host := range hosts {
		for _, strToken := range host.Tokens() {
			token := tokenRing.partitioner.ParseString(strToken)
			tokenRing.tokens = append(tokenRing.tokens, hostToken{token, host})
		}
	}

	sort.Sort(tokenRing)

	return tokenRing, nil
}

func (t *tokenRing) Len() int {
	return len(t.tokens)
}

func (t *tokenRing) Less(i, j int) bool {
	return t.tokens[i].token.Less(t.tokens[j].token)
}

func (t *tokenRing) Swap(i, j int) {
	t.tokens[i], t.tokens[j] = t.tokens[j], t.tokens[i]
}

func (t *tokenRing) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString("TokenRing(")
	if t.partitioner != nil {
		buf.WriteString(t.partitioner.Name())
	}
	buf.WriteString("){")
	sep := ""
	for i, th := range t.tokens {
		buf.WriteString(sep)
		sep = ","
		buf.WriteString("\n\t[")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("]")
		buf.WriteString(th.token.String())
		buf.WriteString(":")
		buf.WriteString(th.host.ConnectAddress().String())
	}
	buf.WriteString("\n}")
	return string(buf.Bytes())
}

// GetHostForPartitionKey finds host information for given partition key.
//
// It returns two tokens. First is token that exactly corresponds to the partition key (and could be used to
// determine shard, for example), second token is the endToken that corresponds to the host.
func (t *tokenRing) GetHostForPartitionKey(partitionKey []byte) (host *HostInfo, token Token, endToken Token) {
	if t == nil {
		return nil, nil, nil
	}

	token = t.partitioner.Hash(partitionKey)
	host, endToken = t.GetHostForToken(token)
	return host, token, endToken
}

func (t *tokenRing) GetHostForToken(token Token) (host *HostInfo, endToken Token) {
	if t == nil || len(t.tokens) == 0 {
		return nil, nil
	}

	// find the primary replica
	p := sort.Search(len(t.tokens), func(i int) bool {
		return !t.tokens[i].token.Less(token)
	})

	if p == len(t.tokens) {
		// wrap around to the first in the ring
		p = 0
	}

	v := t.tokens[p]
	return v.host, v.token
}
