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

package endpoint

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/scylladb/gocql/v2/internal/murmur"
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

// int64Hasher is an optional fast path avoiding Hash()'s Token-boxing
// allocation; Pick uses the raw int64 when a Partitioner implements it.
type InternalInt64Hasher interface {
	InternalHashInt64(partitionKey []byte) int64
}

// murmur3 partitioner
type InternalMurmur3Partitioner struct{}

func (p InternalMurmur3Partitioner) Name() string {
	return "Murmur3Partitioner"
}

func (p InternalMurmur3Partitioner) Hash(partitionKey []byte) Token {
	return InternalInt64Token(p.InternalHashInt64(partitionKey))
}

func (p InternalMurmur3Partitioner) InternalHashInt64(partitionKey []byte) int64 {
	return murmur.Murmur3H1(partitionKey)
}

var _ InternalInt64Hasher = InternalMurmur3Partitioner{}

// murmur3 little-endian, 128-bit hash, but returns only h1
func (p InternalMurmur3Partitioner) ParseString(str string) Token {
	return InternalParseInt64Token(str)
}

// int64 token
type InternalInt64Token int64

func InternalParseInt64Token(str string) InternalInt64Token {
	val, _ := strconv.ParseInt(str, 10, 64)
	return InternalInt64Token(val)
}

func (m InternalInt64Token) String() string {
	return strconv.FormatInt(int64(m), 10)
}

func (m InternalInt64Token) Less(token Token) bool {
	return m < token.(InternalInt64Token)
}

// order preserving partitioner and token
type InternalOrderedPartitioner struct{}
type InternalOrderedToken string

func (p InternalOrderedPartitioner) Name() string {
	return "OrderedPartitioner"
}

func (p InternalOrderedPartitioner) Hash(partitionKey []byte) Token {
	// the partition key is the token
	return InternalOrderedToken(partitionKey)
}

func (p InternalOrderedPartitioner) ParseString(str string) Token {
	return InternalOrderedToken(str)
}

func (o InternalOrderedToken) String() string {
	return string(o)
}

func (o InternalOrderedToken) Less(token Token) bool {
	return o < token.(InternalOrderedToken)
}

// random partitioner and token
type InternalRandomPartitioner struct{}
type InternalRandomToken big.Int

func (r InternalRandomPartitioner) Name() string {
	return "RandomPartitioner"
}

// 2 ** 128
var InternalMaxHashInt, _ = new(big.Int).SetString("340282366920938463463374607431768211456", 10)

func (p InternalRandomPartitioner) Hash(partitionKey []byte) Token {
	sum := md5.Sum(partitionKey)
	val := new(big.Int)
	val.SetBytes(sum[:])
	if sum[0] > 127 {
		val.Sub(val, InternalMaxHashInt)
		val.Abs(val)
	}

	return (*InternalRandomToken)(val)
}

func (p InternalRandomPartitioner) ParseString(str string) Token {
	val := new(big.Int)
	val.SetString(str, 10)
	return (*InternalRandomToken)(val)
}

func (r *InternalRandomToken) String() string {
	return (*big.Int)(r).String()
}

func (r *InternalRandomToken) Less(token Token) bool {
	return -1 == (*big.Int)(r).Cmp((*big.Int)(token.(*InternalRandomToken)))
}

type InternalHostToken struct {
	InternalToken Token
	InternalHost  *HostInfo
}

func (ht InternalHostToken) String() string {
	return fmt.Sprintf("{token=%v host=%v}", ht.InternalToken, ht.InternalHost.HostID())
}

// a data structure for organizing the relationship between tokens and hosts
type InternalTokenRing struct {
	InternalPartitioner Partitioner

	// tokens map token range to primary replica.
	// The elements in tokens are sorted by token ascending.
	// The range for a given item in tokens starts after preceding range and ends with the token specified in
	// token. The end token is part of the range.
	// The lowest (i.e. index 0) range wraps around the ring (its preceding range is the one with largest index).
	InternalTokens []InternalHostToken

	InternalHosts []*HostInfo
}

func InternalNewTokenRing(partitioner string, hosts []*HostInfo) (*InternalTokenRing, error) {
	tokenRing := &InternalTokenRing{
		InternalHosts: hosts,
	}

	if strings.HasSuffix(partitioner, "Murmur3Partitioner") {
		tokenRing.InternalPartitioner = InternalMurmur3Partitioner{}
	} else if strings.HasSuffix(partitioner, "OrderedPartitioner") {
		tokenRing.InternalPartitioner = InternalOrderedPartitioner{}
	} else if strings.HasSuffix(partitioner, "RandomPartitioner") {
		tokenRing.InternalPartitioner = InternalRandomPartitioner{}
	} else {
		return nil, fmt.Errorf("unsupported partitioner '%s'", partitioner)
	}

	for _, host := range hosts {
		for _, strToken := range host.Tokens() {
			token := tokenRing.InternalPartitioner.ParseString(strToken)
			tokenRing.InternalTokens = append(tokenRing.InternalTokens, InternalHostToken{InternalToken: token, InternalHost: host})
		}
	}

	sort.Sort(tokenRing)

	return tokenRing, nil
}

func (t *InternalTokenRing) Len() int {
	return len(t.InternalTokens)
}

func (t *InternalTokenRing) Less(i, j int) bool {
	return t.InternalTokens[i].InternalToken.Less(t.InternalTokens[j].InternalToken)
}

func (t *InternalTokenRing) Swap(i, j int) {
	t.InternalTokens[i], t.InternalTokens[j] = t.InternalTokens[j], t.InternalTokens[i]
}

func (t *InternalTokenRing) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString("TokenRing(")
	if t.InternalPartitioner != nil {
		buf.WriteString(t.InternalPartitioner.Name())
	}
	buf.WriteString("){")
	sep := ""
	for i, th := range t.InternalTokens {
		buf.WriteString(sep)
		sep = ","
		buf.WriteString("\n\t[")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("]")
		buf.WriteString(th.InternalToken.String())
		buf.WriteString(":")
		buf.WriteString(th.InternalHost.ConnectAddress().String())
	}
	buf.WriteString("\n}")
	return string(buf.Bytes())
}

// GetHostForPartitionKey finds host information for given partition key.
//
// It returns two tokens. First is token that exactly corresponds to the partition key (and could be used to
// determine shard, for example), second token is the endToken that corresponds to the host.
func (t *InternalTokenRing) GetHostForPartitionKey(partitionKey []byte) (host *HostInfo, token Token, endToken Token) {
	if t == nil {
		return nil, nil, nil
	}

	token = t.InternalPartitioner.Hash(partitionKey)
	host, endToken = t.GetHostForToken(token)
	return host, token, endToken
}

func (t *InternalTokenRing) GetHostForToken(token Token) (host *HostInfo, endToken Token) {
	if t == nil || len(t.InternalTokens) == 0 {
		return nil, nil
	}

	// find the primary replica
	p := sort.Search(len(t.InternalTokens), func(i int) bool {
		return !t.InternalTokens[i].InternalToken.Less(token)
	})

	if p == len(t.InternalTokens) {
		// wrap around to the first in the ring
		p = 0
	}

	v := t.InternalTokens[p]
	return v.InternalHost, v.InternalToken
}
