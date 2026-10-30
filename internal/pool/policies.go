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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package pool

import (
	"math"
	"math/rand"

	"time"
)

func InternalGetExponentialTime(min time.Duration, max time.Duration, attempts int) time.Duration {
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))
	// add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return time.Duration(max)
	}
	return time.Duration(napDuration)
}

// ReconnectionPolicy interface is used by gocql to determine if reconnection
// can be attempted after connection error. The interface allows gocql users
// to implement their own logic to determine how to attempt reconnection.
type ReconnectionPolicy interface {
	GetInterval(currentRetry int) time.Duration
	GetMaxRetries() int
}

// NoReconnectionPolicy is a policy to have no retry.
//
// Examples of usage:
//
//	cluster.InitialReconnectionPolicy = &NoReconnectionPolicy{}
type NoReconnectionPolicy struct {
}

func (c *NoReconnectionPolicy) GetInterval(currentRetry int) time.Duration {
	return time.Duration(0)
}

func (c *NoReconnectionPolicy) GetMaxRetries() int {
	return 1
}

// ConstantReconnectionPolicy has simple logic for returning a fixed reconnection interval.
//
// Examples of usage:
//
//	cluster.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{MaxRetries: 10, Interval: 8 * time.Second}
type ConstantReconnectionPolicy struct {
	MaxRetries int
	Interval   time.Duration
}

func (c *ConstantReconnectionPolicy) GetInterval(currentRetry int) time.Duration {
	return c.Interval
}

func (c *ConstantReconnectionPolicy) GetMaxRetries() int {
	return c.MaxRetries
}

// ExponentialReconnectionPolicy returns a growing reconnection interval.
type ExponentialReconnectionPolicy struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

func (e *ExponentialReconnectionPolicy) GetInterval(currentRetry int) time.Duration {
	max := e.MaxInterval
	if max < e.InitialInterval {
		max = math.MaxInt16 * time.Second
	}
	return InternalGetExponentialTime(e.InitialInterval, max, currentRetry)
}

func (e *ExponentialReconnectionPolicy) GetMaxRetries() int {
	return e.MaxRetries
}
