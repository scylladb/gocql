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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExecutionCloneCarriesV5Options pins that the proto v5 per-statement options
// survive the clone every execution actually runs from.
//
// cloneQuery lists the fields it copies one by one, so a new Query field is
// dropped unless it is added there too — silently, because nothing fails to
// compile. That clone is not an edge case: an idempotent query with a
// speculative policy runs entirely from queryForSpeculativeExecution's clone,
// every page after the first comes from cloneQueryForNextPage, and every retry
// from queryForRetryExecution. A dropped keyspace override means those execute
// against the session keyspace instead of the requested one, which reads the
// wrong table rather than failing.
//
// TestCloneQueryAccountsForEveryField already forces a new Query field to be
// registered in one of cloneQuery's two lists, which is what catches the
// omission at compile-review time. This is the behavioural half: that the
// registered field actually arrives at the other end, through the entry points
// execution really uses.
//
// Batch takes the whole-struct copy route (executionBatch := *qry) so it cannot
// drop a field today; it is asserted anyway, so that stays true if Batch ever
// grows an explicit clone of its own.
func TestExecutionCloneCarriesV5Options(t *testing.T) {
	t.Parallel()

	const now = 1700000000

	t.Run("Query/cloneQuery", func(t *testing.T) {
		q := &Query{routingInfo: &queryRoutingInfo{}, metrics: newQueryMetrics()}
		q.SetKeyspace("ks_override").WithNowInSeconds(now)

		clone := cloneQuery(q, newQueryMetrics())

		require.Equal(t, "ks_override", clone.keyspace,
			"the keyspace override must survive the execution clone")
		require.NotNil(t, clone.nowInSecondsValue,
			"now_in_seconds must survive the execution clone")
		require.Equal(t, now, *clone.nowInSecondsValue)
		require.Equal(t, "ks_override", clone.Keyspace(),
			"the clone must report the override through Keyspace()")
	})

	t.Run("Query/cloneQueryForNextPage", func(t *testing.T) {
		q := &Query{routingInfo: &queryRoutingInfo{}, metrics: newQueryMetrics()}
		q.SetKeyspace("ks_override").WithNowInSeconds(now)

		next := cloneQueryForNextPage(q, newQueryMetrics(), []byte{0x01})

		require.Equal(t, "ks_override", next.keyspace,
			"auto-paging must not drop the keyspace override after the first page")
		require.NotNil(t, next.nowInSecondsValue,
			"auto-paging must not drop now_in_seconds after the first page")
		require.Equal(t, now, *next.nowInSecondsValue)
	})

	t.Run("Batch/queryForSpeculativeExecution", func(t *testing.T) {
		b := &Batch{routingInfo: &queryRoutingInfo{}, metrics: newQueryMetrics()}
		b.SetKeyspace("ks_override").WithNowInSeconds(now)

		clone, ok := queryForSpeculativeExecution(b, newQueryMetrics()).(*Batch)
		require.True(t, ok, "a *Batch must clone to a *Batch")

		require.Equal(t, "ks_override", clone.keyspace)
		require.NotNil(t, clone.nowInSeconds)
		require.Equal(t, now, *clone.nowInSeconds)
	})
}
