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

package runtimeerrors

import (
	"errors"
	"fmt"

	"time"
)

var (
	ErrQueryArgLength    = errors.New("gocql: query argument length mismatch")
	ErrTimeoutNoResponse = errors.New("gocql: no response received from cassandra within timeout period")
	// Deprecated: ErrTooManyTimeouts is no longer produced by the library.
	// It will be removed in a future major release.
	ErrTooManyTimeouts     = errors.New("gocql: too many query timeouts on the connection")
	ErrConnectionClosed    = errors.New("gocql: connection closed waiting for response")
	ErrNoStreams           = errors.New("gocql: no streams available on connection")
	ErrHostDown            = errors.New("gocql: host is nil or down")
	ErrNoPool              = errors.New("gocql: host does not have a pool")
	ErrNoConnectionsInPool = errors.New("gocql: host pool does not have connections")
)

type QueryError struct {
	state QueryErrorState
}

func (e *QueryError) IsIdempotent() bool {
	return QueryErrorData(e).InternalIsIdempotent
}

func (e *QueryError) PotentiallyExecuted() bool {
	return QueryErrorData(e).InternalPotentiallyExecuted
}

func (e *QueryError) Error() string {
	if QueryErrorData(e).InternalTimeout > 0 {
		return fmt.Sprintf("%s (timeout: %v, in-flight: %d) (potentially executed: %v)", QueryErrorData(e).InternalErr.Error(), QueryErrorData(e).InternalTimeout, QueryErrorData(e).InternalInFlight, QueryErrorData(e).InternalPotentiallyExecuted)
	}
	return fmt.Sprintf("%s (potentially executed: %v)", QueryErrorData(e).InternalErr.Error(), QueryErrorData(e).InternalPotentiallyExecuted)
}

func (e *QueryError) Unwrap() error {
	return QueryErrorData(e).InternalErr
}

type QueryErrorState struct {
	InternalErr                 error
	InternalTimeout             time.Duration
	InternalInFlight            int
	InternalPotentiallyExecuted bool
	InternalIsIdempotent        bool
}
