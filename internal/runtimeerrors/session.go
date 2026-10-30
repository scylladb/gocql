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
)

var (
	ErrNotFound             = errors.New("not found")
	ErrUnavailable          = errors.New("unavailable")
	ErrUnsupported          = errors.New("feature not supported")
	ErrTooManyStmts         = errors.New("too many statements")
	ErrUseStmt              = errors.New("use statements aren't supported. Please see https://github.com/apache/cassandra-gocql-driver for explanation.")
	ErrSessionClosed        = errors.New("session has been closed")
	ErrNoConnections        = errors.New("gocql: no hosts available in the pool")
	ErrNoKeyspace           = errors.New("no keyspace provided")
	ErrNoTable              = errors.New("no table name provided")
	ErrKeyspaceDoesNotExist = errors.New("keyspace does not exist")
	ErrNoMetadata           = errors.New("no metadata available")
	ErrTabletsNotUsed       = errors.New("tablets not used")
	ErrSessionNotReady      = errors.New("session is not ready yet")
)
