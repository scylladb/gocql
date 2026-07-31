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

package gocql_test

import (
	"io"
	"testing"

	"github.com/gocql/gocql"
)

// *gocql.Conn has exposed both Read and Write since before the read-path
// refactor. This module has no /v2 import path, so dropping either one is a
// backward-incompatible removal for downstream code that uses a *Conn as an
// io.Reader/io.Writer. These guards make such a removal a build failure.
var (
	_ io.Reader = (*gocql.Conn)(nil)
	_ io.Writer = (*gocql.Conn)(nil)
)

// TestConnIsReadWriter documents the guards above as an explicit expectation, in
// the same spirit as TestCompressorBackwardCompatibility.
func TestConnIsReadWriter(t *testing.T) {
	t.Parallel()

	var c interface{} = (*gocql.Conn)(nil)
	if _, ok := c.(io.Reader); !ok {
		t.Error("*gocql.Conn must satisfy io.Reader")
	}
	if _, ok := c.(io.Writer); !ok {
		t.Error("*gocql.Conn must satisfy io.Writer")
	}
}
