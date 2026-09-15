//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package gocql

import (
	"net"
	"testing"
)

func TestMSSProbeAndTLSOverhead(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			select {}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	mss, ok := probeMSS(conn)
	if !ok {
		t.Skip("MSS probing unsupported on this platform/environment")
	}
	if mss <= 0 {
		t.Fatalf("probeMSS returned non-positive mss %d", mss)
	}

	plain := coalesceThresholdFor(conn, 0)
	tlsAdjusted := coalesceThresholdFor(conn, tlsRecordOverheadBytes)
	if tlsAdjusted >= plain {
		t.Fatalf("TLS-adjusted threshold %d should be smaller than plain threshold %d", tlsAdjusted, plain)
	}
}
