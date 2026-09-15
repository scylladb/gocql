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

//go:build linux || darwin

package gocql

import (
	"net"
	"syscall"
)

// probeMSS reads the negotiated TCP_MAXSEG for conn via getsockopt. Returns
// (0, false) if conn isn't a *net.TCPConn or the syscall fails.
func probeMSS(conn net.Conn) (int, bool) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return 0, false
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return 0, false
	}
	var mss int
	var sockErr error
	err = raw.Control(func(fd uintptr) {
		mss, sockErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG)
	})
	if err != nil || sockErr != nil || mss <= 0 {
		return 0, false
	}
	return mss, true
}
