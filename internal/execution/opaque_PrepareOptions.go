// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

type PrepareOptionsStorage struct {
	InternalConnection *Conn
}

func PrepareOptionsData(v *PrepareOptions) *PrepareOptionsStorage { return &v.privateStorage }
