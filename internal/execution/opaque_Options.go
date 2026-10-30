// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

type OptionsStorage struct {
	InternalConnection *Conn
}

func OptionsData(v *Options) *OptionsStorage { return &v.privateStorage }
