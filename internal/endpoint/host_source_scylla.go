// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

func (h *HostInfo) SetDatacenter(dc string) {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	h.privateStorage.InternalDataCenter = dc
}
