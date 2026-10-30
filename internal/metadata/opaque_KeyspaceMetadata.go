// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

func KeyspaceData(v *KeyspaceMetadata) *KeyspaceState { return &v.state }
