// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

type countingBlobEncoder struct{ calls *int }

func (c countingBlobEncoder) CheckType(t DataType) error { return BytesEncoder.CheckType(t) }
func (c countingBlobEncoder) Encode(w *ValueWriter, b []byte) error {
	*c.calls++
	return BytesEncoder.Encode(w, b)
}
