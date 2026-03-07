// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build all || unit
// +build all unit

package gocql

import "testing"

// TestVectorFixedElemSize verifies that vectorFixedElemSize returns the correct
// wire-format byte sizes for all CQL types it is expected to handle, and returns 0
// for variable-length types.
func TestVectorFixedElemSize(t *testing.T) {
	tests := []struct {
		typ  Type
		want int
	}{
		// Fixed-length types — must match CQL wire-format sizes.
		{TypeBoolean, 1},
		{TypeInt, 4},
		{TypeFloat, 4},
		{TypeBigInt, 8},
		{TypeDouble, 8},
		{TypeTimestamp, 8},
		{TypeUUID, 16},
		{TypeTimeUUID, 16},
		// Variable-length types — must return 0.
		{TypeVarchar, 0},
		{TypeBlob, 0},
		{TypeText, 0},
		{TypeVarint, 0},
		{TypeDecimal, 0},
		{TypeAscii, 0},
		{TypeInet, 0},
		{TypeCounter, 0},
		{TypeDuration, 0},
		{TypeDate, 0},
		{TypeTime, 0},
		{TypeSmallInt, 0},
		{TypeTinyInt, 0},
		{TypeList, 0},
		{TypeSet, 0},
		{TypeMap, 0},
		{TypeUDT, 0},
		{TypeTuple, 0},
	}
	for _, tt := range tests {
		info := NewNativeType(protoVersion4, tt.typ)
		got := vectorFixedElemSize(info)
		if got != tt.want {
			t.Errorf("vectorFixedElemSize(%v) = %d, want %d", tt.typ, got, tt.want)
		}
	}
}
