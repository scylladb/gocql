// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import "testing"

func TestV2RuntimePageStates(t *testing.T) {
	start := StartPage()
	empty := RestorePageState(nil)
	complete := CompletePageState()
	if start.More() || start.Complete() || !empty.More() || empty.Complete() || !complete.Complete() {
		t.Fatal("page state distinctions lost")
	}
	if empty.Bytes() == nil {
		t.Fatal("empty continuation became absent")
	}
	token := []byte{1}
	state := RestorePageState(token)
	token[0] = 2
	copy := state.Bytes()
	copy[0] = 3
	if state.Bytes()[0] != 1 {
		t.Fatal("state token aliased")
	}
}
