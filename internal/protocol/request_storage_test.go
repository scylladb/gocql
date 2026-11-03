// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
	"testing"
)

func frameStorageBytes(t *testing.T, chain wire.Chain) []byte {
	t.Helper()
	out := make([]byte, chain.Len())
	if _, err := chain.CopyTo(out); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestRequestFrameStorageWaitsForWriterRelease(t *testing.T) {
	var storage RequestFrameStorage
	settings := Settings{Protocol: 4, MaxFrameBytes: 1 << 20}
	if !storage.TryAcquire() {
		t.Fatal("first acquire")
	}
	request := &model.AttemptRequest{Statement: "SELECT x FROM ks.tbl", PageSize: 32, Framing: &storage}
	frame, err := settings.BuildTypedRequest(request, 7)
	if err != nil {
		t.Fatal(err)
	}
	expected := *request
	expected.Framing = nil
	plain, err := settings.BuildTypedRequest(&expected, 7)
	if err != nil {
		t.Fatal(err)
	}
	before := frameStorageBytes(t, frame)
	if !bytes.Equal(before, frameStorageBytes(t, plain)) {
		t.Fatal("framing differs")
	}
	if storage.TryAcquire() {
		t.Fatal("reused framing while a writer still owned it")
	}
	expected.Statement = "SELECT another FROM ks.tbl"
	if _, err := settings.BuildTypedRequest(&expected, 8); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, frameStorageBytes(t, frame)) {
		t.Fatal("fallback changed active framing")
	}
	storage.Release()
	if !storage.TryAcquire() {
		t.Fatal("slot was not reclaimed after writer completion")
	}
	request.Statement = "SELECT next FROM ks.tbl"
	if _, err := settings.BuildTypedRequest(request, 9); err != nil {
		t.Fatal(err)
	}
	storage.Release()
	storage.Trim(0)
	if storage.RetainedBytes() != 0 {
		t.Fatal("oversize idle backing", storage.RetainedBytes())
	}
}

func TestRequestFrameStorageClearsFailedBuildAndBoundsDescriptors(t *testing.T) {
	var storage RequestFrameStorage
	settings := Settings{Protocol: 4, MaxFrameBytes: 1 << 20}
	for round := 0; round < 2; round++ {
		if !storage.TryAcquire() {
			t.Fatal("acquire")
		}
		request := &model.AttemptRequest{Statement: "q", Framing: &storage, Payload: []model.PayloadEntry{{Key: string(make([]byte, 65536))}}}
		if _, err := settings.BuildTypedRequest(request, 1); err == nil {
			t.Fatal("accepted invalid payload key")
		}
		storage.Release()
	}
	if !storage.TryAcquire() {
		t.Fatal("acquire")
	}
	storage.envelope.b.ReserveParts(4096)
	storage.Release()
	if storage.RetainedBytes() <= 64<<10 {
		t.Fatal("test did not create oversize descriptor backing")
	}
	storage.Trim(64 << 10)
	if storage.RetainedBytes() > 64<<10 {
		t.Fatal("unbounded idle descriptors")
	}
}
