/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import implExecution "github.com/scylladb/gocql/v2/internal/execution"

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

func FuzzStep07StreamExecutionBoundary(f *testing.F) {
	f.Add(runtimeRowsBody(nil, "value"), uint16(16))
	f.Add(runtimeRowsBody([]byte{}, ""), uint16(0))
	f.Add(runtimeRowsBody(nil), uint16(10))
	f.Fuzz(func(t *testing.T, body []byte, capacity uint16) {
		if len(body) > 64<<10 {
			return
		}
		frame := []byte{0x84, 0, 0, 0, 8}
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
		frame = append(frame, body...)
		memory := &step04MemoryConn{source: bytes.NewReader(frame)}
		reservation := step04MemoryReservation(4, nil, memory)
		s := func() *Session {
			opaqueStorageValue := new(Session)
			return opaqueStorageValue
		}()

		reservation.InternalConn.InternalHost = func() *HostInfo {
			opaqueStorageValuex := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = UUID{15: 1}
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalState = NodeUp
			return opaqueStorageValuex
		}()
		reservation.InternalConn.InternalHooks = newConnectionHooks(s)
		bound := runtimeQuery(t)
		defer bound.Close()
		ctx, cancel, request, err := admitExecution(context.Background(), s, bound, Options{})
		if err != nil {
			t.Fatal(err)
		}
		request.InternalOptions.InternalProfile.Speculation = nil
		cursor := func() *RowStream[[]byte] {
			opaqueStorageValuexx := new(RowStream[[]byte])
			implExecution.RowStreamData(opaqueStorageValuexx).InternalCtx = ctx
			implExecution.RowStreamData(opaqueStorageValuexx).InternalCancel = cancel
			implExecution.RowStreamData(opaqueStorageValuexx).InternalSession = s
			implExecution.RowStreamData(opaqueStorageValuexx).InternalRequest = request
			implExecution.RowStreamData(opaqueStorageValuexx).InternalDecoder = StreamOneColumn(StreamBlob)
			implExecution.RowStreamData(opaqueStorageValuexx).InternalReservation = reservation
			implExecution.RowStreamData(opaqueStorageValuexx).InternalHistory = pagingHistory{InternalMaxCount: 4, InternalMaxBytes: 64}
			return opaqueStorageValuexx
		}()
		implExecution.RowStreamData(cursor).InternalActive.Lock()
		err = implExecution.InternalRowStreamLoadPage(cursor)
		implExecution.RowStreamData(cursor).InternalActive.Unlock()
		if err != nil {
			implExecution.InternalRowStreamFail(cursor, err)
			implExecution.RowStreamData(cursor).InternalActive.Lock()
			implExecution.InternalRowStreamCleanup(cursor, false)
			implExecution.RowStreamData(cursor).InternalActive.Unlock()
		} else {
			dst := make([]byte, 0, int(capacity))
			for i := 0; i < 4 && cursor.NextInto(&dst); i++ {
			}
			cursor.Close()
			before := append([]byte(nil), dst...)
			if cursor.NextInto(&dst) || !bytes.Equal(before, dst) {
				t.Fatal("closed cursor touched destination")
			}
		}
		bound.Close()
		if err := bound.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if cursor.Info().DeliveredRows > 4 {
			t.Fatal("unbounded delivery")
		}
	})
}
