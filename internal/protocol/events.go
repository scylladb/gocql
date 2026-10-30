/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package protocol

import (
	"errors"
	"fmt"
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	. "github.com/scylladb/gocql/v2/internal/model"
	"net"
)

func (p *TypedParser) StreamEvent(h frm.FrameHeader) frm.Frame {
	p.ReserveAllocation(128)
	switch kind := p.Str(); kind {
	case "TOPOLOGY_CHANGE", "STATUS_CHANGE":
		change := p.Str()
		n := int(p.U8())
		if n != 4 && n != 16 {
			p.Check(errors.New("invalid event address width"))
			return nil
		}
		address := net.IP(p.Copy(n))
		port := p.I32()
		if port < 0 || port > 65535 {
			p.Check(errors.New("invalid event port"))
			return nil
		}
		if kind == "TOPOLOGY_CHANGE" {
			return &frm.TopologyChangeEventFrame{FrameHeader: h, Change: change, Host: address, Port: port}
		}
		return &frm.StatusChangeEventFrame{FrameHeader: h, Change: change, Host: address, Port: port}
	case "CLIENT_ROUTES_CHANGE":
		return &frm.ClientRoutesChanged{FrameHeader: h, ChangeType: p.Str(), ConnectionIDs: p.Strings(), HostIDs: p.Strings()}
	case "SCHEMA_CHANGE":
		change, target, keyspace := p.Str(), p.Str(), p.Str()
		switch target {
		case "KEYSPACE":
			return &frm.SchemaChangeKeyspace{FrameHeader: h, Change: change, Keyspace: keyspace}
		case "TABLE":
			return &frm.SchemaChangeTable{FrameHeader: h, Change: change, Keyspace: keyspace, Object: p.Str()}
		case "TYPE":
			return &frm.SchemaChangeType{FrameHeader: h, Change: change, Keyspace: keyspace, Object: p.Str()}
		case "FUNCTION":
			return &frm.SchemaChangeFunction{FrameHeader: h, Change: change, Keyspace: keyspace, Name: p.Str(), Args: p.Strings()}
		case "AGGREGATE":
			return &frm.SchemaChangeAggregate{FrameHeader: h, Change: change, Keyspace: keyspace, Name: p.Str(), Args: p.Strings()}
		default:
			p.Check(fmt.Errorf("gocql: unsupported event target %q", target))
		}
	default:
		p.Check(fmt.Errorf("gocql: unsupported event kind %q", kind))
	}
	return nil
}

func (p *TypedParser) StreamExtensions(h frm.FrameHeader) Result {
	info := Result{}
	if h.Flags&frm.FlagTracing != 0 {
		b := p.Copy(16)
		if p.Err == nil {
			p.ReserveAllocation(16)
			var id UUID
			copy(id[:], b)
			info.TraceIDs = []UUID{id}
		}
	}
	if h.Flags&frm.FlagWarning != 0 {
		info.Warnings = p.Strings()
	}
	if h.Flags&frm.FlagCustomPayload != 0 {
		count := int(p.U16())
		p.ReserveAllocation(int64(count) * 80)
		if p.Err == nil {
			info.CustomPayload = make([]PayloadEntry, count)
			for i := range info.CustomPayload {
				e := &info.CustomPayload[i]
				e.Key = p.Str()
				b := p.Bytes()
				if b != nil {
					e.Value = Nullable[Bytes]{Valid: true, Value: BorrowBytes(b)}
				}
			}
		}
	}
	return info
}
