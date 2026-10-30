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

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// All typed internal queries share negotiated server timeout selection, including
// topology/bootstrap, scoped schema refresh, agreement and client routes.
func (c *Conn) executeV2System(ctx context.Context, request *attemptRequest) (*responsePage, error) {
	if c.cfg == nil || c.cfg.TypedConfig == nil {
		return c.executeTyped(ctx, request)
	}
	server := c.cfg.TypedConfig.Metadata.ServerTimeout
	if !server.Set && c.isScyllaConn() {
		server = Option[time.Duration]{Set: true, Value: 30 * time.Second}
	}
	if server.Set && server.Value > 0 {
		if !c.isScyllaConn() {
			return nil, &UnsupportedSettingError{Setting: "metadata server timeout", Reason: "server does not support USING TIMEOUT"}
		}
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(request.Statement)), "SELECT") {
			copyRequest := *request
			copyRequest.Statement = strings.TrimSuffix(strings.TrimSpace(request.Statement), ";") + " USING TIMEOUT " + strconv.FormatInt(server.Value.Milliseconds(), 10) + "ms"
			request = &copyRequest
		}
	}
	return c.executeTyped(ctx, request)
}
