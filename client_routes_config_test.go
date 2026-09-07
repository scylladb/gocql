//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gocql

import (
	"testing"

	"github.com/gocql/gocql/internal/tests"
)

func TestNewSessionCommonAppliesClientRoutesShardAwareness(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		clientRoutes          *ClientRoutesConfig
		disableShardAwarePort bool
		wantDisabled          bool
	}{
		{
			name:         "no client routes",
			wantDisabled: false,
		},
		{
			name: "client routes disable shard awareness by default",
			clientRoutes: &ClientRoutesConfig{
				Endpoints: ClientRoutesEndpointList{{ConnectionID: "connection-id"}},
			},
			wantDisabled: true,
		},
		{
			name: "client routes explicitly enable shard awareness",
			clientRoutes: &ClientRoutesConfig{
				Endpoints:            ClientRoutesEndpointList{{ConnectionID: "connection-id"}},
				EnableShardAwareness: true,
			},
			wantDisabled: false,
		},
		{
			name: "cluster-wide disable overrides client routes enable",
			clientRoutes: &ClientRoutesConfig{
				Endpoints:            ClientRoutesEndpointList{{ConnectionID: "connection-id"}},
				EnableShardAwareness: true,
			},
			disableShardAwarePort: true,
			wantDisabled:          true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewCluster("127.0.0.1")
			cfg.ClientRoutesConfig = tt.clientRoutes
			cfg.DisableShardAwarePort = tt.disableShardAwarePort
			session, err := newSessionCommon(*cfg)
			if err != nil {
				t.Fatalf("new session: %v", err)
			}
			t.Cleanup(session.Close)

			tests.AssertEqual(t, "disable shard-aware port", tt.wantDisabled, session.cfg.DisableShardAwarePort)
		})
	}
}

func TestWithShardAwareness(t *testing.T) {
	t.Parallel()

	cfg := NewCluster("127.0.0.1")
	cfg.WithOptions(
		WithClientRoutes(
			WithEndpoints(ClientRoutesEndpoint{ConnectionID: "connection-id"}),
			WithShardAwareness(true),
		),
	)

	if cfg.ClientRoutesConfig == nil {
		t.Fatal("expected client routes config")
	}
	tests.AssertEqual(t, "client routes shard awareness", true, cfg.ClientRoutesConfig.EnableShardAwareness)
}
