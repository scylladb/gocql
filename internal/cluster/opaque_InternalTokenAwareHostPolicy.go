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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"sync"
	"sync/atomic"
)

type InternalTokenAwareHostPolicyStorage struct {
	InternalFallback HostSelectionPolicy
	// atomic store for *clusterMeta
	metadata                    atomic.Pointer[InternalClusterMeta]
	InternalLogger              StdLogger
	InternalGetKeyspaceMetadata func(keyspace string) (*KeyspaceMetadata, error)
	InternalGetKeyspaceName     func() string
	hosts                       InternalCowHostList
	partitioner                 string
	// mu protects writes to hosts, partitioner, metadata.
	// reads can be unlocked as long as they are not used for updating state later.
	mu                               sync.Mutex
	InternalShuffleReplicas          bool
	InternalNonLocalReplicasFallback bool
	avoidSlowReplicas                bool
}

func InternalTokenAwareHostPolicyData(v *InternalTokenAwareHostPolicy) *InternalTokenAwareHostPolicyStorage {
	return &v.privateStorage
}
