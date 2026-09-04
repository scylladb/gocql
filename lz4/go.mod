//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This module is named github.com/scylladb/gocql/lz4 to match its hosting repository
// (github.com/scylladb/gocql), and not by preference: upstream folded its lz4 package
// into its main module and ships no lz4/go.mod, so github.com/gocql/gocql/lz4 is no
// longer a releasable module and nothing newer than its pre-merge versions can appear
// there. Every protocol-v5-capable release comes from here.
// Consumers import it by that path directly, and must not
// map github.com/gocql/gocql/lz4 onto it with a replace the way they do for the parent
// module: the root go.mod requires this module under its own name, so a replace pointing
// at it as well makes Go refuse the build with "used for two different module paths".
// See the "Compression" section of the README.
module github.com/scylladb/gocql/lz4

go 1.25.0

require (
	github.com/pierrec/lz4/v4 v4.1.29
	github.com/stretchr/testify v1.12.1
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect
