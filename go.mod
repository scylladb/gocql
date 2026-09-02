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
module github.com/gocql/gocql

require (
	github.com/google/go-cmp v0.7.0
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/klauspost/compress v1.20.0
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
	github.com/scylladb/gocql/lz4 v1.19.0
	golang.org/x/net v0.58.0
	golang.org/x/sync v0.22.0
	gopkg.in/inf.v0 v0.9.1
	sigs.k8s.io/yaml v1.6.0
)

// The integration suite constructs lz4.LZ4Compressor to run the protocol v5 lane
// (TEST_COMPRESSOR=lz4): lz4 is the only compressor v5 permits, so without this the
// compressed-segment path has no end-to-end coverage. Test-only -- no non-test file
// imports it.
//
// The require above names the published version rather than a placeholder because a
// `replace` applies only while this repository is the main module: consumers see the
// require and not the replace, so it has to resolve for them. It is inert here, where
// the replace redirects to the working tree so CI exercises the lz4 code under review
// rather than the last release. Bump it when the lz4 module is tagged; nothing local
// depends on its value.
replace github.com/scylladb/gocql/lz4 => ./lz4

require (
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/text v0.41.0 // indirect
)

require (
	github.com/bitly/go-hostpool v0.1.1 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/google/uuid v1.6.0
	github.com/kr/pretty v0.3.1 // indirect
	github.com/stretchr/testify v1.12.1
)

retract (
	v1.10.0 // tag from kiwicom/gocql added by mistake to scylladb/gocql
	v1.9.0 // tag from kiwicom/gocql added by mistake to scylladb/gocql
	v1.8.1 // tag from kiwicom/gocql added by mistake to scylladb/gocql
	v1.8.0 // tag from kiwicom/gocql added by mistake to scylladb/gocql
)

go 1.25.0
