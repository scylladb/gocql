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
	github.com/google/go-cmp v0.4.0
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/klauspost/compress v1.17.9
	golang.org/x/net v0.0.0-20220526153639-5463443f8c37
	gopkg.in/inf.v0 v0.9.1
	sigs.k8s.io/yaml v1.3.0
)

require (
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
)

retract (
	v1.10.0 // tag from kiwicom/gocql added by mistake to scylladb/gocql
	v1.9.0 // tag from kiwicom/gocql added by mistake to scylladb/gocql
	v1.8.1 // tag from kiwicom/gocql added by mistake to scylladb/gocql
	v1.8.0 // tag from kiwicom/gocql added by mistake to scylladb/gocql
)

go 1.13
