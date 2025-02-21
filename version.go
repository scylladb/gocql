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

package gocql

import "runtime/debug"

const (
	mainPackage = "github.com/gocql/gocql"
)

var defaultDriverVersion string

func init() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		for _, d := range buildInfo.Deps {
			if d.Path == mainPackage {
				defaultDriverVersion = d.Version
				if d.Replace != nil {
					defaultDriverVersion = d.Replace.Version
				}
				break
			}
		}
	}
}
