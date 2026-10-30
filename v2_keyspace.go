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

import "strings"

// v3/v4 have no per-query keyspace flag. Accept a different effective keyspace
// only when every statement table reference explicitly selects it. This checks
// qualification only; statement kind is always declared/server confirmed.
func qualifiedForKeyspace(cql, keyspace string) bool {
	tokens := cqlIdentifiers(cql)
	if len(tokens) == 0 {
		return false
	}
	found := false
	for i, token := range tokens {
		upper := strings.ToUpper(token)
		if upper != "FROM" && upper != "INTO" && upper != "UPDATE" && upper != "TABLE" && upper != "TRUNCATE" {
			continue
		}
		j := i + 1
		if upper == "TRUNCATE" && j < len(tokens) && strings.EqualFold(tokens[j], "TABLE") {
			j++
		}
		if j < len(tokens) && strings.EqualFold(tokens[j], "IF") {
			j++
			if j < len(tokens) && strings.EqualFold(tokens[j], "NOT") {
				j++
			}
			if j >= len(tokens) || !strings.EqualFold(tokens[j], "EXISTS") {
				return false
			}
			j++
		}
		if j+2 >= len(tokens) || tokens[j+1] != "." || strings.TrimPrefix(tokens[j], "\x00") != keyspace || tokens[j+2] == "." {
			return false
		}
		found = true
	}
	return found
}

// Quoted identifiers retain their case; unquoted identifiers are normalized.
// Values/comments never supply namespace evidence.
func cqlIdentifiers(source string) []string {
	var tokens []string
	for i := 0; i < len(source); {
		ch := source[i]
		if ch == '\'' {
			i++
			for i < len(source) {
				if source[i] == '\'' {
					i++
					if i < len(source) && source[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if i+1 < len(source) && source[i:i+2] == "--" {
			for i < len(source) && source[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(source) && source[i:i+2] == "/*" {
			end := strings.Index(source[i+2:], "*/")
			if end < 0 {
				return nil
			}
			i += end + 4
			continue
		}
		if ch == '"' {
			i++
			var value strings.Builder
			closed := false
			for i < len(source) {
				if source[i] == '"' {
					i++
					if i < len(source) && source[i] == '"' {
						value.WriteByte('"')
						i++
						continue
					}
					closed = true
					break
				}
				value.WriteByte(source[i])
				i++
			}
			if !closed {
				return nil
			}
			tokens = append(tokens, "\x00"+value.String())
			continue
		}
		if ch == '.' {
			tokens = append(tokens, ".")
			i++
			continue
		}
		if ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' {
			start := i
			i++
			for i < len(source) {
				c := source[i]
				if !(c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
					break
				}
				i++
			}
			tokens = append(tokens, strings.ToLower(source[start:i]))
			continue
		}
		i++
	}
	return tokens
}

func requestQualifiedForKeyspace(request *executionRequest, keyspace string) bool {
	if request.batch != nil {
		for _, item := range request.batch.items {
			if !qualifiedForKeyspace(item.statement, keyspace) {
				return false
			}
		}
		return true
	}
	return qualifiedForKeyspace(request.statement, keyspace)
}
