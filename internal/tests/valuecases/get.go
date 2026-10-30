/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package valuecases

type SimpleTypes []SimpleTypeCases

type SimpleTypeCases struct {
	CQLName string
	Cases   []SimpleTypeCase
	CQLType int
}

type SimpleTypeCase struct {
	Name      string
	Data      []byte
	LangCases []LangCase
}

type LangCase struct {
	Value     any
	LangType  string
	ErrInsert bool
	ErrSelect bool
}

var nilBytes = ([]byte)(nil)

func GetSimple() SimpleTypes {
	return simpleTypesCases
}

func nilRef[T any](in T) *T { return nil }

type Duration struct {
	Months, Days int32
	Nanoseconds  int64
}
