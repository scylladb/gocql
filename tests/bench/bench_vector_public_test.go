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

package bench_test

import (
	"fmt"
	"github.com/scylladb/gocql/v2"
	"testing"
)

func BenchmarkVectorPublic(b *testing.B) {
	for _, dim := range []int{128, 384, 768, 1536} {
		b.Run(fmt.Sprint(dim), func(b *testing.B) {
			typ, err := gocql.VectorDataType(scalar(gocql.TypeFloat), uint16(dim))
			if err != nil {
				b.Fatal(err)
			}
			vector := make([]float32, dim)
			for i := range vector {
				vector[i] = float32(i) * 0.1
			}
			benchmarkValue(b, typ, gocql.VectorEncoder(gocql.Float32), gocql.VectorDecoder(gocql.Float32), vector)
		})
	}
}
