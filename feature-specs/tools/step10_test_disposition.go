//go:build ignore

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

// Inventory removed test declarations against accepted step09, separately from
// the immutable174 original integration identities.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const accepted = "f542da9507d9a6bd05a9e3278ff71bd3f7201a3a"

type test struct {
	Name       string `json:"name"`
	SHA        string `json:"sha256"`
	Assertions int    `json:"assertion_expressions"`
}

func functions(body []byte) map[string]test {
	fs := token.NewFileSet()
	f, e := parser.ParseFile(fs, "test", body, 0)
	if e != nil {
		panic(e)
	}
	out := map[string]test{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		if !strings.HasPrefix(name, "Test") && !strings.HasPrefix(name, "Example") && !strings.HasPrefix(name, "Benchmark") {
			continue
		}
		source := body[fs.Position(fn.Pos()).Offset:fs.Position(fn.End()).Offset]
		v := test{Name: name, SHA: fmt.Sprintf("%x", sha256.Sum256(source))}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok {
					if x, ok := s.X.(*ast.Ident); ok && (x.Name == "t" && (strings.HasPrefix(s.Sel.Name, "Fatal") || strings.HasPrefix(s.Sel.Name, "Error")) || x.Name == "require" || x.Name == "assert" || x.Name == "tests") {
						v.Assertions++
					}
				}
			}
			return true
		})
		out[name] = v
	}
	return out
}
func main() {
	paths, e := exec.Command("git", "ls-tree", "-r", "--name-only", accepted).Output()
	if e != nil {
		panic(e)
	}
	removed := []any{}
	files := []any{}
	for _, path := range strings.Split(strings.TrimSpace(string(paths)), "\n") {
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		old, e := exec.Command("git", "show", accepted+":"+path).Output()
		if e != nil {
			panic(e)
		}
		current, e := os.ReadFile(path)
		if os.IsNotExist(e) {
			files = append(files, map[string]any{"path": path, "accepted_sha256": fmt.Sprintf("%x", sha256.Sum256(old))})
		} else if e != nil {
			panic(e)
		}
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}
		before := functions(old)
		after := map[string]test{}
		if current != nil {
			after = functions(current)
		}
		names := make([]string, 0, len(before))
		for name := range before {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			test := before[name]
			if _, ok := after[name]; ok {
				continue
			}
			category := "obsolete_execution_api_and_translated_behavior"
			coverage := []string{"v2_execute_test.go", "v2_prepared_paging_test.go", "v2_execution_step07_test.go", "v2_profiles_test.go", "v2_result_test.go"}
			decision := "Mutable Query/Iter/Batch, callback binding and retry/observer interfaces are removed. Their execution, ownership, errors, paging and callback behavior is exercised through typed APIs in these suites; old object layout, reference counts and copy-on-write metrics are obsolete contracts."
			if strings.Contains(path, "serialization") || strings.Contains(path, "marshal") || strings.Contains(path, "vector_bench") || strings.Contains(path, "helpers") {
				category = "explicit_typed_codec_and_obsolete_reflection_conversions"
				coverage = []string{"v2_scalar_test.go", "v2_value_api_test.go", "v2_collections_test.go", "v2_tuple_udt_test.go", "v2_dynamic_test.go", "v2_codec_direct_test.go", "v2_stream_codec_test.go", "step10_migration_unit_test.go", "integration_serialization_scylla_test.go", "internal/tests/valuecases/simple.go"}
				decision = "Canonical scalar/container wire values, null/unset/empty states, malformed lengths/counts, numeric/date/duration/decimal/vector edges and ownership remain covered by typed suites and all96 retained binary fixtures. Automatic pointer/interface/struct-tag discovery, cross-Go-type coercions, NewWithError, arbitrary reflection containers and MarshalCQL callbacks are intentionally removed. This is a contract disposition, not a claim that every old coercion assertion has an equivalent supported typed conversion."
			}
			if strings.Contains(path, "routing_key") || strings.Contains(path, "policies") {
				category = "typed_routing_and_retry"
				coverage = []string{"v2_routing_step06_test.go", "v2_routing_token_test.go", "v2_policy_test.go", "v2_profiles_test.go", "v2_prepared_paging_test.go", "cassandra_test.go"}
				decision = "Canonical encoded token/routing, deterministic conditional order, locality, replica/tablet/leader targeting, schema/cache fences and safe retry decisions replace routing from mutable Query and reserialization. Old public policy Pick/RetryableQuery and v1 retry setters are removed; retained COW host-index/topology helpers keep their separate tests."
			}
			if strings.Contains(path, "example") {
				category = "compiled_typed_examples"
				coverage = []string{"example_test.go", "example_batch_test.go", "example_lwt_batch_test.go", "example_dynamic_columns_test.go", "example_nulls_test.go", "example_paging_test.go", "example_set_test.go", "example_marshaler_test.go", "example_udt_struct_test.go"}
				decision = "Typed Prepare/Bind/Run/NextInto, DynamicRows, explicit custom codecs, nullable values, paging and named UDT builders replace the old examples. Reflection map/struct/field-callback variants intentionally share one explicit UDT builder example."
			}
			if strings.Contains(path, "bench_") || strings.Contains(path, "bench/") {
				category = "typed_benchmark_callers"
				coverage = []string{"tests/bench/bench_marshal_test.go", "tests/bench/bench_vector_public_test.go", "tests/bench/bench_single_conn_test.go", "bench_recordings_test.go"}
				decision = "Equivalent scalar, blob/text sizes, vector dimensions, tuple/UDT/nested, select and insert callers use explicit codecs and typed binding. Historical control-capture integrity is separate from operational replay. Typed-control captures and equivalent operational replay benchmarks are an explicit pending step13 gate; old replay is not credited as passing."
			}
			if strings.Contains(path, "frame_test") {
				category = "removed_legacy_value_pools"
				coverage = []string{"v2_transport_test.go", "v2_transport_writer.go", "v2_execution_lifetime.go"}
				decision = "Legacy []queryValues bucket/pool assertions concern deleted execution storage. Canonical span ownership, writer cancellation, exact bytes and borrow draining have production typed tests. Oversized int NowInSeconds setters are obsolete: the typed public option is int32 and cannot represent an oversized Go int."
			}
			if strings.Contains(path, "driver_config") {
				category = "typed_configuration_report"
				coverage = []string{"driver_config_test.go", "driver_config_schema_test.go", "step10_migration_unit_test.go"}
				decision = "Canonical Profile retry/routing/defaults and profile replacement now drive reporting. Original schema boundary cases remain with typed retry-policy cases. Deprecated v1 retry/backoff/policy-Pick comparisons are obsolete; typed routing and report properties retain independent tests."
			}
			removed = append(removed, map[string]any{"file": filepath.ToSlash(path), "test": test, "category": category, "disposition": decision, "coverage_sources": coverage})
		}
	}
	out := map[string]any{"accepted_revision": accepted, "removed_source_files": files, "removed_test_declarations": removed, "removed_test_count": len(removed), "interpretation": "Coverage sources identify retained assertions for the supported behavior, not numerical equivalence of assertion counts or preservation of deliberately removed reflection/mutable API contracts. All174 original integration identities are audited separately."}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}
