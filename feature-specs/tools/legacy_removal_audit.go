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

// Audit the step10 public API removal and dedicated streaming source boundary.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type source struct {
	Path    string   `json:"path"`
	SHA     string   `json:"sha256"`
	Exports []string `json:"exports,omitempty"`
}

func main() {
	forbidden := map[string]bool{}
	for _, name := range strings.Fields("Query Iter Batch QueryInfo Marshaler Unmarshaler UDTMarshaler UDTUnmarshaler RowData NamedValue UnsetValue Marshal Unmarshal Scan MapScan SliceMap ExecutableQuery RetryableQuery oldRetryPolicy ClusterConfig NewCluster NewSession NewSessionNonBlocking SingleHostQueryExecutor NewSingleHostQueryExecutor") {
		forbidden[name] = true
	}
	streamForbidden := map[string]bool{}
	for _, name := range strings.Fields("Query Iter Marshal Unmarshal Scan MapScan ValueReader RowReader RowPlan RowDecoder newValueReader newRowReader DecodeValue newPage responsePage Run RunPage RunUnpaged ReadAll") {
		streamForbidden[name] = true
	}
	files := []source{}
	findings := []string{}
	streamFiles := []string{}
	streamCalls := map[string]int{}
	filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") && path != "." || path == "feature-specs" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, body, 0)
		if err != nil {
			panic(err)
		}
		entry := source{Path: path, SHA: fmt.Sprintf("%x", sha256.Sum256(body))}
		root := filepath.Dir(path) == "."
		streaming := root && (strings.HasPrefix(path, "v2_stream_") || path == "v2_row_stream.go")
		if streaming {
			streamFiles = append(streamFiles, path)
		}
		for _, decl := range file.Decls {
			switch node := decl.(type) {
			case *ast.FuncDecl:
				name := node.Name.Name
				if root && forbidden[name] {
					findings = append(findings, fmt.Sprintf("%s: forbidden function %s", path, name))
				}
				if root && ast.IsExported(name) {
					entry.Exports = append(entry.Exports, name)
				}
			case *ast.GenDecl:
				for _, spec := range node.Specs {
					if typ, ok := spec.(*ast.TypeSpec); ok {
						if root && forbidden[typ.Name.Name] {
							findings = append(findings, fmt.Sprintf("%s: forbidden type %s", path, typ.Name.Name))
						}
						if root && ast.IsExported(typ.Name.Name) {
							entry.Exports = append(entry.Exports, typ.Name.Name)
						}
					}
				}
			}
		}
		if streaming {
			for _, i := range file.Imports {
				v, _ := strconv.Unquote(i.Path.Value)
				if v == "reflect" || v == "unsafe" {
					findings = append(findings, path+": forbidden streaming import "+v)
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if name, ok := node.(*ast.Ident); ok && streamForbidden[name.Name] {
					findings = append(findings, fmt.Sprintf("%s:%d: forbidden streaming identifier %s", path, fs.Position(name.Pos()).Line, name.Name))
				}
				if call, ok := node.(*ast.CallExpr); ok {
					switch fn := call.Fun.(type) {
					case *ast.Ident:
						streamCalls[fn.Name]++
					case *ast.SelectorExpr:
						streamCalls[fn.Sel.Name]++
					}
				}
				return true
			})
		}
		sort.Strings(entry.Exports)
		files = append(files, entry)
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	sort.Strings(findings)
	sort.Strings(streamFiles)
	out := map[string]any{"source_count": len(files), "sources": files, "forbidden_api": forbidden, "streaming_sources": streamFiles, "streaming_calls": streamCalls, "findings": findings, "scope": "AST declarations and dedicated streaming identifiers/imports; compiler and manual bounded-allocation/copy review supplement this static audit"}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		panic(err)
	}
	if len(findings) > 0 {
		os.Exit(1)
	}
}
