//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"encoding/json"
	"go/ast"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"strings"
)

var auditRoot = func() string {
	if p := os.Getenv("GOCQL_STEP12_AUDIT_DIR"); p != "" {
		return p
	}
	return "/tmp/gocql-step12-audit"
}()

func main() {
	exports := map[string]string{}
	fd, _ := os.Open(auditRoot + "/packages-all.json")
	dec := json.NewDecoder(fd)
	for {
		var p struct{ ImportPath, Export string }
		if dec.Decode(&p) != nil {
			break
		}
		exports[p.ImportPath] = p.Export
	}
	imp := importer.ForCompiler(token.NewFileSet(), "gc", func(p string) (io.ReadCloser, error) { return os.Open(exports[p]) })
	root, e := imp.Import("github.com/scylladb/gocql/v2")
	if e != nil {
		panic(e)
	}
	seen := map[types.Type]bool{}
	var issues []map[string]string
	named := map[string]bool{}
	var visit func(types.Type)
	visit = func(t types.Type) {
		if t == nil || seen[t] {
			return
		}
		seen[t] = true
		switch x := t.(type) {
		case *types.Alias:
			visit(types.Unalias(x))
		case *types.Named:
			if x.Obj().Pkg() != nil && strings.HasPrefix(x.Obj().Pkg().Path(), "github.com/scylladb/gocql/v2/internal/") {
				key := x.Obj().Pkg().Path() + "." + x.Obj().Name()
				named[key] = true
				ms := types.NewMethodSet(types.NewPointer(x))
				for i := 0; i < ms.Len(); i++ {
					m := ms.At(i).Obj()
					if m.Exported() {
						if strings.HasPrefix(m.Name(), "Internal") {
							issues = append(issues, map[string]string{"kind": "method", "type": key, "name": m.Name()})
						}
						sig := m.Type().(*types.Signature)
						visit(sig.Params())
						visit(sig.Results())
					}
				}
				if st, ok := x.Underlying().(*types.Struct); ok {
					for i := 0; i < st.NumFields(); i++ {
						f := st.Field(i)
						if f.Exported() {
							if strings.HasPrefix(f.Name(), "Internal") {
								issues = append(issues, map[string]string{"kind": "field", "type": key, "name": f.Name()})
							}
							visit(f.Type())
						}
					}
				}
			}
			visit(x.Underlying())
		case *types.Pointer:
			visit(x.Elem())
		case *types.Slice:
			visit(x.Elem())
		case *types.Array:
			visit(x.Elem())
		case *types.Map:
			visit(x.Key())
			visit(x.Elem())
		case *types.Chan:
			visit(x.Elem())
		case *types.Signature:
			visit(x.Params())
			visit(x.Results())
		case *types.Tuple:
			for i := 0; i < x.Len(); i++ {
				visit(x.At(i).Type())
			}
		case *types.Interface:
			for i := 0; i < x.NumMethods(); i++ {
				m := x.Method(i)
				if m.Exported() {
					visit(m.Type())
				}
			}
		case *types.Struct:
			for i := 0; i < x.NumFields(); i++ {
				if x.Field(i).Exported() {
					visit(x.Field(i).Type())
				}
			}
		}
	}
	for _, name := range root.Scope().Names() {
		if ast.IsExported(name) {
			visit(root.Scope().Lookup(name).Type())
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"reachable_internal_types": named, "issues": issues})
	if len(issues) > 0 {
		os.Exit(1)
	}
}
