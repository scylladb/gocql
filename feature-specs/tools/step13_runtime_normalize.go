//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strings"
)

type decl struct{ File, Name, Receiver, Source, Kind, SHA string }

func norm(source string) string {
	f, e := parser.ParseFile(token.NewFileSet(), "x.go", "package p\n"+source, 0)
	if e != nil {
		panic(e)
	}
	if fn, ok := f.Decls[0].(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "InternalSession") {
		fn.Name.Name = strings.TrimPrefix(fn.Name.Name, "InternalSession")
		fn.Recv = &ast.FieldList{List: fn.Type.Params.List[:1]}
		fn.Type.Params.List = fn.Type.Params.List[1:]
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if id, ok := x.Fun.(*ast.Ident); ok && strings.HasPrefix(id.Name, "InternalSession") && len(x.Args) > 0 {
				x.Fun = &ast.SelectorExpr{X: x.Args[0], Sel: ast.NewIdent(strings.TrimPrefix(id.Name, "InternalSession"))}
				x.Args = x.Args[1:]
			}
		case *ast.SelectorExpr:
			if x.Sel.Name == "LZ4Compressor" {
				if id, ok := x.X.(*ast.Ident); ok && id.Name == "wirecompress" {
					id.Name = "lz4"
				}
			}
		}
		return true
	})
	var b bytes.Buffer
	format.Node(&b, token.NewFileSet(), f.Decls[0])
	return b.String()
}
func main() {
	b, _ := os.ReadFile("/tmp/gocql-step13-audit/accepted-declarations.json")
	var old []decl
	json.Unmarshal(b, &old)
	b, _ = os.ReadFile("/tmp/gocql-step13-audit/current-declarations.json")
	var cur []decl
	json.Unmarshal(b, &cur)
	by := map[string]decl{}
	for _, d := range cur {
		by[d.File+"::"+d.Receiver+"."+d.Name] = d
	}
	out := []map[string]any{}
	for _, d := range old {
		if strings.HasSuffix(d.File, "_test.go") || !strings.HasPrefix(d.File, "internal/") || d.Kind != "func" {
			continue
		}
		key := d.File + "::" + d.Receiver + "." + d.Name
		n, ok := by[key]
		if !ok && d.Receiver == "Session" {
			n, ok = by[d.File+"::.InternalSession"+d.Name]
		}
		if !ok || n.Source == d.Source {
			continue
		}
		out = append(out, map[string]any{"accepted_identity": key, "current_identity": n.File + "::" + n.Receiver + "." + n.Name, "accepted_sha256": d.SHA, "current_sha256": n.SHA, "same_normalized_source": norm(d.Source) == norm(n.Source)})
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
