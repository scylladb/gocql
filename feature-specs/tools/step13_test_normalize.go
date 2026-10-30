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

type decl struct{ File, Name, Receiver, Source string }

func norm(source string) string {
	f, e := parser.ParseFile(token.NewFileSet(), "x.go", "package p\n"+source, 0)
	if e != nil {
		panic(e)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if ok && strings.HasPrefix(id.Name, "sessionTest") && len(call.Args) > 0 {
			call.Fun = &ast.SelectorExpr{X: call.Args[0], Sel: ast.NewIdent(strings.TrimPrefix(id.Name, "sessionTest"))}
			call.Args = call.Args[1:]
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
	out := map[string]bool{}
	for _, d := range old {
		if !strings.HasSuffix(d.File, "_test.go") {
			continue
		}
		key := d.File + "::" + d.Receiver + "." + d.Name
		n, ok := by[key]
		if !ok || n.Source == d.Source {
			continue
		}
		if strings.Contains(n.Source, "sessionTest") {
			out[key] = norm(d.Source) == norm(n.Source)
		}
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
