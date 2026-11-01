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
	"path/filepath"
)

func main() {
	root := os.Args[1]
	out := map[string]string{}
	for _, name := range os.Args[2:] {
		f, e := parser.ParseFile(token.NewFileSet(), filepath.Join(root, name), nil, 0)
		if e != nil {
			panic(e)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			var body bytes.Buffer
			if e = format.Node(&body, token.NewFileSet(), fn); e != nil {
				panic(e)
			}
			receiver := ""
			if fn.Recv != nil {
				var r bytes.Buffer
				format.Node(&r, token.NewFileSet(), fn.Recv.List[0].Type)
				receiver = r.String() + "."
			}
			out[name+"::"+receiver+fn.Name.Name] = body.String()
		}
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
