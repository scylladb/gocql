//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type decl struct {
	File, Name, Receiver, Kind, SHA, Tokens, Source string
	Line                                            int
}

func receiver(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.StarExpr:
		return receiver(x.X)
	case *ast.IndexExpr:
		return receiver(x.X)
	case *ast.IndexListExpr:
		return receiver(x.X)
	}
	return ""
}
func tokens(src []byte) string {
	fs := token.NewFileSet()
	file := fs.AddFile("", fs.Base(), len(src))
	var s scanner.Scanner
	s.Init(file, src, nil, 0)
	var b strings.Builder
	for {
		_, t, l := s.Scan()
		if t == token.EOF {
			break
		}
		if t == token.SEMICOLON {
			continue
		}
		fmt.Fprintf(&b, "%d:%q;", t, l)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(b.String())))
}
func main() {
	rev := ""
	if len(os.Args) > 1 {
		rev = os.Args[1]
	}
	var paths []string
	if rev != "" {
		raw, e := exec.Command("git", "ls-tree", "-r", "--name-only", rev).Output()
		if e != nil {
			panic(e)
		}
		paths = strings.Split(strings.TrimSpace(string(raw)), "\n")
	} else {
		filepath.Walk(".", func(p string, i os.FileInfo, e error) error {
			if e != nil {
				return e
			}
			if i.IsDir() && (p == ".git" || p == "lz4" || p == "benchmarks" || p == "feature-specs") {
				return filepath.SkipDir
			}
			if !i.IsDir() {
				paths = append(paths, p)
			}
			return nil
		})
	}
	var out []decl
	for _, path := range paths {
		if !strings.HasSuffix(path, ".go") || strings.HasPrefix(path, "lz4/") || strings.HasPrefix(path, "benchmarks/") || strings.HasPrefix(path, "feature-specs/") || strings.Contains(path, "/testdata/") {
			continue
		}
		var src []byte
		var e error
		if rev != "" {
			src, e = exec.Command("git", "show", rev+":"+path).Output()
		} else {
			src, e = os.ReadFile(path)
		}
		if e != nil {
			panic(e)
		}
		fs := token.NewFileSet()
		f, e := parser.ParseFile(fs, path, src, 0)
		if e != nil {
			panic(e)
		}
		for _, d := range f.Decls {
			var entries []decl
			switch x := d.(type) {
			case *ast.FuncDecl:
				r := ""
				if x.Recv != nil {
					r = receiver(x.Recv.List[0].Type)
				}
				entries = append(entries, decl{Name: x.Name.Name, Receiver: r, Kind: "func"})
			case *ast.GenDecl:
				if x.Tok == token.IMPORT {
					continue
				}
				for _, sp := range x.Specs {
					switch s := sp.(type) {
					case *ast.TypeSpec:
						entries = append(entries, decl{Name: s.Name.Name, Kind: "type"})
					case *ast.ValueSpec:
						for _, n := range s.Names {
							entries = append(entries, decl{Name: n.Name, Kind: x.Tok.String()})
						}
					}
				}
			}
			body := string(src[fs.Position(d.Pos()).Offset:fs.Position(d.End()).Offset])
			for _, x := range entries {
				x.File = path
				x.Line = fs.Position(d.Pos()).Line
				x.Source = body
				x.SHA = fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
				x.Tokens = tokens([]byte(body))
				out = append(out, x)
			}
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}
