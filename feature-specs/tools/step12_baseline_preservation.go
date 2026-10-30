//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"reflect"
	"strings"
	"unicode"
)

type body struct {
	Source, SHA string
	SHA256      string `json:"sha256"`
}
type test struct {
	Identity             string `json:"identity"`
	Original, Translated body
}
type inventory struct{ Tests []test }

var auditRoot = func() string {
	if p := os.Getenv("GOCQL_STEP12_AUDIT_DIR"); p != "" {
		return p
	}
	return "/tmp/gocql-step12-audit"
}()

func low(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}
func load(path string, out any) {
	b, e := os.ReadFile(path)
	if e != nil {
		panic(e)
	}
	if e = json.Unmarshal(b, out); e != nil {
		panic(e)
	}
}
func leaf(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.IndexExpr:
		return leaf(x.X)
	case *ast.IndexListExpr:
		return leaf(x.X)
	case *ast.StarExpr:
		return leaf(x.X)
	}
	return ""
}

var names = map[string]bool{}
var layouts = map[string][]string{}
var methods = map[string]string{}
var access = map[string]bool{}
var aliases = map[string]string{"driverConn": "Conn", "driverConnConfig": "ConnConfig", "driverConnInterface": "ConnInterface", "driverConnErrorHandler": "ConnErrorHandler", "driverConnPicker": "ConnPicker"}

func cname(n string) string {
	if a := aliases[n]; a != "" {
		return a
	}
	if n == "InternalHooks" {
		return "session"
	}
	if strings.HasPrefix(n, "Internal") {
		s := strings.TrimPrefix(n, "Internal")
		if names[low(s)] {
			return low(s)
		}
		if names[s] {
			return s
		}
	}
	return n
}
func parse(src string) *ast.FuncDecl {
	f, e := parser.ParseFile(token.NewFileSet(), "x.go", "package p\n"+src, 0)
	if e != nil {
		panic(e)
	}
	return f.Decls[0].(*ast.FuncDecl)
}
func norm(src string) string {
	fn := parse(src)
	var walk func(reflect.Value) reflect.Value
	walk = func(v reflect.Value) reflect.Value {
		if !v.IsValid() {
			return v
		}
		if v.Kind() == reflect.Interface {
			if v.IsNil() {
				return v
			}
			x := walk(v.Elem())
			o := reflect.New(v.Type()).Elem()
			o.Set(x)
			return o
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return v
			}
			n, ok := v.Interface().(ast.Node)
			if !ok {
				return v
			}
			if parent, ok := n.(*ast.CompositeLit); ok {
				var elem ast.Expr
				switch typ := parent.Type.(type) {
				case *ast.ArrayType:
					elem = typ.Elt
				case *ast.MapType:
					elem = typ.Value
				}
				if elem != nil {
					for i, e := range parent.Elts {
						value := e
						var kv *ast.KeyValueExpr
						if x, ok := e.(*ast.KeyValueExpr); ok {
							kv = x
							value = x.Value
						}
						if child, ok := value.(*ast.CompositeLit); ok && child.Type == nil {
							if ptr, ok := elem.(*ast.StarExpr); ok {
								child.Type = ptr.X
								value = &ast.UnaryExpr{Op: token.AND, X: child}
							} else {
								child.Type = elem
							}
							if kv != nil {
								kv.Value = value
							} else {
								parent.Elts[i] = value
							}
						}
					}
				}
			}
			el := v.Elem()
			if el.Kind() == reflect.Struct {
				for i := 0; i < el.NumField(); i++ {
					e := el.Field(i)
					if e.CanSet() && (e.Kind() == reflect.Interface || e.Kind() == reflect.Pointer || e.Kind() == reflect.Slice) {
						nv := walk(e)
						if nv.Type().AssignableTo(e.Type()) {
							e.Set(nv)
						}
					}
				}
			}
			switch x := n.(type) {
			case *ast.Ident:
				x.Name = cname(x.Name)
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					x.Value = strings.ReplaceAll(x.Value, "driverConn", "Conn")
				}
			case *ast.SelectorExpr:
				if call, ok := x.X.(*ast.CallExpr); ok && access[leaf(call.Fun)] && len(call.Args) == 1 {
					arg := call.Args[0]
					if p, ok := arg.(*ast.UnaryExpr); ok && p.Op == token.AND {
						arg = p.X
					}
					x.X = arg
				}
				if sel, ok := x.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "privateStorage" {
					x.X = sel.X
				}
			case *ast.CallExpr:
				name := leaf(x.Fun)
				if (name == "runtimeFor" || name == "InternalRuntimeFor") && len(x.Args) == 1 {
					return reflect.ValueOf(&ast.CallExpr{Fun: &ast.SelectorExpr{X: x.Args[0], Sel: ast.NewIdent("typedRuntime")}})
				}
				if method := methods[name]; method != "" && len(x.Args) > 0 {
					receiver := x.Args[0]
					if p, ok := receiver.(*ast.UnaryExpr); ok && p.Op == token.AND {
						receiver = p.X
					}
					x.Fun = &ast.SelectorExpr{X: receiver, Sel: ast.NewIdent(cname(method))}
					x.Args = x.Args[1:]
				} else if (name == "newSchemaSource" || name == "newConnectionHooks" || name == "InternalNewSchemaSource" || name == "InternalNewConnectionHooks") && len(x.Args) == 1 {
					return reflect.ValueOf(x.Args[0])
				} else if (name == "CloseAllConnections" || name == "MissingConnections") && len(x.Args) == 1 {
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
						if id, ok := sel.X.(*ast.Ident); ok && strings.Contains(id.Name, "testkit") {
							x.Fun = &ast.SelectorExpr{X: x.Args[0], Sel: ast.NewIdent(name)}
							x.Args = nil
						}
					}
				}
				if lit, ok := x.Fun.(*ast.FuncLit); ok && len(lit.Body.List) >= 2 {
					if first, ok := lit.Body.List[0].(*ast.AssignStmt); ok && len(first.Lhs) == 1 && len(first.Rhs) == 1 {
						if id, ok := first.Lhs[0].(*ast.Ident); ok && strings.HasPrefix(id.Name, "opaqueStorageValue") {
							if alloc, ok := first.Rhs[0].(*ast.CallExpr); ok && leaf(alloc.Fun) == "new" && len(alloc.Args) == 1 {
								ctor := &ast.CompositeLit{Type: alloc.Args[0]}
								valid := true
								for _, stmt := range lit.Body.List[1 : len(lit.Body.List)-1] {
									assign, ok := stmt.(*ast.AssignStmt)
									if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
										valid = false
										break
									}
									sel, ok := assign.Lhs[0].(*ast.SelectorExpr)
									if !ok {
										valid = false
										break
									}
									base, ok := sel.X.(*ast.Ident)
									if !ok || base.Name != id.Name {
										valid = false
										break
									}
									ctor.Elts = append(ctor.Elts, &ast.KeyValueExpr{Key: ast.NewIdent(sel.Sel.Name), Value: assign.Rhs[0]})
								}
								if valid {
									return reflect.ValueOf(&ast.UnaryExpr{Op: token.AND, X: ctor})
								}
							}
						}
					}
				}
				ctors := map[string]string{"NewQueryError": "QueryError", "NewPartitionKey": "PartitionKey", "NewMetadataSnapshot": "MetadataSnapshot"}
				if owner := ctors[name]; owner != "" && len(x.Args) == 1 {
					if state, ok := x.Args[0].(*ast.CompositeLit); ok {
						return reflect.ValueOf(&ast.UnaryExpr{Op: token.AND, X: &ast.CompositeLit{Type: ast.NewIdent(owner), Elts: state.Elts}})
					}
				}
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && (strings.HasPrefix(id.Name, "impl") || id.Name == "metadata" || id.Name == "routing" || id.Name == "execution" || id.Name == "cluster" || id.Name == "binding" || id.Name == "runtimeerrors") {
						if names[sel.Sel.Name] {
							x.Fun = ast.NewIdent(sel.Sel.Name)
						}
					}
				}
			case *ast.StarExpr:
				if u, ok := x.X.(*ast.UnaryExpr); ok && u.Op == token.AND {
					return reflect.ValueOf(u.X)
				}
			case *ast.CompositeLit:
				fieldNames := layouts[leaf(x.Type)]
				if len(fieldNames) == len(x.Elts) && len(x.Elts) > 0 {
					if _, keyed := x.Elts[0].(*ast.KeyValueExpr); !keyed {
						for i, e := range x.Elts {
							x.Elts[i] = &ast.KeyValueExpr{Key: ast.NewIdent(fieldNames[i]), Value: e}
						}
					}
				}
			}
			return v
		}
		if v.Kind() == reflect.Slice {
			for i := 0; i < v.Len(); i++ {
				e := v.Index(i)
				nv := walk(e)
				if e.CanSet() && nv.Type().AssignableTo(e.Type()) {
					e.Set(nv)
				}
			}
		}
		return v
	}
	walk(reflect.ValueOf(fn))
	var out bytes.Buffer
	format.Node(&out, token.NewFileSet(), fn)
	return out.String()
}
func tok(src string) string {
	fs := token.NewFileSet()
	f := fs.AddFile("", fs.Base(), len(src))
	var s scanner.Scanner
	s.Init(f, []byte(src), nil, 0)
	var parts []string
	comma := fmt.Sprintf("%d:%q;", token.COMMA, "")
	for {
		_, t, l := s.Scan()
		if t == token.EOF {
			break
		}
		if t == token.SEMICOLON {
			continue
		}
		if (t == token.RBRACE || t == token.RPAREN || t == token.RBRACK) && len(parts) > 0 && parts[len(parts)-1] == comma {
			parts = parts[:len(parts)-1]
		}
		parts = append(parts, fmt.Sprintf("%d:%q;", t, l))
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(parts, ""))))
}
func main() {
	var decls []struct{ File, Name, Source, Kind string }
	load(auditRoot+"/original-declarations.json", &decls)
	for _, d := range decls {
		f, e := parser.ParseFile(token.NewFileSet(), "x.go", "package p\n"+d.Source, 0)
		if e != nil {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				names[id.Name] = true
			}
			return true
		})
		if d.Kind == "type" {
			for _, de := range f.Decls {
				if g, ok := de.(*ast.GenDecl); ok {
					for _, sp := range g.Specs {
						if t, ok := sp.(*ast.TypeSpec); ok {
							if st, ok := t.Type.(*ast.StructType); ok {
								var fields []string
								for _, field := range st.Fields.List {
									if len(field.Names) > 0 {
										for _, n := range field.Names {
											fields = append(fields, n.Name)
										}
									} else {
										fields = append(fields, leaf(field.Type))
									}
								}
								if layouts[t.Name.Name] == nil || !strings.Contains(d.File, "/") {
									layouts[t.Name.Name] = fields
								}
							}
						}
					}
				}
			}
		}
	}
	for _, path := range []string{auditRoot + "/private-method-relocations.json", auditRoot + "/private-legacy-method-relocations.json"} {
		var rs []struct{ Method, Function string }
		load(path, &rs)
		for _, r := range rs {
			methods[r.Function] = r.Method
		}
	}
	for _, path := range []string{auditRoot + "/private-storage-relocations.json", auditRoot + "/private-legacy-storage-relocations.json"} {
		var rs []struct{ Kind, Owner string }
		load(path, &rs)
		for _, r := range rs {
			if r.Kind == "storage" {
				access[r.Owner+"Data"] = true
				access[r.Owner+"ValueData"] = true
			}
		}
	}
	for _, n := range []string{"SnapshotData", "PartitionKeyData", "QueryErrorData", "KeyspaceData"} {
		access[n] = true
	}
	var old, cur inventory
	load("feature-specs/evidence/step11-assertion-source.json", &old)
	load(auditRoot+"/current-174-assertions.json", &cur)
	index := map[string]test{}
	for _, t := range old.Tests {
		index[t.Identity] = t
	}
	var records []map[string]any
	var failed []string
	for _, t := range cur.Tests {
		o := index[t.Identity]
		a, b := norm(o.Translated.Source), norm(t.Translated.Source)
		same := tok(a) == tok(b)
		if !same {
			failed = append(failed, t.Identity)
			path := auditRoot + "/body-" + strings.ReplaceAll(strings.ReplaceAll(t.Identity, "/", "_"), "::", "-")
			os.WriteFile(path+".old", []byte(a), 0644)
			os.WriteFile(path+".new", []byte(b), 0644)
		}
		records = append(records, map[string]any{"identity": t.Identity, "original_sha256": t.Original.SHA256, "same_normalized_tokens": same, "step11_sha256": o.Translated.SHA256, "current_sha256": t.Translated.SHA256})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"count": len(records), "failed": failed, "tests": records})
	if len(failed) > 0 {
		fmt.Fprintln(os.Stderr, failed)
		os.Exit(1)
	}
}
