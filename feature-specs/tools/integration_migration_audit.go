//go:build ignore

// Inventory the pinned original test bodies and translated assertions.
// This records source evidence; behavioral equivalence is reviewed separately.
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

type Manifest struct {
	Revision string
	Files    []struct {
		File  string
		Tests []string
	}
}
type Assertion struct {
	Line int    `json:"line"`
	Text string `json:"text"`
}
type Function struct {
	File       string      `json:"file"`
	Line       int         `json:"line"`
	SHA        string      `json:"sha256"`
	Source     string      `json:"source"`
	Assertions []Assertion `json:"assertions"`
	Calls      []string    `json:"calls"`
	Skips      []Assertion `json:"skips"`
}

func functions(src []byte) map[string]Function {
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, "source.go", src, 0)
	if err != nil {
		panic(err)
	}
	out := map[string]Function{}
	text := func(n ast.Node) string { return string(src[fs.Position(n.Pos()).Offset:fs.Position(n.End()).Offset]) }
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		body := text(fn)
		v := Function{Line: fs.Position(fn.Pos()).Line, SHA: fmt.Sprintf("%x", sha256.Sum256([]byte(body))), Source: body}
		seen := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := text(c.Fun)
			if !seen[name] {
				v.Calls = append(v.Calls, name)
				seen[name] = true
			}
			a := Assertion{fs.Position(c.Pos()).Line, text(c)}
			if strings.HasPrefix(name, "require.") || strings.HasPrefix(name, "assert.") || strings.HasPrefix(name, "tests.Assert") || name == "t.Fatal" || name == "t.Fatalf" || name == "t.Error" || name == "t.Errorf" {
				v.Assertions = append(v.Assertions, a)
			}
			if name == "t.Skip" || name == "t.Skipf" {
				v.Skips = append(v.Skips, a)
			}
			return true
		})
		name := fn.Name.Name
		if fn.Recv != nil {
			name = strings.TrimPrefix(text(fn.Recv.List[0].Type), "*") + "." + name
		}
		out[name] = v
	}
	return out
}
func helperIDs(root Function, index map[string]Function, retained map[string]Function) []string {
	seen := map[string]bool{}
	var visit func(Function)
	visit = func(fn Function) {
		for _, call := range fn.Calls {
			name := strings.SplitN(call, "[", 2)[0]
			if strings.Contains(name, ".") || strings.HasPrefix(name, "Test") || seen[name] {
				continue
			}
			child, ok := index[name]
			if !ok {
				continue
			}
			seen[name] = true
			retained[child.File+"::"+name] = child
			visit(child)
		}
	}
	visit(root)
	var ids []string
	for name := range seen {
		child := index[name]
		ids = append(ids, child.File+"::"+name)
	}
	sort.Strings(ids)
	return ids
}

func main() {
	data, err := os.ReadFile("feature-specs/integration-baseline.json")
	if err != nil {
		panic(err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		panic(err)
	}
	original, translated := map[string]Function{}, map[string]Function{}
	byFileOriginal, byFileTranslated := map[string]map[string]Function{}, map[string]map[string]Function{}
	paths, err := filepath.Glob("*_test.go")
	if err != nil {
		panic(err)
	}
	paths = append(paths, "internal/ccm/ccm_test.go")
	for _, path := range paths {
		now, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		funcs := functions(now)
		for name, fn := range funcs {
			fn.File = path
			funcs[name] = fn
			translated[name] = fn
		}
		byFileTranslated[path] = funcs
		old, err := exec.Command("git", "show", m.Revision+":"+path).Output()
		if err != nil {
			continue
		} // Helpers introduced by migration have no original.
		funcs = functions(old)
		for name, fn := range funcs {
			fn.File = path
			funcs[name] = fn
			original[name] = fn
		}
		byFileOriginal[path] = funcs
	}
	result := []any{}
	originalHelpers, translatedHelpers := map[string]Function{}, map[string]Function{}
	originalMethods, translatedMethods := map[string]Function{}, map[string]Function{}
	for _, file := range m.Files {
		for name, fn := range byFileOriginal[file.File] {
			if strings.Contains(name, ".") {
				originalMethods[file.File+"::"+name] = fn
			}
		}
		for name, fn := range byFileTranslated[file.File] {
			if strings.Contains(name, ".") {
				translatedMethods[file.File+"::"+name] = fn
			}
		}
	}
	for path, funcs := range byFileTranslated {
		if strings.Contains(path, "fixture") || path == "cassandra_v2_helpers_test.go" {
			for name, fn := range funcs {
				if strings.Contains(name, ".") {
					translatedMethods[path+"::"+name] = fn
				}
			}
		}
	}
	for _, file := range m.Files {
		for _, name := range file.Tests {
			o, ok := byFileOriginal[file.File][name]
			if !ok {
				panic(name)
			}
			n, ok := byFileTranslated[file.File][name]
			if !ok {
				panic(name)
			}
			result = append(result, map[string]any{"identity": file.File + "::" + name, "file": file.File, "test": name, "original": o, "translated": n, "original_helper_ids": helperIDs(o, original, originalHelpers), "translated_helper_ids": helperIDs(n, translated, translatedHelpers)})
		}
	}
	if len(result) != 174 {
		panic("baseline identity count changed")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(map[string]any{"revision": m.Revision, "test_count": len(result), "tests": result, "helpers": map[string]any{"original": originalHelpers, "translated": translatedHelpers}, "supporting_methods": map[string]any{"original": originalMethods, "translated": translatedMethods}}); err != nil {
		panic(err)
	}
}
