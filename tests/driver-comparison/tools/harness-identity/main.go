// Emit hashes of canonical workload code independently of delivery bookkeeping.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) != 1 {
		panic("expected one harness main.go path")
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, args[0], nil, parser.SkipObjectResolution)
	if err != nil {
		panic(err)
	}
	wanted := map[string]bool{}
	for _, name := range []string{"initialize", "variant", "payload", "validator", "operation", "warmDataset",
		"keys", "partitions", "rowsPerPartition", "pointRead", "pageRead", "pointWrite", "pageWrite"} {
		wanted[name] = true
	}
	hashes := map[string]string{}
	add := func(name string, node ast.Node) {
		if !wanted[name] {
			return
		}
		var code bytes.Buffer
		if err := format.Node(&code, set, node); err != nil {
			panic(err)
		}
		sum := sha256.Sum256(code.Bytes())
		hashes[name] = hex.EncodeToString(sum[:])
	}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			add(decl.Name.Name, decl)
		case *ast.GenDecl:
			if decl.Tok != token.CONST {
				continue
			}
			for _, spec := range decl.Specs {
				value := spec.(*ast.ValueSpec)
				for _, name := range value.Names {
					add(name.Name, value)
				}
			}
		}
	}
	if len(hashes) != len(wanted) {
		panic(fmt.Sprintf("incomplete canonical workload identity: %v", hashes))
	}
	if err := json.NewEncoder(os.Stdout).Encode(hashes); err != nil {
		panic(err)
	}
}
