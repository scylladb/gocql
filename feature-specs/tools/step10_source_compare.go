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

// Compare every baseline identity with accepted step09 source evidence.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/scanner"
	"go/token"
	"os"
)

type fn struct {
	Source string `json:"source"`
	SHA    string `json:"sha256"`
}
type record struct {
	Identity   string `json:"identity"`
	File       string `json:"file"`
	Test       string `json:"test"`
	Original   fn     `json:"original"`
	Translated fn     `json:"translated"`
}
type inventory struct {
	Tests []record `json:"tests"`
}

func read(path string) inventory {
	body, e := os.ReadFile(path)
	if e != nil {
		panic(e)
	}
	var v inventory
	if e = json.Unmarshal(body, &v); e != nil {
		panic(e)
	}
	return v
}
func tokens(src string) string {
	fs := token.NewFileSet()
	f := fs.AddFile("fn", fs.Base(), len(src))
	var scan scanner.Scanner
	scan.Init(f, []byte(src), nil, 0)
	out := ""
	for {
		_, tok, lit := scan.Scan()
		if tok == token.SEMICOLON {
			continue
		}
		if tok == token.EOF {
			break
		}
		out += fmt.Sprintf("%d:%q;", tok, lit)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(out)))
}
func main() {
	old := read("feature-specs/evidence/step09-assertion-source.json")
	current := read(os.Args[1])
	previous := map[string]record{}
	for _, r := range old.Tests {
		previous[r.Identity] = r
	}
	changed := []string{}
	out := []any{}
	for _, r := range current.Tests {
		p, ok := previous[r.Identity]
		if !ok || r.Original.SHA != p.Original.SHA {
			panic("original identity/body changed: " + r.Identity)
		}
		same := tokens(r.Translated.Source) == tokens(p.Translated.Source)
		if !same {
			changed = append(changed, r.Identity)
		}
		out = append(out, map[string]any{"identity": r.Identity, "file": r.File, "test": r.Test, "original_sha256": r.Original.SHA, "translated_sha256": r.Translated.SHA, "same_tokens_as_step09": same, "translated_source": r.Translated.Source})
	}
	if len(out) != 174 {
		panic("identity count changed")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"count": len(out), "changed_test_tokens": changed, "original_bodies_verified_against_step09": true, "tests": out})
}
