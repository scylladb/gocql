//go:build ignore

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Verify the approved extraction-only edits against every accepted step10
// integration identity. Expected source changes are explicit, one-way rewrites;
// this does not erase arbitrary identifiers or expressions from either input.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/scanner"
	"go/token"
	"os"
	"regexp"
	"strings"
)

type body struct {
	Source string `json:"source"`
	SHA    string `json:"sha256"`
}
type test struct {
	Identity   string `json:"identity"`
	File       string `json:"file"`
	Test       string `json:"test"`
	Original   body   `json:"original"`
	Translated body   `json:"translated"`
}
type inventory struct {
	Tests []test `json:"tests"`
}

func read(path string) inventory {
	b, e := os.ReadFile(path)
	if e != nil {
		panic(e)
	}
	var v inventory
	if e = json.Unmarshal(b, &v); e != nil {
		panic(e)
	}
	return v
}
func tokens(src string) string {
	fs := token.NewFileSet()
	f := fs.AddFile("", fs.Base(), len(src))
	var scan scanner.Scanner
	scan.Init(f, []byte(src), nil, 0)
	var parts []string
	comma := fmt.Sprintf("%d:%q;", token.COMMA, "")
	for {
		_, t, l := scan.Scan()
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

func expected(s string) string {
	// The three runtime probes name only moved nongeneric model/protocol members.
	for _, p := range [][2]string{{".cells", ".Cells"}, {".writeHeader(", ".WriteHeader("}, {".finish(", ".Finish("}, {".owner.", ".Owner."}, {"attemptRequest{statement:", "attemptRequest{Statement:"}, {", consistency:", ", Consistency:"}, {".writeShort(", ".WriteShort("}, {".writeString(", ".WriteString("}, {".readTypeInfo(", ".ReadTypeInfo("}} {
		s = strings.ReplaceAll(s, p[0], p[1])
	}
	// NativeType's former private fields move intact behind its existing metadata
	// constructor. All omitted proto/custom zero values are explicit here.
	re := regexp.MustCompile(`NativeType\{typ: ([A-Za-z0-9_]+)(?:, custom: ([A-Za-z0-9_]+))?\}`)
	s = re.ReplaceAllStringFunc(s, func(v string) string {
		m := re.FindStringSubmatch(v)
		custom := "\"\""
		if m[2] != "" {
			custom = m[2]
		}
		return "cqltype.NewCustomType(0, " + m[1] + ", " + custom + ")"
	})
	return s
}
func main() {
	old := read("feature-specs/evidence/step10-assertion-source.json")
	cur := read("feature-specs/evidence/step11-assertion-source.json")
	previous := map[string]test{}
	for _, t := range old.Tests {
		previous[t.Identity] = t
	}
	var records []any
	var changed []string
	for _, t := range cur.Tests {
		p, ok := previous[t.Identity]
		if !ok || p.Original.SHA != t.Original.SHA {
			panic("original changed: " + t.Identity)
		}
		same := tokens(p.Translated.Source) == tokens(t.Translated.Source)
		equivalent := same || tokens(expected(p.Translated.Source)) == tokens(t.Translated.Source)
		if !equivalent {
			panic("unapproved test change: " + t.Identity)
		}
		if !same {
			changed = append(changed, t.Identity)
		}
		records = append(records, map[string]any{"identity": t.Identity, "original_sha256": t.Original.SHA, "step10_sha256": p.Translated.SHA, "step11_sha256": t.Translated.SHA, "same_tokens_as_step10": same, "matches_explicit_extraction_rewrite": equivalent})
	}
	if len(records) != 174 {
		panic("identity count changed")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"count": 174, "all_original_bodies_match_step10": true, "all_translated_bodies_match_explicit_extraction_rewrites": true, "changed_token_identities": changed, "tests": records})
}
