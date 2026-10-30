package gocql

import (
	"os"
	"strings"
	"testing"
)

// splitFixtureCQL respects quoted literals, identifiers and $$ Java/Lua bodies.
// The fixtures intentionally have no alternate dollar tags or line comments.
func splitFixtureCQL(source string) []string {
	var result []string
	start := 0
	var quote byte
	dollar := false
	for i := 0; i < len(source); i++ {
		ch := source[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(source) && source[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if i+1 < len(source) && source[i:i+2] == "$$" {
			dollar = !dollar
			i++
			continue
		}
		if dollar {
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ';' {
			result = append(result, source[start:i])
			start = i + 1
		}
	}
	return append(result, source[start:])
}

func fixtureKeyspace(suffix string) string {
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		prefix = "gocql_test"
	}
	if len(prefix) > 24 {
		panic("GOCQL_TEST_RUN exceeds 24 characters")
	}
	for _, c := range prefix {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			panic("invalid GOCQL_TEST_RUN identifier")
		}
	}
	if prefix[0] < 'a' || prefix[0] > 'z' {
		panic("GOCQL_TEST_RUN must start with a lowercase letter")
	}
	return prefix + suffix
}

func TestStep08FixtureCQL(t *testing.T) {
	source := "CREATE FUNCTION f() AS $$ int x=1; return x; $$; SELECT ';'';'; SELECT \"a;\";"
	queries := splitFixtureCQL(source)
	if len(queries) != 4 || !strings.Contains(queries[0], "return x;") || strings.TrimSpace(queries[3]) != "" {
		t.Fatalf("split: %#v", queries)
	}
}
