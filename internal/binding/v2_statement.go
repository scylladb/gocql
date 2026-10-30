// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
import "fmt"
import "github.com/scylladb/gocql/v2/internal/codec"
import "github.com/scylladb/gocql/v2/internal/wire"
import "strings"

func InternalEncodeParameters[P any](plan ParamPlan[P], columns Columns, value P, owned, allowUnset bool) (encodedValues, error) {
	if columns.Len() > 65535 {
		return encodedValues{}, typeValidation("parameters", "maximum count is 65535")
	}
	b := wire.NewBuilder(4096)
	writer := newParamWriter(b, columns, allowUnset)
	codec.SetParamOwnership(writer, owned)
	if err := plan.Encode(writer, value); err != nil {
		return encodedValues{}, err
	}
	if err := finishParamWriter(writer); err != nil {
		return encodedValues{}, err
	}
	framed, err := b.Freeze()
	if err != nil {
		return encodedValues{}, err
	}
	cursor := wire.NewCursor(framed)
	cells := make([]encodedCell, columns.Len())
	for i := range cells {
		column, _ := columns.At(i)
		v, err := readValue(cursor, column.Type, nil, allowUnset)
		if err != nil {
			return encodedValues{}, err
		}
		cells[i] = encodedCell{Typ: column.Type, State: v.State(), Body: cqltype.BytesChain(v.Bytes())}
	}
	if err := cursor.RequireEnd(); err != nil {
		return encodedValues{}, fmt.Errorf("gocql: parameter framing: %w", err)
	}
	return encodedValues{Columns: columns, Cells: cells, Framed: framed}, nil
}

type Statement[P, R any] struct {
	Kind   StatementKind
	CQL    string
	Params ParamEncoder[P]
	Rows   RowDecoder[R]
}

func InternalValidateStatement[P, R any](s Statement[P, R]) error {
	if s.Kind > ConditionalStatement {
		return typeValidation("statement kind", "unknown classification")
	}
	if strings.TrimSpace(s.CQL) == "" {
		return typeValidation("statement", "empty CQL")
	}
	if InternalFirstCQLWord(s.CQL) == "USE" {
		return ErrUseStmt
	}
	if s.Params == nil || s.Rows == nil {
		return typeValidation("statement", "parameter and row codecs are required")
	}
	return nil
}

func InternalFirstCQLWord(cql string) string {
	word := ""
	InternalVisitCQLWords(cql, func(next string) bool { word = next; return false })
	return word
}
func InternalConditionalCQL(cql string) bool {
	conditional := false
	InternalVisitCQLWords(cql, func(word string) bool {
		if word == "IF" {
			conditional = true
			return false
		}
		return true
	})
	return conditional
}
func InternalVisitCQLWords(cql string, visit func(string) bool) {
	for i := 0; i < len(cql); {
		if cql[i] == '\'' || cql[i] == '"' {
			quote := cql[i]
			i++
			for i < len(cql) {
				if cql[i] == quote {
					i++
					if i < len(cql) && cql[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if i+1 < len(cql) && cql[i] == '-' && cql[i+1] == '-' {
			i += 2
			for i < len(cql) && cql[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(cql) && cql[i] == '/' && cql[i+1] == '*' {
			i += 2
			for i+1 < len(cql) && (cql[i] != '*' || cql[i+1] != '/') {
				i++
			}
			i += 2
			continue
		}
		if cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] == '_' {
			start := i
			i++
			for i < len(cql) && (cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] >= '0' && cql[i] <= '9' || cql[i] == '_') {
				i++
			}
			if !visit(strings.ToUpper(cql[start:i])) {
				return
			}
			continue
		}
		i++
	}
}
