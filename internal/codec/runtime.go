// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Runtime constructors reuse the caller's cursor and final row-reader storage.
func RowReaderFromCursor(columns cqltype.Columns, cursor *wire.Cursor, lease *wire.Lease) RowReader {
	return RowReader{columns: columns, cursor: cursor, lease: lease}
}
func SetParamOwnership(w *ParamWriter, owned bool) { w.ownBorrowed = owned }
func BeginStreamRow(r *StreamRowReader)            { r.index = 0 }
