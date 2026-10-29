//go:build integration
// +build integration

package gocql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTracingNewAPI(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session := createV2Session(t)
	defer session.Close()
	table := testTableName(t)
	require.NoError(t, v2CreateTable(ctx, session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key)`, table)))
	write := sessionTestBind(t, session, fmt.Sprintf(`INSERT INTO %s (id) VALUES (?)`, table), IntValue(42))
	defer write.Close()
	inserted, err := Exec(ctx, session, write, Options{Trace: true})
	require.NoError(t, err)
	require.NotEmpty(t, inserted.TraceIDs)
	prepared, err := Prepare(ctx, session, Statement[int32, int32]{CQL: fmt.Sprintf(`SELECT id FROM %s WHERE id = ?`, table), Params: OneParam(Int32), Rows: OneColumn(Int32)}, PrepareOptions{})
	require.NoError(t, err)
	read, err := BindOwned(prepared, int32(42))
	require.NoError(t, err)
	defer read.Close()
	rows, err := Run(ctx, session, read, Options{Trace: true})
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	require.Equal(t, int32(42), rows.Row())
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	selected := rows.Info()
	require.NoError(t, rows.Close())
	require.NotEmpty(t, selected.TraceIDs)
	// Trace polls readiness under the caller context and returns owned typed
	// events; the legacy Tracer's internal table queries are no longer used.
	for _, id := range append(inserted.TraceIDs, selected.TraceIDs...) {
		info, err := session.Trace(ctx, id)
		require.NoError(t, err)
		require.Equal(t, id, info.ID)
		require.True(t, info.Coordinator.IsValid(), "trace coordinator missing")
		require.False(t, info.Started.IsZero(), "trace start missing")
		require.NotEmpty(t, info.Events, "trace activities missing")
		for _, event := range info.Events {
			require.NotEmpty(t, event.Activity)
			require.True(t, event.Source.IsValid())
		}
	}
}
