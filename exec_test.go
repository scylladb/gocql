//go:build integration
// +build integration

package gocql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleHostQueryExecutor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	config := createV2Cluster()
	// The standalone executor is replaced by a typed session restricted to a
	// discovered host identity, rather than a second query execution engine.
	discovery, err := Connect(ctx, config)
	require.NoError(t, err)
	hosts := discovery.Metadata().Hosts()
	require.NotEmpty(t, hosts)
	hostID := hosts[0].ID
	require.NoError(t, discovery.Close())
	session, err := OpenSingleHost(ctx, config, hostID)
	require.NoError(t, err)
	defer session.Close()
	bound, err := BindUnpreparedOwned(Statement[Unit, TimeUUID]{CQL: "SELECT now() FROM system.local", Params: NoParams, Rows: OneColumn(TimeUUIDCodec)}, NewColumns(), Unit{})
	require.NoError(t, err)
	defer bound.Close()
	rows, err := Run(ctx, session, bound, Options{})
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	require.False(t, UUID(rows.Row()).IsEmpty())
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.Equal(t, hostID, rows.Info().HostID)
	for _, host := range session.Metadata().Hosts() {
		if host.Connected {
			require.Equal(t, hostID, host.ID, "single-host session connected to another host")
		}
	}
}
