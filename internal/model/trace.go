package model

import (
	"net/netip"
"github.com/scylladb/gocql/v2/internal/cqltype"
)

type TraceSessionRow struct {
	InternalCoordinator cqltype.Nullable[netip.Addr]
	InternalStarted     cqltype.Nullable[cqltype.Timestamp]
	InternalDuration    cqltype.Nullable[int32]
}
type TraceActivityRow struct {
	InternalId       cqltype.TimeUUID
	InternalActivity cqltype.Text
	InternalSource   netip.Addr
	InternalElapsed  cqltype.Nullable[int32]
	InternalThread   cqltype.Text
}
