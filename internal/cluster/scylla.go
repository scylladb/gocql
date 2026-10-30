// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

var _ cqlProtocolExtension = &tabletsRoutingV1Ext{}

var _ cqlProtocolExtension = &rateLimitExt{}

var _ cqlProtocolExtension = &lwtAddMetadataMarkExt{}

var _ cqlProtocolExtension = &scyllaUseMetadataIDExt{}

// Returns a partitioner specific to the table, or "nil"
// if the cluster-global partitioner should be used
func InternalScyllaGetTablePartitioner(session *Session, keyspaceName, tableName string) (Partitioner, error) {
	isCdc, err := InternalScyllaIsCdcTable(session, keyspaceName, tableName)
	if err != nil {
		return nil, err
	}
	if isCdc {
		return scyllaCDCPartitioner{InternalLogger: &defaultLogger{}}, nil
	}

	return nil, nil
}
