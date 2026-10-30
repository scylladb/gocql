// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import "strings"

var _ Partitioner = scyllaCDCPartitioner{InternalLogger: &defaultLogger{}}

var _ int64Hasher = scyllaCDCPartitioner{}

func InternalScyllaIsCdcTable(session *Session, keyspaceName, tableName string) (bool, error) {
	if !strings.HasSuffix(tableName, scyllaCDCLogTableNameSuffix) {
		// Not a CDC table, use the default partitioner
		return false, nil
	}

	// Check if the table has the CDC partitioner set.
	tableMeta, err := session.TableMetadata(keyspaceName, tableName)
	if err != nil {
		return false, err
	}

	return tableMeta.Options.Partitioner == scyllaCDCPartitionerFullName, nil
}
