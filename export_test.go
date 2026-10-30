//go:build integration
// +build integration

package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

var FlagRunSslTest = flagRunSslTest
var FlagDistribution = flagDistribution

var CreateV2Cluster = createV2Cluster
var TestLogger = &testLogger{}
var WaitUntilPoolsStopFilling = waitUntilPoolsStopFilling

func GetRingAllHosts(sess *Session) []*HostInfo {
	return implCluster.SessionData(sess).InternalHostSource.InternalGetHostsList()
}

// NewTestLogger isolates each fixture capture and supports concurrent writes.
func NewTestLogger() *testLogger { return &testLogger{} }
