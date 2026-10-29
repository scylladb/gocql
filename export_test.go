//go:build integration
// +build integration

package gocql

var FlagRunSslTest = flagRunSslTest
var FlagDistribution = flagDistribution
var CreateCluster = createCluster
var CreateV2Cluster = createV2Cluster
var TestLogger = &testLogger{}
var WaitUntilPoolsStopFilling = waitUntilPoolsStopFilling

func GetRingAllHosts(sess *Session) []*HostInfo {
	return sess.hostSource.getHostsList()
}
