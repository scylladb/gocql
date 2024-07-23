package testcmdline

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	Cluster          = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	MultiNodeCluster = flag.String("multiCluster", "127.0.0.2", "a comma-separated list of host:port tuples")
	Proto            = flag.Int("proto", 0, "protcol version")
	CQL              = flag.String("cql", "3.0.0", "CQL version")
	RF               = flag.Int("rf", 1, "replication factor for test keyspace")
	ClusterSize      = flag.Int("clusterSize", 1, "the expected size of the cluster")
	Retry            = flag.Int("retries", 5, "number of times to retry queries")
	AutoWait         = flag.Duration("autowait", 1000*time.Millisecond, "time to wait for autodiscovery to fill the hosts poll")
	RunSslTest       = flag.Bool("runssl", false, "Set to true to run ssl test")
	RunAuthTest      = flag.Bool("runauth", false, "Set to true to run authentication test")
	CompressTest     = flag.String("compressor", "", "compressor to use")
	Timeout          = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")
	CassVersion      cassVersion
)

type cassVersion struct {
	Major, Minor, Patch int
}

func (c *cassVersion) Set(v string) error {
	if v == "" {
		return nil
	}

	return c.unmarshal([]byte(v))
}

func (c *cassVersion) unmarshal(data []byte) error {
	version := strings.TrimSuffix(string(data), "-SNAPSHOT")
	version = strings.TrimPrefix(version, "v")
	v := strings.Split(version, ".")

	if len(v) < 2 {
		return fmt.Errorf("invalid version string: %s", data)
	}

	var err error
	c.Major, err = strconv.Atoi(v[0])
	if err != nil {
		return fmt.Errorf("invalid major version %v: %v", v[0], err)
	}

	c.Minor, err = strconv.Atoi(v[1])
	if err != nil {
		return fmt.Errorf("invalid minor version %v: %v", v[1], err)
	}

	if len(v) > 2 {
		c.Patch, err = strconv.Atoi(v[2])
		if err != nil {
			return fmt.Errorf("invalid patch version %v: %v", v[2], err)
		}
	}

	return nil
}

func (c cassVersion) Before(major, minor, patch int) bool {
	// We're comparing us (cassVersion) with the provided version (major, minor, patch)
	// We return true if our version is lower (comes before) than the provided one.
	if c.Major < major {
		return true
	} else if c.Major == major {
		if c.Minor < minor {
			return true
		} else if c.Minor == minor && c.Patch < patch {
			return true
		}

	}
	return false
}

func (c cassVersion) AtLeast(major, minor, patch int) bool {
	return !c.Before(major, minor, patch)
}

func (c cassVersion) String() string {
	return fmt.Sprintf("v%d.%d.%d", c.Major, c.Minor, c.Patch)
}

func (c cassVersion) nodeUpDelay() time.Duration {
	if c.Major >= 2 && c.Minor >= 2 {
		// CASSANDRA-8236
		return 0
	}

	return 10 * time.Second
}

func init() {
	flag.Var(&CassVersion, "gocql.cversion", "the cassandra version being tested against")
}
