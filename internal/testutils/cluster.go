package testutils

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

var initOnce sync.Once

func CreateSession(tb testing.TB, opts ...func(config *gocql.ClusterConfig)) *gocql.Session {
	cluster := CreateCluster(opts...)
	return createSessionFromCluster(cluster, tb)
}

func CreateCluster(opts ...func(*gocql.ClusterConfig)) *gocql.ClusterConfig {
	clusterHosts := getClusterHosts()
	cluster := gocql.NewCluster(clusterHosts...)
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = gocql.Quorum
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	switch *flagCompressTest {
	case "snappy":
		cluster.Compressor = &gocql.SnappyCompressor{}
	case "":
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}

	cluster = addSslOptions(cluster)

	for _, opt := range opts {
		opt(cluster)
	}

	return cluster
}

func createSessionFromCluster(cluster *gocql.ClusterConfig, tb testing.TB) *gocql.Session {
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		createKeyspace(tb, cluster, "gocql_test")
	})

	cluster.Keyspace = "gocql_test"
	session, err := cluster.CreateSession()
	if err != nil {
		tb.Fatal("createSession:", err)
	}

	if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
		tb.Fatal(err)
	}

	return session
}

func getClusterHosts() []string {
	return strings.Split(*flagCluster, ",")
}

func createKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string) {
	// TODO: tb.Helper()
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = CreateTable(session, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		panic(fmt.Sprintf("unable to drop keyspace: %v", err))
	}

	err = CreateTable(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))

	if err != nil {
		panic(fmt.Sprintf("unable to create keyspace: %v", err))
	}
}

func CreateTable(s *gocql.Session, table string) error {
	// lets just be really sure
	if err := s.AwaitSchemaAgreement(context.Background()); err != nil {
		log.Printf("error waiting for schema agreement pre create table=%q err=%v\n", table, err)
		return err
	}

	if err := s.Query(table).RetryPolicy(&gocql.SimpleRetryPolicy{}).Exec(); err != nil {
		log.Printf("error creating table table=%q err=%v\n", table, err)
		return err
	}

	if err := s.AwaitSchemaAgreement(context.Background()); err != nil {
		log.Printf("error waiting for schema agreement post create table=%q err=%v\n", table, err)
		return err
	}

	return nil
}

func addSslOptions(cluster *gocql.ClusterConfig) *gocql.ClusterConfig {
	if *flagRunSslTest {
		cluster.Port = 9142
		cluster.SslOpts = &gocql.SslOptions{
			CertPath:               "testdata/pki/gocql.crt",
			KeyPath:                "testdata/pki/gocql.key",
			CaPath:                 "testdata/pki/ca.crt",
			EnableHostVerification: false,
		}
	}
	return cluster
}
