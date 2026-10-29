//go:build integration

package gocql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// createV2Cluster applies the integration flags to the public typed configuration.
// Options run last so each test can override the shared fixture defaults.
func createV2Cluster(opts ...func(*Config)) Config {
	config := DefaultConfig(getClusterHosts()...)
	config.Protocol = *flagProto
	config.CQLVersion = *flagCQL
	config.Metadata.AgreementTimeout = 2 * time.Minute
	profile := config.DefaultProfile.Snapshot()
	profile.Consistency = Quorum
	profile.Timeout = *flagTimeout
	profile.Retry = SafeRetryPolicy{MaxAttempts: max(1, *flagRetry+1)}
	if err := config.DefaultProfile.Replace(profile); err != nil {
		panic(err)
	}
	if *flagRunAuthTest {
		config.Auth = PasswordAuth("cassandra", "cassandra")
	}
	switch *flagCompressTest {
	case "snappy":
		config.Compression = SnappyCompression
	case "lz4":
		config.Compression = LZ4Compression
	case "no-compression", "":
		config.Compression = NoCompression
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}
	if *flagRunSslTest {
		if *flagDistribution == "cassandra" {
			config.Port = 9042
		} else {
			config.Port = 9142
		}
		certificate, err := tls.LoadX509KeyPair("testdata/pki/gocql.crt", "testdata/pki/gocql.key")
		if err != nil {
			panic(fmt.Errorf("load integration client certificate: %w", err))
		}
		pem, err := os.ReadFile("testdata/pki/ca.crt")
		if err != nil {
			panic(fmt.Errorf("load integration CA: %w", err))
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			panic("invalid integration CA certificate")
		}
		config.TLS = &tls.Config{
			Certificates: []tls.Certificate{certificate},
			RootCAs:      roots,
			MinVersion:   tls.VersionTLS12,
		}
	}
	for _, opt := range opts {
		opt(&config)
	}
	if *flagRunSslTest && config.TLS != nil && config.TLSForHost == nil {
		// The fixture identifies its server with a SPIFFE URI, not an IP SAN.
		// Keep the configured client certificates, session cache and callbacks,
		// but replace the default DNS check with chain and URI verification.
		fixtureTLS := config.TLS.Clone()
		verifyConnection := fixtureTLS.VerifyConnection
		fixtureTLS.InsecureSkipVerify = true
		fixtureTLS.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("integration TLS server supplied no certificate")
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range state.PeerCertificates[1:] {
				intermediates.AddCert(certificate)
			}
			now := time.Now()
			if fixtureTLS.Time != nil {
				now = fixtureTLS.Time()
			}
			chains, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         fixtureTLS.RootCAs,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				CurrentTime:   now,
			})
			if err != nil {
				return fmt.Errorf("verify integration TLS server chain: %w", err)
			}
			const expectedURI = "spiffe://test.cassandra.apache.org/cassandra-gocql-driver/integrationTest/cassandra"
			matched := false
			for _, identity := range state.PeerCertificates[0].URIs {
				if identity.String() == expectedURI {
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("integration TLS server certificate has no expected URI SAN %q", expectedURI)
			}
			if verifyConnection != nil {
				state.VerifiedChains = chains
				return verifyConnection(state)
			}
			return nil
		}
		config.TLS = fixtureTLS
		// The per-host API uses this complete TLS configuration directly and
		// avoids the legacy SSL option's mandatory DNS verification override.
		config.TLSForHost = func(context.Context, HostSnapshot) (*tls.Config, error) {
			return fixtureTLS.Clone(), nil
		}
	}
	return config
}

func createV2Session(t testing.TB, opts ...func(*Config)) *Session {
	t.Helper()
	return createV2SessionFromConfig(createV2Cluster(opts...), t, false)
}

func createV2SessionFromConfig(config Config, t testing.TB, tabletsDisabled bool) *Session {
	t.Helper()
	keyspace := "gocql_test"
	if tabletsDisabled {
		keyspace = "gocql_test_tablets_disabled"
	}
	// Use a separate once key while both fixture implementations coexist. Creating
	// an existing keyspace is harmless; dropping it would race other test sessions.
	initKeyspaceOnce.GetOnce("v2:" + keyspace).Do(func() {
		bootstrap := config
		bootstrap.Keyspace = "system"
		profile := config.DefaultProfile.Snapshot()
		profile.Timeout = 30 * time.Second
		var err error
		bootstrap.DefaultProfile, err = NewProfile(profile)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		session, err := Connect(ctx, bootstrap)
		if err != nil {
			t.Fatalf("connect to create typed integration keyspace: %v", err)
		}
		defer session.Close()
		supported, err := v2TabletsSupported(ctx, session)
		if err != nil {
			t.Fatalf("probe tablet support: %v", err)
		}
		cql := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': %d}", keyspace, *flagRF)
		if supported {
			cql += fmt.Sprintf(" AND tablets = {'enabled': %t}", !tabletsDisabled)
		}
		if err := v2CreateTable(ctx, session, cql); err != nil {
			t.Fatalf("create typed integration keyspace: %v", err)
		}
	})
	config.Keyspace = keyspace
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	session, err := Connect(ctx, config)
	if err != nil {
		t.Fatalf("create typed integration session: %v", err)
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		session.Close()
		t.Fatalf("wait for typed integration schema agreement: %v", err)
	}
	if err := session.RefreshMetadata(ctx); err != nil {
		session.Close()
		t.Fatalf("refresh typed integration metadata: %v", err)
	}
	return session
}

func v2TabletsSupported(ctx context.Context, session *Session) (bool, error) {
	row, err := v2ReadOne(ctx, session, "SELECT * FROM system.local")
	if err != nil {
		return false, err
	}
	// Cassandra does not expose supported_features. Check the schema before
	// looking up the value so a missing column is distinct from a decode failure.
	for i := 0; i < row.Columns().Len(); i++ {
		column, _ := row.Columns().At(i)
		if column.Name != "supported_features" {
			continue
		}
		value, err := row.At(i)
		if err != nil {
			return false, err
		}
		if value.State() == Null {
			return false, nil
		}
		features, err := value.Text()
		if err != nil {
			return false, err
		}
		for _, feature := range strings.Split(features.String(), ",") {
			if feature == "TABLETS" {
				return true, nil
			}
		}
		return false, nil
	}
	return false, nil
}

// v2Exec and v2Query accept only explicitly typed dynamic values. Owned binding
// lets callers release or change their inputs as soon as these helpers return.
func v2Exec(ctx context.Context, session *Session, cql string, values ...Value) (Result, error) {
	statement := Statement[[]Value, Unit]{CQL: cql, Params: DynamicParams, Rows: NoRows}
	var bound *Bound[Unit]
	var err error
	if len(values) == 0 {
		bound, err = BindUnpreparedOwned(statement, NewColumns(), values)
	} else {
		var prepared *Prepared[[]Value, Unit]
		prepared, err = Prepare(ctx, session, statement, PrepareOptions{})
		if err == nil {
			bound, err = BindOwned(prepared, values)
		}
	}
	if err != nil {
		return Result{}, err
	}
	defer bound.Close()
	return Exec(ctx, session, bound, Options{})
}

func v2Query(ctx context.Context, session *Session, cql string, values ...Value) (*Rows[DynamicRow], error) {
	statement := Statement[[]Value, DynamicRow]{CQL: cql, Params: DynamicParams, Rows: DynamicRows}
	var bound *Bound[DynamicRow]
	var err error
	if len(values) == 0 {
		bound, err = BindUnpreparedOwned(statement, NewColumns(), values)
	} else {
		var prepared *Prepared[[]Value, DynamicRow]
		prepared, err = Prepare(ctx, session, statement, PrepareOptions{})
		if err == nil {
			bound, err = BindOwned(prepared, values)
		}
	}
	if err != nil {
		return nil, err
	}
	// Run admits the request before returning. Rows owns the execution reference
	// until it is exhausted or closed, even after closing the binding here.
	defer bound.Close()
	return Run(ctx, session, bound, Options{})
}

func v2ReadOne(ctx context.Context, session *Session, cql string, values ...Value) (DynamicRow, error) {
	rows, err := v2Query(ctx, session, cql, values...)
	if err != nil {
		return DynamicRow{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return DynamicRow{}, err
		}
		return DynamicRow{}, ErrNoRows
	}
	row, err := rows.Clone()
	if err != nil {
		return DynamicRow{}, err
	}
	if rows.Next() {
		return DynamicRow{}, ErrMultipleRows
	}
	if err := rows.Err(); err != nil {
		return DynamicRow{}, err
	}
	return row, rows.Close()
}

func v2CreateTable(ctx context.Context, session *Session, cql string) error {
	bound, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
	if err != nil {
		return err
	}
	defer bound.Close()
	if _, err := Exec(ctx, session, bound, Options{Idempotent: true, Retry: SafeRetryPolicy{MaxAttempts: 4}}); err != nil {
		return err
	}
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		return err
	}
	return session.RefreshMetadata(ctx)
}
