// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Command streaming demonstrates typed execution into reusable caller storage.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2"
)

type record struct {
	ID               int32
	Payload          []byte
	Pieces           [][]byte
	ApplicationState string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() (resultErr error) {
	hosts := flag.String("hosts", "127.0.0.1:9042", "comma-separated native endpoints")
	incremental := flag.Bool("incremental", false, "use bounded incremental receive instead of page buffering")
	protocol := flag.Int("protocol", 4, "native protocol version")
	compression := flag.String("compression", "none", "none, lz4 or snappy")
	username := flag.String("username", "", "authentication username")
	password := flag.String("password", "", "authentication password (prefer GOCQL_PASSWORD)")
	ca := flag.String("ca", "", "TLS CA PEM file")
	cert := flag.String("cert", "", "TLS client certificate PEM file")
	key := flag.String("key", "", "TLS client private key PEM file")
	serverName := flag.String("server-name", "", "TLS server DNS identity")
	uri := flag.String("tls-uri", "", "explicit TLS URI identity instead of DNS")
	flag.Parse()
	cfg := gocql.DefaultConfig(strings.Split(*hosts, ",")...)
	if _, port, err := net.SplitHostPort(strings.Split(*hosts, ",")[0]); err == nil {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return err
		}
		cfg.Port = uint16(n)
	}
	cfg.Protocol = *protocol
	cfg.Pool.ConnectionsPerHost = 1
	cfg.Pool.ShardAwarePort = false
	cfg.KeepaliveInterval = 0
	cfg.Metadata.PeriodicRefresh = 0
	switch *compression {
	case "none":
	case "lz4":
		cfg.Compression = gocql.LZ4Compression
	case "snappy":
		cfg.Compression = gocql.SnappyCompression
	default:
		return fmt.Errorf("unknown compression %q", *compression)
	}
	if *password == "" {
		*password = os.Getenv("GOCQL_PASSWORD")
	}
	if *username != "" {
		cfg.Auth = gocql.PasswordAuth(*username, *password)
	}
	if *ca != "" {
		pem, err := os.ReadFile(*ca)
		if err != nil {
			return err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return fmt.Errorf("invalid CA PEM")
		}
		cfg.TLS = &tls.Config{RootCAs: roots, ServerName: *serverName, MinVersion: tls.VersionTLS12}
		if *cert != "" {
			pair, err := tls.LoadX509KeyPair(*cert, *key)
			if err != nil {
				return err
			}
			cfg.TLS.Certificates = []tls.Certificate{pair}
		}
		if *uri != "" {
			// Explicit URI identity verification also verifies the complete CA chain.
			cfg.TLS.InsecureSkipVerify = true
			cfg.TLS.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("missing peer certificate")
				}
				intermediates := x509.NewCertPool()
				for _, c := range state.PeerCertificates[1:] {
					intermediates.AddCert(c)
				}
				if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
					return err
				}
				for _, id := range state.PeerCertificates[0].URIs {
					if id.String() == *uri {
						return nil
					}
				}
				return fmt.Errorf("missing expected TLS URI %q", *uri)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, err := gocql.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, s.Close()) }()
	prefix := os.Getenv("GOCQL_TEST_RUN")
	if prefix == "" {
		prefix = "gocql"
	}
	id, err := gocql.RandomUUID()
	if err != nil {
		return err
	}
	namespace := prefix + "_stream_" + strings.ReplaceAll(id.String(), "-", "")[:16]
	if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`).MatchString(namespace) {
		return fmt.Errorf("invalid owned namespace %q", namespace)
	}
	execContext := func(operation context.Context, cql string) error {
		b, err := gocql.NewUnpreparedQuery(s, gocql.Statement[gocql.Unit, gocql.Unit]{CQL: cql, Params: gocql.NoParams, Rows: gocql.NoRows}, gocql.NewColumns(), gocql.Options{})
		if err != nil {
			return err
		}
		defer b.Close()
		_, err = b.Exec(operation, gocql.Unit{})
		return err
	}
	exec := func(cql string) error { return execContext(ctx, cql) }
	owned := true
	defer func() {
		if owned {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := execContext(cleanup, "DROP KEYSPACE IF EXISTS "+namespace)
			resultErr = errors.Join(resultErr, err)
			if err == nil {
				fmt.Println("CLEANED", namespace)
			}
		}
	}()
	if err := exec("CREATE KEYSPACE " + namespace + " WITH replication = {'class':'NetworkTopologyStrategy','replication_factor':1}"); err != nil {
		var exists *gocql.RequestErrAlreadyExists
		if errors.As(err, &exists) {
			owned = false
		}
		return err
	}
	fmt.Println("READY", namespace)
	switch os.Getenv("GOCQL_EXAMPLE_FAILURE") {
	case "error":
		return errors.New("injected example failure after create")
	case "cancel":
		cancel()
		return ctx.Err()
	case "pause":
		<-ctx.Done()
		return ctx.Err()
	}
	table := namespace + ".records"
	if err := exec("CREATE TABLE " + table + " (id int PRIMARY KEY,payload blob,pieces list<blob>)"); err != nil {
		return err
	}
	insert, err := gocql.Prepare(ctx, s, gocql.Statement[record, gocql.Unit]{CQL: "INSERT INTO " + table + " (id,payload,pieces) VALUES (?,?,?)", Params: gocql.Params(
		gocql.Param("id", gocql.Int32, func(v record) int32 { return v.ID }),
		gocql.Param("payload", gocql.BytesEncoder, func(v record) []byte { return v.Payload }),
		gocql.Param("pieces", gocql.ListEncoder(gocql.BytesEncoder), func(v record) [][]byte { return v.Pieces }),
	), Rows: gocql.NoRows}, gocql.PrepareOptions{})
	if err != nil {
		return err
	}
	writeQuery, err := gocql.NewQuery(insert, gocql.Options{})
	if err != nil {
		return err
	}
	defer writeQuery.Close()
	payload := bytes.Repeat([]byte("typed-stream-"), 16384)
	for i := int32(0); i < 3; i++ {
		if _, err := writeQuery.Exec(ctx, record{ID: i, Payload: payload, Pieces: [][]byte{[]byte("nested"), []byte("storage")}}); err != nil {
			return err
		}
		if err := writeQuery.Wait(ctx); err != nil {
			return err
		}
	}
	prepared, err := gocql.Prepare(ctx, s, gocql.Statement[gocql.Unit, gocql.DynamicRow]{CQL: "SELECT id,payload,pieces FROM " + table, Params: gocql.NoParams, Rows: gocql.DynamicRows}, gocql.PrepareOptions{})
	if err != nil {
		return err
	}
	decoder := gocql.StreamRow(func(r *record) { r.ApplicationState = "" },
		gocql.StreamColumn("id", gocql.StreamInt32, func(r *record) *int32 { return &r.ID }),
		gocql.StreamColumn("payload", gocql.StreamBlob, func(r *record) *[]byte { return &r.Payload }),
		gocql.StreamColumn("pieces", gocql.StreamList(gocql.StreamBlob), func(r *record) *[][]byte { return &r.Pieces }),
	)
	hostsSnapshot := s.Metadata().Hosts()
	if len(hostsSnapshot) == 0 {
		return fmt.Errorf("missing discovered hosts")
	}
	options := gocql.StreamOptions{Incremental: *incremental, Options: gocql.Options{PageSize: gocql.Option[int32]{Set: true, Value: 2}, HostID: gocql.Option[gocql.UUID]{Set: true, Value: hostsSnapshot[0].ID}}}
	query, err := gocql.NewStreamQuery(prepared, decoder, options)
	if err != nil {
		return err
	}
	defer query.Close()
	stream, err := query.Run(ctx, gocql.Unit{})
	if *incremental && cfg.Protocol < 5 && cfg.Compression != gocql.NoCompression {
		var unsupported *gocql.StreamCapabilityError
		if !errors.As(err, &unsupported) {
			return fmt.Errorf("expected frame compression capability error, got %v", err)
		}
		fmt.Println("frame compression explicitly rejected before streaming submission")
		return nil
	}
	if err != nil {
		return err
	}
	defer stream.Close()
	// One worker owns one query; overlapping use is rejected immediately.
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, blockedErr := query.Run(blockedCtx, gocql.Unit{})
	blockedCancel()
	if !errors.Is(blockedErr, gocql.ErrBusy) {
		return fmt.Errorf("query admission: %v", blockedErr)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	waitErr := query.Wait(waitCtx)
	waitCancel()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		return fmt.Errorf("active query drain: %v", waitErr)
	}
	// Seed both the outer slots and every nested payload before the first NextInto.
	row := record{Payload: make([]byte, 0, len(payload)), Pieces: [][]byte{make([]byte, 0, 16), make([]byte, 0, 16)}}
	backing := &row.Payload[:cap(row.Payload)][0]
	nested := &row.Pieces[0][:cap(row.Pieces[0])][0]
	seen := map[int32]bool{}
	for stream.NextInto(&row) {
		if seen[row.ID] || !bytes.Equal(row.Payload, payload) || len(row.Pieces) != 2 || string(row.Pieces[0]) != "nested" || string(row.Pieces[1]) != "storage" || row.ApplicationState != "" {
			return fmt.Errorf("incorrect streamed row %d", row.ID)
		}
		if &row.Payload[0] != backing || &row.Pieces[0][0] != nested {
			return fmt.Errorf("seeded storage replaced")
		}
		seen[row.ID] = true
		row.ApplicationState = "consumed" // Reset clears only this unmapped field.
	}
	if err := stream.Err(); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	if err := query.Wait(ctx); err != nil {
		return err
	}
	if len(seen) != 3 || !bytes.Equal(row.Payload, payload) {
		return fmt.Errorf("incomplete stream or output lost on Close")
	}
	fmt.Println("3 rows; seeded nested storage reused; query admission and drain verified")
	return nil
}
