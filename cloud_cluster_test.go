//go:build integration
// +build integration

package gocql_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	gocql "github.com/scylladb/gocql/v2"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/scylladb/gocql/v2/scyllacloud"
)

func TestCloudConnection(t *testing.T) {
	t.Parallel()

	if !*gocql.FlagRunSslTest {
		t.Skip("Skipping because SSL is not enabled on cluster")
	}

	if *gocql.FlagDistribution != "scylla" {
		t.Skip("Skipping because it is designed for scylla, but running on something else")
	}

	const (
		sslPort        = 9142
		datacenterName = "datacenter1"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := map[string]string{}

	cluster := gocql.CreateV2Cluster(func(config *gocql.Config) {
		config.Port = sslPort
	})
	session, err := gocql.Connect(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}

	defer session.Close()
	rows, err := cloudHostRows(ctx, session, "SELECT broadcast_address, host_id FROM system.local WHERE key='local'", gocql.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("read local host: %v", rows.Err())
	}
	local := rows.Row()
	localAddress, localHostID := local.First.String(), local.Second
	hosts[localHostID.String()] = net.JoinHostPort(localAddress, fmt.Sprintf("%d", sslPort))
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Both system tables must describe the same coordinator's view of the ring.
	rows, err = cloudHostRows(ctx, session, "SELECT peer, host_id FROM system.peers", gocql.Options{
		HostID: gocql.Option[gocql.UUID]{Value: localHostID, Set: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		peer := rows.Row()
		hosts[peer.Second.String()] = net.JoinHostPort(peer.First.String(), fmt.Sprintf("%d", sslPort))
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	session.Close()

	logger := gocql.NewTestLogger()
	defer func() {
		if t.Failed() {
			os.Stdout.WriteString(logger.String())
		}
	}()

	proxy := &sniProxy{
		hosts:          hosts,
		defaultBackend: net.JoinHostPort(localAddress, fmt.Sprintf("%d", sslPort)),
		logger:         logger,
	}

	proxyAddress, err := proxy.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	cc := &scyllacloud.ConnectionConfig{
		Datacenters: map[string]*scyllacloud.Datacenter{
			datacenterName: {
				CertificateAuthorityPath: "testdata/pki/ca.crt",
				Server:                   proxyAddress,
				TLSServerName:            "any",
				NodeDomain:               "cloud.scylladb.com",
				InsecureSkipTLSVerify:    true,
			},
		},
		AuthInfos: map[string]*scyllacloud.AuthInfo{
			"ai-1": {
				Username:              "cassandra",
				Password:              "cassandra",
				ClientKeyPath:         "testdata/pki/gocql.key",
				ClientCertificatePath: "testdata/pki/gocql.crt",
			},
		},
		Contexts: map[string]*scyllacloud.Context{
			"default-context": {
				AuthInfoName:   "ai-1",
				DatacenterName: datacenterName,
			},
		},
		CurrentContext: "default-context",
	}

	configPath, err := writeYamlToTempFile(cc)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(configPath)

	fixtureTLS := cluster.TLS
	cluster, err = scyllacloud.NewCloudCluster(configPath, func(config *gocql.Config) {
		// Forward connections directed to node domain to our test SNI proxy.
		config.Dialer = gocql.DialerFunc(func(ctx context.Context, target gocql.DialTarget) (net.Conn, error) {
			network, addr := target.Network, target.Address
			if strings.Contains(addr, cc.Datacenters[datacenterName].NodeDomain) {
				addr = cc.Datacenters[datacenterName].Server
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	// The test backend identifies itself by URI SAN. Preserve UUID-based SNI
	// and client authentication while validating its CA and fixture URI.
	cloudTLS := cluster.TLSForHost
	cluster.TLSForHost = func(ctx context.Context, host gocql.HostSnapshot) (*tls.Config, error) {
		config, err := cloudTLS(ctx, host)
		if err != nil {
			return nil, err
		}
		config.InsecureSkipVerify = true
		config.VerifyConnection = fixtureTLS.VerifyConnection
		return config, nil
	}
	session, err = gocql.Connect(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if err := gocql.WaitUntilPoolsStopFilling(ctx, session, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	ringHosts := session.Metadata().Hosts()
	if len(ringHosts) != len(hosts) {
		t.Errorf("expected %d hosts in ring, got %d", len(hosts), len(ringHosts))
	}

	for _, host := range ringHosts {
		rows, err := cloudHostRows(ctx, session, "SELECT broadcast_address, host_id FROM system.local WHERE key='local'", gocql.Options{
			HostID: gocql.Option[gocql.UUID]{Value: host.ID, Set: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatalf("read cloud host %s: %v", host.ID, rows.Err())
		}
		if actual := rows.Row().Second; actual != host.ID {
			t.Errorf("SNI route for %s reached host %s", host.ID, actual)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}

	snisCount := map[string]int{}
	events := proxy.GetEvents()
	for _, event := range events {
		snisCount[event]++
	}

	for hostID := range hosts {
		sni := fmt.Sprintf("%s.%s", hostID, cc.Datacenters[datacenterName].NodeDomain)
		count, ok := snisCount[sni]
		if !ok {
			t.Errorf("not found connection to host %q", hostID)
		}
		if count != cluster.Pool.ConnectionsPerHost {
			t.Errorf("expected %d connections to host %q, got %d", cluster.Pool.ConnectionsPerHost, sni, count)
		}
	}
}

func cloudHostRows(ctx context.Context, session *gocql.Session, cql string, options gocql.Options) (*gocql.Rows[gocql.Pair[netip.Addr, gocql.UUID]], error) {
	statement := gocql.Statement[gocql.Unit, gocql.Pair[netip.Addr, gocql.UUID]]{
		CQL:    cql,
		Params: gocql.NoParams,
		Rows:   gocql.PairRows(1, gocql.OneColumn(gocql.InetCodec), gocql.OneColumn(gocql.UUIDCodec)),
	}
	bound, err := gocql.BindUnpreparedOwned(statement, gocql.NewColumns(), gocql.Unit{})
	if err != nil {
		return nil, err
	}
	defer bound.Close()
	return gocql.Run(ctx, session, bound, options)
}

func writeYamlToTempFile(obj any) (string, error) {
	f, err := os.CreateTemp(os.TempDir(), "gocql-cloud")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close temp file: %w", err)
	}

	buf, err := yaml.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("marshal yaml: %w", err)
	}
	if err := os.WriteFile(f.Name(), buf, 0600); err != nil {
		return "", fmt.Errorf("write to file %q: %w", f.Name(), err)
	}

	return f.Name(), nil
}

type dialerContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func (d dialerContextFunc) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d(ctx, network, addr)
}

type sniProxy struct {
	hosts          map[string]string
	defaultBackend string
	logger         gocql.StdLogger

	listener  net.Listener
	wg        sync.WaitGroup
	conns     map[net.Conn]struct{}
	closed    bool
	closeOnce sync.Once
	closeErr  error
	ctx       context.Context
	events    []string
	mu        sync.Mutex
}

func (p *sniProxy) Run(ctx context.Context) (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("failed to listen: %w", err)
	}

	p.listener = listener
	p.ctx = ctx
	p.conns = make(map[net.Conn]struct{})
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		for {
			conn, err := p.listener.Accept()
			if err != nil {
				p.logger.Println("failed to accept connection", err)
				return
			}

			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				conn.Close()
				return
			}
			p.conns[conn] = struct{}{}
			p.wg.Add(1)
			p.mu.Unlock()
			go func() {
				defer p.wg.Done()
				defer func() { p.mu.Lock(); delete(p.conns, conn); p.mu.Unlock() }()
				p.handleConnection(conn)
			}()
		}

	}()

	context.AfterFunc(ctx, func() { _ = p.Close() })
	return listener.Addr().String(), nil
}

func (p *sniProxy) handleConnection(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	var hello *tls.ClientHelloInfo

	peekedBytes := &bytes.Buffer{}
	// Ignore error because TLS library returns it when nil TLSConfig is returned.
	_ = tls.Server(readOnlyConn{reader: io.TeeReader(conn, peekedBytes)}, &tls.Config{
		GetConfigForClient: func(argHello *tls.ClientHelloInfo) (*tls.Config, error) {
			hello = &tls.ClientHelloInfo{}
			*hello = *argHello
			return nil, nil
		},
	}).Handshake()

	if hello == nil {
		p.logger.Println("client hello not sent")
		return
	}

	p.mu.Lock()
	p.events = append(p.events, hello.ServerName)
	p.mu.Unlock()

	hostID, _, _ := strings.Cut(hello.ServerName, ".")
	backend, ok := p.hosts[hostID]
	if !ok {
		backend = p.defaultBackend
	}

	p.logger.Println("Dialing backend", backend, "SNI", hello.ServerName)
	backendConn, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(p.ctx, "tcp", backend)
	if err != nil {
		p.logger.Println("failed to dial backend", backend, err)
		return
	}
	defer backendConn.Close()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.conns[backendConn] = struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.conns, backendConn); p.mu.Unlock() }()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		_, _ = io.Copy(conn, backendConn)
		conn.Close()
		backendConn.Close()
		wg.Done()
	}()
	go func() {
		_, _ = io.Copy(backendConn, peekedBytes)
		_, _ = io.Copy(backendConn, conn)
		conn.Close()
		backendConn.Close()
		wg.Done()
	}()

	wg.Wait()
}

func (p *sniProxy) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.closeErr = p.listener.Close()
		for conn := range p.conns {
			conn.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return p.closeErr
}

func (p *sniProxy) GetEvents() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]string, 0, len(p.events))
	for _, e := range p.events {
		events = append(events, e)
	}
	return events
}

type readOnlyConn struct {
	reader io.Reader
}

var _ net.Conn = readOnlyConn{}

func (conn readOnlyConn) Read(p []byte) (int, error)         { return conn.reader.Read(p) }
func (conn readOnlyConn) Write(p []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (conn readOnlyConn) Close() error                       { return nil }
func (conn readOnlyConn) LocalAddr() net.Addr                { return nil }
func (conn readOnlyConn) RemoteAddr() net.Addr               { return nil }
func (conn readOnlyConn) SetDeadline(t time.Time) error      { return nil }
func (conn readOnlyConn) SetReadDeadline(t time.Time) error  { return nil }
func (conn readOnlyConn) SetWriteDeadline(t time.Time) error { return nil }

func TestStep08SNIProxyCancellationJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &sniProxy{hosts: map[string]string{}, logger: gocql.NewTestLogger()}
	address, err := proxy.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte{0x16}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}
	proxy.mu.Lock()
	remaining := len(proxy.conns)
	proxy.mu.Unlock()
	if remaining != 0 {
		t.Fatal("proxy close returned before joining connections", remaining)
	}
	if err := proxy.Close(); err != nil {
		t.Fatal("repeat proxy close", err)
	}
	if _, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		t.Fatal("proxy listener survived close")
	}
}
