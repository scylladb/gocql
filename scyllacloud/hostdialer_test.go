//go:build unit
// +build unit

package scyllacloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests"
)

const (
	testTimeout = time.Second
)

func TestHostSNIDialer_InvalidConnectionConfig(t *testing.T) {
	t.Parallel()

	_, serverCertPem, clientCertPem, clientKeyPem, err := setupTLSServer(nil)
	if err != nil {
		t.Fatal(err)
	}

	dialer := gocql.DefaultDialer{}

	tt := []struct {
		name          string
		connConfig    *ConnectionConfig
		hostInfo      gocql.HostSnapshot
		expectedError error
	}{
		{
			name: "empty current context",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.CurrentContext = ""
				return cc
			}(),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't get current auth info: %w", fmt.Errorf("can't get current context config: %w", fmt.Errorf("current context can't be empty")))),
		},
		{
			name: "current context is unknown",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.CurrentContext = "unknown-context"
				return cc
			}(),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't get current auth info: %w", fmt.Errorf("can't get current context config: %w", fmt.Errorf(`context "unknown-context" does not exists`)))),
		},
		{
			name: "unknown default authInfo",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.Contexts[cc.CurrentContext].AuthInfoName = "unknown-authinfo"
				return cc
			}(),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't get current auth info: %w", fmt.Errorf(`authInfo "unknown-authinfo" does not exists`))),
		},
		{
			name:          "empty client certificate",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", serverCertPem, nil, clientKeyPem),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't read client certificate: %w", &os.PathError{Op: "open", Path: "", Err: syscall.ENOENT})),
		},
		{
			name:          "empty client key",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, nil),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get client certificate from configuration: %w", fmt.Errorf("can't read client key: %w", &os.PathError{Op: "open", Path: "", Err: syscall.ENOENT})),
		},
		{
			name:          "empty certificate authority",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", nil, clientCertPem, clientKeyPem),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get root CA from configuration: %w", fmt.Errorf(`datacenter "us-east-1" does not include certificate authority`)),
		},
		{
			name: "unknown default datacenter",
			connConfig: func() *ConnectionConfig {
				cc := newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem)
				cc.Contexts[cc.CurrentContext].DatacenterName = "unknown-datacenter"
				return cc
			}(),
			hostInfo:      gocql.HostSnapshot{},
			expectedError: fmt.Errorf("can't get current datacenter config: %w", fmt.Errorf(`datacenter "unknown-datacenter" does not exists`)),
		},
		{
			name:          "unknown host datacenter",
			connConfig:    newBasicConnectionConf("127.0.0.1:9142", serverCertPem, clientCertPem, clientKeyPem),
			hostInfo:      gocql.HostSnapshot{Datacenter: "unknown-datacenter", ID: gocql.ParseUUIDMust("a0000000-0000-0000-0000-000000000099")},
			expectedError: fmt.Errorf(`datacenter "unknown-datacenter" configuration not found in connection bundle`),
		},
	}
	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hostDialer := NewSniHostDialer(tc.connConfig, dialer)
			_, err := hostDialer.TLSForHost(context.Background(), tc.hostInfo)
			if !tests.ErrEqual(err, tc.expectedError) {
				t.Errorf("expected error to be %#v, got %#v", tc.expectedError, err)
			}
		})
	}
}

func TestHostSNIDialer_ServerNameIdentifiers(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name        string
		connConfig  func(server string, serverCertPem, clientCertPem, clientKeyPem []byte) *ConnectionConfig
		hostInfo    gocql.HostSnapshot
		expectedSNI func(config *ConnectionConfig) string
	}{
		{
			name:       "node domain as SNI when host info is unknown",
			hostInfo:   gocql.HostSnapshot{},
			connConfig: newBasicConnectionConf,
			expectedSNI: func(_ *ConnectionConfig) string {
				return "node.scylladb.com"
			},
		},
		{
			name:     "server as SNI when host info is unknown and node domain is empty",
			hostInfo: gocql.HostSnapshot{},
			connConfig: func(server string, serverCertPem, clientCertPem, clientKeyPem []byte) *ConnectionConfig {
				cc := newBasicConnectionConf(server, serverCertPem, clientCertPem, clientKeyPem)
				dcConf := cc.Datacenters[cc.Contexts[cc.CurrentContext].DatacenterName]
				dcConf.NodeDomain = ""
				// Disable verification because serving cert isn't signed for IP address.
				dcConf.InsecureSkipTLSVerify = true
				return cc
			},
			expectedSNI: func(cc *ConnectionConfig) string {
				return cc.Datacenters[cc.Contexts[cc.CurrentContext].DatacenterName].Server
			},
		},
		{
			name:       "host SNI when host is known",
			hostInfo:   gocql.HostSnapshot{Datacenter: "us-east-1", ID: gocql.ParseUUIDMust("a0000000-0000-0000-0000-000000000001")},
			connConfig: newBasicConnectionConf,
			expectedSNI: func(_ *ConnectionConfig) string {
				return "a0000000-0000-0000-0000-000000000001.node.scylladb.com"
			},
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			server, serverCertPem, clientCertPem, clientKeyPem, err := setupTLSServer([]string{"a0000000-0000-0000-0000-000000000001.node.scylladb.com", "node.scylladb.com"})
			if err != nil {
				t.Fatal(err)
			}

			connectionStateCh := make(chan tls.ConnectionState, 1)
			server.TLS.VerifyConnection = func(state tls.ConnectionState) error {
				connectionStateCh <- state
				return nil
			}

			server.StartTLS()
			defer server.Close()

			dialer := gocql.DefaultDialer{}
			connConfig := tc.connConfig(server.Listener.Addr().String(), serverCertPem, clientCertPem, clientKeyPem)
			hostDialer := NewSniHostDialer(connConfig, dialer)

			tlsConfig, err := hostDialer.TLSForHost(context.Background(), tc.hostInfo)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := hostDialer.DialHost(ctx, gocql.DialTarget{Host: tc.hostInfo})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := tls.Client(conn, tlsConfig).HandshakeContext(ctx); err != nil {
				t.Fatal(err)
			}

			select {
			case receivedState := <-connectionStateCh:
				expectedSNI := tc.expectedSNI(connConfig)
				if len(receivedState.PeerCertificates) != 1 {
					t.Errorf("expected client certificate, got %d", len(receivedState.PeerCertificates))
				}
				if receivedState.ServerName != expectedSNI {
					t.Errorf("expected %q SNI, got %q", expectedSNI, receivedState.ServerName)
				}
			case <-ctx.Done():
				t.Fatal("expected to receive connection, but timed out")
			}
		})
	}
}

func setupTLSServer(dnsDomains []string) (*httptest.Server, []byte, []byte, []byte, error) {
	clientCert, clientKey, err := generateClientCert()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	clientCertPem, err := encodeCertificates(clientCert)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	clientKeyPem, err := encodePrivateKey(clientKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	clientCAPool := x509.NewCertPool()
	clientCAPool.AppendCertsFromPEM(clientCertPem)

	serverCert, serverKey, err := generateServingCert(dnsDomains)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	serverCertPem, err := encodeCertificates(serverCert)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	serverKeyPem, err := encodePrivateKey(serverKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	servingCert, err := tls.X509KeyPair(serverCertPem, serverKeyPem)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	server := httptest.NewUnstartedServer(nil)

	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{servingCert},
		ClientCAs:    clientCAPool,
		ClientAuth:   tls.RequestClientCert,
	}

	return server, serverCertPem, clientCertPem, clientKeyPem, nil
}

func newBasicConnectionConf(server string, serverCertPem, clientCertPem, clientKeyPem []byte) *ConnectionConfig {
	return &ConnectionConfig{
		Datacenters: map[string]*Datacenter{
			"us-east-1": {
				CertificateAuthorityData: serverCertPem,
				Server:                   server,
				NodeDomain:               "node.scylladb.com",
			},
		},
		AuthInfos: map[string]*AuthInfo{
			"admin": {
				ClientCertificateData: clientCertPem,
				ClientKeyData:         clientKeyPem,
			},
		},
		Contexts: map[string]*Context{
			"default": {
				DatacenterName: "us-east-1",
				AuthInfoName:   "admin",
			},
		},
		CurrentContext: "default",
	}
}

func generateServingCert(dnsNames []string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 1028)
	if err != nil {
		return nil, nil, fmt.Errorf("can't generate private key: %w", err)
	}

	commonName := "serving-cert"
	cert, err := generateSelfSignedX509Certificate(commonName, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, dnsNames, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}

func generateClientCert() (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 1028)
	if err != nil {
		return nil, nil, fmt.Errorf("can't generate private key: %w", err)
	}

	commonName := "client"
	cert, err := generateSelfSignedX509Certificate(commonName, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}

func generateSerialNumber() (*big.Int, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	return serialNumber, nil
}

func generateSelfSignedX509Certificate(cn string, extKeyUsage []x509.ExtKeyUsage, dnsNames []string, pub, priv any) (*x509.Certificate, error) {
	now := time.Now()

	serialNumber, err := generateSerialNumber()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: cn,
		},
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           extKeyUsage,
		NotBefore:             now.Add(-1 * time.Second),
		NotAfter:              now.Add(time.Hour),
		SignatureAlgorithm:    x509.SHA512WithRSA,
		BasicConstraintsValid: true,
		SerialNumber:          serialNumber,
		DNSNames:              dnsNames,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("can't create certificate: %w", err)
	}

	certs, err := x509.ParseCertificates(derBytes)
	if err != nil {
		return nil, fmt.Errorf("can't parse der encoded certificate: %w", err)
	}
	if len(certs) != 1 {
		return nil, fmt.Errorf("expected to parse 1 certificate from der bytes but %d were present", len(certs))
	}

	return certs[0], nil
}

func encodeCertificates(certificates ...*x509.Certificate) ([]byte, error) {
	buffer := bytes.Buffer{}
	for _, certificate := range certificates {
		err := pem.Encode(&buffer, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certificate.Raw,
		})
		if err != nil {
			return nil, fmt.Errorf("can't pem encode certificate: %w", err)
		}
	}
	return buffer.Bytes(), nil
}

func encodePrivateKey(key *rsa.PrivateKey) ([]byte, error) {
	buffer := bytes.Buffer{}
	err := pem.Encode(&buffer, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err != nil {
		return nil, fmt.Errorf("can't pem encode rsa private key: %w", err)
	}

	return buffer.Bytes(), nil
}

func TestHostSNIDialer_DatacenterRouting(t *testing.T) {
	for _, tc := range []struct {
		name string
		host gocql.HostSnapshot
		want string
	}{
		{name: "initial contact", want: "initial.example:443"},
		{name: "discovered remote datacenter", host: gocql.HostSnapshot{ID: gocql.ParseUUIDMust("a0000000-0000-0000-0000-000000000002"), Datacenter: "remote"}, want: "remote.example:9142"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &ConnectionConfig{
				Datacenters: map[string]*Datacenter{
					"initial": {Server: "initial.example"},
					"remote":  {Server: "remote.example:9142"},
				},
				Contexts:       map[string]*Context{"default": {DatacenterName: "initial"}},
				CurrentContext: "default",
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			called := false
			dialer := cloudTestDialer(func(gotCtx context.Context, network, address string) (net.Conn, error) {
				called = true
				if gotCtx != ctx || network != "tcp" || address != tc.want {
					t.Errorf("dial got context %v, network %q, address %q; want original context, tcp, %q", gotCtx == ctx, network, address, tc.want)
				}
				client, server := net.Pipe()
				server.Close()
				return client, nil
			})
			conn, err := NewSniHostDialer(config, dialer).DialHost(ctx, gocql.DialTarget{Host: tc.host})
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
			if !called {
				t.Fatal("custom socket dialer was not called")
			}
		})
	}
}

type cloudTestDialer func(context.Context, string, string) (net.Conn, error)

func (d cloudTestDialer) DialContext(ctx context.Context, target gocql.DialTarget) (net.Conn, error) {
	network, address := target.Network, target.Address
	return d(ctx, network, address)
}

type cloudTCPControlsRecorder struct {
	*net.TCPConn
	noDelay, keepalive []bool
}

func (c *cloudTCPControlsRecorder) SetNoDelay(v bool) error {
	c.noDelay = append(c.noDelay, v)
	return c.TCPConn.SetNoDelay(v)
}
func (c *cloudTCPControlsRecorder) SetKeepAlive(v bool) error {
	c.keepalive = append(c.keepalive, v)
	return c.TCPConn.SetKeepAlive(v)
}
func TestHostSNIDialerSOCKSPreservesRawSocketControls(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		var greeting [2]byte
		if _, err = io.ReadFull(c, greeting[:]); err != nil {
			done <- err
			return
		}
		if greeting[0] != 5 {
			done <- fmt.Errorf("wrong SOCKS version")
			return
		}
		if _, err = io.CopyN(io.Discard, c, int64(greeting[1])); err != nil {
			done <- err
			return
		}
		if _, err = c.Write([]byte{5, 0}); err != nil {
			done <- err
			return
		}
		var request [4]byte
		if _, err = io.ReadFull(c, request[:]); err != nil {
			done <- err
			return
		}
		var addressBytes int64
		switch request[3] {
		case 1:
			addressBytes = 4
		case 4:
			addressBytes = 16
		case 3:
			var n [1]byte
			if _, err = io.ReadFull(c, n[:]); err != nil {
				done <- err
				return
			}
			addressBytes = int64(n[0])
		default:
			done <- fmt.Errorf("unsupported SOCKS address type")
			return
		}
		if _, err = io.CopyN(io.Discard, c, addressBytes+2); err != nil {
			done <- err
			return
		}
		if _, err = c.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0x23, 0x52}); err != nil {
			done <- err
			return
		}
		var end [1]byte
		_, err = c.Read(end[:])
		if err == io.EOF {
			err = nil
		}
		done <- err
	}()
	captured := make(chan *cloudTCPControlsRecorder, 1)
	dialer := gocql.DialerFunc(func(ctx context.Context, target gocql.DialTarget) (net.Conn, error) {
		raw, err := (gocql.DefaultDialer{}).DialContext(ctx, target)
		if err != nil {
			return nil, err
		}
		record := &cloudTCPControlsRecorder{TCPConn: raw.(*net.TCPConn)}
		captured <- record
		return record, nil
	})
	config := &ConnectionConfig{Datacenters: map[string]*Datacenter{"local": {Server: "cql.example:9042", ProxyURL: "socks5://" + listener.Addr().String()}}, Contexts: map[string]*Context{"default": {DatacenterName: "local"}}, CurrentContext: "default"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := NewSniHostDialer(config, dialer).DialHost(ctx, gocql.DialTarget{Network: "tcp", Address: "cql.example:9042"})
	if err != nil {
		t.Fatal(err)
	}
	controls, ok := conn.(interface {
		SetNoDelay(bool) error
		SetKeepAlive(bool) error
		SetReadBuffer(int) error
	})
	if !ok {
		conn.Close()
		t.Fatal("SOCKS wrapper hid raw socket controls")
	}
	if err = controls.SetNoDelay(false); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if err = controls.SetKeepAlive(false); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if err = controls.SetReadBuffer(4096); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	raw := <-captured
	if len(raw.noDelay) != 1 || raw.noDelay[0] || len(raw.keepalive) != 1 || raw.keepalive[0] {
		t.Fatal("SOCKS settings not forwarded to real raw socket")
	}
	conn.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
