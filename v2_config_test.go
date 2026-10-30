package gocql

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func TestV2RuntimeConfigSnapshotAndValidation(t *testing.T) {
	config := DefaultConfig("localhost")
	config.TLS = &tls.Config{ServerName: "original"}
	config.Metadata.Keyspaces = []string{"ks"}
	copy := snapshotConfig(config)
	config.ContactPoints[0] = "changed"
	config.Metadata.Keyspaces[0] = "changed"
	config.TLS.ServerName = "changed"
	delete(config.Partitioners, "Murmur3Partitioner")
	if copy.ContactPoints[0] != "localhost" || copy.Metadata.Keyspaces[0] != "ks" || copy.TLS.ServerName != "original" || copy.Partitioners["Murmur3Partitioner"] == nil {
		t.Fatal("configuration aliases mutable input")
	}
	for _, edit := range []func(*Config){func(c *Config) { c.Protocol = 6 }, func(c *Config) { c.Metadata.AgreementPoll = -1 }, func(c *Config) { c.Limits.MaxMetadataDepth = 129 }, func(c *Config) { c.Pool.MaxRequestsPerConnection = 32769 }, func(c *Config) { c.Metadata.Keyspaces = []string{"a", "a"} }} {
		invalid := DefaultConfig("localhost")
		edit(&invalid)
		var validation *ValidationError
		if err := validateConfig(invalid); !errors.As(err, &validation) {
			t.Fatalf("validation error %v", err)
		}
	}
}
func TestV2RuntimeTimestampGeneratorConcurrent(t *testing.T) {
	generator := newTimestampGenerator()
	values := make(chan int64, 1000)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				values <- generator()
			}
		}()
	}
	seen := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		value := <-values
		if seen[value] {
			t.Fatal("duplicate timestamp")
		}
		seen[value] = true
	}
}
func TestV2RuntimeDNSContextAndDialHost(t *testing.T) {
	resolver := &v2DNSResolver{InternalBase: runtimeBlockingDNS{done: make(chan struct{})}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resolver.InternalStartup.Store(&contextBox{InternalCtx: ctx})
	if _, err := resolver.LookupIP("ignored"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(resolver.InternalBase.(runtimeBlockingDNS).done)
	config := DefaultConfig("localhost")
	var calls atomic.Int32
	client, server := net.Pipe()
	defer server.Close()
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		host := target.Host
		calls.Add(1)
		if host.Datacenter != "dc" || host.Address != netip.MustParseAddrPort("127.0.0.1:9042") {
			t.Fatalf("host snapshot %+v", host)
		}
		return client, nil
	}
	host := func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = net.IPv4(127, 0, 0, 1)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = 9042
		implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = "dc"
		return opaqueStorageValue
	}()
	dialer := v2TLSHostDialer{InternalConfig: connectivityConfig(config)}
	connection, err := dialer.DialHost(context.Background(), host)
	if err != nil {
		t.Fatal(err)
	}
	connection.Conn.Close()
	if calls.Load() != 1 {
		t.Fatal("host hook not called")
	}
}

type runtimeBlockingDNS struct{ done chan struct{} }

func (d runtimeBlockingDNS) LookupIP(string) ([]net.IP, error) { <-d.done; return nil, nil }
func TestV2RuntimeConnectCancelledBeforeSetup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if _, err := Connect(ctx, DefaultConfig("unresolved")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
