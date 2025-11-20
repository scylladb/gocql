package gocql

import (
	"net"
	"sync"
	"testing"
	"time"
)

type dnsResolverFunc func(string) ([]net.IP, error)

// LookupIP implements DNSResolver for dnsResolverFunc.
func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) { return f(host) }

func TestDNSEndpointResolver_MinResolvePeriod(t *testing.T) {
	var mu sync.Mutex
	var calls int

	lookup := dnsResolverFunc(func(_ string) ([]net.IP, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return []net.IP{net.ParseIP("10.0.0.1")}, nil
	})

	resolver := newSimplePortMuxResolver(50*time.Millisecond, time.Millisecond, lookup)
	endpoint := ResolvedConnectionMetadata{UnresolvedConnectionMetadata: UnresolvedConnectionMetadata{Address: "example.com"}}

	if _, _, err := resolver.Resolve(endpoint); err != nil {
		t.Fatalf("first resolve unexpectedly failed: %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected first resolve to call lookup once, got %d", calls)
	}

	if _, _, err := resolver.Resolve(endpoint); err == nil {
		t.Fatalf("expected resolve to be throttled, got nil error")
	}

	if calls != 1 {
		t.Fatalf("lookup invoked despite throttling, got %d calls", calls)
	}

	time.Sleep(60 * time.Millisecond)

	if _, _, err := resolver.Resolve(endpoint); err != nil {
		t.Fatalf("resolve after waiting failed: %v", err)
	}

	if calls != 2 {
		t.Fatalf("expected lookup to be called again after wait, got %d", calls)
	}
}
